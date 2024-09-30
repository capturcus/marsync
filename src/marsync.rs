use std::future::Future;
use std::io::{self, Read, Write};
use std::mem::MaybeUninit;
use std::os::fd::AsRawFd;
use std::os::unix::net::{UnixListener, UnixStream};
use std::pin::Pin;
use std::sync::{mpsc, Arc, LazyLock, Mutex};
use std::task::{Context, Poll, Waker};

use futures::Stream;
use futures::{
    future::{BoxFuture, FutureExt},
    task::{waker_ref, ArcWake},
};

pub struct Socket {
    s: Arc<Mutex<UnixStream>>,
}

enum SocketTask {
    Connect(
        String,
        Arc<Mutex<Option<Result<Socket, String>>>>,
        Arc<Mutex<Option<Waker>>>,
    ),
    SocketBlockingOp(
        Arc<Mutex<UnixStream>>,
        Arc<Mutex<Option<Waker>>>,
        Arc<Mutex<Result<usize, io::Error>>>,
        SocketOp,
    ),
    NextClient(
        Arc<Mutex<UnixListener>>,
        Arc<Mutex<Option<Waker>>>,
        Arc<Mutex<Option<Result<Socket, String>>>>,
    ),
}

struct Task {
    future: Mutex<Option<BoxFuture<'static, ()>>>,
    task_tx: Arc<Mutex<mpsc::SyncSender<Arc<Task>>>>,
}

impl ArcWake for Task {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self
            .task_tx
            .lock()
            .unwrap()
            .send(arc_self.clone())
            .unwrap();
    }
}

struct MarsyncContext {
    socket_tx: Arc<Mutex<mpsc::SyncSender<Arc<SocketTask>>>>,
    socket_rx: Arc<Mutex<mpsc::Receiver<Arc<SocketTask>>>>,
    task_tx: Arc<Mutex<mpsc::SyncSender<Arc<Task>>>>,
    task_rx: Arc<Mutex<mpsc::Receiver<Arc<Task>>>>,
}

fn new_marsync() -> Arc<Mutex<MarsyncContext>> {
    let (socket_tx, socket_rx) = mpsc::sync_channel(16);
    let (task_tx, task_rx) = mpsc::sync_channel(16);
    Arc::new(Mutex::new(MarsyncContext {
        socket_tx: Arc::new(Mutex::new(socket_tx)),
        socket_rx: Arc::new(Mutex::new(socket_rx)),
        task_rx: Arc::new(Mutex::new(task_rx)),
        task_tx: Arc::new(Mutex::new(task_tx)),
    }))
}

static CONTEXT: LazyLock<Arc<Mutex<MarsyncContext>>> = std::sync::LazyLock::new(new_marsync);
const SOCKET_THREAD_NUM: usize = 3;
const EXECUTOR_THREAD_NUM: usize = 3;

pub struct ConnectSocket {
    ret_socket: Arc<Mutex<Option<Result<Socket, String>>>>,
    waker: Arc<Mutex<Option<Waker>>>,
}

impl Future for ConnectSocket {
    type Output = Result<Socket, String>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut ret = self.ret_socket.lock().unwrap();
        let mut w = self.waker.lock().unwrap();
        *w = Some(cx.waker().clone());
        match ret.take() {
            Some(s) => Poll::Ready(s),
            None => Poll::Pending,
        }
    }
}

impl ConnectSocket {
    fn new(path: String) -> Self {
        let ret = Arc::new(Mutex::new(None));
        let waker = Arc::new(Mutex::new(None));
        let marsync = CONTEXT.lock().unwrap();
        marsync
            .socket_tx
            .lock()
            .unwrap()
            .send(Arc::new(SocketTask::Connect(
                path,
                ret.clone(),
                waker.clone(),
            )))
            .unwrap();
        ConnectSocket {
            ret_socket: ret,
            waker,
        }
    }
}

impl Socket {
    pub fn read<'a>(&self, buf: &'a mut [u8]) -> BlockingSocketOp<'a> {
        BlockingSocketOp {
            read_data: Some(buf),
            waker: Arc::new(Mutex::new(None)),
            s: self.s.clone(),
            socket_op_status: Arc::new(Mutex::new(Ok(0))),
            op: SocketOp::Read,
            write_data: &[0],
        }
    }
    pub fn write<'a>(&self, buf: &'a [u8]) -> BlockingSocketOp<'a> {
        BlockingSocketOp {
            read_data: None,
            waker: Arc::new(Mutex::new(None)),
            s: self.s.clone(),
            socket_op_status: Arc::new(Mutex::new(Ok(0))),
            op: SocketOp::Write,
            write_data: buf,
        }
    }
}

pub struct BlockingSocketOp<'a> {
    read_data: Option<&'a mut [u8]>,
    write_data: &'a [u8],
    waker: Arc<Mutex<Option<Waker>>>,
    s: Arc<Mutex<UnixStream>>,
    socket_op_status: Arc<Mutex<Result<usize, io::Error>>>,
    op: SocketOp,
}

impl<'a> Future for BlockingSocketOp<'a> {
    type Output = io::Result<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        *self.waker.lock().unwrap() = Some(cx.waker().clone());
        let other_socket_op_status = self.socket_op_status.clone();
        let mut socket_result = other_socket_op_status.lock().unwrap();
        if socket_result.is_err() {
            let e = socket_result.as_mut().err().unwrap();
            return Poll::Ready(Err(std::io::Error::new(e.kind(), e.to_string())));
        }
        let other_s = self.s.clone();
        let ret;
        match self.op {
            SocketOp::Read => {
                let read_buf = self.read_data.take().unwrap();
                ret = other_s.lock().unwrap().read(read_buf);
                self.read_data = Some(read_buf);
            }
            SocketOp::Write => {
                ret = other_s.lock().unwrap().write(self.write_data);
            }
        }
        match ret {
            Ok(bytes) => Poll::Ready(Ok(bytes)),
            Err(err) => {
                if err.kind() == std::io::ErrorKind::WouldBlock {
                    let marsync = CONTEXT.lock().unwrap();
                    marsync
                        .socket_tx
                        .lock()
                        .unwrap()
                        .send(Arc::new(SocketTask::SocketBlockingOp(
                            other_s.clone(),
                            self.waker.clone(),
                            self.socket_op_status.clone(),
                            self.op,
                        )))
                        .unwrap();
                    Poll::Pending
                } else {
                    Poll::Ready(Err(err))
                }
            }
        }
    }
}

pub fn spawn(future: impl Future<Output = ()> + 'static + Send) {
    let marsync = CONTEXT.lock().unwrap();
    let t = Arc::new(Task {
        future: Mutex::new(Some(future.boxed())),
        task_tx: marsync.task_tx.clone(),
    });
    marsync.task_tx.lock().unwrap().send(t).unwrap();
}

pub fn run() {
    let marsync = CONTEXT.lock().unwrap();
    let socket_rx = marsync.socket_rx.clone();
    let task_rx = marsync.task_rx.clone();
    drop(marsync);
    let mut handles = Vec::new();
    for _ in 0..SOCKET_THREAD_NUM {
        let new_socket_rx = socket_rx.clone();
        handles.push(std::thread::spawn(move || socket_thread(new_socket_rx)));
    }
    for _ in 0..EXECUTOR_THREAD_NUM {
        let new_task_rx = task_rx.clone();
        handles.push(std::thread::spawn(|| executor_thread(new_task_rx)));
    }
    for h in handles {
        h.join().unwrap();
    }
}

fn socket_thread_connect(
    path: &String,
    task_ret: &Arc<std::sync::Mutex<Option<Result<Socket, String>>>>,
    waker: &Arc<std::sync::Mutex<Option<Waker>>>,
) {
    let res = UnixStream::connect(path);
    let mut t = task_ret.lock().unwrap();
    match res {
        Ok(s) => {
            s.set_nonblocking(false).unwrap();
            s.set_read_timeout(Some(std::time::Duration::from_nanos(1)))
                .unwrap();
            *t = Some(Ok(Socket {
                s: Arc::new(Mutex::new(s)),
            }));
        }
        Err(e) => *t = Some(Err(e.to_string())),
    }
    if let Some(w) = waker.lock().unwrap().take() {
        w.wake();
    }
}

#[derive(PartialEq, Debug, Clone, Copy)]
enum SocketOp {
    Read,
    Write,
}

fn socket_thread_wait_for_op(
    s_arc: &Arc<Mutex<UnixStream>>,
    waker: &Arc<Mutex<Option<Waker>>>,
    res: &Arc<Mutex<Result<usize, io::Error>>>,
    op: SocketOp,
) {
    let s = s_arc.lock().unwrap();
    let mut r = res.lock().unwrap();
    match wait_for_socket_op(&s, op) {
        Ok(true) => {
            if let Some(w) = waker.lock().unwrap().take() {
                w.wake();
            }
        }
        Ok(false) => {
            *r = Err(std::io::Error::new(io::ErrorKind::TimedOut, ""));
            if let Some(w) = waker.lock().unwrap().take() {
                w.wake();
            }
        }
        Err(e) => {
            *r = Err(e);
            if let Some(w) = waker.lock().unwrap().take() {
                w.wake();
            }
        }
    }
}

fn socket_thread_next_client(
    listener: &Arc<Mutex<UnixListener>>,
    waker_slot: &Arc<Mutex<Option<Waker>>>,
    result_socket_slot: &Arc<Mutex<Option<Result<Socket, String>>>>,
) {
    match listener.lock().unwrap().accept() {
        Ok((s, _)) => {
            *result_socket_slot.lock().unwrap() = Some(Ok(Socket {
                s: Arc::new(Mutex::new(s)),
            }))
        }
        Err(e) => *result_socket_slot.lock().unwrap() = Some(Err(e.to_string())),
    }
    if let Some(w) = waker_slot.lock().unwrap().take() {
        w.wake();
    }
}

fn socket_thread(socket_rx: Arc<Mutex<mpsc::Receiver<Arc<SocketTask>>>>) {
    loop {
        let socket_task;
        {
            socket_task = socket_rx.lock().unwrap().recv().unwrap();
        }
        match socket_task.as_ref() {
            SocketTask::Connect(path, task_ret, waker) => {
                socket_thread_connect(path, task_ret, waker)
            }
            SocketTask::SocketBlockingOp(s_arc, waker, res, op) => {
                socket_thread_wait_for_op(s_arc, waker, res, *op)
            }
            SocketTask::NextClient(listener, waker_slot, result_socket_slot) => {
                socket_thread_next_client(listener, waker_slot, result_socket_slot)
            }
        }
    }
}

fn executor_thread(task_rx: Arc<Mutex<mpsc::Receiver<Arc<Task>>>>) {
    loop {
        let task;
        {
            task = task_rx.lock().unwrap().recv().unwrap();
        }
        let waker = waker_ref(&task);
        let context = &mut Context::from_waker(&waker);
        let mut future_slot = task.future.lock().unwrap();
        if let Some(mut f) = future_slot.take() {
            if f.as_mut().poll(context).is_pending() {
                *future_slot = Some(f);
            }
        }
    }
}

pub fn connect(path: String) -> ConnectSocket {
    ConnectSocket::new(path)
}

fn wait_for_socket_op(stream: &UnixStream, op: SocketOp) -> io::Result<bool> {
    let fd = stream.as_raw_fd();
    let mut fds: libc::fd_set = unsafe { MaybeUninit::zeroed().assume_init() };
    unsafe { libc::FD_ZERO(&mut fds) };
    unsafe { libc::FD_SET(fd, &mut fds) };
    let result = unsafe {
        libc::select(
            fd + 1,
            if op == SocketOp::Read {
                &mut fds
            } else {
                std::ptr::null_mut()
            },
            if op == SocketOp::Write {
                &mut fds
            } else {
                std::ptr::null_mut()
            },
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };

    match result {
        -1 => Err(io::Error::last_os_error()),
        0 => Ok(false), // Timeout
        _ => Ok(unsafe { libc::FD_ISSET(fd, &fds) }),
    }
}

pub struct NextClient {
    waker_slot: Arc<Mutex<Option<Waker>>>,
    result_socket_slot: Arc<Mutex<Option<Result<Socket, String>>>>,
}

impl Stream for NextClient {
    type Item = Result<Socket, String>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        *self.waker_slot.lock().unwrap() = Some(cx.waker().clone());
        let mut s = self.result_socket_slot.lock().unwrap();
        if s.is_none() {
            return Poll::Pending;
        }
        let res = s.take().unwrap();
        Poll::Ready(Some(res))
    }
}

pub struct Listener {
    l: Arc<Mutex<UnixListener>>,
}

impl Listener {
    pub fn next(&self) -> NextClient {
        let marsync = CONTEXT.lock().unwrap();
        let waker_slot = Arc::new(Mutex::new(None));
        let result_socket_slot = Arc::new(Mutex::new(None));
        marsync
            .socket_tx
            .lock()
            .unwrap()
            .send(Arc::new(SocketTask::NextClient(
                self.l.clone(),
                waker_slot.clone(),
                result_socket_slot.clone(),
            )))
            .unwrap();
        NextClient {
            waker_slot,
            result_socket_slot,
        }
    }
}

pub fn create_listener(path: String) -> Listener {
    let l = UnixListener::bind(path).unwrap();
    Listener {
        l: Arc::new(Mutex::new(l)),
    }
}
