use once_cell::sync::Lazy;
use std::borrow::BorrowMut;
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::error::Error;
use std::ffi::FromBytesUntilNulError;
use std::future::Future;
use std::io::{self, Read, Write};
use std::mem::MaybeUninit;
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::pin::Pin;
use std::sync::{mpsc, Arc, LazyLock, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;
use thiserror::Error;

use futures::{
    future::{BoxFuture, FutureExt},
    task::{waker_ref, ArcWake},
};

pub struct Socket {
    s: Arc<Mutex<UnixStream>>,
}

enum SocketTask {
    Create(
        String,
        Arc<Mutex<Option<Result<Socket, String>>>>,
        Arc<Mutex<Option<Waker>>>,
    ),
    Read(
        Arc<Mutex<UnixStream>>,
        Arc<Mutex<Option<Waker>>>,
        Arc<Mutex<Result<usize, io::Error>>>,
    ),
}

struct Task {
    future: Mutex<Option<BoxFuture<'static, ()>>>,
    task_tx: mpsc::SyncSender<Arc<Task>>,
}

impl ArcWake for Task {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.task_tx.send(arc_self.clone()).unwrap();
    }
}

struct MarsyncContext {
    socket_tx: mpsc::SyncSender<Arc<SocketTask>>,
    socket_rx: Option<mpsc::Receiver<Arc<SocketTask>>>,
    task_tx: mpsc::SyncSender<Arc<Task>>,
    task_rx: Option<mpsc::Receiver<Arc<Task>>>,
}

fn new_marsync() -> Arc<Mutex<MarsyncContext>> {
    let (socket_tx, socket_rx) = mpsc::sync_channel(16);
    let (task_tx, task_rx) = mpsc::sync_channel(16);
    Arc::new(Mutex::new(MarsyncContext {
        socket_tx: socket_tx,
        socket_rx: Some(socket_rx),
        task_rx: Some(task_rx),
        task_tx: task_tx,
    }))
}

static CONTEXT: LazyLock<Arc<Mutex<MarsyncContext>>> = std::sync::LazyLock::new(|| new_marsync());

pub struct CreateSocket {
    ret_socket: Arc<Mutex<Option<Result<Socket, String>>>>,
    waker: Arc<Mutex<Option<Waker>>>,
}

impl Future for CreateSocket {
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

impl CreateSocket {
    fn new(path: String) -> Self {
        let ret = Arc::new(Mutex::new(None));
        let waker = Arc::new(Mutex::new(None));
        let marsync = CONTEXT.lock().unwrap();
        marsync
            .socket_tx
            .send(Arc::new(SocketTask::Create(
                path,
                ret.clone(),
                waker.clone(),
            )))
            .unwrap();
        CreateSocket {
            ret_socket: ret,
            waker,
        }
    }
}

impl Socket {
    pub fn read<'a, const T: usize>(&self, buf: &'a mut [u8; T]) -> ReadSocket<'a, T> {
        ReadSocket {
            ret_data: buf,
            waker: Arc::new(Mutex::new(None)),
            s: self.s.clone(),
            socket_op_status: Arc::new(Mutex::new(Ok(0))),
        }
    }
}

pub struct ReadSocket<'a, const T: usize> {
    ret_data: &'a mut [u8; T],
    waker: Arc<Mutex<Option<Waker>>>,
    s: Arc<Mutex<UnixStream>>,
    socket_op_status: Arc<Mutex<Result<usize, io::Error>>>,
}

impl<'a, const T: usize> Future for ReadSocket<'a, T> {
    type Output = io::Result<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        println!("poll");
        *self.waker.lock().unwrap() = Some(cx.waker().clone());
        let other_socket_op_status = self.socket_op_status.clone();
        let mut socket_result = other_socket_op_status.lock().unwrap();
        if socket_result.is_err() {
            let e = socket_result.as_mut().err().unwrap();
            return Poll::Ready(Err(std::io::Error::new(e.kind(), e.to_string())));
        }
        let other_s = self.s.clone();
        let ret = match other_s.lock().unwrap().read(self.ret_data) {
            Ok(bytes) => {
                println!("ok {}", bytes);
                Poll::Ready(Ok(bytes))
            }
            Err(err) => {
                println!("err {:#?}", err);
                if err.kind() == std::io::ErrorKind::WouldBlock {
                    let marsync = CONTEXT.lock().unwrap();
                    marsync
                        .socket_tx
                        .send(Arc::new(SocketTask::Read(
                            other_s.clone(),
                            self.waker.clone(),
                            self.socket_op_status.clone(),
                        )))
                        .unwrap();
                    Poll::Pending
                } else {
                    Poll::Ready(Err(err))
                }
            }
        };
        ret
    }
}

pub fn spawn(future: impl Future<Output = ()> + 'static + Send) {
    let marsync = CONTEXT.lock().unwrap();
    let t = Arc::new(Task {
        future: Mutex::new(Some(future.boxed())),
        task_tx: marsync.task_tx.clone(),
    });
    marsync.task_tx.send(t).unwrap();
}

pub fn run() {
    let mut marsync = CONTEXT.lock().unwrap();
    let socket_rx = marsync.socket_rx.take().unwrap();
    let task_rx = marsync.task_rx.take().unwrap();
    drop(marsync);
    let t1 = std::thread::spawn(|| socket_thread(socket_rx));
    let t2 = std::thread::spawn(|| executor_thread(task_rx));
    t1.join().unwrap();
    t2.join().unwrap();
}

fn socket_thread_create(
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

fn socket_thread_read(
    s_arc: &Arc<Mutex<UnixStream>>,
    waker: &Arc<Mutex<Option<Waker>>>,
    res: &Arc<Mutex<Result<usize, io::Error>>>,
) {
    let s = s_arc.lock().unwrap();
    let mut r = res.lock().unwrap();
    println!("waiting for data");
    match wait_for_data(&s) {
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
    println!("data arrived");
}

fn socket_thread(socket_rx: mpsc::Receiver<Arc<SocketTask>>) {
    loop {
        let socket_task = socket_rx.recv().unwrap();
        match socket_task.as_ref() {
            SocketTask::Create(path, task_ret, waker) => {
                socket_thread_create(path, task_ret, waker)
            }
            SocketTask::Read(s_arc, waker, res) => socket_thread_read(s_arc, waker, res),
        }
    }
}

fn executor_thread(task_rx: mpsc::Receiver<Arc<Task>>) {
    loop {
        let task = task_rx.recv().unwrap();
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

pub fn create_socket(path: String) -> CreateSocket {
    CreateSocket::new(path)
}

fn wait_for_data(stream: &UnixStream) -> io::Result<bool> {
    let fd = stream.as_raw_fd();
    let mut read_fds: libc::fd_set = unsafe { MaybeUninit::zeroed().assume_init() };
    unsafe { libc::FD_ZERO(&mut read_fds) };
    unsafe { libc::FD_SET(fd, &mut read_fds) };

    let result = unsafe {
        libc::select(
            fd + 1,
            &mut read_fds,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };

    match result {
        -1 => Err(io::Error::last_os_error()),
        0 => Ok(false), // Timeout
        _ => Ok(unsafe { libc::FD_ISSET(fd, &read_fds) }),
    }
}
