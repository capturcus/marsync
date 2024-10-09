use std::future::Future;
use std::io::{self, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::net::{UnixListener, UnixStream};
use std::pin::Pin;
use std::sync::{mpsc, Arc, LazyLock, Mutex};
use std::task::Context;

use futures::channel::oneshot::{self, Canceled};
use futures::task::{waker_ref, ArcWake};
use futures::StreamExt;

use libc::{pollfd, POLLIN, POLLOUT};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum MarsyncError {
    #[error("io error")]
    Io(#[from] io::Error),

    #[error("cancelled")]
    Cancelled(#[from] Canceled),

    #[error("send error")]
    SendError(String),
}

/*
todo

pozbyć się futures

*/

/// An async UNIX socket.
/// Provides async read and write functionality.
#[derive(Debug)]
pub struct Socket {
    s: Arc<Mutex<UnixStream>>,
}

trait SocketTask {
    fn oneshot_workload(self: Box<Self>);
}

struct SocketCreate {
    path: String,
    tx: oneshot::Sender<Result<Socket, MarsyncError>>,
}

impl SocketTask for SocketCreate {
    fn oneshot_workload(self: Box<Self>) {
        let res = UnixStream::connect(self.path);
        match res {
            Ok(s) => {
                s.set_nonblocking(true).unwrap();
                let ret = Ok(Socket {
                    s: Arc::new(Mutex::new(s)),
                });
                self.tx.send(ret).expect("internal error")
            }
            Err(e) => self.tx.send(Err(e.into())).expect("internal error"),
        }
    }
}

struct SocketBlockingOp {
    s: Arc<Mutex<UnixStream>>,
    tx: oneshot::Sender<Option<MarsyncError>>,
    op: SocketOp,
}

impl SocketTask for SocketBlockingOp {
    fn oneshot_workload(self: Box<Self>) {
        let s = self.s.lock().expect("lock failed");
        match wait_for_socket_op(&s, self.op) {
            Some(e) => self.tx.send(Some(e.into())).expect("internal error"),
            None => self.tx.send(None).expect("internal error"),
        }
    }
}

struct SocketNextClient {
    l: Arc<Mutex<UnixListener>>,
    tx: futures::channel::mpsc::Sender<Result<Socket, MarsyncError>>,
}

impl SocketTask for SocketNextClient {
    fn oneshot_workload(mut self: Box<Self>) {
        loop {
            match self.l.lock().expect("lock failed").accept() {
                Ok((s, _)) => self.tx
                    .start_send(Ok(Socket {
                        s: Arc::new(Mutex::new(s)),
                    }))
                    .expect("internal error"),
                Err(e) => self.tx.start_send(Err(e.into())).expect("internal error"),
            }
        }
    }
}

fn socket_thread(
    socket_rx: Arc<
        std::sync::Mutex<
            std::sync::mpsc::Receiver<Box<(dyn SocketTask + std::marker::Send + Sync + 'static)>>,
        >,
    >,
) {
    loop {
        let socket_task = socket_rx.lock().expect("lock failed").recv().unwrap();
        socket_task.oneshot_workload();
    }
}

struct Task {
    future: Mutex<Option<Pin<Box<dyn Future<Output = ()> + Send>>>>,
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
    socket_tx: Arc<Mutex<mpsc::SyncSender<Box<dyn SocketTask + Send + Sync>>>>,
    socket_rx: Arc<Mutex<mpsc::Receiver<Box<dyn SocketTask + Send + Sync>>>>,
    task_tx: Arc<Mutex<mpsc::SyncSender<Arc<Task>>>>,
    task_rx: Arc<Mutex<mpsc::Receiver<Arc<Task>>>>,
}

fn new_marsync() -> Arc<Mutex<MarsyncContext>> {
    let (socket_tx, socket_rx) = mpsc::sync_channel(TASK_QUEUE_LEN);
    let (task_tx, task_rx) = mpsc::sync_channel(TASK_QUEUE_LEN);
    Arc::new(Mutex::new(MarsyncContext {
        socket_tx: Arc::new(Mutex::new(socket_tx)),
        socket_rx: Arc::new(Mutex::new(socket_rx)),
        task_rx: Arc::new(Mutex::new(task_rx)),
        task_tx: Arc::new(Mutex::new(task_tx)),
    }))
}

static CONTEXT: LazyLock<Arc<Mutex<MarsyncContext>>> = std::sync::LazyLock::new(new_marsync);
const SOCKET_THREAD_NUM: usize = 4;
const EXECUTOR_THREAD_NUM: usize = 4;
const TASK_QUEUE_LEN: usize = 16;

fn send_op_oneshot(
    s: Arc<Mutex<UnixStream>>,
    op: SocketOp,
) -> oneshot::Receiver<Option<MarsyncError>> {
    let marsync = CONTEXT.lock().expect("lock failed");
    let (tx, rx) = oneshot::channel();
    marsync
        .socket_tx
        .lock()
        .unwrap()
        .send(Box::new(SocketBlockingOp {
            s,
            tx,
            op,
        }))
        .unwrap();
    rx
}

impl Socket {
    /// Returns a future that on completion will have read data into the buffer buf.
    pub async fn read<'a>(&self, buf: &'a mut [u8]) -> Result<usize, MarsyncError> {
        let s = self.s.clone();
        {
            let mut socket = s.lock().expect("lock failed");
            match socket.read(buf) {
                Ok(n) => return Ok(n),
                Err(err) => {
                    if err.kind() != io::ErrorKind::WouldBlock {
                        return Err(err.into());
                    }
                }
            };
        }
        let rx = send_op_oneshot(self.s.clone(), SocketOp::Read);
        let ret = rx.await.expect("internal error");
        match ret {
            Some(err) => return Err(err),
            None => {}
        };

        let mut socket = s.lock().expect("lock failed");
        Ok(socket.read(buf)?)
    }
    /// Returns a future that on completion will have written data from the buffer buf.
    pub async fn write<'a>(&self, buf: &'a [u8]) -> Result<usize, MarsyncError> {
        let s = self.s.clone();
        {
            let mut socket = s.lock().expect("lock failed");
            match socket.write(buf) {
                Ok(n) => return Ok(n),
                Err(err) => {
                    if err.kind() != io::ErrorKind::WouldBlock {
                        return Err(err.into());
                    }
                }
            };
        }
        let rx = send_op_oneshot(self.s.clone(), SocketOp::Write);
        let ret = rx.await.expect("internal error");
        match ret {
            Some(err) => return Err(err),
            None => {}
        };

        let mut socket = s.lock().expect("lock failed");
        Ok(socket.write(buf)?)
    }
}

/// Spawns a new async thread. The thread is immediately added to the executor's queue.
pub fn spawn(future: impl Future<Output = ()> + 'static + Send) {
    let marsync = CONTEXT.lock().expect("lock failed");
    let t = Arc::new(Task {
        future: Mutex::new(Some(Box::pin(future))),
        task_tx: marsync.task_tx.clone(),
    });
    marsync
        .task_tx
        .lock()
        .expect("lock failed")
        .send(t)
        .unwrap();
}

/// Starts the reactor. This never returns. Make sure to spawn the entry point to the async app before running this.
pub fn run() {
    let marsync = CONTEXT.lock().expect("lock failed");
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

#[derive(PartialEq, Debug, Clone, Copy)]
enum SocketOp {
    Read,
    Write,
}

fn executor_thread(task_rx: Arc<Mutex<mpsc::Receiver<Arc<Task>>>>) {
    loop {
        let task = task_rx.lock().expect("lock failed").recv().unwrap();
        let waker = waker_ref(&task);
        let context = &mut Context::from_waker(&waker);
        let mut future_slot = task.future.lock().expect("lock failed");
        if let Some(mut f) = future_slot.take() {
            if f.as_mut().poll(context).is_pending() {
                *future_slot = Some(f);
            }
        }
    }
}

/// Returns a future that on completion will have connected to a UNIX socket.
pub fn connect(path: String) -> oneshot::Receiver<Result<Socket, MarsyncError>> {
    let marsync = CONTEXT.lock().expect("lock failed");
    let (tx, rx) = oneshot::channel();
    marsync
        .socket_tx
        .lock()
        .expect("lock failed")
        .send(Box::new(SocketCreate {
            path: path.to_owned(),
            tx,
        }))
        .expect("internal error");
    rx
}

fn wait_for_socket_op(stream: &UnixStream, op: SocketOp) -> Option<io::Error> {
    let events = match op {
        SocketOp::Read => POLLIN,
        SocketOp::Write => POLLOUT,
    };

    let mut fds = [pollfd {
        fd: stream.as_raw_fd(),
        events,
        revents: 0,
    }];
    loop {
        let result = unsafe { libc::poll(fds.as_mut_ptr(), 1, -1) };

        match result {
            -1 => {
                let err = io::Error::last_os_error();
                eprintln!("poll failed: {}", err);
                return Some(err);
            }
            0 => {
                println!("Timed out!");
                return None;
            }
            _ => match op {
                SocketOp::Read => {
                    if fds[0].revents & POLLIN != 0 {
                        return None;
                    } else {
                        continue;
                    }
                }
                SocketOp::Write => {
                    if fds[0].revents & POLLOUT != 0 {
                        return None;
                    } else {
                        continue;
                    }
                }
            },
        }
    }
}

/// A UNIX listener. Can be used to spawn new marsync::Sockets to communicate with clients.
pub struct Listener {
    rx: futures::channel::mpsc::Receiver<Result<Socket, MarsyncError>>,
}

impl Listener {
    /// Returns a stream that will yield marsync::Sockets for incoming clients.
    pub async fn next(&mut self) -> Result<Socket, MarsyncError> {
        self.rx.next().await.expect("internal error")
    }
}

/// Creates a new marsync::Listener by binding to a given path.
pub fn create_listener(path: &str) -> Listener {
    let listener = UnixListener::bind(path).unwrap();
    let l = Arc::new(Mutex::new(listener));
    let marsync = CONTEXT.lock().expect("lock failed");
    let (tx, rx) = futures::channel::mpsc::channel(TASK_QUEUE_LEN);
    marsync
        .socket_tx
        .lock()
        .unwrap()
        .send(Box::new(SocketNextClient { l: l.clone(), tx }))
        .unwrap();
    Listener { rx }
}
