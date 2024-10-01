use std::future::Future;
use std::io::{self, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::net::{UnixListener, UnixStream};
use std::pin::Pin;
use std::sync::{mpsc, Arc, LazyLock, Mutex};
use std::task::{Context, Poll};

use futures::channel::oneshot::{self, Canceled};
use futures::Stream;
use futures::{
    future::{BoxFuture, FutureExt},
    task::{waker_ref, ArcWake},
};

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

/// An async UNIX socket.
/// Provides async read and write functionality.
#[derive(Debug)]
pub struct Socket {
    s: Arc<Mutex<UnixStream>>,
}

enum SocketTask {
    Connect {
        path: String,
        tx: Option<oneshot::Sender<Result<Socket, MarsyncError>>>,
    },
    SocketBlockingOp {
        s: Arc<Mutex<UnixStream>>,
        tx: Option<oneshot::Sender<Option<MarsyncError>>>,
        op: SocketOp,
    },
    NextClient {
        l: Arc<Mutex<UnixListener>>,
        tx: futures::channel::mpsc::Sender<Result<Socket, MarsyncError>>,
    },
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
    socket_tx: Arc<Mutex<mpsc::SyncSender<SocketTask>>>,
    socket_rx: Arc<Mutex<mpsc::Receiver<SocketTask>>>,
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

/// A future that implements UNIX socket connection.
pub struct ConnectSocketFuture {
    rx: oneshot::Receiver<Result<Socket, MarsyncError>>,
}

impl Future for ConnectSocketFuture {
    type Output = Result<Socket, MarsyncError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        Pin::new(&mut self.rx).poll(cx)?
    }
}

impl ConnectSocketFuture {
    fn new(path: impl ToOwned<Owned = String>) -> Result<Self, MarsyncError> {
        let marsync = CONTEXT.lock().expect("lock failed");
        let (tx, rx) = oneshot::channel();
        let r = marsync.socket_tx.lock().expect("lock failed").send(SocketTask::Connect {
            path: path.to_owned(),
            tx: Some(tx),
        });
        match r {
            Ok(_) => Ok(ConnectSocketFuture { rx }),
            Err(e) => Err(MarsyncError::SendError(e.to_string())),
        }
    }
}

impl Socket {
    /// Returns a future that on completion will have read data into the buffer buf.
    pub fn read<'a>(&self, buf: &'a mut [u8]) -> BlockingSocketOp<'a> {
        BlockingSocketOp {
            data: SocketOpData::ReadData(Some(buf)),
            s: self.s.clone(),
            rx: None,
        }
    }
    /// Returns a future that on completion will have written data from the buffer buf.
    pub fn write<'a>(&self, buf: &'a [u8]) -> BlockingSocketOp<'a> {
        BlockingSocketOp {
            data: SocketOpData::WriteData(buf),
            s: self.s.clone(),
            rx: None,
        }
    }
}

enum SocketOpData<'a> {
    ReadData(Option<&'a mut [u8]>),
    WriteData(&'a [u8]),
}

impl SocketOpData<'_> {
    fn socket_op(&self) -> SocketOp {
        match self {
            SocketOpData::ReadData(_) => SocketOp::Read,
            SocketOpData::WriteData(_) => SocketOp::Write,
        }
    }
}

/// A future that on completion will have performed a blocking UNIX socket operation. This could be a read or a write.
pub struct BlockingSocketOp<'a> {
    data: SocketOpData<'a>,
    s: Arc<Mutex<UnixStream>>,
    rx: Option<oneshot::Receiver<Option<MarsyncError>>>,
}

impl<'a> Future for BlockingSocketOp<'a> {
    type Output = Result<usize, MarsyncError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let other_s = self.s.clone();
        let ret = match &mut self.data {
            SocketOpData::ReadData(op_data) => {
                let buf = op_data.take().unwrap();
                let r = other_s.lock().expect("lock failed").read(buf);
                *op_data = Some(buf);
                r
            }
            SocketOpData::WriteData(data) => other_s.lock().expect("lock failed").write(data),
        };
        match ret {
            Ok(bytes) => {
                self.rx = None;
                Poll::Ready(Ok(bytes))
            }
            Err(err) => {
                if err.kind() == io::ErrorKind::WouldBlock {
                    let marsync = CONTEXT.lock().expect("lock failed");
                    let (tx, mut rx) = oneshot::channel();
                    marsync
                        .socket_tx
                        .lock()
                        .unwrap()
                        .send(SocketTask::SocketBlockingOp {
                            s: self.s.clone(),
                            tx: Some(tx),
                            op: self.data.socket_op(),
                        })
                        .unwrap();
                    let r = Pin::new(&mut rx).poll(cx)?;
                    self.rx = Some(rx);
                    match r {
                        Poll::Ready(op) => match op {
                            Some(e) => return Poll::Ready(Err(e.into())),
                            None => {
                                cx.waker().clone().wake();
                                return Poll::Pending;
                            }
                        },
                        Poll::Pending => {
                            return Poll::Pending;
                        }
                    };
                } else {
                    Poll::Ready(Err(err.into()))
                }
            }
        }
    }
}

/// Spawns a new async thread. The thread is immediately added to the executor's queue.
pub fn spawn(future: impl Future<Output = ()> + 'static + Send) {
    let marsync = CONTEXT.lock().expect("lock failed");
    let t = Arc::new(Task {
        future: Mutex::new(Some(future.boxed())),
        task_tx: marsync.task_tx.clone(),
    });
    marsync.task_tx.lock().expect("lock failed").send(t).unwrap();
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
        handles.push(std::thread::spawn(move || socket_thread(&new_socket_rx)));
    }
    for _ in 0..EXECUTOR_THREAD_NUM {
        let new_task_rx = task_rx.clone();
        handles.push(std::thread::spawn(|| executor_thread(new_task_rx)));
    }
    for h in handles {
        h.join().unwrap();
    }
}

fn socket_thread_connect(path: &str, tx: oneshot::Sender<Result<Socket, MarsyncError>>) {
    let res = UnixStream::connect(path);
    match res {
        Ok(s) => {
            s.set_nonblocking(true).unwrap();
            let ret = Ok(Socket {
                s: Arc::new(Mutex::new(s)),
            });
            tx.send(ret).expect("internal error")
        }
        Err(e) => tx.send(Err(e.into())).expect("internal error"),
    }
}

#[derive(PartialEq, Debug, Clone, Copy)]
enum SocketOp {
    Read,
    Write,
}

fn socket_thread_wait_for_op(
    s_arc: &Mutex<UnixStream>,
    tx: oneshot::Sender<Option<MarsyncError>>,
    op: SocketOp,
) {
    let s = s_arc.lock().expect("lock failed");
    match wait_for_socket_op(&s, op) {
        Some(e) => tx.send(Some(e.into())).expect("internal error"),
        None => tx.send(None).expect("internal error"),
    }
}

fn socket_thread_next_client(
    listener: &Mutex<UnixListener>,
    tx: &mut futures::channel::mpsc::Sender<Result<Socket, MarsyncError>>,
) {
    loop {
        match listener.lock().expect("lock failed").accept() {
            Ok((s, _)) => tx
                .start_send(Ok(Socket {
                    s: Arc::new(Mutex::new(s)),
                }))
                .expect("internal error"),
            Err(e) => tx.start_send(Err(e.into())).expect("internal error"),
        }
    }
}

fn socket_thread(socket_rx: &Mutex<mpsc::Receiver<SocketTask>>) {
    loop {
        let socket_task = socket_rx.lock().expect("lock failed").recv().unwrap();
        match socket_task {
            SocketTask::Connect { path, tx } => {
                socket_thread_connect(&path, tx.expect("internal error"))
            }
            SocketTask::SocketBlockingOp { s, tx, op } => {
                socket_thread_wait_for_op(&s, tx.expect("internal error"), op)
            }
            SocketTask::NextClient { l, mut tx } => socket_thread_next_client(&l, &mut tx),
        }
    }
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
pub fn connect(path: String) -> Result<ConnectSocketFuture, MarsyncError> {
    ConnectSocketFuture::new(path)
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

/// A future that on completion will contain a new client that was created by listening using a UNIX listener.
pub struct NextClientFuture {
    rx: futures::channel::mpsc::Receiver<Result<Socket, MarsyncError>>,
}

impl Stream for NextClientFuture {
    type Item = Result<Socket, MarsyncError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.rx).poll_next(cx)
    }
}

/// A UNIX listener. Can be used to spawn new marsync::Sockets to communicate with clients.
pub struct Listener {
    l: Arc<Mutex<UnixListener>>,
}

impl Listener {
    /// Returns a stream that will yield marsync::Sockets for incoming clients.
    pub fn next(&self) -> NextClientFuture {
        let marsync = CONTEXT.lock().expect("lock failed");
        let (tx, rx) = futures::channel::mpsc::channel(TASK_QUEUE_LEN);
        marsync
            .socket_tx
            .lock()
            .unwrap()
            .send(SocketTask::NextClient {
                l: self.l.clone(),
                tx,
            })
            .unwrap();
        NextClientFuture { rx }
    }
}

/// Creates a new marsync::Listener by binding to a given path.
pub fn create_listener(path: &str) -> Listener {
    let l = UnixListener::bind(path).unwrap();
    Listener {
        l: Arc::new(Mutex::new(l)),
    }
}
