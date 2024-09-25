use once_cell::sync::Lazy;
use std::borrow::BorrowMut;
use std::cell::Cell;
use std::collections::VecDeque;
use std::error::Error;
use std::future::Future;
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::pin::Pin;
use std::sync::{mpsc, Arc, LazyLock, Mutex};
use std::task::{Context, Poll, Waker};
use thiserror::Error;

use futures::{
    future::{BoxFuture, FutureExt},
    task::{waker_ref, ArcWake},
};

pub struct Socket {
    s: UnixStream,
}

enum SocketTask {
    Create(
        String,
        Arc<Mutex<Option<Result<Socket, String>>>>,
        Arc<Mutex<Option<Waker>>>,
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
    pub fn new(path: String) -> Self {
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

fn socket_thread(socket_rx: mpsc::Receiver<Arc<SocketTask>>) {
    loop {
        let socket_task = socket_rx.recv().unwrap();
        match socket_task.as_ref() {
            SocketTask::Create(path, task_ret, waker) => {
                let res = UnixStream::connect(path);
                let mut t = task_ret.lock().unwrap();
                match res {
                    Ok(s) => {
                        *t = Some(Ok(Socket { s: s }));
                    }
                    Err(e) => *t = Some(Err(e.to_string())),
                }
                if let Some(w) = waker.lock().unwrap().take() {
                    w.wake();
                }
            }
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
