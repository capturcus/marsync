use oneshot::Sender;

use super::*;

pub trait SocketTask {
    fn oneshot_workload(self: Box<Self>);
}

pub struct SocketCreate {
    pub path: String,
    pub tx: Sender<Result<Socket, MarsyncError>>,
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
                self.tx.send(ret)
            }
            Err(e) => self.tx.send(Err(e.into())),
        }
    }
}

pub struct SocketBlockingOp {
    pub s: Arc<Mutex<UnixStream>>,
    pub tx: Sender<Option<MarsyncError>>,
    pub op: SocketOp,
}

impl SocketTask for SocketBlockingOp {
    fn oneshot_workload(self: Box<Self>) {
        let s = self.s.lock().expect("lock failed");
        match wait_for_socket_op(&s, self.op) {
            Some(e) => self.tx.send(Some(e.into())),
            None => self.tx.send(None),
        }
    }
}

pub struct SocketNextClient {
    pub l: Arc<Mutex<UnixListener>>,
    pub tx: futures::channel::mpsc::Sender<Result<Socket, MarsyncError>>,
}

impl SocketTask for SocketNextClient {
    fn oneshot_workload(mut self: Box<Self>) {
        loop {
            match self.l.lock().expect("lock failed").accept() {
                Ok((s, _)) => self
                    .tx
                    .start_send(Ok(Socket {
                        s: Arc::new(Mutex::new(s)),
                    }))
                    .expect("internal error"),
                Err(e) => self.tx.start_send(Err(e.into())).expect("internal error"),
            }
        }
    }
}

pub fn socket_thread(
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
