use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

struct Shared<T> {
    value: Option<T>,
    waker: Option<Waker>,
}

pub struct Sender<T> {
    shared: Arc<Mutex<Shared<T>>>,
}

pub struct Receiver<T> {
    shared: Arc<Mutex<Shared<T>>>,
}

impl<T> Future for Receiver<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> std::task::Poll<Self::Output> {
        let mut shared = self.shared.lock().expect("lock failed");
        if let Some(v) = shared.value.take() {
            return Poll::Ready(v);
        }
        shared.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl<T> Sender<T> {
    pub fn send(self, t: T) {
        let mut shared = self.shared.lock().expect("lock failed");
        shared.value = Some(t);
        if let Some(w) = shared.waker.take() {
            w.wake();
        }
    }
}

pub fn new<T>() -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Mutex::new(Shared {
        value: None,
        waker: None,
    }));
    (
        Sender {
            shared: shared.clone(),
        },
        Receiver { shared },
    )
}
