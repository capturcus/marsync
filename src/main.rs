#![feature(unix_socket_peek)]

use std::{
    io::{self, Read},
    os::unix::net::UnixStream,
    time::Duration,
};
pub mod marsync;

async fn async_main() {
    let s = marsync::create_socket(String::from("/tmp/test.sock")).await;
    loop {
        match s {
            Ok(ref s) => {
                println!("got socket");
                let mut buf = [0; 32];
                let n = s.read(&mut buf).await.unwrap();
                println!("n {}", n);
                println!("data {:#?}", buf);
            }
            Err(ref e) => println!("socket err {}", e),
        }
    }
}

fn main() {
    marsync::spawn(async_main());
    marsync::run();
}
