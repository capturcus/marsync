#![feature(unix_socket_peek)]

use std::time::Duration;

use futures::StreamExt;

pub mod marsync;

async fn run_listener(l: marsync::Listener) {
    println!("running listener");
    l.next()
        .then(|r| async move {
            match r {
                Ok(s) => loop {
                    let mut buf = [0; 32];
                    let n = s.read(&mut buf).await.unwrap();
                    println!("server received {}", n);
                    if n == 0 {
                        break;
                    }
                    let data = String::from_utf8(buf.to_vec()).unwrap().trim_matches('\0').to_string();
                    let ret = format!("received: {}", data);
                    let m = s.write(ret.as_bytes()).await.unwrap();
                    println!("server sent {}", m);
                },
                Err(e) => println!("next err {}", e.to_string()),
            }
        })
        .collect::<Vec<_>>()
        .await;
}

async fn async_main() {
    let _ = std::fs::remove_file("/tmp/test.sock");
    let l = marsync::create_listener(String::from("/tmp/test.sock"));
    marsync::spawn(run_listener(l));
    let s = marsync::connect(String::from("/tmp/test.sock")).await;
    println!("connect");
    loop {
        match s {
            Ok(ref s) => {
                let test = String::from("siemka tasiemka");
                let n = s.write(test.as_bytes()).await.unwrap();
                println!("client sent {}", n);
                let mut buf = [0; 32];
                let m = s.read(&mut buf).await.unwrap();
                println!("client received {}", m);
                let ret = String::from_utf8(Vec::from(buf)).unwrap();
                println!("client: {}", ret);
            }
            Err(ref e) => {
                println!("socket err {}", e);
                return;
            }
        }
    }
}

fn main() {
    marsync::spawn(async_main());
    marsync::run();
}
