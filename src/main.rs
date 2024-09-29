#![feature(unix_socket_peek)]

pub mod marsync;

async fn async_main() {
    let s = marsync::connect(String::from("/tmp/test.sock")).await;
    println!("got socket");
    loop {
        match s {
            Ok(ref s) => {
                let mut buf = [0; 32];
                let n = s.read(&mut buf).await.unwrap();
                if n == 0 {
                    return;
                }
                let data = String::from_utf8(buf.to_vec()).unwrap();
                println!("received: {}", data);
                buf[0] = b'a';
                let o = s.write(&buf).await.unwrap();
                println!("sent: {}", o);
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
