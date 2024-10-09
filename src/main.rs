use futures::StreamExt;

pub mod marsync;

const BUF_SIZE: usize = 32;

async fn run_listener(mut l: marsync::Listener) {
    println!("running listener");
    loop {
        let r = l.next().await;
        match r {
            Ok(s) => {
                println!("got socket");
                loop {
                    let mut buf = [0; BUF_SIZE];
                    let n = s.read(&mut buf).await.unwrap();
                    println!("server received {}", n);
                    if n == 0 {
                        break;
                    }
                    let data = String::from_utf8(buf.to_vec())
                        .unwrap()
                        .trim_matches('\0')
                        .to_string();
                    let ret = format!("received: {}", data);
                    let m = s.write(ret.as_bytes()).await.unwrap();
                    println!("server sent {}", m);
                }
            }
            Err(e) => println!("next err {}", e),
        }
    }
}

async fn async_main() {
    let _ = std::fs::remove_file("/tmp/test.sock");
    let l = marsync::create_listener("/tmp/test.sock");
    marsync::spawn(run_listener(l));
    loop {
        let s = marsync::connect(String::from("/tmp/test.sock"))
            .await
            .unwrap();
        println!("connect");
        match s {
            Ok(ref s) => {
                let test = String::from("siemka tasiemka ").repeat(BUF_SIZE / 16 - 1);
                let n = s.write(test.as_bytes()).await.unwrap();
                println!("client sent {}", n);
                let mut buf = [0; BUF_SIZE];
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
