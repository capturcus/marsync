
pub mod marsync;

async fn async_main() {
    let s = marsync::CreateSocket::new(String::from("siemka")).await;
    match s {
        Ok(_s) => {
            println!("socket ok")
        }
        Err(e) => println!("socket err {}", e),
    }
}

fn main() {
    marsync::spawn(async_main());
    marsync::run();
}
