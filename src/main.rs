
pub mod marsync;

async fn async_main(m: &marsync::Marsync) {
    println!("hello");
    let s = m.create_socket(String::from("siemka")).await;
    match s {
        Ok(s) => {
            println!("socket ok")
        }
        Err(e) => println!("socket err {}", e),
    }
}

fn main() {
    let m = marsync::Marsync::new();
    m.spawn(async_main(&m));
    m.run();
}
