use crate::error::Error;
use figlet_rs::FIGfont;

mod error;

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt::init();
    let standard_font = FIGfont::standard().unwrap();
    let figure = standard_font.convert("Iggy Node");
    println!("{}", figure.unwrap());
    Ok(())
}
