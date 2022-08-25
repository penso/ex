pub mod entities;
mod parser;

use std::env;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = env::args().collect::<Vec<String>>();
    if args.len() != 2 {
        panic!("Call with filename input")
    }
    let file_name: &String = args.last().unwrap();

    eprintln!("Parsing {}", file_name);
    parser::parse_data(file_name.as_str()).await?;
    Ok(())
}
