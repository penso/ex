pub mod entities;
mod generator;
mod parser;

use clap::Parser;

#[derive(Parser, Debug)]
#[clap(author, version)]
#[clap(about = "CSV transactions parser")]
struct Args {
    #[clap(short, long, value_parser)]
    generate_data: Option<bool>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Easy way to generate random data. I'd have used different binaries but exercise just
    // want to run `cargo run -- filename` for parsing
    if args.generate_data.is_some() && args.generate_data.unwrap() {
        generator::generate_data("data.csv");
        return Ok(());
    }

    parser::parse_data("data.csv", "output.csv").await?;
    Ok(())
}
