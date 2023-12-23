use clap::{command, Parser, Subcommand};

use kvs::{DBResult, Error, KvStore, LogStructured};

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    operation: Option<Operation>,
}

#[derive(Subcommand, Debug)]
enum Operation {
    Get {
        key: String,
    },
    Set {
        key: String,
        value: String,
    },
    #[command(name = "rm")]
    Remove {
        key: String,
    },
}

fn main() -> DBResult<()> {
    let mut store: KvStore<LogStructured> = KvStore::open(&*std::env::current_dir().unwrap())?;
    let args = Cli::parse();

    match &args.operation {
        Some(Operation::Get { key }) => {
            let g = store.get(key);

            match g {
                Some(v) => println!("{}", v),
                None => println!("Key not found"),
            }
        }

        Some(Operation::Set { key, value }) => {
            store.set(key.to_string(), value.to_string())?;
        }

        Some(Operation::Remove { key }) => {
            match store.remove(key) {
                Ok(_) => {}
                Err(e) => match e {
                    Error::Storage(err) => {
                        println!("{}", err);
                        std::process::exit(-1);
                    }
                },
            };
        }
        _ => {
            eprintln!("Must provide an argument");
            std::process::exit(-1);
        }
    }

    Ok(())
}
