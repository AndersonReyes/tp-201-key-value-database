use clap::{command, Parser, Subcommand};

use kvs::{DBResult, InMemoryStorage, KvStore};

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
    let mut store: KvStore<InMemoryStorage> = KvStore::new();
    let args = Cli::parse();

    match &args.operation {
        Some(Operation::Get { key }) => {
            let g = store.get(key.to_string());
            g.map(|v| println!("{}", v));
        }

        Some(Operation::Set { key, value }) => {
            store.set(key.to_string(), value.to_string())?;
        }

        Some(Operation::Remove { key }) => {
            store.remove(key.to_string())?;
        }
        _ => {
            eprintln!("Must provide an argument");
            std::process::exit(-1);
        }
    }

    Ok(())
}
