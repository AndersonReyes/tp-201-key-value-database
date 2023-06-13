use clap::{command, Parser, Subcommand};

use kvs::{InMemoryStorage, KvStore};

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

fn main() -> Result<(), clap::Error> {
    let store: KvStore<InMemoryStorage> = KvStore::new();
    let args = Cli::parse();

    match &args.operation {
        Some(Operation::Get { key: _ }) => {
            // let v = store.get(key.clone());
            eprintln!("unimplemented");
            std::process::exit(-1);
        }

        Some(Operation::Set { key: _, value: _ }) => {
            // let v = store.get(key.clone());
            eprintln!("unimplemented");
            std::process::exit(-1);
        }

        Some(Operation::Remove { key: _ }) => {
            // let v = store.get(key.clone());
            eprintln!("unimplemented");
            std::process::exit(-1);
        }
        _ => {
            eprintln!("Must provide an argument");
            std::process::exit(-1);
        }
    }

    Ok(())
}
