use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::io::SeekFrom;

use serde::{Deserialize, Serialize};
use serde_json::{Result, Value};

use crate::{DBResult, Error, Storage};

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
enum LogEntry {
    /// key value
    Set {
        key: String,
        value: String,
    },
    Remove {
        key: String,
    },
}

/// Uses log based storage
pub struct LogStructured {
    data_dir: String,
    writer: File,
    reader: File,
    debug: File,
}

impl LogStructured {
    fn new(path: &std::path::Path) -> Self {
        let log_path = path.join("log.json");

        Self {
            data_dir: path.to_path_buf().into_os_string().into_string().unwrap(),
            writer: File::options()
                .append(true)
                .create(true)
                .open(log_path.clone())
                .expect("failed to open db writer"),
            reader: File::options()
                .read(true)
                .write(false)
                .open(log_path)
                .expect("failed to open db reader"),
            debug: File::options()
                .append(true)
                .read(false)
                .create(true)
                .open(path.join("debug.log"))
                .expect("failed to open db debug logger"),
        }
    }

    fn append(&mut self, entry: LogEntry) -> DBResult<usize> {
        let serialized =
            serde_json::to_string(&entry).map_err(|e| Error::Storage(e.to_string()))?;

        // let offset = self.writer.seek(SeekFrom::Current(0)).expect("failed to get current pointer") + serialized.len() ;
        match writeln!(&mut self.writer, "{}", serialized) {
            Ok(_) => Ok(serialized.len()),
            Err(_) => Err(Error::Storage("Failed to write to log".to_string())),
        }
    }

    fn replay(&mut self, key_to_find: &String) -> DBResult<Option<String>> {
        self.reader
            .seek(SeekFrom::Start(0))
            .map_err(|e| Error::Storage(e.to_string()))?;
        let mut result = None;
        for lineResult in io::BufReader::new(&self.reader).lines() {
            let line = lineResult.map_err(|e| Error::Storage(e.to_string()))?;
            match serde_json::from_str(&line).map_err(|e| Error::Storage(e.to_string()))? {
                LogEntry::Set { key, value } => {
                    if key_to_find == &key {
                        result = Some(value);
                    }
                }
                LogEntry::Remove { key } => {
                    if key_to_find == &key {
                        result = None;
                    }
                }
            }
        }

        Ok(result)
    }
}

impl Storage for LogStructured {
    type Key = String;
    type Value = String;

    fn get(&mut self, key: &Self::Key) -> Option<Self::Value> {
        match self.replay(key) {
            Err(e) => {
                match e {
                    Error::Storage(e) => {
                        writeln!(&mut self.debug, "get() error: {}", e)
                            .expect("writing to debug logger");
                    }
                }
                None
            }
            Ok(v) => v,
        }
    }

    fn set(&mut self, key: Self::Key, value: Self::Value) -> DBResult<()> {
        self.append(LogEntry::Set { key, value })?;
        Ok(())
    }

    fn remove(&mut self, key: &Self::Key) -> DBResult<()> {
        match self.get(key) {
            None => Err(Error::Storage("Key not found".to_string())),
            Some(_) => self
                .append(LogEntry::Remove { key: key.clone() })
                .map(|_| ()),
        }
    }

    fn open(path: &std::path::Path) -> DBResult<Self> {
        Ok(LogStructured::new(path))
    }
}
