use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::io::SeekFrom;

use serde::{Deserialize, Serialize};
use serde_json::Result;

use crate::{DBResult, Error, Storage};

// TODO: implement more compact serialization? Right now we use ndjson

struct LogPointer {
    offset: u64,
}

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
    // data_dir: String,
    writer: File,
    reader: File,
    // debug: File,
    index: HashMap<String, LogPointer>,
}

impl LogStructured {
    fn new(path: &std::path::Path) -> Self {
        let log_path = path.join("log.json");

        Self {
            // data_dir: path.to_path_buf().into_os_string().into_string().unwrap(),
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
            // debug: File::options()
            //     .append(true)
            //     .read(false)
            //     .create(true)
            //     .open(path.join("debug.log"))
            //     .expect("failed to open db debug logger"),
            index: HashMap::new(),
        }
    }

    /// Adds entry to log and returns entry offset in the log
    fn append(&mut self, entry: LogEntry) -> DBResult<()> {
        let serialized =
            serde_json::to_string(&entry).map_err(|e| Error::Storage(e.to_string()))?;

        writeln!(&mut self.writer, "{}", serialized)
            .map_err(|_| Error::Storage("Failed to write to log".to_string()))?;

        let curr_offset = self
            .writer
            .seek(SeekFrom::Current(0))
            .map_err(|e| Error::Storage(e.to_string()))?;

        let pointer: LogPointer = LogPointer {
            offset: curr_offset - (serialized.len() as u64) - 1,
        };
        match entry {
            LogEntry::Set { key, .. } => self.index.insert(key, pointer),
            LogEntry::Remove { key, .. } => self.index.insert(key, pointer),
        };

        Ok(())
    }

    /// looks for entry in the index and then reads from disk if not exist
    fn find(&mut self, key_to_find: &str) -> Result<Option<LogEntry>> {
        let result = self.index.get(key_to_find).and_then(|pointer| {
            let mut reader = io::BufReader::new(&self.reader);
            reader.seek(SeekFrom::Start((*pointer).offset)).unwrap();
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            line = line.trim_end_matches('\n').to_string();
            
            serde_json::from_str(&line).expect("Failed to parse LogEntry")
        });

        Ok(result)
    }

    /// populates local index from the log
    fn hydrate(&mut self) -> Result<()> {
        self.reader
            .seek(SeekFrom::Start(0))
            .map_err(|e| Error::Storage(e.to_string()))
            .expect("Failed to seek");

        let mut offset: u64 = 0;
        for line_result in io::BufReader::new(&self.reader).lines() {
            let line = line_result.expect("Failed to read line");
            match serde_json::from_str(&line).expect("Failed to parse line") {
                LogEntry::Set { key, .. } => self.index.insert(key, LogPointer { offset }),
                LogEntry::Remove { key } => self.index.remove(&key),
            };
            offset += line.len() as u64 + 1; // + 1 is for newline char
        }

        Ok(())
    }
}

impl Storage for LogStructured {
    fn get(&mut self, key: &str) -> Option<String> {
        let string: Option<String> = self
            .find(key)
            // TODO: this expect should happen if the log entry is remove type???
            .expect("Error looking for key")
            .and_then(|e| match e {
                LogEntry::Set { value, .. } => Some(value),
                LogEntry::Remove { .. } => None,
            });

        string
    }

    fn set(&mut self, key: String, value: String) -> DBResult<()> {
        self.append(LogEntry::Set { key, value })?;
        Ok(())
    }

    fn remove(&mut self, key: &str) -> DBResult<()> {
        match self.get(key) {
            None => Err(Error::Storage("Key not found".to_string())),
            Some(_) => self
                .append(LogEntry::Remove {
                    key: key.to_string(),
                })
                .map(|_| ()),
        }
    }

    fn open(path: &std::path::Path) -> DBResult<Self> {
        let mut storage = LogStructured::new(path);
        storage.hydrate().expect("Failed to hydrate db");
        Ok(storage)
    }
}
