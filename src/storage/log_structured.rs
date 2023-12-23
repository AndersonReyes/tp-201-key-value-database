use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::path::Path;
use uuid::Uuid;

use serde::{Deserialize, Serialize};
use serde_json::Result;

use crate::{DBResult, Error, Storage};

// TODO: implement more compact serialization? Right now we use ndjson but maybe raw bytes is better

#[derive(Debug)]
struct LogPointer {
    offset: u64,
    size: usize,
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
    base_path: String,
    main_log: String,
    writer: File,
    reader: File,
    index: HashMap<String, LogPointer>,
}

impl LogStructured {
    fn new(path: &Path) -> Self {
        let log_path = path.join("log.json");

        Self {
            base_path: path.display().to_string(),
            main_log: log_path.display().to_string(),
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
            index: HashMap::new(),
        }
    }

    /// Adds entry to log and returns entry offset in the log
    fn append(&mut self, entry: LogEntry) -> DBResult<()> {
        let log_pointer = LogStructured::append_to_writer(&mut self.writer, &entry)?;
        match entry {
            LogEntry::Set { key, .. } => self.index.insert(key, log_pointer),
            LogEntry::Remove { key, .. } => self.index.insert(key, log_pointer),
        };
        Ok(())
    }

    /// Append the new log entry to writer and return the log pointer
    fn append_to_writer(writer: &mut File, entry: &LogEntry) -> DBResult<LogPointer> {
        let serialized = serde_json::to_string(entry).map_err(|e| Error::Storage(e.to_string()))?;

        writeln!(writer, "{}", serialized)
            .map_err(|_| Error::Storage("Failed to write to log".to_string()))?;

        let curr_offset = writer
            .seek(SeekFrom::Current(0))
            .map_err(|e| Error::Storage(e.to_string()))?;

        let pointer: LogPointer = LogPointer {
            offset: curr_offset - (serialized.len() as u64) - 1,
            size: serialized.len(),
        };

        Ok(pointer)
    }

    /// runs compaction.
    /// Steps:
    /// 1. get all the file offsets we need from the index
    /// 2. Copy those entries to a temp file
    /// 3. rotate temp file as main log file
    /// TODO: can compaction be done in place?
    fn compaction(&mut self) -> DBResult<()> {
        // don't compact until 10kb or 10k byte size
        if self.reader.metadata().unwrap().len() <= 40_000 {
            return Ok(());
        }

        let temp_path = format!("{}/log-{}.json", self.base_path, Uuid::new_v4().to_string());
        let mut new_writer = File::options()
            .append(true)
            .create(true)
            .open(Path::new(&temp_path))
            .expect("failed to open db writer");

        let keys: Vec<String> = self.index.keys().cloned().collect();
        for key in keys {
            let entry = self
                .find(&key)
                .map_err(|e| Error::Storage(e.to_string()))?
                .expect("[COMPACTION] expected valid log pointer");

            let log_pointer = LogStructured::append_to_writer(&mut new_writer, &entry)?;
            // TODO: updated self.index here could be bad if there is an error, new log point to
            // failed file
            match entry {
                LogEntry::Set { key, .. } => self.index.insert(key, log_pointer),
                LogEntry::Remove { key, .. } => self.index.insert(key, log_pointer),
            };
        }

        std::fs::copy(&temp_path, &self.main_log).expect("[COMPACTION] Failed to rotate log");
        std::fs::remove_file(temp_path).expect("[COMPACTION] failed to remove temp file");

        Ok(())
    }

    /// looks for entry in the index and then reads from disk if not exist
    fn find(&self, key_to_find: &str) -> Result<Option<LogEntry>> {
        let result = self.index.get(key_to_find).and_then(|pointer| {
            let mut reader = io::BufReader::new(&self.reader);
            reader.seek(SeekFrom::Start((*pointer).offset)).unwrap();
            let mut line = String::new();
            // TODO: maybe use the LogPointer::size here?
            reader.read_line(&mut line).unwrap();
            line = line.trim_end_matches('\n').to_string();

            serde_json::from_str(&line).expect("[FIND] Failed to parse LogEntry")
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
            let line = line_result.expect("[HYDRATE] Failed to read line");
            let size = line.len();
            match serde_json::from_str(&line).expect("[HYDRATE] Failed to parse line") {
                LogEntry::Set { key, .. } => self.index.insert(key, LogPointer { offset, size }),
                LogEntry::Remove { key } => self.index.remove(&key),
            };
            offset += size as u64 + 1; // + 1 is for newline char
        }

        Ok(())
    }
}

impl Storage for LogStructured {
    fn get(&self, key: &str) -> Option<String> {
        let string: Option<String> = self
            .find(key)
            .expect("[LOG_STRUCTURED.GET] Error looking for key")
            .and_then(|e| match e {
                LogEntry::Set { value, .. } => Some(value),
                LogEntry::Remove { .. } => None,
            });

        string
    }

    fn set(&mut self, key: String, value: String) -> DBResult<()> {
        self.append(LogEntry::Set { key, value })?;
        self.compaction()?;
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
        storage
            .hydrate()
            .expect("[LOG_STRUCTURED.OPEN] Failed to hydrate db");
        Ok(storage)
    }
}
