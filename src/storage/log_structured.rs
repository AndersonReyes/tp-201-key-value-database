use crate::storage;
use std::collections::HashMap;
use std::fs;
use std::fs::DirEntry;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::io::SeekFrom;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context};
use serde::{Deserialize, Serialize};

use crate::Storage;

/// TODO: clean up the map to custom errors by implementing the From trait

const COMPACTION_SIZE_TRIGGER_KB: u64 = 1_000 * 40;

#[derive(Debug)]
struct LogPointer {
    offset: u64,
    size: usize,
    path: String,
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
    log_dir: String,
    main_log: String,
    index: HashMap<String, LogPointer>,
}

impl LogStructured {
    fn new(path: &Path) -> Self {
        let log_dir = path.join("log-files");
        if !log_dir.exists() {
            fs::create_dir(&log_dir).expect("failed to create log dir");
        }
        let log_path = log_dir.join(format!(
            "{}.json",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ));

        Self {
            log_dir: log_dir.display().to_string(),
            main_log: log_path.display().to_string(),
            index: HashMap::new(),
        }
    }

    fn read_file(&self, path: &Path) -> anyhow::Result<File> {
        File::options()
            .read(true)
            .write(false)
            .open(path)
            .with_context(|| format!("failed to read: {:?}", path))
    }

    fn write_append_file(&self, path: &Path) -> anyhow::Result<File> {
        File::options()
            .create(true)
            .read(false)
            .write(true)
            .append(true)
            .open(path)
            .with_context(|| format!("failed to write: {:?}", path))
    }

    /// Adds entry to log and returns entry offset in the log
    fn append(&mut self, entry: LogEntry) -> anyhow::Result<()> {
        let serialized = serde_json::to_string(&entry)
            .with_context(|| format!("failed to serialize {:?}", &entry))?;
        let mut writer = self.write_append_file(&Path::new(&self.main_log))?;

        writeln!(writer, "{}", serialized)?;

        let curr_offset = writer.seek(SeekFrom::Current(0))?;

        let log_pointer: LogPointer = LogPointer {
            offset: curr_offset - (serialized.len() as u64) - 1,
            size: serialized.len(),
            path: self.main_log.to_string(),
        };

        match entry {
            LogEntry::Set { key, .. } => self.index.insert(key, log_pointer),
            LogEntry::Remove { key, .. } => self.index.insert(key, log_pointer),
        };

        Ok(())
    }

    /// runs compaction.
    /// Steps:
    /// 1. get all the file offsets we need from the index
    /// 2. Copy those entries to a temp file
    /// 3. rotate temp file as main log file
    fn compaction(&mut self) -> anyhow::Result<()> {
        // // don't compact until 10kb or 10k byte size
        // if self.reader.metadata().unwrap().len() <= COMPACTION_SIZE_TRIGGER_KB {
        //     return Ok(());
        // }
        //
        // let temp_path = format!("{}/log-{}.json", self.log_dir, Uuid::new_v4().to_string());
        // let mut new_writer = File::options()
        //     .append(true)
        //     .create(true)
        //     .open(Path::new(&temp_path))
        //     .expect("failed to open db writer");
        //
        // let keys: Vec<String> = self.index.keys().cloned().collect();
        // for key in keys {
        //     let entry = self
        //         .find(&key)
        //         .map_err(|e| Error::Storage(e.to_string()))?
        //         .expect("[COMPACTION] expected valid log pointer");
        //
        //     let log_pointer = self.append_to_writer(&entry)?;
        //     match entry {
        //         LogEntry::Set { key, .. } => self.index.insert(key, log_pointer),
        //         LogEntry::Remove { key, .. } => self.index.insert(key, log_pointer),
        //     };
        // }
        //
        // std::fs::copy(&temp_path, &self.main_log).expect("[COMPACTION] Failed to rotate log");
        // std::fs::remove_file(temp_path).expect("[COMPACTION] failed to remove temp file");
        //
        Ok(())
    }

    /// looks for entry in the index and then reads from disk if not exist
    fn find(&self, key_to_find: &str) -> Option<LogEntry> {
        let result = self.index.get(key_to_find).and_then(|pointer| {
            let file = self.read_file(&Path::new(&pointer.path)).ok()?;

            let mut reader = BufReader::new(file);
            reader.seek(SeekFrom::Start((*pointer).offset)).ok()?;
            let mut line = String::new();
            // TODO: maybe use the LogPointer::size here?
            reader.read_line(&mut line).ok()?;
            line = line.trim_end_matches('\n').to_string();

            serde_json::from_str(&line).ok()?
        });

        result
    }

    /// populates local index from the log
    fn hydrate(&mut self) -> anyhow::Result<()> {
        let mut logs: Vec<DirEntry> = fs::read_dir(&Path::new(&self.log_dir))?
            .map(|e| e.expect("[HYDRATE] failed unwrap DirEntry"))
            .filter(|e| !e.path().is_dir())
            .collect();
        logs.sort_by_key(|e| e.path());

        for path in logs {
            let path = path.path();
            let reader = self.read_file(&path)?;

            let mut offset: u64 = 0;
            for line_result in BufReader::new(&reader).lines() {
                let line = line_result?;
                let size = line.len();
                match serde_json::from_str(&line).expect("[HYDRATE] Failed to parse line") {
                    LogEntry::Set { key, .. } => self.index.insert(
                        key,
                        LogPointer {
                            offset,
                            size,
                            path: path.display().to_string(),
                        },
                    ),
                    LogEntry::Remove { key } => self.index.remove(&key),
                };
                offset += size as u64 + 1; // + 1 is for newline char
            }
        }
        Ok(())
    }
}

impl Storage for LogStructured {
    fn get(&self, key: &str) -> Option<String> {
        let string: Option<String> = self.find(key).and_then(|e| match e {
            LogEntry::Set { value, .. } => Some(value),
            LogEntry::Remove { .. } => None,
        });

        string
    }

    fn set(&mut self, key: String, value: String) -> anyhow::Result<()> {
        self.append(LogEntry::Set { key, value })?;
        // self.compaction()?;
        Ok(())
    }

    fn remove(&mut self, key: &str) -> anyhow::Result<()> {
        match self.get(key) {
            None => Err(anyhow!(storage::result::StorageError::KeyNotFound(
                key.to_string()
            ))),
            Some(_) => self
                .append(LogEntry::Remove {
                    key: key.to_string(),
                })
                .map(|_| ()),
        }
    }

    fn open(path: &Path) -> anyhow::Result<Self> {
        let mut storage = LogStructured::new(path);
        storage
            .hydrate()
            .expect("[LOG_STRUCTURED.OPEN] Failed to hydrate db");
        Ok(storage)
    }
}
