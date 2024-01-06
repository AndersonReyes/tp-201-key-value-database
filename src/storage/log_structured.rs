use std::collections::HashMap;
use std::fs::DirEntry;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{fs, io};

use anyhow::{anyhow, Context};
use serde::{Deserialize, Serialize};

use crate::storage;
use crate::Engine;

/// after this many operations, we compact the current log file
const COMPACTION_OPS_THRESHOLD: u64 = 1024 * 3;

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
        #[serde(rename = "k")]
        key: String,
        #[serde(rename = "v")]
        value: String,
    },
    Remove {
        #[serde(rename = "k")]
        key: String,
    },
}

/// Uses log based storage
pub struct LogStructured {
    log_dir: String,
    main_log: String,
    index: HashMap<String, LogPointer>,
    uncompacted: u64,
}

impl LogStructured {
    fn log_file_name(dir: &Path) -> PathBuf {
        dir.join(format!(
            "{}.json",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ))
    }

    fn new(path: &Path) -> Self {
        let log_dir = path.join("log-files");
        if !log_dir.exists() {
            fs::create_dir(&log_dir).expect("failed to create log dir");
        }
        let log_path = LogStructured::log_file_name(&log_dir);

        Self {
            log_dir: log_dir.display().to_string(),
            main_log: log_path.display().to_string(),
            index: HashMap::new(),
            uncompacted: 0,
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
    fn append_to_log(&mut self, entry: LogEntry) -> anyhow::Result<LogPointer> {
        let serialized = serde_json::to_string(&entry)
            .with_context(|| format!("failed to serialize {:?}", &entry))?;
        let mut writer = self.write_append_file(&Path::new(&self.main_log))?;

        writeln!(writer, "{}", serialized)?;

        let curr_offset = writer.seek(SeekFrom::Current(0))?;

        Ok(LogPointer {
            offset: curr_offset - (serialized.len() as u64) - 1,
            size: serialized.len() + 1, // new line char
            path: self.main_log.to_string(),
        })
    }

    /// runs compaction on the current log file by moving the index to a new compacted file
    fn ops_compaction(&mut self) -> anyhow::Result<()> {
        if self.uncompacted <= COMPACTION_OPS_THRESHOLD {
            return Ok(());
        }

        let old_log = self.main_log.to_string();
        let new_path = LogStructured::log_file_name(&Path::new(&self.log_dir));
        self.main_log = new_path.display().to_string();

        let mut writer = self.write_append_file(&new_path)?;

        for log_pointer in self.index.values() {
            let mut reader = self.read_file(Path::new(&log_pointer.path))?;
            if reader.seek(SeekFrom::Current(0))? != log_pointer.offset {
                reader.seek(SeekFrom::Start(log_pointer.offset))?;
            }
            let mut entry_reader = reader.take(log_pointer.size as u64);
            io::copy(&mut entry_reader, &mut writer)
                .expect(&format!("failed to compact entry {:?}", log_pointer));
        }

        fs::remove_file(old_log)?;
        self.uncompacted = 0;
        Ok(())
    }

    /// looks for entry in the index and then reads from disk if not exist
    fn find(&self, log_pointer: &LogPointer) -> anyhow::Result<LogEntry> {
        let file = self.read_file(&Path::new(&log_pointer.path))?;
        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(log_pointer.offset))?;

        let mut line = String::new();
        reader.read_line(&mut line)?;
        assert_eq!(
            line.len(),
            log_pointer.size,
            "data corruption, expected line of size {} but got {} in {}",
            log_pointer.size,
            line.len(),
            line
        );
        line.truncate(log_pointer.size);

        Ok(serde_json::from_str(&line).expect(&format!("failed to deserialize {line}")))
    }

    /// populates local index from the log.
    /// Walks through all the logs in the directory in order
    /// and hydrates the index, keeping the last operation
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
                let size = line.len() + 1; // newline char
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
                offset += size as u64;
            }
        }
        Ok(())
    }
}

impl Engine for LogStructured {
    fn get(&self, key: &str) -> Option<String> {
        if let Some(LogEntry::Set { value, .. }) =
            self.index.get(key).and_then(|p| self.find(p).ok())
        {
            Some(value)
        } else {
            None
        }
    }

    fn set(&mut self, key: String, value: String) -> anyhow::Result<()> {
        let log_pointer = self.append_to_log(LogEntry::Set {
            key: key.clone(),
            value,
        })?;
        self.index.insert(key, log_pointer);
        self.uncompacted += 1;
        self.ops_compaction()?;
        Ok(())
    }

    fn remove(&mut self, key: &str) -> anyhow::Result<()> {
        if let Some(_) = self.index.remove(key) {
            self.append_to_log(LogEntry::Remove {
                key: key.to_string(),
            })?;
            self.uncompacted += 1;
            self.ops_compaction()?;

            Ok(())
        } else {
            Err(anyhow!(storage::result::StorageError::KeyNotFound(
                key.to_string()
            )))
        }
    }

    fn open(path: &Path) -> anyhow::Result<Self> {
        let mut storage = LogStructured::new(path);
        storage.hydrate().expect("failed to hydrate db");
        Ok(storage)
    }
}
