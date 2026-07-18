use std::{
    fs,
    fs::{File, OpenOptions},
    io::{Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use crate::{
    error::{Error, Result},
    format::{
        MAX_SEQUENCE, RECORD_SIZE, Record, checksum, get_u32, get_u64, put_u32, put_u64, read_exact_at, sync_directory,
    },
};

const WAL_MAGIC: [u8; 8] = *b"FXLSMW02";
const FORMAT_VERSION: u32 = 2;
const HEADER_SIZE: usize = 48;
const PAYLOAD_CHECKSUM_OFFSET: usize = 40;
const HEADER_CHECKSUM_OFFSET: usize = 44;

#[derive(Debug, Default)]
pub struct Replay {
    pub records: Vec<Record>,
    pub max_sequence: u64,
    pub user_state: Option<u64>,
    pub valid_bytes: u64,
    pub discarded_tail_bytes: u64,
    pub highest_wal_id: u64,
}

#[derive(Debug)]
pub struct Wal {
    id: u64,
    path: PathBuf,
    file: File,
    length: u64,
}

impl Wal {
    pub fn frame_size(record_count: usize) -> Result<u64> {
        if record_count == 0 {
            return Ok(0);
        }
        if record_count > u32::MAX as usize {
            return Err(Error::InvalidOptions("WAL batch has too many records".to_string()));
        }
        record_count
            .checked_mul(RECORD_SIZE)
            .and_then(|bytes| bytes.checked_add(HEADER_SIZE))
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(|| Error::InvalidOptions("WAL frame size overflows u64".to_string()))
    }

    pub fn create(directory: &Path, id: u64) -> Result<Self> {
        if id == 0 {
            return Err(Error::corruption(directory, "WAL id must not be zero"));
        }
        let path = wal_path(directory, id);
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|error| Error::io("create WAL", error))?;
        file.sync_data().map_err(|error| Error::io("sync new WAL", error))?;
        sync_directory(directory)?;
        Ok(Self {
            id,
            path,
            file,
            length: 0,
        })
    }

    pub fn append(&mut self, records: &[Record], user_state: u64, sync: bool) -> Result<u64> {
        if records.is_empty() {
            return Ok(0);
        }
        validate_batch(&self.path, records)?;
        if records.len() == 1 {
            let mut frame = [0; HEADER_SIZE + RECORD_SIZE];
            encode_frame(&mut frame, records, user_state);
            return self.write_frame(&frame, sync);
        }
        let payload_len = records
            .len()
            .checked_mul(RECORD_SIZE)
            .ok_or_else(|| Error::corruption(&self.path, "WAL batch length overflows"))?;
        let mut frame = vec![0; HEADER_SIZE + payload_len];
        encode_frame(&mut frame, records, user_state);
        self.write_frame(&frame, sync)
    }

    fn write_frame(&mut self, frame: &[u8], sync: bool) -> Result<u64> {
        self.file
            .seek(SeekFrom::Start(self.length))
            .and_then(|_| self.file.write_all(frame))
            .map_err(|error| Error::io("append WAL batch", error))?;
        self.length += frame.len() as u64;
        if sync {
            self.file
                .sync_data()
                .map_err(|error| Error::io("sync WAL batch", error))?;
        }
        Ok(frame.len() as u64)
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    #[cfg(test)]
    pub fn length(&self) -> u64 {
        self.length
    }
}

fn encode_frame(frame: &mut [u8], records: &[Record], user_state: u64) {
    debug_assert_eq!(frame.len(), HEADER_SIZE + records.len() * RECORD_SIZE);
    let payload_len = records.len() * RECORD_SIZE;
    debug_assert_eq!(frame.len(), HEADER_SIZE + payload_len);
    frame.fill(0);
    frame[..8].copy_from_slice(&WAL_MAGIC);
    put_u32(frame, 8, FORMAT_VERSION);
    put_u32(frame, 12, records.len() as u32);
    put_u64(frame, 16, records[0].sequence);
    put_u64(frame, 24, user_state);
    for (index, record) in records.iter().enumerate() {
        let offset = HEADER_SIZE + index * RECORD_SIZE;
        frame[offset..offset + RECORD_SIZE].copy_from_slice(&record.encode());
    }
    let payload_checksum = checksum(&frame[HEADER_SIZE..]);
    put_u32(frame, PAYLOAD_CHECKSUM_OFFSET, payload_checksum);
    let header_checksum = checksum(&frame[..HEADER_CHECKSUM_OFFSET]);
    put_u32(frame, HEADER_CHECKSUM_OFFSET, header_checksum);
}

pub fn replay_all(directory: &Path, flushed_sequence: u64) -> Result<Replay> {
    let mut files = list_wal_files(directory)?;
    files.sort_unstable_by_key(|(id, _)| *id);
    let highest_wal_id = files.last().map_or(0, |(id, _)| *id);
    let mut replay = Replay {
        highest_wal_id,
        ..Replay::default()
    };
    let mut previous_sequence = 0;
    for (id, path) in files {
        let part = replay_file(&path, flushed_sequence, &mut previous_sequence)?;
        if part.discarded_tail_bytes > 0 && id != highest_wal_id {
            return Err(Error::corruption(
                &path,
                "incomplete WAL tail precedes a newer WAL file",
            ));
        }
        replay.records.extend(part.records);
        replay.max_sequence = replay.max_sequence.max(part.max_sequence);
        if part.user_state.is_some() {
            replay.user_state = part.user_state;
        }
        replay.valid_bytes = replay.valid_bytes.saturating_add(part.valid_bytes);
        replay.discarded_tail_bytes = replay.discarded_tail_bytes.saturating_add(part.discarded_tail_bytes);
        if part.discarded_tail_bytes > 0 {
            let file = OpenOptions::new()
                .write(true)
                .open(&path)
                .map_err(|error| Error::io("open incomplete WAL for truncation", error))?;
            file.set_len(part.valid_bytes)
                .and_then(|_| file.sync_data())
                .map_err(|error| Error::io("truncate incomplete WAL tail", error))?;
        }
    }
    Ok(replay)
}

pub fn cleanup_wal_files(directory: &Path, keep_id: u64) -> Result<u64> {
    let mut removed_bytes = 0_u64;
    for (id, path) in list_wal_files(directory)? {
        if id != keep_id {
            let bytes = fs::metadata(&path)
                .map_err(|error| Error::io("stat obsolete WAL", error))?
                .len();
            fs::remove_file(path).map_err(|error| Error::io("remove obsolete WAL", error))?;
            removed_bytes = removed_bytes
                .checked_add(bytes)
                .ok_or_else(|| Error::InvalidOptions("removed WAL bytes overflow u64".to_string()))?;
        }
    }
    if removed_bytes > 0 {
        sync_directory(directory)?;
    }
    Ok(removed_bytes)
}

pub fn cleanup_wal_files_through(directory: &Path, maximum_id: u64) -> Result<u64> {
    let mut removed_bytes = 0_u64;
    for (id, path) in list_wal_files(directory)? {
        if id <= maximum_id {
            let bytes = fs::metadata(&path)
                .map_err(|error| Error::io("stat flushed WAL", error))?
                .len();
            fs::remove_file(path).map_err(|error| Error::io("remove flushed WAL", error))?;
            removed_bytes = removed_bytes
                .checked_add(bytes)
                .ok_or_else(|| Error::InvalidOptions("removed WAL bytes overflow u64".to_string()))?;
        }
    }
    if removed_bytes > 0 {
        sync_directory(directory)?;
    }
    Ok(removed_bytes)
}

pub fn sync_all_wal_files(directory: &Path) -> Result<()> {
    // The caller serializes WAL append/rotation. A background flush may concurrently unlink an old
    // WAL, but only after its SST and manifest are durable, so a vanished path already satisfies
    // the durability fence.
    for (_, path) in list_wal_files(directory)? {
        let file = match OpenOptions::new().read(true).write(true).open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(Error::io("open WAL for sync", error)),
        };
        file.sync_data().map_err(|error| Error::io("sync WAL", error))?;
    }
    Ok(())
}

pub fn total_wal_bytes(directory: &Path) -> u64 {
    list_wal_files(directory)
        .map(|files| {
            files
                .into_iter()
                .filter_map(|(_, path)| fs::metadata(path).ok().map(|metadata| metadata.len()))
                .sum()
        })
        .unwrap_or(0)
}

fn replay_file(path: &Path, flushed_sequence: u64, previous_sequence: &mut u64) -> Result<Replay> {
    let file = File::open(path).map_err(|error| Error::io("open WAL for recovery", error))?;
    let file_size = file
        .metadata()
        .map_err(|error| Error::io("stat WAL for recovery", error))?
        .len();
    let mut replay = Replay::default();
    let mut offset = 0_u64;
    while offset < file_size {
        if file_size - offset < HEADER_SIZE as u64 {
            break;
        }
        let mut header = [0; HEADER_SIZE];
        read_exact_at(&file, &mut header, offset).map_err(|error| Error::io("read WAL frame header", error))?;
        if header[..8] != WAL_MAGIC
            || get_u32(&header, 8) != FORMAT_VERSION
            || checksum(&header[..HEADER_CHECKSUM_OFFSET]) != get_u32(&header, HEADER_CHECKSUM_OFFSET)
        {
            return Err(Error::corruption(path, "invalid WAL frame header"));
        }
        let count = get_u32(&header, 12) as usize;
        let first_sequence = get_u64(&header, 16);
        if count == 0 || first_sequence == 0 {
            return Err(Error::corruption(path, "invalid WAL batch metadata"));
        }
        let payload_len = count
            .checked_mul(RECORD_SIZE)
            .ok_or_else(|| Error::corruption(path, "WAL payload length overflows"))?;
        let frame_len = HEADER_SIZE as u64 + payload_len as u64;
        if frame_len > file_size - offset {
            break;
        }
        let mut payload = vec![0; payload_len];
        read_exact_at(&file, &mut payload, offset + HEADER_SIZE as u64)
            .map_err(|error| Error::io("read WAL frame payload", error))?;
        if checksum(&payload) != get_u32(&header, PAYLOAD_CHECKSUM_OFFSET) {
            return Err(Error::corruption(path, "WAL frame checksum mismatch"));
        }
        let user_state = get_u64(&header, 24);
        let mut replayed = false;
        for (index, encoded) in payload.chunks_exact(RECORD_SIZE).enumerate() {
            let record = Record::decode(encoded).ok_or_else(|| Error::corruption(path, "invalid WAL record"))?;
            let expected_sequence = first_sequence
                .checked_add(index as u64)
                .filter(|sequence| *sequence <= MAX_SEQUENCE)
                .ok_or_else(|| Error::corruption(path, "WAL sequence overflows"))?;
            if record.sequence != expected_sequence || record.sequence <= *previous_sequence {
                return Err(Error::corruption(path, "non-monotonic WAL sequence"));
            }
            *previous_sequence = record.sequence;
            replay.max_sequence = replay.max_sequence.max(record.sequence);
            if record.sequence > flushed_sequence {
                replay.records.push(record);
                replayed = true;
            }
        }
        if replayed {
            replay.user_state = Some(user_state);
        }
        offset += frame_len;
    }
    replay.valid_bytes = offset;
    replay.discarded_tail_bytes = file_size - offset;
    Ok(replay)
}

fn list_wal_files(directory: &Path) -> Result<Vec<(u64, PathBuf)>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(directory).map_err(|error| Error::io("list WAL files", error))? {
        let entry = entry.map_err(|error| Error::io("read WAL directory entry", error))?;
        if let Some(id) = entry.file_name().to_str().and_then(parse_wal_file_name) {
            files.push((id, entry.path()));
        }
    }
    Ok(files)
}

pub fn wal_path(directory: &Path, id: u64) -> PathBuf {
    directory.join(format!("wal-{id:020}.log"))
}

fn parse_wal_file_name(name: &str) -> Option<u64> {
    let id = name.strip_prefix("wal-")?.strip_suffix(".log")?;
    (id.len() == 20).then(|| id.parse().ok()).flatten()
}

fn validate_batch(path: &Path, records: &[Record]) -> Result<()> {
    if records.len() > u32::MAX as usize {
        return Err(Error::corruption(path, "WAL batch has too many records"));
    }
    let first = records[0].sequence;
    for (index, record) in records.iter().enumerate() {
        let expected = first
            .checked_add(index as u64)
            .filter(|sequence| *sequence <= MAX_SEQUENCE)
            .ok_or(Error::SequenceExhausted)?;
        if record.sequence != expected {
            return Err(Error::corruption(path, "WAL batch sequences are not contiguous"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{fs, fs::OpenOptions, io::Write};

    use crate::{
        error::Error,
        format::Record,
        wal::{HEADER_SIZE, Wal, replay_all, wal_path},
    };

    fn records(first: u64) -> Vec<Record> {
        (first..first + 4)
            .map(|sequence| Record::put([sequence as u8; 24], [sequence as u8; 32], sequence))
            .collect()
    }

    #[test]
    fn replay_orders_wal_files_skips_flushed_records_and_discards_partial_tail() {
        let directory = tempfile::tempdir().unwrap();
        let mut first = Wal::create(directory.path(), 1).unwrap();
        first.append(&records(1), 11, true).unwrap();
        let first_bytes = first.length();
        drop(first);
        let mut second = Wal::create(directory.path(), 2).unwrap();
        second.append(&records(5), 22, true).unwrap();
        let second_bytes = second.length();
        drop(second);
        OpenOptions::new()
            .append(true)
            .open(crate::wal::wal_path(directory.path(), 2))
            .unwrap()
            .write_all(b"partial")
            .unwrap();

        let replay = replay_all(directory.path(), 6).unwrap();
        assert_eq!(replay.records.len(), 2);
        assert_eq!(replay.records[0].sequence, 7);
        assert_eq!(replay.user_state, Some(22));
        assert_eq!(replay.valid_bytes, first_bytes + second_bytes);
        assert_eq!(replay.discarded_tail_bytes, 7);
        assert_eq!(replay.highest_wal_id, 2);
    }

    #[test]
    fn complete_final_frame_with_a_bad_checksum_is_corruption() {
        let directory = tempfile::tempdir().unwrap();
        let mut wal = Wal::create(directory.path(), 1).unwrap();
        wal.append(&records(1), 7, true).unwrap();
        drop(wal);

        let path = wal_path(directory.path(), 1);
        let mut bytes = fs::read(&path).unwrap();
        bytes[HEADER_SIZE] ^= 1;
        fs::write(path, bytes).unwrap();

        assert!(matches!(replay_all(directory.path(), 0), Err(Error::Corruption { .. })));
    }
}
