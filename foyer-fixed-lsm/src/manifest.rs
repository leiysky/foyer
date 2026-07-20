use std::{
    collections::HashSet,
    fs,
    fs::OpenOptions,
    io::Write,
    path::{Path, PathBuf},
};

use crate::{
    error::{Error, Result},
    format::{
        DATA_BLOCK_SIZE, KEY_SIZE, MAX_SEQUENCE, RECORD_SIZE, VALUE_SIZE, checksum, get_u32, get_u64, put_u32, put_u64,
        sync_directory,
    },
};

const MANIFEST_MAGIC: [u8; 8] = *b"FXLSMM01";
const FORMAT_VERSION: u32 = 1;
const HEADER_SIZE: usize = 96;
const TABLE_ENTRY_SIZE: usize = 16;
const CHECKSUM_SIZE: usize = size_of::<u32>();
const MAX_MANIFEST_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManifestTable {
    pub file_id: u64,
    pub level: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    pub generation: u64,
    pub flushed_sequence: u64,
    pub next_sequence: u64,
    pub next_file_id: u64,
    pub user_state: u64,
    pub tables: Vec<ManifestTable>,
}

impl Manifest {
    pub fn initial() -> Self {
        Self {
            generation: 1,
            flushed_sequence: 0,
            next_sequence: 1,
            next_file_id: 1,
            user_state: 0,
            tables: Vec::new(),
        }
    }

    pub fn load_candidates(directory: &Path, level_count: usize) -> Result<Vec<Self>> {
        let mut valid = Vec::new();
        let mut failures = Vec::new();
        for slot in 0..2 {
            let path = manifest_path(directory, slot);
            match fs::read(&path) {
                Ok(bytes) => match decode(&path, &bytes, level_count) {
                    Ok(manifest) => valid.push(manifest),
                    Err(error) => failures.push(error),
                },
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(Error::io("read manifest", error)),
            }
        }
        valid.sort_unstable_by_key(|manifest| std::cmp::Reverse(manifest.generation));
        if valid.is_empty() {
            return Err(failures
                .into_iter()
                .next()
                .unwrap_or_else(|| Error::MissingDatabase(directory.to_path_buf())));
        }
        Ok(valid)
    }

    pub fn persist(&self, directory: &Path, level_count: usize) -> Result<()> {
        let bytes = encode(self, level_count)?;
        let slot = self.generation as usize % 2;
        let path = manifest_path(directory, slot);
        let temporary = temporary_manifest_path(directory, slot);
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)
            .map_err(|error| Error::io("create temporary manifest", error))?;
        file.write_all(&bytes)
            .map_err(|error| Error::io("write manifest", error))?;
        file.sync_data().map_err(|error| Error::io("sync manifest", error))?;
        fs::rename(&temporary, &path).map_err(|error| Error::io("publish manifest", error))?;
        sync_directory(directory)
    }

    pub fn encoded_size(&self, level_count: usize) -> Result<u64> {
        u64::try_from(encode(self, level_count)?.len())
            .map_err(|_| Error::InvalidOptions("manifest size overflows u64".to_string()))
    }

    pub fn replaced_file_size(&self, directory: &Path) -> Result<u64> {
        match fs::metadata(manifest_path(directory, self.generation as usize % 2)) {
            Ok(metadata) => Ok(metadata.len()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(error) => Err(Error::io("stat replaced manifest", error)),
        }
    }

    pub fn cleanup_temporary_files(directory: &Path) -> Result<()> {
        let mut removed = false;
        for slot in 0..2 {
            let path = temporary_manifest_path(directory, slot);
            match fs::remove_file(path) {
                Ok(()) => removed = true,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(Error::io("remove stale temporary manifest", error)),
            }
        }
        if removed {
            sync_directory(directory)?;
        }
        Ok(())
    }
}

fn encode(manifest: &Manifest, level_count: usize) -> Result<Vec<u8>> {
    validate(manifest, level_count, Path::new("manifest"))?;
    let body_len = manifest
        .tables
        .len()
        .checked_mul(TABLE_ENTRY_SIZE)
        .ok_or_else(|| Error::InvalidOptions("manifest table count overflows usize".to_string()))?;
    let mut output = vec![0; HEADER_SIZE + body_len + CHECKSUM_SIZE];
    output[..8].copy_from_slice(&MANIFEST_MAGIC);
    put_u32(&mut output, 8, FORMAT_VERSION);
    put_u32(&mut output, 12, HEADER_SIZE as u32);
    put_u64(&mut output, 16, manifest.generation);
    put_u64(&mut output, 24, manifest.flushed_sequence);
    put_u64(&mut output, 32, manifest.next_sequence);
    put_u64(&mut output, 40, manifest.next_file_id);
    put_u32(&mut output, 48, level_count as u32);
    put_u32(&mut output, 52, manifest.tables.len() as u32);
    put_u32(&mut output, 56, KEY_SIZE as u32);
    put_u32(&mut output, 60, VALUE_SIZE as u32);
    put_u32(&mut output, 64, RECORD_SIZE as u32);
    put_u32(&mut output, 68, DATA_BLOCK_SIZE as u32);
    put_u64(&mut output, 72, body_len as u64);
    put_u64(&mut output, 80, manifest.user_state);
    let mut tables = manifest.tables.clone();
    tables.sort_unstable_by_key(|table| (table.level, table.file_id));
    for (index, table) in tables.into_iter().enumerate() {
        let offset = HEADER_SIZE + index * TABLE_ENTRY_SIZE;
        put_u64(&mut output, offset, table.file_id);
        put_u32(&mut output, offset + 8, table.level);
    }
    let checksum_offset = output.len() - CHECKSUM_SIZE;
    let digest = checksum(&output[..checksum_offset]);
    put_u32(&mut output, checksum_offset, digest);
    Ok(output)
}

fn decode(path: &Path, input: &[u8], level_count: usize) -> Result<Manifest> {
    if input.len() < HEADER_SIZE + CHECKSUM_SIZE
        || input.len() as u64 > MAX_MANIFEST_BYTES
        || input[..8] != MANIFEST_MAGIC
        || get_u32(input, 8) != FORMAT_VERSION
        || get_u32(input, 12) != HEADER_SIZE as u32
        || get_u32(input, 48) != level_count as u32
        || get_u32(input, 56) != KEY_SIZE as u32
        || get_u32(input, 60) != VALUE_SIZE as u32
        || get_u32(input, 64) != RECORD_SIZE as u32
        || get_u32(input, 68) != DATA_BLOCK_SIZE as u32
    {
        return Err(Error::corruption(path, "invalid manifest header"));
    }
    let table_count = get_u32(input, 52) as usize;
    let body_len = get_u64(input, 72);
    let expected_body_len = table_count
        .checked_mul(TABLE_ENTRY_SIZE)
        .and_then(|size| u64::try_from(size).ok())
        .ok_or_else(|| Error::corruption(path, "manifest table count overflows"))?;
    if body_len != expected_body_len || HEADER_SIZE as u64 + body_len + CHECKSUM_SIZE as u64 != input.len() as u64 {
        return Err(Error::corruption(path, "invalid manifest length"));
    }
    let checksum_offset = input.len() - CHECKSUM_SIZE;
    if checksum(&input[..checksum_offset]) != get_u32(input, checksum_offset) {
        return Err(Error::corruption(path, "manifest checksum mismatch"));
    }
    let mut tables = Vec::with_capacity(table_count);
    for index in 0..table_count {
        let offset = HEADER_SIZE + index * TABLE_ENTRY_SIZE;
        tables.push(ManifestTable {
            file_id: get_u64(input, offset),
            level: get_u32(input, offset + 8),
        });
    }
    let manifest = Manifest {
        generation: get_u64(input, 16),
        flushed_sequence: get_u64(input, 24),
        next_sequence: get_u64(input, 32),
        next_file_id: get_u64(input, 40),
        user_state: get_u64(input, 80),
        tables,
    };
    validate(&manifest, level_count, path)?;
    Ok(manifest)
}

fn validate(manifest: &Manifest, level_count: usize, path: &Path) -> Result<()> {
    if level_count == 0
        || level_count > u32::MAX as usize
        || manifest.generation == 0
        || manifest.next_sequence == 0
        || manifest.next_sequence > MAX_SEQUENCE + 1
        || manifest.flushed_sequence >= manifest.next_sequence
        || manifest.next_file_id == 0
        || manifest.tables.len() > u32::MAX as usize
    {
        return Err(Error::corruption(path, "invalid manifest state"));
    }
    let mut ids = HashSet::with_capacity(manifest.tables.len());
    for table in &manifest.tables {
        if table.file_id == 0
            || table.file_id >= manifest.next_file_id
            || table.level as usize >= level_count
            || !ids.insert(table.file_id)
        {
            return Err(Error::corruption(path, "invalid manifest table set"));
        }
    }
    Ok(())
}

fn manifest_path(directory: &Path, slot: usize) -> PathBuf {
    directory.join(format!("MANIFEST-{slot}"))
}

fn temporary_manifest_path(directory: &Path, slot: usize) -> PathBuf {
    directory.join(format!("MANIFEST-{slot}.tmp"))
}

#[cfg(test)]
mod tests {
    use crate::manifest::{Manifest, ManifestTable};

    #[test]
    fn newest_valid_manifest_wins() {
        let directory = tempfile::tempdir().unwrap();
        let initial = Manifest::initial();
        initial.persist(directory.path(), 7).unwrap();
        let next = Manifest {
            generation: 2,
            flushed_sequence: 9,
            next_sequence: 10,
            next_file_id: 2,
            user_state: 7,
            tables: vec![ManifestTable { file_id: 1, level: 0 }],
        };
        next.persist(directory.path(), 7).unwrap();
        assert_eq!(
            Manifest::load_candidates(directory.path(), 7).unwrap(),
            vec![next.clone(), initial.clone()]
        );

        std::fs::write(directory.path().join("MANIFEST-0"), b"torn").unwrap();
        assert_eq!(Manifest::load_candidates(directory.path(), 7).unwrap(), vec![initial]);
    }
}
