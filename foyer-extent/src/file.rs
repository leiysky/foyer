use std::{
    fs::{File, OpenOptions},
    path::Path,
};

use crate::format::PAGE_SIZE;

pub(crate) fn open_cache_file(path: &Path, create: bool, direct_io: bool) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    if create {
        options.create(true).truncate(true);
    }

    #[cfg(target_os = "linux")]
    if direct_io {
        use std::os::unix::fs::OpenOptionsExt;

        let flags = i32::try_from(rustix::fs::OFlags::DIRECT.bits()).expect("Linux O_DIRECT flag must fit i32");
        options.custom_flags(flags);
    }
    #[cfg(not(target_os = "linux"))]
    let _ = direct_io;

    options.open(path)
}

pub(crate) fn reserve_cache_file(file: &File, len: u64) -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        rustix::fs::fallocate(file, rustix::fs::FallocateFlags::empty(), 0, len)
            .map_err(|error| std::io::Error::from_raw_os_error(error.raw_os_error()))
    }
    #[cfg(not(target_os = "linux"))]
    {
        file.set_len(len)
    }
}

pub(crate) fn ensure_cache_file_reserved(file: &File, len: u64) -> std::io::Result<()> {
    if allocated_file_size(file)? < len {
        reserve_cache_file(file, len)?;
    }
    Ok(())
}

pub(crate) fn allocated_file_size(file: &File) -> std::io::Result<u64> {
    let metadata = file.metadata()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        Ok(metadata.blocks().saturating_mul(512))
    }
    #[cfg(not(unix))]
    {
        Ok(metadata.len())
    }
}

pub(crate) struct AlignedBuffer {
    storage: Vec<u8>,
    start: usize,
    len: usize,
}

impl AlignedBuffer {
    pub(crate) fn new(len: usize) -> Self {
        debug_assert_eq!(len % PAGE_SIZE, 0);
        let storage = vec![0; len + PAGE_SIZE];
        let address = storage.as_ptr() as usize;
        let start = (PAGE_SIZE - address % PAGE_SIZE) % PAGE_SIZE;
        Self { storage, start, len }
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.storage[self.start..self.start + self.len]
    }

    pub(crate) fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.storage[self.start..self.start + self.len]
    }
}

#[cfg(unix)]
pub(crate) fn read_exact_at(file: &File, output: &mut [u8], offset: u64) -> std::io::Result<()> {
    std::os::unix::fs::FileExt::read_exact_at(file, output, offset)
}

#[cfg(windows)]
pub(crate) fn read_exact_at(file: &File, mut output: &mut [u8], mut offset: u64) -> std::io::Result<()> {
    use std::os::windows::fs::FileExt;

    while !output.is_empty() {
        let read = file.seek_read(output, offset)?;
        if read == 0 {
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
        }
        output = &mut output[read..];
        offset += read as u64;
    }
    Ok(())
}

#[cfg(unix)]
pub(crate) fn write_all_at(file: &File, input: &[u8], offset: u64) -> std::io::Result<()> {
    std::os::unix::fs::FileExt::write_all_at(file, input, offset)
}

#[cfg(windows)]
pub(crate) fn write_all_at(file: &File, mut input: &[u8], mut offset: u64) -> std::io::Result<()> {
    use std::os::windows::fs::FileExt;

    while !input.is_empty() {
        let written = file.seek_write(input, offset)?;
        if written == 0 {
            return Err(std::io::Error::from(std::io::ErrorKind::WriteZero));
        }
        input = &input[written..];
        offset += written as u64;
    }
    Ok(())
}
