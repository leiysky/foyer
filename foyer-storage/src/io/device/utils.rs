// Copyright 2026 foyer Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#[cfg(unix)]
pub fn get_dev_capacity(path: impl AsRef<std::path::Path>) -> foyer_common::error::Result<usize> {
    use std::{fs::File, os::fd::AsRawFd};

    let file = File::open(path.as_ref())?;
    get_dev_capacity_fd(file.as_raw_fd())
}

#[cfg(target_os = "linux")]
fn get_dev_capacity_fd(fd: std::os::fd::RawFd) -> foyer_common::error::Result<usize> {
    use foyer_common::error::{Error, ErrorKind};

    const BLKGETSIZE64: libc::c_ulong = 0x80081272;

    let mut size: u64 = 0;
    let res = unsafe { libc::ioctl(fd, BLKGETSIZE64, &mut size) };
    if res == -1 {
        return Err(std::io::Error::last_os_error().into());
    }
    capacity_to_usize(size)
}

#[cfg(target_os = "freebsd")]
fn get_dev_capacity_fd(fd: std::os::fd::RawFd) -> foyer_common::error::Result<usize> {
    use foyer_common::error::{Error, ErrorKind};

    const DIOCGMEDIASIZE: libc::c_ulong = 0x40086481;

    let mut size: libc::off_t = 0;
    let res = unsafe { libc::ioctl(fd, DIOCGMEDIASIZE, &mut size) };
    if res == -1 {
        return Err(std::io::Error::last_os_error().into());
    }
    let size = u64::try_from(size)
        .map_err(|_| Error::new(ErrorKind::OutOfRange, format!("device capacity {size} is negative")))?;
    capacity_to_usize(size)
}

#[cfg(target_os = "macos")]
fn get_dev_capacity_fd(fd: std::os::fd::RawFd) -> foyer_common::error::Result<usize> {
    use foyer_common::error::{Error, ErrorKind};

    const DKIOCGETBLOCKSIZE: libc::c_ulong = 0x40046418;
    const DKIOCGETBLOCKCOUNT: libc::c_ulong = 0x40086419;

    let mut block_size: u32 = 0;
    let mut block_count: u64 = 0;
    let res = unsafe { libc::ioctl(fd, DKIOCGETBLOCKSIZE, &mut block_size) };
    if res == -1 {
        return Err(std::io::Error::last_os_error().into());
    }
    let res = unsafe { libc::ioctl(fd, DKIOCGETBLOCKCOUNT, &mut block_count) };
    if res == -1 {
        return Err(std::io::Error::last_os_error().into());
    }
    let size = u64::from(block_size).checked_mul(block_count).ok_or_else(|| {
        Error::new(
            ErrorKind::OutOfRange,
            format!("device geometry {block_size} * {block_count} overflows u64"),
        )
    })?;
    capacity_to_usize(size)
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))))]
fn get_dev_capacity_fd(_: std::os::fd::RawFd) -> foyer_common::error::Result<usize> {
    use foyer_common::error::{Error, ErrorKind};

    Err(Error::new(
        ErrorKind::Unsupported,
        "get_dev_capacity() is not supported on this platform".to_string(),
    ))
}

#[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]
fn capacity_to_usize(size: u64) -> foyer_common::error::Result<usize> {
    use foyer_common::error::{Error, ErrorKind};

    usize::try_from(size).map_err(|_| {
        Error::new(
            ErrorKind::OutOfRange,
            format!("device capacity {size} does not fit in usize"),
        )
    })
}

#[cfg(all(test, any(target_os = "linux", target_os = "freebsd", target_os = "macos")))]
mod tests {
    use foyer_common::error::ErrorKind;

    use super::*;

    #[test]
    fn regular_file_reports_the_ioctl_error() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let error = get_dev_capacity(file.path()).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::Io);
    }
}
