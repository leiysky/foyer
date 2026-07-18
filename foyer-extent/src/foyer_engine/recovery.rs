use std::{fs, path::Path};

use foyer::RecoverMode;

use crate::{
    Error,
    segment::{SegmentEngine, SegmentEngineConfig, SegmentLayout},
};

const STATE_FILE: &str = "state";

/// Applies Foyer's recovery policy without leaking it into SegmentEngine.
pub fn open_segment(
    path: &Path,
    config: SegmentEngineConfig,
    recover_mode: RecoverMode,
) -> crate::Result<SegmentEngine> {
    match recover_mode {
        RecoverMode::None => recreate_segment(path, config),
        RecoverMode::Quiet => {
            if path.join(STATE_FILE).exists() {
                match SegmentEngine::open_with_options(path, config.options)
                    .and_then(|engine| verify_layout(engine, config))
                {
                    Ok(engine) => Ok(engine),
                    Err(_) => recreate_segment(path, config),
                }
            } else {
                recreate_segment(path, config)
            }
        }
        RecoverMode::Strict => {
            if path.join(STATE_FILE).exists() {
                let engine = SegmentEngine::open_with_options(path, config.options)?;
                verify_layout(engine, config)
            } else if directory_is_empty(path)? {
                SegmentEngine::create(path, config)
            } else {
                Err(Error::InvalidSuperblock(
                    "Extent cache directory is non-empty but has no state file".to_string(),
                ))
            }
        }
    }
}

fn recreate_segment(path: &Path, config: SegmentEngineConfig) -> crate::Result<SegmentEngine> {
    SegmentEngine::recreate(path, config)
}

fn verify_layout(engine: SegmentEngine, config: SegmentEngineConfig) -> crate::Result<SegmentEngine> {
    let expected = SegmentLayout::create(config)?;
    if engine.slot_size() != expected.slot_size || engine.file_size() != expected.total_file_size {
        return Err(Error::InvalidSuperblock(
            "recovered Extent layout does not match static configuration".to_string(),
        ));
    }
    Ok(engine)
}

fn directory_is_empty(path: &Path) -> crate::Result<bool> {
    if !path.exists() {
        return Ok(true);
    }
    let mut entries = fs::read_dir(path).map_err(|error| Error::io("inspect Extent cache directory", error))?;
    Ok(entries.next().is_none())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{format::PAGE_SIZE, segment::SegmentEngineOptions};

    fn config() -> SegmentEngineConfig {
        SegmentEngineConfig::new(4 * 1024 * 1024)
            .with_slot_size(PAGE_SIZE)
            .with_options(
                SegmentEngineOptions::default()
                    .with_segment_size(PAGE_SIZE * 8)
                    .with_index_write_buffer_size(PAGE_SIZE * 4)
                    .with_index_cache_size(1024 * 1024),
            )
    }

    #[test]
    fn reset_removes_only_extent_owned_paths() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("cache");
        fs::create_dir_all(&root).unwrap();
        let sentinel = root.join("owned-by-caller");
        fs::write(&sentinel, b"keep").unwrap();

        let engine = open_segment(&root, config(), RecoverMode::None).unwrap();
        engine.sync().unwrap();
        drop(engine);
        let engine = open_segment(&root, config(), RecoverMode::None).unwrap();
        engine.sync().unwrap();
        drop(engine);

        assert_eq!(fs::read(sentinel).unwrap(), b"keep");
    }
}
