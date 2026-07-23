use std::{fs, path::Path};

use foyer::RecoverMode;

use crate::{
    Error,
    store::{ExtentStore, ExtentStoreConfig, StoreLayout},
};

const STATE_FILE: &str = "state";

pub struct StoreOpen {
    pub store: ExtentStore,
    pub outcome: RecoveryOutcome,
}

pub enum RecoveryOutcome {
    Created,
    Recovered,
    Recreated(String),
}

/// Applies Foyer's recovery policy without leaking it into ExtentStore.
pub fn open_store(path: &Path, config: ExtentStoreConfig, recover_mode: RecoverMode) -> crate::Result<StoreOpen> {
    match recover_mode {
        RecoverMode::None => recreate_store(path, config).map(|store| StoreOpen {
            store,
            outcome: RecoveryOutcome::Created,
        }),
        RecoverMode::Quiet => {
            if state_exists(path)? {
                match ExtentStore::open_with_options(path, config.options)
                    .and_then(|store| verify_layout(store, config))
                {
                    Ok(store) => Ok(StoreOpen {
                        store,
                        outcome: RecoveryOutcome::Recovered,
                    }),
                    Err(error) if quiet_can_clear(&error) => recreate_store(path, config).map(|store| StoreOpen {
                        store,
                        outcome: RecoveryOutcome::Recreated(error.to_string()),
                    }),
                    Err(error) => Err(error),
                }
            } else {
                let reason = (!directory_is_empty(path)?).then(|| "state file is missing".to_string());
                recreate_store(path, config).map(|store| StoreOpen {
                    store,
                    outcome: reason.map_or(RecoveryOutcome::Created, RecoveryOutcome::Recreated),
                })
            }
        }
        RecoverMode::Strict => {
            if state_exists(path)? {
                let store = ExtentStore::open_with_options(path, config.options)?;
                verify_layout(store, config).map(|store| StoreOpen {
                    store,
                    outcome: RecoveryOutcome::Recovered,
                })
            } else if directory_is_empty(path)? {
                ExtentStore::create(path, config).map(|store| StoreOpen {
                    store,
                    outcome: RecoveryOutcome::Created,
                })
            } else {
                Err(Error::InvalidSuperblock(
                    "Extent cache directory is non-empty but has no state file".to_string(),
                ))
            }
        }
    }
}

fn recreate_store(path: &Path, config: ExtentStoreConfig) -> crate::Result<ExtentStore> {
    ExtentStore::recreate(path, config)
}

fn state_exists(path: &Path) -> crate::Result<bool> {
    path.join(STATE_FILE)
        .try_exists()
        .map_err(|error| Error::io("inspect Extent state file", error))
}

/// Quiet recovery may discard unusable cache contents, but must not turn environmental or
/// concurrency failures into destructive cache resets.
fn quiet_can_clear(error: &Error) -> bool {
    match error {
        Error::InvalidSuperblock(_) => true,
        Error::Io { source, .. } => matches!(
            source.kind(),
            std::io::ErrorKind::NotFound | std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::InvalidData
        ),
        _ => false,
    }
}

fn verify_layout(store: ExtentStore, config: ExtentStoreConfig) -> crate::Result<ExtentStore> {
    let expected = StoreLayout::create(config)?;
    if store.layout() != expected {
        return Err(Error::InvalidSuperblock(
            "recovered Extent layout does not match static configuration".to_string(),
        ));
    }
    Ok(store)
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
    use crate::{format::PAGE_SIZE, store::ExtentStoreOptions};

    fn config() -> ExtentStoreConfig {
        ExtentStoreConfig::new(4 * 1024 * 1024)
            .with_entry_charge(PAGE_SIZE)
            .with_options(
                ExtentStoreOptions::default()
                    .with_extent_size(PAGE_SIZE * 8)
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

        let open = open_store(&root, config(), RecoverMode::None).unwrap();
        open.store.sync().unwrap();
        drop(open.store);
        let open = open_store(&root, config(), RecoverMode::None).unwrap();
        open.store.sync().unwrap();
        drop(open.store);

        assert_eq!(fs::read(sentinel).unwrap(), b"keep");
    }

    #[test]
    fn quiet_clears_corrupt_cache() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("cache");
        let sentinel = root.join("owned-by-caller");

        let open = open_store(&root, config(), RecoverMode::None).unwrap();
        open.store.sync().unwrap();
        drop(open.store);
        fs::write(&sentinel, b"keep").unwrap();

        let state = root.join(STATE_FILE);
        let state_len = fs::metadata(&state).unwrap().len();
        fs::write(&state, vec![0xff; usize::try_from(state_len).unwrap()]).unwrap();

        let open = open_store(&root, config(), RecoverMode::Quiet).unwrap();
        assert!(matches!(open.outcome, RecoveryOutcome::Recreated(_)));
        open.store.sync().unwrap();
        drop(open.store);

        assert_eq!(fs::read(sentinel).unwrap(), b"keep");
    }

    #[test]
    fn quiet_does_not_clear_a_locked_cache() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("cache");
        let first = open_store(&root, config(), RecoverMode::None).unwrap();

        let second = open_store(&root, config(), RecoverMode::Quiet);
        assert!(matches!(second, Err(Error::DatabaseLocked(_))));
        assert!(root.join(STATE_FILE).exists());

        drop(first.store);
        let reopened = open_store(&root, config(), RecoverMode::Quiet).unwrap();
        assert!(matches!(reopened.outcome, RecoveryOutcome::Recovered));
    }

    #[test]
    fn quiet_clears_only_unusable_cache_contents() {
        assert!(quiet_can_clear(&Error::InvalidSuperblock("corrupt state".to_string())));
        for kind in [
            std::io::ErrorKind::NotFound,
            std::io::ErrorKind::UnexpectedEof,
            std::io::ErrorKind::InvalidData,
        ] {
            assert!(quiet_can_clear(&Error::io("open cache", std::io::Error::from(kind))));
        }

        assert!(!quiet_can_clear(&Error::DatabaseLocked("cache".into())));
        assert!(!quiet_can_clear(&Error::InvalidConfig("bad size".to_string())));
        assert!(!quiet_can_clear(&Error::io(
            "open cache",
            std::io::Error::from(std::io::ErrorKind::PermissionDenied),
        )));
    }
}
