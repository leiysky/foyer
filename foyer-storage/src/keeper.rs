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

use std::{
    fmt::Debug,
    hash::Hash,
    ops::Deref,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use foyer_common::code::StorageKey;
use foyer_memory::Piece;
use hashbrown::hash_table::{Entry as HashTableEntry, HashTable};
use parking_lot::RwLock;

struct KeptPiece<K, V, P> {
    piece: Piece<K, V, P>,
    generation: u64,
}

type Shard<K, V, P> = HashTable<KeptPiece<K, V, P>>;

struct Inner<K, V, P>
where
    K: StorageKey,
{
    shards: Vec<Arc<RwLock<Shard<K, V, P>>>>,
    next_generation: AtomicU64,
}

pub struct Keeper<K, V, P>
where
    K: StorageKey,
{
    inner: Arc<Inner<K, V, P>>,
}

impl<K, V, P> Debug for Keeper<K, V, P>
where
    K: StorageKey,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Keeper")
            .field("shards", &self.inner.shards.len())
            .finish()
    }
}

impl<K, V, P> Keeper<K, V, P>
where
    K: StorageKey,
{
    pub fn new(shards: usize) -> Self {
        let shards = (0..shards).map(|_| Arc::new(RwLock::new(Shard::default()))).collect();
        Self {
            inner: Arc::new(Inner {
                shards,
                next_generation: AtomicU64::new(1),
            }),
        }
    }

    pub fn insert(&self, piece: Piece<K, V, P>) -> PieceRef<K, V, P> {
        let shard = self.shard(piece.hash());
        let generation = self
            .inner
            .next_generation
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |generation| {
                generation.checked_add(1)
            })
            .expect("pending-write keeper generation is exhausted");

        match shard
            .write()
            .entry(piece.hash(), |p| piece.key() == p.piece.key(), |p| p.piece.hash())
        {
            HashTableEntry::Occupied(mut o) => {
                *o.get_mut() = KeptPiece {
                    piece: piece.clone(),
                    generation,
                };
            }
            HashTableEntry::Vacant(v) => {
                v.insert(KeptPiece {
                    piece: piece.clone(),
                    generation,
                });
            }
        }

        PieceRef {
            piece,
            shard: Some(shard),
            generation,
        }
    }

    pub fn get<Q>(&self, hash: u64, key: &Q) -> Option<Piece<K, V, P>>
    where
        Q: Hash + equivalent::Equivalent<K> + ?Sized,
    {
        let shard = self.shard(hash);
        let shard = shard.read();
        shard
            .find(hash, |p| key.equivalent(p.piece.key()))
            .map(|p| p.piece.clone())
    }

    fn shard(&self, hash: u64) -> Arc<RwLock<Shard<K, V, P>>> {
        let index = (hash as usize) % self.inner.shards.len();
        self.inner.shards[index].clone()
    }
}

/// A retained cache piece submitted to a disk engine.
///
/// Dropping this reference removes the piece from the pending-write keeper.
pub struct PieceRef<K, V, P>
where
    K: StorageKey,
{
    piece: Piece<K, V, P>,
    // TODO(MrCroxx): Remove `Option`?
    shard: Option<Arc<RwLock<Shard<K, V, P>>>>,
    generation: u64,
}

impl<K, V, P> Debug for PieceRef<K, V, P>
where
    K: StorageKey,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PieceRef").field("piece", &self.piece).finish()
    }
}

impl<K, V, P> Deref for PieceRef<K, V, P>
where
    K: StorageKey,
{
    type Target = Piece<K, V, P>;

    fn deref(&self) -> &Self::Target {
        &self.piece
    }
}

impl<K, V, P> From<Piece<K, V, P>> for PieceRef<K, V, P>
where
    K: StorageKey,
{
    fn from(piece: Piece<K, V, P>) -> Self {
        PieceRef {
            piece,
            shard: None,
            generation: 0,
        }
    }
}

impl<K, V, P> Drop for PieceRef<K, V, P>
where
    K: StorageKey,
{
    fn drop(&mut self) {
        if let Some(shard) = self.shard.take() {
            let mut shard = shard.write();
            match shard.entry(self.hash(), |p| self.key() == p.piece.key(), |p| p.piece.hash()) {
                HashTableEntry::Occupied(o) if o.get().generation == self.generation => {
                    o.remove();
                }
                HashTableEntry::Occupied(_) | HashTableEntry::Vacant(_) => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use foyer_memory::{Cache, CacheBuilder};

    use super::*;

    #[test]
    fn older_reference_does_not_remove_a_newer_value() {
        let memory: Cache<u64, u64> = CacheBuilder::new(16).build();
        let keeper = Keeper::new(memory.shards());

        let older = memory.insert(7, 11).piece();
        let hash = older.hash();
        let older = keeper.insert(older);
        let newer = keeper.insert(memory.insert(7, 22).piece());

        drop(older);
        assert_eq!(*keeper.get(hash, &7).unwrap().value(), 22);

        drop(newer);
        assert!(keeper.get(hash, &7).is_none());
    }

    #[test]
    fn repeated_submission_has_an_independent_lifetime() {
        let memory: Cache<u64, u64> = CacheBuilder::new(16).build();
        let keeper = Keeper::new(memory.shards());
        let piece = memory.insert(7, 11).piece();
        let hash = piece.hash();

        let first = keeper.insert(piece.clone());
        let second = keeper.insert(piece);
        drop(first);
        assert!(keeper.get(hash, &7).is_some());

        drop(second);
        assert!(keeper.get(hash, &7).is_none());
    }
}
