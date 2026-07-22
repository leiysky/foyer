use twox_hash::XxHash3_64;

use crate::format::Key;

pub const FILTER_BYTES: usize = 224;
const FILTER_BITS: u64 = (FILTER_BYTES * 8) as u64;
const HASHES: u64 = 10;
const PRIMARY_SEED: u64 = 0x243f_6a88_85a3_08d3;
const SECONDARY_SEED: u64 = 0x1319_8a2e_0370_7344;

pub fn build<'a>(keys: impl IntoIterator<Item = &'a Key>) -> [u8; FILTER_BYTES] {
    let mut filter = [0; FILTER_BYTES];
    for key in keys {
        insert(&mut filter, key);
    }
    filter
}

pub fn may_contain(filter: &[u8], key: &Key) -> bool {
    debug_assert_eq!(filter.len(), FILTER_BYTES);
    let (mut hash, delta) = hashes(key);
    for _ in 0..HASHES {
        let bit = hash % FILTER_BITS;
        if filter[bit as usize / 8] & (1 << (bit % 8)) == 0 {
            return false;
        }
        hash = hash.wrapping_add(delta);
    }
    true
}

fn insert(filter: &mut [u8; FILTER_BYTES], key: &Key) {
    let (mut hash, delta) = hashes(key);
    for _ in 0..HASHES {
        let bit = hash % FILTER_BITS;
        filter[bit as usize / 8] |= 1 << (bit % 8);
        hash = hash.wrapping_add(delta);
    }
}

fn hashes(key: &Key) -> (u64, u64) {
    let primary = XxHash3_64::oneshot_with_seed(PRIMARY_SEED, key);
    let secondary = XxHash3_64::oneshot_with_seed(SECONDARY_SEED, key) | 1;
    (primary, secondary)
}

#[cfg(test)]
mod tests {
    use crate::{
        bloom::{build, may_contain},
        format::RECORDS_PER_BLOCK,
    };

    #[test]
    fn inserted_keys_are_never_absent() {
        let keys = (0..RECORDS_PER_BLOCK)
            .map(|index| {
                let mut key = [0; 24];
                key[..8].copy_from_slice(&(index as u64).to_le_bytes());
                key
            })
            .collect::<Vec<_>>();
        let filter = build(&keys);
        for key in &keys {
            assert!(may_contain(&filter, key));
        }
    }
}
