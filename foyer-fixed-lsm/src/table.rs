use std::{
    fs,
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
};

use crc_fast::{CrcAlgorithm, Digest};

use crate::{
    bloom,
    bloom::FILTER_BYTES,
    cache::{BlockCache, CacheKey, CacheKind, PinnedMetadata},
    error::{Error, Result},
    format::{
        DATA_BLOCK_HEADER_SIZE, DATA_BLOCK_SIZE, KEY_SIZE, Key, MAX_SEQUENCE, RECORD_SIZE, RECORDS_PER_BLOCK, Record,
        TABLE_FOOTER_SIZE, checksum, get_u32, get_u64, put_u32, put_u64, read_exact_at, sync_directory,
    },
};

const DATA_BLOCK_MAGIC: [u8; 8] = *b"FXLSMB07";
const FENCE_PAGE_MAGIC: [u8; 8] = *b"FXLSMX07";
const FILTER_PAGE_MAGIC: [u8; 8] = *b"FXLSMF07";
const TABLE_FOOTER_MAGIC: [u8; 8] = *b"FXLSMT07";
const FORMAT_VERSION: u32 = 7;
const BLOCK_CHECKSUM_OFFSET: usize = 60;
const FOOTER_CHECKSUM_OFFSET: usize = TABLE_FOOTER_SIZE - size_of::<u32>();
const FENCE_PAGE_SIZE: usize = DATA_BLOCK_SIZE;
const FENCE_PAGE_HEADER_SIZE: usize = 64;
const FENCES_PER_PAGE: usize = (FENCE_PAGE_SIZE - FENCE_PAGE_HEADER_SIZE) / KEY_SIZE;
const FENCE_PAGE_CHECKSUM_OFFSET: usize = 60;
const FILTER_PAGE_SIZE: usize = DATA_BLOCK_SIZE;
const FILTER_PAGE_HEADER_SIZE: usize = 64;
const FILTERS_PER_PAGE: usize = (FILTER_PAGE_SIZE - FILTER_PAGE_HEADER_SIZE) / FILTER_BYTES;
const FILTER_PAGE_CHECKSUM_OFFSET: usize = 60;
const ALIGNMENT: u64 = 4 * 1024;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TableIoStats {
    pub read_operations: u64,
    pub read_bytes: u64,
    pub write_operations: u64,
    pub write_bytes: u64,
    pub point_data_reads: u64,
    pub point_false_positives: u64,
}

#[derive(Debug, Default)]
pub struct TableIoCounters {
    read_operations: AtomicU64,
    read_bytes: AtomicU64,
    write_operations: AtomicU64,
    write_bytes: AtomicU64,
    point_data_reads: AtomicU64,
    point_false_positives: AtomicU64,
}

impl TableIoCounters {
    pub fn snapshot(&self) -> TableIoStats {
        TableIoStats {
            read_operations: self.read_operations.load(Ordering::Relaxed),
            read_bytes: self.read_bytes.load(Ordering::Relaxed),
            write_operations: self.write_operations.load(Ordering::Relaxed),
            write_bytes: self.write_bytes.load(Ordering::Relaxed),
            point_data_reads: self.point_data_reads.load(Ordering::Relaxed),
            point_false_positives: self.point_false_positives.load(Ordering::Relaxed),
        }
    }

    fn record_read(&self, bytes: usize) {
        self.read_operations.fetch_add(1, Ordering::Relaxed);
        self.read_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    fn record_write(&self, bytes: u64) {
        self.write_operations.fetch_add(1, Ordering::Relaxed);
        self.write_bytes.fetch_add(bytes, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableMeta {
    pub file_id: u64,
    pub level: u32,
    pub file_size: u64,
    pub record_count: u64,
    pub tombstone_count: u64,
    pub block_count: u32,
    pub min_sequence: u64,
    pub max_sequence: u64,
    pub smallest: Key,
    pub largest: Key,
}

#[derive(Debug)]
pub struct Table {
    path: PathBuf,
    file: Arc<File>,
    meta: TableMeta,
    top_fences: Arc<[Key]>,
    fence_pages: Arc<[OnceLock<Box<[Key]>>]>,
    filter_pages: Arc<[OnceLock<PinnedMetadata>]>,
    fences_offset: u64,
    filters_offset: u64,
    cache: Arc<BlockCache>,
    io: Arc<TableIoCounters>,
}

impl Table {
    pub fn create(
        directory: &Path,
        file_id: u64,
        level: u32,
        records: &[Record],
        cache: Arc<BlockCache>,
        io: Arc<TableIoCounters>,
    ) -> Result<Arc<Self>> {
        validate_records(directory, records)?;
        let final_path = table_path(directory, file_id);
        let temporary_path = temporary_table_path(directory, file_id);
        let result = write_table(&temporary_path, file_id, level, records);
        let written = match result {
            Ok(written) => written,
            Err(error) => {
                let _ = fs::remove_file(&temporary_path);
                return Err(error);
            }
        };
        fs::rename(&temporary_path, &final_path).map_err(|error| Error::io("publish SST file", error))?;
        sync_directory(directory)?;
        io.record_write(written.meta.file_size);
        let file = File::open(&final_path).map_err(|error| Error::io("open new SST file", error))?;
        let fence_pages = fence_page_slots(written.top_fences.len());
        let filter_pages = filter_page_slots(written.meta.block_count);
        Ok(Arc::new(Self {
            path: final_path,
            file: Arc::new(file),
            meta: written.meta,
            top_fences: written.top_fences.into(),
            fence_pages,
            filter_pages,
            fences_offset: written.fences_offset,
            filters_offset: written.filters_offset,
            cache,
            io,
        }))
    }

    pub fn open(
        directory: &Path,
        file_id: u64,
        level: u32,
        cache: Arc<BlockCache>,
        io: Arc<TableIoCounters>,
    ) -> Result<Arc<Self>> {
        let path = table_path(directory, file_id);
        let file = File::open(&path).map_err(|error| Error::io("open SST file", error))?;
        let file_size = file
            .metadata()
            .map_err(|error| Error::io("stat SST file", error))?
            .len();
        if file_size < TABLE_FOOTER_SIZE as u64 {
            return Err(Error::corruption(&path, "file is shorter than its footer"));
        }
        let mut footer = [0; TABLE_FOOTER_SIZE];
        read_exact_at(&file, &mut footer, file_size - TABLE_FOOTER_SIZE as u64)
            .map_err(|error| Error::io("read SST footer", error))?;
        io.record_read(TABLE_FOOTER_SIZE);
        let decoded = decode_footer(&path, &footer, file_size, file_id, level)?;

        let mut top_fence_bytes = vec![0; decoded.top_fences_len as usize];
        read_exact_at(&file, &mut top_fence_bytes, decoded.top_fences_offset)
            .map_err(|error| Error::io("read SST top-level fence index", error))?;
        io.record_read(top_fence_bytes.len());
        if checksum(&top_fence_bytes) != decoded.top_fences_checksum {
            return Err(Error::corruption(&path, "top-level fence index checksum mismatch"));
        }
        let top_fences = decode_top_fences(&path, &top_fence_bytes, decoded.meta)?;
        let fence_pages = fence_page_slots(top_fences.len());
        let filter_pages = filter_page_slots(decoded.meta.block_count);
        Ok(Arc::new(Self {
            path,
            file: Arc::new(file),
            meta: decoded.meta,
            top_fences: top_fences.into(),
            fence_pages,
            filter_pages,
            fences_offset: decoded.fences_offset,
            filters_offset: decoded.filters_offset,
            cache,
            io,
        }))
    }

    pub fn meta(&self) -> TableMeta {
        self.meta
    }

    pub fn at_level(&self, level: u32) -> Arc<Self> {
        let mut meta = self.meta;
        meta.level = level;
        Arc::new(Self {
            path: self.path.clone(),
            file: self.file.clone(),
            meta,
            top_fences: self.top_fences.clone(),
            fence_pages: self.fence_pages.clone(),
            filter_pages: self.filter_pages.clone(),
            fences_offset: self.fences_offset,
            filters_offset: self.filters_offset,
            cache: self.cache.clone(),
            io: self.io.clone(),
        })
    }

    pub fn contains_range(&self, key: &Key) -> bool {
        self.meta.smallest <= *key && *key <= self.meta.largest
    }

    pub fn get(&self, key: &Key) -> Result<Option<Record>> {
        if !self.contains_range(key) {
            return Ok(None);
        }
        let (block, fence) = self.data_block_for(key)?;
        let data_key = CacheKey {
            file_id: self.meta.file_id,
            block,
            kind: CacheKind::Data,
        };
        let filter_first = self.meta.level == 0;
        if filter_first && !self.filter_may_contain(block, key)? {
            return Ok(None);
        }
        if let Some(record) = self.cache.get_with(data_key, |data| find_record(&self.path, data, key)) {
            let record = record?;
            if filter_first && record.is_none() {
                self.io.point_false_positives.fetch_add(1, Ordering::Relaxed);
            }
            return Ok(record);
        }
        if !filter_first && !self.filter_may_contain(block, key)? {
            return Ok(None);
        }
        let data = self.read_data_block(block, Some(fence))?;
        self.io.point_data_reads.fetch_add(1, Ordering::Relaxed);
        let data = self.cache.insert(data_key, data);
        let record = find_record(&self.path, &data, key)?;
        if record.is_none() {
            self.io.point_false_positives.fetch_add(1, Ordering::Relaxed);
        }
        Ok(record)
    }

    pub fn iterator(self: &Arc<Self>) -> TableIterator {
        TableIterator {
            table: self.clone(),
            block: 0,
            slot: 0,
            current: None,
        }
    }

    fn data_block_for(&self, key: &Key) -> Result<(u32, Key)> {
        let page = self.top_fences.partition_point(|fence| fence < key);
        if page >= self.top_fences.len() {
            return Err(Error::corruption(
                &self.path,
                "top-level fence index does not cover the table key range",
            ));
        }
        let page = u32::try_from(page).unwrap();
        let slot = &self.fence_pages[page as usize];
        if slot.get().is_none() {
            let mut fence_page = vec![0; FENCE_PAGE_SIZE];
            let offset = self.fences_offset + u64::from(page) * FENCE_PAGE_SIZE as u64;
            read_exact_at(&self.file, &mut fence_page, offset)
                .map_err(|error| Error::io("read SST fence page", error))?;
            self.io.record_read(fence_page.len());
            validate_fence_page(&self.path, &fence_page, self.meta, page, self.top_fences[page as usize])?;
            let _ = slot.set(decode_fence_page(&fence_page));
        }
        let fences = slot.get().unwrap();
        let block = fences.partition_point(|fence| fence < key);
        if block == fences.len() {
            return Err(Error::corruption(
                &self.path,
                format!("SST fence page {page} does not cover its key range"),
            ));
        }
        let fence = fences[block];
        let block = page as usize * FENCES_PER_PAGE + block;
        Ok((u32::try_from(block).unwrap(), fence))
    }

    fn filter_may_contain(&self, block: u32, key: &Key) -> Result<bool> {
        let page = block / FILTERS_PER_PAGE as u32;
        let slot = block as usize % FILTERS_PER_PAGE;
        let cache_key = CacheKey {
            file_id: self.meta.file_id,
            block: page,
            kind: CacheKind::Filter,
        };
        let page_slot = &self.filter_pages[page as usize];
        if let Some(filter_page) = page_slot.get() {
            let offset = FILTER_PAGE_HEADER_SIZE + slot * FILTER_BYTES;
            let may_contain = bloom::may_contain(&filter_page.data()[offset..offset + FILTER_BYTES], key);
            self.cache.record_pinned_filter(cache_key, may_contain);
            return Ok(may_contain);
        }
        if let Some(may_contain) = self.cache.get_filter_with(cache_key, |filter_page| {
            let offset = FILTER_PAGE_HEADER_SIZE + slot * FILTER_BYTES;
            bloom::may_contain(&filter_page[offset..offset + FILTER_BYTES], key)
        }) {
            return Ok(may_contain);
        }
        let mut filter_page = vec![0; FILTER_PAGE_SIZE];
        let offset = self.filters_offset + u64::from(page) * FILTER_PAGE_SIZE as u64;
        read_exact_at(&self.file, &mut filter_page, offset).map_err(|error| Error::io("read SST Bloom page", error))?;
        self.io.record_read(filter_page.len());
        validate_filter_page(&self.path, &filter_page, self.meta, page)?;
        let filter_page = match self.cache.pin_metadata(filter_page.into_boxed_slice()) {
            Ok(filter_page) => {
                if let Err(filter_page) = page_slot.set(filter_page) {
                    drop(filter_page);
                }
                let filter_page = page_slot.get().unwrap();
                let offset = FILTER_PAGE_HEADER_SIZE + slot * FILTER_BYTES;
                let may_contain = bloom::may_contain(&filter_page.data()[offset..offset + FILTER_BYTES], key);
                self.cache.record_pinned_filter(cache_key, may_contain);
                return Ok(may_contain);
            }
            Err(filter_page) => self.cache.insert(cache_key, filter_page.into()),
        };
        let offset = FILTER_PAGE_HEADER_SIZE + slot * FILTER_BYTES;
        let may_contain = bloom::may_contain(&filter_page[offset..offset + FILTER_BYTES], key);
        self.cache.record_filter_result(cache_key, may_contain);
        Ok(may_contain)
    }

    fn read_data_block(&self, block: u32, fence: Option<Key>) -> Result<Arc<[u8]>> {
        let mut data = vec![0; DATA_BLOCK_SIZE];
        read_exact_at(&self.file, &mut data, u64::from(block) * DATA_BLOCK_SIZE as u64)
            .map_err(|error| Error::io("read SST data block", error))?;
        self.io.record_read(data.len());
        validate_data_block_header(&self.path, &data, self.meta, block, fence)?;
        Ok(data.into())
    }
}

#[derive(Debug)]
pub struct TableIterator {
    table: Arc<Table>,
    block: u32,
    slot: usize,
    current: Option<Arc<[u8]>>,
}

impl TableIterator {
    pub fn next_record(&mut self) -> Result<Option<Record>> {
        loop {
            if self.block >= self.table.meta.block_count {
                return Ok(None);
            }
            if self.current.is_none() {
                let data = self.table.read_data_block(self.block, None)?;
                validate_data_block_records(&self.table.path, &data, self.table.meta, self.block)?;
                self.current = Some(data);
                self.slot = 0;
            }
            let data = self.current.as_ref().unwrap();
            let count = get_u32(data, 12) as usize;
            if self.slot < count {
                let offset = DATA_BLOCK_HEADER_SIZE + self.slot * RECORD_SIZE;
                self.slot += 1;
                return Record::decode(&data[offset..offset + RECORD_SIZE])
                    .map(Some)
                    .ok_or_else(|| Error::corruption(&self.table.path, "invalid SST record"));
            }
            self.block += 1;
            self.current = None;
        }
    }
}

struct WrittenTable {
    meta: TableMeta,
    top_fences: Vec<Key>,
    fences_offset: u64,
    filters_offset: u64,
}

struct DecodedFooter {
    meta: TableMeta,
    fences_offset: u64,
    top_fences_offset: u64,
    top_fences_len: u64,
    filters_offset: u64,
    top_fences_checksum: u32,
}

pub fn table_file_size(record_count: usize) -> Option<u64> {
    if record_count == 0 {
        return None;
    }
    let block_count = record_count.div_ceil(RECORDS_PER_BLOCK);
    let data_bytes = u64::try_from(block_count).ok()?.checked_mul(DATA_BLOCK_SIZE as u64)?;
    let fence_page_count = block_count.div_ceil(FENCES_PER_PAGE);
    let fence_page_bytes = u64::try_from(fence_page_count)
        .ok()?
        .checked_mul(FENCE_PAGE_SIZE as u64)?;
    let top_fence_bytes = u64::try_from(fence_page_count).ok()?.checked_mul(KEY_SIZE as u64)?;
    let filters_offset = checked_align_up(
        data_bytes.checked_add(fence_page_bytes)?.checked_add(top_fence_bytes)?,
        FILTER_PAGE_SIZE as u64,
    )?;
    let filter_page_count = block_count.div_ceil(FILTERS_PER_PAGE);
    let filter_page_bytes = u64::try_from(filter_page_count)
        .ok()?
        .checked_mul(FILTER_PAGE_SIZE as u64)?;
    checked_align_up(filters_offset.checked_add(filter_page_bytes)?, ALIGNMENT)?.checked_add(TABLE_FOOTER_SIZE as u64)
}

fn write_table(path: &Path, file_id: u64, level: u32, records: &[Record]) -> Result<WrittenTable> {
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|error| Error::io("create temporary SST file", error))?;
    let mut writer = BufWriter::with_capacity(1024 * 1024, file);
    let mut fences = Vec::with_capacity(records.len().div_ceil(RECORDS_PER_BLOCK));
    let mut filters = Vec::with_capacity(fences.capacity() * FILTER_BYTES);
    let mut min_sequence = MAX_SEQUENCE;
    let mut max_sequence = 0;

    for (block_index, block_records) in records.chunks(RECORDS_PER_BLOCK).enumerate() {
        let mut block = [0; DATA_BLOCK_SIZE];
        block[..8].copy_from_slice(&DATA_BLOCK_MAGIC);
        put_u32(&mut block, 8, FORMAT_VERSION);
        put_u32(&mut block, 12, block_records.len() as u32);
        put_u64(&mut block, 16, file_id);
        put_u32(&mut block, 24, block_index as u32);
        let mut block_min_sequence = MAX_SEQUENCE;
        let mut block_max_sequence = 0;
        for (slot, record) in block_records.iter().enumerate() {
            let offset = DATA_BLOCK_HEADER_SIZE + slot * RECORD_SIZE;
            block[offset..offset + RECORD_SIZE].copy_from_slice(&record.encode());
            block_min_sequence = block_min_sequence.min(record.sequence);
            block_max_sequence = block_max_sequence.max(record.sequence);
        }
        put_u64(&mut block, 32, block_min_sequence);
        put_u64(&mut block, 40, block_max_sequence);
        let block_checksum = data_block_checksum(&block);
        put_u32(&mut block, BLOCK_CHECKSUM_OFFSET, block_checksum);
        writer
            .write_all(&block)
            .map_err(|error| Error::io("write SST data block", error))?;
        fences.push(block_records.last().unwrap().key);
        let filter = bloom::build(block_records.iter().map(|record| &record.key));
        filters.extend_from_slice(&filter);
        min_sequence = min_sequence.min(block_min_sequence);
        max_sequence = max_sequence.max(block_max_sequence);
    }

    let fences_offset = records.len().div_ceil(RECORDS_PER_BLOCK) as u64 * DATA_BLOCK_SIZE as u64;
    let (fence_pages, top_fences) = encode_fence_pages(file_id, &fences);
    writer
        .write_all(&fence_pages)
        .map_err(|error| Error::io("write SST fence pages", error))?;
    let top_fences_offset = fences_offset + fence_pages.len() as u64;
    let top_fence_bytes = encode_keys(&top_fences);
    writer
        .write_all(&top_fence_bytes)
        .map_err(|error| Error::io("write SST top-level fence index", error))?;
    let filters_offset = align_up(
        top_fences_offset + top_fence_bytes.len() as u64,
        FILTER_PAGE_SIZE as u64,
    );
    write_padding(
        &mut writer,
        filters_offset - top_fences_offset - top_fence_bytes.len() as u64,
    )?;
    let filter_pages = encode_filter_pages(file_id, &filters);
    writer
        .write_all(&filter_pages)
        .map_err(|error| Error::io("write SST Bloom pages", error))?;
    let footer_offset = align_up(filters_offset + filter_pages.len() as u64, ALIGNMENT);
    write_padding(&mut writer, footer_offset - filters_offset - filter_pages.len() as u64)?;
    let file_size = footer_offset + TABLE_FOOTER_SIZE as u64;
    debug_assert_eq!(table_file_size(records.len()), Some(file_size));
    let meta = TableMeta {
        file_id,
        level,
        file_size,
        record_count: records.len() as u64,
        tombstone_count: records.iter().filter(|record| record.value.is_none()).count() as u64,
        block_count: fences.len() as u32,
        min_sequence,
        max_sequence,
        smallest: records.first().unwrap().key,
        largest: records.last().unwrap().key,
    };
    let footer = encode_footer(
        meta,
        fences_offset,
        fence_pages.len() as u64,
        top_fences_offset,
        top_fence_bytes.len() as u64,
        filters_offset,
        filter_pages.len() as u64,
        checksum(&top_fence_bytes),
    );
    writer
        .write_all(&footer)
        .and_then(|_| writer.flush())
        .map_err(|error| Error::io("finish SST file", error))?;
    let file = writer
        .into_inner()
        .map_err(|error| Error::io("finish SST buffer", error.into_error()))?;
    file.sync_data().map_err(|error| Error::io("sync SST file", error))?;
    Ok(WrittenTable {
        meta,
        top_fences,
        fences_offset,
        filters_offset,
    })
}

fn encode_footer(
    meta: TableMeta,
    fences_offset: u64,
    fences_len: u64,
    top_fences_offset: u64,
    top_fences_len: u64,
    filters_offset: u64,
    filters_len: u64,
    top_fences_checksum: u32,
) -> [u8; TABLE_FOOTER_SIZE] {
    let mut footer = [0; TABLE_FOOTER_SIZE];
    footer[..8].copy_from_slice(&TABLE_FOOTER_MAGIC);
    put_u32(&mut footer, 8, FORMAT_VERSION);
    put_u32(&mut footer, 12, TABLE_FOOTER_SIZE as u32);
    put_u32(&mut footer, 16, DATA_BLOCK_SIZE as u32);
    put_u32(&mut footer, 20, RECORDS_PER_BLOCK as u32);
    put_u64(&mut footer, 24, meta.file_id);
    put_u32(&mut footer, 32, meta.level);
    put_u32(&mut footer, 36, meta.block_count);
    put_u64(&mut footer, 40, meta.record_count);
    put_u64(&mut footer, 48, meta.min_sequence);
    put_u64(&mut footer, 56, meta.max_sequence);
    footer[64..64 + KEY_SIZE].copy_from_slice(&meta.smallest);
    footer[88..88 + KEY_SIZE].copy_from_slice(&meta.largest);
    put_u64(&mut footer, 112, fences_offset);
    put_u64(&mut footer, 120, fences_len);
    put_u64(&mut footer, 128, top_fences_offset);
    put_u64(&mut footer, 136, top_fences_len);
    put_u64(&mut footer, 144, filters_offset);
    put_u64(&mut footer, 152, filters_len);
    put_u64(&mut footer, 160, meta.file_size);
    put_u32(&mut footer, 168, top_fences_checksum);
    put_u64(&mut footer, 176, meta.tombstone_count);
    let footer_checksum = checksum(&footer[..FOOTER_CHECKSUM_OFFSET]);
    put_u32(&mut footer, FOOTER_CHECKSUM_OFFSET, footer_checksum);
    footer
}

fn decode_footer(
    path: &Path,
    footer: &[u8; TABLE_FOOTER_SIZE],
    file_size: u64,
    file_id: u64,
    level: u32,
) -> Result<DecodedFooter> {
    if footer[..8] != TABLE_FOOTER_MAGIC
        || get_u32(footer, 8) != FORMAT_VERSION
        || get_u32(footer, 12) != TABLE_FOOTER_SIZE as u32
        || get_u32(footer, 16) != DATA_BLOCK_SIZE as u32
        || get_u32(footer, 20) != RECORDS_PER_BLOCK as u32
        || get_u64(footer, 24) != file_id
        || get_u64(footer, 160) != file_size
        || checksum(&footer[..FOOTER_CHECKSUM_OFFSET]) != get_u32(footer, FOOTER_CHECKSUM_OFFSET)
    {
        return Err(Error::corruption(path, "invalid SST footer"));
    }
    let block_count = get_u32(footer, 36);
    let record_count = get_u64(footer, 40);
    let min_sequence = get_u64(footer, 48);
    let max_sequence = get_u64(footer, 56);
    let tombstone_count = get_u64(footer, 176);
    let fences_offset = get_u64(footer, 112);
    let fences_len = get_u64(footer, 120);
    let top_fences_offset = get_u64(footer, 128);
    let top_fences_len = get_u64(footer, 136);
    let filters_offset = get_u64(footer, 144);
    let filters_len = get_u64(footer, 152);
    let expected_blocks = record_count.div_ceil(RECORDS_PER_BLOCK as u64);
    let expected_fence_pages = u64::from(block_count).div_ceil(FENCES_PER_PAGE as u64);
    let expected_footer_offset = align_up(filters_offset + filters_len, ALIGNMENT);
    if record_count == 0
        || u64::from(block_count) != expected_blocks
        || min_sequence == 0
        || min_sequence > max_sequence
        || max_sequence > MAX_SEQUENCE
        || tombstone_count > record_count
        || fences_offset != u64::from(block_count) * DATA_BLOCK_SIZE as u64
        || fences_len != expected_fence_pages * FENCE_PAGE_SIZE as u64
        || top_fences_offset != fences_offset + fences_len
        || top_fences_len != expected_fence_pages * KEY_SIZE as u64
        || filters_offset != align_up(top_fences_offset + top_fences_len, FILTER_PAGE_SIZE as u64)
        || filters_len != u64::from(block_count).div_ceil(FILTERS_PER_PAGE as u64) * FILTER_PAGE_SIZE as u64
        || expected_footer_offset + TABLE_FOOTER_SIZE as u64 != file_size
    {
        return Err(Error::corruption(path, "inconsistent SST footer layout"));
    }
    let mut smallest = [0; KEY_SIZE];
    smallest.copy_from_slice(&footer[64..64 + KEY_SIZE]);
    let mut largest = [0; KEY_SIZE];
    largest.copy_from_slice(&footer[88..88 + KEY_SIZE]);
    if smallest > largest {
        return Err(Error::corruption(path, "inverted SST key range"));
    }
    Ok(DecodedFooter {
        meta: TableMeta {
            file_id,
            level,
            file_size,
            record_count,
            tombstone_count,
            block_count,
            min_sequence,
            max_sequence,
            smallest,
            largest,
        },
        fences_offset,
        top_fences_offset,
        top_fences_len,
        filters_offset,
        top_fences_checksum: get_u32(footer, 168),
    })
}

fn encode_keys(keys: &[Key]) -> Vec<u8> {
    let mut output = Vec::with_capacity(keys.len() * KEY_SIZE);
    for key in keys {
        output.extend_from_slice(key);
    }
    output
}

fn encode_fence_pages(file_id: u64, fences: &[Key]) -> (Vec<u8>, Vec<Key>) {
    let page_count = fences.len().div_ceil(FENCES_PER_PAGE);
    let mut pages = vec![0; page_count * FENCE_PAGE_SIZE];
    let mut top_fences = Vec::with_capacity(page_count);
    for (page_index, page) in pages.chunks_exact_mut(FENCE_PAGE_SIZE).enumerate() {
        let first = page_index * FENCES_PER_PAGE;
        let count = (fences.len() - first).min(FENCES_PER_PAGE);
        page[..8].copy_from_slice(&FENCE_PAGE_MAGIC);
        put_u32(page, 8, FORMAT_VERSION);
        put_u32(page, 12, count as u32);
        put_u64(page, 16, file_id);
        put_u32(page, 24, page_index as u32);
        let keys = encode_keys(&fences[first..first + count]);
        page[FENCE_PAGE_HEADER_SIZE..FENCE_PAGE_HEADER_SIZE + keys.len()].copy_from_slice(&keys);
        let page_checksum = fence_page_checksum(page);
        put_u32(page, FENCE_PAGE_CHECKSUM_OFFSET, page_checksum);
        top_fences.push(fences[first + count - 1]);
    }
    (pages, top_fences)
}

fn decode_top_fences(path: &Path, input: &[u8], meta: TableMeta) -> Result<Vec<Key>> {
    let expected = (meta.block_count as usize).div_ceil(FENCES_PER_PAGE);
    let mut fences = Vec::with_capacity(expected);
    for bytes in input.chunks_exact(KEY_SIZE) {
        let mut key = [0; KEY_SIZE];
        key.copy_from_slice(bytes);
        if fences.last().is_some_and(|previous| previous >= &key) {
            return Err(Error::corruption(path, "SST fences are not strictly ordered"));
        }
        fences.push(key);
    }
    if fences.len() != expected || fences.last() != Some(&meta.largest) {
        return Err(Error::corruption(path, "SST top-level fences do not match its footer"));
    }
    Ok(fences)
}

fn validate_fence_page(path: &Path, page: &[u8], meta: TableMeta, page_index: u32, top_fence: Key) -> Result<()> {
    let first = page_index as usize * FENCES_PER_PAGE;
    let expected_count = (meta.block_count as usize - first).min(FENCES_PER_PAGE);
    let mut previous = None;
    if page.len() == FENCE_PAGE_SIZE {
        for slot in 0..expected_count {
            let offset = FENCE_PAGE_HEADER_SIZE + slot * KEY_SIZE;
            let mut key = [0; KEY_SIZE];
            key.copy_from_slice(&page[offset..offset + KEY_SIZE]);
            if previous.is_some_and(|previous| previous >= key) {
                return Err(Error::corruption(
                    path,
                    format!("unordered SST fence page {page_index}"),
                ));
            }
            previous = Some(key);
        }
    }
    if page.len() != FENCE_PAGE_SIZE
        || page[..8] != FENCE_PAGE_MAGIC
        || get_u32(page, 8) != FORMAT_VERSION
        || get_u32(page, 12) as usize != expected_count
        || get_u64(page, 16) != meta.file_id
        || get_u32(page, 24) != page_index
        || previous != Some(top_fence)
        || fence_page_checksum(page) != get_u32(page, FENCE_PAGE_CHECKSUM_OFFSET)
    {
        return Err(Error::corruption(path, format!("invalid SST fence page {page_index}")));
    }
    Ok(())
}

fn decode_fence_page(page: &[u8]) -> Box<[Key]> {
    let count = get_u32(page, 12) as usize;
    (0..count)
        .map(|slot| {
            let offset = FENCE_PAGE_HEADER_SIZE + slot * KEY_SIZE;
            let mut key = [0; KEY_SIZE];
            key.copy_from_slice(&page[offset..offset + KEY_SIZE]);
            key
        })
        .collect::<Vec<_>>()
        .into_boxed_slice()
}

fn encode_filter_pages(file_id: u64, filters: &[u8]) -> Vec<u8> {
    debug_assert_eq!(filters.len() % FILTER_BYTES, 0);
    let filter_count = filters.len() / FILTER_BYTES;
    let mut pages = vec![0; filter_count.div_ceil(FILTERS_PER_PAGE) * FILTER_PAGE_SIZE];
    for (page_index, page) in pages.chunks_exact_mut(FILTER_PAGE_SIZE).enumerate() {
        let first = page_index * FILTERS_PER_PAGE;
        let count = (filter_count - first).min(FILTERS_PER_PAGE);
        page[..8].copy_from_slice(&FILTER_PAGE_MAGIC);
        put_u32(page, 8, FORMAT_VERSION);
        put_u32(page, 12, count as u32);
        put_u64(page, 16, file_id);
        put_u32(page, 24, page_index as u32);
        let source = &filters[first * FILTER_BYTES..(first + count) * FILTER_BYTES];
        page[FILTER_PAGE_HEADER_SIZE..FILTER_PAGE_HEADER_SIZE + source.len()].copy_from_slice(source);
        let page_checksum = filter_page_checksum(page);
        put_u32(page, FILTER_PAGE_CHECKSUM_OFFSET, page_checksum);
    }
    pages
}

fn validate_filter_page(path: &Path, page: &[u8], meta: TableMeta, page_index: u32) -> Result<()> {
    let first = page_index as usize * FILTERS_PER_PAGE;
    let expected_count = (meta.block_count as usize - first).min(FILTERS_PER_PAGE);
    if page.len() != FILTER_PAGE_SIZE
        || page[..8] != FILTER_PAGE_MAGIC
        || get_u32(page, 8) != FORMAT_VERSION
        || get_u32(page, 12) as usize != expected_count
        || get_u64(page, 16) != meta.file_id
        || get_u32(page, 24) != page_index
        || filter_page_checksum(page) != get_u32(page, FILTER_PAGE_CHECKSUM_OFFSET)
    {
        return Err(Error::corruption(path, format!("invalid SST Bloom page {page_index}")));
    }
    Ok(())
}

fn data_block_record_count(meta: TableMeta, block: u32) -> usize {
    if block + 1 == meta.block_count {
        let remainder = usize::try_from(meta.record_count % RECORDS_PER_BLOCK as u64).unwrap();
        if remainder == 0 { RECORDS_PER_BLOCK } else { remainder }
    } else {
        RECORDS_PER_BLOCK
    }
}

fn validate_data_block_header(path: &Path, data: &[u8], meta: TableMeta, block: u32, fence: Option<Key>) -> Result<()> {
    let expected_count = data_block_record_count(meta, block);
    if data.len() != DATA_BLOCK_SIZE
        || data[..8] != DATA_BLOCK_MAGIC
        || get_u32(data, 8) != FORMAT_VERSION
        || get_u32(data, 12) as usize != expected_count
        || get_u64(data, 16) != meta.file_id
        || get_u32(data, 24) != block
        || data_block_checksum(data) != get_u32(data, BLOCK_CHECKSUM_OFFSET)
    {
        return Err(Error::corruption(path, format!("invalid SST data block {block}")));
    }
    let min_sequence = get_u64(data, 32);
    let max_sequence = get_u64(data, 40);
    let last_offset = DATA_BLOCK_HEADER_SIZE + (expected_count - 1) * RECORD_SIZE;
    let last = Record::decode(&data[last_offset..last_offset + RECORD_SIZE]);
    if min_sequence == 0
        || min_sequence > max_sequence
        || max_sequence > MAX_SEQUENCE
        || last.is_none()
        || fence.is_some_and(|fence| last.is_none_or(|record| record.key != fence))
    {
        return Err(Error::corruption(
            path,
            format!("SST data block {block} metadata mismatch"),
        ));
    }
    Ok(())
}

fn validate_data_block_records(path: &Path, data: &[u8], meta: TableMeta, block: u32) -> Result<()> {
    let expected_count = data_block_record_count(meta, block);
    let mut previous = None;
    let mut min_sequence = MAX_SEQUENCE;
    let mut max_sequence = 0;
    for slot in 0..expected_count {
        let offset = DATA_BLOCK_HEADER_SIZE + slot * RECORD_SIZE;
        let record = Record::decode(&data[offset..offset + RECORD_SIZE])
            .ok_or_else(|| Error::corruption(path, format!("invalid record in block {block}")))?;
        if previous.is_some_and(|key| key >= record.key) {
            return Err(Error::corruption(
                path,
                format!("records in block {block} are not strictly ordered"),
            ));
        }
        previous = Some(record.key);
        min_sequence = min_sequence.min(record.sequence);
        max_sequence = max_sequence.max(record.sequence);
    }
    if min_sequence != get_u64(data, 32) || max_sequence != get_u64(data, 40) {
        return Err(Error::corruption(path, format!("data block {block} metadata mismatch")));
    }
    Ok(())
}

fn find_record(path: &Path, data: &[u8], key: &Key) -> Result<Option<Record>> {
    let count = get_u32(data, 12) as usize;
    let mut low = 0;
    let mut high = count;
    while low < high {
        let middle = (low + high) / 2;
        let offset = DATA_BLOCK_HEADER_SIZE + middle * RECORD_SIZE;
        match data[offset..offset + KEY_SIZE].cmp(key) {
            std::cmp::Ordering::Less => low = middle + 1,
            std::cmp::Ordering::Greater => high = middle,
            std::cmp::Ordering::Equal => {
                return Record::decode(&data[offset..offset + RECORD_SIZE])
                    .map(Some)
                    .ok_or_else(|| Error::corruption(path, "invalid cached SST record"));
            }
        }
    }
    Ok(None)
}

fn validate_records(path: &Path, records: &[Record]) -> Result<()> {
    if records.is_empty() {
        return Err(Error::corruption(path, "cannot create an empty SST"));
    }
    let mut previous = None;
    for record in records {
        if record.sequence == 0 || record.sequence > MAX_SEQUENCE {
            return Err(Error::corruption(path, "SST record has an invalid sequence"));
        }
        if previous.is_some_and(|key| key >= record.key) {
            return Err(Error::corruption(path, "SST input records are not strictly ordered"));
        }
        previous = Some(record.key);
    }
    Ok(())
}

fn data_block_checksum(input: &[u8]) -> u32 {
    let mut digest = Digest::new(CrcAlgorithm::Crc32Iscsi);
    digest.update(&input[..BLOCK_CHECKSUM_OFFSET]);
    digest.update(&input[BLOCK_CHECKSUM_OFFSET + size_of::<u32>()..]);
    digest.finalize() as u32
}

fn fence_page_checksum(input: &[u8]) -> u32 {
    let mut digest = Digest::new(CrcAlgorithm::Crc32Iscsi);
    digest.update(&input[..FENCE_PAGE_CHECKSUM_OFFSET]);
    digest.update(&input[FENCE_PAGE_CHECKSUM_OFFSET + size_of::<u32>()..]);
    digest.finalize() as u32
}

fn filter_page_checksum(input: &[u8]) -> u32 {
    let mut digest = Digest::new(CrcAlgorithm::Crc32Iscsi);
    digest.update(&input[..FILTER_PAGE_CHECKSUM_OFFSET]);
    digest.update(&input[FILTER_PAGE_CHECKSUM_OFFSET + size_of::<u32>()..]);
    digest.finalize() as u32
}

fn write_padding(writer: &mut impl Write, bytes: u64) -> Result<()> {
    const ZEROES: [u8; 4096] = [0; 4096];
    let mut remaining = bytes;
    while remaining > 0 {
        let count = remaining.min(ZEROES.len() as u64) as usize;
        writer
            .write_all(&ZEROES[..count])
            .map_err(|error| Error::io("write SST alignment padding", error))?;
        remaining -= count as u64;
    }
    Ok(())
}

fn align_up(value: u64, alignment: u64) -> u64 {
    value.div_ceil(alignment) * alignment
}

fn checked_align_up(value: u64, alignment: u64) -> Option<u64> {
    value
        .checked_add(alignment.checked_sub(1)?)?
        .checked_div(alignment)?
        .checked_mul(alignment)
}

fn fence_page_slots(page_count: usize) -> Arc<[OnceLock<Box<[Key]>>]> {
    (0..page_count).map(|_| OnceLock::new()).collect::<Vec<_>>().into()
}

fn filter_page_slots(block_count: u32) -> Arc<[OnceLock<PinnedMetadata>]> {
    (0..(block_count as usize).div_ceil(FILTERS_PER_PAGE))
        .map(|_| OnceLock::new())
        .collect::<Vec<_>>()
        .into()
}

pub fn table_path(directory: &Path, file_id: u64) -> PathBuf {
    directory.join(format!("sst-{file_id:020}.sst"))
}

pub fn temporary_table_path(directory: &Path, file_id: u64) -> PathBuf {
    directory.join(format!("sst-{file_id:020}.sst.tmp"))
}

pub fn parse_table_file_name(name: &str) -> Option<(u64, bool)> {
    let body = name.strip_prefix("sst-")?;
    let (id, temporary) = if let Some(id) = body.strip_suffix(".sst.tmp") {
        (id, true)
    } else {
        (body.strip_suffix(".sst")?, false)
    };
    (id.len() == 20)
        .then(|| id.parse().ok().map(|id| (id, temporary)))
        .flatten()
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{File, OpenOptions},
        io,
        sync::Arc,
    };

    use crate::{
        cache::BlockCache,
        error::Error,
        format::{DATA_BLOCK_SIZE, RECORDS_PER_BLOCK, Record},
        table::{FENCE_PAGE_SIZE, FILTER_PAGE_SIZE, FILTERS_PER_PAGE, TABLE_FOOTER_SIZE, Table, TableIoCounters},
    };

    fn record(index: u64, sequence: u64) -> Record {
        let mut key = [0; 24];
        key[..8].copy_from_slice(&index.to_be_bytes());
        let mut value = [0; 32];
        value[..8].copy_from_slice(&index.to_le_bytes());
        Record::put(key, value, sequence)
    }

    #[cfg(unix)]
    fn write_all_at(file: &File, input: &[u8], offset: u64) -> io::Result<()> {
        std::os::unix::fs::FileExt::write_all_at(file, input, offset)
    }

    #[cfg(windows)]
    fn write_all_at(file: &File, mut input: &[u8], mut offset: u64) -> io::Result<()> {
        use std::os::windows::fs::FileExt;

        while !input.is_empty() {
            let written = file.seek_write(input, offset)?;
            if written == 0 {
                return Err(io::ErrorKind::WriteZero.into());
            }
            input = &input[written..];
            offset += written as u64;
        }
        Ok(())
    }

    #[test]
    fn table_roundtrips_multiple_blocks_and_reopens() {
        let directory = tempfile::tempdir().unwrap();
        let records = (0..RECORDS_PER_BLOCK as u64 * (FILTERS_PER_PAGE as u64 + 1) + 7)
            .map(|index| record(index, index + 1))
            .collect::<Vec<_>>();
        let io = Arc::new(TableIoCounters::default());
        let table = Table::create(
            directory.path(),
            7,
            0,
            &records,
            Arc::new(BlockCache::new(1024 * 1024)),
            io.clone(),
        )
        .unwrap();
        assert_eq!(table.get(&records[0].key).unwrap(), Some(records[0]));
        assert_eq!(table.get(&records[256].key).unwrap(), Some(records[256]));
        let second_filter_page = FILTERS_PER_PAGE * RECORDS_PER_BLOCK;
        assert_eq!(
            table.get(&records[second_filter_page].key).unwrap(),
            Some(records[second_filter_page])
        );
        let missing = [0xff; 24];
        assert_eq!(table.get(&missing).unwrap(), None);
        drop(table);

        let table = Table::open(directory.path(), 7, 0, Arc::new(BlockCache::new(1024 * 1024)), io).unwrap();
        let mut iterator = table.iterator();
        for expected in records {
            assert_eq!(iterator.next_record().unwrap(), Some(expected));
        }
        assert_eq!(iterator.next_record().unwrap(), None);
    }

    #[test]
    fn corrupt_filter_page_is_rejected_before_a_data_read() {
        let directory = tempfile::tempdir().unwrap();
        let records = (0..RECORDS_PER_BLOCK as u64 + 1)
            .map(|index| record(index, index + 1))
            .collect::<Vec<_>>();
        let io = Arc::new(TableIoCounters::default());
        let table = Table::create(
            directory.path(),
            9,
            0,
            &records,
            Arc::new(BlockCache::new(1024 * 1024)),
            io.clone(),
        )
        .unwrap();
        let file = OpenOptions::new().write(true).open(&table.path).unwrap();
        write_all_at(&file, &[1], table.filters_offset).unwrap();

        assert!(matches!(table.get(&records[0].key), Err(Error::Corruption { .. })));
        assert_eq!(io.snapshot().read_bytes, (FENCE_PAGE_SIZE + FILTER_PAGE_SIZE) as u64);
    }

    #[test]
    fn corrupt_fence_page_is_rejected_before_filter_or_data_reads() {
        let directory = tempfile::tempdir().unwrap();
        let records = (0..RECORDS_PER_BLOCK as u64 + 1)
            .map(|index| record(index, index + 1))
            .collect::<Vec<_>>();
        let io = Arc::new(TableIoCounters::default());
        let table = Table::create(
            directory.path(),
            10,
            0,
            &records,
            Arc::new(BlockCache::new(1024 * 1024)),
            io.clone(),
        )
        .unwrap();
        let file = OpenOptions::new().write(true).open(&table.path).unwrap();
        write_all_at(&file, &[1], table.fences_offset).unwrap();

        assert!(matches!(table.get(&records[0].key), Err(Error::Corruption { .. })));
        assert_eq!(io.snapshot().read_bytes, FENCE_PAGE_SIZE as u64);
    }

    #[test]
    fn corrupt_data_block_is_rejected_on_point_read() {
        let directory = tempfile::tempdir().unwrap();
        let records = (0..RECORDS_PER_BLOCK as u64 + 1)
            .map(|index| record(index, index + 1))
            .collect::<Vec<_>>();
        let io = Arc::new(TableIoCounters::default());
        let table = Table::create(
            directory.path(),
            11,
            0,
            &records,
            Arc::new(BlockCache::new(1024 * 1024)),
            io.clone(),
        )
        .unwrap();
        let file = OpenOptions::new().write(true).open(&table.path).unwrap();
        write_all_at(&file, &[0], 0).unwrap();

        assert!(matches!(table.get(&records[0].key), Err(Error::Corruption { .. })));
        assert_eq!(
            io.snapshot().read_bytes,
            (FENCE_PAGE_SIZE + FILTER_PAGE_SIZE + DATA_BLOCK_SIZE) as u64
        );
    }

    #[test]
    fn corrupt_top_level_fence_index_is_rejected_on_open() {
        let directory = tempfile::tempdir().unwrap();
        let records = (0..RECORDS_PER_BLOCK as u64 + 1)
            .map(|index| record(index, index + 1))
            .collect::<Vec<_>>();
        let table = Table::create(
            directory.path(),
            12,
            0,
            &records,
            Arc::new(BlockCache::new(1024 * 1024)),
            Arc::new(TableIoCounters::default()),
        )
        .unwrap();
        let top_fences_offset = table.fences_offset + table.top_fences.len() as u64 * FENCE_PAGE_SIZE as u64;
        drop(table);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(crate::table::table_path(directory.path(), 12))
            .unwrap();
        let mut byte = [0];
        crate::format::read_exact_at(&file, &mut byte, top_fences_offset).unwrap();
        byte[0] ^= 1;
        write_all_at(&file, &byte, top_fences_offset).unwrap();

        assert!(matches!(
            Table::open(
                directory.path(),
                12,
                0,
                Arc::new(BlockCache::new(1024 * 1024)),
                Arc::new(TableIoCounters::default()),
            ),
            Err(Error::Corruption { .. })
        ));
    }

    #[test]
    fn corrupt_footer_is_rejected_on_open() {
        let directory = tempfile::tempdir().unwrap();
        let records = vec![record(1, 1)];
        let table = Table::create(
            directory.path(),
            13,
            0,
            &records,
            Arc::new(BlockCache::new(1024 * 1024)),
            Arc::new(TableIoCounters::default()),
        )
        .unwrap();
        let footer_offset = table.meta.file_size - TABLE_FOOTER_SIZE as u64;
        drop(table);
        let file = OpenOptions::new()
            .write(true)
            .open(crate::table::table_path(directory.path(), 13))
            .unwrap();
        write_all_at(&file, &[0], footer_offset).unwrap();

        assert!(matches!(
            Table::open(
                directory.path(),
                13,
                0,
                Arc::new(BlockCache::new(1024 * 1024)),
                Arc::new(TableIoCounters::default()),
            ),
            Err(Error::Corruption { .. })
        ));
    }
}
