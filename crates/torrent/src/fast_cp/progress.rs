use std::collections::HashMap;

use crc32fast::Hasher;

use super::{utils::unit_fmt, MetaInfo};

/// To record current progress of items transfering.
pub struct Progress {
    hasher: Hasher,
    should_check: bool,
    pub total_readed_bytes: u64,
    pub current_item: String,
    pub current_checksum: u32,
    pub current_length: usize,
    pub current_readed: usize,
}

impl Progress {
    pub fn new() -> Self {
        Self {
            hasher: Hasher::new(),
            should_check: false,
            total_readed_bytes: 0u64,
            current_item: Default::default(),
            current_checksum: 0,
            current_length: 0,
            current_readed: 0,
        }
    }

    pub fn statistics(&self) -> HashMap<String, String> {
        let mut mertris = HashMap::new();
        mertris.insert(
            "total_read".to_owned(),
            format!("{:?}", self.total_readed_bytes),
        );
        mertris.insert("current_item".to_owned(), self.current_item.clone());

        let pct = unit_fmt::percent_used(self.current_readed as f64, self.current_length as f64);
        mertris.insert("current_percentage".to_owned(), unit_fmt::fmt_percent(pct));
        mertris
    }

    #[inline]
    fn _should_check(&self) -> bool {
        self.should_check
    }

    pub fn current_progress(&mut self, current: &MetaInfo) {
        let MetaInfo {
            checksum,
            identify,
            bytes,
        } = current;
        self.should_check = *checksum != 0;
        self.current_item = identify.clone();
        self.current_checksum = *checksum;
        self.current_readed = 0;
        self.current_length = *bytes;
    }

    #[inline]
    pub fn update(&mut self, buf: &[u8]) {
        if !self._should_check() {
            return;
        }
        self.hasher.update(buf);
        self.total_readed_bytes += buf.len() as u64;
        self.current_readed += buf.len();
    }

    pub fn compute_crc(&mut self) -> u32 {
        if !self._should_check() {
            return 0;
        }
        let crc32 = self.hasher.clone().finalize();
        self.hasher.reset();
        crc32
    }

    pub fn check(&mut self) -> bool {
        if !self._should_check() {
            return true;
        }
        let computed = self.compute_crc();
        self.current_checksum == computed
    }
}
