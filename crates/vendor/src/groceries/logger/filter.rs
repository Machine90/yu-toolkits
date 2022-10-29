use std::sync::atomic::Ordering;
use std::sync::atomic::{self, AtomicBool, AtomicU8};
use std::sync::Arc;

use slog::{Drain, LevelFilter};

use self::level::{from_level, to_level};

pub mod level {
    pub const CRITICAL: u8 = 1; // Critical
    pub const ERROR: u8 = 2;
    pub const WARN: u8 = 3;
    pub const INFO: u8 = 4;
    pub const DEBUG: u8 = 5;
    pub const TRACE: u8 = 6;

    #[inline]
    pub fn to_level(level: u8) -> slog::Level {
        match level {
            INFO => slog::Level::Info,
            ERROR => slog::Level::Error,
            WARN => slog::Level::Warning,
            DEBUG => slog::Level::Debug,
            TRACE => slog::Level::Trace,
            CRITICAL => slog::Level::Critical,
            _ => slog::Level::Info,
        }
    }

    #[inline]
    pub fn from_level(level: slog::Level) -> u8 {
        match level {
            slog::Level::Critical => CRITICAL,
            slog::Level::Error => ERROR,
            slog::Level::Warning => WARN,
            slog::Level::Info => INFO,
            slog::Level::Debug => DEBUG,
            slog::Level::Trace => TRACE,
        }
    }
}

/// Dynamic log level filter, support change log level at runtime.
pub struct DynLevelFilter<D>(pub D, pub Arc<atomic::AtomicU8>);

impl<D> DynLevelFilter<D> {
    pub fn new(drain: D, level: slog::Level) -> Self {
        Self(drain, Arc::new(AtomicU8::new(from_level(level))))
    }

    #[inline]
    pub fn change_level(&self, level: slog::Level) {
        self.1.store(from_level(level), Ordering::SeqCst);
    }
}

impl<D> Drain for DynLevelFilter<D>
where
    D: Drain,
{
    type Ok = Option<D::Ok>;
    type Err = Option<D::Err>;

    fn log(
        &self,
        record: &slog::Record,
        values: &slog::OwnedKVList,
    ) -> std::result::Result<Self::Ok, Self::Err> {
        let current_level = to_level(self.1.load(Ordering::Relaxed));
        if record.level().is_at_least(current_level) {
            self.0.log(record, values).map(Some).map_err(Some)
        } else {
            Ok(None)
        }
    }
}

impl<D: slog::Drain> From<LevelFilter<D>> for DynLevelFilter<D> {
    fn from(raw_filter: LevelFilter<D>) -> Self {
        let (drain, level) = (raw_filter.0, raw_filter.1);
        Self::new(drain, level)
    }
}
