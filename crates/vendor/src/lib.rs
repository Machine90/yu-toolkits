pub mod groceries {
    pub mod id_gen;
    #[allow(unused)]
    pub mod ip_utils;
    #[allow(unused)]
    #[cfg(feature = "logger")]
    pub mod logger;
    pub mod map;
    #[allow(unused)]
    pub mod singleton;
    pub mod lease_value;
}

pub mod prelude {
    #[cfg(feature = "logger")]
    pub use crate::{
        change_logger_level, crit, debug, error, get_logger,
        groceries::{
            logger::*,
            logger::{config::*, impls::*},
        },
        info, register_logger, trace, warn,
    };

    pub use crate::{
        groceries::{
            id_gen,
            ip_utils::*,
            map::*,
            singleton::{self, Singleton, *},
        },
        shorthands, singleton,
    };
    pub use dashmap::{self, *};
    pub mod lock {
        // compatible with dashmap RwLock since version 5.x (from 4.x)
        // dashmap 5.x has optimized the lock behaviour by trying fast lock first.
        pub use dashmap::{RawRwLock, RwLock, RwLockReadGuard, RwLockWriteGuard};
    }
}

pub mod shorthands {
    use std::time::SystemTime;

    /// Get timestamp from Unix timestamp "1970-01-01 00:00:00 UTC"
    /// to now.
    #[inline]
    pub fn now_timestamp_ms() -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }
}
