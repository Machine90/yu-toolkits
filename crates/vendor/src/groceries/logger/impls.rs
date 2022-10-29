use std::{
    fs::OpenOptions,
    path::Path,
    sync::{atomic::AtomicU8, Arc},
};

use slog::{o, Drain, LevelFilter, Logger};

use crate::groceries::logger::{
    config::LoggerConfig,
    filter::{level::from_level, DynLevelFilter},
};

pub fn default_logger() -> slog::Logger {
    let log_config = LoggerConfig::default();
    log_config.into()
}

impl From<LoggerConfig> for slog::Logger {
    fn from(config: LoggerConfig) -> Self {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = LevelFilter(drain, config.level).fuse();
        let drain = slog_async::Async::new(drain)
            .thread_name(config.name)
            .chan_size(config.async_channels)
            .overflow_strategy(config.overflow_blocking_policy)
            .build()
            .fuse();
        let label = if let Some(label) = config.label {
            o!("tag" => label)
        } else {
            o!("tag" => "terminal logger".to_owned())
        };
        let logger = Logger::root(drain, label);
        logger
    }
}

/// Create the logger with dynamic level, this logger can 
/// change the level at runtime by `change_logger_level!(slog::Level::Debug);`
/// ### Example:
/// ```rust
/// let (logger, controller) = dynamic_level_logger(LoggerConfig {..Default::default()}.into());
/// ```
pub fn dynamic_level_logger(config: LoggerConfig) -> (slog::Logger, Arc<AtomicU8>) {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let level = from_level(config.level);
    let level_signal = Arc::new(AtomicU8::new(level));
    let drain = DynLevelFilter(drain, level_signal.clone()).fuse();
    let drain = slog_async::Async::new(drain)
        .thread_name(config.name)
        .chan_size(config.async_channels)
        .overflow_strategy(config.overflow_blocking_policy)
        .build()
        .fuse();
    let label = if let Some(label) = config.label {
        o!("tag" => label)
    } else {
        o!("tag" => "dynamic level logger".to_owned())
    };
    let logger = Logger::root(drain, label);
    (logger, level_signal)
}

pub fn file_logger<P: AsRef<Path>>(p: P) -> slog::Logger {
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .append(true)
        .open(p.as_ref())
        .unwrap();

    let config = LoggerConfig::default();
    let decorator = slog_term::PlainDecorator::new(file);
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let drain = slog_async::Async::new(drain)
        .thread_name(config.name)
        .chan_size(config.async_channels)
        .overflow_strategy(config.overflow_blocking_policy)
        .build().fuse();
    let drain = LevelFilter(drain, config.level).fuse();
    let logger = slog::Logger::root(drain, o!("tag" => format!("file logger")));
    logger
}

pub fn conf_file_logger<P: AsRef<Path>>(p: P, config: LoggerConfig) -> slog::Logger {
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .append(true)
        .open(p.as_ref())
        .unwrap();
    let decorator = slog_term::PlainDecorator::new(file);
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain)
        .thread_name(config.name)
        .chan_size(config.async_channels)
        .overflow_strategy(config.overflow_blocking_policy)
        .build().fuse();
    let drain = LevelFilter(drain, config.level).fuse();
    let label = if let Some(label) = config.label {
        o!("tag" => label)
    } else {
        o!("tag" => "file logger".to_owned())
    };
    slog::Logger::root(drain, label)
}