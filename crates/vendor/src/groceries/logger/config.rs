use slog_async::OverflowStrategy;

pub enum LogLevel {
    Critical, Error, Warning, Info, Debug, Trace,
}

impl Into<slog::Level> for LogLevel {
    fn into(self) -> slog::Level {
        match self {
            LogLevel::Critical => slog::Level::Critical,
            LogLevel::Error => slog::Level::Error,
            LogLevel::Warning => slog::Level::Warning,
            LogLevel::Info => slog::Level::Info,
            LogLevel::Debug => slog::Level::Debug,
            LogLevel::Trace => slog::Level::Trace,
        }
    }
}

/// Logger config, used to create logger by specific config.
pub struct LoggerConfig {
    /// logger's thread name. default to "default logger"
    pub name: String,
    pub level: slog::Level,
    pub async_channels: usize,
    pub overflow_blocking_policy: OverflowStrategy,
    pub label: Option<String>,
}

impl LoggerConfig {
    /// A simple terminal logger in given level
    pub fn in_level(level: LogLevel) -> Self {
        Self {
            level: level.into(),
            ..Default::default()
        }
    }
}

impl Default for LoggerConfig {
    fn default() -> Self {
        Self { 
            name: "default logger".to_owned(),
            level: slog::Level::Info,
            async_channels: 4096,
            overflow_blocking_policy: OverflowStrategy::Block,
            label: Some("default logger".to_owned())
        }
    }
}
