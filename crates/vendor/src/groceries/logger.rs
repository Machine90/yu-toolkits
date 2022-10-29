
use self::{
    filter::level::{self, from_level},
    impls::default_logger,
    resolver::{empty_resolver, package_resolver},
};
use dashmap::{DashMap, DashSet};
use std::{
    any::Any,
    collections::HashSet,
    hash::Hash,
    path::PathBuf,
    sync::{atomic::AtomicU8, Arc},
};

use super::singleton::Singleton;

#[macro_use]
pub mod log_macros;
pub mod config;
pub mod filter;
pub mod impls;

/// Logger payload
#[derive(Debug)]
struct Logger(String, Option<slog::Logger>, Arc<AtomicU8>);
impl Hash for Logger {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl PartialEq for Logger {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for Logger {}

pub static FACTORY: Singleton<LogFactory> = Singleton::INIT;
/// create and set LogFactory to singleton factory holder
/// ## Example
/// ```rust
/// // create a default factory
/// let mut factory = LogFactory::default();
/// // can change default logger that macros used
/// factory.change_default_logger(Config { level: slog::Level::Debug, ..Default::default() });
/// // then set it before use.
/// init_logger_factory(factory);
/// ```
/// Noting, this method can call only once.
pub fn init_logger_factory(factory: LogFactory) {
    FACTORY.get(|| factory);
}

/// Logger factory, which hold all registered logger in a set.
/// The key of the logger default to `module` path,
/// `module` defined to `std::module_path!()`, and also
/// developers can modify the `module` key
pub struct LogFactory {
    _default_level: Arc<AtomicU8>,
    module_resolver: Arc<dyn Fn(&str) -> String + Send + Sync + 'static>,
    default_logger: slog::Logger,
    loggers: DashSet<Logger>,
}

impl Default for LogFactory {
    fn default() -> Self {
        Self {
            _default_level: Arc::new(AtomicU8::new(level::INFO)),
            module_resolver: Arc::new(empty_resolver),
            default_logger: default_logger(),
            loggers: Default::default(),
        }
    }
}

impl LogFactory {

    /// log factory with "crate" layer filter in specific level.
    /// e.g. in crate "your_project" call info!("some info") would use 
    /// logger that registered in "your_project" path. 
    /// ## Example
    /// ```rust
    /// init_logger_factory(crate_log_factory(Debug));
    /// // in "other_project"
    /// register_logger!(file_logger("./other_project.log"));
    /// info!("some info"); // => other_project.log
    /// // in "your_project"
    /// register_logger!(file_logger("./your_project.log"));
    /// info!("some info"); // => your_project.log
    /// ```
    pub fn crate_log_factory(level: slog::Level) -> Self {
        Self::with_module_resolver_in_level(package_resolver, level)
    }

    /// log factory with "mod" layer filter in specific level.
    /// e.g. in "your_project" difference mod call info!("some info") would use 
    /// logger that registered in "your_project::mod" path. 
    /// ## Example
    /// ```rust
    /// init_logger_factory(mod_log_factory(Debug));
    /// // in "other_project"
    /// mod mod1 {
    ///     register_logger!(file_logger("./mod1.log"));
    ///     info!("some info"); // => mod1.log
    /// }
    /// 
    /// mod mod2 {
    ///     register_logger!(file_logger("./mod2.log"));
    ///     info!("some info"); // => mod2.log
    /// }
    /// 
    /// ```
    pub fn mod_log_factory(level: slog::Level) -> Self {
        Self::with_module_resolver_in_level(empty_resolver, level)
    }

    pub fn with_module_resolver<F>(resolver: F) -> Self
    where
        F: Fn(&str) -> String + Send + Sync + 'static,
    {
        Self {
            module_resolver: Arc::new(resolver),
            // TODO: support others logger implements
            ..Default::default()
        }
    }

    pub fn with_module_resolver_in_level<F>(resolver: F, level: slog::Level) -> Self
    where
        F: Fn(&str) -> String + Send + Sync + 'static,
    {
        let level = from_level(level);
        Self {
            module_resolver: Arc::new(resolver),
            _default_level: Arc::new(AtomicU8::new(level)),
            ..Default::default()
        }
    }
    
    /// same as `change_default_logger`, it's only 
    /// perform as a builder.
    pub fn use_logger(mut self, logger: slog::Logger) -> Self {
        self.default_logger = logger;
        self
    }

    pub fn register(&self, module: &str, logger: slog::Logger) {
        let enhanced = self.module_resolver.clone();
        let key = enhanced(module);
        let new_logger = Logger(key, Some(logger), self._default_level.clone());
        self.loggers.insert(new_logger);
    }

    pub fn register_dyn_level(
        &self,
        module: &str,
        logger: slog::Logger,
        level_signal: Arc<AtomicU8>,
    ) {
        let enhanced = self.module_resolver.clone();
        let key = enhanced(module);
        let new_logger = Logger(key, Some(logger), level_signal);
        self.loggers.insert(new_logger);
    }

    pub fn change_level(&self, module: &str, level: slog::Level) {
        let enhanced = self.module_resolver.clone();
        let key = enhanced(module);
        let mut q = Logger(key, None, self._default_level.clone());
        let logger = self.loggers.get(&q);
        if let Some(logger) = logger {
            logger
                .2
                .store(from_level(level), std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// Change unique default logger to LogFactory
    #[inline]
    pub fn change_default_logger(&mut self, logger: slog::Logger) {
        self.default_logger = logger;
    }

    pub fn get_logger(&self, module: &str) -> slog::Logger {
        let enhanced = self.module_resolver.clone();
        let key = enhanced(module);
        let mut q = Logger(key, None, self._default_level.clone());
        let logger = self.loggers.get(&q);
        drop(q);
        if let Some(logger) = logger {
            logger.1.clone().unwrap().to_owned()
        } else {
            self.default_logger.to_owned()
        }
    }
}

pub mod resolver {
    pub fn package_resolver(module: &str) -> String {
        if module.is_empty() {
            return module.to_owned();
        }
        let idx = module.find(':');
        if let Some(idx) = idx {
            module[0..idx].to_owned()
        } else {
            module.to_owned()
        }
    }

    #[inline]
    pub fn empty_resolver(module: &str) -> String {
        module.to_owned()
    }
}

#[cfg(test)]
mod test {

    use crate::groceries::logger::{
        config::LoggerConfig,
        impls::{dynamic_level_logger, file_logger},
        resolver::{empty_resolver, package_resolver},
        LogFactory,
    };

    #[test]
    fn test_info_level_logger() {
        register_logger!(file_logger("./info_level.log"));
        trace!("this is trace");
        debug!("this is debug");
        info!("this is info");
        warn!("this is warn");
        error!("this is error");
        crit!("this is critical");
        std::thread::sleep(std::time::Duration::from_millis(1000));
    }

    #[test]
    fn test_trace_level_logger() {
        register_logger!(LoggerConfig {
            level: slog::Level::Trace,
            ..Default::default()
        }
        .into());
        trace!("this is trace");
        debug!("this is debug");
        info!("this is info");
        warn!("this is warn");
        error!("this is error");
        crit!("this is critical");
        std::thread::sleep(std::time::Duration::from_millis(1000));
    }

    #[test]
    fn change_level() {
        let (logger, level) = dynamic_level_logger(LoggerConfig::default());
        register_logger!(logger, level);
        trace!("this is trace");
        debug!("this is debug");
        info!("this is info");
        warn!("this is warn");
        error!("this is error");
        crit!("this is critical");
        std::thread::sleep(std::time::Duration::from_millis(1000));
        println!("now change level to debug");
        change_logger_level!(slog::Level::Trace);
        trace!("this is trace");
        debug!("this is debug");
        info!("this is info");
        warn!("this is warn");
        error!("this is error");
        crit!("this is critical");
        std::thread::sleep(std::time::Duration::from_millis(1000));
    }
}
