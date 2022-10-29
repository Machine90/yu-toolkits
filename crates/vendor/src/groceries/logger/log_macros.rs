/// ### Description 
/// Get logger via `context`, the `context` we use `module_path` to get raw key.
/// `module_path` always be current `mod`'s path, if you guys wants to wrapper 
/// the key, then you can specific the resolver to the `LogFactory` via
/// `with_resolver`
#[macro_export(local_inner_macros)]
macro_rules! get_logger (
    ($module:expr) => {{
        let logger_factory = $crate::groceries::logger::FACTORY.get(|| $crate::groceries::logger::LogFactory::default());
        logger_factory.get_logger($module)
    }};
    () => {{
        let module = std::module_path!();
        let logger_factory = $crate::groceries::logger::FACTORY.get(|| $crate::groceries::logger::LogFactory::default());
        logger_factory.get_logger(module)
    }}
);

#[macro_export(local_inner_macros)]
macro_rules! register_logger (
    ($logger:expr) => {
        let module = std::module_path!();
        let logger_factory =
            $crate::groceries::logger::FACTORY.get(|| $crate::groceries::logger::LogFactory::default());
        logger_factory.register(module, $logger);
    };
    ($logger:expr, $level:expr) => {
        let module = std::module_path!();
        let logger_factory =
            $crate::groceries::logger::FACTORY.get(|| $crate::groceries::logger::LogFactory::default());
        logger_factory.register_dyn_level(module, $logger, $level);
    };
);

#[macro_export(local_inner_macros)]
macro_rules! change_logger_level (
    ($module:expr, $level:expr) => {{
        let logger_factory = $crate::groceries::logger::FACTORY.get(|| $crate::groceries::logger::LogFactory::default());
        logger_factory.change_level($module, $level);
    }};
    ($level:expr) => {{
        let module = std::module_path!();
        let logger_factory = $crate::groceries::logger::FACTORY.get(|| $crate::groceries::logger::LogFactory::default());
        logger_factory.change_level(module, $level);
    }}
);

/// ### Description
/// Use as slog::info, this method would detect to use logger automatic via `module_path`.
/// the final key could be wrapper via `LogFactory.module_resolver`
/// ### Noting
/// Make sure you have import `slog` to your crate before using.
#[macro_export(local_inner_macros)]
macro_rules! info(
    (#$tag:expr, $($args:tt)*) => {
        let module = std::module_path!();
        let logger_factory =
            $crate::groceries::logger::FACTORY.get(|| $crate::groceries::logger::LogFactory::default());
        let target = logger_factory.get_logger(module);
        slog::info!(target, $tag, $($args)*)
    };
    ($($args:tt)*) => {
        let module = std::module_path!();
        let logger_factory =
            $crate::groceries::logger::FACTORY.get(|| $crate::groceries::logger::LogFactory::default());
        let target = logger_factory.get_logger(module);
        slog::info!(target, $($args)*)
    };
);

/// ### Description
/// Use as slog::warn, this method would detect to use logger automatic via `module_path`.
/// the final key could be wrapper via `LogFactory.module_resolver`
/// ### Noting
/// Make sure you have import `slog` to your crate before using.
#[macro_export(local_inner_macros)]
macro_rules! warn(
    (#$tag:expr, $($args:tt)*) => {
        let module = std::module_path!();
        let logger_factory =
            $crate::groceries::logger::FACTORY.get(|| $crate::groceries::logger::LogFactory::default());
        let target = logger_factory.get_logger(module);
        slog::warn!(target, $tag, $($args)*)
    };
    ($($args:tt)*) => {
        let module = std::module_path!();
        let logger_factory =
            $crate::groceries::logger::FACTORY.get(|| $crate::groceries::logger::LogFactory::default());
        let target = logger_factory.get_logger(module);
        slog::warn!(target, $($args)*)
    };
);

/// ### Description
/// Use as slog::debug, this method would detect to use logger automatic via `module_path`.
/// the final key could be wrapper via `LogFactory.module_resolver`
/// ### Noting
/// Make sure you have import `slog` to your crate before using.
#[macro_export(local_inner_macros)]
macro_rules! debug(
    (#$tag:expr, $($args:tt)*) => {
        let module = std::module_path!();
        let logger_factory =
            $crate::groceries::logger::FACTORY.get(|| $crate::groceries::logger::LogFactory::default());
        let target = logger_factory.get_logger(module);
        slog::debug!(target, $tag, $($args)*)
    };
    ($($args:tt)*) => {
        let module = std::module_path!();
        let logger_factory =
            $crate::groceries::logger::FACTORY.get(|| $crate::groceries::logger::LogFactory::default());
        let target = logger_factory.get_logger(module);
        slog::debug!(target, $($args)*)
    };
);

/// ### Description
/// Use as slog::trace, this method would detect to use logger automatic via `module_path`.
/// the final key could be wrapper via `LogFactory.module_resolver`
/// ### Noting
/// Make sure you have import `slog` to your crate before using.
#[macro_export(local_inner_macros)]
macro_rules! trace(
    (#$tag:expr, $($args:tt)*) => {
        let module = std::module_path!();
        let logger_factory =
            $crate::groceries::logger::FACTORY.get(|| $crate::groceries::logger::LogFactory::default());
        let target = logger_factory.get_logger(module);
        slog::trace!(target, $tag, $($args)*)
    };
    ($($args:tt)*) => {
        let module = std::module_path!();
        let logger_factory =
            $crate::groceries::logger::FACTORY.get(|| $crate::groceries::logger::LogFactory::default());
        let target = logger_factory.get_logger(module);
        slog::trace!(target, $($args)*)
    };
);

/// ### Description
/// Use as slog::error, this method would detect to use logger automatic via `module_path`.
/// the final key could be wrapper via `LogFactory.module_resolver`
/// ### Noting
/// Make sure you have import `slog` to your crate before using.
#[macro_export(local_inner_macros)]
macro_rules! error(
    (#$tag:expr, $($args:tt)*) => {
        let module = std::module_path!();
        let logger_factory =
            $crate::groceries::logger::FACTORY.get(|| $crate::groceries::logger::LogFactory::default());
        let target = logger_factory.get_logger(module);
        slog::error!(target, $tag, $($args)*)
    };
    ($($args:tt)*) => {
        let module = std::module_path!();
        let logger_factory =
            $crate::groceries::logger::FACTORY.get(|| $crate::groceries::logger::LogFactory::default());
        let target = logger_factory.get_logger(module);
        slog::error!(target, $($args)*)
    };
);

/// ### Description
/// Use as slog::crit, this method would detect to use logger automatic via `module_path`.
/// the final key could be wrapper via `LogFactory.module_resolver`
/// ### Noting
/// Make sure you have import `slog` to your crate before using.
#[macro_export(local_inner_macros)]
macro_rules! crit(
    (#$tag:expr, $($args:tt)*) => {
        let module = std::module_path!();
        let logger_factory =
            $crate::groceries::logger::FACTORY.get(|| $crate::groceries::logger::LogFactory::default());
        let target = logger_factory.get_logger(module);
        slog::crit!(target, $tag, $($args)*)
    };
    ($($args:tt)*) => {
        let module = std::module_path!();
        let logger_factory =
            $crate::groceries::logger::FACTORY.get(|| $crate::groceries::logger::LogFactory::default());
        let target = logger_factory.get_logger(module);
        slog::crit!(target, $($args)*)
    };
);


#[cfg(test)]
mod test_for_logger {
    use slog::Value;
    use std::fmt::Display;
    struct Color(u8, u8, u8);

    impl Display for Color {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Color(255, 255, 255) => {
                    write!(f, "white")
                }
                Color(0, 0, 0) => {
                    write!(f, "black")
                }
                _ => {
                    write!(f, "sorry I don't know")
                }
            }
        }
    }

    impl Value for Color {
        fn serialize(
            &self,
            _record: &slog::Record,
            key: slog::Key,
            serializer: &mut dyn slog::Serializer,
        ) -> slog::Result {
            let rec = match self {
                Color(255, 255, 255) => "white",
                Color(0, 0, 0) => "black",
                _ => "sorry I don't know",
            };
            serializer.emit_str(key, rec)
        }
    }
}
