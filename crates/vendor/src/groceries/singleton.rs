use std::{
    cell::UnsafeCell,
    sync::{self, Once},
};

pub struct Singleton<T: Sync> {
    lock: sync::Once,
    ptr: UnsafeCell<*const T>,
}

unsafe impl<T: Sync> Sync for Singleton<T> {}

impl<T: Sync> Singleton<T> {
    pub const INIT: Singleton<T> = Singleton {
        lock: Once::new(),
        ptr: UnsafeCell::new(0 as *const T),
    };

    pub fn get<F>(&'static self, or_create: F) -> &'static T
    where
        F: FnOnce() -> T,
    {
        self.lock.call_once(|| unsafe {
            *self.ptr.get() = Box::into_raw(Box::new(or_create()));
        });
        unsafe { &**self.ptr.get() }
    }
}

/// Sorthand macro for Singleton
/// ## Params
/// * instance of singleton
/// * 
/// # Example
/// ```
/// #[derive(Debug, PartialEq)]
/// struct Color(u8, u8, u8);
///
/// singleton!(COLOR, Color);
///
/// let c = COLOR.get(|| {Color(255,255,255)});
///
/// assert!(*c == Color(255,255,255));
/// ```
#[macro_export]
#[doc(hidden)]
macro_rules! singleton {
    ($NAME:ident, $T:ty) => {
        static $NAME: $crate::groceries::singleton::Singleton<$T> = $crate::groceries::singleton::Singleton::INIT;
    };
}

pub mod test_for_singleton {
    use std::collections::HashSet;


    #[derive(Debug, PartialEq)]
    struct Color(u8, u8, u8);
    
    #[test]
    pub fn test_singleton() {
        singleton!(COLOR, Color);
        let c = COLOR.get(|| {Color(255,255,255)});
        singleton!(COLOR2, Color);
        let c2 = COLOR2.get(|| {Color(255,255,0)});
        let c3 = COLOR.get(|| {Color(0,255,255)});
        assert!(*c == Color(255,255,255));
        assert_ne!(c, c2);
        assert_eq!(c, c3);
    }
}