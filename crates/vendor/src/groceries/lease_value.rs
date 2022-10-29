use std::{
    hash::Hash,
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime},
};

/// A leased value.
#[derive(Debug)]
pub struct Lease<T> {
    pub val: T,
    last: AtomicU64,
    dur_millis: AtomicU64,
}

impl<T> Lease<T> {
    pub fn new(val: T, dur_millis: u64) -> Self {
        Self {
            val,
            last: AtomicU64::new(Self::_now_stamp()),
            dur_millis: AtomicU64::new(dur_millis),
        }
    }

    #[inline]
    pub fn in_lease(&self) -> bool {
        self.remained() > 0
    }

    #[inline]
    pub fn expire(&self) -> bool {
        !self.in_lease()
    }

    #[inline]
    pub fn refresh(&self) {
        let ts = Self::_now_stamp();
        self.last.store(ts, Ordering::Release)
    }

    #[inline]
    pub fn change_dur(&self, dur_millis: u32) {
        self.dur_millis.store(dur_millis as u64, Ordering::Release)
    }

    #[inline]
    pub fn remained(&self) -> i128 {
        let elapsed = Self::_now_stamp() as i128 - self.last.load(Ordering::Acquire) as i128;
        self.dur_millis.load(Ordering::Relaxed) as i128 - elapsed
    }

    #[inline]
    fn _now_stamp() -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("clock went backforward and earlier than unix epoch")
            .as_millis() as u64
    }
}

impl<T: Hash> Hash for Lease<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.val.hash(state);
    }
}

impl<T: Eq> Eq for Lease<T> {}

impl<T: PartialEq> PartialEq for Lease<T> {
    fn eq(&self, other: &Self) -> bool {
        self.val == other.val
    }
}

impl<T: Ord> Ord for Lease<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.remained().cmp(&other.remained()).reverse() {
            std::cmp::Ordering::Equal => self.val.cmp(&other.val),
            ord => ord,
        }
    }
}

impl<T: PartialOrd> PartialOrd for Lease<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.remained().partial_cmp(&other.remained()) {
            Some(core::cmp::Ordering::Equal) => self.val.partial_cmp(&other.val),
            ord => return ord.map(|o| o.reverse()),
        }
    }
}

impl<T> Deref for Lease<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.val
    }
}

impl<T> DerefMut for Lease<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.val
    }
}

impl<T: Clone> Clone for Lease<T> {
    fn clone(&self) -> Self {
        Self::new(self.val.clone(), self.dur_millis.load(Ordering::Acquire))
    }
}

#[cfg(test)] mod tests {
    use std::time::Duration;

    use super::Lease;


    #[derive(Debug)]
    struct Sampling {
        used: usize,
        #[allow(unused)] total: usize,
    }

    #[test] fn test_lease() {
        let mut sampling = Lease::new(
            Sampling {used: 0, total: 10}, 
            1000
        );

        for _ in 0..4 {
            // sleep may not accurate enough.
            std::thread::sleep(Duration::from_millis(500));
            if sampling.expire() {
                sampling.used += 1;
                sampling.refresh();
            }
        }
        assert!(sampling.used == 2);
    }
}