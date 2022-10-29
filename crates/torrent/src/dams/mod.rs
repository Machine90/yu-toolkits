use std::{
    hint::spin_loop,
    sync::{
        atomic::{AtomicI64, AtomicU64, Ordering, AtomicBool},
        Arc,
    },
    time::SystemTime, error::Error, collections::HashMap,
};

use vendor::prelude::{id_gen::get_now_timestamp_from, DashMap};

#[derive(Default, Debug)]
pub struct Window {
    gauges: DashMap<String, AtomicI64>,
    epoch: AtomicU64,
}

impl Window {

    #[inline]
    pub fn incr<M: ToString>(&self, metric: M, incremental: i64) -> &Self {
        self.gauges
            .entry(metric.to_string())
            .or_insert(AtomicI64::new(0))
            .fetch_add(incremental, Ordering::SeqCst);
        self
    }

    #[inline]
    pub fn gauge(&self, metric: &str) -> i64 {
        self.gauges
            .get(metric)
            .map(|v| v.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    #[inline]
    pub fn rm(&self, metric: &str) -> Option<(String, i64)> {
        self.gauges.remove(metric).map(|(metric, gauge)| {
            (metric, gauge.load(Ordering::Relaxed))
        })
    }

    #[inline]
    fn _reset(&self) {
        self._visit();
        self.gauges.clear();
    }

    #[inline]
    fn _visit(&self) {
        self.epoch.fetch_add(1, Ordering::SeqCst);
    }

    #[inline]
    fn _visited(&self) -> bool {
        self.epoch.load(Ordering::Relaxed) > 0
    }
}

#[derive(Clone)]
pub struct Terminate {
    signal: Arc<AtomicBool>
}

impl Terminate {

    pub fn new() -> Self {
        Self { signal: Arc::new(AtomicBool::new(false)) }
    }

    #[inline]
    pub fn is_running(&self) -> bool {
        !self.signal.load(Ordering::Acquire)
    }

    #[inline]
    pub fn start(&self) {
        self.signal.store(false, Ordering::Release);
    }

    #[inline]
    pub fn stop(&self) {
        self.signal.store(true, Ordering::Release);
    }
}

/// Dams is a lightweight sliding window statistics tool which can be 
/// used to sample the metrics in a given duration.
/// ```rust
/// use torrent::dams::*;
/// 
/// // sampling the metrics in past 500ms, each 50ms is a samping
/// // window, total 10 windows in duration. 
/// let dam = Dams::from_timer(500, 10).unwrap();
/// // create a terminate signal.
/// let term = Terminate::new();
/// let daemon = std::thread::spawn(dam.forward(&term));
/// let stop = std::thread::spawn(move || {  
///     std::thread::sleep(std::time::Duration::from_millis(600));
///     term.stop(); 
/// });
/// 
/// // simulate 100 requests in current thread.
/// for i in 0..100 {
///     dam.window().incr("request", 1);
/// }
/// 
/// // total requests in past 500ms is 100.
/// assert_eq!(dams.sum("request"), 100);
/// 
/// let _ = daemon.join();
/// ```
pub struct Dam {
    sampling_dur_ms: u64,
    sampling_frequency: usize,
    sliding_windows: Arc<Vec<Window>>,
    advancer: Arc<dyn Advancer>,
}

impl Dam {

    /// Create sliding window sampling tool from a stopwatch.
    /// * stat_dur_in_ms: "statistics duration", maitain the sampling result in given duration.
    /// * sampling_freq: split "statistics duration" into several windows.
    pub fn from_timer(
        stat_dur_in_ms: u64,
        sampling_freq: usize
    ) -> Result<Self, Box<dyn Error>> {
        if sampling_freq == 0 {
            return Err("sampling_freq should not be zero".into());
        }
        if stat_dur_in_ms == 0 {
            return Err("stat_dur_in_ms should not be zero".into());
        }
        let sampling_dur_ms = stat_dur_in_ms / sampling_freq as u64;
        if sampling_dur_ms < 10 {
            return Err(format!(
                "The calculated sampling duration is recommended to be greater than 10 ms, now 
                is {sampling_dur_ms}ms. (formula: stat_dur_in_ms / sampling_freq)").into()
            );
        }
        let mut windows = Vec::with_capacity(sampling_freq);
        for _ in 0..sampling_freq {
            windows.push(Window::default());
        }
        let this = Self {
            sampling_dur_ms,
            sampling_frequency: sampling_freq,
            advancer: Arc::new(Stopwatch::from_now()),
            sliding_windows: Arc::new(windows),
        };
        this.window()._visit();
        Ok(this)
    }

    pub fn new<A: Advancer + 'static>(
        stat_dur_in_ms: u64,
        sampling_freq: usize,
        advancer: A,
    ) -> Self {
        assert!(sampling_freq > 0);
        assert!(stat_dur_in_ms > 0);

        let sampling_dur_ms = stat_dur_in_ms / sampling_freq as u64;
        let mut windows = Vec::with_capacity(sampling_freq);
        for _ in 0..sampling_freq {
            windows.push(Window::default());
        }
        let this = Self {
            sampling_dur_ms,
            sampling_frequency: sampling_freq,
            advancer: Arc::new(advancer),
            sliding_windows: Arc::new(windows),
        };
        this.window()._visit();
        this
    }

    #[inline]
    pub fn windows(&self) -> impl Iterator<Item = &Window> {
        self.sliding_windows.iter().filter(|window| window._visited())
    }

    #[inline]
    pub fn sum(&self, metric: &str) -> i64 {
        self.windows().map(|w| w.gauge(metric)).sum()
    }

    /// Average incremental in mill-seconds
    #[inline]
    pub fn avg(&self, metric: &str) -> f64 {
        let mut sum = 0;
        let mut dur = 0;
        let sampling_dur_ms = self.sampling_dur_ms;
        for window in self.windows() {
            sum += window.gauge(metric);
            dur += sampling_dur_ms;
        }
        sum as f64 / dur as f64
    }

    #[inline]
    pub fn max(&self, metric: &str) -> Option<i64> {
        self.windows().map(|w| w.gauge(metric)).max()
    }

    #[inline]
    pub fn min(&self, metric: &str) -> Option<i64> {
        self.windows().map(|w| w.gauge(metric)).min()
    }

    /// Scan the sliding windows.
    pub fn scan<S: FnMut(i64) -> bool>(&self, metric: &str, mut scanner: S) {
        for window in self.windows() {
            if !scanner(window.gauge(metric)) {
                break;
            }
        }
    }

    /// Get current window.
    #[inline]
    pub fn window(&self) -> &Window {
        &self.sliding_windows[self._cursor()]
    }

    pub fn collect(&self) -> HashMap<String, i64> {
        let mut collected = HashMap::new();
        for window in self.sliding_windows.iter() {
            for gauge in window.gauges.iter() {
                let v = collected
                    .entry(gauge.key().clone())
                    .or_insert(0);
                *v += gauge.load(Ordering::Relaxed);
            }
        }
        collected
    }

    #[inline]
    fn _cursor(&self) -> usize {
        (self.advancer.current() / self.sampling_dur_ms) as usize % self.sampling_frequency
    }

    /// Get a prepare forwarding closure, this closure can be running 
    /// in any situation, for example running in a standalone thread,
    /// when procedure invoked, sliding window keep moving forward
    /// until a termination signal is received.
    pub fn forward(&self, term: &Terminate) -> impl Fn() -> () {
        let advancer = self.advancer.clone();
        let sampling_dur_ms = self.sampling_dur_ms;
        let sampling_frequency = self.sampling_frequency;
        let windows = self.sliding_windows.clone();
        
        let term_sign = term.clone();
        let closure = move || {
            let mut last_cur = (advancer.current() / sampling_dur_ms) as usize % sampling_frequency;
            while term_sign.is_running() {
                let next_cur = (advancer.advance(sampling_dur_ms) / sampling_dur_ms) as usize % sampling_frequency;
                if next_cur != last_cur {
                    windows[next_cur]._reset();
                    last_cur = next_cur;
                }
                spin_loop();
            }
        };
        closure
    }
}

/// Advancer is used to move forward the sliding window in the 
/// "Dams".
pub trait Advancer: Send + Sync {

    /// advance the count in given step_in_ms.
    fn advance(&self, step_in_ms: u64) -> u64;

    /// get current count.
    fn current(&self) -> u64;
}

/// One of delegation implement of `Advancer`.
pub struct Stopwatch {
    last_ms: AtomicU64,
    epoch: SystemTime,
}

impl Stopwatch {
    pub fn from_now() -> Self {
        Self {
            last_ms: AtomicU64::new(0),
            epoch: SystemTime::now(),
        }
    }
}

impl Advancer for Stopwatch {

    fn advance(&self, step_in_ms: u64) -> u64 {
        let mut last = self.last_ms.load(Ordering::Relaxed);
        loop {
            let now = get_now_timestamp_from(self.epoch);
            if now > last + step_in_ms {
                if let Err(actual) = self.last_ms.compare_exchange(
                    last, 
                    last + step_in_ms, 
                    Ordering::SeqCst, 
                    Ordering::Acquire
                ) {
                    last = actual;
                } else {
                    return now;
                }
            }
            spin_loop();
        }
    }

    #[inline]
    fn current(&self) -> u64 {
        self.last_ms.load(Ordering::Relaxed)
    }
}

#[cfg(all(test, feature = "tokio1-rt"))]
mod tests {
    use std::{sync::{Arc}, time::Duration};
    use crate::runtime;
    use super::{Dam, Terminate};

    #[test]
    fn test_dams() {
        let dam = Arc::new(Dam::from_timer(
            100, 
            10
        ).unwrap());
        let sig = Terminate::new();
        let closure = dam.forward(&sig);

        runtime::blocking(async {
            let task = runtime::spawn_blocking(closure);
            let mut ts = vec![];
            for i in 0..8 {
                let d = dam.clone();
                let t = runtime::spawn(async move {
                    for _ in 0..5000 {
                        d.window().incr(format!("write_bytes_{i}"), 10);
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    for _ in 0..5000 {
                        d.window().incr(format!("del_bytes_{i}"), -10);
                    }
                    if i != 5 {
                        return;
                    }
                });
                ts.push(t);
            }

            for t in ts {
                let _ = t.await;
            }

            let sum = dam.sum(format!("write_bytes_1").as_str());
            println!("1 total write {:?}", sum);

            let avg = dam.avg(format!("write_bytes_1").as_str());
            println!("1 avg write {:?}", avg);
            sig.stop();
            let c = dam.collect();
            println!("{:#?}", c);
            let _ = task.await;
        });
    }
}
