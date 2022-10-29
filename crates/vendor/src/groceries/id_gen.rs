use std::hint::spin_loop;
use std::time::{SystemTime, Duration};

use std::io::{Error, ErrorKind, Result};

type PreferConcurrency = bool;

pub trait IDGenerator: Send + Sync {
    /// Get next ID from this generator.
    fn next_id(&mut self) -> u64;
}

/// The policy of snowflake ID generator, choose one of 
/// policy below as the policy of [IdGen](crate::groceries::id_gen::IdGen),
/// each policy has it's limited and advantages, see:
/// 
/// | Policy              | Group ID  | Node ID      | Lifecycle | IDs per millis |
/// | ------------------- | --------- | ------------ | --------- | -------------- |
/// | N8(_, false)        | -         | [0, 256)     | 557.8 Y   | 4096           |
/// | N8(_, true)         | -         | [0, 256)     | 139.5 Y   | 16384          |
/// | N16(_, false)       | -         | [0, 65536)   | 139.5 Y   | 64             |
/// | N16(_, true)        | -         | [0, 65536)   | 34.9 Y    | 256            |
/// | N20(_, false)       | -         | [0, 1048576) | 2.2 Year  | 256            |
/// | N20(_, true)        | -         | [0, 1048576) | 49.7 Day  | 4096           |
/// | G5N5(_, _, false)   | [0, 32)   | [0, 32)      | 139.5 Y   | 4096           |
/// | G5N5(_, _, true)    | [0, 32)   | [0, 32)      | 49.7 Day  | 4194304        |
/// | G8N8(_, _, false)   | [0, 256)  | [0, 256)     | 139.5 Y   | 64             |
/// | G8N8(_, _, true)    | [0, 256)  | [0, 256)     | 2.2 Y     | 4096           |
/// | G10N10(_, _, false) | [0, 1024) | [0, 1024)    | 2.2 Y     | 256            |
/// | G10N10(_, _, true)  | [0, 1024) | [0, 1024)    | 49.7 D    | 4096           |
#[derive(Debug, Clone, Copy)]
pub enum Policy {
    /// | Policy              | Group ID  | Node ID      | Lifecycle | IDs per millis |
    /// | ------------------- | --------- | ------------ | --------- | -------------- |
    /// | N8(_, false)        | -         | [0, 256)     | 557.8 Y   | 4096           |
    /// | N8(_, true)         | -         | [0, 256)     | 139.5 Y   | 16384          |
    N8(u8, PreferConcurrency),
    /// | Policy              | Group ID  | Node ID      | Lifecycle | IDs per millis |
    /// | ------------------- | --------- | ------------ | --------- | -------------- |
    /// | N16(_, false)       | -         | [0, 65536)   | 139.5 Y   | 64             |
    /// | N16(_, true)        | -         | [0, 65536)   | 34.9 Y    | 256            |
    N16(u16, PreferConcurrency),
    /// | Policy              | Group ID  | Node ID      | Lifecycle | IDs per millis |
    /// | ------------------- | --------- | ------------ | --------- | -------------- |
    /// | N20(_, false)       | -         | [0, 1048576) | 2.2 Year  | 256            |
    /// | N20(_, true)        | -         | [0, 1048576) | 49.7 Day  | 4096           |
    N20(u32, PreferConcurrency),
    /// | Policy              | Group ID  | Node ID      | Lifecycle | IDs per millis |
    /// | ------------------- | --------- | ------------ | --------- | -------------- |
    /// | G5N5(_, _, false)   | [0, 32)   | [0, 32)      | 139.5 Y   | 4096           |
    /// | G5N5(_, _, true)    | [0, 32)   | [0, 32)      | 49.7 Day  | 4194304        |
    G5N5(u8, u8, PreferConcurrency),
    /// | Policy              | Group ID  | Node ID      | Lifecycle | IDs per millis |
    /// | ------------------- | --------- | ------------ | --------- | -------------- |
    /// | G8N8(_, _, false)   | [0, 256)  | [0, 256)     | 139.5 Y   | 64             |
    /// | G8N8(_, _, true)    | [0, 256)  | [0, 256)     | 2.2 Y     | 4096           |
    G8N8(u8, u8, PreferConcurrency),
    /// | Policy              | Group ID  | Node ID      | Lifecycle | IDs per millis |
    /// | ------------------- | --------- | ------------ | --------- | -------------- |
    /// | G10N10(_, _, false) | [0, 1024) | [0, 1024)    | 2.2 Y     | 256            |
    /// | G10N10(_, _, true)  | [0, 1024) | [0, 1024)    | 49.7 D    | 4096           |
    G10N10(u16, u16, PreferConcurrency),
}

impl Policy {
    pub fn validate(self) -> Result<Self> {
        match self {
            Policy::N20(node_id, _) => {
                if node_id >= 1048576 {
                    return Err(Error::new(
                        ErrorKind::InvalidInput, 
                        "node_id should never large than 1048576, otherwise maybe generate same id concurrency"
                    ));
                }
            },
            Policy::G5N5(group_id, node_id, _) => {
                if group_id >= 32 || node_id >= 32 {
                    return Err(Error::new(
                        ErrorKind::InvalidInput, 
                        "group_id or node_id should never large than 32, otherwise maybe generate same id concurrency"
                    ));
                }
            },
            Policy::G10N10(group_id, node_id, _) => {
                if group_id >= 1024 || node_id >= 1024 {
                    return Err(Error::new(
                        ErrorKind::InvalidInput, 
                        "group_id or node_id should never large than 1024, otherwise maybe generate same id concurrency"
                    ));
                }
            },
            _ => (),
        }
        Ok(self)
    }

    #[inline(always)]
    pub fn concurrency(&self) -> u32 {
        match self {
            Policy::N8(_, prefer_concurrency) => {
                if *prefer_concurrency { 16384 } else { 4096 }
            },
            Policy::N16(_, prefer_concurrency) => {
                if *prefer_concurrency { 256 } else { 64 }
            },
            Policy::N20(_, prefer_concurrency) => {
                if *prefer_concurrency { 4096 } else { 256 }
            },
            Policy::G5N5(_, _, prefer_concurrency) => {
                if *prefer_concurrency { 4194304 } else { 4096 }
            },
            Policy::G8N8(_, _, prefer_concurrency) => {
                if *prefer_concurrency { 4096 } else { 64 }
            },
            Policy::G10N10(_, _, prefer_concurrency) => {
                if *prefer_concurrency { 4096 } else { 256 }
            },
        }
    }

    pub fn calculate(&self, timestamp: u64, incr: u32) -> u64 {
        match self {
            Policy::N8(node_id, prefer_concurrency) => {
                let (stamp_shift, node_shift) = if *prefer_concurrency {
                    // each millis gen 256 ids,
                    // 139.46 years. 16384 concurrency
                    (22, 14)
                } else {
                    // each millis gen 64 ids. 
                    // 557.8 years. 4096 concurrency
                    (20, 12)
                };
                // 2^32 keep 32 bits (64 - 32) for timestamp.
                timestamp << stamp_shift
                // 2^20 [0, 1048576) keep 20 bits (32~12) for node.
                | (*node_id as u64) << node_shift
                // then remained 2^12 (0~4096) for incr each millisec
                | incr as u64
            }
            Policy::N16(node_id, prefer_concurrency) => {
                let (stamp_shift, node_shift) = if *prefer_concurrency {
                    // each millis gen 256 ids,
                    // 34.87 years.
                    (24, 8)
                } else {
                    // each millis gen 64 ids. 
                    // 139.46 years.
                    (22, 6)
                };
                // 2^32 keep 32 bits (64 - 32) for timestamp.
                timestamp << stamp_shift
                | (*node_id as u64) << node_shift
                | incr as u64
            }
            Policy::N20(node_id, prefer_concurrency) => {
                let (stamp_shift, node_shift) = if *prefer_concurrency {
                    // each millis gen 4096 ids.
                    // 0.14 years, eq to 49.71 days
                    (32, 12)
                } else {
                    // each millis gen 256 ids,
                    // 2.18 years, 795.36 days
                    (28, 8)
                };
                // 2^32 keep 32 bits (64 - 32) for timestamp.
                timestamp << stamp_shift
                | (*node_id as u64) << node_shift
                | incr as u64
            }
            Policy::G5N5(group, node, prefer_concurrency) => {
                let group = *group;
                let node = *node;
                let (stamp_shift, group_shift, node_shift) = if *prefer_concurrency {
                    // each millis gen 4194304 ids.
                    // support work for 49.71 days from epoch.
                    (32, 27, 22)
                } else {
                    // each millis gen 4096 ids.
                    // support work for 139.46114000202945 years from epoch.
                    (22, 17, 12)
                };
                // 2^42 keep 42 bits (64 - 22) for timestamp.
                timestamp << stamp_shift
                    | (group as u64) << group_shift
                    | (node as u64) << node_shift
                    | incr as u64
            }
            Policy::G8N8(group, node, prefer_concurrency) => {
                let group = *group;
                let node = *node;
                let (stamp_shift, group_shift, node_shift) = if *prefer_concurrency {
                    // each millis gen 4096 ids. 2.18 years
                    (28, 20, 12)
                } else {
                    // each millis gen 64 ids. 139 years
                    (22, 14, 6)
                };
                timestamp << stamp_shift
                    | (group as u64) << group_shift
                    | (node as u64) << node_shift
                    | incr as u64
            }
            Policy::G10N10(group, node, prefer_concurrency) => {
                let group = *group;
                let node = *node;

                let (stamp_shift, group_shift, node_shift) = if *prefer_concurrency {
                    // each millis gen 4096 ids.
                    (32, 22, 12)
                } else {
                    // each millis gen 256 ids
                    (28, 18, 8)
                };
                timestamp << stamp_shift
                    | (group as u64) << group_shift
                    | (node as u64) << node_shift
                    | incr as u64
            }
        }
    }
}

/// An ID generator based on snowflake algorithm, 
/// it's can be used in various scenarios, for example
/// hight-concurrency distributed cluster, 
/// support some generate 
/// [Policy](crate::groceries::id_gen::Policy)
/// 
/// ### Example
/// ```rust
/// // this will create an id generator to gen unique id in 139 years
/// // from now on, and each milliseconds one generator can generate 
/// // at most 64 unique ID, we can create same generator on 65536 nodes.
/// let mut id_gen = IdGen::now_epoch(Policy::N16(65535, false));
/// 
/// let unique = id_gen.next_id();
/// ```
#[derive(Debug, Clone)]
pub struct Snowflake {
    epoch: SystemTime,
    last_timestamp: u64,
    incr: u32,
    policy: Policy,
}

impl Snowflake {

    pub fn new(policy: Policy, epoch: SystemTime) -> Result<Self> {
        let now = SystemTime::now();
        let last_timestamp = now
            .duration_since(epoch);
        if let Err(_) = last_timestamp {
            return Err(Error::new(
                ErrorKind::InvalidInput, format!("epoch {:?} must be earlier than now {:?}", epoch, now)))
        }
        let last_timestamp = last_timestamp.unwrap().as_millis() as u64;
        Ok(Self {
            epoch,
            last_timestamp,
            policy,
            incr: 0,
        })
    }

    /// It's always from now timestamp, suggest 
    /// use it for testing only.
    pub fn now_epoch(policy: Policy) -> Result<Self> {
        Self::new(policy, SystemTime::now())
    }

    /// Default to use [UNIX_EPOCH](std::time::SystemTime::UNIX_EPOCH)
    /// as generator epoch.
    pub fn unix_epoch(policy: Policy) -> Result<Self> {
        Self::new(policy, SystemTime::UNIX_EPOCH)
    }

    pub fn from_timestamp(policy: Policy, millis_timestamp: u64) -> Result<Self> {
        let stamp = SystemTime::UNIX_EPOCH
            .checked_add(Duration::from_millis(millis_timestamp));
        if stamp.is_none() {
            return Err(Error::new(
                ErrorKind::InvalidInput, 
                format!(
                    "input `millis_timestamp`: {:?} maybe out of boundary from UNIX_EPOCH", 
                    millis_timestamp
                )
            ));
        }
        Self::new(policy, stamp.unwrap())
    }
}

impl IDGenerator for Snowflake {
    fn next_id(&mut self) -> u64 {
        self.incr = (self.incr + 1) % self.policy.concurrency();

        if self.incr == 0 {
            let mut now_timestamp = get_now_timestamp_from(self.epoch);
            if now_timestamp == self.last_timestamp {
                now_timestamp = biding_time_conditions(self.last_timestamp, self.epoch);
            }
            self.last_timestamp = now_timestamp;
        }
        self.policy.calculate(self.last_timestamp, self.incr)
    }
}

#[inline]
pub fn get_now_timestamp_from(anchor: SystemTime) -> u64 {
    SystemTime::now()
        .duration_since(anchor)
        .expect("Time went backward, please check your input epoch")
        .as_millis() as u64
}

#[inline(always)]
// Constantly refreshing the latest milliseconds by busy waiting.
pub fn biding_time_conditions(last_time_millis: u64, epoch: SystemTime) -> u64 {
    let mut latest_time_millis: u64;
    loop {
        latest_time_millis = get_now_timestamp_from(epoch);
        if latest_time_millis > last_time_millis {
            return latest_time_millis;
        }
        spin_loop();
    }
}

/// A simple and without distributed support Id generator, which 
/// has very high performance, when in `prefer_concurrency` mode,
/// this generator allow to generate 256 id per nanos for 2.3 years,
/// othewise generate 2 id per nanos for 292.5 years
pub struct Standalone {
    last_timestamp: u64,
    incr: u32,
    prefer_concurrency: bool,
    anchor: SystemTime,
}

impl Standalone {
    pub fn new(prefer_concurrency: bool) -> Self {
        let last_timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Time went backward, please check your machine")
            .as_nanos() as u64;
        Self { last_timestamp, incr: 0, prefer_concurrency, anchor: SystemTime::UNIX_EPOCH }
    }

    pub fn from_now(prefer_concurrency: bool) -> Self {
        Self { last_timestamp: 0, incr: 0, prefer_concurrency, anchor: SystemTime::now() }
    }

    fn _from_unix_epoch_nanos(&self) -> u64 {
        SystemTime::now()
            .duration_since(self.anchor)
            .expect("Time went backward, please check your input epoch")
            .as_nanos() as u64
    }

    fn _biding_time_conditions_nanos(&self, last_time_millis: u64) -> u64 {
        let mut latest_time_millis: u64;
        loop {
            latest_time_millis = self._from_unix_epoch_nanos();
            if latest_time_millis > last_time_millis {
                return latest_time_millis;
            }
            spin_loop();
        }
    }
}

impl IDGenerator for Standalone {

    fn next_id(&mut self) -> u64 {
        let (concurrency, stamp_shift_bits) = if self.prefer_concurrency {
            (256, 8)
        } else {
            (2, 1)
        };
        self.incr = (self.incr + 1) % concurrency;

        if self.incr == 0 {
            let mut now_timestamp = self._from_unix_epoch_nanos();
            if now_timestamp == self.last_timestamp {
                now_timestamp = self._biding_time_conditions_nanos(self.last_timestamp);
            }
            self.last_timestamp = now_timestamp;
        }
        self.last_timestamp << stamp_shift_bits | self.incr as u64
    }
}