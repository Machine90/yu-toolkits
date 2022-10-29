use std::{
    collections::{HashMap, btree_map::Iter}, 
    io::{Result, Error, ErrorKind}, 
    sync::{Arc, atomic::{AtomicPtr, Ordering}}, ops::Range, fmt::Debug
};
use vendor::prelude::id_gen::{Standalone, IDGenerator};
use crate::partitions::{partition::Partition, index::sparse::Sparse, key::{Key, RefKey}};
use super::version::{Version, Editing, Edit};

#[derive(PartialEq, Eq)]
pub enum Acquired<T> {
    /// target partition is locked now.
    Locked, 
    /// other concurrency writting timeline now, 
    /// suggest to retry for a while.
    Occupied, 
    Reentry, 
    None, 
    Some(T)
}

impl<T> Debug for Acquired<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Locked => write!(f, "Locked"),
            Self::Occupied => write!(f, "Occupied"),
            Self::Reentry => write!(f, "Reentry"),
            Self::None => write!(f, "None"),
            Self::Some(_) => write!(f, "Some(..)"),
        }
    }
}

struct GapLock {
    locked: Sparse<u64>,
    belongs: HashMap<u64, HashMap<Key, Key>>,
}

impl GapLock {
    fn new() -> Self {
        Self { 
            locked: Sparse::default(),
            belongs: HashMap::default()
        }
    }

    fn try_lock<'a, R>(
        &self, 
        owner: u64, 
        partition: &'a Partition<R>
    ) -> Acquired<(&'a Key, &'a Key)> {
        let Partition { from_key, to_key, .. } = partition;
        let mut reentry = false;
        if self.locked.is_overlap_by_range(from_key..to_key, |p| {
            if p.resident == owner {
                reentry = true;
                false
            } else {
                true
            }
        }) {
            return Acquired::Locked;
        }

        if reentry {
            return Acquired::Reentry;
        }
        Acquired::Some((from_key, to_key))
    }

    fn lock(&mut self, range: Partition<u64>) {
        let Partition { from_key, to_key, resident: owner } = &range;
        let owner = *owner;
        let from = from_key.clone();
        let to = to_key.clone();

        if self.locked.maybe_add(range) {
            self.belongs.entry(owner)
                .or_insert(HashMap::new()).insert(from, to);
        }
    }

    #[inline]
    fn hold_lock(&self, owner: u64) -> bool {
        self.belongs.get(&owner).map(|locks| !locks.is_empty()).unwrap_or(false)
    }

    fn release<R>(&mut self, owner: u64, partition: &Partition<R>) {
        let should_rm = self.belongs.get_mut(&owner).map(|locks| {
            let del = locks.remove(&partition.from_key);
            if let Some(to_key) = del {
                self.locked.remove_by_range(&partition.from_key..&to_key);
            }
            locks.is_empty()
        }).unwrap_or(false);
        if should_rm {
            self.belongs.remove(&owner);
        }
    }

    fn release_all_of(&mut self, owner: u64) {
        let removed = self.belongs.remove(&owner);
        if let Some(locked) = removed {
            for (s, e) in locked {
                self.locked.remove_by_range(&s..&e);
            }
        }
    }
}

pub struct Timeline<R> {
    id_gen: Box<dyn IDGenerator>,
    cur_ptr: Arc<AtomicPtr<Sparse<R>>>,
    pub current: Box<Sparse<R>>,
    gap_lock: GapLock,
}

impl<R> Timeline<R> {
    pub fn new() -> Self {
        Self::from_exists(Sparse::default())
    }

    pub fn from_exists(base: Sparse<R>) -> Self {
        let mut current = Box::new(base);
        let ptr: *mut Sparse<R> = &mut *current;
        let cur_ptr = Arc::new(AtomicPtr::new(ptr));

        let default_id_gen = Box::new(Standalone::new(true));
        Self { 
            id_gen: default_id_gen,
            gap_lock: GapLock::new(),
            cur_ptr,
            current,
        }
    }

    #[inline]
    pub fn set_id_generator<G: IDGenerator + 'static>(&mut self, gen: G) {
        self.id_gen = Box::new(gen);
    }

    #[inline]
    pub fn gen_id(&mut self) -> u64 {
        self.id_gen.next_id()
    }

    pub(crate) fn assign(&mut self, version: &mut Version<R>) {
        let txid = self.id_gen.next_id();
        version.id = txid;
        version.current.store(
            self.cur_ptr.load(Ordering::SeqCst), 
            Ordering::SeqCst
        );
    }

    pub(crate) fn try_editing_more(
        &mut self, 
        txid: u64, 
        partitions: std::slice::Iter<'_, Partition<R>>
    ) -> Result<&mut Self> {
        let mut candidates = vec![];
        // check if all partitions is unlocked or reentry
        for partition in partitions {
            match self.gap_lock.try_lock(txid, partition) {
                Acquired::Locked => return Err(Error::new(
                    ErrorKind::WouldBlock, 
                    "one of partition is locked, please wait until lock released"
                )),
                Acquired::Some((from, to)) => {
                    let prepare_lock = Partition::new(
                        from.clone(), 
                        to.clone(),
                        txid
                    );
                    candidates.push(prepare_lock);
                },
                _ => ()
            }
        }
        // then lock each candidates that unlocked before
        for candidate in candidates {
            self.gap_lock.lock(candidate);
        }
        Ok(self)
    }

    /// Only exists and not be acquired by other txn's partition 
    /// can be editing. The same txid trying to edit the same 
    /// partition perform as reentry.
    pub(crate) fn try_editing(
        &mut self, 
        txid: u64, 
        partition: &Partition<R>
    ) -> Result<&mut Self> {
        if !self.current.contains(partition) {
            return Err(Error::new(ErrorKind::NotFound, "partition not found in map"));
        }

        match self.gap_lock.try_lock(txid, partition) {
            Acquired::Locked => {
                return Err(Error::new(ErrorKind::WouldBlock, "partition is locked now, try it later"))
            },
            Acquired::Reentry => {
                Ok(self)
            },
            Acquired::Some((from, to)) => {
                let prepare_lock = Partition::new(
                    from.clone(), 
                    to.clone(),
                    txid
                );
                self.gap_lock.lock(prepare_lock);
                Ok(self)
            },
            _ => {
                unreachable!()
            }
        }
    }

    #[allow(unused)]
    pub(crate) fn release(&mut self, txid: u64, partition: &Partition<R>) {
        self.gap_lock.release(txid, partition);
    }

    pub(crate) fn commit(&mut self, _: u64, mut editing: Editing<R>) {
        while let Some(edit) = editing.edits.pop_front() {
            match edit {
                Edit::Added(p) => {
                    self.current.maybe_add(p);
                },
                Edit::Remove(p) => {
                    self.current.remove(&p);
                },
            }
        }
    }

    #[inline]
    pub fn has_locks(&self, txid: u64) -> bool {
        self.gap_lock.hold_lock(txid)
    }

    pub(crate) fn clear(&mut self, txid: u64) {
        self.gap_lock.release_all_of(txid);
    }
}

impl<R> Timeline<R> {
    
    /// Find the gaps of partitions from Min (exclude last partition to Max). 
    /// For instance, there has partitions:
    /// ```json
    /// {
    ///     "1 to 5": "partition 1",
    ///     "5 to 10": "partition 2",
    ///     "12 to 17": "partition 3",
    /// }
    /// ```
    /// the gap is \[Min, 1\) and \[10, 12\)
    #[inline]
    pub fn gaps(&self) -> impl Iterator<Item = (RefKey, RefKey)> {
        self.current.gaps()
    }

    /// Iterate all partition's key range.
    #[inline]
    pub fn list(&self) -> impl Iterator<Item = (RefKey, RefKey)> {
        self.current.list()
    }

    #[inline]
    pub fn iter(&self) -> Iter<'_, Key, Partition<R>> {
        self.current.iter()
    }

    #[inline]
    pub fn range_by_key(&self, range: Range<&Key>) -> impl Iterator<Item = &Partition<R>> {
        self.current.range_by_key(range)
    }

    #[inline]
    pub fn range<K: AsRef<[u8]>>(&self, range: Range<K>) -> impl Iterator<Item = &Partition<R>> {
        self.current.range(range)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.current.len()
    }

    /// Check if contains partition, when 2 partitions have the same from_key and to_key
    /// then these 2 partions are the same.
    #[inline] pub fn contains(&self, partition: &Partition<R>) -> bool {
        self.current.contains(partition)
    }

    /// Determine if target partition is overlap with one partition
    /// of this set, when a partition equal (have the same boundary) 
    /// to another partition, they're not overlap.
    /// ### Params
    /// * when_eq: fn(&same_partition) -> should_overlap;
    #[inline]
    pub fn is_overlap<F>(&self, partition: &Partition<R>, when_eq: F) -> bool
    where 
        F: FnOnce(&Partition<R>) -> bool 
    {
        self.current.is_overlap(partition, when_eq)
    }

    #[inline]
    pub fn is_overlap_by_range<F>(&self, range: Range<&Key>, when_eq: F) -> bool 
    where 
        F: FnOnce(&Partition<R>) -> bool
    {
        self.current.is_overlap_by_range(range, when_eq)
    }

    /// Trying to find location of key if existed, the key is
    /// not a accurate value, if a partition contains the key,
    /// then return this partition.
    #[inline] pub fn find<K: AsRef<[u8]>>(&self, key: K) -> Option<&Partition<R>> {
        self.current.find(key)
    }

    /// Try to estimate split partition's key range by given split key.
    /// For example there has partition \[1, 100\) in partitions, spit key
    /// is 50, then return result is Some((1, 50)), then use this result 
    /// to build a new partition to request for splitting.
    #[inline] pub fn estimate_split<K: AsRef<[u8]>>(&self, split_key: K) -> Option<(Key, Key)> {
        self.current.estimate_split(split_key)
    }
}

#[cfg(test)] mod tests {

    use crate::partitions::{index::sparse::Sparse, partition::Partition};

    use super::Timeline;

    #[test] fn test_editing() {
        let mut index = Sparse::default();
        index.maybe_add(Partition::from_range("aaa".."bbb", 1));
        index.maybe_add(Partition::from_range("bbb".."ccc", 2));
        index.maybe_add(Partition::from_range("ggg".."kkk", 3));
        index.maybe_add(Partition::from_range("nnn".."ttt", 3));

        let mut timeline = Timeline::from_exists(index);

        let part_a = Partition::pure("aaa".."bbb");
        let permit = timeline
            .try_editing(1, &part_a).is_ok();
        println!("1 edit aaa..bbb {:?}", permit);

        let permit = timeline
            .try_editing(2, &part_a).is_ok();
        println!("2 edit aaa..bbb {:?}", permit);

        let permit = timeline
            .try_editing(1, &Partition::pure("aaa".."bbb")).is_ok();
        println!("1 reentry aaa..bbb {:?}", permit);

        timeline.release(1, &part_a);

        let permit = timeline
            .try_editing(2, &part_a).is_ok();
        println!("2 edit aaa..bbb after 1 release {:?}", permit);

        let permit = timeline
            .try_editing(2, &Partition::pure("ggg".."kkk")).is_ok();
        println!("2 edit ggg..kkk {:?}", permit);

        timeline.clear(1);

        let permit = timeline
            .try_editing(2, &Partition::pure("aaa".."bbb")).is_ok();
        println!("permit {:?}", permit);
    }
}