use std::{
    fmt::{Debug},
    sync::{
        atomic::{AtomicPtr, Ordering},
        Arc,
    }, 
    collections::{btree_map::Iter, VecDeque},
    io::{Result, Error, ErrorKind},
    slice,
};
use vendor::prelude::lock::{RwLock};
use super::timeline::{Timeline, Acquired};
use crate::partitions::{
    index::sparse::Sparse,
    partition::{Partition}, key::Key,
};

#[derive(Debug)]
pub enum RefEdit<'a, R> {
    Added(&'a Partition<R>),
    Remove(&'a Partition<R>),
}

pub enum Edit<R> {
    Added(Partition<R>),
    Remove(Partition<R>),
}

impl<R> Edit<R> {

    #[inline]
    pub fn as_ref<'a>(&'a self) -> RefEdit<'a, R> {
        match self {
            Edit::Added(p) => RefEdit::Added(p),
            Edit::Remove(p) => RefEdit::Remove(p),
        }
    }
}

pub struct Editing<R> {
    pub(super) added: Sparse<R>,
    pub(super) removed: Sparse<R>,
    pub(super) edits: VecDeque<Edit<R>>,
}

impl<R> Default for Editing<R> {
    fn default() -> Self {
        Self::new()
    }
}

impl<R: Clone> Editing<R> {
    fn _insert(&mut self, partition: Partition<R>) -> bool {
        if self.removed.contains(&partition) {
            return if let Some(p) = self.removed.remove(&partition) {
                // re-insert.
                self.edits.push_back(Edit::Added(p));
                true
            } else {
                false
            };
        }
        let added = self.added.maybe_add(partition.clone());
        if added {
            self.edits.push_back(Edit::Added(partition));
        }
        added
    }

    fn _remove(&mut self, partition: Partition<R>) -> bool {
        if self.added.contains(&partition) {
            return if let Some(rm) = self.added.remove(&partition) {
                self.edits.push_back(Edit::Remove(rm));
                true
            } else {
                false
            };
        }

        if self.removed.maybe_add(partition.clone()) {
            self.edits.push_back(Edit::Remove(partition));
            true
        } else {
            false
        }
    }
}

impl<R> Editing<R> {
    pub fn new() -> Self {
        Self {
            added: Sparse::default(),
            removed: Sparse::default(),
            edits: VecDeque::new(),
        }
    }

    pub fn visit(&self) -> impl Iterator<Item = RefEdit<'_, R>> {
        self.edits.iter()
            .map(|p| p.as_ref())
    }

    fn _contains(&self, partition: &Partition<R>) -> bool {
        self._contains_add(partition) && !self._contains_rm(partition)
    }

    fn _contains_add(&self, partition: &Partition<R>) -> bool {
        if let Some(found) = self.added.indexes.get(&partition.to_key) {
            return found == partition;
        }
        false
    }

    fn _contains_rm(&self, partition: &Partition<R>) -> bool {
        if let Some(found) = self.removed.indexes.get(&partition.to_key) {
            return found == partition;
        }
        false
    }

    fn _is_modified(&self, partition: &Partition<R>) -> bool {
        self.added.indexes.get(&partition.to_key).is_some()
    }

    fn _find<K: AsRef<[u8]>>(&self, key: K) -> Option<&Partition<R>> {
        self.added.find(key)
    }

    fn _find_mut<K: AsRef<[u8]>>(&mut self, key: K) -> Option<&mut Partition<R>> {
        self.added.find_mut(key)
    }
}

pub struct Version<R> {
    pub(super) id: u64,
    pub(super) editing: Editing<R>,
    /// Applied partitions only for `read`.
    pub(super) current: AtomicPtr<Sparse<R>>,
    /// use the RwLock of `dashmap` which implement as cas 
    /// operation, avoid to use raw point directly which 
    /// may cause concurrency problem without cas acquire.
    pub(super) timeline: Arc<RwLock<Timeline<R>>>,
}

impl<R> Debug for Version<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Version").field("id", &self.id).finish()
    }
}

impl<R> Drop for Version<R> {
    fn drop(&mut self) {
        self.timeline.write().clear(self.id)
    }
}

impl<R: Clone> Version<R> {

    /// Try to add partition to this version, failure if:
    /// * target partition is editing by other version now.
    /// * target partition is not allow to add to partitions, maybe overlap 
    /// with another partition.
    /// ### Params
    /// * partition: prepare to add to partitions.
    /// * blocking: blocking to acquire the `write_lock` of `timeline` if true.
    pub fn maybe_add(&mut self, partition: Partition<R>, blocking: bool) -> bool {
        let Partition { from_key, .. } = &partition;
        match self._try_acquire(from_key, blocking) {
            Acquired::Locked | Acquired::Occupied => return false,
            Acquired::Some(ori) => {
                if !self.editing._contains(ori) {
                    // indicate should split exists one.
                    self.editing._insert(ori.clone());
                }
            },
            _ => ()
        };
        self.editing._insert(partition)
    }

    /// Try to merge several continuous small partitions to a larger one.
    /// ### Params
    /// * sorted: continuous small partitions. e.g. `[[1, 2), [2, 3), [3, 4)]`
    /// * blocking: blocking to acquire the `write_lock` of `timeline` if true.
    pub fn try_compact(
        &mut self, 
        sorted: &Vec<Partition<R>>, 
        replacement: R,
        blocking: bool
    ) -> Result<()> {
        let num = sorted.len();
        if num <= 1 {
            return Err(Error::new(
                ErrorKind::InvalidInput, 
                "partitions to be compacted should more than 1"
            ));
        }
        // step 1: makesure all partition exists.
        for partition in sorted.iter() {
            if !self.contains(&partition) {
                return Err(Error::new(
                    ErrorKind::InvalidInput, 
                    format!("{partition} prepare to compact is not existed")
                ));
            }
        }

        // step 2: check if the partitions are connected
        if !Partition::is_continuous(sorted) {
            return Err(Error::new(
                ErrorKind::InvalidInput, 
                "partitions to be compacted is not continuous"
            ));
        }

        let acquired = self._try_acquire_partitions(
            sorted.iter(), blocking
        );
        match acquired {
            Acquired::Locked | Acquired::Occupied => return Err(Error::new(
                ErrorKind::WouldBlock, 
                format!(
                    "could not compact partitions because one of partition is `{:?}` now",
                    acquired
                )
            )),
            _ => ()
        }

        for clear in sorted {
            self.editing._remove(clear.clone());
        }

        let from_key = sorted[0].from_key.clone();
        let to_key = sorted[num - 1].to_key.clone();
        let merged = Partition::new(from_key, to_key, replacement);
        self.editing._insert(merged);
        Ok(())
    }

    pub fn try_remove(&mut self, target: &Partition<R>, blocking: bool) -> Result<bool> {
        let acquired = self._try_acquire(&target.from_key, blocking);
        match acquired {
            Acquired::Locked | Acquired::Occupied => return Err(Error::new(
                ErrorKind::WouldBlock, 
                format!("partition is `{:?}` now", acquired)
            )),
            _ => ()
        }
        Ok(self.editing._remove(target.clone()))
    }
}

impl<R> Version<R> {
    pub(super) fn new(timeline: Arc<RwLock<Timeline<R>>>) -> Self {
        let current = AtomicPtr::new(std::ptr::null_mut());
        Self {
            id: 0,
            current,
            editing: Editing::new(),
            timeline,
        }
    }

    /// Visit all changes of this version.
    #[inline]
    pub fn visit_changes(&self) -> impl Iterator<Item = RefEdit<'_, R>> {
        self.editing.visit()
    }

    /// Visit origin data of partitions without acquire `read_lock`. This 
    /// will read iter partitions by using raw pointer.
    pub unsafe fn visit_committed(&self) -> Iter<'_, Key, Partition<R>> {
        (*self.current.load(Ordering::Relaxed)).iter()
    }

    /// Check if contains partition, when 2 partitions have the same from_key and to_key
    /// then these 2 partions are the same.
    pub fn contains(&self, partition: &Partition<R>) -> bool {
        (self.editing._contains_add(partition) || unsafe { 
            (*self.current.load(Ordering::Acquire)).contains(partition) 
        }) && !self.editing._contains_rm(partition)
    }

    /// Try to find the key belongs partition from editing first, 
    /// otherwise find it from committed partitions if not existed
    /// in editing.
    pub fn find<K: AsRef<[u8]>>(&self, key: K) -> Option<&Partition<R>> {
        let found = self.editing._find(key.as_ref()).or(unsafe { 
            (*self.current.load(Ordering::Acquire)).find(key) 
        })?;
        if self.editing._contains_rm(found) {
            return None;
        }
        Some(found)
    }

    pub fn commit(mut self) {
        // commit edits before final release lock of ranges.
        let editing = std::mem::take(&mut self.editing);
        self.timeline.write().commit(self.id, editing);
        // release lock when version dropped.
    }

    /// try to acquire a partition that include given `key`,
    /// returns:
    /// * **Acquire::None**: If not partition found in committed partitions.
    /// * **Acquire::Locked**: If partition is hold by other version.
    /// * **Acquire::Some(p)**: Get or reentry the locked partition.
    /// * **Acquire::Occupied**: Other version hold the lock of timeline.
    fn _try_acquire<K: AsRef<[u8]>>(
        &self, key: K, 
        blocking: bool
    ) -> Acquired<&Partition<R>> {
        if let Some(p) = unsafe {
            (*self.current.load(Ordering::Relaxed)).find(key.as_ref())
        } {
            let mut acquired = if !blocking {
                let try_acquire = self
                    .timeline
                    .try_write();
                if try_acquire.is_none() { return Acquired::Occupied; }
                try_acquire.unwrap()
            } else {
                self.timeline.write()
            };

            if acquired.try_editing(self.id, p).is_ok() 
            {
                Acquired::Some(p)
            } else {
                // range is locked
                Acquired::Locked
            }
        } else {
            Acquired::None
        }
    }

    fn _try_acquire_partitions(
        &self, 
        partitions: slice::Iter<'_, Partition<R>>,
        blocking: bool
    ) -> Acquired<()> {
        let mut acquired = if !blocking {
            let try_acquire = self
                .timeline
                .try_write();
            if try_acquire.is_none() { return Acquired::Occupied; }
            try_acquire.unwrap()
        } else {
            self.timeline.write()
        };

        if acquired.try_editing_more(self.id, partitions).is_ok() {
            Acquired::Some(())
        } else {
            Acquired::Locked
        }
    }
}
