use crate::partitions::{
    key::{Key, RefKey},
    partition::{Partition, Relationship},
};
use std::{
    collections::{BTreeMap, btree_map::{Iter, IterMut}}, 
    ops::{Bound, Range},
    io::{Result, Error, ErrorKind}
};

#[derive(Debug, Clone)]
pub struct Sparse<R> {
    pub(super) indexes: BTreeMap<Key, Partition<R>>,
}

impl<R> Default for Sparse<R> {
    fn default() -> Self {
        Self {
            indexes: BTreeMap::default(),
        }
    }
}

impl<R> Sparse<R> {

    /// Create an unboundary partition, this partition's keys
    /// from Min to Max.
    pub fn infinity(resident: R) -> Self {
        let infinity = Partition::new(
            Key::Min, 
            Key::Max, 
            resident
        );
        let mut this = Self::default();
        this.maybe_add(infinity);
        this
    }

    #[inline]
    pub fn take(self) -> BTreeMap<Key, Partition<R>> {
        self.indexes
    }

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
    pub fn gaps(&self) -> impl Iterator<Item = (RefKey, RefKey)> {
        let mut last_key = RefKey::Min;
        self.indexes.iter()
            .filter_map(move |(_, par)| {
                if par.from_key.as_left() != last_key {
                    let gap = (last_key.clone(), par.from_key.as_left());
                    last_key = par.to_key.as_left();
                    Some(gap)
                } else {
                    last_key = par.to_key.as_left();
                    None
                }
            })
    }

    /// Iterate all partition's key range.
    #[inline]
    pub fn list(&self) -> impl Iterator<Item = (RefKey, RefKey)> {
        self.indexes.iter()
            .map(|(_, par)| 
                (par.from_key.as_left(), par.to_key.as_right())
            )
    }

    #[inline]
    pub fn iter(&self) -> Iter<'_, Key, Partition<R>> {
        self.indexes.iter()
    }

    #[inline]
    pub fn iter_mut(&mut self) -> IterMut<'_, Key, Partition<R>> {
        self.indexes.iter_mut()
    }

    pub fn range_by_key(&self, range: Range<&Key>) -> impl Iterator<Item = &Partition<R>> {
        self.indexes
            .range(range)
            .map(|(_, p)| {
                p
            })
    }

    pub fn range<K: AsRef<[u8]>>(&self, range: Range<K>) -> impl Iterator<Item = &Partition<R>> {
        let Range { start, end } = range;
        let s = Key::left(start.as_ref());
        let e = Key::left(end.as_ref());
        self.indexes
            .range(&s..&e)
            .map(|(_, p)| {
                p
            })
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.indexes.len()
    }

    /// Add or split partition, if the partition is overlap with more than 1
    /// partition, then it'll be failed to insert.
    /// For instance, there has partitions: \[[10, 20), [50, 100)\],
    /// it's ok to add [10, 15) as split, [50, 100) as replace or 
    /// [105, 120) as create to this partitons, but it's not allow to 
    /// add [8, 18), [60, 80) or [40, 110).
    pub fn maybe_add(&mut self, target: Partition<R>) -> bool {
        let Partition {
            from_key, to_key, ..
        } = &target;

        if let Some(existed) = self.find_mut(from_key) {
            // perform as split existed partiton.
            if target.is(Relationship::LeftPartOf, existed) {
                // split right & add left
                existed.update_left(to_key.as_left());
                self.indexes.insert(to_key.clone(), target);
                return true;
            } else if existed == &target {
                // has the same range, then replace with new partition.
                existed.replace(target.resident);
                return true;
            }
        } else {
            self.indexes.insert(to_key.clone(), target);
            return true;
        }
        false
    }

    pub fn maybe_add_if<F>(&mut self, target: Partition<R>, allow: F) -> bool 
    where 
        F: FnOnce(Option<&Partition<R>>, &Partition<R>) -> bool
    {
        let Partition {
            from_key, to_key, ..
        } = &target;

        if let Some(existed) = self.find_mut(from_key) {
            // perform as split existed partiton.
            if target.is(Relationship::LeftPartOf, existed) {
                // split right & add left
                if allow(Some(&existed), &target) {
                    existed.update_left(to_key.as_left());
                    self.indexes.insert(to_key.clone(), target);
                    return true;
                }
            } else if existed == &target {
                // has the same range, then replace with new partition.
                if allow(Some(&existed), &target) {
                    existed.replace(target.resident);
                    return true;
                }
            }
        } else {
            if !allow(None, &target) {
                return false;
            }
            self.indexes.insert(to_key.clone(), target);
            return true;
        }
        false
    }

    /// Merge target continuous partitions into a large partition with replaced 
    /// resident. make sure all partitions existed.
    pub fn compact(&mut self, partitions: &mut Vec<Partition<R>>, replacement: R) -> Result<()> {
        let num = partitions.len();
        if num <= 1 {
            return Err(Error::new(
                ErrorKind::InvalidInput, 
                "partitions to be compacted should more than 1"
            ));
        }
        // step 1: makesure all partition exists.
        for partition in partitions.iter() {
            if !self.contains(&partition) {
                return Err(Error::new(
                    ErrorKind::InvalidInput, 
                    format!("{partition} prepare to compact is not existed")
                ));
            }
        }

        // step 2: check if the partitions are connected
        if !Partition::maybe_continuous(partitions) {
            return Err(Error::new(
                ErrorKind::InvalidInput, 
                "partitions to be compacted is not continuous"
            ));
        }

        let from_key = partitions[0].from_key.clone();
        let to_key = partitions[num - 1].to_key.clone();
        let merged = Partition::new(from_key, to_key, replacement);

        for clear in partitions.iter() {
            self.remove(clear);
        }
        self.maybe_add(merged);
        Ok(())
    }

    #[inline]
    pub fn remove(&mut self, target: &Partition<R>) -> Option<Partition<R>> {
        let (start, end ) = (&target.from_key, &target.to_key);
        let part = self.indexes.get(end)?;
        if &part.from_key != start {
            return None;
        }
        self.indexes.remove(end)
    }

    pub fn remove_by_range(&mut self, range: Range<&Key>) -> Option<Partition<R>> {
        let Range { start, end } = range;
        let part = self.indexes.get(end)?;
        if &part.from_key != start {
            return None;
        }
        self.indexes.remove(end)
    }

    /// Check if contains partition, when 2 partitions have the same from_key and to_key
    /// then these 2 partions are the same.
    pub fn contains(&self, partition: &Partition<R>) -> bool {
        if let Some(found) = self.indexes.get(&partition.to_key) {
            return found == partition;
        }
        false
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
        if let Some(left) = self.find(&partition.from_key) {
            if left == partition {
                // when eq, should judged to be overlaped?
                return when_eq(left);
            }
            true
        } else if let Some(right) = self.find(&partition.to_key) {
            if right == partition {
                return when_eq(right);
            } 
            // // maybe just linked.
            &partition.to_key != &right.from_key
        } else {
            false
        }
    }

    #[inline]
    pub fn is_overlap_by_range<F>(&self, range: Range<&Key>, when_eq: F) -> bool 
    where 
        F: FnOnce(&Partition<R>) -> bool
    {
        let Range { start, end } = range;

        if let Some(left) = self.find(start) {
            if &left.from_key == start && &left.to_key == end {
                // when eq, should judged to be overlaped?
                return when_eq(left);
            } 
            true
        } else if let Some(right) = self.find(end) {
            if &right.from_key == start && &right.to_key == end {
                return when_eq(right);
            } 
            // // maybe just linked.
            end != &right.from_key
        } else {
            false
        }
    }

    /// Trying to find location of key if existed, the key is
    /// not a accurate value, if a partition contains the key,
    /// then return this partition.
    pub fn find<K: AsRef<[u8]>>(&self, key: K) -> Option<&Partition<R>> {
        let index = key.as_ref();
        self.indexes
            .range((Bound::Excluded(&Key::left(index)), Bound::Unbounded))
            .take(1)
            .filter(
                |(_, partition)| partition.contains(RefKey::left(index)).is_ok()
            )
            .next()
            .map(|(_, p)| p)
    }

    pub fn find_mut<K: AsRef<[u8]>>(&mut self, key: K) -> Option<&mut Partition<R>> {
        let index = key.as_ref();
        self.indexes
            .range_mut((Bound::Excluded(&Key::left(index)), Bound::Unbounded))
            .take(1)
            .filter(
                |(_, partition)| partition.contains(RefKey::left(index)).is_ok()
            )
            .next()
            .map(|(_, p)| p)
    }

    /// Try to estimate split partition's key range by given split key.
    /// For example there has partition \[1, 100\) in partitions, spit key
    /// is 50, then return result is Some((1, 50)), then use this result 
    /// to build a new partition to request for splitting.
    pub fn estimate_split<K: AsRef<[u8]>>(&self, split_key: K) -> Option<(Key, Key)> {
        let split = split_key.as_ref();
        if let Some(partition) = self.find(split) {
            Some((partition.from_key.clone(), Key::right(split)))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::partitions::{
        key::Key, partition::{Partition, Relationship},
    };

    use super::Sparse;

    #[derive(Debug, Clone)]
    pub struct Value<T: Clone>(pub T);

    #[test]
    fn test_is() {
        let p1 = Partition::new(
            Key::left(b"aaaa"),
            Key::right(b"eeee"),
            Value("123"),
        );

        let p2 = Partition::new(
            Key::left(b"aaaaa"),
            Key::right(b"ccc"),
            Value("123"),
        );

        let p3 = Partition::new(
            Key::left(b"aaaa"),
            Key::right(b"ccc"),
            Value("123"),
        );

        let contained = p2.is(Relationship::ContainedIn, &p1);
        assert!(contained);
        let is_left = p3.is(Relationship::LeftPartOf, &p1);
        assert!(is_left);
    }

    #[test]
    fn test_split() {
        let mut parts = Sparse::infinity("123");
        let split_key = ["ajskdl", "123123", "kjqjwn", ";[] lpl", "01204__@@"];
        for key in split_key {
            if let Some((from_key, to_key)) = parts.estimate_split(key) {
                parts.maybe_add(Partition::new(from_key, to_key, key));
            }
        }

        for (_, p) in parts.iter() {
            println!("{:?}", p);
        }
    }

    #[test]
    fn test_overlap() {
        let mut parts = Sparse::default();
        parts.maybe_add(Partition::new(
            Key::left(b"aaaa"),
            Key::right(b"eeee"),
            (),
        ));

        parts.maybe_add(Partition::new(
            Key::left(b"gggg"),
            Key::right(b"kkkk"),
            (),
        ));

        let overlap = parts.is_overlap(
            &Partition::pure("bbbb".."eeee"), |_| true
        );
        assert!(overlap);

        let overlap = parts.is_overlap(
            &Partition::pure("aaaa".."eeee"), |_| false
        );
        assert!(!overlap);

        let overlap = parts.is_overlap(
            &Partition::pure("aaaa".."eeee"), |_| true
        );
        assert!(overlap);

        let overlap = parts.is_overlap_by_range(
            &Key::left("aaaa")..&Key::right("eeee"), |_| false
        );
        assert!(!overlap);

        let overlap = parts.is_overlap_by_range(
            &Key::left("aaaa")..&Key::right("eeee"), |_| true
        );
        assert!(overlap);

        let overlap = parts.is_overlap(
            &Partition::pure("ffff".."gggg"), |_| true
        );
        assert!(!overlap);

        let overlap = parts.is_overlap(
            &Partition::pure("cccc".."hhhh"), |_| true
        );
        assert!(overlap);

        let overlap = parts.is_overlap(
            &Partition::pure("ffff".."hhhh"), |_| true
        );
        assert!(overlap);

        let overlap = parts.is_overlap(
            &Partition::pure("gggg".."hhhh"), |_| true
        );
        assert!(overlap);

        let overlap = parts.is_overlap(
            &Partition::pure("bbbb".."cccc"), |_| true
        );
        assert!(overlap);
    }

    #[test]
    fn test_compact() {
        let mut parts = Sparse::infinity(-1);

        let mut to_compact = vec![];
        for key in 0..10 {
            if let Some((from_key, to_key)) = parts
                .estimate_split(format!("{}", key * 10)) 
            {
                let part = Partition::new(from_key, to_key, key);

                if key > 3 && key <=7 {
                    to_compact.push(part.clone());
                }

                parts.maybe_add(part);
            }
        }

        let _ = parts.compact(&mut to_compact, 999);

        for (_, p) in parts.iter() {
            println!("{} => {}", p, p.resident);
        }
    }

    #[test]
    fn test() {
        let mut index = Sparse::default();
        index.maybe_add(Partition::new(
            Key::left(b"aaaa"),
            Key::right(b"eeee"),
            Value("123"),
        ));

        let _ = index.maybe_add(Partition::new(
            Key::left(b"eeee"),
            Key::right(b"iiii"),
            Value("456"),
        ));

        index.maybe_add(Partition::new(
            Key::left(b"aaaa"),
            Key::right(b"cccc"),
            Value("789"),
        ));

        index.maybe_add(Partition::new(
            Key::left(b"lll"),
            Key::right(b"nnn"),
            Value("abc"),
        ));

        index.maybe_add(Partition::new(
            Key::left(b"tttt"),
            Key::right(b"wwww"),
            Value("def"),
        ));

        assert!(index.gaps().count() == 3);
        assert!(index.list().count() == 5);

        let (from, to) = index.estimate_split(b"uuuu").unwrap();
        index.maybe_add(Partition::new(
            from.clone(),
            to.clone(),
            Value("split"),
        ));

        index.remove_by_range(&from..&to);
        for (s,e) in index.gaps() {
            println!("{s}, {e}");
        }

        let contained = index.contains(&Partition::new(
            Key::left(b"aaaa"),
            Key::right(b"cccc"),
            Value("789")
        ));
        assert!(contained);
    }
}
