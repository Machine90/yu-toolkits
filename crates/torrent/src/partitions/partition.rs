use std::{
    fmt::{Debug, Display},
    io::{Result, Error, ErrorKind}, ops, hash::Hash,
};

use super::{key::{Key, RefKey}};

pub struct Partition<R> {
    pub from_key: Key,
    pub to_key: Key,
    pub resident: R
}

impl<R: Clone> Clone for Partition<R> {
    fn clone(&self) -> Self {
        Self { 
            from_key: self.from_key.clone(), 
            to_key: self.to_key.clone(), 
            resident: self.resident.clone(), 
        }
    }
}

impl<R: Debug> Debug for Partition<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Partition")
            .field("from_key", &self.from_key)
            .field("to_key", &self.to_key)
            .field("resident", &self.resident)
            .finish()
    }
}

impl<R> Display for Partition<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = &self.from_key;
        let e = &self.to_key;
        write!(f, "[{s}, {e})")
    }
}

impl<R> std::ops::Deref for Partition<R> {
    type Target = R;

    fn deref(&self) -> &Self::Target {
        &self.resident
    }
}

impl<R> std::ops::DerefMut for Partition<R> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.resident
    }
}

impl<R> PartialEq for Partition<R>  {
    fn eq(&self, other: &Self) -> bool {
        self.from_key == other.from_key && self.to_key == other.to_key
    }
}

impl<R> Eq for Partition<R>  {}

impl<R> Hash for Partition<R> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.from_key.hash(state);
        self.to_key.hash(state);
    }
}

impl<R: Default> Partition<R> {

    #[inline]
    pub fn pure<K: AsRef<[u8]>>(range: ops::Range<K>) -> Self {
        Self::from_range(range, R::default())
    }
}

impl<R> Partition<R> {
    pub fn from_range<K: AsRef<[u8]>>(range: ops::Range<K>, resident: R) -> Self {
        let ops::Range { start, end } = range;
        Self {
            from_key: Key::left(start), 
            to_key: Key::right(end), 
            resident
        }
    }

    pub fn new(from_key: Key, to_key: Key, resident: R) -> Self {
        let this = Self::new_and_validate(from_key, to_key, resident);
        if let Err(e) = this {
            panic!("{e}");
        }
        this.unwrap()
    }

    pub fn new_and_validate(from_key: Key, to_key: Key, resident: R) -> Result<Self> {
        let this = Self {
            from_key,
            to_key,
            resident,
        };
        this.validate()?;
        Ok(this)
    }

    pub fn validate(&self) -> Result<()> {
        let Self { from_key, to_key, .. } = self;
        if from_key < to_key {
            return Ok(());
        }
        Err(Error::new(
            ErrorKind::InvalidInput, 
            "unexpected key range of partition, `from_key` should never larger than `to_key`"
        ))
    }

    #[inline]
    pub fn update_bound(&mut self, from: RefKey<'_>, to: RefKey<'_>) -> bool {
        self.update_left(from) || self.update_right(to)
    }

    pub fn update_left(&mut self, from: RefKey<'_>) -> bool {
        if from >= self.to_key.as_right() || from == self.from_key.as_left() {
            false
        } else {
            self.from_key = from.to_left();
            true
        }
    }

    pub fn update_right(&mut self, to: RefKey<'_>) -> bool {
        if to <= self.from_key.as_left() || to == self.to_key.as_right() {
            false
        } else {
            self.to_key = to.to_right();
            true
        }
    }

    #[inline]
    pub fn replace(&mut self, resident: R) {
        self.resident = resident;
    }

    /// Determine if given key is in contained in this `partition`
    pub fn contains(&self, key: RefKey<'_>) -> Result<()> {
        if key >= self.from_key.as_left() && key < self.to_key.as_right() {
            Ok(())
        } else {
            Err(Error::new(
                ErrorKind::InvalidInput, 
                format!(
                    "key: {:?} out of boundary, expected [{:?}, {:?})", 
                    key, self.from_key, self.to_key
                ))
            )
        }
    }

    pub fn is(&self, rs: Relationship, that: &Self) -> bool {
        let Partition { from_key: my_fk, to_key: my_tk, .. } = self;
        let Partition { from_key: that_fk, to_key: that_tk, .. } = that;

        match rs {
            Relationship::LeftPartOf => {
                my_fk == that_fk && my_tk < that_tk
            },
            Relationship::Contains => {
                (my_fk <= that_fk && my_tk > that_tk) || (my_tk >= that_tk && my_fk < that_fk)
            },
            Relationship::ContainedIn => {
                (my_fk >= that_fk && my_tk < that_tk) || (my_tk <= that_tk && my_fk > that_fk)
            },
            Relationship::Eq => {
                my_fk == that_fk && my_tk == that_tk
            },
            Relationship::LeftOf => {
                my_tk == that_fk
            },
            Relationship::RightOf => {
                my_fk == that_tk
            },
        }
    }

    /// Accept an unsorted partitions and sort it first, then 
    /// detect if they're continuous.
    #[inline]
    pub fn maybe_continuous(unsorted: &mut Vec<Self>) -> bool {
        if unsorted.len() <= 1 {
            return true;
        }
        Self::sort(unsorted);
        Self::is_continuous(unsorted)
    }

    pub fn sort(unsorted: &mut Vec<Self>) {
        unsorted.sort_by(|this, that| {
            this.from_key.cmp(&that.from_key)
        });
    }

    /// Detect if a sorted partitions is continuous.
    pub fn is_continuous(sorted: &Vec<Self>) -> bool {
        if sorted.len() <= 1 {
            return true;
        }
        let mut last = &sorted[0];
        for i in 1..sorted.len() {
            let current = &sorted[i];
            if !last.is(Relationship::LeftOf, current) {
                return false;
            }
            last = current;
        }
        true
    }
}

pub enum Relationship {
    /// When this: [0, 50), that: [50, 100), this is `LeftOf` of that.
    LeftOf,
    /// When this: [100, 150), that: [50, 100), this is `RightOf` of that.
    RightOf,
    /// When this: [0, 50), that: [0, 100), this is `LeftPartOf` of that.
    LeftPartOf, 
    /// When this: [0, 100), that: [20, 80), this `Contains` that.
    Contains, 
    /// When this: [20, 50), that: [0, 100), this `ContainedIn` that.
    ContainedIn, 
    /// When this: [0, 50), that: [0, 50), this `Eq` that. 
    /// This approach equal to `partition_1 == partition_2`
    Eq
}
