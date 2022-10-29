use std::{cmp::Ordering, ops::Bound, fmt::Display};

impl std::cmp::PartialEq<Key> for RefKey<'_> {
    fn eq(&self, other: &Key) -> bool {
        match (self, other) {
            (Self::Is(l0), Key::Is(r0)) => l0 == r0,
            (Self::Min, Key::Min) => true, 
            (Self::Max, Key::Max) => true, 
            _ => false
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RefKey<'a> {
    Is(&'a [u8]), Min, Max
}

impl Display for RefKey<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Is(index) => {
                if let Ok(txt) = String::from_utf8(index.to_vec()) {
                    write!(f, "{}", txt)
                } else {
                    f.debug_tuple("Has").field(index).finish()
                }
            },
            Self::Min => write!(f, "Min"),
            Self::Max => write!(f, "Max"),
        }
    }
}

impl<'a> RefKey<'a> {

    #[inline]
    pub fn left(index: &'a [u8]) -> Self {
        if index.is_empty() {
            Self::Min
        } else {
            Self::Is(index)
        }
    }

    #[inline]
    pub fn right(index: &'a [u8]) -> Self {
        if index.is_empty() {
            Self::Max
        } else {
            Self::Is(index)
        }
    }

    #[inline]
    pub fn to_vec(&self) -> Vec<u8> {
        match self {
            RefKey::Is(index) => index.to_vec(),
            _ => Vec::new()
        }
    }

    #[inline]
    pub fn to_left(&self) -> Key {
        match self {
            RefKey::Is(index) => Key::Is(index.to_vec()),
            _ => Key::Min
        }
    }

    #[inline]
    pub fn to_right(&self) -> Key {
        match self {
            RefKey::Is(index) => Key::Is(index.to_vec()),
            _ => Key::Max
        }
    }

    pub fn left_bound(&self) -> Bound<Vec<u8>> {
        match self {
            RefKey::Is(index) => Bound::Included(index.to_vec()),
            _ => Bound::Included(Vec::new())
        }
    }

    pub fn right_bound(&self) -> Bound<Vec<u8>> {
        match self {
            RefKey::Is(index) => Bound::Excluded(index.to_vec()),
            _ => Bound::Unbounded
        }
    }
}

impl<'a> PartialOrd for RefKey<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let this_max = *self == Self::Max;
        let that_max = *other == Self::Max;
        let this_min = *self == Self::Min;
        let that_min = *other == Self::Min;

        if (this_max && that_max) || (this_min && that_min) {
            Some(Ordering::Equal)
        } else if this_max || that_min {
            Some(Ordering::Greater)
        } else if that_max || this_min {
            Some(Ordering::Less)
        } else {
            let this_val = match self {
                RefKey::Is(index) => index,
                RefKey::Max => return None,
                RefKey::Min => return None,
            };
            let other_val = match other {
                RefKey::Is(index) => index,
                RefKey::Max => return None,
                RefKey::Min => return None,
            };
            Some(this_val.cmp(other_val))
        }
    }
}

impl<'a> Ord for RefKey<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Binary key
#[derive(Clone, PartialEq, Eq, Debug, Hash)]
pub enum Key {
    Is(Vec<u8>), Min, Max
}

impl Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Is(index) => {
                if let Ok(txt) = String::from_utf8(index.to_vec()) {
                    write!(f, "{}", txt)
                } else {
                    f.debug_tuple("Has").field(index).finish()
                }
            },
            Self::Min => write!(f, "Min"),
            Self::Max => write!(f, "Max"),
        }
    }
}

impl Key {

    #[inline]
    pub fn left<I: AsRef<[u8]>>(index: I) -> Self {
        let index = index.as_ref().to_vec();
        if index.is_empty() {
            Self::Min
        } else {
            Self::Is(index)
        }
    }

    #[inline]
    pub fn right<I: AsRef<[u8]>>(index: I) -> Self {
        let index = index.as_ref().to_vec();
        if index.is_empty() {
            Self::Max
        } else {
            Self::Is(index)
        }
    }

    pub fn take(self) -> Vec<u8> {
        match self {
            Key::Is(index) => index,
            _ => Vec::new()
        }
    }

    #[inline]
    pub fn as_left(&self) -> RefKey {
        match self {
            Key::Is(index) => RefKey::Is(index),
            _ => RefKey::Min
        }
    }

    #[inline]
    pub fn from_left(key: RefKey) -> Self {
        match key {
            RefKey::Is(index) => Key::Is(index.to_vec()),
            _ => Key::Min
        }
    }

    #[inline]
    pub fn as_right(&self) -> RefKey {
        match self {
            Key::Is(index) => RefKey::Is(index),
            _ => RefKey::Max
        }
    }

    #[inline]
    pub fn from_right(key: RefKey) -> Self {
        match key {
            RefKey::Is(index) => Key::Is(index.to_vec()),
            _ => Key::Max
        }
    }

    pub fn left_bound(self) -> Bound<Vec<u8>> {
        match self {
            Key::Is(index) => Bound::Included(index),
            _ => Bound::Included(Vec::new())
        }
    }

    pub fn right_bound(self) -> Bound<Vec<u8>> {
        match self {
            Key::Is(index) => Bound::Excluded(index),
            _ => Bound::Unbounded
        }
    }
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let this_max = *self == Self::Max;
        let that_max = *other == Self::Max;
        let this_min = *self == Self::Min;
        let that_min = *other == Self::Min;

        if (this_max && that_max) || (this_min && that_min) {
            Some(Ordering::Equal)
        } else if this_max || that_min {
            Some(Ordering::Greater)
        } else if that_max || this_min {
            Some(Ordering::Less)
        } else {
            let this_val = match &self {
                Key::Is(index) => index,
                Key::Max => return None,
                Key::Min => return None,
            };
            let other_val = match other {
                Key::Is(index) => index,
                Key::Max => return None,
                Key::Min => return None,
            };
            Some(this_val.cmp(other_val))
        }
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl AsRef<[u8]> for Key {
    fn as_ref(&self) -> &[u8] {
        match self {
            Key::Is(index) => &index[..],
            _ => &[]
        }
    }
}

impl<'a> AsRef<[u8]> for RefKey<'a> {
    fn as_ref(&self) -> &'a [u8] {
        match self {
            RefKey::Is(index) => &index[..],
            _ => &[]
        }
    }
}