use std::{any::Any, fmt::Debug, hash::Hash, ops::Deref, sync::Arc};
use dashmap::{DashMap, mapref::entry::Entry};

pub type Value = dyn Any + Send + Sync;

/// ## Description
/// The `WildMap` allow developers insert value in `Any` type. And also
/// listening changed of the key-value pairs.
#[derive(Default)]
pub struct WildMap<K: Eq + Hash> {
    inner: DashMap<K, Arc<Value>>,
    changed: Option<Arc<dyn Fn(K, Arc<Value>) + Send + Sync + 'static>>,
}

impl<K: Eq + Hash + Debug> Debug for WildMap<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WildMap")
            .field("inner", &self.inner)
            .finish()
    }
}

impl<K: Eq + Hash + Debug + Clone> Deref for WildMap<K> {
    type Target = DashMap<K, Arc<Value>>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[allow(unused)]
impl<K> WildMap<K>
where
    K: Eq + Hash + Debug + Clone,
{
    pub fn new() -> Self {
        Self { inner: DashMap::new(), changed: None }
    }

    pub fn with_aware<F>(changed: F) -> Self
    where
        F: Fn(K, Arc<Value>) + Send + Sync + 'static,
    {
        let inner: DashMap<K, Arc<Value>> = DashMap::default();
        WildMap {
            inner,
            changed: Some(Arc::new(changed)),
        }
    }

    pub fn listening<F>(&mut self, changed: F)
    where
        F: Fn(K, Arc<Value>) + Send + Sync + 'static,
    {
        self.changed = Some(Arc::new(changed));
    }

    /// Insert key value (as reference) to inner,
    /// then return current value's refence if existed.
    /// If overwrite is true, then force to insert value 
    /// first, then return inserted value's ref.
    #[inline]
    pub fn insert<V: Any + Send + Sync>(&self, key: K, val: V, overwrite: bool) -> Arc<V> {
        let mut new_value = Arc::new(val);
        match self.inner.entry(key) {
            Entry::Occupied(mut entry) => {
                if overwrite {
                    entry.insert(new_value.clone());
                } else {
                    let val = entry.get().clone().downcast::<V>();
                    if let Err(e) = val {
                        entry.insert(new_value.clone());
                    } else {
                        new_value = val.unwrap();
                    }
                }
            },
            Entry::Vacant(mut entry) => {
                entry.insert(new_value.clone());
            },
        }
        new_value
    }

    pub fn change<V: Any + Send + Sync>(&self, key: K, val: V) {
        let for_update = Arc::new(val);
        self.inner
            .entry(key.clone())
            .and_modify(|v| *v = for_update);
        let v = self.get::<V>(key.clone());
        if let Some(v) = v {
            let change_aware = self.changed.clone();
            if let Some(change_aware) = change_aware {
                change_aware(key, v);
            }
        }
    }

    pub fn get<V: Any + Send + Sync>(&self, key: K) -> Option<Arc<V>> {
        let val = self.inner.get(&key);
        if val.is_none() {
            return None;
        }
        let val = val.unwrap().value().clone();
        let v = val.downcast::<V>();
        if let Ok(v) = v {
            return Some(v);
        } else {
            return None;
        }
    }

    pub fn get_or<V: Any + Send + Sync>(&self, key: K, default: V) -> Arc<V> {
        let val = self.inner.get(&key);
        let default = Arc::new(default);
        if val.is_none() {
            return default;
        }
        let val = val.unwrap().value().clone();
        let v = val.downcast::<V>();
        if let Ok(v) = v {
            return v;
        } else {
            return default;
        }
    }

    #[inline]
    pub fn contains_key(&self, key: K) -> bool {
        self.inner.contains_key(&key)
    }
}

#[allow(unused)]
fn try_convert<V1, V2>(v1: &V1, v2: &V2)
where
    V1: Any,
    V2: Any,
{
    // todo
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    pub fn test_conf() {
        #[derive(Debug)]
        struct Color(u8, u8, u8);
        let conf = WildMap::with_aware(move |k: String, v| {
            if k.eq("test.conf.struct") {
                let v = v.downcast_ref::<Color>();
                println!("changing: {:?}", (&k, v));
            } else if k.eq("test.conf.int") {
                let v = v.downcast_ref::<u64>();
                println!("awww!!! u update: {:?}", (&k, v))
            }
        });

        conf.insert("test.conf.struct".to_owned(), Color(255, 255, 0), true);
        println!("{:?}", conf.get::<Color>("test.conf.struct".to_owned()));

        conf.change("test.conf.struct".to_owned(), Color(0, 255, 0));

        conf.insert("test.conf.int".to_owned(), 64u64, true);
        conf.change("test.conf.int".to_owned(), 128u64);
        conf.insert("test.conf.str".to_owned(), "str", true);
        let v = *conf.get_or("test.conf.int".to_owned(), 0u64).as_ref();
        println!("get v: {:?}", v);
        let s = conf.get::<u64>("test.conf.int".to_owned());
        println!("value(u64): {:?}", s.unwrap());

        let s = conf.get::<u16>("test.conf.u16".to_owned());
        println!("value(u16): {:?}", s.unwrap_or(Arc::new(16u16)));

        let s = conf.get::<String>("test.conf.strings".to_owned());
        println!("value: {:?}", s.unwrap_or(Arc::new(String::from("asd"))));
        let s = conf.get::<&'static str>("test.conf.str".to_owned());
        println!("value: {:?}", s.unwrap());

        conf.insert("test.conf.string".to_owned(), "String".to_owned(), true);
        let c = Arc::new(conf);
        let s = c.get::<String>("test.conf.string".to_owned());
        println!("arc: {:?}", s);

        let mut pool = vec![];
        for i in 0..10u8 {
            let c = c.clone();
            let h = std::thread::spawn(move || {
                let s = c.get::<String>("test.conf.string".to_owned());
                if let Some(s) = s {
                    let mut s = (*s).clone();
                    s.push_str(": ");
                    s.push((i + 'a' as u8) as char);
                    println!("arc{:?}: {:?}", i, s);
                }
            });
            pool.push(Box::new(h));
        }

        for t in pool {
            let _ = t.join();
        }
    }
}
