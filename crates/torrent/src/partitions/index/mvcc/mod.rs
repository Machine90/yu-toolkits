pub mod timeline;
pub mod version;

use self::{timeline::Timeline, version::Version};
use std::sync::Arc;
use vendor::prelude::lock::{RwLock, RwLockReadGuard};

/// Multi-version concurrency control of partitions,
/// unlike mvcc in others approach, this only suitable
/// for partitons operation, the editing partitions (in range)
/// will be locked while other version couldn't see it.
/// All read operations are allowed for committed partitions.
pub struct Mvcc<R> {
    pub(super) timeline: Arc<RwLock<Timeline<R>>>,
}

impl<R> Clone for Mvcc<R> {
    fn clone(&self) -> Self {
        Self {
            timeline: self.timeline.clone(),
        }
    }
}

impl<R> Mvcc<R> {
    pub fn new() -> Self {
        Self::from_timeline(Timeline::new())
    }

    pub fn from_timeline(tl: Timeline<R>) -> Self {
        Self {
            timeline: Arc::new(RwLock::new(tl)),
        }
    }

    /// Create a new version from current view, this version
    /// allow to change the "unlocked partitions", don't forget
    /// to commit if changes succeeds.
    pub fn new_version(&self) -> Version<R> {
        let mut version = Version::new(self.timeline.clone());
        self.timeline.write().assign(&mut version);
        version
    }

    /// This only for read uncommitted partitions, use `new_version`
    /// if wants to write partitions.
    pub fn current(&self) -> RwLockReadGuard<'_, Timeline<R>> {
        self.timeline.read()
    }
}

#[cfg(all(feature = "tokio1-rt", test))]
pub mod tests {
    use std::collections::HashSet;
    use std::ops::Range;
    use std::time::Duration;

    use super::{timeline::Timeline, version::Version, Mvcc};
    use crate::partitions::{key::Key, partition::Partition};
    use crate::{
        partitions::index::{mvcc::version::RefEdit, sparse::Sparse},
        tokio1::runtime::Runtime,
    };

    #[test]
    pub fn test_mvcc() {
        let mvcc = Mvcc::new();
        let mut version = mvcc.new_version();

        version.maybe_add(Partition::new(Key::left(b"a"), Key::right(b"b"), 1), true);
    }

    #[test]
    pub fn test_mvcc_async() {
        Runtime::new().unwrap().block_on(async move {
            let mut index = Sparse::default();
            index.maybe_add(Partition::from_range("aaa".."ccc", 1));
            index.maybe_add(Partition::from_range("ggg".."kkk", 2));
            index.maybe_add(Partition::from_range("nnn".."ttt", 3));
            index.maybe_add(Partition::from_range("ttt".."uuu", 3));
            index.maybe_add(Partition::from_range("uuu".."vvv", 4));
            index.maybe_add(Partition::from_range("vvv".."www", 4));
            index.maybe_add(Partition::from_range("www".."xxx", 4));
            let mvcc = Mvcc::from_timeline(Timeline::from_exists(index));

            let mut ts = vec![];
            for i in 0..20 {
                let mut version = mvcc.new_version();

                let t = tokio::spawn(async move {
                    if i == 5 {
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    }
                    let to_add = if i != 2 {
                        Partition::from_range("aaa".."bbb", i)
                    } else {
                        Partition::from_range("lll".."mmm", i)
                    };

                    let permit = if [6, 7, 8].contains(&i) {
                        let j = version.find("jjjj");
                        if let Some(j) = j {
                            println!("Jack: {j}");
                        }
                        let compacts = vec![
                            Partition::from_range("uuu".."vvv", 4),
                            Partition::from_range("vvv".."www", 4),
                            Partition::from_range("www".."xxx", 4),
                        ];

                        let p = version.try_compact(&compacts, i, true);
                        println!("[{i}] compact {:?}", p);
                        p.is_ok()
                    } else {
                        version.maybe_add(to_add.clone(), false)
                    };

                    let txid = version.id;
                    for change in version.visit_changes() {
                        match change {
                            RefEdit::Added(p) => {
                                println!("[{txid}] add partition {p}");
                            }
                            RefEdit::Remove(p) => {
                                println!("[{txid}] remove partition {p}");
                            }
                        }
                    }

                    if !permit {
                        return;
                    }

                    let _permit = version.maybe_add(Partition::from_range("nnn".."ppp", i), true);
                    version.commit();
                });

                ts.push(t);
            }

            tokio::time::sleep(std::time::Duration::from_nanos(2)).await;
            for t in ts {
                let _ = t.await;
            }

            println!("========== final ===========");
            for (_, part) in mvcc.current().iter() {
                println!("{part}");
            }
            println!("============================");

            println!("========== gaps ===========");
            for (s, e) in mvcc.current().gaps() {
                println!("gap: [{s}, {e})");
            }
            println!("===========================");
        });
    }

    type GroupID = u32;

    struct GroupProto {
        id: GroupID,
        from_key: Vec<u8>,
        to_key: Vec<u8>,
        voters: HashSet<u64>,
    }

    impl GroupProto {
        pub fn is_voter(&self, id: u64) -> bool {
            self.voters.contains(&id)
        }

        pub fn new(range: Range<&[u8]>, id: GroupID) -> Self {
            let Range { start, end } = range;
            Self {
                id, 
                from_key: start.to_vec(),
                to_key: end.to_vec(),
                voters: [1,2,3].into()
            }
        }
    }

    struct Groups {
        mvcc: Mvcc<GroupID>,
    }

    impl Groups {
        pub fn new(index: Sparse<u32>) -> Self {
            Self {
                mvcc: Mvcc::from_timeline(Timeline::from_exists(index)),
            }
        }

        pub fn txn(&self) -> Txn {
            let version = self.mvcc.new_version();
            Txn { node: 1, version }
        }
    }

    struct Txn {
        node: u64,
        version: Version<GroupID>,
    }

    impl Txn {
        fn split_group(&mut self, left: GroupProto) -> bool {
            let this_node = self.node;
            let new = Partition::new_and_validate(
                Key::left(&left.from_key),
                Key::right(&left.to_key),
                left.id,
            );
            if new.is_err() { return false; }
            let new = new.unwrap();

            if !self.version.maybe_add(new.clone(), true) {
                // maybe failure when other version is editing, or add an unexpected partition
                return false;
            }
            if !left.is_voter(this_node) {
                // this node is not in split group, indicate that should
                // remove group from this node, but keep the divided one.
                if !self.version.try_remove(&new, true).is_ok() {
                    return false;
                }
            }
            true
        }

        fn compact(&mut self, groups: Vec<GroupProto>, new: GroupID) -> bool {
            let compact: Vec<_> = groups.iter().map(|g| {
                    Partition::new(
                        Key::left(&g.from_key), 
                        Key::right(&g.to_key), 
                        g.id
                    )
                }).collect();
            self.version.try_compact(
                &compact, 
                new, 
                true
            ).is_ok()
        }
    }

    #[test]
    fn test_groups() {
        let mut index = Sparse::default();
        index.maybe_add(Partition::from_range("aa".."ee", 1));
        index.maybe_add(Partition::from_range("ee".."kk", 2));
        index.maybe_add(Partition::from_range("oo".."tt", 3));
        index.maybe_add(Partition::from_range("uu".."vv", 4));
        index.maybe_add(Partition::from_range("vv".."ww", 5));
        index.maybe_add(Partition::from_range("ww".."xx", 6));
        index.maybe_add(Partition::from_range("xx".."yy", 7));
        index.maybe_add(Partition::from_range("yy".."zz", 8));
        let groups = Groups::new(index);

        let mut ts = vec![];
        for i in 0..10 {
            let mut txn = groups.txn();

            ts.push(std::thread::spawn(move || {
                let permit = if [2,4,9].contains(&i) {
                    txn.split_group(GroupProto {
                        id: 9,
                        from_key: b"aa".to_vec(),
                        to_key: b"cc".to_vec(),
                        voters: [2, 3, 4].into(),
                    })
                } else if [0,1,5].contains(&i) {
                    txn.split_group(GroupProto {
                        id: 10,
                        from_key: b"oo".to_vec(),
                        to_key: b"rr".to_vec(),
                        voters: [1, 3, 4].into(),
                    })
                } else if [3,6,7].contains(&i) {
                    std::thread::sleep(Duration::from_secs(3));
                    txn.compact(vec![
                        GroupProto::new(b"vv"..b"ww", 5),
                        GroupProto::new(b"ww"..b"xx", 6),
                        GroupProto::new(b"xx"..b"yy", 7),
                        GroupProto::new(b"yy"..b"zz", 8),
                    ], 11)
                } else {
                    println!("read");
                    for (_, p) in unsafe {
                        txn.version.visit_committed()
                    } {
                        println!("{p}");
                    }
                    false
                };

                if permit {
                    txn.version.commit();
                }
            }));
        }

        for t in ts {
            let _ = t.join();
        }

        for (_, p) in groups.mvcc.current().iter() {
            println!("{p} => {:?}", p.resident);
        }
    }
}
