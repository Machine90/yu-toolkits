use std::collections::{HashSet};
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::{fmt::Debug, hash::Hash};

use vendor::prelude::{DashMap, mapref::entry::{Entry}};

use super::node::Node;
use super::{TopoHashBuilder, TashMap};

pub struct Nodes<T>(TashMap<T, (Arc<Node<T>>, AtomicU64)>);

impl<T> Deref for Nodes<T> {
    type Target = TashMap<T, (Arc<Node<T>>, AtomicU64)>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: Debug + Eq + Hash> Debug for Nodes<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Nodes").field(&self.0).finish()
    }
}

impl<T: Clone + Eq + Hash> Clone for Nodes<T> {
    fn clone(&self) -> Self {
        let new_nodes: TashMap<T, (Arc<Node<T>>, AtomicU64)> = 
            DashMap::with_hasher(TopoHashBuilder::default());

        for n in self.0.iter() {
            let (node, ref_cnt) = n.value();
            let node = node.as_ref().clone();
            let ref_cnt = ref_cnt.load(Ordering::Acquire);
            new_nodes.insert(
                n.key().clone(), 
                (Arc::new(node), AtomicU64::new(ref_cnt))
            );
        }
        Self(new_nodes)
    }
}

impl<T: Eq + Hash> Nodes<T> {
    pub fn new() -> Self {
        Self(DashMap::with_hasher(TopoHashBuilder::default()))
    }

    #[inline]
    pub fn node_ref_count(&self, node_id: &T) -> usize {
        self.0
            .get(node_id)
            .map(|node| node.1.load(Ordering::Acquire))
            .unwrap_or(0) as usize
    }
}

impl<T: Eq + Hash + Clone> Nodes<T> {
    /// Add a node to nodes if it's absent, or increase reference
    /// count for it.
    pub fn add_or_borrow(&self, node: Node<T>) -> Arc<Node<T>> {
        match self.0.entry(node.id.clone()) {
            Entry::Occupied(exist) => {
                let (node, refs) = exist.get();
                refs.fetch_add(1, Ordering::Release);
                node.clone()
            }
            Entry::Vacant(new) => {
                let new_node = Arc::new(node);
                new.insert((new_node.clone(), AtomicU64::new(1)));
                new_node
            }
        }
    }

    pub fn node_ids(&self) -> HashSet<T> {
        self.0.iter()
            .map(|n| n.key().clone())
            .collect()
    }
}

impl<T: Eq + Hash> Nodes<T> {
    /// Maybe clear the node from nodes if it's the last
    /// reference of this node, otherwise descrease the 
    /// reference count.
    pub fn maybe_clear(&self, node_id: &T) -> bool {
        self.remove_if(node_id, |_, (_, ref_cnt)| {
            ref_cnt.fetch_sub(1, Ordering::SeqCst) == 1
        }).is_some()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test] 
    fn test_dcr() {
        let nodes = Nodes::new();
        const TOTAL: u64 = 100;
        const REMOVED: u64 = 93;

        for i in 0..TOTAL {
            let id = i % 5;
            let node = Node::parse(id, "rpc://localhost:8080").unwrap();
            nodes.add_or_borrow(node);
        }

        let nodes = Arc::new(nodes);
        let mut tasks = vec![];
        for i in 0..REMOVED {
            let nodes_ref = nodes.clone();
            let t = std::thread::spawn(move || {
                let id = i % 5;
                nodes_ref.maybe_clear(&id);
            });
            tasks.push(t);
        } 
        for t in tasks {
            let _ = t.join();
        }
        let mut remaind = 0;
        for n in nodes.iter() {
            remaind += n.1.load(Ordering::Acquire);
        }
        assert!(remaind == TOTAL - REMOVED);
    }

    #[test]
    #[cfg(feature = "tokio1-rt")]
    fn test() {
        use tokio::{runtime::Runtime, time::Instant};
        use crate::topology::topo::Topology;

        let topo = Arc::new(Topology::new());
        let mut ts = vec![];
        let asynto = topo.clone();
        Runtime::new().unwrap().block_on(async move {
            let t = Instant::now();
            for g in 0..15 {
                let to = asynto.clone();
                ts.push(tokio::spawn(async move {
                    let mut append = to.get_or_add_group(g);
                    for node in 0..10 {
                        append = append.add(Node::parse(node, "http://127.0.0.1:8080").unwrap());
                    }
                }));
            }
            for t in ts {
                let _ = t.await;
            }
            println!("{:?} ms", t.elapsed().as_millis());
        });

        topo.add_node(1, Node::parse(0, "http://127.0.0.1:8080").unwrap());
        topo.add_node(1, Node::parse(1, "http://127.0.0.1:8080").unwrap());
        println!("{:?}", topo.get_group_mut(&1).unwrap().node_num());
    }
}
