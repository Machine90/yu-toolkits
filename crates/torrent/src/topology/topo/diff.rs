use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::{fmt::Debug, hash::Hash};

use crate::topology::node::Node;
use super::Topology;


////////////////////////////////////
///       experimental features
////////////////////////////////////

/// Diff is an importance feature for Topology. Imagine 
/// a scenario, suppose there has endpoint A, endpoint B and 
/// endpoint A wants to report it's topology to another 
/// endpoint B, and the topology on endpoint A is large enough, 
/// assume it has more than 100_000 groups and each 
/// group has 5 nodes. There has 2 ways to report this 
/// topo, the 1 is to report whole topo each time, another
/// way is to report incremental, obviously way 1 is 
/// better, but it require the topo support diff feature.
#[derive(Deserialize, Serialize, Debug)]
pub(self) struct Diff<G: Eq + Hash, T: Default + Clone + Eq + Hash> {
    pub incoming: HashMap<G, HashSet<T>>,
    pub outgoing: HashMap<G, HashSet<T>>,
    pub incoming_nodes: HashMap<T, Node<T>>,
    pub outgoing_nodes: HashSet<T>,
}

impl<G: Eq + Hash, T: Default + Clone + Eq + Hash> Diff<G, T> {
    pub(self) fn is_empty(&self) -> bool {
        self.incoming.is_empty() && self.outgoing.is_empty() && 
        self.incoming_nodes.is_empty() && self.outgoing_nodes.is_empty()
    }
}

impl<G: Eq + Hash + Clone, T: Eq + Hash + Clone + Default> Topology<G, T> {

    pub(self) fn diff(&self, old_version: &Self) -> Diff<G, T> {
        let Self { 
            index: old_idx,
            nodes: that_ns,
            .. 
        } = old_version;
        let Self { 
            index: cur_idx, 
            nodes: this_ns,
            .. 
        } = self;
        
        let mut incoming = HashMap::new();
        let mut outgoing = HashMap::new();

        for ent in old_idx.iter() {
            let g = ent.key();
            if !cur_idx.contains_key(g) {
                outgoing.insert(g.clone(), HashSet::new());
            }
        }

        for ent in cur_idx.iter() {
            let g = ent.key();
            if !old_idx.contains_key(g) {
                let n: HashSet<T> = ent.value()
                    .iter()
                    .map(|n| n.clone())
                    .collect();
                incoming.insert(g.clone(), n);
                continue;
            }

            let that_idx_n = old_idx.get(g).unwrap();
            let this_idx_n = ent.value();
            
            let mut ns_in = HashSet::<T>::new();

            for n in this_idx_n.iter() {
                if !that_idx_n.contains(n.key()) {
                    ns_in.insert(n.clone());
                }
            }

            if !ns_in.is_empty() {
                incoming.insert(g.clone(), ns_in);
            }

            let mut ns_out = HashSet::<T>::new();
            for n in that_idx_n.iter() {
                if !this_idx_n.contains(n.key()) {
                    ns_out.insert(n.clone());
                }
            }

            if !ns_out.is_empty() {
                outgoing.insert(g.clone(), ns_out);
            }
        }

        let cur = this_ns.node_ids();
        let old = that_ns.node_ids();

        let mut incoming_nodes = HashMap::with_capacity(cur.len() / 10);
        for incoming in cur.difference(&old) {
            let node = this_ns.get(incoming)
                .map(|v| v.0.as_ref().clone());
            if let Some(node) = node {
                incoming_nodes.insert(incoming.clone(), node);
            }
        }

        let mut outgoing_nodes = HashSet::with_capacity(old.len() / 10);
        for outgoing in old.difference(&cur) {
            outgoing_nodes.insert(outgoing.clone());
        }

        Diff {
            incoming, 
            outgoing, 
            incoming_nodes, 
            outgoing_nodes
        }
    }

    pub(self) fn apply_diff(&self, diff: Diff<G, T>) {
        let Diff { 
            incoming, 
            outgoing, 
            incoming_nodes, 
            outgoing_nodes 
        } = diff;

        // todo!("apply")
    }
}

#[cfg(test)] mod tests {
    use std::{time::Instant, collections::{HashMap, HashSet}};

    use crate::topology::node::Node;

    use super::Topology;

    #[test] fn test_diff() {
        
        let topo = Topology::new();
        let mut nodes: Vec<Node<u32>> = Vec::with_capacity(100);
        for i in 0..100 {
            let n = Node::parse(i as u32, "http://127.0.0.1:8080").unwrap();
            nodes.push(n);
        }

        let mut inst = Instant::now();
        for i in 0..5000 {
            let mut group = topo.get_or_add_group(i);
            for j in 0..3 {
                group = group
                    .add(nodes[((i + j) % 100) as usize].clone());
            }
            group.finish();
        }
        println!("added {:?}ms", inst.elapsed().as_millis());

        inst = Instant::now();
        let snap = topo.clone();
        println!("cloned {:?}ms", inst.elapsed().as_millis());

        topo.add_node(
            10010, 
            Node::parse(71, "http://127.0.0.1:8081").unwrap()
        );
        topo.get_or_add_group(20100)
            .add(Node::parse(77, "http://127.0.0.1:8081").unwrap());
        topo.get_or_add_group(8888).remove(&38);
        topo.remove_group(&2);
        snap.remove_group(&1);

        inst = Instant::now();
        let diff = topo.diff(&snap);
        println!("diff {:?}ms\n{:#?}", inst.elapsed().as_millis(), diff);

        snap.apply_diff(diff);

    }
}
