// experimental feature
#[allow(unused)] mod diff;

use std::collections::{HashMap, HashSet};
use std::{fmt::Debug, hash::Hash, sync::Arc};

use vendor::prelude::{DashMap, DashSet, mapref::{entry::{Entry, OccupiedEntry}, one::RefMut}};

use super::{TopoHashBuilder, TashMap, TashSet, node::Node, nodes::Nodes};

////////////////////////////////////////
///          For group operations
////////////////////////////////////////
pub struct OccupiedGroup<'a, G, T> {
    node_index: OccupiedEntry<'a, G, TashSet<T>, TopoHashBuilder>,
    nodes: &'a Nodes<T>,
}

impl<'a, G: Eq + Hash, T: Eq + Hash + Clone> OccupiedGroup<'a, G, T> {
    pub fn add(mut self, node: Node<T>) -> Self {
        let update_group = self.node_index.get_mut();
        if update_group.contains(&node.id) {
            return self;
        }
        let node = self.nodes.add_or_borrow(node);
        update_group.insert(node.id.clone());
        self
    }
}

impl<'a, G: Eq + Hash, T: Eq + Hash> OccupiedGroup<'a, G, T> {
    pub fn get(&self, node: &T) -> Option<Arc<Node<T>>> {
        if !self.node_index.get().contains(node) {
            return None;
        }

        self.nodes.get(node).map(|node| node.value().0.clone())
    }

    pub(super) fn remove(&self, node: &T) -> Option<(T, bool)> {
        if let Some(node) = self.node_index.get().remove(node) {
            let clear = self.nodes.maybe_clear(&node);
            Some((node, clear))
        } else {
            None
        }
    }

    #[inline]
    pub fn node_num(&self) -> usize {
        self.node_index.get().len()
    }
}

pub struct MutGroup<'a, G, T> {
    node_index: RefMut<'a, G, TashSet<T>, TopoHashBuilder>,
    nodes: &'a Nodes<T>,
}

impl<'a, G: Eq + Hash, T: Eq + Hash + Clone> MutGroup<'a, G, T> {
    pub fn add(self, node: Node<T>) -> Self {
        if self.node_index.contains(&node.id) {
            return self;
        }
        let node = self.nodes.add_or_borrow(node);
        self.node_index.insert(node.id.clone());
        self
    }
}

impl<'a, G: Eq + Hash, T: Eq + Hash> MutGroup<'a, G, T> {
    pub fn get(&self, node: &T) -> Option<Arc<Node<T>>> {
        if !self.node_index.contains(node) {
            return None;
        }

        self.nodes.get(node).map(|node| node.value().0.clone())
    }

    pub(super) fn remove(&self, node: &T) -> Option<(T, bool)> {
        if let Some(node) = self.node_index.remove(node) {
            let clear = self.nodes.maybe_clear(&node);
            Some((node, clear))
        } else {
            None
        }
    }

    #[inline]
    pub fn node_num(&self) -> usize {
        self.node_index.len()
    }
}

pub enum Group<'a, G, T> {
    Exist(OccupiedGroup<'a, G, T>),
    Mut(MutGroup<'a, G, T>),
}

impl<'a, G: Eq + Hash, T: Eq + Hash + Clone> Group<'a, G, T> {
    #[inline]
    pub fn add(self, node: Node<T>) -> Self {
        match self {
            Group::Exist(group) => Group::Exist(group.add(node)),
            Group::Mut(group) => Group::Mut(group.add(node)),
        }
    }
}

impl<'a, G: Eq + Hash, T: Eq + Hash> Group<'a, G, T> {
    /// Try get node reference from current group.
    #[inline]
    pub fn get(&self, node: &T) -> Option<Arc<Node<T>>> {
        match self {
            Group::Exist(group) => group.get(node),
            Group::Mut(group) => group.get(node),
        }
    }

    /// Remove specific node from current group.
    #[inline]
    pub(super) fn remove(&self, node: &T) -> Option<(T, bool)> {
        match self {
            Group::Exist(group) => group.remove(node),
            Group::Mut(group) => group.remove(node),
        }
    }

    /// Get the nodes number of this group.
    #[inline]
    pub fn node_num(&self) -> usize {
        match self {
            Group::Exist(group) => group.node_num(),
            Group::Mut(group) => group.node_num(),
        }
    }

    /// Iterate the nodes in group with scanner. If scanner return false, 
    /// then stop scan.
    pub fn scan_nodes(&self, mut scanner: impl FnMut(&Node<T>) -> bool) {
        let (id_iter, nodes) = match self {
            Group::Exist(group) => (group.node_index.get().iter(), group.nodes),
            Group::Mut(group) => (group.node_index.iter(), group.nodes),
        };
        for node in id_iter {
            if let Some(node) = nodes.get(node.key()) {
                if !scanner(node.0.as_ref()) {
                    break;
                }
            }
        }
    }

    /// Don't forget to release the MutGroup in case the 
    /// deadlock of the group.
    #[inline]
    pub fn finish(self) {
        drop(self);
    }
}

////////////////////////////////////////
///          Topology for Node.
////////////////////////////////////////
pub struct Topology<G, T> {
    index: TashMap<G, TashSet<T>>,
    nodes: Nodes<T>,
}

impl<G: Debug + Eq + Hash, T: Debug + Eq + Hash> Debug for Topology<G, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Topology")
            .field("index", &self.index)
            .field("nodes", &self.nodes)
            .finish()
    }
}

impl<G: Clone + Eq + Hash, T: Clone + Eq + Hash> Clone for Topology<G, T> {
    fn clone(&self) -> Self {
        Self { 
            index: self.index.clone(), 
            nodes: self.nodes.clone(), 
        }
    }
}

impl<G: Eq + Hash, T: Eq + Hash> Topology<G, T> {
    pub fn new() -> Self {
        Topology {
            index: DashMap::with_hasher(TopoHashBuilder::default()),
            nodes: Nodes::new(),
        }
    }

    /// Get reference of total unique nodes in topo.
    #[inline] pub(crate) fn nodes<'a>(&'a self) -> &Nodes<T> {
        &self.nodes
    }

    #[inline]
    pub fn contained_group(&self, group: &G) -> bool {
        self.index.contains_key(group)
    }

    pub fn contained_group_node(&self, group: &G, node: &T) -> bool {
        let group = self.index.get(group);
        if group.is_none() || !group.map(|nodes| nodes.contains(node)).unwrap_or(false) {
            return false;
        }
        true
    }

    /// Try to get a node reference from specific group.
    pub fn get_group_node(&self, group: &G, node: &T) -> Option<Arc<Node<T>>> {
        if !self.contained_group_node(group, node) {
            return None;
        }
        self.nodes.get(node).map(|node| node.value().0.clone())
    }

    /// Try to get a node from whole topology.
    pub fn get_node(&self, node: &T) -> Option<Arc<Node<T>>> {
        self.nodes().get(&node).map(|node| node.0.clone())
    }

    /// Get reference count of node. If return 0
    /// means this node not exists on topology.
    #[inline]
    pub fn node_ref_count(&self, node: &T) -> usize {
        self.nodes().node_ref_count(node)
    }

    #[inline]
    pub fn get_group_mut(&self, group: &G) -> Option<Group<G, T>> {
        self.index.get_mut(group).map(|group| {
            Group::Mut(MutGroup {
                node_index: group,
                nodes: &self.nodes,
            })
        })
    }

    /// Try remove node from topology, if all node references
    /// by topology has been removed, then clear it from nodes,
    /// and return true in returns.
    /// ### Returns
    /// * (T, bool): (removed node id, has been clear?)
    #[inline]
    pub fn remove_node(&self, group_id: &G, node: &T) -> Option<(T, bool)> {
        let remove = self.get_group_mut(group_id)
            .and_then(|group| group.remove(node));
        self.index.remove_if(group_id, |_, nodes| {
            nodes.is_empty()
        });
        remove
    }

    pub fn remove_group(&self, group_id: &G) -> Option<G> {
        let (group, nodes) = self.index.remove(group_id)?;
        for n in nodes {
            self.nodes.maybe_clear(&n);
        }
        Some(group)
    }

    #[inline]
    pub fn group_num(&self) -> usize {
        self.index.len()
    }

    /// Total nodes number, not computed from each group's nodes.
    #[inline]
    pub fn node_num(&self) -> usize {
        self.nodes.len()
    }

    #[inline]
    pub fn contained_node(&self, node: &T) -> bool {
        self.nodes.contains_key(node)
    }

    pub fn has_same_nodes(&self, group: &G, node_indexes: &HashSet<T>) -> bool {
        if let Some(nodes) = self.index.get(group) {
            for node in nodes.iter() {
                if !node_indexes.contains(node.key()) {
                    return false;
                }
            }
            true
        } else {
            false
        }
    }
}

impl<G: Eq + Hash, T: Eq + Hash + Clone> Topology<G, T> {
    pub fn copy_group_node_ids(&self, group: &G) -> Option<HashSet<T>> {
        self.index.get(group).map(|nodes| {
            let mut collect = HashSet::with_capacity(nodes.len());
            for n in nodes.iter() {
                collect.insert(n.key().clone());
            }
            collect
        })
    }
}

impl<G: Eq + Hash + Clone, T: Eq + Hash + Clone> Topology<G, T> {

    /// Deep copy "Group" and "Node" information of this topology 
    /// without any endpoints copy.
    pub fn copy_index_table(&self) -> HashMap<G, HashSet<T>> {
        let mut collect = HashMap::with_capacity(self.index.len());
        for group in self.index.iter() {
            let mut set = HashSet::with_capacity(group.value().len());
            for n in group.value().iter() {
                set.insert(n.clone());
            }
            collect.insert(group.key().clone(), set);
        }
        collect
    }

    pub fn get_or_add_group(&self, group: G) -> Group<G, T> {
        match self.index.entry(group) {
            Entry::Occupied(occupied) => {
                let node_index = occupied;
                Group::Exist(OccupiedGroup {
                    node_index,
                    nodes: &self.nodes,
                })
            }
            Entry::Vacant(vacant) => {
                let node_index = vacant.insert(DashSet::with_hasher(TopoHashBuilder::default()));
                Group::Mut(MutGroup {
                    node_index,
                    nodes: &self.nodes,
                })
            }
        }
    }

    pub fn add_node(&self, group: G, node: Node<T>) -> Arc<Node<T>> {
        match self.index.entry(group) {
            Entry::Occupied(mut group) => {
                let update_group = group.get_mut();
                if update_group.contains(&node.id) {
                    let node = self.nodes.get(&node.id);
                    if node.is_none() {
                        panic!("node should never be null if it's in index table");
                    }
                    return node.unwrap().0.clone();
                }
                let node = self.nodes.add_or_borrow(node);
                update_group.insert(node.id.clone());
                node
            }
            Entry::Vacant(group) => {
                let nodes = DashSet::with_hasher(TopoHashBuilder::default());
                let node = self.nodes.add_or_borrow(node);
                nodes.insert(node.id.clone());
                group.insert(nodes);
                node
            }
        }
    }
}
