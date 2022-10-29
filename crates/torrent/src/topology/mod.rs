pub mod topo;
pub mod nodes;
pub mod node;

use vendor::prelude::{DashMap, DashSet};

type TopoHashBuilder = std::hash::BuildHasherDefault<fxhash::FxHasher>;
type TashMap<K, V> = DashMap<K, V, TopoHashBuilder>;
type TashSet<K> = DashSet<K, TopoHashBuilder>;
