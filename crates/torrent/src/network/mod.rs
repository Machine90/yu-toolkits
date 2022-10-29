pub mod connections;
pub mod network;
#[cfg(feature = "tokio1-rt")] pub mod network_async;



use std::{sync::Arc, net::SocketAddr};
use vendor::prelude::DashSet;
use crate::topology::topo::Topology;
use self::connections::connection_pool::Connections;

/// ## Description
/// Connections with network topology.
/// Developer can give topo first, then register connections later after all peers has been initialized.
/// Cluster always with more than one peer, and `Connections` should master all peers' long-connection and
/// address.
/// ## Diagram
/// The relationship between `connections` and `topology` can be explain as:
/// ```mermaid
/// flowchart LR
/// subgraph Cluster
/// subgraph Topology
/// subgraph Group1
/// 	Node1
/// 	subgraph e2[Node2]
/// 		s1[http socket:\nlocalhost:80]
/// 	end
/// 	Node3
/// end
/// subgraph Group2
/// 	subgraph e5[Node5]
/// 		s21[http socket:\nlocalhost:80]
/// 	end
/// 	Node6
/// 	Node7
/// end
/// end
///
/// subgraph Connections
/// 	c1((connection1\nlocalhost:80))
/// 	c2((connection2))
/// 	c3((connection3))
/// end
/// e2 ==> c1
/// e5 ==> c1
/// end
/// ```
///
/// ## Example
/// ```rust
/// let topo = Topology::<u64, u64>::new();
/// let n1 = Node::parse(1, "rpc://127.0.0.1:50011").unwrap();
/// let n2 = Node::parse(2, "rpc://127.0.0.1:50012").unwrap();
/// let n3 = Node::parse(3, "rpc://127.0.0.1:50013").unwrap();
/// let n4 = Node::parse(4, "rpc://127.0.0.1:50014").unwrap();
/// topo.add_group(1)
///     .add(n1)
///     .add(n2)
///     .add(n3)
///     .add(n4);
/// let cluster = Network::restore(topo);
/// ```
pub struct Network<G, T> {
    topo: Arc<Topology<G, T>>,
    conns: Arc<Connections>,
    pending_connect: DashSet<SocketAddr>,
}

impl<G, T> std::ops::Deref for Network<G, T> {
    type Target = Topology<G, T>;

    fn deref(&self) -> &Self::Target {
        self.topo.as_ref()
    }
}
