use std::{
    fmt::Debug,
    hash::Hash,
    io::{Error, ErrorKind, Result},
    net::SocketAddr,
    sync::Arc,
};

use vendor::prelude::DashSet;

use super::{
    connections::{connection::Connection, connection_pool::Connections},
    Network,
};
use crate::topology::topo::Topology;

impl<G: Eq + Hash, T: Eq + Hash> Network<G, T> {
    pub fn new() -> Self {
        Self {
            topo: Arc::new(Topology::new()),
            conns: Arc::new(Connections::new()),
            pending_connect: DashSet::new(),
        }
    }

    pub fn restore(topo: Topology<G, T>) -> Self {
        Self {
            topo: Arc::new(topo),
            conns: Arc::new(Connections::new()),
            pending_connect: DashSet::new(),
        }
    }

    /// connections of this cluster.
    #[inline]
    pub fn get_conns(&self) -> &Connections {
        self.conns.as_ref()
    }

    /// Get topology reference of cluster without cloning.
    #[inline]
    pub fn get_topo<'a>(&'a self) -> &'a Topology<G, T> {
        self.topo.as_ref()
    }

    /// Get the node's socket address from topo, unlike `get_node_addr`,
    /// this method will check if the group and node contained in topo
    /// first.
    #[inline]
    pub fn get_peer_addr(&self, group: &G, node: &T, scheme: &str) -> Option<SocketAddr> {
        self.topo.get_group_node(group, node).map(|n| n.socket(scheme))?
    }

    /// Get the socket address of scheme from a node directly.
    #[inline]
    pub fn get_node_addr(&self, node: &T, scheme: &str) -> Option<SocketAddr> {
        self.topo.nodes()
            .get(node)
            .map(|node| node.value().0.socket(scheme))?
    }

    pub fn maybe_get_client<C: Send + Sync + 'static + Clone>(
        &self,
        group: &G,
        node: &T,
        scheme: &str,
    ) -> Option<C> {
        let socket_addr = self.get_peer_addr(group, node, scheme)?;
        self.conns.maybe_get_client(socket_addr)
    }

    pub fn maybe_get_node_client<C: Send + Sync + 'static + Clone>(
        &self,
        node: &T,
        scheme: &str,
    ) -> Option<C> {
        let socket_addr = self.get_node_addr(node, scheme)?;
        self.conns.maybe_get_client(socket_addr)
    }

    /// Register connector for given node without trying to connect to it 
    /// immediately, return true if node is present in network.
    pub fn register_connector<P, C>(
        &self,
        group: &G,
        node: &T,
        scheme: &str,
        force_new: bool,
        connector: P,
    ) -> bool
    where
        C: Send + Sync + 'static + Clone,
        P: Fn(SocketAddr) -> Result<C> + Send + Sync + 'static,
    {
        let socket = self.get_peer_addr(group, node, scheme);
        if socket.is_none() {
            return false;
        }
        self.conns
            .push_client(socket.unwrap(), force_new, connector);
        true
    }

    /// Register connector to all peers in all groups.
    /// The connector will be used to establish connection to
    /// target peer, and reconnect when calling
    /// `reconnect<C>(&self, group: G, node: T, scheme: &str)`
    /// ### Params
    /// * **connector**: connector is a closure that can be used to establish a connection to target peer,
    /// this closure need return an abstract connection `C`, means that if invoke connector, we should got
    /// a connection, for example a http client.
    pub fn register_connector_all<P, C>(
        &self,
        scheme: &str,
        force: bool,
        connect_immediately: bool,
        connector: P,
    ) where
        C: Send + Sync + 'static + Clone,
        P: Fn(SocketAddr) -> Result<C> + Send + Sync + 'static,
    {
        let provider = Arc::new(connector);
        let mut tasks = vec![];

        for node in self.topo.nodes().iter() {
            let node = node.0.as_ref();
            let socket = node.socket(scheme);
            if socket.is_none() {
                continue;
            }
            let socket_addr = socket.unwrap();
            if !force && !self._try_connecting(socket_addr) {
                continue;
            }

            let connector = provider.clone();
            tasks.push(std::thread::spawn(move || {
                Connection::with_connector_ref(socket_addr, connector, connect_immediately)
            }));
        }
        for task in tasks {
            if let Ok(connection) = task.join() {
                let socket = connection.socket;
                let _ = self.conns.add_connection(connection);
                self._release_connecting(socket);
            }
        }
    }

    /// try create a connecting to prevent repeat connecting,
    /// don't forget to release this connecting after success.
    /// ### Returns:
    /// * true: if the connection not exists before && occupy connecting lock success.
    /// * false: connection already in connection pool or socket is in connecting.
    #[inline]
    pub(super) fn _try_connecting(&self, socket: SocketAddr) -> bool {
        !self.conns.contains(socket) && self.pending_connect.insert(socket)
    }

    #[inline]
    pub(super) fn _release_connecting(&self, socket: SocketAddr) {
        self.pending_connect.remove(&socket);
    }
}

impl<G: Eq + Hash + Debug, T: Eq + Hash + Debug> Network<G, T> {
    /// Reconnect specific peer's connection in topology.
    pub fn maybe_reconnect<C>(&self, group: &G, node: &T, scheme: &str) -> Result<C>
    where
        C: Send + Sync + 'static + Clone,
    {
        let err_msg = format!(
            "could not found peer address at group: {:?} node: {:?} of scheme: {:?}",
            group, node, scheme
        );
        let socket = self.get_peer_addr(group, node, scheme);
        if let Some(socket) = socket {
            let c = self.conns.maybe_reconnect(socket)?;
            return Ok(c);
        }
        Err(Error::new(ErrorKind::NotFound, err_msg))
    }

    /// Register connection provider to specific peer in group.
    /// The connector will be used to establish connection to
    /// target peer, and reconnect when calling
    /// `reconnect<C>(&self, group: G, node: T, scheme: &str)`
    pub fn register_connector_get<P, C>(
        &self,
        group: &G,
        node: &T,
        scheme: &str,
        force_new: bool,
        connector: P,
    ) -> Result<C>
    where
        C: Send + Sync + 'static + Clone,
        P: Fn(SocketAddr) -> Result<C> + Send + Sync + 'static,
    {
        let socket = self.get_peer_addr(group, &node, scheme);
        if socket.is_none() {
            return Err(Error::new(
                ErrorKind::NotFound,
                format!(
                    "could not found peer address at group: {:?} node: {:?} of scheme: {:?}",
                    group, node, scheme
                ),
            ));
        }
        let socket = socket.unwrap();
        Ok(self.conns.add_or_get_client(socket, force_new, connector)?)
    }
}
