use std::{
    fmt::Debug,
    hash::Hash,
    io::{Error, ErrorKind, Result},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

use tokio::time::timeout;

use crate::runtime;

use super::{connections::connection::Connection, Network};

impl<G, T> Network<G, T>
where
    G: Debug + Clone + Eq + Hash + Send + 'static,
    T: Default + Debug + Clone + Eq + Hash + Send + Sync + 'static,
{
    pub async fn register_connector_all_async<P, C>(
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
            tasks.push((
                socket_addr,
                runtime::spawn_blocking(move || {
                    Connection::with_connector_ref(socket_addr, connector, connect_immediately)
                }),
            ));
        }

        for (socket, task) in tasks {
            let protect = timeout(Duration::from_secs(10), task);
            let in_timeout = protect.await;
            if let Err(_e) = in_timeout {
                self._release_connecting(socket);
                continue;
            }
            if let Ok(connection) = in_timeout.unwrap() {
                let _ = self.conns.add_connection(connection);
                self._release_connecting(socket);
            }
        }
    }

    /// Get the client of a node directly.
    pub async fn reconnect_and_get_node<C>(
        &self,
        node: &T,
        scheme: &str,
        timeout_millis: u64,
    ) -> Result<C>
    where
        C: Send + Sync + 'static + Clone,
    {
        let socket = self
            .get_node_addr(node, scheme)
            .ok_or(Error::new(
                ErrorKind::NotFound, 
                format!("could not found node-{:?} address of scheme: {:?}", node, scheme)
            ))?;
        self.conns.reconnect_and_get(socket, timeout_millis).await
    }

    pub async fn reconnect_and_get<C>(
        &self,
        group: &G,
        node: &T,
        scheme: &str,
        timeout_millis: u64,
    ) -> Result<C>
    where
        C: Send + Sync + 'static + Clone,
    {
        let socket = self.get_peer_addr(group, node, scheme);
        if let Some(socket) = socket {
            let c = self.conns.reconnect_and_get(socket, timeout_millis).await?;
            return Ok(c);
        }
        let err_msg = format!(
            "could not found peer address at group: {:?} node: {:?} of scheme: {:?}",
            group, node, scheme
        );
        Err(Error::new(ErrorKind::NotFound, err_msg))
    }

    /// Try to get a client in timeout from cluster. It's difference from
    /// `maybe_get_client`, which only call once to get a client in place but not to
    /// retry in given timeout when connect failed, some times this connection
    /// is still in connecting or remote server is down for a while, then `maybe_get_client`
    /// possibly get an error.
    pub async fn get_client_timeout<C: Send + Sync + 'static + Clone>(
        &self,
        group: &G,
        node: &T,
        scheme: &str,
        timeout_millis: u64,
    ) -> Result<C> {
        let socket = self.get_peer_addr(group, node, scheme);
        if socket.is_none() {
            return Err(Error::new(ErrorKind::NotFound, format!(
                "could not found peer address at group: {:?} node: {:?} of scheme: {scheme}",
                group, node
            )));
        }
        self.conns
            .get_client_timeout(socket.unwrap(), timeout_millis)
            .await
    }

    pub async fn get_node_client_timeout<C: Send + Sync + 'static + Clone>(
        &self,
        node: &T,
        scheme: &str,
        timeout_millis: u64,
    ) -> Result<C> {
        let socket = self
            .get_node_addr(node, scheme)
            .ok_or(Error::new(
                ErrorKind::NotFound, 
                format!("could not found node-{:?} address of scheme: {:?}", node, scheme)
            ))?;
        self.conns
            .get_client_timeout(socket, timeout_millis)
            .await
    }
}
