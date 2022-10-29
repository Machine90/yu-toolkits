use super::connection::Connection;
use std::{
    fmt::Debug,
    io::{Error, ErrorKind, Result},
    net::SocketAddr,
};
use vendor::prelude::WildMap;

/// Connection pool for [Connection](self::Connection)
pub struct Connections {
    inner: WildMap<SocketAddr>,
}

impl Debug for Connections {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let conns: Vec<_> = self.inner.iter().map(|conn| *conn.key()).collect();
        f.debug_struct("Connections")
            .field("pooled", &conns)
            .finish()
    }
}

impl Connections {
    pub fn new() -> Connections {
        Self {
            inner: WildMap::new(),
        }
    }

    pub fn maybe_get_client<C: Send + Sync + 'static + Clone>(
        &self,
        socket_addr: SocketAddr,
    ) -> Option<C> {
        let connection = self
            .inner
            .get::<Connection<C>>(socket_addr)
            .map(|c| c.clone());
        if let Some(conn) = connection {
            conn.maybe_fork().ok()
        } else {
            None
        }
    }

    /// Only create and push connection of client to pool without
    /// connecting to target.
    pub fn push_client<P, C>(&self, socket_addr: SocketAddr, force_new: bool, connector: P)
    where
        C: Send + Sync + 'static + Clone,
        P: Fn(SocketAddr) -> Result<C> + Send + Sync + 'static,
    {
        // fast determine if exists and not to overwrite.
        if !force_new && self.contains(socket_addr) {
            return;
        }
        let connection = Connection::with_connector(
            socket_addr, 
            connector, 
            false
        );
        self.inner.insert(socket_addr, connection, force_new);
    }

    /// Add a client connector of given socket to connection pool,
    /// then try create a client from connector and return.
    /// ### Params
    /// * socket_addr: target address of the client (connection)
    /// * force_new: if true, the connection with connector will overwrite the
    /// existed connection of socket.
    /// * connector: used to (re)connect to target and build client.
    pub fn add_or_get_client<P, C>(
        &self,
        socket_addr: SocketAddr,
        force_new: bool,
        connector: P,
    ) -> Result<C>
    where
        C: Send + Sync + 'static + Clone,
        P: Fn(SocketAddr) -> Result<C> + Send + Sync + 'static,
    {
        if !force_new && self.contains(socket_addr) {
            let client = self.maybe_get_client(socket_addr);
            return if client.is_none() {
                Err(Error::new(
                    ErrorKind::NotFound,
                    format!(
                        "not exist client of socket: {:?} when add_client in force_new",
                        socket_addr
                    ),
                ))
            } else {
                Ok(client.unwrap())
            };
        }
        let connection = Connection::with_connector(
            socket_addr, 
            connector, 
            true
        );
        self.inner
            .insert(socket_addr, connection, force_new)
            .maybe_fork()
    }

    pub fn add_connection<C>(&self, connection: Connection<C>)
    where
        C: Send + Sync + 'static,
    {
        let socket_addr = connection.socket;
        self.inner.insert(socket_addr, connection, false);
    }

    #[inline]
    pub fn contains(&self, socket_addr: SocketAddr) -> bool {
        self.inner.contains_key(socket_addr)
    }

    #[inline]
    pub fn remove_client(&self, socket_addr: SocketAddr) {
        self.inner.remove(&socket_addr);
    }

    pub fn maybe_reconnect<C: Send + Sync + 'static + Clone>(
        &self,
        socket_addr: SocketAddr,
    ) -> Result<C> {
        let connection = self.inner.get::<Connection<C>>(socket_addr).clone();
        if let Some(connection) = connection {
            connection.maybe_reconnect()
        } else {
            Err(Error::new(
                ErrorKind::NotFound,
                format!("connection of socket({:?}) not found ", socket_addr),
            ))
        }
    }

    #[inline]
    pub fn connection_num(&self) -> usize {
        self.inner.len()
    }

    #[inline]
    fn _connection_not_found(&self, socket_addr: SocketAddr) -> Error {
        Error::new(
            ErrorKind::NotFound,
            format!("connection of socket({:?}) not found ", socket_addr),
        )
    }
}

#[cfg(feature = "tokio1-rt")]
impl Connections {
    pub async fn reconnect_and_get<C: Send + Sync + 'static + Clone>(
        &self,
        socket_addr: SocketAddr,
        timeout_millis: u64,
    ) -> Result<C> {
        let connection = self.inner.get::<Connection<C>>(socket_addr).clone();
        if let Some(connection) = connection {
            connection.reconnect_and_get(timeout_millis).await
        } else {
            Err(self._connection_not_found(socket_addr))
        }
    }

    /// Try to ensure to get a client in timeout from connection pool. It's difference from
    /// `maybe_get_client`, which only call once to get a client in place but not to
    /// retry in timeout when connecting failed, some times this `Connection`
    /// is still in connecting or remote server is down for a while, then `maybe_get_client`
    /// possibly get an error.
    pub async fn get_client_timeout<C: Send + Sync + 'static + Clone>(
        &self,
        socket_addr: SocketAddr,
        timeout_millis: u64,
    ) -> Result<C> {
        let connection = self
            .inner
            .get::<Connection<C>>(socket_addr)
            .map(|c| c.clone());
        if let Some(conn) = connection {
            conn.fork_timeout(timeout_millis).await
        } else {
            Err(self._connection_not_found(socket_addr))
        }
    }
}
