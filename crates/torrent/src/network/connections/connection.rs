
use std::{
    io::{Error, ErrorKind, Result},
    net::SocketAddr,
    sync::{Arc, atomic::{AtomicBool, Ordering}}, hint::spin_loop, fmt::Debug,
};

use vendor::prelude::lock::RwLock;

pub trait BlockPolicy {
    fn try_acquire(&self, occupied: Arc<AtomicBool>) -> bool;
}

pub struct CasTryAcquire {
    limited_tries: u32
}

impl CasTryAcquire {
    pub fn default() -> Self {
        Self { limited_tries: 100 }
    }
}

impl BlockPolicy for CasTryAcquire {
    fn try_acquire(&self, occupied: Arc<AtomicBool>) -> bool {
        let mut tried = 0;
        loop {
            let acquired = occupied
                .compare_exchange(
                    false, true, 
                    Ordering::Relaxed, 
                    Ordering::Relaxed
                ).is_ok();
            tried += 1;
            if acquired || tried >= self.limited_tries {
                return acquired;
            }
            spin_loop();
        }
    }
}

pub type Connector<C> = Arc<dyn Fn(SocketAddr) -> Result<C> + Send + Sync + 'static>;
pub type BlockPolicyImpl = Arc<dyn BlockPolicy + Send + Sync + 'static>;

/// Wrapped connection with (re)connection_provider.
/// ## Example
/// ```rust
/// let connection = Connection::new("172.0.0.1:8080".into(), move |socket_addr| {
///     let transport = block_on(shorthands::tcp_connect(socket_addr).await?);
///     Ok(FooClient::new(Config::default(), transport).spawn())
/// });
/// let client: FooClient = connection.fork();
/// ```
pub struct Connection<C: Send + 'static> {
    pub socket: SocketAddr,
    occupied: Arc<AtomicBool>,
    conn: Arc<RwLock<Option<C>>>,
    pub connection_provider: Connector<C>,
    race_policy: BlockPolicyImpl
}

impl<C: Send + 'static> Debug for Connection<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("socket", &self.socket)
            .field("occupied", &self.occupied)
            .finish()
    }
}

impl<C> Clone for Connection<C>
where
    C: Clone + Send + 'static,
{
    fn clone(&self) -> Self {
        let socket = self.socket.clone();
        let connection_provider = self
            .connection_provider
            .clone();

        let c = self.conn.read().as_ref().map(|c| c.clone());
        let conn = if c.is_none() {
            let conn = (connection_provider.as_ref())(socket);
            conn.ok()
        } else {
            Some(c.unwrap())
        };
        
        let race_policy = self.race_policy.clone();
        Self {
            socket,
            occupied: Arc::new(AtomicBool::new(false)),
            conn: Arc::new(RwLock::new(conn)),
            connection_provider,
            race_policy
        }
    }
}

impl<C: Send + 'static,> Connection<C> 
{
    pub fn with_connector<P>(addr: SocketAddr, connector: P, connect_immediately: bool) -> Self
    where
        P: Fn(SocketAddr) -> Result<C> + Send + Sync + 'static,
    {
        let connector = Arc::new(connector);
        Self::new(addr, connector, Arc::new(CasTryAcquire::default()), connect_immediately)
    }

    pub fn with_connector_ref(
        addr: SocketAddr, 
        connector: Connector<C>,
        connect_immediately: bool,
    ) -> Self
    {
        Self::new(addr, connector, Arc::new(CasTryAcquire::default()), connect_immediately)
    }

    pub fn with_connector_and_blocker<P>(
        addr: SocketAddr, 
        connector: P,
        racer: BlockPolicyImpl,
        connect_immediately: bool,
    ) -> Self 
    where
        P: Fn(SocketAddr) -> Result<C> + Send + Sync + 'static,
    {
        let connector = Arc::new(connector);
        Self::new(addr, connector, racer, connect_immediately)
    }

    pub fn with_connector_and_blocker_ref(
        addr: SocketAddr, 
        connector: Connector<C>,
        racer: BlockPolicyImpl,
        connect_immediately: bool,
    ) -> Self
    {
        Self::new(addr, connector, racer, connect_immediately)
    }

    pub fn new(
        addr: SocketAddr,
        connector: Connector<C>,
        race_policy: BlockPolicyImpl,
        connect_immediately: bool,
    ) -> Self {
        // try to connect target, not matter success or failed.
        let connection = if connect_immediately { 
            (connector.as_ref())(addr).ok()
        } else {
            None
        };
        Self {
            socket: addr,
            occupied: Arc::new(AtomicBool::new(false)),
            conn: Arc::new(RwLock::new(connection)),
            connection_provider: connector,
            race_policy
        }
    }

    /// Remember, `Connection` keep some connection doesn't means to keep alive.
    /// the connection maybe reset. <br/>
    /// Then call [reconnect](crate::network::connections::Connection)
    #[inline] pub fn maybe_alive(&self) -> bool {
        if self.occupied.load(Ordering::Relaxed) {
            return false;
        }
        self.conn.read().is_some()
    }

    #[inline]
    fn _repeat_connect(&self) -> Error {
        Error::new(
            ErrorKind::ConnectionRefused, 
            format!("socket({:?}) still in connecting, please retry later", self.socket)
        )
    }

    #[inline]
    fn _should_block(&self) -> Error {
        Error::new(
            ErrorKind::WouldBlock, 
            format!("connection({:?}) should block to acquire", self.socket)
        )
    }

    /// Connect to socket of this connection by invoking 
    /// closure `connector` that specific at constructor
    pub fn do_connect(&self) -> Result<C> {
        if self._try_acquire() {
            let c = self._do_connect();
            // not matter success or not, change state to connected.
            self._release_lock(c.is_ok());
            c
        } else {
            return Err(self._repeat_connect());
        }
    }

    #[inline]
    fn _try_acquire(&self) -> bool {
        self.race_policy.try_acquire(self.occupied.clone())
    }

    #[inline]
    fn _release_lock(&self, _success: bool) {
        self.occupied.store(false, Ordering::Relaxed);
    }

    #[inline]
    fn _do_connect(&self) -> Result<C> {
        (self.connection_provider.as_ref())(self.socket.clone())
    }
}

/// The connection is clonable, so that we can
/// fork a connection for use. Connection here 
/// is an abstract conception, for example an 
/// Arc<TcpStream> etc, just satisfy it can be 
/// cloned. 
impl<C: Clone + Send + 'static> Connection<C> {

    /// Using this approach to fork a `Client` from current connection,
    /// the connection to the client maybe has not been established.
    /// ### Exception
    /// * **WouldBlock**: should blocking to acquire client, maybe 
    /// in connection now, please retry later.
    /// * **Other IO Error**: other error that generated from `Connector` 
    /// when `do_connect`, for example `ConnectionRefuse`.
    #[inline]
    pub fn maybe_fork(&self) -> Result<C> {
        // use atomic bool rather than RwLock of thread is to
        // prevent the problem of write_lock occupied in some 
        // thread-pool such like tokio, it maybe get in deadlock when 
        // stale a locked worker
        if !self._try_acquire() {
            return Err(self._should_block());
        }
        let mut wlock = self.conn.write();
        if let Some(c) = wlock.as_ref() {
            // this will take in place without any blocking
            let c = c.clone();
            drop(wlock);
            self._release_lock(false);
            Ok(c)
        } else {
            // caution, if "connection_provider" blocked in thread pool such like 
            // tokio's Runtime, the "write lock" of "self.conn" may be step in
            // deadlock, so protect this cornercase by detect if current connection is 
            // in connecting
            let new_conn = self._do_connect();
            if let Err(e) = new_conn {
                drop(wlock);
                self._release_lock(false);
                return Err(e);
            }
            *wlock = Some(new_conn.unwrap());
            let c = wlock.as_ref().unwrap().clone();
            drop(wlock);
            self._release_lock(true);
            Ok(c)
        }
    }

    /// Maybe reconnect and try get a Client from this 
    /// connection after reconnect success, maybe another 
    /// thread (or coroutines) is trigger to reconnect 
    /// concurrency, so this maybe reconnect failed, and get 
    /// and error rather than a Client.
    #[inline]
    pub fn maybe_reconnect(&self) -> Result<C> {
        self._maybe_reconnect()?;
        self.maybe_fork()
    }

    fn _maybe_reconnect(&self) -> Result<()> {
        if !self._try_acquire() {
            return Err(self._repeat_connect());
        }
        let try_update = self.conn.try_write();
        if try_update.is_none() {
            self._release_lock(false);
            return Err(Error::new(
                ErrorKind::WouldBlock, 
                "could not reconnecting in concurrency"
            ));
        }
        let mut wlock = try_update.unwrap();
        let c = self._do_connect();
        if let Err(e) = c {
            drop(wlock);
            self._release_lock(false);
            return Err(e);
        }
        *wlock = c.ok();
        drop(wlock);
        self._release_lock(true);
        Ok(())
    }
}

#[cfg(feature = "tokio1-rt")] use crate::tokio1;
#[cfg(feature = "tokio1-rt")]
impl<C: Clone + Send + 'static> Connection<C> {
    
    /// try reconnect(if current connection is not in re-connecting), and 
    /// fork a connection in specific timeout.
    #[inline]
    pub async fn reconnect_and_get(&self, timeout_millis: u64) -> Result<C> {
        let connected = self._maybe_reconnect();
        if connected.is_ok() {
            self.maybe_fork()
        } else {
            self.fork_timeout(timeout_millis).await
        }
    }

    /// Try to ensure to fork a connection in timeout. It's difference from 
    /// `maybe_fork`, which only try acquire a client in place but not to 
    /// retry in timeout when this connection failed, some times this connection 
    /// is still in connecting or remote server is down for a while
    pub async fn fork_timeout(&self, timeout_millis: u64) -> Result<C> {
        let mut client = self.maybe_fork();
        if client.is_ok() { return client; }

        let dur = std::time::Duration::from_millis(timeout_millis / 10);
        let mut tried = 0;
        loop {
            if client.is_ok() || tried > 10 {
                break;
            }
            // yied to another thread, not blocking.
            tokio1::time::sleep(dur).await;
            client = self.maybe_fork();
            tried += 1;
        }
        client
    }
}