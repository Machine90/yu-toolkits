use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};

use futures::{
    future::{self, AbortHandle, Abortable},
    Future, StreamExt,
};
use tarpc::server::{BaseChannel, Channel, Incoming, Serve};
use tokio_serde::formats::Bincode;

/// Config of RPC server, all these configures see:
/// ```rust
/// ServerConfig {
///     bind_address: address, // this server binded address (ip: port)
///     max_channels_per_ip: 2 // a server can serve how much connections (channel) per address.
/// }
///
/// Client 1-1 _________________. channel1
///            (connection1)    |___________.
/// Client 1-2 _________________.           |
///            (connection1)    |           |
/// Client 1-3 _________________|           |
///            (connection1)                |
///                               channel2  |
/// Client 2-1 (connection2) -------------------> Server(address, service)
///
/// ```
#[derive(Debug)]
pub struct ServerConfig {
    pub bind_address: SocketAddr,
    pub max_frame_buffer_size: usize,
    pub max_channels_per_ip: u32,
    pub max_channels: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_address: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
            max_frame_buffer_size: usize::MAX,
            max_channels_per_ip: 1,
            max_channels: 16,
        }
    }
}

#[derive(Debug, Default)]
pub struct Server {
    config: ServerConfig,
    control: Option<Arc<AbortHandle>>,
}

impl Server {
    pub fn new(config: ServerConfig) -> Server {
        Self {
            config,
            control: None,
        }
    }

    pub fn start<S, Req, Resp>(&mut self, service: S) -> Abortable<impl Future<Output = std::io::Result<()>>>
    where
        S: Serve<Req, Resp = Resp> + Send + 'static + Clone,
        S::Fut: Send,
        Req: Send + 'static + for<'a> tarpc::serde::Deserialize<'a>,
        Resp: Send + 'static + tarpc::serde::Serialize,
    {
        self.start_with(service, move |_| {})
    }

    /// ## Example
    /// ```rust
    /// let (tx, rx) = channel();
    /// // create a client and waiting for server ready.
    /// tokio::task::spawn(async move {
    ///     match rx.await {
    ///         Ok(addr) => {
    ///             let resp = new_rpc_client!(addr, FooClient)
    ///                 .unwrap()
    ///                 .say_hello(context::current(), "admin".to_owned()).await;
    ///             println!("resp: {:?}", resp);
    ///         },
    ///         Err(_) => (),
    ///     }
    /// });
    /// // blocking the server
    /// server.start_with(Bar.serve(), move |addr| {
    ///     println!("server startup");
    ///     tx.send(addr);
    /// }).await;
    /// ```
    pub fn start_with<S, Req, Resp, H>(
        &mut self,
        service: S,
        startup: H,
    ) -> Abortable<impl Future<Output = std::io::Result<()>>>
    where
        S: Serve<Req, Resp = Resp> + Send + 'static + Clone,
        S::Fut: Send,
        Req: Send + 'static + for<'a> tarpc::serde::Deserialize<'a>,
        Resp: Send + 'static + tarpc::serde::Serialize,
        H: FnOnce(SocketAddr),
    {
        let (control, registration) = AbortHandle::new_pair();
        let ServerConfig {
            bind_address,
            max_frame_buffer_size,
            max_channels_per_ip,
            max_channels,
        } = self.config;

        let proc = async move {
            let listener = tarpc::serde_transport::tcp::listen(bind_address, Bincode::default)
                .await;

            if let Err(err) = listener {
                return Err(err);
            }
            let mut listener = listener.unwrap();
            listener
                .config_mut()
                .max_frame_length(max_frame_buffer_size);

            startup(bind_address);
            listener
                .filter_map(|incoming| future::ready(incoming.ok()))
                .map(BaseChannel::with_defaults)
                .max_channels_per_key(max_channels_per_ip, |t| {
                    t.transport().peer_addr().unwrap().ip()
                })
                .map(move |channel| channel.execute(service.to_owned()))
                .buffer_unordered(max_channels)
                .for_each(|_| async {})
                .await;
            Ok(())
        };
        let task = Abortable::new(async { proc.await }, registration);
        self.control = Some(Arc::new(control));
        task
    }

    #[inline]
    pub fn started(&self) -> bool {
        self.control.is_some()
    }

    pub fn shutdown(&mut self) {
        if let Some(proc) = self.control.take() {
            proc.abort();
        }
    }
}
