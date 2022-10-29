use std::{
    io::{Error, Result},
    net::SocketAddr,
    time::Duration,
};

use tarpc::{
    client::{NewClient, RequestDispatch},
    serde::{Deserialize, Serialize},
    serde_transport::{
        tcp::{self},
        Transport,
    },
    ClientMessage, Response,
};
use torrent::tokio1 as tokio;
use tokio::{
    net::TcpStream,
    time::{error::Elapsed, sleep, timeout, Instant},
};
use tokio_serde::formats::Bincode;

pub type TcpBincode<Req, Res> = Transport<
    TcpStream,
    Response<Res>,
    ClientMessage<Req>,
    Bincode<Response<Res>, ClientMessage<Req>>,
>;

pub type TcpBinClient<Interface, Req, Res> =
    NewClient<Interface, RequestDispatch<Req, Res, TcpBincode<Req, Res>>>;

pub struct ClientConfig {
    /// This client connect to which remote address.
    pub target_address: SocketAddr,
    /// When create a RPC client, this client
    /// will try to [connect](crate::network::client::rpc_client::tcp_connect)
    /// `target_address` in a given timeout.
    pub handshake_timeout: Duration,
    /// The number of requests that can be in flight at once.
    /// `max_in_flight_requests` controls the size of the map used by the client
    /// for storing pending requests.
    pub max_in_flight_requests: usize,
    /// The number of requests that can be buffered client-side before being sent.
    /// `pending_requests_buffer` controls the size of the channel clients use
    /// to communicate with the request dispatch task.
    pub pending_request_buffer: usize,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            target_address: "127.0.0.1:0".parse().unwrap(),
            handshake_timeout: std::time::Duration::from_millis(100),
            max_in_flight_requests: 1_000,
            pending_request_buffer: 100,
        }
    }
}

impl Into<crate::tcp::TarpcConfig> for ClientConfig {
    fn into(self) -> crate::tcp::TarpcConfig {
        let mut conf = crate::tcp::TarpcConfig::default();
        conf.max_in_flight_requests = self.max_in_flight_requests;
        conf.pending_request_buffer = self.pending_request_buffer;
        conf
    }
}

/// Create a TCP bincode transport RPC Client via tarpc.
/// ### Params
/// * addr: SocketAddr
/// * T: tarpc client
/// ### Example
/// ```rust
/// #[tarpc::service]
/// pub trait Foo {
///     async fn say_hello(name: String) -> String;
/// }
///
/// let client = new_client!("127.0.0.1:50010".parse().unwrap(), FooClient);
/// ```
#[macro_export]
macro_rules! new_client {
    ($addr:expr, $T:ty) => {{
        let fut = async move {
            let socket_addr: std::net::SocketAddr = $addr;
            let transport = $crate::tcp::rpc_client::tcp_connect(
                socket_addr,
                std::time::Duration::from_millis(100),
            )
            .await;
            if let Err(error) = transport {
                return Err(error);
            }
            Ok(<$T>::new($crate::tcp::TarpcConfig::default(), transport.unwrap()).spawn())
        };
        $crate::runtime::blocking(fut)
    }};
}

/// Construct a RPC client with configuration. All connection's configure fields
/// can be specified in arg `config`.
/// ## Example
/// ```rust
/// let client = new_client_config!(ClientConfig {
///     target_address: "127.0.0.1:8080".parse().unwarp(),
///     handshake_timeout: Duration::from_millis(10),
///     ..Default::default()
/// }, FooClient)
/// let greeting = client.say_hello("Alice".to_owned()).await;
/// ```
#[macro_export]
macro_rules! new_client_config {
    ($config:expr, $T:ty) => {{
        let fut = async move {
            let cfg: $crate::tcp::rpc_client::ClientConfig = $config;
            let socket_addr = cfg.target_address;
            let timeout = cfg.handshake_timeout;
            let transport = $crate::tcp::rpc_client::tcp_connect(socket_addr, timeout).await;
            if let Err(error) = transport {
                return Err(error);
            }

            let client_cfg: $crate::tcp::TarpcConfig = cfg.into();
            Ok(<$T>::new(client_cfg, transport.unwrap()).spawn())
        };
        $crate::runtime::blocking(fut)
    }};
}

/// Create a TCP bincode transport RPC Client with connection timeout via tarpc.
/// ### Params
/// * addr: SocketAddr
/// * T: tarpc client
/// * timeout: Duration::from_millis(timeout);
/// ## Example
/// ```rust
/// let client =
///     new_client_timeout!("127.0.0.1:8080".parse().into(), FooClient, std::time::Duration::from_millis(10));
///
/// let client =
///     new_client_timeout!(local_dns_lookup("localhost:8080").unwrap(), FooClient, std::time::Duration::from_millis(10));
/// ```
#[macro_export]
macro_rules! new_client_timeout {
    ($addr:expr, $T:ty, $timeout:expr) => {{
        let fut = async move {
            let socket_addr: std::net::SocketAddr = $addr;
            let transport = $crate::tcp::rpc_client::tcp_connect(socket_addr, $timeout).await;
            if let Err(error) = transport {
                return Err(error);
            }
            Ok(<$T>::new($crate::tcp::TarpcConfig::default(), transport.unwrap()).spawn())
        };
        $crate::runtime::blocking(fut)
    }};
}

/// Make a tcp connection to target address in timeout as transporter, then we can
/// use this transport to build a tarpc client.
pub async fn tcp_connect<Req, Res>(
    addr: SocketAddr,
    timeout: Duration,
) -> Result<TcpBincode<Req, Res>>
where
    Res: for<'de> Deserialize<'de>,
    Req: Serialize,
{
    tcp_connect_retry(addr, timeout, retry::non_retry).await
}

pub async fn tcp_connect_retry<Req, Res, R>(
    addr: SocketAddr,
    connect_timeout: Duration,
    retry: R,
) -> Result<TcpBincode<Req, Res>>
where
    Res: for<'de> Deserialize<'de>,
    Req: Serialize,
    R: Fn(usize, &std::io::Error) -> Option<Duration>,
{
    let timer = Instant::now();
    let mut c: std::result::Result<Result<TcpBincode<Req, Res>>, Elapsed> =
        timeout(connect_timeout, tcp::connect(addr, Bincode::default)).await;
    let mut tries = 0;
    let result: Result<TcpBincode<Req, Res>>;
    loop {
        if let Err(_elapsed) = &c {
            let error = Error::new(
                std::io::ErrorKind::TimedOut,
                format!("connection timeout for {:?}ms", timer.elapsed().as_millis()),
            );
            tries += 1;
            if let Some(dur) = retry(tries, &error) {
                sleep(dur).await;
                c = timeout(connect_timeout, tcp::connect(addr, Bincode::default)).await;
                continue;
            } else {
                result = Err(error);
                break;
            }
        }
        let conn = c.unwrap();
        if let Err(error) = conn {
            tries += 1;
            if let Some(dur) = retry(tries, &error) {
                sleep(dur).await;
                c = timeout(connect_timeout, tcp::connect(addr, Bincode::default)).await;
                continue;
            } else {
                result = Err(error);
            }
        } else {
            result = Ok(conn.unwrap());
        }
        break;
    }
    result
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tarpc::client::Config;
    use tokio::runtime::Runtime;

    use super::{tcp_connect_retry, *};

    #[tarpc::service]
    trait Foo {
        async fn hello() -> String;
    }

    #[test]
    fn test_conn() {
        Runtime::new().unwrap().block_on(async move {
            let t = tcp_connect_retry(
                "127.0.0.1:8080".parse().unwrap(),
                Duration::from_millis(10),
                retry::exponential,
            )
            .await;

            if let Err(e) = t {
                println!("err: {:?}", e);
                return;
            }
            FooClient::new(Config::default(), t.unwrap());
        });
    }
}

pub mod retry {
    use std::time::Duration;

    pub fn exponential(current: usize, _e: &std::io::Error) -> Option<Duration> {
        if current >= 3 {
            return None;
        }
        let times = (current * current) * 1000;
        Some(Duration::from_millis(times as u64))
    }

    #[inline]
    pub fn non_retry(_: usize, _: &std::io::Error) -> Option<Duration> {
        None
    }
}
