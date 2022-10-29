#[macro_use] pub mod rpc_client;
pub mod rpc_server;

pub mod rpc {
    pub use tarpc::{self, *};
    pub use tokio_serde::{self, *};
}

pub use rpc::client::Config as TarpcConfig;