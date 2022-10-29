use std::{
    io::{Error, ErrorKind},
    net::{SocketAddr},
    ops::Range,
    time::Duration,
};

use vendor::prelude::local_dns_lookup;

#[derive(Debug, Clone)]
pub struct RecvConfig {
    pub bind_host: String,
    pub port_range: Range<u16>,
    pub waiting_timeout_ms: usize,
    pub read_timeout_ms: usize,
}

impl Default for RecvConfig {
    fn default() -> Self {
        Self {
            bind_host: "127.0.0.1".to_owned(),
            port_range: 20020..22000,  // todo give a make sense range.
            waiting_timeout_ms: 60000, // 1min
            read_timeout_ms: 300000,   // 5min
        }
    }
}

impl RecvConfig {
    pub fn waiting_timeout(&self) -> Option<Duration> {
        if self.waiting_timeout_ms == 0 {
            None
        } else {
            let timeout = std::cmp::max(self.waiting_timeout_ms, 100);
            let timeout = std::cmp::min(timeout, u64::MAX as usize);
            Some(Duration::from_millis(timeout as u64))
        }
    }

    pub fn read_timeout(&self) -> Option<Duration> {
        if self.read_timeout_ms == 0 {
            None
        } else {
            let timeout = std::cmp::max(self.read_timeout_ms, 100);
            let timeout = std::cmp::min(timeout, u64::MAX as usize);
            Some(Duration::from_millis(timeout as u64))
        }
    }

    #[inline]
    pub fn bind_address(&self, with_port: u16) -> String {
        format!("{}:{}", self.bind_host, with_port)
    }

    /// Try listening on socket address (constructed from host 
    /// and port in given range), return listener immediately if 
    /// success, or keep try with next port.
    pub fn try_establish<F, L>(&self, mut listen: F) -> std::io::Result<L>
    where
        F: FnMut(SocketAddr, u16) -> std::io::Result<L>,
    {
        let host = self.bind_host.as_str();
        for port in self.port_range.clone() {
            let address = format!("{host}:{port}");
            let socket = local_dns_lookup(address.as_str());
            if let Ok(socket) = socket {
                if let Ok(listener) = listen(socket, port) {
                    return Ok(listener);
                }
            }
        }
        Err(Error::new(
            ErrorKind::AddrInUse,
            format!("not valid port to bind in range: {:?}", self.port_range),
        ))
    }
}
