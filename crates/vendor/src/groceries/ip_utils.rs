use std::{
    io::{Error, ErrorKind},
    net::{AddrParseError, SocketAddr, ToSocketAddrs},
};

/// trying to parse String ip to SocketAddr from local DNS (hosts file)
pub fn local_dns_lookup(host: &str) -> std::io::Result<SocketAddr> {
    let try_parse_first: Result<SocketAddr, AddrParseError> = host.parse::<SocketAddr>();
    if let Ok(try_parse_first) = try_parse_first {
        return Ok(try_parse_first);
    }
    let mut lookups = host.to_socket_addrs()?;
    while let Some(socket) = lookups.next() {
        return Ok(socket);
    }
    Err(Error::new(
        ErrorKind::AddrNotAvailable,
        format!("could not found host: {:?} in local hosts", host),
    ))
}
