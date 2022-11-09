pub mod sync;
#[cfg(all(feature = "tokio1"))]
pub mod tokio;

pub mod config;
pub mod progress;
pub mod utils;

use std::{
    collections::HashSet,
    io::{Error, ErrorKind},
    net::SocketAddr,
    ops::{Deref, DerefMut},
};

use serde::{Deserialize, Serialize};
use vendor::prelude::local_dns_lookup;

use self::utils::{write_u32, write_u64};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Flag {
    Handshake,
    Transfer,
}

impl Flag {
    pub fn to_u8(&self) -> u8 {
        match self {
            Flag::Handshake => 1,
            Flag::Transfer => 2,
        }
    }

    pub fn from_u8(flag: u8) -> Option<Self> {
        match flag {
            1 => Some(Flag::Handshake),
            2 => Some(Flag::Transfer),
            _ => None,
        }
    }
}

#[derive(Default, Debug, Clone, Deserialize, Serialize)]
pub struct MetaInfo {
    pub identify: String,
    pub bytes: usize,
    pub checksum: u32,
}

#[derive(Debug, Clone)]
pub struct Item<T> {
    meta: MetaInfo,
    payload: T,
}

impl<T> Deref for Item<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.payload
    }
}

impl<T> DerefMut for Item<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.payload
    }
}

/// Session is send between `FastCp` sender and receiver.
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    pub id: u64,
    pub items: Vec<MetaInfo>,
    pub listen_at: String,
}

impl Session {
    pub fn check_access(&self, _socket: SocketAddr) -> bool {
        // TODO
        true
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn filter_items<T>(&self, items: &mut Vec<Item<T>>) -> Vec<Item<T>> {
        let mut i = 0;
        let ids: HashSet<_> = self
            .items
            .iter()
            .map(|item| item.identify.as_str())
            .collect();
        let mut removes = vec![];
        while i < items.len() {
            if !ids.contains(items[i].meta.identify.as_str()) {
                let removed = items.remove(i);
                removes.push(removed);
            } else {
                i += 1;
            }
        }
        removes
    }
}

#[derive(Debug)]
pub struct Packet {
    content: Vec<u8>,
    checksum: u32,
}

impl Into<Vec<u8>> for Packet {
    fn into(self) -> Vec<u8> {
        let Packet { content, checksum } = self;
        let len = content.len() as u64;
        let len_buf = write_u64(len);
        let crc_buf = write_u32(checksum);
        // [8 .... , 4, ...., len ....   ]
        // [len_buf, crc_buf, content_buf]
        let mut buf = len_buf.to_vec();
        buf.extend(crc_buf);
        buf.extend(content);
        buf
    }
}

impl TryFrom<Session> for Packet {
    type Error = std::io::Error;

    fn try_from(session: Session) -> Result<Self, Self::Error> {
        let bin_content = bincode::serialize(&session);
        if let Err(e) = bin_content {
            return Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                format!("[Codec] encode error: {}", e),
            ));
        }
        let bin_content = bin_content.unwrap();
        let crc32 = crc32fast::hash(&bin_content[..]);
        Ok(Self {
            content: bin_content,
            checksum: crc32,
        })
    }
}

impl TryInto<Session> for Packet {
    type Error = std::io::Error;

    fn try_into(self) -> Result<Session, Self::Error> {
        let Packet { content, .. } = self;
        let session = bincode::deserialize::<Session>(&content);
        if let Err(e) = session {
            return Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                format!("[Codec] decode error: {}", e),
            ));
        }
        Ok(session.unwrap())
    }
}

/// An easy used fast copy tool, used to transfer `Item`s
/// between [FastCp<T>](crate::fast_cp::FastCp) and
/// [Receiver](crate::fast_cp::sync::cp_receiver::Receiver).
/// [Item](crate::fast_cp::Item) could be any thing.
///
/// ## Example
/// ```
/// #[test] fn test() {
///     let mut recv = Receiver::new(VecHandler);
///     recv.set_config(RecvConfig {
///         bind_host: "localhost".to_owned(),
///         port_range: 8080..8081,
///         waiting_timeout_ms: 1000,
///         ..Default::default()
///     });
///     let daemon = recv.daemon();  
///
///     let mut client = CpFile::new();
///     let _ = client.src_dir("../../assets/backup", true);
///     let _ = client
///         .try_handshake("localhost:8080", Duration::from_millis(100))
///         .and_then(|transfer| transfer.flush());
///     let _ = daemon.join();
/// }
/// ```
#[derive(Debug)]
pub struct FastCp<T> {
    dest_address: Option<SocketAddr>,
    pub src_items: Vec<Item<T>>,
}

impl<T> FastCp<T> {
    pub fn new() -> Self {
        Self {
            dest_address: None,
            src_items: Vec::new(),
        }
    }

    #[inline]
    fn _socket_address(&self) -> std::io::Result<SocketAddr> {
        self.dest_address.ok_or(Error::new(
            ErrorKind::NotFound,
            "dest peer's socket address must be assigned",
        ))
    }

    /// Apply with reply session and
    /// return removed items number after
    /// compare with current session.
    #[inline]
    pub fn apply(&mut self, reply: Session) -> std::io::Result<&mut Self> {
        self.dest_address = Some(local_dns_lookup(reply.listen_at.as_str())?);

        if !self.src_items.is_empty() {
            reply.filter_items(&mut self.src_items);
        }
        Ok(self)
    }

    pub fn get_session(&self) -> Session {
        let items: Vec<_> = self
            .src_items
            .iter()
            .map(|item| item.meta.clone())
            .collect();
        Session {
            items,
            ..Default::default()
        }
    }
}

impl<T: AsRef<[u8]>> FastCp<T> {
    /// Write binary content to buffer, after that,
    /// call `send` to send these content to dest peer.
    /// If `data` is too large to send, then suggest to
    /// write it to a `File` first, then copy it to dest
    /// by using `src_dir` and `flush` rather than this.
    /// Nothing: `compute_checksum` will scan the binary
    /// of item, if data is too large, it'll cost lots of
    /// time to compute.
    pub fn write_buffer(&mut self, id: &str, data: T, compute_checksum: bool) {
        let buf = data.as_ref();
        let meta = MetaInfo {
            identify: id.to_owned(),
            bytes: buf.len(),
            checksum: if compute_checksum {
                crc32fast::hash(buf)
            } else {
                0
            },
        };
        self.src_items.push(Item {
            meta,
            payload: data,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use crate::fast_cp::sync::cp_file::CpFile;
    use crate::fast_cp::{sync::cp_receiver::Receiver, FastCp};
    use crate::fast_cp::{sync::handler::ReceiveHandler, MetaInfo};

    use super::config::RecvConfig;

    pub struct VecHandler;
    impl ReceiveHandler<Vec<u8>> for VecHandler {
        fn exists(&self, _item: &MetaInfo) -> bool {
            false
        }

        fn open_writer(&self, _item: &MetaInfo) -> std::io::Result<Vec<u8>> {
            Ok(vec![])
        }

        fn complete(&self, writer: &mut Vec<u8>) {
            let data = String::from_utf8(writer.clone());
            assert!(data.is_ok());
        }
    }

    #[test]
    fn test_vec() {
        // step1: client side collect items prepare to send.
        let mut fast_cp = FastCp::<&[u8]>::new();
        for i in 0..1000 {
            fast_cp.write_buffer(format!("key-{:?}", i).as_str(), b"value", true);
        }
        
        let session = fast_cp.get_session();

        // step2: listening on server side and try to accept handshake from step1.
        let mut recv = Receiver::new(VecHandler);
        // then generate reply
        let reply = recv.accept(session).unwrap();
        let daemon = recv.daemon();

        let inst = Instant::now();
        // step3: client decide to send what server need
        let _r = fast_cp.apply(reply);
        let written = fast_cp.send();
        println!("{:?} ms, total written: {:?}bytes", inst.elapsed().as_millis(), written);

        let _ = daemon.join();
    }

    #[test]
    fn test_file() {
        // step1: client side collect items prepare to send.
        let mut fast_cp = CpFile::new();
        let r = fast_cp.src_dir("../../assets/backup", true);

        if r.is_err() {
            return;
        }
        // fast_cp.write_buffer("key", b"value", true);
        let session = fast_cp.get_session();

        // step2: listening on server side and try to accept handshake from step1.
        let mut recv = Receiver::new(VecHandler);
        // then generate reply
        let reply = recv.accept(session).unwrap();
        let daemon = recv.daemon();

        let inst = Instant::now();
        // step3: client decide to send what server need
        let _r = fast_cp.apply(reply);
        let _w = fast_cp.flush();
        println!("{:?} ms", inst.elapsed().as_millis());

        let _ = daemon.join();
    }

    #[test]
    fn test_handshake() {
        let mut recv = Receiver::new(VecHandler);
        recv.set_config(RecvConfig {
            bind_host: "localhost".to_owned(),
            port_range: 8080..8081,
            waiting_timeout_ms: 1000,
            ..Default::default()
        });
        let daemon = recv.daemon();

        let mut client = CpFile::new();
        let _ = client.src_dir("../../assets/backup", true);
        let _ = client
            .try_handshake("localhost:8080", Duration::from_millis(100))
            .and_then(|transfer| transfer.flush());
        let _ = daemon.join();
    }
}

#[cfg(all(feature = "tokio1", test))]
mod tests_async {
    use std::time::Duration;

    use tokio::{runtime::Runtime, time::Instant};

    use crate::{
        fast_cp::{sync::cp_receiver::Receiver, tests::VecHandler, tokio::cp_file::CpFile, FastCp},
        runtime,
    };

    use super::config::RecvConfig;

    #[test]
    fn test_vec_async() {
        // step1: client side collect items prepare to send.
        let mut fast_cp = FastCp::<&[u8]>::new();
        for i in 0..1000 {
            fast_cp.write_buffer(format!("key-{:?}", i).as_str(), b"value", true);
        }
        // fast_cp.write_buffer("key", b"value", true);
        let session = fast_cp.get_session();

        // step2: listening on server side and try to accept handshake from step1.
        let mut recv = Receiver::new(VecHandler);
        // then generate reply
        let reply = recv.accept(session).unwrap();
        let daemon = recv.daemon();

        Runtime::new().unwrap().block_on(async move {
            let inst = Instant::now();
            // step3: client decide to send what server need
            let _r = fast_cp.apply(reply);
            let written = fast_cp.send_async().await;
            println!("{:?} ms, total written: {:?}bytes", inst.elapsed().as_millis(), written);
        });

        let _ = daemon.join();
    }

    #[test] 
    fn test_file_cp_async() {
        Runtime::new().unwrap().block_on(async move {
            test_file_async().await;
        });
    }

    async fn test_file_async() {
        // step1: client side collect items prepare to send.
        let mut fast_cp = CpFile::new();
        let r = fast_cp
            .src_dir_async("../../assets/backup", true)
            .await;

        if r.is_err() {
            return;
        }
        // fast_cp.write_buffer("key", b"value", true);
        let session = fast_cp.get_session();

        // step2: listening on server side and try to accept handshake from step1.
        let mut recv = Receiver::new(VecHandler);
        // then generate reply
        let reply = recv.accept(session).unwrap();
        let daemon = recv.daemon();

        let inst = Instant::now();
        // step3: client decide to send what server need
        let _r = fast_cp.apply(reply);
        let _w = fast_cp.flush_async().await;
        println!("{:?} ms", inst.elapsed().as_millis());

        let _ = daemon.join();
    }

    #[test] 
    fn test_hs_async() {
        Runtime::new().unwrap().block_on(async move {
            test_handshake_async().await;
        });
    }

    async fn test_handshake_async() {
        let mut recv = Receiver::new(VecHandler);
        recv.set_config(RecvConfig {
            bind_host: "localhost".to_owned(),
            port_range: 8080..8081,
            waiting_timeout_ms: 1000,
            ..Default::default()
        });
        let daemon = runtime::spawn_blocking(move || {
            let _ = recv.block();
        });

        let mut client = CpFile::new();
        let _ = client
            .src_dir_async("../../assets/backup", true)
            .await;
        let transfer = client.try_handshake_async(
            "localhost:8080", 
            Duration::from_millis(100)
        ).await;

        let _ = transfer.unwrap().flush_async().await;

        let _ = daemon.await;
    }
}
