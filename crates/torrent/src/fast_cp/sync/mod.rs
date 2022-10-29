pub mod cp_file;
pub mod cp_receiver;
pub mod handler;

use vendor::prelude::local_dns_lookup;
use std::{
    io::{ErrorKind, Read, Write},
    net::TcpStream,
    time::Duration,
};

use super::{
    utils::{read_u32, read_u64},
    FastCp, Flag, Item, MetaInfo, Packet, Session,
};

impl Packet {
    pub fn read_from<R: Read + ?Sized>(incoming: &mut R) -> std::io::Result<Self> {
        let mut len_buf: [u8; 8] = [0; 8];
        let mut _readed = incoming.read(&mut len_buf)?;
        let len = read_u64(&len_buf)?;
        let mut crc_buf = [0; 4];
        _readed += incoming.read(&mut crc_buf)?;
        let checksum = read_u32(&crc_buf)?;
        let mut content_buf = vec![0u8; len as usize];
        _readed += incoming.read(content_buf.as_mut_slice())?;
        let compute_crc = crc32fast::hash(&content_buf[..]);
        if compute_crc != checksum {
            return Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                "unexpected checksum of readed content",
            ));
        }
        Ok(Self {
            content: content_buf,
            checksum,
        })
    }
}

impl<T> FastCp<T> {
    /// Try do handshake to `dest` and waiting for reply in sync mode.
    /// then return `Transfer` to do really items transfering.
    /// ### Params
    /// * with: target peer address, could be convert to socket address.
    /// * timeout: max limited wait timeout for handshake.
    pub fn try_handshake(self, with: &str, timeout: Duration) -> std::io::Result<Transfer<T>> {
        let socket_addr = local_dns_lookup(with)?;
        let session = self.get_session();
        if session.is_empty() {
            return Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                "please give a session before handshake",
            ));
        }

        let mut items = self.src_items;

        let packet: Packet = session.try_into()?;
        let mut stream = TcpStream::connect_timeout(&socket_addr, timeout)?;

        let buf: Vec<u8> = packet.into();
        // step 1: send session to target.
        stream.write(&[Flag::Handshake.to_u8()])?;
        let _s = stream.write(buf.as_slice())?;
        stream.flush()?;
        // step 2: waiting for reply
        let reply: Packet = Packet::read_from(&mut stream)?;
        let reply: Session = reply.try_into()?;
        reply.filter_items(&mut items);

        // step 3: begin to transfer require items to target
        Ok(Transfer {
            outgoing: stream,
            items,
        })
    }
}

impl<T: AsRef<[u8]>> FastCp<T> {
    pub fn send(self) -> std::io::Result<u64> {
        let to = self._socket_address()?;
        let Self {
            src_items,
            ..
        } = self;
        let conn = TcpStream::connect(to)?;
        Transfer {
            outgoing: conn,
            items: src_items,
        }
        .send()
    }
}

impl TryFrom<MetaInfo> for Vec<u8> {
    type Error = std::io::Error;

    fn try_from(value: MetaInfo) -> Result<Self, Self::Error> {
        Ok(Vec::with_capacity(value.bytes))
    }
}

/// Sync tcp stream transfer
pub struct Transfer<T> {
    outgoing: TcpStream,
    items: Vec<Item<T>>,
}

impl<T> Transfer<T> {
    #[inline]
    pub fn send_flag(&mut self) -> std::io::Result<()> {
        self.outgoing.write(&[Flag::Transfer.to_u8()])?;
        Ok(())
    }
}

impl<T: AsRef<[u8]>> Transfer<T> {
    /// Send all binary items to dest
    pub fn send(mut self) -> std::io::Result<u64> {
        let mut total_written = 0;
        if self.items.is_empty() {
            return Ok(total_written);
        }

        self.send_flag()?;

        let Self {
            mut outgoing,
            items,
            ..
        } = self;
        for item in items {
            let item_length = item.meta.bytes;
            if item_length == 0 {
                continue;
            }
            let written = outgoing.write(item.payload.as_ref())?;
            total_written += written as u64;
        }
        Ok(total_written)
    }
}