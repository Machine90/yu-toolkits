pub mod cp_file;

use std::{
    io::{Error, ErrorKind},
    time::Duration,
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};
use vendor::prelude::local_dns_lookup;

use super::{
    utils::{read_u32, read_u64},
    FastCp, Flag, Item, Packet, Session,
};

/// Sync tcp stream transfer
pub struct Transfer<T> {
    outgoing: TcpStream,
    items: Vec<Item<T>>,
}

impl<T> Transfer<T> {
    #[inline]
    pub async fn send_flag_async(&mut self) -> std::io::Result<()> {
        self.outgoing.write(&[Flag::Transfer.to_u8()]).await?;
        Ok(())
    }
}

impl<T: AsRef<[u8]>> Transfer<T> {
    /// Send all binary items to dest
    pub async fn send_async(mut self) -> std::io::Result<u64> {
        let mut total_written = 0;
        if self.items.is_empty() {
            return Ok(total_written);
        }

        self.send_flag_async().await?;

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
            let written = outgoing.write(item.payload.as_ref()).await?;
            total_written += written as u64;
        }
        Ok(total_written)
    }
}

impl Packet {
    pub async fn read_from_async<R: AsyncReadExt + Unpin>(
        incoming: &mut R,
    ) -> std::io::Result<Self> {
        let mut len_buf: [u8; 8] = [0; 8];
        let mut _readed = incoming.read(&mut len_buf).await?;
        let len = read_u64(&len_buf)?;
        let mut crc_buf = [0; 4];
        _readed += incoming.read(&mut crc_buf).await?;
        let checksum = read_u32(&crc_buf)?;
        let mut content_buf = vec![0u8; len as usize];
        _readed += incoming.read(content_buf.as_mut_slice()).await?;
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
    pub async fn try_handshake_async(
        self,
        with: &str,
        timeout_dur: Duration,
    ) -> std::io::Result<Transfer<T>> {
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
        let stream = timeout(timeout_dur, TcpStream::connect(socket_addr)).await;
        if let Err(_) = stream {
            return Err(Error::new(
                ErrorKind::TimedOut,
                format!(
                    "try establish connect to {socket_addr} in timeout {:?}md, but failed",
                    timeout_dur.as_millis()
                ),
            ));
        }
        let mut stream = stream.unwrap()?;

        let buf: Vec<u8> = packet.into();
        // step 1: send session to target.
        stream.write(&[Flag::Handshake.to_u8()]).await?;
        let _s = stream.write(buf.as_slice()).await?;
        stream.flush().await?;
        // step 2: waiting for reply
        let reply: Packet = Packet::read_from_async(&mut stream).await?;
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
    pub async fn send_async(self) -> std::io::Result<u64> {
        let to = self._socket_address()?;
        let Self { src_items, .. } = self;
        let conn = TcpStream::connect(to).await?;
        Transfer {
            outgoing: conn,
            items: src_items,
        }
        .send_async()
        .await
    }
}

#[cfg(test)]
mod tests {
    use crate::{fast_cp::FastCp, tokio1::runtime::Runtime};

    #[test]
    fn test_scp_async() {
        Runtime::new().unwrap().block_on(async {
            let mut scp = FastCp::new();
            let r = scp.src_dir_async("../../assets", true).await;
            assert!(r.is_ok());
        });
    }
}
