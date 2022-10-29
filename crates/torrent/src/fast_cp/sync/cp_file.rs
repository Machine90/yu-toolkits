use crate::fast_cp::{
    utils::std_file::{file_crc32, scan_files},
    FastCp, Item, MetaInfo,
};
use std::{
    fs::{File, OpenOptions},
    io::{Error, ErrorKind, Read},
    net::TcpStream,
    path::{Path, PathBuf},
};

use super::Transfer;

pub type CpFile = FastCp<File>;
pub type FileItem = Item<File>;
pub type FileMeta = MetaInfo;
pub const EMPTY_CRC: u32 = 0;

impl TryFrom<FileMeta> for File {
    type Error = std::io::Error;

    fn try_from(meta: FileMeta) -> Result<Self, Self::Error> {
        let path = Path::new(meta.identify.as_str());
        if !path.is_file() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("unexpected file path: {}, it's a directory", meta.identify),
            ));
        }
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path)?;
        Ok(file)
    }
}

impl FileItem {
    pub fn from_file(fp: PathBuf, with_checksum: bool) -> std::io::Result<Self> {
        if !fp.is_file() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("{:?} is not a file", fp),
            ));
        }
        let identify = fp.to_str().unwrap().to_owned();

        let mut checksum = None;
        if with_checksum {
            let file = OpenOptions::new().read(true).open(fp.clone())?;
            checksum = Some(file_crc32(file)?);
        }
        let file = OpenOptions::new().read(true).open(fp)?;
        let meta = file.metadata()?;

        let item = Self {
            meta: FileMeta {
                identify,
                bytes: meta.len() as usize,
                checksum: checksum.unwrap_or_default(),
            },
            payload: file,
        };
        Ok(item)
    }

    /// Calculate crc32 of this `FileItem`, this
    /// operation will consume instance of file.
    #[inline]
    pub fn crc32(self) -> std::io::Result<u32> {
        file_crc32(self.payload)
    }
}

impl Read for FileItem {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.payload.read(buf)
    }
}

impl CpFile {
    pub fn src_dir<P: AsRef<Path>>(
        &mut self,
        dir: P,
        compute_checksum: bool,
    ) -> std::io::Result<()> {
        let mut items = vec![];
        let mut total_size = 0;
        scan_files(
            dir,
            |file| {
                let item = FileItem::from_file(file, compute_checksum);
                if let Err(_) = item {
                    return false;
                }
                let item = item.unwrap();
                total_size += item.meta.bytes;
                items.push(item);
                true
            },
            false,
        )?;
        self.src_items = items;
        Ok(())
    }

    pub fn flush(self) -> std::io::Result<u64> {
        let to = self._socket_address()?;
        let Self {
            src_items,
            ..
        } = self;

        if src_items.is_empty() {
            return Ok(0);
        }

        let conn = TcpStream::connect(to)?;

        Transfer {
            outgoing: conn,
            items: src_items,
        }
        .flush()
    }
}

impl Transfer<File> {

    /// Flush file-items to socket IO without 
    /// use heap memory.
    pub fn flush(mut self) -> std::io::Result<u64> {
        let mut total_written = 0;
        if self.items.is_empty() {
            // nothing to send.
            return Ok(total_written);
        }

        self.send_flag()?;
        let Self {
            mut outgoing,
            items,
            ..
        } = self;
        for mut item in items {
            if item.meta.bytes == 0 {
                continue;
            }
            let written = std::io::copy(&mut item, &mut outgoing)?;
            total_written += written;
        }
        Ok(total_written)
    }
}
