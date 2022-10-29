use crate::{
    runtime,
    tokio1::{
        fs::{File, OpenOptions, ReadDir},
        net::TcpStream,
        sync::RwLock,
    },
};
use crc32fast::Hasher;

use crate::fast_cp::{utils::stack_buffer_visit, FastCp, Item, MetaInfo};
use std::{
    future::Future,
    io::{Error, ErrorKind},
    path::{Path, PathBuf},
    sync::Arc,
};

use super::Transfer;

pub type CpFile = FastCp<File>;
pub type FileItem = Item<File>;
pub type FileMeta = MetaInfo;
pub const EMPTY_CRC: u32 = 0;

impl FileItem {
    pub async fn from_file_async(fp: PathBuf, with_checksum: bool) -> std::io::Result<Self> {
        if !fp.is_file() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("{:?} is not a file", fp),
            ));
        }
        let identify = fp.to_str().unwrap().to_owned();

        let mut checksum = None;
        if with_checksum {
            let file = OpenOptions::new().read(true).open(fp.clone()).await?;
            checksum = Some(file_crc32(file).await?);
        }
        let file = OpenOptions::new().read(true).open(fp).await?;
        let meta = file.metadata().await?;

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
    pub async fn crc32(self) -> std::io::Result<u32> {
        file_crc32(self.payload).await
    }
}

pub async fn file_crc32(file: File) -> std::io::Result<u32> {
    let mut hasher = Hasher::new();
    let file = file.into_std().await;
    let crc32 = runtime::spawn_blocking(move || {
        let mut file = file;
        // it's a "compute bound" scenario. thus run it in a
        // standalone thread
        stack_buffer_visit(&mut file, |snippet| {
            hasher.update(snippet);
            Ok(())
        })?;
        Ok(hasher.finalize())
    })
    .await?;
    crc32
}

impl CpFile {
    pub async fn src_dir_async<P: AsRef<Path>>(
        &mut self,
        dir: P,
        compute_checksum: bool,
    ) -> std::io::Result<()> {
        let mut items = vec![];
        let items_mut = Arc::new(RwLock::new(&mut items));
        scan_files(
            dir,
            |file| async {
                let item = FileItem::from_file_async(file, compute_checksum).await;
                if let Err(_) = item {
                    return false;
                }
                let item = item.unwrap();
                items_mut.write().await.push(item);
                true
            },
            false,
        )
        .await?;
        self.src_items = items;
        Ok(())
    }

    pub async fn flush_async(self) -> std::io::Result<u64> {
        let to = self._socket_address()?;
        let Self {
            src_items,
            ..
        } = self;

        if src_items.is_empty() {
            return Ok(0);
        }
        
        let conn = TcpStream::connect(to).await?;

        Transfer {
            outgoing: conn,
            items: src_items,
        }
        .flush_async().await
    }
}

impl Transfer<File> {
    /// Flush file-items to socket IO without
    /// use heap memory.
    pub async fn flush_async(mut self) -> std::io::Result<u64> {
        let mut total_written = 0;
        if self.items.is_empty() {
            // nothing to send.
            return Ok(total_written);
        }

        self.send_flag_async().await?;
        let Self {
            mut outgoing,
            items,
            ..
        } = self;
        for mut item in items {
            if item.meta.bytes == 0 {
                continue;
            }
            let written = tokio::io::copy(&mut item.payload, &mut outgoing).await?;
            total_written += written as u64;
        }
        Ok(total_written)
    }
}

/// Scan specified directory async and visit all files under
/// this directory. The `PathBuf` passed in always be file.
///
/// ## Example
/// ```rust
/// let _ = scan_dir("snapshot", |path| async move {
///     println!("{:?}", path);
///     true
/// }, true).await;
/// ```
pub async fn scan_files<P: AsRef<Path>, F, Fut>(
    dir: P,
    mut collect: F,
    ignore_err: bool,
) -> std::io::Result<()>
where
    Fut: Future<Output = bool> + Send,
    F: FnMut(PathBuf) -> Fut + Send,
{
    let dir = dir.as_ref();
    if dir.is_file() {
        collect(dir.to_path_buf()).await;
        return Ok(());
    }
    let dir: ReadDir = tokio::fs::read_dir(dir).await?;
    let _r = _scan_dir(dir, collect, ignore_err).await;
    Ok(())
}

#[async_recursion::async_recursion]
async fn _scan_dir<F, Fut>(
    mut current_dir: ReadDir,
    mut collect: F,
    ignore_err: bool,
) -> std::io::Result<F>
where
    Fut: Future<Output = bool> + Send,
    F: FnMut(PathBuf) -> Fut + Send,
{
    loop {
        let item = current_dir.next_entry().await;
        if let Err(e) = item {
            if ignore_err {
                continue;
            }
            return Err(e);
        }
        let item = item.unwrap();
        if item.is_none() {
            break;
        }
        let item = item.unwrap().path();
        if item.is_dir() {
            let children = tokio::fs::read_dir(item).await;
            if let Err(e) = children {
                if ignore_err {
                    continue;
                }
                return Err(e);
            }
            collect = _scan_dir(children.unwrap(), collect, ignore_err).await?;
            continue;
        }
        let succes = collect(item).await;
        if !succes {
            return Err(Error::new(
                ErrorKind::Interrupted,
                "scan process has been cancel by collector",
            ));
        }
    }
    Ok(collect)
}
