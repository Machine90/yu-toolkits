use byteorder::{BigEndian, ByteOrder};
use std::{
    fmt::Debug,
    io::{Error, ErrorKind, Read, Result},
    mem::MaybeUninit,
};

#[allow(unused)]
pub(crate) const DEFAULT_BUF_SIZE: usize = if cfg!(target_os = "espidf") {
    512
} else {
    8 * 1024
};

pub fn write_u16(num: u16) -> [u8; 2] {
    let mut buf = [0; 2];
    BigEndian::write_u16(&mut buf, num);
    buf
}

pub fn read_u16(buf: &[u8]) -> Result<u16> {
    _check_bits(buf.len(), 2)?;
    Ok(BigEndian::read_u16(buf))
}

pub fn write_u32(num: u32) -> [u8; 4] {
    let mut buf = [0; 4];
    BigEndian::write_u32(&mut buf, num);
    buf
}

pub fn read_u32(buf: &[u8]) -> Result<u32> {
    _check_bits(buf.len(), 4)?;
    Ok(BigEndian::read_u32(buf))
}

pub fn write_u64(num: u64) -> [u8; 8] {
    let mut buf = [0; 8];
    BigEndian::write_u64(&mut buf, num);
    buf
}

pub fn read_u64(buf: &[u8]) -> Result<u64> {
    _check_bits(buf.len(), 8)?;
    Ok(BigEndian::read_u64(buf))
}

pub fn write_i16(num: i16) -> [u8; 2] {
    let mut buf = [0; 2];
    BigEndian::write_i16(&mut buf, num);
    buf
}

pub fn read_i16(buf: &[u8]) -> Result<i16> {
    _check_bits(buf.len(), 2)?;
    Ok(BigEndian::read_i16(buf))
}

pub fn write_i32(num: i32) -> [u8; 4] {
    let mut buf = [0; 4];
    BigEndian::write_i32(&mut buf, num);
    buf
}

pub fn read_i32(buf: &[u8]) -> Result<i32> {
    _check_bits(buf.len(), 4)?;
    Ok(BigEndian::read_i32(buf))
}

pub fn write_i64(num: i64) -> [u8; 8] {
    let mut buf = [0; 8];
    BigEndian::write_i64(&mut buf, num);
    buf
}

pub fn read_i64(buf: &[u8]) -> Result<i64> {
    _check_bits(buf.len(), 8)?;
    Ok(BigEndian::read_i64(buf))
}

/// Check if actual
fn _check_bits(actual: usize, expected: usize) -> Result<()> {
    if actual < expected {
        Err(Error::new(
            ErrorKind::InvalidInput,
            format!(
                "unexpected buf size: {:?}, should not less than {:?}",
                actual, expected
            ),
        ))
    } else {
        Ok(())
    }
}

/// `BufferedReader` is used to manage buffered array, and
/// can be readed in given length.
/// ### Example
/// ```
/// let buf: [u8; 8] = [0,1,1,0,0,1,0,1];
/// let mut reader = BufferedReader::new(&buf);
/// 
/// let readed = reader.read_piece(3);
/// assert_eq!(readed, Some(&buf[0..3]));
/// 
/// let readed = reader.read_piece(6);
/// assert_eq!(readed, Some(&buf[3..8]));
/// 
/// let readed = reader.read_piece(3);
/// assert_eq!(readed, None);
/// ```
#[derive(Clone, Copy)]
pub struct BufferedReader<'a> {
    buf: &'a [u8],
    len: usize,
    offset: usize,
}

impl<'a> Read for BufferedReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        if self.is_empty() {
            return Ok(0);
        }
        let expected = buf.len();
        let len = std::cmp::min(expected, self.remain());
        for i in 0..len {
            buf[i] = self.buf[self.offset + i];
        }
        self.offset += len;
        Ok(len)
    }
}

impl<'a> Debug for BufferedReader<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BufferedReader")
            .field("remain", &(self.len - self.offset))
            .field("total", &self.len)
            .field("readed", &self.offset)
            .finish()
    }
}

impl<'a> BufferedReader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        let len = buf.len();
        Self {
            buf,
            len,
            offset: 0,
        }
    }

    pub fn read_piece(&mut self, bytes: usize) -> Option<&'a [u8]> {
        if self.is_empty() {
            return None;
        }
        let offset = self.offset;
        let expected = offset + bytes;
        let actual = std::cmp::min(expected, self.len);
        self.offset = actual;
        Some(&self.buf[offset..actual])
    }

    #[inline]
    pub fn update(&mut self, buf: &'a [u8]) {
        self.len = buf.len();
        self.buf = buf;
        self.offset = 0;
    }

    #[inline]
    pub fn reset(&mut self) {
        self.offset = 0;
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.offset == self.len
    }

    #[inline]
    pub fn remain(&self) -> usize {
        self.len - self.offset
    }
}

/// Visit byte codes from reader with visitor to the end, 
/// the byte codes will copy to stack buffer without
/// more memory allocate.
pub fn stack_buffer_visit<R, V>(reader: &mut R, mut visitor: V) -> std::io::Result<u64>
where
    R: Read + ?Sized,
    V: FnMut(&[u8]) -> std::io::Result<()>,
{
    let mut buf = MaybeUninit::<[u8; DEFAULT_BUF_SIZE]>::uninit();
    let mut written = 0u64;
    loop {
        let len = match reader.read(unsafe { buf.assume_init_mut() }) {
            Ok(0) => return Ok(written),
            Ok(len) => len,
            Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };
        written += len as u64;
        visitor(unsafe { &buf.assume_init_ref()[..len] })?;
    }
}

/// Remove element from vector if checker return true.
pub fn vec_remove<T, F>(elements: &mut Vec<T>, checker: F) where F: Fn(&mut T) -> bool {
    let mut i = 0;
    while i < elements.len() {
        if checker(&mut elements[i]) {
            let _ = elements.remove(i);
        } else {
            i += 1;
        }
    }
}

pub mod std_file {
    use crc32fast::Hasher;

    use super::*;
    use std::{
        fs::{File, ReadDir},
        io::Error,
        path::{Path, PathBuf},
    };

    pub fn ls_files<P: AsRef<Path>>(dir: P) -> Result<Vec<PathBuf>> {
        let mut collect = Vec::new();
        scan_files(
            dir,
            |file| {
                collect.push(file);
                true
            },
            false,
        )?;
        Ok(collect)
    }

    pub fn file_crc32(mut file: File) -> std::io::Result<u32> {
        let mut hasher = Hasher::new();
        stack_buffer_visit(&mut file, |snippet| {
            hasher.update(snippet);
            Ok(())
        })?;
        Ok(hasher.finalize())
    }

    pub fn scan_files<P: AsRef<Path>, F>(dir: P, mut collect: F, ignore_err: bool) -> Result<()>
    where
        F: FnMut(PathBuf) -> bool,
    {
        let dir = dir.as_ref();
        if dir.is_file() {
            collect(dir.to_path_buf());
            return Ok(());
        }
        let dir = std::fs::read_dir(dir)?;
        let _ = _scan_files(dir, collect, ignore_err);
        Ok(())
    }

    fn _scan_files<F>(current_dir: ReadDir, mut collect: F, ignore_err: bool) -> Result<F>
    where
        F: FnMut(PathBuf) -> bool,
    {
        for item in current_dir {
            if let Err(e) = item {
                if ignore_err {
                    continue;
                }
                return Err(e);
            }
            let item = item.unwrap().path();
            if item.is_dir() {
                let children = std::fs::read_dir(item);
                if let Err(e) = children {
                    if ignore_err {
                        continue;
                    }
                    return Err(e);
                }
                collect = _scan_files(children.unwrap(), collect, ignore_err)?;
                continue;
            }
            let succes = collect(item);
            if !succes {
                return Err(Error::new(
                    ErrorKind::Interrupted,
                    "scan process has been cancel by collector",
                ));
            }
        }
        Ok(collect)
    }
}

pub mod unit_fmt {
    #[allow(unused)]
    const UNITS: [&'static str; 8] = ["B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB"];

    /// compute bytes to it's readable unit format.
    /// e.g. 1024 bytes will be "1 KB"
    pub fn fmt_bytes(mut bytes: f64) -> String {
        let (mut prev, mut i) = (0.0, 0);
        while bytes.floor() > 0. && i < UNITS.len() {
            prev = bytes;
            bytes /= 1024.0;
            i += 1;
        }
        if i > 0 {
            bytes = prev;
            i -= 1;
        }
        let round_v = ((bytes * 100.0).round() as u64) / 100;
        format!("{:?} {}", round_v, UNITS[i])
    }

    #[inline]
    pub fn fmt_percent(percent: f64) -> String {
        format!("{}%", (percent * 100.).round() as u64 / 100)
    }

    #[inline]
    pub fn percent_used(used: f64, capacity: f64) -> f64 {
        if capacity <= 0.0 {
            100.0
        } else {
            (used * 100.0) / capacity
        }
    }

    #[inline]
    pub fn percent_remaining(remaining: f64, capacity: f64) -> f64 {
        if capacity <= 0.0 {
            0.0
        } else {
            (remaining * 100.0) / capacity
        }
    }
}
