use super::handler::{ReceiveHandler, ReceiveHandlerRef};
use crate::fast_cp::{
    config::RecvConfig,
    progress::Progress,
    utils::{stack_buffer_visit, BufferedReader, vec_remove},
    Flag, MetaInfo, Packet, Session,
};
use byteorder::ReadBytesExt;
use std::{
    fmt::Debug,
    io::{ErrorKind, Read, Result, Write},
    net::{TcpListener, TcpStream},
    slice::Iter,
    sync::Arc,
    thread::JoinHandle,
    time::Duration,
};

/// Connection state with elapsed ms
#[derive(Debug)]
enum ConnState {
    Idle(u64),
    Transfering(u64),
    Finished,
}

impl ConnState {
    pub fn to_transfering(&mut self) {
        match self {
            ConnState::Idle(_) => *self = Self::Transfering(0),
            _ => (),
        }
    }

    #[allow(unused)]
    pub fn to_idle(&mut self) {
        match self {
            ConnState::Transfering(_) => *self = Self::Idle(0),
            _ => (),
        }
    }

    pub fn to_finish(&mut self) {
        match self {
            ConnState::Idle(_) => *self = Self::Finished,
            ConnState::Transfering(_) => *self = Self::Finished,
            _ => (),
        }
    }

    pub fn elapse(&mut self, dur_ms: u64) {
        match self {
            ConnState::Idle(timeout) => *self = ConnState::Idle(*timeout + dur_ms),
            ConnState::Transfering(timeout) => *self = ConnState::Transfering(*timeout + dur_ms),
            ConnState::Finished => (),
        };
    }

    #[allow(unused)]
    #[inline]
    pub fn elapsed_ms(&self) -> u64 {
        match self {
            ConnState::Idle(timeout) => *timeout,
            ConnState::Transfering(timeout) => *timeout,
            ConnState::Finished => 0,
        }
    }
}

/// Receiver (of [Item](crate::fast_cp::Item)) based on std tcp socket listener.
/// The receiver will listen on an optional socket 
/// and attempt to block receiving `items` from sender.
/// ### Example
/// #### Blocking for items
/// ```rust
/// // create a sender and prepare to send `items`.
/// let mut sender = FastCp::<&[u8]>::new();
/// sender.write_buffer("key", b"value", true);
/// 
/// // before that, communicate with session first.
/// // for example send session by other RPC protocol.
/// let session = sender.get_session();
/// let mut recv = Receiver::new(VecHandler);
/// let reply = recv.accept(session).unwrap();
/// 
/// // sender would update it's dest address and items by apply reply.
/// let applied = sender.apply(reply);
/// 
/// // then sehd `items` to receiver.
/// let _w = sender.send();
/// ```
pub struct Receiver<T> {
    recv_handler: ReceiveHandlerRef<T>,
    session: Option<Session>,
    config: RecvConfig,
    listener: Option<(TcpListener, u16)>,
}

impl<T: Write + 'static> Receiver<T> {
    pub fn new<H: ReceiveHandler<T> + 'static>(recv_handler: H) -> Self {
        Self {
            recv_handler: Arc::new(recv_handler),
            session: None,
            config: RecvConfig::default(),
            listener: None,
        }
    }

    pub fn from_handler(write_handler: ReceiveHandlerRef<T>) -> Self {
        Self {
            recv_handler: write_handler,
            session: None,
            config: RecvConfig::default(),
            listener: None,
        }
    }

    #[inline]
    pub fn set_config(&mut self, config: RecvConfig) {
        self.config = config;
    }

    /// Try listen on config port in range.
    #[inline]
    fn _try_listen(&mut self) -> Result<u16> {
        let mut bind_port = 0;
        let listener = self.config.try_establish(|socket, port| {
            bind_port = port;
            TcpListener::bind(socket)
        })?;
        self.listener = Some((listener, bind_port));
        Ok(bind_port)
    }

    fn _take_listener(&mut self) -> Result<(TcpListener, u16)> {
        if self.listener.is_none() {
            self._try_listen()?;
        }
        Ok(self.listener.take().unwrap())
    }

    pub fn accept_mut(&mut self, session: &mut Session) -> Result<()> {
        let items = &mut session.items;
        vec_remove(items, |item| { 
            self.recv_handler.exists(item)
        });
        let id = session.id;

        let port = self._try_listen()?;
        let listen_at = self.config.bind_address(port);

        session.listen_at = listen_at.clone();
        self.session = Some(Session { id, items: items.clone(), listen_at });
        Ok(())
    }

    /// Accept request session and reply with session
    /// that checked at local.
    pub fn accept(&mut self, session: Session) -> Result<Session> {
        let Session { mut items, id, .. } = session;

        vec_remove(&mut items, |item| { 
            self.recv_handler.exists(item)
        });

        let items_reply = items.clone();

        let port = self._try_listen()?;

        let listen_at = self.config.bind_address(port);
        self.session = Some(Session { id, items, listen_at: listen_at.clone() });
        let reply = Session {
            id,
            items: items_reply,
            listen_at
        };
        Ok(reply)
    }

    #[inline]
    fn _has_session(&self) -> Result<()> {
        if self.session.is_none() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "require session before run",
            ));
        }
        Ok(())
    }

    fn _check_timeout(config: &RecvConfig, current_state: &ConnState) -> bool {
        let RecvConfig {
            waiting_timeout_ms,
            read_timeout_ms,
            ..
        } = config;
        match current_state {
            ConnState::Idle(elapsed) => *waiting_timeout_ms <= *elapsed as usize,
            ConnState::Transfering(elapsed) => *read_timeout_ms <= *elapsed as usize,
            ConnState::Finished => true,
        }
    }

    #[inline]
    fn _check_flag(&self, flag: u8, expected: Flag) -> bool {
        match Flag::from_u8(flag) {
            Some(flag) => flag == expected,
            None => false,
        }
    }

    /// Do handshake and get a session
    fn do_handshake(
        write_handler: ReceiveHandlerRef<T>,
        stream: &mut TcpStream,
        bind_addr: &str,
    ) -> std::io::Result<Session> {
        // try read request
        let packet: Packet = Packet::read_from(stream)?;
        let Session { id, mut items, .. } = packet.try_into()?;

        // check and remove if items already exists
        let mut i = 0;
        while i < items.len() {
            if write_handler.exists(&items[i]) {
                let _ = items.remove(i);
            } else {
                i += 1;
            }
        }

        // reply
        let reply = Session { id, items, listen_at: bind_addr.to_owned() };
        let packet: Packet = reply.clone().try_into()?;
        let buf: Vec<u8> = packet.into();
        let _w = stream.write(buf.as_slice())?;
        stream.flush()?;
        Ok(reply)
    }

    /// Blocking for request incoming until timeout
    /// or finish transfering.
    #[inline]
    pub fn block(self) -> Result<u64> {
        let interval = 100u64; // ms
        self.block_with_interval(interval)
    }

    pub fn block_with_interval(mut self, block_interval: u64) -> Result<u64> {
        let (listener, port) = self._take_listener()?;

        let config = &self.config;
        let bind_address = config.bind_address(port);
        let mut total = 0;

        listener.set_nonblocking(true)?;
        let mut conn_state = ConnState::Idle(0);

        let mut hint = None;

        for stream in listener.incoming() {
            match stream {
                Ok(mut incoming) => {
                    let remote_address = incoming.peer_addr()?;

                    let recv_handler = self.recv_handler.clone();
                    // blocking for this incoming.
                    let _ = incoming.set_nonblocking(false)?;

                    let mut flag = incoming.read_u8()?;
                    let session = if self._check_flag(flag, Flag::Handshake) {
                        // do handshake first
                        conn_state.to_transfering();
                        let session = Self::do_handshake(
                            recv_handler.clone(), 
                            &mut incoming, 
                            bind_address.as_str()
                        )?;
                        flag = incoming.read_u8()?;
                        conn_state.to_idle();
                        session
                    } else {
                        self._has_session()?;
                        let session = self.session.as_ref().unwrap();

                        if !session.check_access(remote_address) {
                            hint = Some("unexpected remote address");
                            continue;
                        }

                        session.clone()
                    };

                    if !self._check_flag(flag, Flag::Transfer) {
                        hint = Some("unexpected access flag");
                        continue;
                    }

                    let mut acceptor = Acceptor::new(session.items.iter(), recv_handler)?;
                    conn_state.to_transfering();
                    acceptor.record_progress();
                    let total_read = acceptor.step(&mut incoming)?;
                    conn_state.to_finish();
                    total = total_read;
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(block_interval));
                    conn_state.elapse(block_interval);

                    match &conn_state {
                        ConnState::Finished => break,
                        _ => {
                            if Self::_check_timeout(config, &conn_state) {
                                let err_msg = if let Some(hint) = hint {
                                    format!(
                                        "timeout for {}ms when blocking for FastCp client, hint: {}",
                                        conn_state.elapsed_ms(), hint
                                    )
                                } else {
                                    format!(
                                        "timeout for {}ms when blocking for FastCp client",
                                        conn_state.elapsed_ms()
                                    )
                                };
                                return Err(std::io::Error::new(ErrorKind::TimedOut, err_msg));
                            }
                            continue;
                        }
                    }
                }
                Err(e) => {
                    return Err(std::io::Error::new(
                        ErrorKind::ConnectionAborted,
                        format!("IO error when blocking for FastCp client, see: {}", e),
                    ));
                }
            }
        }
        Ok(total)
    }

    pub fn daemon(self) -> JoinHandle<Result<u64>> {
        let this = self;
        std::thread::spawn(move || this.block())
    }
}

pub struct Acceptor<'a, W: Write + 'static> {
    iter: Iter<'a, MetaInfo>,
    current: &'a MetaInfo,
    current_remained: usize,
    writer: W,
    write_handler: ReceiveHandlerRef<W>,
    finished: bool,
    progress: Option<Progress>,
}

impl<'a, W: Write + 'static> Acceptor<'a, W> {
    pub fn new(mut iter: Iter<'a, MetaInfo>, wh: ReceiveHandlerRef<W>) -> Result<Self> {
        let first = iter.next();
        if first.is_none() {
            return Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                "metainfo list should not be empty"
            ));
        }
        let first = first.unwrap();
        let writer = wh.open_writer(first)?;
        let mut this = Self {
            iter,
            current: first,
            current_remained: first.bytes,
            write_handler: wh,
            writer,
            finished: false,
            progress: None,
        };
        if this.current_remained == 0 {
            this._advance()?;
        }
        Ok(this)
    }

    #[inline]
    pub fn record_progress(&mut self) {
        let mut progress = Progress::new();
        progress.current_progress(&self.current);
        self.progress = Some(progress);
    }

    pub fn step<R: Read + ?Sized>(mut self, incoming: &mut R) -> Result<u64> {
        if self.finished {
            return Ok(0);
        }
        let total_size = stack_buffer_visit(incoming, |buf| {
            let mut buffered = BufferedReader::new(buf);
            while let Some(buf) = buffered.read_piece(self.current_remained) {
                let read_len = buf.len();
                let advance = read_len >= self.current_remained;
                self._write(buf, advance)?;
                if advance {
                    self._advance()?;
                } else {
                    self.current_remained -= read_len;
                }
                if self.finished {
                    break;
                }
            }
            Ok(())
        })?;
        Ok(total_size)
    }

    fn _write(&mut self, buf: &[u8], flush: bool) -> Result<usize> {
        let written = self.writer.write(buf)?;
        if let Some(progress) = self.progress.as_mut() {
            progress.update(buf);
            self.write_handler.update_progress(progress);
        }
        if flush {
            self.writer.flush()?;
        }
        Ok(written)
    }

    fn _advance(&mut self) -> Result<()> {
        let next = self.iter.next();

        if next.is_none() {
            self.write_handler.complete(&mut self.writer);
            self.finished = true;
            return Ok(());
        }

        self.write_handler.complete(&mut self.writer);

        let current = next.unwrap();
        self.current_remained = current.bytes;
        self.writer = self.write_handler.open_writer(current)?;

        if let Some(progress) = self.progress.as_mut() {
            progress.check();
            progress.current_progress(&current);
        }

        self.current = current;

        if self.current_remained == 0 {
            self._advance()?;
        }
        Ok(())
    }
}
