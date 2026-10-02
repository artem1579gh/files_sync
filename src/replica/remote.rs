//! A replica on another host, reached over TLS (design §7.1).
//!
//! [`RemoteReplica`] implements [`Replica`] as a stub of the wire protocol
//! ([`super::proto`]): every call is a request to the `serve` process that
//! runs the replica (see [`crate::server`]), and every precondition is
//! checked there, next to the files. So the network adds no race.
//!
//! **Connections.** Requests go over an idle connection, one at a time;
//! [`Replica::open_read`]'s reader takes the connection along while it
//! streams and hands it back once the stream ended (or after draining a
//! small rest when dropped early; a large one is closed instead). A request
//! that finds no idle connection opens a new one, so the protocol never
//! gets out of step, whatever the caller does with readers. A connection
//! that fails is closed; the call fails with [`Error::Connection`] (fatal for
//! the sync cycle) and the next call connects again.
//!
//! **Index exchange.** The replica mirrors the server's index (wire form) and
//! asks only for the entries put since the last seq it saw
//! (`ChangesSince { seq }`); [`Replica::changes_since`] is answered from the
//! mirror. Entries only disappear by tombstone collection, which goes
//! through [`Replica::record_sync`] and is applied to the mirror. The mirror
//! starts over on each new connection (the server's index may have been
//! replaced meanwhile).
//!
//! **Hints.** [`Replica::watch`] opens a separate connection, sends `Watch`,
//! and a thread forwards the server's pushed hints. If that connection is
//! lost, the thread reconnects (with backoff) and sends a
//! [`Hint::FullRescan`], since changes may have been missed meanwhile.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream, ToSocketAddrs};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};
use rustls::{ClientConfig, ClientConnection, StreamOwned};

use super::proto::{
    BATCH_BYTES, Content, ContentStream, Request, Response, batches, client_handshake, read_frame,
    send_content, write_frame,
};
use super::{ContentReader, Op, Outcome, Precondition, Replica};
use crate::config::{ReplicaConfig, ReplicaId};
use crate::error::{Error, Result};
use crate::fs::RelPath;
use crate::index::{Entry, Kind, PeerState, VersionVector};
use crate::scan::{ScanStats, Scope};
use crate::tls::{Identity, server_name};
use crate::watch::Hint;

/// How long a TCP connect may take, per address.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// How long the TLS and protocol handshakes may take.
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);

/// A content reader dropped before its end drains the rest if the file is
/// at most this big (keeping the connection); a bigger one closes it.
const DRAIN_LIMIT: u64 = 1 << 20;

/// First and longest wait before the hint connection is tried again.
const RECONNECT_MIN: Duration = Duration::from_millis(500);
const RECONNECT_MAX: Duration = Duration::from_secs(30);

/// A TLS connection to a server.
type Tls = StreamOwned<ClientConnection, TcpStream>;

/// The filter of a content stream on the client: only content belongs there.
type Filter = fn(Response) -> Result<Option<Content>>;

fn content_only(msg: Response) -> Result<Option<Content>> {
    match msg {
        Response::Content(c) => Ok(Some(c)),
        other => Err(unexpected(&other)),
    }
}

fn unexpected(msg: &Response) -> Error {
    Error::Protocol {
        reason: format!("unexpected response {}", response_name(msg)),
    }
}

fn response_name(msg: &Response) -> &'static str {
    match msg {
        Response::Scanned(_) => "Scanned",
        Response::Changes { .. } => "Changes",
        Response::Reading => "Reading",
        Response::Applied(_) => "Applied",
        Response::Watching(_) => "Watching",
        Response::Adopted(_) => "Adopted",
        Response::Collected { .. } => "Collected",
        Response::Error(_) => "Error",
        Response::Content(_) => "Content",
        Response::Hint(_) => "Hint",
    }
}

/// Turns on TCP keepalive (a peer that vanished is noticed within about two
/// minutes, even by a connection that only waits) and disables Nagle (our
/// messages are small and answered one by one).
pub(crate) fn tune_socket(tcp: &TcpStream) {
    use rustix::net::sockopt;
    let result = sockopt::set_socket_keepalive(tcp, true)
        .and_then(|()| sockopt::set_tcp_keepidle(tcp, Duration::from_secs(60)))
        .and_then(|()| sockopt::set_tcp_keepintvl(tcp, Duration::from_secs(10)))
        .and_then(|()| sockopt::set_tcp_keepcnt(tcp, 6))
        .and_then(|()| sockopt::set_tcp_nodelay(tcp, true));
    if let Err(e) = result {
        tracing::debug!(error = %e, "cannot set socket options");
    }
}

/// Reads the next message; a closed connection is an error here.
fn recv(tls: &mut Tls) -> Result<Response> {
    read_frame(tls)?.ok_or_else(|| Error::Protocol {
        reason: "connection closed by the server".into(),
    })
}

/// Where and how to reach the server, and the idle connection.
struct Link {
    addr: String,
    /// The replica we act for (the server's peer).
    local: ReplicaId,
    /// The replica the server runs.
    remote: ReplicaId,
    tls: Arc<ClientConfig>,
    idle: Mutex<Option<Conn>>,
    /// Numbers connections, so the mirror notices a new one.
    sessions: AtomicU64,
}

/// An established connection.
struct Conn {
    tls: Tls,
    session: u64,
}

impl Link {
    fn lost(&self, reason: impl std::fmt::Display) -> Error {
        Error::Connection {
            peer: self.addr.clone(),
            reason: reason.to_string(),
        }
    }

    /// Opens a connection: TCP, TLS (both sides check the pinned
    /// certificates), then the protocol handshake.
    fn connect(&self) -> Result<Tls> {
        let addrs = self
            .addr
            .to_socket_addrs()
            .map_err(|e| self.lost(format!("cannot resolve the address: {e}")))?;
        let mut last = None;
        let mut tcp = None;
        for addr in addrs {
            match TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT) {
                Ok(s) => {
                    tcp = Some(s);
                    break;
                }
                Err(e) => last = Some(e),
            }
        }
        let tcp = tcp.ok_or_else(|| match last {
            Some(e) => self.lost(e),
            None => self.lost("the address resolves to nothing"),
        })?;
        tune_socket(&tcp);
        let timeouts = |t: Option<Duration>| {
            tcp.set_read_timeout(t)
                .and_then(|()| tcp.set_write_timeout(t))
                .map_err(|e| self.lost(e))
        };
        timeouts(Some(HANDSHAKE_TIMEOUT))?;
        let conn = ClientConnection::new(self.tls.clone(), server_name())
            .map_err(|e| self.lost(format!("TLS: {e}")))?;
        let mut tls = StreamOwned::new(conn, tcp);
        while tls.conn.is_handshaking() {
            tls.conn
                .complete_io(&mut tls.sock)
                .map_err(|e| self.lost(format!("TLS handshake failed: {e}")))?;
        }
        // TLS 1.3: the server checks our certificate after we finished, so
        // a rejection shows up here, as an alert.
        client_handshake(&mut tls, self.local, self.remote)
            .map_err(|e| self.lost(format!("handshake failed: {e}")))?;
        let tcp = &tls.sock;
        tcp.set_read_timeout(None)
            .and_then(|()| tcp.set_write_timeout(None))
            .map_err(|e| self.lost(e))?;
        Ok(tls)
    }

    /// The idle connection, or a new one.
    fn take(&self) -> Result<Conn> {
        let idle = lock(&self.idle).take();
        if let Some(conn) = idle {
            if is_idle(&conn.tls.sock) {
                return Ok(conn);
            }
            tracing::debug!(addr = %self.addr, "idle connection closed by the server; reconnecting");
        }
        let tls = self.connect()?;
        let session = self.sessions.fetch_add(1, Ordering::Relaxed) + 1;
        tracing::debug!(addr = %self.addr, session, "connected");
        Ok(Conn { tls, session })
    }

    /// Makes `conn` the idle connection, unless there is one already.
    fn put(&self, conn: Conn) {
        let mut idle = lock(&self.idle);
        if idle.is_none() {
            *idle = Some(conn);
        }
    }

    /// Opens a hint connection: `None` if the server has no watcher.
    fn watch(&self) -> Result<Option<Tls>> {
        let mut tls = self.connect()?;
        let answer = write_frame(&mut tls, &Request::Watch).and_then(|()| recv(&mut tls));
        match answer.map_err(|e| self.lost(e))? {
            Response::Watching(true) => Ok(Some(tls)),
            Response::Watching(false) => Ok(None),
            Response::Error(e) => Err(e.into()),
            other => Err(self.lost(unexpected(&other))),
        }
    }
}

/// The idle connection is still open and has nothing to read (an idle
/// connection never has: anything there is the server closing it, e.g. a
/// TLS `close_notify` from a server that stopped).
fn is_idle(tcp: &TcpStream) -> bool {
    if tcp.set_nonblocking(true).is_err() {
        return false;
    }
    let peeked = tcp.peek(&mut [0u8; 1]);
    if tcp.set_nonblocking(false).is_err() {
        return false;
    }
    matches!(peeked, Err(e) if e.kind() == std::io::ErrorKind::WouldBlock)
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// The server's index as far as we have seen it.
#[derive(Default)]
struct Mirror {
    /// The connection it was built over.
    session: u64,
    /// The highest seq received.
    seen: u64,
    entries: BTreeMap<RelPath, Entry>,
    /// Entries received over the wire in all (for tests and logs).
    received: u64,
}

/// The hint side of [`RemoteReplica::watch`].
enum Watching {
    /// The server has no watcher.
    No,
    Yes {
        hints: Receiver<Hint>,
        /// Dropped to stop the hint thread.
        _stop: Sender<()>,
        /// The hint thread's current socket, shut down to stop it.
        socket: Arc<Mutex<Option<TcpStream>>>,
    },
}

/// A replica served by a `serve` process on another host (or another
/// process), reached over TLS.
pub struct RemoteReplica {
    link: Arc<Link>,
    mirror: Mutex<Mirror>,
    watching: Option<Watching>,
}

impl std::fmt::Debug for RemoteReplica {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteReplica")
            .field("addr", &self.link.addr)
            .field("replica", &self.link.remote)
            .finish_non_exhaustive()
    }
}

impl RemoteReplica {
    /// Connects to the server of `remote` (at its [`ReplicaConfig::remote`]
    /// address) on behalf of its peer `local`, presenting `local`'s
    /// identity from `pair_dir` and accepting only `remote`'s pinned
    /// certificate.
    pub fn connect(
        remote: &ReplicaConfig,
        local: &ReplicaConfig,
        pair_dir: &Path,
    ) -> Result<RemoteReplica> {
        let addr = remote.remote.clone().ok_or_else(|| Error::Connection {
            peer: format!("replica {}", remote.id),
            reason: "the config gives no remote address".into(),
        })?;
        let identity = Identity::load(pair_dir, local.id, local.device()?)?;
        let tls = identity.client_config(remote.device()?)?;
        RemoteReplica::with_tls(addr, local.id, remote.id, tls)
    }

    /// Connects to `addr`, which must serve `remote`, on behalf of `local`,
    /// with the TLS client config `tls`. Fails if the first connection
    /// cannot be made.
    pub fn with_tls(
        addr: impl Into<String>,
        local: ReplicaId,
        remote: ReplicaId,
        tls: Arc<ClientConfig>,
    ) -> Result<RemoteReplica> {
        let link = Arc::new(Link {
            addr: addr.into(),
            local,
            remote,
            tls,
            idle: Mutex::new(None),
            sessions: AtomicU64::new(0),
        });
        let conn = link.take()?;
        link.put(conn);
        Ok(RemoteReplica {
            link,
            mirror: Mutex::new(Mirror::default()),
            watching: None,
        })
    }

    /// The server's address.
    pub fn addr(&self) -> &str {
        &self.link.addr
    }

    /// Index entries received from the server so far (each `ChangesSince`
    /// sends only what changed since the last one).
    pub fn entries_received(&self) -> u64 {
        lock(&self.mirror).received
    }

    /// Connections opened so far.
    pub fn connections(&self) -> u64 {
        self.link.sessions.load(Ordering::Relaxed)
    }

    /// Runs one exchange over a connection. `f`'s outer error is the
    /// connection's: it is closed, and the error becomes
    /// [`Error::Connection`]. The inner result is the server's answer.
    fn call<T>(&self, f: impl FnOnce(&mut Conn) -> Result<Result<T>>) -> Result<T> {
        let mut conn = self.link.take()?;
        match f(&mut conn) {
            Ok(answer) => {
                self.link.put(conn);
                answer
            }
            Err(e) => {
                let e = match e {
                    e @ Error::Connection { .. } => e,
                    e => self.link.lost(e),
                };
                tracing::warn!(error = %e, "connection to the remote replica lost");
                Err(e)
            }
        }
    }

    /// Sends `req` and reads its single answer, which `pick` takes apart.
    fn request<T>(&self, req: &Request, pick: impl FnOnce(Response) -> Option<T>) -> Result<T> {
        self.call(|conn| {
            write_frame(&mut conn.tls, req)?;
            match recv(&mut conn.tls)? {
                Response::Error(e) => Ok(Err(e.into())),
                msg => {
                    let name = response_name(&msg);
                    pick(msg).map(Ok).ok_or_else(|| Error::Protocol {
                        reason: format!("unexpected response {name}"),
                    })
                }
            }
        })
    }
}

impl Replica for RemoteReplica {
    fn id(&self) -> ReplicaId {
        self.link.remote
    }

    fn scan(&mut self, scope: Scope) -> Result<ScanStats> {
        self.request(&Request::Scan { scope }, |msg| match msg {
            Response::Scanned(stats) => Some(stats),
            _ => None,
        })
    }

    fn changes_since(&self, seq: u64) -> Result<Vec<(RelPath, Entry)>> {
        let mut guard = lock(&self.mirror);
        let mirror = &mut *guard;
        self.call(|conn| {
            if conn.session != mirror.session {
                *mirror = Mirror {
                    session: conn.session,
                    received: mirror.received,
                    ..Mirror::default()
                };
            }
            write_frame(&mut conn.tls, &Request::ChangesSince { seq: mirror.seen })?;
            loop {
                match recv(&mut conn.tls)? {
                    Response::Changes { entries, more } => {
                        mirror.received += entries.len() as u64;
                        for (path, entry) in entries {
                            mirror.seen = mirror.seen.max(entry.seq);
                            mirror.entries.insert(path, entry);
                        }
                        if !more {
                            return Ok(Ok(()));
                        }
                    }
                    Response::Error(e) => return Ok(Err(e.into())),
                    other => return Err(unexpected(&other)),
                }
            }
        })?;
        let mut out: Vec<(RelPath, Entry)> = mirror
            .entries
            .iter()
            .filter(|(_, e)| e.seq > seq)
            .map(|(p, e)| (p.clone(), e.clone()))
            .collect();
        out.sort_by_key(|(_, e)| e.seq);
        Ok(out)
    }

    fn open_read(&self, path: &RelPath, expect: &Entry) -> Result<Box<dyn ContentReader>> {
        let mut conn = self.link.take()?;
        let req = Request::OpenRead {
            path: path.clone(),
            expect: expect.clone(),
        };
        let answer = write_frame(&mut conn.tls, &req).and_then(|()| recv(&mut conn.tls));
        match answer {
            Ok(Response::Reading) => {
                let size = match expect.kind {
                    Kind::File { size, .. } => size,
                    _ => u64::MAX,
                };
                Ok(Box::new(RemoteReader {
                    link: self.link.clone(),
                    session: conn.session,
                    drain: size <= DRAIN_LIMIT,
                    stream: Some(ContentStream::new(conn.tls, content_only as Filter)),
                }))
            }
            Ok(Response::Error(e)) => {
                self.link.put(conn);
                Err(e.into())
            }
            Ok(other) => Err(self.link.lost(unexpected(&other))),
            Err(e) => Err(self.link.lost(e)),
        }
    }

    fn apply(
        &mut self,
        path: &RelPath,
        op: Op,
        pre: Precondition,
        content: Option<&mut dyn Read>,
    ) -> Result<Outcome> {
        let req = Request::Apply {
            path: path.clone(),
            op,
            pre,
            content: content.is_some(),
        };
        self.call(|conn| {
            write_frame(&mut conn.tls, &req)?;
            let mut source = Ok(());
            if let Some(src) = content {
                source = send_content(&mut conn.tls, src, Request::Content)?.map(|_| ());
            }
            let answer = match recv(&mut conn.tls)? {
                Response::Applied(outcome) => Ok(outcome),
                Response::Error(e) => Err(Error::from(e)),
                other => return Err(unexpected(&other)),
            };
            // A source that failed mid-stream (e.g. `Unstable`) is the
            // reason the server failed; report it as it is.
            Ok(match (answer, source) {
                (Err(_), Err(e)) => Err(e),
                (answer, _) => answer,
            })
        })
    }

    fn watch(&mut self) -> Option<Receiver<Hint>> {
        match &self.watching {
            Some(Watching::Yes { hints, .. }) => return Some(hints.clone()),
            Some(Watching::No) => return None,
            None => {}
        }
        let first = match self.link.watch() {
            Ok(Some(tls)) => Some(tls),
            Ok(None) => {
                tracing::warn!(addr = %self.link.addr, "the server cannot watch its replica; relying on periodic rescans");
                self.watching = Some(Watching::No);
                return None;
            }
            Err(e) => {
                // The thread keeps trying, and asks for a full rescan once
                // it is through.
                tracing::warn!(error = %e, "cannot watch the remote replica yet; retrying");
                None
            }
        };
        let (tx, hints) = crossbeam_channel::unbounded();
        let (stop, stopped) = crossbeam_channel::bounded(0);
        let socket = Arc::new(Mutex::new(None));
        let thread = HintThread {
            link: self.link.clone(),
            tx,
            stopped,
            socket: socket.clone(),
        };
        let spawned = std::thread::Builder::new()
            .name(format!("hints-{:.7}", self.link.remote.to_string()))
            .spawn(move || thread.run(first));
        if let Err(e) = spawned {
            tracing::warn!(error = %e, "cannot start the hint thread; relying on periodic rescans");
            self.watching = Some(Watching::No);
            return None;
        }
        self.watching = Some(Watching::Yes {
            hints: hints.clone(),
            _stop: stop,
            socket,
        });
        Some(hints)
    }

    fn adopt(&mut self, path: &RelPath) -> Result<bool> {
        self.request(&Request::Adopt { path: path.clone() }, |msg| match msg {
            Response::Adopted(adopted) => Some(adopted),
            _ => None,
        })
    }

    fn record_sync(
        &mut self,
        peer: ReplicaId,
        tombstones: Vec<(RelPath, VersionVector, PeerState)>,
        retention: Duration,
    ) -> Result<Vec<RelPath>> {
        let mut guard = lock(&self.mirror);
        let mirror = &mut *guard;
        let removed = self.call(|conn| {
            let parts = batches(tombstones, BATCH_BYTES);
            let last = parts.len() - 1;
            for (i, tombstones) in parts.into_iter().enumerate() {
                let req = Request::RecordSync {
                    peer,
                    tombstones,
                    retention,
                    more: i < last,
                };
                write_frame(&mut conn.tls, &req)?;
            }
            let mut removed = Vec::new();
            loop {
                match recv(&mut conn.tls)? {
                    Response::Collected { paths, more } => {
                        removed.extend(paths);
                        if !more {
                            break;
                        }
                    }
                    Response::Error(e) => return Ok(Err(e.into())),
                    other => return Err(unexpected(&other)),
                }
            }
            if conn.session == mirror.session {
                for path in &removed {
                    mirror.entries.remove(path);
                }
            }
            Ok(Ok(removed))
        })?;
        Ok(removed)
    }
}

impl Drop for RemoteReplica {
    fn drop(&mut self) {
        // A clean close, so the server does not see a torn connection.
        if let Some(mut conn) = lock(&self.link.idle).take() {
            conn.tls.conn.send_close_notify();
            let _ = conn.tls.flush();
        }
        if let Some(Watching::Yes { socket, .. }) = &self.watching
            && let Some(s) = lock(socket).as_ref()
        {
            let _ = s.shutdown(Shutdown::Both);
        }
        // `_stop` is dropped after this, which ends the thread's wait.
    }
}

/// Streams a file from the server; gives the connection back when done.
struct RemoteReader {
    link: Arc<Link>,
    session: u64,
    /// Drain the rest if dropped early (the file is small).
    drain: bool,
    stream: Option<ContentStream<Tls, Response, Filter>>,
}

impl Read for RemoteReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.stream
            .as_mut()
            .expect("present until dropped")
            .read(buf)
    }
}

impl Drop for RemoteReader {
    fn drop(&mut self) {
        let Some(mut stream) = self.stream.take() else {
            return;
        };
        if stream.is_finished() || (self.drain && stream.drain().is_ok()) {
            self.link.put(Conn {
                tls: stream.into_inner(),
                session: self.session,
            });
        }
    }
}

/// Forwards pushed hints; reconnects when the hint connection is lost.
struct HintThread {
    link: Arc<Link>,
    tx: Sender<Hint>,
    /// Disconnected when the replica is dropped.
    stopped: Receiver<()>,
    socket: Arc<Mutex<Option<TcpStream>>>,
}

impl HintThread {
    fn run(self, first: Option<Tls>) {
        let mut tls = first;
        let mut delay = RECONNECT_MIN;
        loop {
            if let Some(t) = tls.take() {
                if !self.forward(t) {
                    return;
                }
                tracing::warn!(addr = %self.link.addr, "hint connection lost; reconnecting");
            }
            match self.stopped.recv_timeout(delay) {
                Err(RecvTimeoutError::Timeout) => {}
                _ => return,
            }
            match self.link.watch() {
                Ok(Some(t)) => {
                    tracing::info!(addr = %self.link.addr, "watching the remote replica");
                    delay = RECONNECT_MIN;
                    tls = Some(t);
                    // Whatever changed while we were away.
                    if self.tx.send(Hint::FullRescan).is_err() {
                        return;
                    }
                }
                Ok(None) => {
                    tracing::warn!(addr = %self.link.addr, "the server stopped watching; relying on periodic rescans");
                    return;
                }
                Err(e) => {
                    tracing::debug!(error = %e, "cannot reconnect the hint connection");
                    delay = (delay * 2).min(RECONNECT_MAX);
                }
            }
        }
    }

    /// Forwards hints until the connection ends. Returns whether to go on
    /// (false: the replica is gone).
    fn forward(&self, mut tls: Tls) -> bool {
        match tls.sock.try_clone() {
            Ok(s) => *lock(&self.socket) = Some(s),
            Err(e) => tracing::debug!(error = %e, "cannot clone the hint socket"),
        }
        // Registered before this check, so a drop either sees the socket or
        // has already disconnected `stopped`.
        if matches!(
            self.stopped.try_recv(),
            Err(crossbeam_channel::TryRecvError::Disconnected)
        ) {
            return false;
        }
        let go_on = loop {
            match read_frame::<_, Response>(&mut tls) {
                Ok(Some(Response::Hint(hint))) => {
                    if self.tx.send(hint).is_err() {
                        break false;
                    }
                }
                Ok(Some(other)) => {
                    tracing::warn!(error = %unexpected(&other), "on the hint connection");
                    break true;
                }
                Ok(None) => break true,
                Err(e) => {
                    tracing::debug!(error = %e, "hint connection");
                    break true;
                }
            }
        };
        *lock(&self.socket) = None;
        go_on
    }
}
