extern crate env_logger;
extern crate ipnetwork;
extern crate log;
extern crate pnet;
#[cfg(feature = "debug")]
extern crate vigil;
use bitvec::vec::BitVec;
use core::time::Duration;
use futures::executor::block_on;
use log::*;
use pnet::datalink;
use std::fmt::Debug;
use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::{thread::sleep, time::SystemTime};
mod multimap;
mod queue;
mod topology;
use multimap::*;
#[cfg(not(feature = "debug"))]
use std::sync::Mutex;
#[cfg(feature = "debug")]
use tracing_mutex::stdsync::TracingMutex as Mutex;

use self::queue::TaskQueue;
pub type Rank = u16;
pub type Tag = i64;

pub struct HandleData {
    tag: Tag,
    // The rank hosting this handle
    rank: Rank,
    // The list of rank who have a valid copy of this handle
    nodes: BitVec,
}

impl HandleData {
    pub fn new(tag: Tag, rank: Rank, world_size: usize) -> Self {
        let mut nodes = bitvec::bitvec![0; world_size];
        nodes.set(rank.into(), true);
        Self { tag, rank, nodes }
    }

    pub fn tag(&self) -> Tag {
        self.tag
    }

    pub fn rank(&self) -> Rank {
        self.rank
    }

    /// Return true if the handle data is valid on the given node
    pub fn is_on(&self, rank: Rank) -> bool {
        self.nodes[rank as usize]
    }

    pub fn set_on(&mut self, rank: Rank) {
        self.nodes.set(rank.into(), true);
    }

    pub fn set_on_only(&mut self, rank: Rank) {
        self.nodes.fill(false);
        self.set_on(rank);
    }

    pub fn cur_rank(&self) -> Rank {
        if self.is_on(self.rank) {
            self.rank
        } else {
            self.nodes.first_one().unwrap() as Rank
        }
    }
}

// conception:
// HandleManager <- one to rule them all
// Registry <--- current HandleRegistry but opaque
// ClusterConfig
// Cluster <-- the ib0 TCPListener + remote socketaddr + all remote TCP stream
// InitServer <-- rank0 server + remote socketaddr

pub trait SendReq: Send + Sync + Debug {
    type Buf: AsRef<[u8]>;
    fn data(&self) -> Self::Buf;
    fn tag(&self) -> Tag;
    fn clear(&self);
    /// Approximate size of this handle (only used for statistics)
    fn size(&self) -> usize;
    fn priority(&self) -> isize;
}

pub trait RecvReq: Send + Sync + Debug {
    type Buf: AsMut<[u8]>;
    fn tag(&self) -> Tag;
    fn create_buf(&self, size: usize) -> Self::Buf;
    fn finish(&self, buf: Self::Buf);
}

pub struct HandleManager<S: SendReq + Send + Sync, R: RecvReq>(ManagerImpl<S, R>);

impl<S: 'static + SendReq + Send + Sync, R: 'static + RecvReq + Send> HandleManager<S, R> {
    pub fn new() -> Self {
        env_logger::init();
        Self(ManagerImpl {
            cluster: Cluster::new(),
        })
    }

    pub fn start(&'static mut self) {
        self.0.cluster.start();
    }

    pub fn send(&'static self, rank: Rank, req: S) {
        self.0.cluster.send(rank, req)
    }

    pub fn recv(&'static self, rank: Rank, req: R) {
        self.0.cluster.recv(rank, req);
    }

    pub fn rank(&self) -> Rank {
        self.0.cluster.config.rank()
    }

    pub fn world_size(&self) -> Rank {
        self.0.cluster.config.world_size()
    }

    pub fn is_done(&self) -> bool {
        let r = self.0.cluster.is_done();
        trace!("Rank {} is_done: {}", self.0.cluster.config.rank(), r);
        r
    }
    pub fn shutdown(&self) {
        self.0.cluster.shutdown();
    }
}

struct ManagerImpl<S: SendReq + Send + Sync, R: RecvReq> {
    cluster: Cluster<S, R>,
}

/// Get the local address (IP with port 0) for a given network CIDR or interface name
fn bindSocketAddr(es: &str) -> SocketAddr {
    let all = "0.0.0.0:0".parse().unwrap();
    let ipr = es.parse::<ipnetwork::IpNetwork>();
    let iprr = ipr.as_ref();
    let ipok = ipr.is_ok();
    for inter in datalink::interfaces() {
        if ipok {
            for ip in &inter.ips {
                if iprr.unwrap().contains(ip.ip()) {
                    return SocketAddr::new(ip.ip(), 0);
                }
            }
        }
        if inter.name == es {
            return SocketAddr::new(inter.ips[0].ip(), 0);
        }
    }
    all
}

trait Serializable {
    fn read<R: std::io::Read>(&mut self, reader: &mut R) -> std::io::Result<()>;
    fn write<W: Write>(&self, writer: &mut W) -> std::io::Result<()>;
}

impl Serializable for SocketAddr {
    fn read<R: std::io::Read>(&mut self, reader: &mut R) -> std::io::Result<()> {
        let mut typ: u8 = 0;
        reader.read_exact(std::slice::from_mut(&mut typ))?;
        match typ {
            4 => {
                let mut buf: [u8; 4] = [0; 4];
                reader.read_exact(&mut buf)?;
                self.set_ip(IpAddr::V4(Ipv4Addr::from(buf)));
                let mut buf: [u8; 2] = [0; 2];
                reader.read_exact(&mut buf)?;
                self.set_port(u16::from_ne_bytes(buf));
            }
            6 => {
                todo!()
            }
            _ => {
                panic!("Protocol error: {}", typ);
            }
        }
        Ok(())
    }

    fn write<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        match self {
            SocketAddr::V4(a) => {
                writer.write_all(std::slice::from_ref(&4))?;
                writer.write_all(&a.ip().octets())?;
                writer.write_all(&a.port().to_ne_bytes())?;
            }
            SocketAddr::V6(_) => {
                todo!()
            }
        }
        Ok(())
    }
}

impl Serializable for [SocketAddr] {
    fn read<R: std::io::Read>(&mut self, reader: &mut R) -> std::io::Result<()> {
        let mut world_size: Rank = 0;
        world_size.read(reader)?;
        assert_eq!(world_size, self.len() as Rank);
        for addr in self {
            addr.read(reader)?;
        }
        Ok(())
    }

    fn write<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        let world_size: Rank = self.len() as Rank;
        world_size.write(writer)?;
        for addr in self {
            addr.write(writer)?;
        }
        Ok(())
    }
}

impl Serializable for Rank {
    fn read<R: std::io::Read>(&mut self, reader: &mut R) -> std::io::Result<()> {
        let mut buf: [u8; 2] = [0; 2];
        reader.read_exact(&mut buf)?;
        *self = Rank::from_ne_bytes(buf);
        Ok(())
    }
    fn write<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_all(&self.to_ne_bytes())
    }
}

// TODO make port configurable
// TODO do not use 0.0.0.0 as bind address if user specify a bind address
const RANK0_INIT_SRV: &str = "0.0.0.0:25201";
const RANK0_INIT_PORT: &str = "25201";

struct InitServer {
    listener: Option<TcpListener>,
    streams: Vec<TcpStream>,
}

impl InitServer {
    fn new(rank: Rank) -> Self {
        if rank == 0 {
            InitServer {
                listener: Some(TcpListener::bind(RANK0_INIT_SRV).unwrap()),
                streams: Vec::new(),
            }
        } else {
            InitServer {
                listener: None,
                streams: Vec::new(),
            }
        }
    }

    /// Send all bind address to all nodes
    fn send(&mut self, topo: &Vec<SocketAddr>) {
        for mut stream in &self.streams {
            topo.write(&mut stream)
                .expect("Error sending cluster addresses to a node");
        }
    }

    /// Receive bind address from each node
    async fn recv(&mut self, topo: &mut [SocketAddr]) {
        if self.listener.is_none() {
            return;
        }
        let mut toread = topo.len();
        let mut tmp_streams = Vec::new();
        tmp_streams.resize_with(toread, || None);

        // add a timeout to detect if some nodes are missing
        for streamr in self.listener.as_ref().unwrap().incoming() {
            match streamr {
                Ok(mut stream) => {
                    let mut rank: Rank = 0;
                    match rank.read(&mut stream) {
                        Ok(_) => {}
                        Err(e) => {
                            warn!("Protocol error while reading rank: {}", e);
                            continue;
                        }
                    };
                    if rank >= topo.len() as u16 {
                        warn!("Protocol error: invalid rank: {}", rank);
                    }
                    match topo[rank as usize].read(&mut stream) {
                        Ok(_) => {
                            toread -= 1;
                        }
                        Err(e) => {
                            warn!("Protocol error while reading IP: {}", e);
                        }
                    }
                    tmp_streams[rank as usize] = Some(stream);
                    if toread == 0 {
                        break;
                    }
                }
                Err(err) => {
                    warn!("TCP error: {}", err);
                }
            }
        }
        self.streams = tmp_streams.into_iter().flatten().collect::<Vec<_>>();
    }
}

#[repr(u8)]
#[derive(Copy, Clone, Debug)]
enum MsgType {
    Data,
    Ready,
}

#[derive(Debug)]
struct MsgHeader {
    t: MsgType,
    tag: Tag,
}

impl MsgHeader {
    fn new() -> Self {
        MsgHeader {
            t: MsgType::Data,
            tag: 0,
        }
    }
}
impl Serializable for MsgHeader {
    fn read<R: std::io::Read>(&mut self, reader: &mut R) -> std::io::Result<()> {
        let mut buf: [u8; 1] = [0; 1];
        reader.read_exact(&mut buf)?;
        self.t = match buf[0] {
            0 => MsgType::Data,
            1 => MsgType::Ready,
            t => panic!("Invalid message type: {}", t),
        };
        let mut buf: [u8; 8] = [0; 8];
        reader.read_exact(&mut buf)?;
        self.tag = Tag::from_ne_bytes(buf);
        Ok(())
    }

    fn write<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        let t: u8 = match self.t {
            MsgType::Data => 0,
            MsgType::Ready => 1,
        };
        writer.write_all(&[t])?;
        writer.write_all(&self.tag.to_ne_bytes())?;
        writer.flush()?;
        trace!("{:?} written", self);
        Ok(())
    }
}

struct Peer<S: SendReq, R: RecvReq> {
    rank: Rank,
    addr: SocketAddr,
    /// Stream for writting
    stream: Mutex<Option<TcpStream>>,
    connected: Mutex<bool>,
    /// Pending receive requests (connection established)
    recv_reqs: MultiMap<Tag, R>,
    /// Pending send requests (connection established)
    send_reqs: UnkValMap<Tag, S>,
    /// Send requests waiting for connection
    pending_send: Mutex<Vec<S>>,
    /// Receive requests waiting for connection
    pending_recv: Mutex<Vec<R>>,
    #[cfg(feature = "debug")]
    watchdog: Mutex<Option<vigil::Vigil>>,
    write_tasks: queue::TaskQueue,
    log_data_size: usize,
}

impl<S: SendReq, R: RecvReq> Debug for Peer<S, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        f.debug_struct("Peer")
            .field("rank", &self.rank)
            .field("addr", &self.addr)
            .field("recv_reqs", &self.recv_reqs)
            .field("send_reqs", &self.send_reqs)
            //.field("pending_send", &self.pending_send)
            //.field("pending_recv", &self.pending_recv)
            .finish()
    }
}

fn zeroSocketAddr() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0)
}

fn parse_env_var<K: AsRef<std::ffi::OsStr>, T: std::str::FromStr>(key: K, default_val: T) -> T {
    let k = key.as_ref().to_str().unwrap().to_string();
    match std::env::var(key) {
        Ok(s) => match s.parse::<T>() {
            Ok(v) => v,
            Err(_) => panic!("{} is not valid for {}", s, k),
        },
        Err(_) => default_val,
    }
}

impl<S: SendReq + 'static, R: RecvReq + 'static> Peer<S, R> {
    /// return true if all comms/request have been processed
    fn is_done(&self) -> bool {
        let ps = self.pending_send.lock().unwrap();
        let pr = self.pending_recv.lock().unwrap();
        self.recv_reqs.is_empty() && self.send_reqs.is_empty() && ps.is_empty() && pr.is_empty()
    }

    fn new(rank: Rank, write_tasks: TaskQueue, logging_threshold: usize, me: Rank) -> Self {
        let mib = (1 << 20) as f32;
        let el: f32 = parse_env_var("STARPU_TCP_LOG_DATA_SIZE", usize::MAX as f32 / mib);
        let mut recv_reqs = MultiMap::new();
        recv_reqs.log(format!("[{}<={}] Recv req", me, rank), logging_threshold);
        let mut send_reqs = UnkValMap::new();
        send_reqs.log(format!("[{}=>{}] Send req", me, rank), logging_threshold);
        Peer {
            rank,
            addr: zeroSocketAddr(),
            stream: Mutex::new(None),
            connected: Mutex::new(false),
            recv_reqs,
            send_reqs,
            pending_send: Mutex::new(Vec::new()),
            pending_recv: Mutex::new(Vec::new()),
            #[cfg(feature = "debug")]
            watchdog: Mutex::new(None),
            write_tasks,
            log_data_size: (el * mib) as usize,
        }
    }

    #[cfg(feature = "debug")]
    fn watchdog_ping(&'static self) {
        let mut w = self.watchdog.lock().unwrap();
        match &*w {
            None => {
                let me = Box::new(self);
                let b = Box::new(move || debug!("{:?}", me));
                let v = vigil::Vigil::create(500, Some(b), None, None).0;
                v.notify();
                *w = Some(v);
            }
            Some(v) => {
                v.notify();
            }
        }
    }
    #[cfg(not(feature = "debug"))]
    fn watchdog_ping(&'static self) {}

    /// Return true if this peer is responsible for initiating the TCP connection or if the
    /// connection is already established.
    fn owner_or_connected(&'static self, me: Rank, world_size: Rank) -> bool {
        let mut e = self.connected.lock().unwrap();
        if *e {
            return true;
        }
        // Number of connection owned by the peer (i.e the connection that it will initiate)
        let mut nc = (world_size - 1) / 2;
        if world_size % 2 == 0 && me < world_size / 2 {
            nc += 1;
        }
        if nc == 0 {
            return false;
        }
        assert!(self.rank != me);
        let minb = (me + 1) % world_size;
        let maxb = (me + nc) % world_size;
        let in_interval = minb <= maxb && self.rank >= minb && self.rank <= maxb;
        // After worldsize-1 we loop back to 0 so check the rank is on each side of
        // the interval
        let in_looped = minb > maxb && (self.rank >= minb || self.rank <= maxb);
        if in_interval || in_looped {
            let socket = connect_retry(&self.addr);
            info!(
                "Node {} connect to {} because it's the owner of the connection: {:?}",
                me, self.rank, socket
            );
            *e = true;
            let mut ls = self.stream.lock().unwrap();
            *ls = Some(socket.try_clone().unwrap());
            drop(e);
            me.write((*ls).as_mut().unwrap()).unwrap();
            self.start_read(socket);
            return true;
        }
        false
    }

    fn set_write_stream(&'static self, s: &TcpStream) {
        {
            let mut lc = self.connected.lock().unwrap();
            *lc = true;
            let mut o = self.stream.lock().unwrap();
            assert!(o.is_none());
            *o = Some(s.try_clone().unwrap());
        }
        // unstack pending requests
        while let Some(r) = self.pending_send.lock().unwrap().pop() {
            self.send(r);
        }
        while let Some(r) = self.pending_recv.lock().unwrap().pop() {
            self.recv(r);
        }
    }

    fn start_read(&'static self, s: TcpStream) {
        let thread_name = format!("Peer {}", self.rank);
        std::thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                self.read(s);
            })
            .unwrap();
    }

    fn read(&'static self, mut s: TcpStream) {
        let mut header = MsgHeader::new();
        loop {
            if let Err(e) = header.read(&mut s) {
                info!("Connection closed by peer: {}", e);
                break;
            }
            self.watchdog_ping();
            trace!("{:?} from {}", header, self.rank);
            match header {
                MsgHeader {
                    t: MsgType::Data,
                    tag,
                } => {
                    self.process_data_msg(&mut s, tag);
                }
                MsgHeader {
                    t: MsgType::Ready,
                    tag,
                } => {
                    if let Some(r) = self.send_reqs.pop_or_insert_nv(tag) {
                        self.push_send_req(r)
                    }
                }
            }
        }
    }
    fn process_data_msg(&self, p: &mut TcpStream, tag: Tag) {
        trace!("Receiving {} from {}", tag, self.rank);
        let r = self.recv_reqs.pop(tag).unwrap();
        let mut buf: [u8; 8] = [0; 8];
        p.read_exact(&mut buf).unwrap();
        let size = usize::from_ne_bytes(buf);
        let mut buf = r.create_buf(size);
        if size > self.log_data_size {
            let start = SystemTime::now();
            p.read_exact(buf.as_mut()).unwrap();
            let e = start.elapsed().unwrap();
            let smib = size as f32 / (1 << 20) as f32;
            info!("Read {}MiB at {}MiB/s", smib, smib / e.as_secs_f32());
        } else {
            p.read_exact(buf.as_mut()).unwrap();
        }
        r.finish(buf);
    }

    fn push_send_req(&'static self, req: S) {
        let size = req.size();
        let prio = req.priority();
        if size < 16 {
            self.process_send_req(req);
        } else {
            self.write_tasks
                .push(move || self.process_send_req(req), size, prio);
        }
    }

    fn process_send_req(&self, req: S) {
        trace!("Sending {} to {}", req.tag(), self.rank);
        {
            let mut l = self.stream.lock().unwrap();
            let p = (*l).as_mut().unwrap();
            let buf = req.data();
            let s = buf.as_ref();
            MsgHeader {
                t: MsgType::Data,
                tag: req.tag(),
            }
            .write(p)
            .unwrap();
            p.write_all(&s.len().to_ne_bytes()).unwrap();
            if s.len() > self.log_data_size {
                let start = SystemTime::now();
                p.write_all(s).unwrap();
                let e = start.elapsed().unwrap();
                let smib = s.len() as f32 / (1 << 20) as f32;
                info!("Wrote {}MiB at {}MiB/s", smib, smib / e.as_secs_f32());
            } else {
                p.write_all(s).unwrap();
            }
        }
        req.clear();
    }

    fn send(&'static self, req: S) {
        self.watchdog_ping();
        if let Some(req) = self.send_reqs.pop_nv_or_insert(req.tag(), req) {
            // The receiver is already waiting so process right now
            self.push_send_req(req)
        } // else put it in pending request list
    }

    fn send_ready(&self, tag: Tag) {
        let msg = MsgHeader {
            t: MsgType::Ready,
            tag,
        };
        let mut l = self.stream.lock().unwrap();
        let p = (*l).as_mut().unwrap();
        msg.write(p).unwrap();
    }

    fn recv(&'static self, req: R) {
        self.watchdog_ping();
        let tag = req.tag();
        self.recv_reqs.insert(tag, req);
        // TODO: do we need this for fast task insertion ?
        /*self.write_tasks.push(
            move || self.send_ready(tag),
            std::mem::size_of::<MsgHeader>(),
        );*/
        self.send_ready(tag);
    }
}

struct Cluster<S: SendReq, R: RecvReq> {
    config: topology::Config,
    peers: Vec<Peer<S, R>>,
    listener: Option<TcpListener>,
}

fn connect_retry(addr: &SocketAddr) -> TcpStream {
    let start = SystemTime::now();
    loop {
        let r = TcpStream::connect(addr);
        match r {
            Ok(stream) => return stream,
            Err(e) if e.kind() == std::io::ErrorKind::ConnectionRefused => {
                if SystemTime::now() > start + Duration::from_secs(5) {
                    panic!("Cannot connect to {}", addr)
                }
                sleep(Duration::from_secs_f32(0.1));
                continue;
            }
            Err(e) => panic!("Cannot connect to {}: {}", addr, e),
        }
    }
}

impl<S: SendReq + 'static, R: RecvReq + 'static> Cluster<S, R> {
    fn new() -> Self {
        let cn = topology::Config::create();
        let ws: usize = cn.world_size() as usize;
        let mut peers = Vec::with_capacity(ws);
        let lms: usize = parse_env_var("STARPU_TCP_LOG_MAP_SIZE", usize::MAX);
        let q = TaskQueue::with_logging(lms, format!("[{}] reqs to write: ", cn.rank()));
        for rank in 0..ws {
            peers.push(Peer::new(rank as Rank, q.clone(), lms, cn.rank()));
        }
        assert!(ws >= 1);
        Cluster {
            config: cn,
            peers,
            listener: None,
        }
    }

    fn start(&'static mut self) {
        let listener = TcpListener::bind(bindSocketAddr(&self.config.bindAddr())).unwrap();
        let laddr = listener.local_addr().unwrap();
        let mut is = InitServer::new(self.config.rank());
        info!("Rank {} listening on {:?}", self.config.rank(), laddr);
        let mut peerAddrs = vec![zeroSocketAddr(); self.config.world_size() as usize];
        let f = is.recv(&mut peerAddrs);
        let socketStr = self.config.rank0() + ":" + RANK0_INIT_PORT;
        let mut sAddr = socketStr
            .to_socket_addrs()
            .unwrap_or_else(|_| panic!("{:?}", socketStr));
        // Rank 0 is now ready to receive each node address, so send them
        let rank0 = sAddr.next().unwrap();
        let mut stream = connect_retry(&rank0);
        self.config.rank().write(&mut stream).unwrap();
        laddr.write(&mut stream).unwrap();
        block_on(f);
        // FIXME: how to avoid this clone ?
        let nodeAddr0 = peerAddrs.clone();
        // At this point only the rank 0 have the whole list of node address.
        // It now send them to each node
        let f2 = async {
            // And each node receive it
            peerAddrs
                .read(&mut stream)
                .expect("Cannot receive nodes addresses");
        };
        is.send(&nodeAddr0);
        block_on(f2);
        for (i, p) in self.peers.iter_mut().enumerate() {
            p.addr = peerAddrs[i];
        }
        std::thread::spawn(move || {
            self.server_loop(listener);
        });
    }

    fn server_loop(&'static mut self, listener: TcpListener) {
        self.listener = Some(listener);
        for sr in self.listener.as_ref().unwrap().incoming() {
            match sr {
                Ok(mut socket) => {
                    trace!(
                        "Node {} received connection: {:?}",
                        self.config.rank(),
                        socket
                    );
                    let mut buf: [u8; 2] = [0; 2];
                    socket.read_exact(&mut buf).unwrap();
                    let rank = Rank::from_ne_bytes(buf);
                    debug!(
                        "Node {} received connection from {}: {:?}",
                        self.config.rank(),
                        rank,
                        socket
                    );
                    let pr = &self.peers[rank as usize];
                    pr.set_write_stream(&socket);
                    pr.start_read(socket);
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    info!("Stopping server");
                    break;
                }
                Err(e) => {
                    warn!("{:?}", e);
                }
            }
        }
    }

    fn get_peer(&self, rank: Rank) -> &Peer<S, R> {
        assert!(rank != self.config.rank());
        assert!(rank < self.config.world_size());
        &self.peers[rank as usize]
    }

    fn send(&'static self, rank: Rank, req: S) {
        let me = self.config.rank();
        trace!("{} wants to send {} to {}", me, req.tag(), rank);
        let pr = self.get_peer(rank);
        if pr.owner_or_connected(me, self.config.world_size()) {
            pr.send(req);
        } else {
            trace!(
                "Sending {} to {} from {} not possible yet",
                req.tag(),
                rank,
                self.config.rank()
            );
            pr.pending_send.lock().unwrap().push(req);
        }
    }

    fn recv(&'static self, rank: Rank, req: R) {
        let me = self.config.rank();
        trace!(
            "{} wants to receive {} from {}",
            self.config.rank(),
            req.tag(),
            rank
        );
        let pr = self.get_peer(rank);
        if pr.owner_or_connected(me, self.config.world_size()) {
            pr.recv(req);
        } else {
            trace!(
                "Receving {} on {} from {} not possible yet",
                req.tag(),
                self.config.rank(),
                rank
            );
            pr.pending_recv.lock().unwrap().push(req);
        }
    }

    fn is_done(&self) -> bool {
        for p in &self.peers {
            if !p.is_done() {
                return false;
            }
        }
        true
    }

    fn shutdown(&self) {
        match &self.listener {
            Some(l) => {
                l.set_nonblocking(true).unwrap();
            }
            None => {}
        }
        if !self.peers.is_empty() {
            self.peers[0].write_tasks.log_stats();
        }
    }
}
