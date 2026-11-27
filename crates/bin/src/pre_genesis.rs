use std::{
    io::{self, Read, Write},
    net::TcpListener,
    os::unix::net::UnixListener,
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use silver_common::{AGENT_VERSION, Enr, Eth2Addr, Keypair};
use silver_httpcore::Bind;

const POLL: Duration = Duration::from_millis(20);
const READ_TIMEOUT: Duration = Duration::from_secs(1);
const MAX_REQUEST: usize = 8192;

enum PreGenesisListener {
    Tcp(TcpListener),
    Unix(UnixListener),
}

pub struct PreGenesisApi {
    identity: Vec<u8>,
    version: Vec<u8>,
    syncing: Vec<u8>,
}

impl PreGenesisApi {
    pub fn new(keypair: &Keypair, local_enr: &Enr) -> Self {
        let multiaddr = Eth2Addr::PeerId(keypair.peer_id()).to_string();
        let peer_id = multiaddr.strip_prefix("/p2p/").unwrap_or(&multiaddr);
        let identity = format!(
            "{{\"data\":{{\"peer_id\":\"{peer_id}\",\"enr\":\"{}\",\"p2p_addresses\":[],\"discovery_addresses\":[],\"metadata\":{{\"seq_number\":\"0\",\"attnets\":\"0x0000000000000000\",\"syncnets\":\"0x00\"}}}}}}",
            local_enr.to_base64()
        );
        let version = format!("{{\"data\":{{\"version\":\"{AGENT_VERSION}\"}}}}");
        let syncing = "{\"data\":{\"head_slot\":\"0\",\"sync_distance\":\"0\",\"is_syncing\":false,\"is_optimistic\":false,\"el_offline\":false}}".to_owned();
        Self {
            identity: identity.into_bytes(),
            version: version.into_bytes(),
            syncing: syncing.into_bytes(),
        }
    }

    /// Answers the node endpoints on the beacon API binds until genesis, then
    /// releases them for the real API.
    pub fn serve_until(&self, binds: &[Bind], genesis_unix_secs: u64) {
        let genesis = UNIX_EPOCH + Duration::from_secs(genesis_unix_secs);
        let Ok(remaining) = genesis.duration_since(SystemTime::now()) else {
            return;
        };
        silver_log::info!(
            "waiting {}s for genesis at {genesis_unix_secs}, serving the node endpoints meanwhile",
            remaining.as_secs()
        );

        let listeners: Vec<_> = binds
            .iter()
            .filter_map(|bind| match bind_listener(bind) {
                Ok(listener) => Some(listener),
                Err(error) => {
                    silver_log::warn!(?bind, %error, "pre-genesis API could not bind");
                    None
                }
            })
            .collect();

        while SystemTime::now() < genesis {
            let mut idle = true;
            for listener in &listeners {
                let accepted = match listener {
                    PreGenesisListener::Tcp(l) => l.accept().map(|(s, _)| self.answer(s)),
                    PreGenesisListener::Unix(l) => l.accept().map(|(s, _)| self.answer(s)),
                };
                match accepted {
                    Ok(()) => idle = false,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                    Err(error) => silver_log::warn!(%error, "pre-genesis API accept failed"),
                }
            }
            if idle {
                thread::sleep(
                    POLL.min(genesis.duration_since(SystemTime::now()).unwrap_or_default()),
                );
            }
        }
    }

    fn answer<S: Read + Write + Timeouts>(&self, mut stream: S) {
        let _ = stream.set_timeouts(READ_TIMEOUT);
        let mut request = Vec::with_capacity(512);
        let mut buf = [0u8; 1024];
        while !request.windows(4).any(|w| w == b"\r\n\r\n") && request.len() < MAX_REQUEST {
            match stream.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => request.extend_from_slice(&buf[..n]),
            }
        }
        let (status, body) = self.route(&request);
        let head = format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        let _ = stream.write_all(head.as_bytes()).and_then(|()| stream.write_all(body));
    }

    fn route(&self, request: &[u8]) -> (&'static str, &[u8]) {
        let line = request.split(|&b| b == b'\r').next().unwrap_or_default();
        let mut parts = line.split(|&b| b == b' ');
        let (method, target) = (parts.next().unwrap_or_default(), parts.next().unwrap_or_default());
        let path = target.split(|&b| b == b'?').next().unwrap_or_default();
        match (method, path) {
            (b"GET", b"/eth/v1/node/identity") => ("200 OK", &self.identity),
            (b"GET", b"/eth/v1/node/version") => ("200 OK", &self.version),
            (b"GET", b"/eth/v1/node/syncing") => ("200 OK", &self.syncing),
            (b"GET", b"/eth/v1/node/health") => ("200 OK", b""),
            _ => ("503 Service Unavailable", b"{\"code\":503,\"message\":\"waiting for genesis\"}"),
        }
    }
}

fn bind_listener(bind: &Bind) -> io::Result<PreGenesisListener> {
    match bind {
        Bind::Tcp(addr) => {
            let listener = TcpListener::bind(addr)?;
            listener.set_nonblocking(true)?;
            Ok(PreGenesisListener::Tcp(listener))
        }
        Bind::Unix(path) => {
            let _ = std::fs::remove_file(path);
            let listener = UnixListener::bind(path)?;
            listener.set_nonblocking(true)?;
            Ok(PreGenesisListener::Unix(listener))
        }
    }
}

trait Timeouts {
    fn set_timeouts(&mut self, timeout: Duration) -> io::Result<()>;
}

impl Timeouts for std::net::TcpStream {
    fn set_timeouts(&mut self, timeout: Duration) -> io::Result<()> {
        self.set_nonblocking(false)?;
        self.set_read_timeout(Some(timeout))?;
        self.set_write_timeout(Some(timeout))
    }
}

impl Timeouts for std::os::unix::net::UnixStream {
    fn set_timeouts(&mut self, timeout: Duration) -> io::Result<()> {
        self.set_nonblocking(false)?;
        self.set_read_timeout(Some(timeout))?;
        self.set_write_timeout(Some(timeout))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn api() -> PreGenesisApi {
        PreGenesisApi {
            identity: b"{\"data\":{\"peer_id\":\"p\"}}".to_vec(),
            version: b"{\"data\":{\"version\":\"v\"}}".to_vec(),
            syncing: b"{\"data\":{}}".to_vec(),
        }
    }

    #[test]
    fn answers_node_endpoints_and_refuses_the_rest() {
        let api = api();
        assert_eq!(api.route(b"GET /eth/v1/node/identity HTTP/1.1\r\n\r\n").0, "200 OK");
        assert_eq!(
            api.route(b"GET /eth/v1/node/version HTTP/1.1\r\n\r\n").1,
            api.version.as_slice()
        );
        assert_eq!(api.route(b"GET /eth/v1/node/syncing?x=1 HTTP/1.1\r\n\r\n").0, "200 OK");
        assert_eq!(api.route(b"GET /eth/v1/node/health HTTP/1.1\r\n\r\n").0, "200 OK");
        assert_eq!(
            api.route(b"GET /eth/v1/beacon/genesis HTTP/1.1\r\n\r\n").0,
            "503 Service Unavailable"
        );
        assert_eq!(
            api.route(b"POST /eth/v1/node/identity HTTP/1.1\r\n\r\n").0,
            "503 Service Unavailable"
        );
    }

    #[test]
    fn serves_over_tcp_until_genesis_then_releases_the_port() {
        let probe = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let genesis = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() + 2;
        let server = thread::spawn(move || api().serve_until(&[Bind::Tcp(addr)], genesis));
        thread::sleep(Duration::from_millis(200));
        let mut stream = std::net::TcpStream::connect(addr).unwrap();
        stream.write_all(b"GET /eth/v1/node/identity HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();
        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        assert!(response.ends_with("{\"data\":{\"peer_id\":\"p\"}}"), "{response}");
        server.join().unwrap();
        TcpListener::bind(addr).expect("port released at genesis");
    }
}
