use crate::torrent::Metainfo;
use std::net::{Ipv4Addr, SocketAddrV4};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TrackerError {
    #[error("HTTP request failed: {0}")]
    Http(String),
    #[error("failed to decode tracker response: {0}")]
    Decode(String),
    #[error("tracker returned failure: {0}")]
    Failure(String),
    #[error("invalid compact peer data")]
    InvalidPeers,
}

#[derive(Debug, Clone)]
pub struct TrackerResponse {
    pub interval: u64,
    pub peers: Vec<SocketAddrV4>,
    pub seeders: Option<u64>,
    pub leechers: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
pub struct TrackerClient {
    pub peer_id: [u8; 20],
    pub port: u16,
}

impl TrackerClient {
    pub fn new(port: u16) -> Self {
        let mut peer_id = [0u8; 20];
        // Use -TR0001- prefix (like Transmission) + random bytes
        peer_id[..8].copy_from_slice(b"-TR0001-");
        for b in &mut peer_id[8..] {
            *b = rand::random();
        }
        Self { peer_id, port }
    }

    /// Announce to the tracker specified in metainfo (primary announce URL).
    pub async fn announce(
        &self,
        metainfo: &Metainfo,
        uploaded: u64,
        downloaded: u64,
        left: u64,
        event: Option<&str>,
    ) -> Result<TrackerResponse, TrackerError> {
        self.announce_to_url(
            &metainfo.announce,
            &metainfo.info_hash,
            uploaded,
            downloaded,
            left,
            event,
        )
        .await
    }

    /// Announce to a specific tracker URL (supports both HTTP and UDP).
    pub async fn announce_to_url(
        &self,
        url: &str,
        info_hash: &[u8; 20],
        uploaded: u64,
        downloaded: u64,
        left: u64,
        event: Option<&str>,
    ) -> Result<TrackerResponse, TrackerError> {
        if url.starts_with("udp://") {
            udp_announce(
                url,
                info_hash,
                &self.peer_id,
                self.port,
                uploaded,
                downloaded,
                left,
                event,
            )
            .await
        } else if url.starts_with("http://") || url.starts_with("https://") {
            self.http_announce(url, info_hash, uploaded, downloaded, left, event)
                .await
        } else {
            Err(TrackerError::Http(format!(
                "unsupported tracker protocol: {}",
                url
            )))
        }
    }

    async fn http_announce(
        &self,
        announce_url: &str,
        info_hash: &[u8; 20],
        uploaded: u64,
        downloaded: u64,
        left: u64,
        event: Option<&str>,
    ) -> Result<TrackerResponse, TrackerError> {
        let info_hash_encoded = urlencoding::encode_binary(info_hash);
        let peer_id_encoded = urlencoding::encode_binary(&self.peer_id);

        let mut url = format!(
            "{}?info_hash={}&peer_id={}&port={}&uploaded={}&downloaded={}&left={}&compact=1",
            announce_url,
            info_hash_encoded,
            peer_id_encoded,
            self.port,
            uploaded,
            downloaded,
            left,
        );

        if let Some(event) = event {
            url.push_str(&format!("&event={}", event));
        }

        log::info!("HTTP announcing to: {}", announce_url);

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .map_err(|e| TrackerError::Http(e.to_string()))?;

        let response = client
            .get(&url)
            .send()
            .await
            .map_err(|e| TrackerError::Http(e.to_string()))?;

        let bytes = response
            .bytes()
            .await
            .map_err(|e| TrackerError::Http(e.to_string()))?;

        let (value, _) =
            crate::bencode::decode(&bytes).map_err(|e| TrackerError::Decode(e.to_string()))?;

        // Check for failure reason
        if let Some(reason) = value.get("failure reason").and_then(|v| v.as_str()) {
            return Err(TrackerError::Failure(reason.to_string()));
        }

        let interval = value
            .get("interval")
            .and_then(|v| v.as_int())
            .unwrap_or(1800) as u64;

        let peers = if let Some(peers_bytes) = value.get("peers").and_then(|v| v.as_bytes()) {
            // Compact format: 6 bytes per peer (4 IP + 2 port)
            parse_compact_peers(peers_bytes)?
        } else if let Some(peers_list) = value.get("peers").and_then(|v| v.as_list()) {
            // Dictionary format
            parse_dict_peers(peers_list)?
        } else {
            Vec::new()
        };

        let seeders = value
            .get("complete")
            .and_then(|v| v.as_int())
            .map(|v| v as u64);
        let leechers = value
            .get("incomplete")
            .and_then(|v| v.as_int())
            .map(|v| v as u64);

        log::info!(
            "HTTP tracker returned {} peers (seeders: {:?}, leechers: {:?})",
            peers.len(),
            seeders,
            leechers
        );

        Ok(TrackerResponse {
            interval,
            peers,
            seeders,
            leechers,
        })
    }
}

/// Announce to a UDP tracker (BEP 15).
async fn udp_announce(
    url: &str,
    info_hash: &[u8; 20],
    peer_id: &[u8; 20],
    port: u16,
    uploaded: u64,
    downloaded: u64,
    left: u64,
    event: Option<&str>,
) -> Result<TrackerResponse, TrackerError> {
    let stripped = url
        .strip_prefix("udp://")
        .ok_or_else(|| TrackerError::Http("not a UDP URL".into()))?;
    let addr_part = stripped.split('/').next().unwrap_or(stripped);

    log::info!("UDP announcing to: {}", addr_part);

    let socket = tokio::net::UdpSocket::bind("0.0.0.0:0")
        .await
        .map_err(|e| TrackerError::Http(e.to_string()))?;

    // Resolve hostname
    let addrs: Vec<std::net::SocketAddr> = tokio::net::lookup_host(addr_part)
        .await
        .map_err(|e| {
            TrackerError::Http(format!("DNS resolution failed for {}: {}", addr_part, e))
        })?
        .collect();

    let addr = addrs
        .first()
        .ok_or_else(|| TrackerError::Http("no addresses resolved".into()))?;
    socket
        .connect(addr)
        .await
        .map_err(|e| TrackerError::Http(e.to_string()))?;

    // Step 1: Connect request
    let transaction_id: u32 = rand::random();
    let mut connect_req = [0u8; 16];
    connect_req[..8].copy_from_slice(&0x41727101980u64.to_be_bytes()); // magic constant
    // action = 0 (connect) — bytes 8..12 already zero
    connect_req[12..16].copy_from_slice(&transaction_id.to_be_bytes());

    socket
        .send(&connect_req)
        .await
        .map_err(|e| TrackerError::Http(e.to_string()))?;

    let mut buf = vec![0u8; 2048];
    let len = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        socket.recv(&mut buf),
    )
    .await
    .map_err(|_| TrackerError::Http("UDP connect timeout".into()))?
    .map_err(|e| TrackerError::Http(e.to_string()))?;

    if len < 16 {
        return Err(TrackerError::Http("UDP connect response too short".into()));
    }

    let action = u32::from_be_bytes(buf[0..4].try_into().unwrap());
    let resp_tid = u32::from_be_bytes(buf[4..8].try_into().unwrap());
    if action != 0 || resp_tid != transaction_id {
        return Err(TrackerError::Http("invalid UDP connect response".into()));
    }
    let connection_id = u64::from_be_bytes(buf[8..16].try_into().unwrap());

    log::debug!("UDP tracker connected, connection_id={}", connection_id);

    // Step 2: Announce request
    let transaction_id: u32 = rand::random();
    let event_num: u32 = match event {
        Some("started") => 2,
        Some("completed") => 1,
        Some("stopped") => 3,
        _ => 0,
    };

    let mut announce_req = [0u8; 98];
    announce_req[0..8].copy_from_slice(&connection_id.to_be_bytes());
    announce_req[8..12].copy_from_slice(&1u32.to_be_bytes()); // action = announce
    announce_req[12..16].copy_from_slice(&transaction_id.to_be_bytes());
    announce_req[16..36].copy_from_slice(info_hash);
    announce_req[36..56].copy_from_slice(peer_id);
    announce_req[56..64].copy_from_slice(&downloaded.to_be_bytes());
    announce_req[64..72].copy_from_slice(&left.to_be_bytes());
    announce_req[72..80].copy_from_slice(&uploaded.to_be_bytes());
    announce_req[80..84].copy_from_slice(&event_num.to_be_bytes());
    // bytes 84..88 = IP address (0 = default)
    announce_req[88..92].copy_from_slice(&rand::random::<u32>().to_be_bytes()); // key
    announce_req[92..96].copy_from_slice(&(-1i32).to_be_bytes()); // num_want = -1
    announce_req[96..98].copy_from_slice(&port.to_be_bytes());

    socket
        .send(&announce_req)
        .await
        .map_err(|e| TrackerError::Http(e.to_string()))?;

    let len = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        socket.recv(&mut buf),
    )
    .await
    .map_err(|_| TrackerError::Http("UDP announce timeout".into()))?
    .map_err(|e| TrackerError::Http(e.to_string()))?;

    if len < 20 {
        return Err(TrackerError::Http(
            "UDP announce response too short".into(),
        ));
    }

    let action = u32::from_be_bytes(buf[0..4].try_into().unwrap());
    let resp_tid = u32::from_be_bytes(buf[4..8].try_into().unwrap());

    if action == 3 {
        // Error response
        let msg = String::from_utf8_lossy(&buf[8..len]).to_string();
        return Err(TrackerError::Failure(msg));
    }

    if action != 1 || resp_tid != transaction_id {
        return Err(TrackerError::Http(
            "invalid UDP announce response".into(),
        ));
    }

    let interval = u32::from_be_bytes(buf[8..12].try_into().unwrap()) as u64;
    let leechers = u32::from_be_bytes(buf[12..16].try_into().unwrap()) as u64;
    let seeders = u32::from_be_bytes(buf[16..20].try_into().unwrap()) as u64;

    let peer_data = &buf[20..len];
    let peers = parse_compact_peers(peer_data)?;

    log::info!(
        "UDP tracker returned {} peers (seeders: {}, leechers: {})",
        peers.len(),
        seeders,
        leechers
    );

    Ok(TrackerResponse {
        interval,
        peers,
        seeders: Some(seeders),
        leechers: Some(leechers),
    })
}

fn parse_compact_peers(data: &[u8]) -> Result<Vec<SocketAddrV4>, TrackerError> {
    if data.len() % 6 != 0 {
        return Err(TrackerError::InvalidPeers);
    }

    Ok(data
        .chunks_exact(6)
        .map(|chunk| {
            let ip = Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]);
            let port = u16::from_be_bytes([chunk[4], chunk[5]]);
            SocketAddrV4::new(ip, port)
        })
        .collect())
}

fn parse_dict_peers(
    peers: &[crate::bencode::Value],
) -> Result<Vec<SocketAddrV4>, TrackerError> {
    let mut addrs = Vec::new();
    for peer in peers {
        if let (Some(ip_str), Some(port)) = (
            peer.get("ip").and_then(|v| v.as_str()),
            peer.get("port").and_then(|v| v.as_int()),
        ) {
            if let Ok(ip) = ip_str.parse::<Ipv4Addr>() {
                addrs.push(SocketAddrV4::new(ip, port as u16));
            }
        }
    }
    Ok(addrs)
}
