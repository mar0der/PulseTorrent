use crate::bencode::{self, Value};
use crate::magnet::MagnetLink;
use crate::peer::{Message, PeerConnection, PeerError};
use crate::torrent::Metainfo;
use crate::tracker::TrackerClient;
use sha1::{Digest, Sha1};
use std::collections::BTreeMap;
use std::net::SocketAddrV4;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::Mutex;

const METADATA_PIECE_SIZE: usize = 16384; // 16 KiB per BEP 9

#[derive(Debug, Error)]
pub enum MetadataError {
    #[error("no peers available")]
    NoPeers,
    #[error("no trackers in magnet link")]
    NoTrackers,
    #[error("peer does not support ut_metadata")]
    NoMetadataSupport,
    #[error("metadata hash mismatch")]
    HashMismatch,
    #[error("failed to parse metadata into torrent info")]
    InvalidMetadata,
    #[error("peer error: {0}")]
    Peer(#[from] PeerError),
    #[error("tracker error: {0}")]
    Tracker(#[from] crate::tracker::TrackerError),
    #[error("all peers failed")]
    AllPeersFailed,
}

/// Build the BEP 10 extension handshake payload.
/// We advertise support for ut_metadata (BEP 9).
fn build_ext_handshake() -> Vec<u8> {
    let mut m = BTreeMap::new();
    // We assign local id 1 to ut_metadata
    let mut exts = BTreeMap::new();
    exts.insert(b"ut_metadata".to_vec(), Value::Int(1));
    m.insert(b"m".to_vec(), Value::Dict(exts));

    bencode::encode(&Value::Dict(m))
}

/// Parse the peer's extension handshake to find their ut_metadata id and metadata_size.
fn parse_ext_handshake(payload: &[u8]) -> Option<(u8, usize)> {
    let (value, _) = bencode::decode(payload).ok()?;
    let dict = value.as_dict()?;

    let m = dict.get(b"m".as_slice())?.as_dict()?;
    let ut_metadata_id = m.get(b"ut_metadata".as_slice())?.as_int()? as u8;
    let metadata_size = dict.get(b"metadata_size".as_slice())?.as_int()? as usize;

    Some((ut_metadata_id, metadata_size))
}

/// Build a ut_metadata request message for a specific piece.
fn build_metadata_request(piece: usize) -> Vec<u8> {
    let mut d = BTreeMap::new();
    d.insert(b"msg_type".to_vec(), Value::Int(0)); // 0 = request
    d.insert(b"piece".to_vec(), Value::Int(piece as i64));
    bencode::encode(&Value::Dict(d))
}

/// Parse a ut_metadata response. Returns (msg_type, piece_index, data_after_dict).
fn parse_metadata_response(payload: &[u8]) -> Option<(i64, usize, &[u8])> {
    let (value, consumed) = bencode::decode(payload).ok()?;
    let msg_type = value.get("msg_type")?.as_int()?;
    let piece = value.get("piece")?.as_int()? as usize;
    let data = &payload[consumed..];
    Some((msg_type, piece, data))
}

/// Fetch torrent metadata from peers discovered via trackers in the magnet link.
/// Returns a fully parsed Metainfo.
pub async fn fetch_metadata(magnet: &MagnetLink) -> Result<Metainfo, MetadataError> {
    if magnet.trackers.is_empty() {
        return Err(MetadataError::NoTrackers);
    }

    let tracker_client = TrackerClient::new(6881);

    // Try each tracker to get peers (supports both HTTP and UDP)
    let mut all_peers: Vec<SocketAddrV4> = Vec::new();

    for tracker_url in &magnet.trackers {
        log::info!("Trying tracker for metadata: {}", tracker_url);

        // Use announce_to_url which handles both HTTP and UDP trackers
        let result = tokio::time::timeout(
            Duration::from_secs(15),
            tracker_client.announce_to_url(
                tracker_url,
                &magnet.info_hash,
                0,
                0,
                0,
                Some("started"),
            ),
        )
        .await;

        match result {
            Ok(Ok(response)) => {
                log::info!(
                    "Tracker {} returned {} peers (seeders: {:?}, leechers: {:?})",
                    tracker_url,
                    response.peers.len(),
                    response.seeders,
                    response.leechers,
                );
                for peer in response.peers {
                    if !all_peers.contains(&peer) {
                        all_peers.push(peer);
                    }
                }
            }
            Ok(Err(e)) => {
                log::warn!("Tracker {} failed: {}", tracker_url, e);
            }
            Err(_) => {
                log::warn!("Tracker {} timed out", tracker_url);
            }
        }

        if all_peers.len() >= 30 {
            break; // Enough peers to try
        }
    }

    if all_peers.is_empty() {
        return Err(MetadataError::NoPeers);
    }

    log::info!(
        "Discovered {} unique peers, trying metadata fetch in parallel...",
        all_peers.len()
    );

    // Try ALL peers in parallel (limited to 20 concurrent connections).
    // First peer to succeed wins.
    let result: Arc<Mutex<Option<Metainfo>>> = Arc::new(Mutex::new(None));
    let semaphore = Arc::new(tokio::sync::Semaphore::new(20));
    let trackers = magnet.trackers.clone();

    let mut handles = Vec::new();
    for peer_addr in all_peers {
        let sem = semaphore.clone();
        let result = result.clone();
        let info_hash = magnet.info_hash;
        let peer_id = tracker_client.peer_id;
        let trackers = trackers.clone();

        handles.push(tokio::spawn(async move {
            // Skip if another peer already succeeded
            if result.lock().await.is_some() {
                return;
            }

            let _permit = match sem.acquire().await {
                Ok(p) => p,
                Err(_) => return,
            };

            // Skip if another peer succeeded while we waited
            if result.lock().await.is_some() {
                return;
            }

            log::debug!("Trying metadata from peer: {}", peer_addr);
            match tokio::time::timeout(
                Duration::from_secs(15),
                fetch_from_peer(peer_addr, info_hash, peer_id, &trackers),
            )
            .await
            {
                Ok(Ok(metainfo)) => {
                    log::info!("Got metadata from peer: {}", peer_addr);
                    let mut r = result.lock().await;
                    if r.is_none() {
                        *r = Some(metainfo);
                    }
                }
                Ok(Err(e)) => {
                    log::debug!("Peer {} metadata fetch failed: {}", peer_addr, e);
                }
                Err(_) => {
                    log::debug!("Peer {} metadata fetch timed out", peer_addr);
                }
            }
        }));
    }

    // Wait for all tasks (they'll short-circuit once one succeeds)
    // But set a global deadline
    let _ = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            if result.lock().await.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
            // Check if all handles are done
            if handles.iter().all(|h| h.is_finished()) {
                break;
            }
        }
    })
    .await;

    // Grab the result
    let metainfo = result.lock().await.take();
    match metainfo {
        Some(m) => Ok(m),
        None => Err(MetadataError::AllPeersFailed),
    }
}

/// Fetch metadata from a single peer using BEP 9.
async fn fetch_from_peer(
    addr: SocketAddrV4,
    info_hash: [u8; 20],
    peer_id: [u8; 20],
    trackers: &[String],
) -> Result<Metainfo, MetadataError> {
    // Use a shorter connect timeout (5s) for metadata fetch
    let (mut conn, _peer_handshake) =
        PeerConnection::connect_with_timeout(addr, info_hash, peer_id, Duration::from_secs(5))
            .await?;

    // Send our extension handshake (ext_id=0 is always the ext handshake)
    let ext_hs = build_ext_handshake();
    conn.send(&Message::Extended {
        ext_id: 0,
        payload: ext_hs,
    })
    .await?;

    // Read messages until we get their extension handshake
    let (peer_ut_metadata_id, metadata_size) = loop {
        let msg = tokio::time::timeout(
            std::time::Duration::from_secs(15),
            conn.receive(),
        )
        .await
        .map_err(|_| PeerError::Timeout)??;

        match msg {
            Message::Extended { ext_id: 0, payload } => {
                if let Some(result) = parse_ext_handshake(&payload) {
                    break result;
                }
                return Err(MetadataError::NoMetadataSupport);
            }
            Message::Bitfield(_) | Message::Have(_) | Message::Unchoke | Message::Choke => {
                continue;
            }
            _ => continue,
        }
    };

    if peer_ut_metadata_id == 0 {
        return Err(MetadataError::NoMetadataSupport);
    }

    log::info!(
        "Peer {} supports ut_metadata (id={}), metadata_size={}",
        addr,
        peer_ut_metadata_id,
        metadata_size
    );

    // Calculate number of pieces
    let num_pieces = (metadata_size + METADATA_PIECE_SIZE - 1) / METADATA_PIECE_SIZE;
    let mut metadata = vec![0u8; metadata_size];
    let mut received = vec![false; num_pieces];

    // Request all pieces
    for piece in 0..num_pieces {
        let req = build_metadata_request(piece);
        conn.send(&Message::Extended {
            ext_id: peer_ut_metadata_id,
            payload: req,
        })
        .await?;
    }

    // Receive pieces
    let mut pieces_left = num_pieces;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);

    while pieces_left > 0 {
        let remaining = deadline - tokio::time::Instant::now();
        let msg = tokio::time::timeout(remaining, conn.receive())
            .await
            .map_err(|_| PeerError::Timeout)??;

        if let Message::Extended { ext_id, payload } = msg {
            // ext_id=1 means ut_metadata response (our local mapping)
            if ext_id == 1 {
                if let Some((msg_type, piece, data)) = parse_metadata_response(&payload) {
                    match msg_type {
                        1 => {
                            // data response
                            if piece < num_pieces && !received[piece] {
                                let offset = piece * METADATA_PIECE_SIZE;
                                let end = std::cmp::min(offset + data.len(), metadata_size);
                                metadata[offset..end]
                                    .copy_from_slice(&data[..end - offset]);
                                received[piece] = true;
                                pieces_left -= 1;
                            }
                        }
                        2 => {
                            // reject
                            log::warn!("Peer rejected metadata piece {}", piece);
                            return Err(MetadataError::NoMetadataSupport);
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    // Verify hash
    let hash: [u8; 20] = Sha1::digest(&metadata).into();
    if hash != info_hash {
        return Err(MetadataError::HashMismatch);
    }

    log::info!("Metadata fetched and verified ({} bytes)", metadata_size);

    // Parse the info dict into a full Metainfo
    build_metainfo_from_raw(info_hash, &metadata, trackers)
}

/// Build a Metainfo from the raw bencoded info dictionary.
fn build_metainfo_from_raw(
    info_hash: [u8; 20],
    raw_info: &[u8],
    trackers: &[String],
) -> Result<Metainfo, MetadataError> {
    let (info_value, _) =
        bencode::decode(raw_info).map_err(|_| MetadataError::InvalidMetadata)?;

    let announce = trackers.first().cloned().unwrap_or_default();

    let name = info_value
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();

    let piece_length = info_value
        .get("piece length")
        .and_then(|v| v.as_int())
        .ok_or(MetadataError::InvalidMetadata)? as u64;

    let pieces_raw = info_value
        .get("pieces")
        .and_then(|v| v.as_bytes())
        .ok_or(MetadataError::InvalidMetadata)?;

    if pieces_raw.len() % 20 != 0 {
        return Err(MetadataError::InvalidMetadata);
    }

    let pieces: Vec<[u8; 20]> = pieces_raw
        .chunks_exact(20)
        .map(|c| {
            let mut h = [0u8; 20];
            h.copy_from_slice(c);
            h
        })
        .collect();

    let (files, total_size) = if let Some(length) = info_value.get("length").and_then(|v| v.as_int())
    {
        let length = length as u64;
        (
            vec![crate::torrent::FileInfo {
                path: std::path::PathBuf::from(&name),
                length,
            }],
            length,
        )
    } else if let Some(file_list) = info_value.get("files").and_then(|v| v.as_list()) {
        let mut files = Vec::new();
        let mut total = 0u64;
        for fv in file_list {
            let length = fv
                .get("length")
                .and_then(|v| v.as_int())
                .ok_or(MetadataError::InvalidMetadata)? as u64;
            let path_parts = fv
                .get("path")
                .and_then(|v| v.as_list())
                .ok_or(MetadataError::InvalidMetadata)?;
            let mut path = std::path::PathBuf::from(&name);
            for part in path_parts {
                if let Some(s) = part.as_str() {
                    path.push(s);
                }
            }
            total += length;
            files.push(crate::torrent::FileInfo { path, length });
        }
        (files, total)
    } else {
        return Err(MetadataError::InvalidMetadata);
    };

    let announce_list = if trackers.len() > 1 {
        Some(vec![trackers.to_vec()])
    } else {
        None
    };

    Ok(Metainfo {
        announce,
        announce_list,
        info_hash,
        name,
        piece_length,
        pieces,
        files,
        total_size,
        info_hash_bytes: raw_info.to_vec(),
    })
}
