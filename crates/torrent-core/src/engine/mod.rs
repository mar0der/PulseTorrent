use crate::peer::{Bitfield, PeerConnection, Message};
use crate::piece::PieceManager;
use crate::torrent::Metainfo;
use crate::tracker::TrackerClient;
use std::net::SocketAddrV4;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::collections::HashSet;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::time::{Duration, Instant};

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("tracker error: {0}")]
    Tracker(#[from] crate::tracker::TrackerError),
    #[error("piece error: {0}")]
    Piece(#[from] crate::piece::PieceError),
    #[error("peer error: {0}")]
    Peer(#[from] crate::peer::PeerError),
    #[error("no peers found from any tracker")]
    NoPeers,
}

/// Events emitted by the engine for the UI.
#[derive(Debug, Clone)]
pub enum EngineEvent {
    PeerConnected(SocketAddrV4),
    PeerDisconnected(SocketAddrV4),
    PieceCompleted(usize),
    DownloadComplete,
    Progress {
        pieces_done: usize,
        pieces_total: usize,
        downloaded_bytes: u64,
        uploaded_bytes: u64,
        download_speed: f64,
        upload_speed: f64,
        num_peers: usize,
        seeders: Option<u64>,
        leechers: Option<u64>,
    },
    Error(String),
}

/// Stats tracked for speed calculation.
#[derive(Debug, Clone)]
pub struct TransferStats {
    pub downloaded_bytes: u64,
    pub uploaded_bytes: u64,
    pub download_speed: f64,
    pub upload_speed: f64,
    last_downloaded: u64,
    last_uploaded: u64,
    last_update: Instant,
}

impl TransferStats {
    fn new() -> Self {
        Self {
            downloaded_bytes: 0,
            uploaded_bytes: 0,
            download_speed: 0.0,
            upload_speed: 0.0,
            last_downloaded: 0,
            last_uploaded: 0,
            last_update: Instant::now(),
        }
    }

    fn update(&mut self) {
        let elapsed = self.last_update.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            self.download_speed =
                (self.downloaded_bytes - self.last_downloaded) as f64 / elapsed;
            self.upload_speed =
                (self.uploaded_bytes - self.last_uploaded) as f64 / elapsed;
            self.last_downloaded = self.downloaded_bytes;
            self.last_uploaded = self.uploaded_bytes;
            self.last_update = Instant::now();
        }
    }
}

/// The main download/upload engine for a single torrent.
pub struct TorrentEngine {
    metainfo: Arc<Metainfo>,
    piece_manager: Arc<Mutex<PieceManager>>,
    tracker_client: TrackerClient,
    stats: Arc<RwLock<TransferStats>>,
    event_tx: mpsc::UnboundedSender<EngineEvent>,
    /// Per-piece availability count across all connected peers.
    availability: Arc<RwLock<Vec<u32>>>,
    #[allow(dead_code)]
    download_dir: PathBuf,
    shutdown: tokio::sync::watch::Sender<bool>,
    /// Number of currently connected peers.
    active_peers: Arc<AtomicUsize>,
    /// Seeders count from tracker.
    seeders: Arc<RwLock<Option<u64>>>,
    /// Leechers count from tracker.
    leechers: Arc<RwLock<Option<u64>>>,
}

impl TorrentEngine {
    pub fn new(
        metainfo: Metainfo,
        download_dir: PathBuf,
        event_tx: mpsc::UnboundedSender<EngineEvent>,
    ) -> Self {
        let num_pieces = metainfo.num_pieces();
        let metainfo = Arc::new(metainfo);
        let piece_manager = Arc::new(Mutex::new(PieceManager::new(
            (*metainfo).clone(),
            download_dir.clone(),
        )));
        let (shutdown_tx, _) = tokio::sync::watch::channel(false);

        Self {
            metainfo,
            piece_manager,
            tracker_client: TrackerClient::new(6881),
            stats: Arc::new(RwLock::new(TransferStats::new())),
            event_tx,
            availability: Arc::new(RwLock::new(vec![0u32; num_pieces])),
            download_dir,
            shutdown: shutdown_tx,
            active_peers: Arc::new(AtomicUsize::new(0)),
            seeders: Arc::new(RwLock::new(None)),
            leechers: Arc::new(RwLock::new(None)),
        }
    }

    pub fn peer_id(&self) -> [u8; 20] {
        self.tracker_client.peer_id
    }

    /// Collect all tracker URLs to try (primary + announce_list).
    fn tracker_urls(&self) -> Vec<String> {
        let mut urls = Vec::new();
        if !self.metainfo.announce.is_empty() {
            urls.push(self.metainfo.announce.clone());
        }
        if let Some(ref lists) = self.metainfo.announce_list {
            for tier in lists {
                for url in tier {
                    if !urls.contains(url) {
                        urls.push(url.clone());
                    }
                }
            }
        }
        urls
    }

    /// Start the download.
    pub async fn start(&self) -> Result<(), EngineError> {
        let metainfo = self.metainfo.clone();
        let pm = self.piece_manager.clone();

        // Calculate bytes remaining
        let left = {
            let pm = pm.lock().await;
            let done = pm.completed_pieces() as u64;
            metainfo.total_size.saturating_sub(done * metainfo.piece_length)
        };

        // Try all trackers until we get peers
        let tracker_urls = self.tracker_urls();
        let mut all_peers: Vec<SocketAddrV4> = Vec::new();

        for url in &tracker_urls {
            log::info!("Trying tracker: {}", url);
            match self
                .tracker_client
                .announce_to_url(url, &metainfo.info_hash, 0, 0, left, Some("started"))
                .await
            {
                Ok(response) => {
                    log::info!(
                        "Tracker {} returned {} peers (seeders: {:?}, leechers: {:?})",
                        url,
                        response.peers.len(),
                        response.seeders,
                        response.leechers
                    );
                    // Store seeders/leechers from first successful tracker
                    if response.seeders.is_some() {
                        *self.seeders.write().await = response.seeders;
                    }
                    if response.leechers.is_some() {
                        *self.leechers.write().await = response.leechers;
                    }
                    // Deduplicate peers
                    for peer in response.peers {
                        if !all_peers.contains(&peer) {
                            all_peers.push(peer);
                        }
                    }
                    if all_peers.len() >= 50 {
                        break; // Enough peers
                    }
                }
                Err(e) => {
                    log::warn!("Tracker {} failed: {}", url, e);
                }
            }
        }

        if all_peers.is_empty() {
            log::error!("No peers found from any tracker");
            let _ = self
                .event_tx
                .send(EngineEvent::Error("No peers found from any tracker".into()));
            return Err(EngineError::NoPeers);
        }

        log::info!("Total unique peers discovered: {}", all_peers.len());

        // Connect to peers with a concurrency limit (max 30 at a time)
        // and stagger attempts to avoid overwhelming the network.
        let conn_semaphore = Arc::new(tokio::sync::Semaphore::new(30));

        for (i, peer_addr) in all_peers.into_iter().enumerate() {
            let metainfo = self.metainfo.clone();
            let pm = self.piece_manager.clone();
            let stats = self.stats.clone();
            let event_tx = self.event_tx.clone();
            let availability = self.availability.clone();
            let peer_id = self.tracker_client.peer_id;
            let mut shutdown_rx = self.shutdown.subscribe();
            let active_peers = self.active_peers.clone();
            let sem = conn_semaphore.clone();

            tokio::spawn(async move {
                // Stagger: small delay per batch to avoid burst
                if i >= 30 {
                    tokio::time::sleep(Duration::from_millis((i as u64 / 30) * 500)).await;
                }

                let _permit = match sem.acquire().await {
                    Ok(p) => p,
                    Err(_) => return,
                };

                active_peers.fetch_add(1, Ordering::Relaxed);
                let result = handle_peer(
                    peer_addr,
                    metainfo,
                    pm,
                    stats,
                    event_tx.clone(),
                    availability,
                    peer_id,
                    &mut shutdown_rx,
                )
                .await;
                active_peers.fetch_sub(1, Ordering::Relaxed);

                if let Err(e) = result {
                    log::debug!("Peer {} error: {}", peer_addr, e);
                    let _ = event_tx.send(EngineEvent::PeerDisconnected(peer_addr));
                }
            });
        }

        // Stats reporting loop
        let stats = self.stats.clone();
        let pm = self.piece_manager.clone();
        let event_tx = self.event_tx.clone();
        let mut shutdown_rx = self.shutdown.subscribe();
        let active_peers = self.active_peers.clone();
        let seeders = self.seeders.clone();
        let leechers = self.leechers.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let mut s = stats.write().await;
                        s.update();
                        let pm = pm.lock().await;
                        let _ = event_tx.send(EngineEvent::Progress {
                            pieces_done: pm.completed_pieces(),
                            pieces_total: pm.total_pieces(),
                            downloaded_bytes: s.downloaded_bytes,
                            uploaded_bytes: s.uploaded_bytes,
                            download_speed: s.download_speed,
                            upload_speed: s.upload_speed,
                            num_peers: active_peers.load(Ordering::Relaxed),
                            seeders: *seeders.read().await,
                            leechers: *leechers.read().await,
                        });
                        if pm.is_complete() {
                            let _ = event_tx.send(EngineEvent::DownloadComplete);
                            break;
                        }
                    }
                    _ = shutdown_rx.changed() => break,
                }
            }
        });

        Ok(())
    }

    /// Stop the engine.
    pub fn stop(&self) {
        let _ = self.shutdown.send(true);
    }

    /// Create an engine with a pre-verified PieceManager (for resume).
    pub fn with_piece_manager(
        metainfo: Metainfo,
        download_dir: PathBuf,
        piece_manager: PieceManager,
        downloaded_bytes: u64,
        uploaded_bytes: u64,
        event_tx: mpsc::UnboundedSender<EngineEvent>,
    ) -> Self {
        let num_pieces = metainfo.num_pieces();
        let metainfo = Arc::new(metainfo);
        let (shutdown_tx, _) = tokio::sync::watch::channel(false);

        let mut stats = TransferStats::new();
        stats.downloaded_bytes = downloaded_bytes;
        stats.uploaded_bytes = uploaded_bytes;
        stats.last_downloaded = downloaded_bytes;
        stats.last_uploaded = uploaded_bytes;

        Self {
            metainfo,
            piece_manager: Arc::new(Mutex::new(piece_manager)),
            tracker_client: TrackerClient::new(6881),
            stats: Arc::new(RwLock::new(stats)),
            event_tx,
            availability: Arc::new(RwLock::new(vec![0u32; num_pieces])),
            download_dir,
            shutdown: shutdown_tx,
            active_peers: Arc::new(AtomicUsize::new(0)),
            seeders: Arc::new(RwLock::new(None)),
            leechers: Arc::new(RwLock::new(None)),
        }
    }

    /// Update which pieces should be skipped during download.
    pub async fn set_skipped_pieces(&self, skipped: HashSet<usize>) {
        self.piece_manager.lock().await.set_skipped_pieces(skipped);
    }

    /// Capture the current state for persistence.
    pub async fn snapshot_state(&self) -> (Vec<u8>, usize, u64, u64) {
        let pm = self.piece_manager.lock().await;
        let stats = self.stats.read().await;
        (
            pm.bitfield_bytes(),
            pm.total_pieces(),
            stats.downloaded_bytes,
            stats.uploaded_bytes,
        )
    }

    /// Get per-piece progress fractions (0.0-1.0) including partial blocks.
    pub async fn snapshot_piece_progress(&self) -> Vec<f64> {
        self.piece_manager.lock().await.piece_progress_fractions()
    }
}

async fn handle_peer(
    addr: SocketAddrV4,
    metainfo: Arc<Metainfo>,
    piece_manager: Arc<Mutex<PieceManager>>,
    stats: Arc<RwLock<TransferStats>>,
    event_tx: mpsc::UnboundedSender<EngineEvent>,
    availability: Arc<RwLock<Vec<u32>>>,
    our_peer_id: [u8; 20],
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<(), EngineError> {
    // Connect and handshake
    let (mut conn, _peer_handshake) =
        PeerConnection::connect(addr, metainfo.info_hash, our_peer_id).await?;

    let _ = event_tx.send(EngineEvent::PeerConnected(addr));

    let num_pieces = metainfo.num_pieces();
    let mut peer_bitfield = Bitfield::new(num_pieces);
    let mut _am_interested = false;
    let mut peer_choking = true;
    let mut pending_requests: Vec<crate::piece::BlockRequest> = Vec::new();
    let mut current_piece: Option<usize> = None;

    // Send interested
    conn.send(&Message::Interested).await?;
    _am_interested = true;

    let result: Result<(), EngineError> = async {
        loop {
            tokio::select! {
                result = conn.receive() => {
                    let msg = result?;
                    match msg {
                        Message::KeepAlive => {}
                        Message::Choke => {
                            peer_choking = true;
                            // Reset any pending piece
                            if let Some(idx) = current_piece.take() {
                                piece_manager.lock().await.reset_piece(idx);
                            }
                            pending_requests.clear();
                        }
                        Message::Unchoke => {
                            peer_choking = false;
                            // Start requesting pieces
                            if current_piece.is_none() {
                                let avail = availability.read().await;
                                let pm = &mut *piece_manager.lock().await;
                                if let Some(piece_idx) = pm.pick_piece(&peer_bitfield, &avail) {
                                    let blocks = pm.start_piece(piece_idx);
                                    current_piece = Some(piece_idx);
                                    pending_requests = blocks;
                                }
                            }
                            // Send up to 5 pipelined requests
                            let to_send: Vec<_> = pending_requests.iter().take(5).cloned().collect();
                            for req in &to_send {
                                conn.send(&Message::Request {
                                    index: req.piece_index,
                                    begin: req.offset,
                                    length: req.length,
                                }).await?;
                            }
                        }
                        Message::Have(index) => {
                            peer_bitfield.set_piece(index as usize);
                            let mut avail = availability.write().await;
                            if let Some(count) = avail.get_mut(index as usize) {
                                *count += 1;
                            }
                        }
                        Message::Bitfield(data) => {
                            peer_bitfield = Bitfield::from_bytes(data, num_pieces);
                            let mut avail = availability.write().await;
                            for i in 0..num_pieces {
                                if peer_bitfield.has_piece(i) {
                                    if let Some(count) = avail.get_mut(i) {
                                        *count += 1;
                                    }
                                }
                            }
                        }
                        Message::Piece { index, begin, block } => {
                            let block_len = block.len() as u64;
                            let mut pm = piece_manager.lock().await;
                            let complete = match pm.receive_block(index as usize, begin, &block) {
                                Ok(c) => c,
                                Err(_) => {
                                    // Stale block for already-completed piece; skip it
                                    continue;
                                }
                            };

                            {
                                let mut s = stats.write().await;
                                s.downloaded_bytes += block_len;
                            }

                            // Remove this from pending
                            pending_requests.retain(|r| {
                                !(r.piece_index == index && r.offset == begin)
                            });

                            if complete {
                                let valid = pm.finalize_piece(index as usize).await?;
                                if valid {
                                    let _ = event_tx.send(EngineEvent::PieceCompleted(index as usize));
                                }
                                current_piece = None;

                                // Pick next piece
                                if !peer_choking {
                                    let avail = availability.read().await;
                                    if let Some(next_idx) = pm.pick_piece(&peer_bitfield, &avail) {
                                        let blocks = pm.start_piece(next_idx);
                                        current_piece = Some(next_idx);
                                        pending_requests = blocks;
                                    }
                                }
                            }

                            // Pipeline more requests
                            if !peer_choking {
                                let to_send: Vec<_> = pending_requests.iter().take(5).cloned().collect();
                                for req in &to_send {
                                    conn.send(&Message::Request {
                                        index: req.piece_index,
                                        begin: req.offset,
                                        length: req.length,
                                    }).await?;
                                }
                            }
                        }
                        Message::Interested | Message::NotInterested | Message::Request { .. } | Message::Cancel { .. } | Message::Extended { .. } => {
                            // Seeding / extension logic: not yet implemented
                        }
                    }
                }
                _ = shutdown_rx.changed() => {
                    break;
                }
            }
        }
        Ok(())
    }.await;

    // CRITICAL: When peer disconnects, reset any in-progress piece back to Missing
    // so other peers can pick it up. Without this, pieces get stuck as InProgress forever.
    if let Some(idx) = current_piece.take() {
        piece_manager.lock().await.reset_piece(idx);
    }

    result
}
