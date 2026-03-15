use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use tauri::{Manager, State};
use tokio::sync::{mpsc, RwLock};
use torrent_core::engine::{EngineEvent, TorrentEngine};
use torrent_core::magnet::MagnetLink;
use torrent_core::metadata;
use torrent_core::peer::Bitfield;
use torrent_core::persistence::{self, TorrentState};
use torrent_core::piece::{self as piece_ops, PieceManager};
use torrent_core::torrent::Metainfo;

/// Represents a torrent in the UI.
#[derive(Debug, Clone, Serialize)]
pub struct TorrentInfo {
    pub id: String,
    pub name: String,
    pub total_size: u64,
    pub num_pieces: usize,
    pub pieces_done: usize,
    pub downloaded_bytes: u64,
    pub uploaded_bytes: u64,
    pub download_speed: f64,
    pub upload_speed: f64,
    pub num_peers: usize,
    pub status: String,
    pub progress: f64,
    pub seeders: Option<u64>,
    pub leechers: Option<u64>,
    /// How many of our connected peers are seeders (have all pieces).
    pub connected_seeders: usize,
    /// How many of our connected peers are leechers (missing some pieces).
    pub connected_leechers: usize,
    pub download_dir: String,
    /// Estimated time remaining in seconds (None when paused or speed is zero).
    pub eta_secs: Option<u64>,
    /// Non-fatal warning (e.g. disk permission error on some peers). Cleared on next successful progress.
    pub warning: Option<String>,
}

/// File info returned to the frontend.
#[derive(Debug, Clone, Serialize)]
pub struct TorrentFileInfo {
    pub path: String,
    pub size: u64,
    pub progress: f64,
    pub skipped: bool,
}

/// State shared across Tauri commands.
pub struct AppState {
    torrents: RwLock<HashMap<String, TorrentEntry>>,
    download_dir: RwLock<PathBuf>,
    state_dir: PathBuf,
}

struct TorrentEntry {
    metainfo: Metainfo,
    engine: Option<Arc<TorrentEngine>>,
    info: TorrentInfo,
    event_rx: Option<mpsc::UnboundedReceiver<EngineEvent>>,
    download_dir: PathBuf,
    saved_bitfield: Vec<u8>,
    last_saved: Option<std::time::Instant>,
    skipped_files: HashSet<usize>,
    /// True after the first successful verify_pieces() in this session.
    /// Allows resume to skip re-reading all pieces from disk.
    bitfield_verified: bool,
}

impl AppState {
    fn new() -> Self {
        let download_dir = dirs_next::download_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("TorrentRust");
        let state_dir = dirs_next::data_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("com.torrentrust.app")
            .join("state");
        Self {
            torrents: RwLock::new(HashMap::new()),
            download_dir: RwLock::new(download_dir),
            state_dir,
        }
    }
}

/// Build a TorrentState from an entry for persistence.
fn build_torrent_state(entry: &TorrentEntry) -> TorrentState {
    TorrentState {
        version: 1,
        metainfo: entry.metainfo.clone(),
        download_dir: entry.download_dir.clone(),
        completed_pieces: entry.saved_bitfield.clone(),
        num_pieces: entry.info.num_pieces,
        downloaded_bytes: entry.info.downloaded_bytes,
        uploaded_bytes: entry.info.uploaded_bytes,
        status: entry.info.status.clone(),
        skipped_files: entry.skipped_files.iter().copied().collect(),
    }
}

/// Save a torrent's state in the background.
fn save_in_background(state: TorrentState, state_dir: PathBuf) {
    tokio::spawn(async move {
        if let Err(e) = persistence::save_state(&state, &state_dir).await {
            log::error!("Failed to save torrent state: {}", e);
        }
    });
}

/// Compute which piece indices should be skipped based on skipped file indices.
/// A piece is skipped only if ALL files it touches are in `skipped_files`.
fn compute_skipped_pieces(metainfo: &Metainfo, skipped_files: &HashSet<usize>) -> HashSet<usize> {
    if skipped_files.is_empty() {
        return HashSet::new();
    }

    let piece_length = metainfo.piece_length;
    let num_pieces = metainfo.num_pieces();
    let mut skipped = HashSet::new();

    for piece_idx in 0..num_pieces {
        let piece_start = piece_idx as u64 * piece_length;
        let piece_end = std::cmp::min(piece_start + piece_length, metainfo.total_size);

        // Check which files this piece touches
        let mut file_offset = 0u64;
        let mut all_skipped = true;
        let mut touches_any = false;

        for (file_idx, file) in metainfo.files.iter().enumerate() {
            let file_start = file_offset;
            let file_end = file_start + file.length;
            file_offset = file_end;

            // Does this piece overlap with this file?
            if piece_start < file_end && piece_end > file_start {
                touches_any = true;
                if !skipped_files.contains(&file_idx) {
                    all_skipped = false;
                    break;
                }
            }
        }

        if touches_any && all_skipped {
            skipped.insert(piece_idx);
        }
    }

    skipped
}

/// Calculate per-file progress from per-piece progress fractions.
/// Each entry in `piece_progress` is 0.0 (missing), 0.x (partial blocks), or 1.0 (complete).
fn calculate_file_progress(metainfo: &Metainfo, piece_progress: &[f64], skipped_files: &HashSet<usize>) -> Vec<TorrentFileInfo> {
    let piece_length = metainfo.piece_length;
    let mut file_offset = 0u64;

    metainfo.files.iter().enumerate().map(|(file_idx, file)| {
        let file_start = file_offset;
        let file_end = file_start + file.length;
        file_offset = file_end;

        if piece_length == 0 || file.length == 0 {
            return TorrentFileInfo {
                path: file.path.to_string_lossy().to_string(),
                size: file.length,
                progress: 0.0,
                skipped: skipped_files.contains(&file_idx),
            };
        }

        let first_piece = (file_start / piece_length) as usize;
        let last_piece = if file_end == 0 {
            0
        } else {
            ((file_end - 1) / piece_length) as usize
        };

        let mut completed_bytes = 0.0f64;
        for piece_idx in first_piece..=last_piece {
            if piece_idx >= piece_progress.len() {
                break;
            }
            let frac = piece_progress[piece_idx];
            if frac > 0.0 {
                let piece_start = piece_idx as u64 * piece_length;
                let piece_end = std::cmp::min(piece_start + piece_length, metainfo.total_size);
                let overlap_start = std::cmp::max(piece_start, file_start);
                let overlap_end = std::cmp::min(piece_end, file_end);
                if overlap_end > overlap_start {
                    completed_bytes += (overlap_end - overlap_start) as f64 * frac;
                }
            }
        }

        let progress = if file.length > 0 {
            (completed_bytes / file.length as f64).min(1.0)
        } else {
            0.0
        };

        TorrentFileInfo {
            path: file.path.to_string_lossy().to_string(),
            size: file.length,
            progress,
            skipped: skipped_files.contains(&file_idx),
        }
    }).collect()
}

#[tauri::command]
async fn add_torrent(
    state: State<'_, AppState>,
    path: String,
    download_dir: Option<String>,
) -> Result<TorrentInfo, String> {
    let metainfo =
        Metainfo::from_file(&PathBuf::from(&path)).map_err(|e| e.to_string())?;

    let dl_dir = match download_dir {
        Some(d) => PathBuf::from(d),
        None => state.download_dir.read().await.clone(),
    };

    let id = metainfo.info_hash_hex();
    let info = TorrentInfo {
        id: id.clone(),
        name: metainfo.name.clone(),
        total_size: metainfo.total_size,
        num_pieces: metainfo.num_pieces(),
        pieces_done: 0,
        downloaded_bytes: 0,
        uploaded_bytes: 0,
        download_speed: 0.0,
        upload_speed: 0.0,
        num_peers: 0,
        status: "paused".to_string(),
        progress: 0.0,
        seeders: None,
        leechers: None,
        connected_seeders: 0,
        connected_leechers: 0,
        download_dir: dl_dir.to_string_lossy().to_string(),
        eta_secs: None,
        warning: None,
    };

    let entry = TorrentEntry {
        metainfo,
        engine: None,
        info: info.clone(),
        event_rx: None,
        download_dir: dl_dir,
        saved_bitfield: Vec::new(),
        last_saved: None,
        skipped_files: HashSet::new(),
        bitfield_verified: false,
    };

    // Save state to disk
    let torrent_state = build_torrent_state(&entry);
    save_in_background(torrent_state, state.state_dir.clone());

    state.torrents.write().await.insert(id, entry);
    Ok(info)
}

#[tauri::command]
async fn add_magnet(
    state: State<'_, AppState>,
    uri: String,
    download_dir: Option<String>,
) -> Result<TorrentInfo, String> {
    let magnet = MagnetLink::parse(&uri).map_err(|e| e.to_string())?;
    let id = magnet.info_hash_hex();

    // Return early if already added
    if state.torrents.read().await.contains_key(&id) {
        return Err("Torrent already added".to_string());
    }

    let dl_dir = match download_dir {
        Some(d) => PathBuf::from(d),
        None => state.download_dir.read().await.clone(),
    };

    // Add a placeholder entry while fetching metadata
    let placeholder_name = magnet
        .display_name
        .clone()
        .unwrap_or_else(|| format!("magnet:{}", &id[..8]));

    let placeholder_info = TorrentInfo {
        id: id.clone(),
        name: placeholder_name,
        total_size: 0,
        num_pieces: 0,
        pieces_done: 0,
        downloaded_bytes: 0,
        uploaded_bytes: 0,
        download_speed: 0.0,
        upload_speed: 0.0,
        num_peers: 0,
        status: "fetching metadata".to_string(),
        progress: 0.0,
        seeders: None,
        leechers: None,
        connected_seeders: 0,
        connected_leechers: 0,
        download_dir: dl_dir.to_string_lossy().to_string(),
        eta_secs: None,
        warning: None,
    };

    // Insert placeholder
    {
        let entry = TorrentEntry {
            metainfo: Metainfo {
                announce: String::new(),
                announce_list: None,
                info_hash: magnet.info_hash,
                name: placeholder_info.name.clone(),
                piece_length: 0,
                pieces: Vec::new(),
                files: Vec::new(),
                total_size: 0,
                info_hash_bytes: Vec::new(),
            },
            engine: None,
            info: placeholder_info.clone(),
            event_rx: None,
            download_dir: dl_dir.clone(),
            saved_bitfield: Vec::new(),
            last_saved: None,
            skipped_files: HashSet::new(),
            bitfield_verified: false,
        };
        state.torrents.write().await.insert(id.clone(), entry);
    }

    // Fetch metadata from peers (this may take a while)
    let metainfo = match metadata::fetch_metadata(&magnet).await {
        Ok(m) => m,
        Err(e) => {
            // Update status to error
            if let Some(entry) = state.torrents.write().await.get_mut(&id) {
                entry.info.status = format!("error: {}", e);
            }
            return Err(e.to_string());
        }
    };

    // Update entry with real metadata
    let info = TorrentInfo {
        id: id.clone(),
        name: metainfo.name.clone(),
        total_size: metainfo.total_size,
        num_pieces: metainfo.num_pieces(),
        pieces_done: 0,
        downloaded_bytes: 0,
        uploaded_bytes: 0,
        download_speed: 0.0,
        upload_speed: 0.0,
        num_peers: 0,
        status: "paused".to_string(),
        progress: 0.0,
        seeders: None,
        leechers: None,
        connected_seeders: 0,
        connected_leechers: 0,
        download_dir: dl_dir.to_string_lossy().to_string(),
        eta_secs: None,
        warning: None,
    };

    {
        let mut torrents = state.torrents.write().await;
        if let Some(entry) = torrents.get_mut(&id) {
            entry.metainfo = metainfo;
            entry.info = info.clone();

            // Save state now that we have real metadata
            let torrent_state = build_torrent_state(entry);
            save_in_background(torrent_state, state.state_dir.clone());
        }
    }

    Ok(info)
}

#[tauri::command]
async fn start_torrent(
    state: State<'_, AppState>,
    id: String,
) -> Result<(), String> {
    // Step 1: Quick guard + data extraction — hold write lock only briefly.
    let (metainfo, download_dir, saved_bitfield, num_pieces, skipped_files,
         downloaded_bytes, uploaded_bytes, already_verified) = {
        let mut torrents = state.torrents.write().await;
        let entry = torrents.get_mut(&id).ok_or("Torrent not found")?;

        if entry.engine.is_some() || entry.info.status == "verifying" {
            return Ok(()); // Already running or start already in progress
        }

        // Mark as verifying so the UI shows progress and concurrent starts are rejected.
        entry.info.status = "verifying".to_string();

        (
            entry.metainfo.clone(),
            entry.download_dir.clone(),
            entry.saved_bitfield.clone(),
            entry.info.num_pieces,
            entry.skipped_files.clone(),
            entry.info.downloaded_bytes,
            entry.info.uploaded_bytes,
            entry.bitfield_verified,
        )
    }; // write lock released — poll_events and get_torrents can now run freely

    // Step 2: Slow work with no lock held.

    // Ensure download directory exists.
    if let Err(e) = tokio::fs::create_dir_all(&download_dir).await {
        if let Some(entry) = state.torrents.write().await.get_mut(&id) {
            entry.info.status = "paused".to_string();
        }
        return Err(format!("Failed to create download directory: {}", e));
    }

    let (event_tx, event_rx) = mpsc::unbounded_channel();

    let mut pm = if !saved_bitfield.is_empty() {
        PieceManager::with_saved_bitfield(
            metainfo.clone(),
            download_dir.clone(),
            saved_bitfield,
            num_pieces,
        )
    } else {
        PieceManager::new(metainfo.clone(), download_dir.clone())
    };

    if !skipped_files.is_empty() {
        let skipped = compute_skipped_pieces(&metainfo, &skipped_files);
        pm.set_skipped_pieces(skipped);
    }

    // Verify pieces, or trust the already-verified bitfield for same-session resume.
    let verified = if already_verified {
        pm.apply_bitfield_without_verify()
    } else {
        match pm.verify_pieces().await {
            Ok(v) => v,
            Err(e) => {
                if let Some(entry) = state.torrents.write().await.get_mut(&id) {
                    entry.info.status = "paused".to_string();
                }
                return Err(e.to_string());
            }
        }
    };

    let skipped_count = pm.skipped_count();
    let effective_total = num_pieces.saturating_sub(skipped_count);
    let progress = if effective_total > 0 {
        verified as f64 / effective_total as f64
    } else {
        0.0
    };
    let new_bitfield = pm.bitfield_bytes();
    let is_complete = pm.is_complete();

    let engine = Arc::new(TorrentEngine::with_piece_manager(
        metainfo.clone(),
        download_dir.clone(),
        pm,
        downloaded_bytes,
        uploaded_bytes,
        event_tx,
    ));

    // Tracker contact happens here — still no lock held.
    let start_result = engine.start().await;

    // Step 3: Re-acquire write lock to commit results.
    let mut torrents = state.torrents.write().await;
    let entry = match torrents.get_mut(&id) {
        Some(e) => e,
        None => return Err("Torrent was removed during start".to_string()),
    };

    // Guard against a concurrent start that beat us here (unlikely but safe).
    if entry.engine.is_some() {
        return Ok(());
    }

    // Always persist the verified bitfield so next resume skips re-verification.
    entry.saved_bitfield = new_bitfield;
    entry.info.pieces_done = verified;
    entry.info.progress = progress;
    entry.bitfield_verified = true;

    match start_result {
        Ok(()) => {
            entry.engine = Some(engine);
            entry.event_rx = Some(event_rx);
            entry.info.status = if is_complete {
                "complete".to_string()
            } else {
                "downloading".to_string()
            };
            entry.last_saved = Some(std::time::Instant::now()); // Don't trigger immediate save

            // Pre-allocate in background (skip for complete torrents)
            if !is_complete {
                let meta = entry.metainfo.clone();
                let dl_dir = entry.download_dir.clone();
                let skipped = entry.skipped_files.clone();
                tokio::spawn(async move {
                    if let Err(e) = piece_ops::preallocate_files(&meta, &dl_dir, &skipped).await {
                        log::warn!("File pre-allocation failed: {}", e);
                    }
                });
            }

            Ok(())
        }
        Err(e) => {
            // Engine start failed (e.g. no peers). Reset to paused and save the
            // verified bitfield so the next resume attempt skips re-verification.
            entry.info.status = "paused".to_string();
            let torrent_state = build_torrent_state(entry);
            save_in_background(torrent_state, state.state_dir.clone());
            Err(e.to_string())
        }
    }
}

#[tauri::command]
async fn stop_torrent(
    state: State<'_, AppState>,
    id: String,
) -> Result<(), String> {
    let mut torrents = state.torrents.write().await;
    let entry = torrents.get_mut(&id).ok_or("Torrent not found")?;

    if let Some(engine) = &entry.engine {
        // Snapshot state before stopping
        let (bitfield, _num_pieces, downloaded, uploaded) = engine.snapshot_state().await;
        entry.saved_bitfield = bitfield;
        entry.info.downloaded_bytes = downloaded;
        entry.info.uploaded_bytes = uploaded;
        engine.stop();
    }
    entry.engine = None;
    entry.event_rx = None;
    entry.info.status = "paused".to_string();
    entry.info.download_speed = 0.0;
    entry.info.upload_speed = 0.0;
    entry.info.num_peers = 0;
    entry.info.eta_secs = None;

    // Save state to disk
    let torrent_state = build_torrent_state(entry);
    save_in_background(torrent_state, state.state_dir.clone());

    Ok(())
}

#[tauri::command]
async fn remove_torrent(
    state: State<'_, AppState>,
    id: String,
    delete_files: Option<bool>,
) -> Result<(), String> {
    let mut torrents = state.torrents.write().await;
    let files_to_delete = if delete_files.unwrap_or(false) {
        if let Some(entry) = torrents.get(&id) {
            // Collect file paths before removing entry
            let download_dir = entry.download_dir.clone();
            let file_paths: Vec<PathBuf> = entry
                .metainfo
                .files
                .iter()
                .map(|f| download_dir.join(&f.path))
                .collect();
            Some(file_paths)
        } else {
            None
        }
    } else {
        None
    };

    if let Some(entry) = torrents.get(&id) {
        if let Some(engine) = &entry.engine {
            engine.stop();
        }
    }
    torrents.remove(&id);
    drop(torrents); // Release lock before doing I/O

    // Delete state file and optionally downloaded files
    let state_dir = state.state_dir.clone();
    let id_clone = id.clone();
    tokio::spawn(async move {
        if let Err(e) = persistence::delete_state(&id_clone, &state_dir).await {
            log::error!("Failed to delete torrent state: {}", e);
        }
        if let Some(paths) = files_to_delete {
            for path in paths {
                if path.exists() {
                    if let Err(e) = tokio::fs::remove_file(&path).await {
                        log::error!("Failed to delete file {:?}: {}", path, e);
                    }
                }
            }
        }
    });

    Ok(())
}

#[tauri::command]
async fn get_torrents(state: State<'_, AppState>) -> Result<Vec<TorrentInfo>, String> {
    let torrents = state.torrents.read().await;
    Ok(torrents.values().map(|e| {
        let mut info = e.info.clone();
        // Cap downloaded_bytes at total_size for display (may overcount due to re-downloads)
        if info.total_size > 0 && info.downloaded_bytes > info.total_size {
            info.downloaded_bytes = info.total_size;
        }
        info
    }).collect())
}

#[tauri::command]
async fn get_torrent_files(
    state: State<'_, AppState>,
    id: String,
) -> Result<Vec<TorrentFileInfo>, String> {
    // Extract what we need and release the read lock BEFORE awaiting the engine mutex.
    // This avoids contention with poll_events which needs a write lock every second.
    let (metainfo, engine_opt, saved_bitfield, num_pieces, skipped_files) = {
        let torrents = state.torrents.read().await;
        let entry = torrents.get(&id).ok_or("Torrent not found")?;

        if entry.metainfo.piece_length == 0 {
            return Ok(Vec::new()); // Placeholder entry, no metadata yet
        }

        (
            entry.metainfo.clone(),
            entry.engine.clone(),
            entry.saved_bitfield.clone(),
            entry.info.num_pieces,
            entry.skipped_files.clone(),
        )
    }; // read lock released here

    let piece_progress: Vec<f64> = if let Some(engine) = engine_opt {
        engine.snapshot_piece_progress().await
    } else {
        let bitfield = Bitfield::from_bytes(saved_bitfield, num_pieces);
        (0..num_pieces)
            .map(|i| if bitfield.has_piece(i) { 1.0 } else { 0.0 })
            .collect()
    };

    Ok(calculate_file_progress(&metainfo, &piece_progress, &skipped_files))
}

#[tauri::command]
async fn toggle_file_skip(
    state: State<'_, AppState>,
    id: String,
    file_index: usize,
) -> Result<(), String> {
    let mut torrents = state.torrents.write().await;
    let entry = torrents.get_mut(&id).ok_or("Torrent not found")?;

    if file_index >= entry.metainfo.files.len() {
        return Err("Invalid file index".to_string());
    }

    // Toggle
    let was_skipped = entry.skipped_files.contains(&file_index);
    if was_skipped {
        entry.skipped_files.remove(&file_index);
    } else {
        entry.skipped_files.insert(file_index);
    }

    // Recompute skipped pieces and apply to engine
    let skipped_pieces = compute_skipped_pieces(&entry.metainfo, &entry.skipped_files);
    if let Some(engine) = &entry.engine {
        engine.set_skipped_pieces(skipped_pieces).await;
    }

    // When unskipping, pre-allocate the file (standalone — no piece_manager lock)
    if was_skipped {
        let meta = entry.metainfo.clone();
        let dl_dir = entry.download_dir.clone();
        tokio::spawn(async move {
            if let Err(e) = piece_ops::preallocate_file_by_index(&meta, &dl_dir, file_index).await {
                log::warn!("Failed to pre-allocate unskipped file: {}", e);
            }
        });
    }

    // Save state
    let torrent_state = build_torrent_state(entry);
    save_in_background(torrent_state, state.state_dir.clone());

    Ok(())
}

#[tauri::command]
async fn poll_events(
    state: State<'_, AppState>,
    id: String,
) -> Result<Vec<String>, String> {
    let mut torrents = state.torrents.write().await;
    let entry = torrents.get_mut(&id).ok_or("Torrent not found")?;

    let mut events = Vec::new();
    let mut download_completed = false;

    if let Some(rx) = &mut entry.event_rx {
        while let Ok(event) = rx.try_recv() {
            match &event {
                EngineEvent::Progress {
                    pieces_done,
                    pieces_total,
                    downloaded_bytes,
                    uploaded_bytes,
                    download_speed,
                    upload_speed,
                    num_peers,
                    seeders,
                    leechers,
                    connected_seeders,
                    connected_leechers,
                } => {
                    entry.info.pieces_done = *pieces_done;
                    entry.info.warning = None; // Clear warning on successful progress
                    entry.info.downloaded_bytes = *downloaded_bytes;
                    entry.info.uploaded_bytes = *uploaded_bytes;
                    entry.info.download_speed = *download_speed;
                    entry.info.upload_speed = *upload_speed;
                    entry.info.num_peers = *num_peers;
                    entry.info.seeders = *seeders;
                    entry.info.leechers = *leechers;
                    entry.info.connected_seeders = *connected_seeders;
                    entry.info.connected_leechers = *connected_leechers;
                    entry.info.progress = if *pieces_total > 0 {
                        *pieces_done as f64 / *pieces_total as f64
                    } else {
                        0.0
                    };
                    // Calculate ETA from remaining bytes and current speed
                    entry.info.eta_secs = if *download_speed > 0.0 {
                        let remaining = entry.info.total_size as f64
                            * (1.0 - entry.info.progress);
                        Some((remaining / download_speed) as u64)
                    } else {
                        None
                    };
                }
                EngineEvent::DownloadComplete => {
                    entry.info.status = "complete".to_string();
                    entry.info.progress = 1.0;
                    entry.info.eta_secs = Some(0);
                    download_completed = true;
                }
                EngineEvent::Error(msg) => {
                    // Non-fatal: store as warning, don't change status
                    // (engine is still running with other peers)
                    entry.info.warning = Some(msg.clone());
                }
                _ => {}
            }
            events.push(format!("{:?}", event));
        }
    }

    // Save on download complete — use snapshot_bitfield (only pm lock, no stats lock)
    // Stats are already up-to-date from the Progress events above.
    if download_completed {
        if let Some(engine) = &entry.engine {
            entry.saved_bitfield = engine.snapshot_bitfield().await;
        }
        let torrent_state = build_torrent_state(entry);
        save_in_background(torrent_state, state.state_dir.clone());
    }

    // Periodic save every 30 seconds during active download
    // Use snapshot_bitfield instead of snapshot_state to avoid lock contention
    // with the engine's stats reporting loop (which holds stats.write + pm.lock).
    if entry.engine.is_some() && !download_completed {
        let should_save = entry
            .last_saved
            .map_or(true, |t| t.elapsed() > std::time::Duration::from_secs(30));
        if should_save {
            if let Some(engine) = &entry.engine {
                entry.saved_bitfield = engine.snapshot_bitfield().await;
            }
            entry.last_saved = Some(std::time::Instant::now());
            let torrent_state = build_torrent_state(entry);
            save_in_background(torrent_state, state.state_dir.clone());
        }
    }

    Ok(events)
}

#[tauri::command]
async fn change_torrent_download_dir(
    state: State<'_, AppState>,
    id: String,
    path: String,
) -> Result<(), String> {
    let mut torrents = state.torrents.write().await;
    let entry = torrents.get_mut(&id).ok_or("Torrent not found")?;

    if entry.engine.is_some() {
        return Err("Stop the torrent before changing its download directory".to_string());
    }

    let new_dir = PathBuf::from(&path);
    entry.download_dir = new_dir.clone();
    entry.info.download_dir = path;
    // Reset verification since files may differ in new location
    entry.bitfield_verified = false;

    let torrent_state = build_torrent_state(entry);
    save_in_background(torrent_state, state.state_dir.clone());

    Ok(())
}

#[tauri::command]
async fn set_download_dir(
    state: State<'_, AppState>,
    path: String,
) -> Result<(), String> {
    *state.download_dir.write().await = PathBuf::from(path);
    Ok(())
}

#[tauri::command]
async fn get_download_dir(state: State<'_, AppState>) -> Result<String, String> {
    Ok(state
        .download_dir
        .read()
        .await
        .to_string_lossy()
        .to_string())
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .manage(AppState::new())
        .plugin(tauri_plugin_single_instance::init(|app, _args, _cwd| {
            // Focus the existing window when a second instance is launched
            if let Some(window) = app.get_webview_window("main") {
                let _ = window.show();
                let _ = window.set_focus();
            }
        }))
        .plugin(tauri_plugin_log::Builder::default().build())
        .plugin(tauri_plugin_dialog::init())
        .invoke_handler(tauri::generate_handler![
            add_torrent,
            add_magnet,
            start_torrent,
            stop_torrent,
            remove_torrent,
            get_torrents,
            get_torrent_files,
            toggle_file_skip,
            poll_events,
            change_torrent_download_dir,
            set_download_dir,
            get_download_dir,
        ])
        .on_window_event(|window, event| {
            // macOS: hide window instead of closing (minimize to dock)
            #[cfg(target_os = "macos")]
            if let tauri::WindowEvent::CloseRequested { api, .. } = event {
                api.prevent_close();
                let _ = window.hide();
            }
        })
        .setup(|app| {
            // Load persisted torrent states on startup
            let app_handle = app.handle().clone();
            tauri::async_runtime::spawn(async move {
                let app_state = app_handle.state::<AppState>();
                let state_dir = app_state.state_dir.clone();

                let mut to_autostart = Vec::new();

                match persistence::load_all_states(&state_dir).await {
                    Ok(states) => {
                        let mut torrents = app_state.torrents.write().await;
                        for saved in states {
                            let id = saved.metainfo.info_hash_hex();
                            let num_pieces = saved.num_pieces;
                            let pieces_done = saved.completed_pieces.iter()
                                .enumerate()
                                .flat_map(|(byte_idx, &byte)| {
                                    (0..8).filter_map(move |bit| {
                                        let piece_idx = byte_idx * 8 + (7 - bit);
                                        if piece_idx < num_pieces && (byte & (1 << bit)) != 0 {
                                            Some(piece_idx)
                                        } else {
                                            None
                                        }
                                    })
                                })
                                .count();
                            let progress = if num_pieces > 0 {
                                pieces_done as f64 / num_pieces as f64
                            } else {
                                0.0
                            };

                            let should_autostart = saved.status == "downloading";

                            let info = TorrentInfo {
                                id: id.clone(),
                                name: saved.metainfo.name.clone(),
                                total_size: saved.metainfo.total_size,
                                num_pieces,
                                pieces_done,
                                downloaded_bytes: saved.downloaded_bytes,
                                uploaded_bytes: saved.uploaded_bytes,
                                download_speed: 0.0,
                                upload_speed: 0.0,
                                num_peers: 0,
                                status: if should_autostart {
                                    "paused".to_string() // Will be started shortly
                                } else {
                                    saved.status.clone()
                                },
                                progress,
                                seeders: None,
                                leechers: None,
                                connected_seeders: 0,
                                connected_leechers: 0,
                                download_dir: saved.download_dir.to_string_lossy().to_string(),
                                eta_secs: None,
                                warning: None,
                            };

                            let restored_skipped: HashSet<usize> = saved.skipped_files.into_iter().collect();

                            let entry = TorrentEntry {
                                metainfo: saved.metainfo,
                                engine: None,
                                info,
                                event_rx: None,
                                download_dir: saved.download_dir,
                                saved_bitfield: saved.completed_pieces,
                                last_saved: None,
                                skipped_files: restored_skipped,
                                bitfield_verified: false,
                            };

                            if should_autostart {
                                to_autostart.push(id.clone());
                            }

                            torrents.insert(id, entry);
                        }
                        log::info!("Loaded {} torrents from disk", torrents.len());
                    }
                    Err(e) => log::error!("Failed to load torrent states: {}", e),
                }

                // Auto-start torrents that were downloading
                for id in to_autostart {
                    log::info!("Auto-starting torrent: {}", id);
                    let mut torrents = app_state.torrents.write().await;
                    let entry = match torrents.get_mut(&id) {
                        Some(e) => e,
                        None => continue,
                    };

                    if entry.metainfo.piece_length == 0 {
                        continue;
                    }

                    let download_dir = entry.download_dir.clone();
                    if let Err(e) = tokio::fs::create_dir_all(&download_dir).await {
                        log::error!("Failed to create download dir for auto-start: {}", e);
                        continue;
                    }

                    let (event_tx, event_rx) = mpsc::unbounded_channel();

                    let mut pm = if !entry.saved_bitfield.is_empty() {
                        PieceManager::with_saved_bitfield(
                            entry.metainfo.clone(),
                            download_dir.clone(),
                            entry.saved_bitfield.clone(),
                            entry.info.num_pieces,
                        )
                    } else {
                        PieceManager::new(entry.metainfo.clone(), download_dir.clone())
                    };

                    // Apply skipped files
                    if !entry.skipped_files.is_empty() {
                        let skipped = compute_skipped_pieces(&entry.metainfo, &entry.skipped_files);
                        pm.set_skipped_pieces(skipped);
                    }

                    match pm.verify_pieces().await {
                        Ok(verified) => {
                            entry.info.pieces_done = verified;
                            entry.info.progress = if entry.info.num_pieces > 0 {
                                verified as f64 / entry.info.num_pieces as f64
                            } else {
                                0.0
                            };
                            entry.saved_bitfield = pm.bitfield_bytes();
                            let torrent_complete = pm.is_complete();

                            let engine = Arc::new(TorrentEngine::with_piece_manager(
                                entry.metainfo.clone(),
                                download_dir,
                                pm,
                                entry.info.downloaded_bytes,
                                entry.info.uploaded_bytes,
                                event_tx,
                            ));

                            match engine.start().await {
                                Ok(()) => {
                                    entry.engine = Some(engine);
                                    entry.event_rx = Some(event_rx);
                                    entry.info.status = if torrent_complete {
                                        "complete".to_string()
                                    } else {
                                        "downloading".to_string()
                                    };
                                    log::info!("Auto-started torrent: {} (complete: {})", id, torrent_complete);

                                    if !torrent_complete {
                                        let meta = entry.metainfo.clone();
                                        let dl_dir = entry.download_dir.clone();
                                        let skipped = entry.skipped_files.clone();
                                        tokio::spawn(async move {
                                            if let Err(e) = piece_ops::preallocate_files(&meta, &dl_dir, &skipped).await {
                                                log::warn!("File pre-allocation failed: {}", e);
                                            }
                                        });
                                    }
                                }
                                Err(e) => {
                                    log::error!("Failed to auto-start torrent {}: {}", id, e);
                                    entry.info.status = format!("error: {}", e);
                                }
                            }
                        }
                        Err(e) => {
                            log::error!("Piece verification failed for {}: {}", id, e);
                            entry.info.status = format!("error: {}", e);
                        }
                    }
                }
            });

            Ok(())
        })
        .build(tauri::generate_context!())
        .expect("error while building tauri application")
        .run(|app, event| {
            match event {
                // macOS: re-show window when dock icon is clicked
                #[cfg(target_os = "macos")]
                tauri::RunEvent::Reopen {
                    has_visible_windows,
                    ..
                } => {
                    if !has_visible_windows {
                        if let Some(window) = app.get_webview_window("main") {
                            let _ = window.show();
                            let _ = window.set_focus();
                        }
                    }
                }
                // Save all torrent states before exiting.
                // Wrapped in catch_unwind because block_on can panic if the
                // tokio runtime is already shutting down, and panicking across
                // the FFI boundary (tao's applicationWillTerminate) causes SIGABRT.
                tauri::RunEvent::Exit => {
                    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        let app_state = app.state::<AppState>();
                        let state_dir = app_state.state_dir.clone();
                        if let Ok(handle) = tokio::runtime::Handle::try_current() {
                            handle.block_on(async {
                                let mut torrents = app_state.torrents.write().await;
                                for entry in torrents.values_mut() {
                                    // Skip placeholder entries (no metadata yet)
                                    if entry.metainfo.piece_length == 0 {
                                        continue;
                                    }
                                    // Snapshot live engine state before saving
                                    if let Some(engine) = &entry.engine {
                                        let (bitfield, _, downloaded, uploaded) = engine.snapshot_state().await;
                                        entry.saved_bitfield = bitfield;
                                        entry.info.downloaded_bytes = downloaded;
                                        entry.info.uploaded_bytes = uploaded;
                                    }
                                    let ts = build_torrent_state(entry);
                                    if let Err(e) = persistence::save_state(&ts, &state_dir).await {
                                        log::error!("Failed to save state on exit: {}", e);
                                    }
                                }
                            });
                        }
                    }));
                }
                _ => {}
            }
        });
}
