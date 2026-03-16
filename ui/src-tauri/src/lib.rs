use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tauri::{Manager, State};
use tokio::sync::{mpsc, RwLock};
use torrent_core::engine::{EngineEvent, TorrentEngine};
use torrent_core::magnet::MagnetLink;
use torrent_core::metadata;
use torrent_core::peer::Bitfield;
use torrent_core::persistence::{self, GlobalStats, TorrentState};
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
    /// Global all-time traffic stats (persisted to global_stats.json).
    global_stats: RwLock<GlobalStats>,
    /// Tracks last-known per-torrent bytes so we can compute deltas for global stats.
    last_known_bytes: RwLock<HashMap<String, (u64, u64)>>,
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
    /// Shared verification progress counters (checked, total, verified).
    /// Written by verify_pieces callback, read by get_torrents for UI updates.
    verify_checked: Arc<AtomicUsize>,
    verify_total: Arc<AtomicUsize>,
    verify_verified: Arc<AtomicUsize>,
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
            global_stats: RwLock::new(GlobalStats::default()),
            last_known_bytes: RwLock::new(HashMap::new()),
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
        status: if entry.info.status == "seeding" { "complete".to_string() } else { entry.info.status.clone() },
        skipped_files: entry.skipped_files.iter().copied().collect(),
        bitfield_verified: entry.bitfield_verified,
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
    app: tauri::AppHandle,
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

    // If this torrent is already in the list, just return its current info
    {
        let torrents = state.torrents.read().await;
        if let Some(existing) = torrents.get(&id) {
            return Ok(existing.info.clone());
        }
    }

    // Check if there's a saved state file for this torrent (e.g. from a previous session)
    let existing_state = {
        let path = state.state_dir.join(format!("{}.json", id));
        if path.exists() {
            match tokio::fs::read_to_string(&path).await {
                Ok(contents) => serde_json::from_str::<TorrentState>(&contents).ok(),
                Err(_) => None,
            }
        } else {
            None
        }
    };

    // Restore progress from existing state if available
    let (saved_bitfield, downloaded_bytes, uploaded_bytes, restored_skipped, pieces_done, progress) =
        if let Some(ref saved) = existing_state {
            let num_pieces = saved.num_pieces;
            let pd = saved.completed_pieces.iter()
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
            let prog = if num_pieces > 0 { pd as f64 / num_pieces as f64 } else { 0.0 };
            (
                saved.completed_pieces.clone(),
                saved.downloaded_bytes,
                saved.uploaded_bytes,
                saved.skipped_files.iter().copied().collect::<HashSet<usize>>(),
                pd,
                prog,
            )
        } else {
            (Vec::new(), 0, 0, HashSet::new(), 0, 0.0)
        };

    let info = TorrentInfo {
        id: id.clone(),
        name: metainfo.name.clone(),
        total_size: metainfo.total_size,
        num_pieces: metainfo.num_pieces(),
        pieces_done,
        downloaded_bytes,
        uploaded_bytes,
        download_speed: 0.0,
        upload_speed: 0.0,
        num_peers: 0,
        status: "paused".to_string(),
        progress,
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
        saved_bitfield,
        last_saved: None,
        skipped_files: restored_skipped,
        bitfield_verified: false,
        verify_checked: Arc::new(AtomicUsize::new(0)),
        verify_total: Arc::new(AtomicUsize::new(0)),
        verify_verified: Arc::new(AtomicUsize::new(0)),
    };

    // Only save state to disk for truly new torrents (don't overwrite existing state)
    if existing_state.is_none() {
        let torrent_state = build_torrent_state(&entry);
        save_in_background(torrent_state, state.state_dir.clone());
    }

    state.torrents.write().await.insert(id.clone(), entry);

    // Auto-start in background: verify pieces and begin downloading/seeding.
    // Returns immediately so the UI isn't blocked during piece verification.
    let id_clone = id.clone();
    tokio::spawn(async move {
        let app_state = app.state::<AppState>();
        if let Err(e) = start_torrent_inner(&app_state, id_clone.clone()).await {
            log::warn!("Auto-start failed for {}: {}", id_clone, e);
        }
    });

    Ok(info)
}

#[tauri::command]
async fn add_magnet(
    app: tauri::AppHandle,
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
        verify_checked: Arc::new(AtomicUsize::new(0)),
        verify_total: Arc::new(AtomicUsize::new(0)),
        verify_verified: Arc::new(AtomicUsize::new(0)),
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

    // Auto-start in background: verify pieces and begin downloading/seeding
    let bg_id = id.clone();
    tokio::spawn(async move {
        let app_state = app.state::<AppState>();
        if let Err(e) = start_torrent_inner(&app_state, bg_id.clone()).await {
            log::warn!("Auto-start failed for {}: {}", bg_id, e);
        }
    });

    Ok(info)
}

/// Inner start logic that takes `&AppState` directly (callable from both
/// the IPC command and background auto-start tasks).
async fn start_torrent_inner(state: &AppState, id: String) -> Result<(), String> {
    // Step 1: Quick guard + data extraction — hold write lock only briefly.
    let (metainfo, download_dir, saved_bitfield, num_pieces, skipped_files,
         downloaded_bytes, uploaded_bytes, already_verified,
         v_checked, v_total, v_verified) = {
        let mut torrents = state.torrents.write().await;
        let entry = torrents.get_mut(&id).ok_or("Torrent not found")?;

        if entry.engine.is_some() || entry.info.status == "verifying" {
            return Ok(()); // Already running or start already in progress
        }

        // Mark as verifying so the UI shows progress and concurrent starts are rejected.
        entry.info.status = "verifying".to_string();
        // Reset verification progress counters
        entry.verify_checked.store(0, Ordering::Relaxed);
        entry.verify_total.store(0, Ordering::Relaxed);
        entry.verify_verified.store(0, Ordering::Relaxed);

        (
            entry.metainfo.clone(),
            entry.download_dir.clone(),
            entry.saved_bitfield.clone(),
            entry.info.num_pieces,
            entry.skipped_files.clone(),
            entry.info.downloaded_bytes,
            entry.info.uploaded_bytes,
            entry.bitfield_verified,
            entry.verify_checked.clone(),
            entry.verify_total.clone(),
            entry.verify_verified.clone(),
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
        match pm.verify_pieces_with_progress(|checked, total, verified| {
            v_checked.store(checked, Ordering::Relaxed);
            v_total.store(total, Ordering::Relaxed);
            v_verified.store(verified, Ordering::Relaxed);
        }).await {
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
                "seeding".to_string()
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
async fn start_torrent(
    state: State<'_, AppState>,
    id: String,
) -> Result<(), String> {
    start_torrent_inner(&state, id).await
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
    let current_dl = entry.info.downloaded_bytes;
    let current_ul = entry.info.uploaded_bytes;

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
    drop(torrents);

    // Update and persist global stats
    update_global_stats(&state, &id, current_dl, current_ul).await;
    let gs = state.global_stats.read().await.clone();
    save_global_stats_bg(gs, state.state_dir.clone());

    Ok(())
}

#[tauri::command]
async fn open_download_dir(
    state: State<'_, AppState>,
    id: String,
) -> Result<(), String> {
    let torrents = state.torrents.read().await;
    let entry = torrents.get(&id).ok_or("torrent not found")?;
    let dir = entry.download_dir.clone();
    drop(torrents);

    // Reveal the download directory in Finder/Explorer
    tauri_plugin_opener::reveal_item_in_dir(&dir)
        .map_err(|e| e.to_string())?;
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
        // Show live verification progress when status is "verifying"
        if info.status == "verifying" {
            let checked = e.verify_checked.load(Ordering::Relaxed);
            let total = e.verify_total.load(Ordering::Relaxed);
            let verified = e.verify_verified.load(Ordering::Relaxed);
            if total > 0 {
                info.progress = checked as f64 / total as f64;
                info.pieces_done = verified;
            }
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
                    entry.info.status = "seeding".to_string();
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

    // Capture bytes for global stats update (before dropping lock)
    let current_dl = entry.info.downloaded_bytes;
    let current_ul = entry.info.uploaded_bytes;

    // Save on download complete — use snapshot_bitfield (only pm lock, no stats lock)
    // Stats are already up-to-date from the Progress events above.
    let mut should_save_global = false;
    if download_completed {
        if let Some(engine) = &entry.engine {
            entry.saved_bitfield = engine.snapshot_bitfield().await;
        }
        let torrent_state = build_torrent_state(entry);
        save_in_background(torrent_state, state.state_dir.clone());
        should_save_global = true;
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
            should_save_global = true;
        }
    }

    // Release torrents lock before updating global stats
    drop(torrents);

    // Update global traffic stats with deltas
    update_global_stats(&state, &id, current_dl, current_ul).await;

    // Periodically persist global stats (piggyback on torrent save interval)
    if should_save_global {
        let gs = state.global_stats.read().await.clone();
        save_global_stats_bg(gs, state.state_dir.clone());
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

#[tauri::command]
async fn get_app_version(app: tauri::AppHandle) -> Result<String, String> {
    Ok(app.package_info().version.to_string())
}

#[derive(Debug, Clone, Serialize)]
pub struct PeerInfoUI {
    pub addr: String,
    pub is_seeder: bool,
    pub pieces_have: usize,
    pub pieces_total: usize,
    pub peer_interested: bool,
    pub am_choking: bool,
    pub peer_choking: bool,
    pub client: String,
}

#[tauri::command]
async fn get_peers(state: State<'_, AppState>, id: String) -> Result<Vec<PeerInfoUI>, String> {
    let torrents = state.torrents.read().await;
    let entry = torrents.get(&id).ok_or("torrent not found")?;
    if let Some(engine) = &entry.engine {
        let peers = engine.snapshot_peers().await;
        Ok(peers.into_iter().map(|p| PeerInfoUI {
            addr: p.addr.to_string(),
            is_seeder: p.is_seeder,
            pieces_have: p.pieces_have,
            pieces_total: p.pieces_total,
            peer_interested: p.peer_interested,
            am_choking: p.am_choking,
            peer_choking: p.peer_choking,
            client: p.client,
        }).collect())
    } else {
        Ok(vec![])
    }
}

#[tauri::command]
async fn get_piece_map(state: State<'_, AppState>, id: String) -> Result<Vec<f64>, String> {
    let torrents = state.torrents.read().await;
    let entry = torrents.get(&id).ok_or("torrent not found")?;
    if let Some(engine) = &entry.engine {
        Ok(engine.snapshot_piece_progress().await)
    } else {
        // Return from saved bitfield
        let num_pieces = entry.info.num_pieces;
        let mut progress = vec![0.0f64; num_pieces];
        for i in 0..num_pieces {
            let byte_idx = i / 8;
            let bit_idx = 7 - (i % 8);
            if byte_idx < entry.saved_bitfield.len() && (entry.saved_bitfield[byte_idx] & (1 << bit_idx)) != 0 {
                progress[i] = 1.0;
            }
        }
        Ok(progress)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct GlobalStatsInfo {
    pub total_downloaded: u64,
    pub total_uploaded: u64,
    pub ratio: f64,
}

#[tauri::command]
async fn get_global_stats(state: State<'_, AppState>) -> Result<GlobalStatsInfo, String> {
    let gs = state.global_stats.read().await;
    let ratio = if gs.total_downloaded > 0 {
        gs.total_uploaded as f64 / gs.total_downloaded as f64
    } else {
        0.0
    };
    Ok(GlobalStatsInfo {
        total_downloaded: gs.total_downloaded,
        total_uploaded: gs.total_uploaded,
        ratio,
    })
}

/// Update global stats by computing deltas from last-known per-torrent bytes.
async fn update_global_stats(state: &AppState, id: &str, downloaded: u64, uploaded: u64) {
    let mut last = state.last_known_bytes.write().await;
    let (prev_dl, prev_ul) = last.get(id).copied().unwrap_or((0, 0));
    let dl_delta = downloaded.saturating_sub(prev_dl);
    let ul_delta = uploaded.saturating_sub(prev_ul);
    last.insert(id.to_string(), (downloaded, uploaded));
    drop(last);

    if dl_delta > 0 || ul_delta > 0 {
        let mut gs = state.global_stats.write().await;
        gs.total_downloaded += dl_delta;
        gs.total_uploaded += ul_delta;
    }
}

/// Save global stats in the background.
fn save_global_stats_bg(stats: GlobalStats, state_dir: PathBuf) {
    tokio::spawn(async move {
        if let Err(e) = persistence::save_global_stats(&stats, &state_dir).await {
            log::error!("Failed to save global stats: {}", e);
        }
    });
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
        .plugin(tauri_plugin_opener::init())
        .invoke_handler(tauri::generate_handler![
            add_torrent,
            add_magnet,
            start_torrent,
            stop_torrent,
            remove_torrent,
            open_download_dir,
            get_torrents,
            get_torrent_files,
            toggle_file_skip,
            poll_events,
            change_torrent_download_dir,
            set_download_dir,
            get_download_dir,
            get_global_stats,
            get_app_version,
            get_peers,
            get_piece_map,
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

                // Load global stats
                let loaded_gs = persistence::load_global_stats(&state_dir).await;
                *app_state.global_stats.write().await = loaded_gs;

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

                            let should_autostart = saved.status == "downloading" || saved.status == "complete";
                            log::info!("Torrent {} status='{}' should_autostart={}", id, saved.status, should_autostart);

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
                                status: if should_autostart && saved.status == "downloading" {
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
                                bitfield_verified: saved.bitfield_verified,
        verify_checked: Arc::new(AtomicUsize::new(0)),
        verify_total: Arc::new(AtomicUsize::new(0)),
        verify_verified: Arc::new(AtomicUsize::new(0)),
                            };

                            if should_autostart {
                                to_autostart.push(id.clone());
                            }

                            torrents.insert(id, entry);
                        }
                        // Initialize last_known_bytes so future deltas are correct
                        let mut last = app_state.last_known_bytes.write().await;
                        for (id, entry) in torrents.iter() {
                            last.insert(id.clone(), (entry.info.downloaded_bytes, entry.info.uploaded_bytes));
                        }
                        drop(last);

                        log::info!("Loaded {} torrents from disk", torrents.len());
                    }
                    Err(e) => log::error!("Failed to load torrent states: {}", e),
                }

                // Auto-start torrents: complete ones first (no verify needed),
                // then downloading ones (need expensive disk verification).
                {
                    let torrents = app_state.torrents.read().await;
                    to_autostart.sort_by_key(|id| {
                        if torrents.get(id).map(|e| e.info.status.as_str()) == Some("complete") {
                            0 // complete first
                        } else {
                            1 // downloading after
                        }
                    });
                }
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

                    // If the bitfield was already verified (saved to disk after a
                    // prior verification), trust it and skip the expensive SHA1 check.
                    // Otherwise verify all pieces so we don't seed corrupt data.
                    let needs_verify = !entry.bitfield_verified;
                    if needs_verify {
                        entry.info.status = "verifying".to_string();
                        entry.verify_checked.store(0, Ordering::Relaxed);
                        entry.verify_total.store(entry.info.num_pieces, Ordering::Relaxed);
                        entry.verify_verified.store(0, Ordering::Relaxed);
                    }
                    let vc = entry.verify_checked.clone();
                    let vt = entry.verify_total.clone();
                    let vv = entry.verify_verified.clone();
                    // Drop the write lock so UI can poll progress during verification
                    drop(torrents);

                    if needs_verify {
                        match pm.verify_pieces_with_progress(|checked, total, verified| {
                            vc.store(checked, Ordering::Relaxed);
                            vt.store(total, Ordering::Relaxed);
                            vv.store(verified, Ordering::Relaxed);
                        }).await {
                            Ok(verified) => {
                                let mut torrents = app_state.torrents.write().await;
                                let entry = match torrents.get_mut(&id) {
                                    Some(e) => e,
                                    None => continue,
                                };
                                entry.info.pieces_done = verified;
                                entry.info.progress = if entry.info.num_pieces > 0 {
                                    verified as f64 / entry.info.num_pieces as f64
                                } else {
                                    0.0
                                };
                                entry.saved_bitfield = pm.bitfield_bytes();
                                entry.bitfield_verified = true;
                            }
                            Err(e) => {
                                log::error!("Piece verification failed for {}: {}", id, e);
                                let mut torrents = app_state.torrents.write().await;
                                if let Some(entry) = torrents.get_mut(&id) {
                                    entry.info.status = "paused".to_string();
                                }
                                continue;
                            }
                        }
                    } else {
                        pm.apply_bitfield_without_verify();
                        log::info!("Skipping verification for {} — bitfield already verified", id);
                    }

                    let mut torrents = app_state.torrents.write().await;
                    let entry = match torrents.get_mut(&id) {
                        Some(e) => e,
                        None => continue,
                    };
                    let torrent_complete = entry.info.pieces_done == entry.info.num_pieces;

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
                                "seeding".to_string()
                            } else {
                                "downloading".to_string()
                            };
                            log::info!("Auto-started torrent: {} (seeding: {})", id, torrent_complete);

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
                                for (id, entry) in torrents.iter_mut() {
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
                                    // Update global stats with final values
                                    update_global_stats(app_state.inner(), id, entry.info.downloaded_bytes, entry.info.uploaded_bytes).await;
                                    let ts = build_torrent_state(entry);
                                    if let Err(e) = persistence::save_state(&ts, &state_dir).await {
                                        log::error!("Failed to save state on exit: {}", e);
                                    }
                                }
                                // Save global stats
                                let gs = app_state.global_stats.read().await.clone();
                                if let Err(e) = persistence::save_global_stats(&gs, &state_dir).await {
                                    log::error!("Failed to save global stats on exit: {}", e);
                                }
                            });
                        }
                    }));
                }
                _ => {}
            }
        });
}
