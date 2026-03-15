use crate::torrent::Metainfo;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use thiserror::Error;
use tokio::fs;

#[derive(Debug, Error)]
pub enum PersistenceError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Persistable state for a single torrent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TorrentState {
    /// Schema version for forward compatibility.
    pub version: u32,
    /// Full parsed metainfo (so we don't need the .torrent file again).
    pub metainfo: Metainfo,
    /// Directory where files are being downloaded.
    pub download_dir: PathBuf,
    /// Bitfield of completed pieces (raw bytes from Bitfield::as_bytes).
    pub completed_pieces: Vec<u8>,
    /// Number of pieces in this torrent.
    pub num_pieces: usize,
    /// Cumulative bytes downloaded across sessions.
    pub downloaded_bytes: u64,
    /// Cumulative bytes uploaded across sessions.
    pub uploaded_bytes: u64,
    /// Status at save time: "paused" or "complete".
    pub status: String,
    /// File indices that the user has chosen to skip.
    #[serde(default)]
    pub skipped_files: Vec<usize>,
}

/// Save torrent state to a JSON file (atomic: write tmp + rename).
pub async fn save_state(state: &TorrentState, state_dir: &Path) -> Result<(), PersistenceError> {
    fs::create_dir_all(state_dir).await?;

    let id = state.metainfo.info_hash_hex();
    let path = state_dir.join(format!("{}.json", id));
    let tmp_path = state_dir.join(format!("{}.json.tmp", id));

    let json = serde_json::to_string_pretty(state)?;
    fs::write(&tmp_path, json.as_bytes()).await?;
    fs::rename(&tmp_path, &path).await?;

    Ok(())
}

/// Load all torrent states from the state directory.
pub async fn load_all_states(state_dir: &Path) -> Result<Vec<TorrentState>, PersistenceError> {
    let mut states = Vec::new();

    if !state_dir.exists() {
        return Ok(states);
    }

    let mut entries = fs::read_dir(state_dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.extension().map_or(false, |ext| ext == "json") {
            match fs::read_to_string(&path).await {
                Ok(contents) => match serde_json::from_str::<TorrentState>(&contents) {
                    Ok(state) => states.push(state),
                    Err(e) => log::warn!("Failed to parse state file {:?}: {}", path, e),
                },
                Err(e) => log::warn!("Failed to read state file {:?}: {}", path, e),
            }
        }
    }

    Ok(states)
}

/// Delete a torrent's state file.
pub async fn delete_state(info_hash_hex: &str, state_dir: &Path) -> Result<(), PersistenceError> {
    let path = state_dir.join(format!("{}.json", info_hash_hex));
    if path.exists() {
        fs::remove_file(&path).await?;
    }
    Ok(())
}

/// Global client-wide traffic statistics.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GlobalStats {
    /// Total bytes downloaded across all torrents, all time.
    pub total_downloaded: u64,
    /// Total bytes uploaded across all torrents, all time.
    pub total_uploaded: u64,
}

/// Save global stats to `global_stats.json` (atomic: write tmp + rename).
pub async fn save_global_stats(stats: &GlobalStats, state_dir: &Path) -> Result<(), PersistenceError> {
    fs::create_dir_all(state_dir).await?;
    let path = state_dir.join("global_stats.json");
    let tmp_path = state_dir.join("global_stats.json.tmp");
    let json = serde_json::to_string_pretty(stats)?;
    fs::write(&tmp_path, json.as_bytes()).await?;
    fs::rename(&tmp_path, &path).await?;
    Ok(())
}

/// Load global stats from `global_stats.json`, returning defaults if missing.
pub async fn load_global_stats(state_dir: &Path) -> GlobalStats {
    let path = state_dir.join("global_stats.json");
    match fs::read_to_string(&path).await {
        Ok(contents) => serde_json::from_str(&contents).unwrap_or_default(),
        Err(_) => GlobalStats::default(),
    }
}
