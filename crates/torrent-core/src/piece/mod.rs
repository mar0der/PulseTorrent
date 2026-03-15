use crate::peer::Bitfield;
use crate::peer::STANDARD_BLOCK_SIZE;
use crate::torrent::Metainfo;
use sha1::{Digest, Sha1};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use thiserror::Error;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

#[derive(Debug, Error)]
pub enum PieceError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("SHA1 hash mismatch for piece {0}")]
    HashMismatch(usize),
    #[error("invalid piece index: {0}")]
    InvalidIndex(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PieceStatus {
    Missing,
    InProgress,
    Complete,
}

#[derive(Debug, Clone)]
pub struct BlockRequest {
    pub piece_index: u32,
    pub offset: u32,
    pub length: u32,
}

/// Tracks in-progress piece downloads.
#[derive(Debug)]
struct PieceWork {
    data: Vec<u8>,
    blocks_received: Vec<bool>,
    blocks_total: usize,
}

/// Manages piece selection, block tracking, and disk I/O.
pub struct PieceManager {
    metainfo: Metainfo,
    download_dir: PathBuf,
    piece_status: Vec<PieceStatus>,
    in_progress: HashMap<usize, PieceWork>,
    our_bitfield: Bitfield,
    skipped_pieces: HashSet<usize>,
}

impl PieceManager {
    pub fn new(metainfo: Metainfo, download_dir: PathBuf) -> Self {
        let num_pieces = metainfo.num_pieces();
        Self {
            piece_status: vec![PieceStatus::Missing; num_pieces],
            in_progress: HashMap::new(),
            our_bitfield: Bitfield::new(num_pieces),
            skipped_pieces: HashSet::new(),
            metainfo,
            download_dir,
        }
    }

    pub fn our_bitfield(&self) -> &Bitfield {
        &self.our_bitfield
    }

    pub fn is_complete(&self) -> bool {
        self.piece_status.iter().enumerate().all(|(i, s)| {
            *s == PieceStatus::Complete || self.skipped_pieces.contains(&i)
        })
    }

    pub fn completed_pieces(&self) -> usize {
        self.piece_status
            .iter()
            .filter(|s| **s == PieceStatus::Complete)
            .count()
    }

    pub fn total_pieces(&self) -> usize {
        self.metainfo.num_pieces()
    }

    /// Pick the next piece to request from a peer using rarest-first.
    /// Returns None if the peer has nothing we need.
    pub fn pick_piece(
        &self,
        peer_bitfield: &Bitfield,
        peer_availability: &[u32],
    ) -> Option<usize> {
        let mut candidates: Vec<(usize, u32)> = (0..self.metainfo.num_pieces())
            .filter(|&i| {
                self.piece_status[i] == PieceStatus::Missing
                    && peer_bitfield.has_piece(i)
                    && !self.skipped_pieces.contains(&i)
            })
            .map(|i| (i, peer_availability.get(i).copied().unwrap_or(0)))
            .collect();

        if candidates.is_empty() {
            return None;
        }

        // Sort by rarity (ascending availability count)
        candidates.sort_by_key(|&(_, count)| count);

        // Pick randomly among the rarest
        let min_count = candidates[0].1;
        let rarest: Vec<_> = candidates
            .iter()
            .filter(|&&(_, count)| count == min_count)
            .collect();

        let idx = rand::random::<usize>() % rarest.len();
        Some(rarest[idx].0)
    }

    /// Generate block requests for a piece.
    /// If the piece has partial data from a previous peer, only the missing blocks are requested.
    pub fn start_piece(&mut self, piece_index: usize) -> Vec<BlockRequest> {
        let piece_size = self.metainfo.piece_size(piece_index) as u32;
        let num_blocks = (piece_size + STANDARD_BLOCK_SIZE - 1) / STANDARD_BLOCK_SIZE;

        // Reuse existing work if available (peer disconnected mid-piece)
        if !self.in_progress.contains_key(&piece_index) {
            let work = PieceWork {
                data: vec![0u8; piece_size as usize],
                blocks_received: vec![false; num_blocks as usize],
                blocks_total: num_blocks as usize,
            };
            self.in_progress.insert(piece_index, work);
        }

        self.piece_status[piece_index] = PieceStatus::InProgress;

        // Only request blocks we haven't received yet
        let work = self.in_progress.get(&piece_index).unwrap();
        (0..num_blocks)
            .filter(|&i| !work.blocks_received[i as usize])
            .map(|i| {
                let offset = i * STANDARD_BLOCK_SIZE;
                let length = std::cmp::min(STANDARD_BLOCK_SIZE, piece_size - offset);
                BlockRequest {
                    piece_index: piece_index as u32,
                    offset,
                    length,
                }
            })
            .collect()
    }

    /// Store a received block. Returns true if the piece is now complete.
    pub fn receive_block(
        &mut self,
        piece_index: usize,
        offset: u32,
        data: &[u8],
    ) -> Result<bool, PieceError> {
        let work = self
            .in_progress
            .get_mut(&piece_index)
            .ok_or(PieceError::InvalidIndex(piece_index))?;

        let block_index = offset as usize / STANDARD_BLOCK_SIZE as usize;
        if block_index >= work.blocks_total {
            return Err(PieceError::InvalidIndex(piece_index));
        }

        // Copy block data
        let start = offset as usize;
        let end = start + data.len();
        if end > work.data.len() {
            return Err(PieceError::InvalidIndex(piece_index));
        }
        work.data[start..end].copy_from_slice(data);
        work.blocks_received[block_index] = true;

        // Check if all blocks received
        let complete = work.blocks_received.iter().all(|&b| b);
        Ok(complete)
    }

    /// Verify piece hash and write to disk. Returns Ok(true) if valid.
    pub async fn finalize_piece(&mut self, piece_index: usize) -> Result<bool, PieceError> {
        let work = self
            .in_progress
            .remove(&piece_index)
            .ok_or(PieceError::InvalidIndex(piece_index))?;

        // Verify SHA1 hash
        let hash: [u8; 20] = Sha1::digest(&work.data).into();
        if hash != self.metainfo.pieces[piece_index] {
            // Hash mismatch — mark as missing again
            self.piece_status[piece_index] = PieceStatus::Missing;
            log::warn!("Piece {} hash mismatch, re-downloading", piece_index);
            return Ok(false);
        }

        // Write to disk
        self.write_piece(piece_index, &work.data).await?;

        self.piece_status[piece_index] = PieceStatus::Complete;
        self.our_bitfield.set_piece(piece_index);

        log::info!(
            "Piece {} complete ({}/{})",
            piece_index,
            self.completed_pieces(),
            self.total_pieces()
        );

        Ok(true)
    }

    /// Write piece data to the correct file(s) on disk.
    async fn write_piece(&self, piece_index: usize, data: &[u8]) -> Result<(), PieceError> {
        let piece_offset = piece_index as u64 * self.metainfo.piece_length;
        let mut data_offset = 0usize;
        let mut file_offset_acc = 0u64;

        for file_info in &self.metainfo.files {
            let file_start = file_offset_acc;
            let file_end = file_start + file_info.length;
            file_offset_acc = file_end;

            // Check if this piece overlaps with this file
            let piece_start = piece_offset + data_offset as u64;
            let piece_end = piece_offset + data.len() as u64;

            if piece_start >= file_end || piece_end <= file_start {
                continue;
            }

            // Calculate the overlap
            let write_start = std::cmp::max(piece_start, file_start);
            let write_end = std::cmp::min(piece_end, file_end);
            let write_len = (write_end - write_start) as usize;

            let file_write_offset = write_start - file_start;
            let data_read_offset = (write_start - piece_offset) as usize;

            let file_path = self.download_dir.join(&file_info.path);

            // Ensure parent directory exists
            if let Some(parent) = file_path.parent() {
                fs::create_dir_all(parent).await?;
            }

            // Open file and write at the correct offset
            let mut file = fs::OpenOptions::new()
                .create(true)
                .write(true)
                .open(&file_path)
                .await?;

            file.seek(std::io::SeekFrom::Start(file_write_offset)).await?;
            file.write_all(&data[data_read_offset..data_read_offset + write_len])
                .await?;

            data_offset += write_len;
        }

        Ok(())
    }

    /// Reset a piece back to missing (e.g. after peer disconnect).
    /// Keeps partial block data so the next peer can resume where the last left off.
    pub fn reset_piece(&mut self, piece_index: usize) {
        // Do NOT remove in_progress data — start_piece will reuse it
        if piece_index < self.piece_status.len() {
            self.piece_status[piece_index] = PieceStatus::Missing;
        }
    }

    /// Create a PieceManager with a saved bitfield hint from a previous session.
    /// The bitfield is NOT trusted; `verify_pieces()` must be called before downloading.
    pub fn with_saved_bitfield(
        metainfo: Metainfo,
        download_dir: PathBuf,
        saved_bitfield_bytes: Vec<u8>,
        num_pieces: usize,
    ) -> Self {
        Self {
            piece_status: vec![PieceStatus::Missing; num_pieces],
            in_progress: HashMap::new(),
            our_bitfield: Bitfield::from_bytes(saved_bitfield_bytes, num_pieces),
            skipped_pieces: HashSet::new(),
            metainfo,
            download_dir,
        }
    }

    /// Set piece indices that should be skipped during download.
    pub fn set_skipped_pieces(&mut self, skipped: HashSet<usize>) {
        self.skipped_pieces = skipped;
    }

    /// Get the current set of skipped piece indices.
    pub fn skipped_pieces(&self) -> &HashSet<usize> {
        &self.skipped_pieces
    }

    /// Trust the saved bitfield without re-reading from disk.
    /// Use only when pieces were already verified in the current session.
    /// Returns the number of pieces marked complete.
    pub fn apply_bitfield_without_verify(&mut self) -> usize {
        let mut count = 0;
        for i in 0..self.metainfo.num_pieces() {
            if self.our_bitfield.has_piece(i) {
                self.piece_status[i] = PieceStatus::Complete;
                count += 1;
            }
        }
        count
    }

    /// Verify which pieces on disk match their expected SHA1 hashes.
    /// Rebuilds `piece_status` and `our_bitfield` from reality.
    /// Returns the number of verified-good pieces.
    pub async fn verify_pieces(&mut self) -> Result<usize, PieceError> {
        let num_pieces = self.metainfo.num_pieces();
        let saved_bitfield = self.our_bitfield.clone();

        // Reset bitfield — rebuild from verified data
        self.our_bitfield = Bitfield::new(num_pieces);
        let mut verified = 0;

        for piece_index in 0..num_pieces {
            // Only verify pieces the saved bitfield claims are complete
            if !saved_bitfield.has_piece(piece_index) {
                continue;
            }

            let piece_data = match self.read_piece(piece_index).await {
                Ok(data) => data,
                Err(_) => continue,
            };

            let expected_size = self.metainfo.piece_size(piece_index) as usize;
            if piece_data.len() != expected_size {
                continue;
            }

            let hash: [u8; 20] = Sha1::digest(&piece_data).into();
            if hash == self.metainfo.pieces[piece_index] {
                self.piece_status[piece_index] = PieceStatus::Complete;
                self.our_bitfield.set_piece(piece_index);
                verified += 1;
            }
        }

        log::info!("Piece verification: {}/{} pieces valid", verified, num_pieces);
        Ok(verified)
    }

    /// Read a block from a completed piece on disk. Returns the block data.
    /// Used by the upload path to serve blocks to peers.
    pub async fn read_block(
        &self,
        piece_index: usize,
        offset: u32,
        length: u32,
    ) -> Result<Vec<u8>, PieceError> {
        if piece_index >= self.metainfo.num_pieces() {
            return Err(PieceError::InvalidIndex(piece_index));
        }
        if !self.our_bitfield.has_piece(piece_index) {
            return Err(PieceError::InvalidIndex(piece_index));
        }
        let piece_data = self.read_piece(piece_index).await?;
        let start = offset as usize;
        let end = start + length as usize;
        if end > piece_data.len() {
            return Err(PieceError::InvalidIndex(piece_index));
        }
        Ok(piece_data[start..end].to_vec())
    }

    /// Read a piece from disk by reading the correct byte ranges from the appropriate file(s).
    async fn read_piece(&self, piece_index: usize) -> Result<Vec<u8>, PieceError> {
        let piece_size = self.metainfo.piece_size(piece_index) as usize;
        let piece_offset = piece_index as u64 * self.metainfo.piece_length;
        let mut data = vec![0u8; piece_size];
        let mut file_offset_acc = 0u64;

        for file_info in &self.metainfo.files {
            let file_start = file_offset_acc;
            let file_end = file_start + file_info.length;
            file_offset_acc = file_end;

            let piece_start = piece_offset;
            let piece_end = piece_offset + piece_size as u64;

            if piece_start >= file_end || piece_end <= file_start {
                continue;
            }

            let read_start = std::cmp::max(piece_start, file_start);
            let read_end = std::cmp::min(piece_end, file_end);
            let read_len = (read_end - read_start) as usize;
            let file_read_offset = read_start - file_start;
            let data_read_offset = (read_start - piece_offset) as usize;

            let file_path = self.download_dir.join(&file_info.path);
            let mut file = fs::File::open(&file_path).await?;
            file.seek(std::io::SeekFrom::Start(file_read_offset)).await?;
            file.read_exact(&mut data[data_read_offset..data_read_offset + read_len])
                .await?;
        }

        Ok(data)
    }

    /// Return the number of pieces that are skipped.
    pub fn skipped_count(&self) -> usize {
        self.skipped_pieces.len()
    }

    /// Get the raw bitfield bytes for persistence.
    pub fn bitfield_bytes(&self) -> Vec<u8> {
        self.our_bitfield.as_bytes().to_vec()
    }

    /// Per-piece progress fractions (0.0 = missing, 0.x = partial, 1.0 = complete).
    /// Accounts for partially-downloaded blocks in in-progress pieces.
    pub fn piece_progress_fractions(&self) -> Vec<f64> {
        (0..self.metainfo.num_pieces())
            .map(|i| match self.piece_status[i] {
                PieceStatus::Complete => 1.0,
                PieceStatus::InProgress => self.in_progress.get(&i).map_or(0.0, |work| {
                    let received = work.blocks_received.iter().filter(|&&b| b).count();
                    received as f64 / work.blocks_total.max(1) as f64
                }),
                PieceStatus::Missing => 0.0,
            })
            .collect()
    }
}

/// Pre-allocate files on disk at their full size. Standalone function that does NOT
/// require holding the PieceManager lock, so downloads can proceed in parallel.
pub async fn preallocate_files(
    metainfo: &Metainfo,
    download_dir: &std::path::Path,
    skipped_files: &HashSet<usize>,
) -> Result<(), PieceError> {
    for (file_idx, file_info) in metainfo.files.iter().enumerate() {
        if skipped_files.contains(&file_idx) {
            continue;
        }
        allocate_single_file(download_dir, file_info).await?;
    }
    Ok(())
}

/// Pre-allocate a single file by index. Standalone — no PieceManager lock needed.
pub async fn preallocate_file_by_index(
    metainfo: &Metainfo,
    download_dir: &std::path::Path,
    file_index: usize,
) -> Result<(), PieceError> {
    if let Some(file_info) = metainfo.files.get(file_index) {
        allocate_single_file(download_dir, file_info).await?;
    }
    Ok(())
}

async fn allocate_single_file(
    download_dir: &std::path::Path,
    file_info: &crate::torrent::FileInfo,
) -> Result<(), PieceError> {
    let file_path = download_dir.join(&file_info.path);

    if let Some(parent) = file_path.parent() {
        fs::create_dir_all(parent).await?;
    }

    let needs_alloc = match fs::metadata(&file_path).await {
        Ok(meta) => meta.len() < file_info.length,
        Err(_) => true,
    };

    if needs_alloc {
        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&file_path)
            .await?;
        file.set_len(file_info.length).await?;
        log::info!("Pre-allocated: {} ({} bytes)", file_path.display(), file_info.length);
    }
    Ok(())
}
