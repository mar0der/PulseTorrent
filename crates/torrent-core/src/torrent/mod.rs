use crate::bencode;
use sha1::{Digest, Sha1};
use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TorrentError {
    #[error("failed to decode bencode: {0}")]
    BencodeDecode(String),
    #[error("missing required field: {0}")]
    MissingField(&'static str),
    #[error("invalid field type for: {0}")]
    InvalidField(&'static str),
    #[error("invalid pieces length (not multiple of 20)")]
    InvalidPieces,
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FileInfo {
    pub path: PathBuf,
    pub length: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Metainfo {
    /// Tracker URL.
    pub announce: String,
    /// Optional list of tracker tiers.
    pub announce_list: Option<Vec<Vec<String>>>,
    /// 20-byte SHA1 hash of the bencoded info dictionary.
    pub info_hash: [u8; 20],
    /// Suggested name (file or directory).
    pub name: String,
    /// Number of bytes per piece.
    pub piece_length: u64,
    /// SHA1 hashes of each piece (20 bytes each).
    pub pieces: Vec<[u8; 20]>,
    /// Files in this torrent.
    pub files: Vec<FileInfo>,
    /// Total size in bytes.
    pub total_size: u64,
    /// Raw bencoded info dict (needed for tracker requests).
    pub info_hash_bytes: Vec<u8>,
}

impl Metainfo {
    /// Parse a .torrent file from raw bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self, TorrentError> {
        let (value, _) =
            bencode::decode(data).map_err(|e| TorrentError::BencodeDecode(e.to_string()))?;

        let dict = value.as_dict().ok_or(TorrentError::InvalidField("root"))?;

        // Announce URL
        let announce = value
            .get("announce")
            .and_then(|v| v.as_str())
            .ok_or(TorrentError::MissingField("announce"))?
            .to_string();

        // Optional announce-list
        let announce_list = value.get("announce-list").and_then(|v| {
            v.as_list().map(|tiers| {
                tiers
                    .iter()
                    .filter_map(|tier| {
                        tier.as_list().map(|urls| {
                            urls.iter()
                                .filter_map(|u| u.as_str().map(String::from))
                                .collect()
                        })
                    })
                    .collect()
            })
        });

        // Info dictionary
        let info_value = dict
            .get(b"info".as_slice())
            .ok_or(TorrentError::MissingField("info"))?;
        let _info = info_value
            .as_dict()
            .ok_or(TorrentError::InvalidField("info"))?;

        // Compute info_hash from the raw bencoded info dict
        let info_encoded = bencode::encode(info_value);
        let info_hash: [u8; 20] = Sha1::digest(&info_encoded).into();

        // Name
        let name = info_value
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or(TorrentError::MissingField("info.name"))?
            .to_string();

        // Piece length
        let piece_length = info_value
            .get("piece length")
            .and_then(|v| v.as_int())
            .ok_or(TorrentError::MissingField("info.piece length"))?
            as u64;

        // Pieces (concatenated 20-byte SHA1 hashes)
        let pieces_raw = info_value
            .get("pieces")
            .and_then(|v| v.as_bytes())
            .ok_or(TorrentError::MissingField("info.pieces"))?;

        if pieces_raw.len() % 20 != 0 {
            return Err(TorrentError::InvalidPieces);
        }

        let pieces: Vec<[u8; 20]> = pieces_raw
            .chunks_exact(20)
            .map(|chunk| {
                let mut hash = [0u8; 20];
                hash.copy_from_slice(chunk);
                hash
            })
            .collect();

        // Files - single file or multi-file mode
        let (files, total_size) = if let Some(length) = info_value.get("length").and_then(|v| v.as_int()) {
            // Single file mode
            let length = length as u64;
            (
                vec![FileInfo {
                    path: PathBuf::from(&name),
                    length,
                }],
                length,
            )
        } else if let Some(file_list) = info_value.get("files").and_then(|v| v.as_list()) {
            // Multi-file mode
            let mut files = Vec::new();
            let mut total = 0u64;

            for file_value in file_list {
                let length = file_value
                    .get("length")
                    .and_then(|v| v.as_int())
                    .ok_or(TorrentError::MissingField("files[].length"))?
                    as u64;

                let path_parts = file_value
                    .get("path")
                    .and_then(|v| v.as_list())
                    .ok_or(TorrentError::MissingField("files[].path"))?;

                let mut path = PathBuf::from(&name);
                for part in path_parts {
                    if let Some(s) = part.as_str() {
                        path.push(s);
                    }
                }

                total += length;
                files.push(FileInfo { path, length });
            }

            (files, total)
        } else {
            return Err(TorrentError::MissingField("info.length or info.files"));
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
            info_hash_bytes: info_encoded,
        })
    }

    /// Load a .torrent file from disk.
    pub fn from_file(path: &std::path::Path) -> Result<Self, TorrentError> {
        let data = std::fs::read(path)?;
        Self::from_bytes(&data)
    }

    /// Number of pieces in this torrent.
    pub fn num_pieces(&self) -> usize {
        self.pieces.len()
    }

    /// Size of a specific piece (last piece may be smaller).
    pub fn piece_size(&self, index: usize) -> u64 {
        if index == self.pieces.len() - 1 {
            let remainder = self.total_size % self.piece_length;
            if remainder == 0 {
                self.piece_length
            } else {
                remainder
            }
        } else {
            self.piece_length
        }
    }

    /// Info hash as a hex string.
    pub fn info_hash_hex(&self) -> String {
        self.info_hash.iter().map(|b| format!("{:02x}", b)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bencode::{self, Value};
    use std::collections::BTreeMap;

    fn make_single_file_torrent() -> Vec<u8> {
        let mut info = BTreeMap::new();
        info.insert(b"name".to_vec(), Value::Bytes(b"test.txt".to_vec()));
        info.insert(b"piece length".to_vec(), Value::Int(262144));
        // 1 piece = 20 bytes of SHA1
        info.insert(b"pieces".to_vec(), Value::Bytes(vec![0u8; 20]));
        info.insert(b"length".to_vec(), Value::Int(1024));

        let mut root = BTreeMap::new();
        root.insert(
            b"announce".to_vec(),
            Value::Bytes(b"http://tracker.example.com/announce".to_vec()),
        );
        root.insert(b"info".to_vec(), Value::Dict(info));

        bencode::encode(&Value::Dict(root))
    }

    #[test]
    fn test_parse_single_file() {
        let data = make_single_file_torrent();
        let meta = Metainfo::from_bytes(&data).unwrap();
        assert_eq!(meta.name, "test.txt");
        assert_eq!(meta.total_size, 1024);
        assert_eq!(meta.piece_length, 262144);
        assert_eq!(meta.pieces.len(), 1);
        assert_eq!(meta.files.len(), 1);
        assert_eq!(meta.info_hash.len(), 20);
    }

    #[test]
    fn test_parse_multi_file() {
        let mut files = Vec::new();

        let mut f1 = BTreeMap::new();
        f1.insert(b"length".to_vec(), Value::Int(500));
        f1.insert(
            b"path".to_vec(),
            Value::List(vec![Value::Bytes(b"file1.txt".to_vec())]),
        );
        files.push(Value::Dict(f1));

        let mut f2 = BTreeMap::new();
        f2.insert(b"length".to_vec(), Value::Int(700));
        f2.insert(
            b"path".to_vec(),
            Value::List(vec![
                Value::Bytes(b"subdir".to_vec()),
                Value::Bytes(b"file2.txt".to_vec()),
            ]),
        );
        files.push(Value::Dict(f2));

        let mut info = BTreeMap::new();
        info.insert(b"name".to_vec(), Value::Bytes(b"mydir".to_vec()));
        info.insert(b"piece length".to_vec(), Value::Int(262144));
        info.insert(b"pieces".to_vec(), Value::Bytes(vec![0u8; 20]));
        info.insert(b"files".to_vec(), Value::List(files));

        let mut root = BTreeMap::new();
        root.insert(
            b"announce".to_vec(),
            Value::Bytes(b"http://tracker.example.com/announce".to_vec()),
        );
        root.insert(b"info".to_vec(), Value::Dict(info));

        let data = bencode::encode(&Value::Dict(root));
        let meta = Metainfo::from_bytes(&data).unwrap();

        assert_eq!(meta.name, "mydir");
        assert_eq!(meta.total_size, 1200);
        assert_eq!(meta.files.len(), 2);
        assert_eq!(meta.files[1].path.to_str().unwrap(), "mydir/subdir/file2.txt");
    }
}
