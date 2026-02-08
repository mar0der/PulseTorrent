use thiserror::Error;

#[derive(Debug, Error)]
pub enum MagnetError {
    #[error("not a magnet URI")]
    NotMagnet,
    #[error("missing info hash (xt parameter)")]
    MissingInfoHash,
    #[error("invalid info hash length: expected 40 hex chars or 32 base32 chars")]
    InvalidInfoHash,
    #[error("invalid hex character in info hash")]
    InvalidHex,
}

/// Parsed magnet link.
#[derive(Debug, Clone)]
pub struct MagnetLink {
    /// 20-byte info hash.
    pub info_hash: [u8; 20],
    /// Display name (optional).
    pub display_name: Option<String>,
    /// Tracker URLs from the magnet link.
    pub trackers: Vec<String>,
}

impl MagnetLink {
    /// Parse a magnet URI string.
    ///
    /// Supports:
    ///   magnet:?xt=urn:btih:<40 hex chars>
    ///   magnet:?xt=urn:btih:<32 base32 chars>
    pub fn parse(uri: &str) -> Result<Self, MagnetError> {
        if !uri.starts_with("magnet:?") {
            return Err(MagnetError::NotMagnet);
        }

        let query = &uri["magnet:?".len()..];
        let params = parse_query(query);

        // Extract info hash from xt=urn:btih:<hash>
        let xt = params
            .iter()
            .find(|(k, _)| k == "xt")
            .map(|(_, v)| v.as_str())
            .ok_or(MagnetError::MissingInfoHash)?;

        let hash_str = xt
            .strip_prefix("urn:btih:")
            .ok_or(MagnetError::MissingInfoHash)?;

        let info_hash = if hash_str.len() == 40 {
            // Hex-encoded
            hex_to_bytes(hash_str)?
        } else if hash_str.len() == 32 {
            // Base32-encoded
            base32_to_bytes(hash_str)?
        } else {
            return Err(MagnetError::InvalidInfoHash);
        };

        // Display name
        let display_name = params
            .iter()
            .find(|(k, _)| k == "dn")
            .map(|(_, v)| url_decode(v));

        // Tracker URLs (can appear multiple times as &tr=)
        let trackers: Vec<String> = params
            .iter()
            .filter(|(k, _)| k == "tr")
            .map(|(_, v)| url_decode(v))
            .collect();

        Ok(MagnetLink {
            info_hash,
            display_name,
            trackers,
        })
    }

    /// Info hash as a hex string.
    pub fn info_hash_hex(&self) -> String {
        self.info_hash.iter().map(|b| format!("{:02x}", b)).collect()
    }
}

fn parse_query(query: &str) -> Vec<(String, String)> {
    query
        .split('&')
        .filter_map(|pair| {
            let mut parts = pair.splitn(2, '=');
            let key = parts.next()?;
            let value = parts.next().unwrap_or("");
            Some((key.to_string(), value.to_string()))
        })
        .collect()
}

fn hex_to_bytes(hex: &str) -> Result<[u8; 20], MagnetError> {
    if hex.len() != 40 {
        return Err(MagnetError::InvalidInfoHash);
    }
    let mut bytes = [0u8; 20];
    for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
        let s = std::str::from_utf8(chunk).map_err(|_| MagnetError::InvalidHex)?;
        bytes[i] = u8::from_str_radix(s, 16).map_err(|_| MagnetError::InvalidHex)?;
    }
    Ok(bytes)
}

fn base32_to_bytes(b32: &str) -> Result<[u8; 20], MagnetError> {
    let b32 = b32.to_uppercase();
    let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

    let mut bits = Vec::with_capacity(b32.len() * 5);
    for c in b32.bytes() {
        let val = alphabet
            .iter()
            .position(|&a| a == c)
            .ok_or(MagnetError::InvalidInfoHash)?;
        for bit in (0..5).rev() {
            bits.push((val >> bit) & 1);
        }
    }

    if bits.len() < 160 {
        return Err(MagnetError::InvalidInfoHash);
    }

    let mut bytes = [0u8; 20];
    for (i, byte) in bytes.iter_mut().enumerate() {
        let offset = i * 8;
        for bit in 0..8 {
            *byte |= (bits[offset + bit] as u8) << (7 - bit);
        }
    }
    Ok(bytes)
}

fn url_decode(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.bytes();
    while let Some(b) = chars.next() {
        match b {
            b'%' => {
                let h1 = chars.next().unwrap_or(0);
                let h2 = chars.next().unwrap_or(0);
                let hex = [h1, h2];
                if let Ok(s) = std::str::from_utf8(&hex) {
                    if let Ok(val) = u8::from_str_radix(s, 16) {
                        result.push(val as char);
                        continue;
                    }
                }
                result.push('%');
                result.push(h1 as char);
                result.push(h2 as char);
            }
            b'+' => result.push(' '),
            _ => result.push(b as char),
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_magnet_hex() {
        let uri = "magnet:?xt=urn:btih:aabbccddee11223344556677889900aabbccddee&dn=Test+File&tr=http%3A%2F%2Ftracker.example.com%2Fannounce";
        let m = MagnetLink::parse(uri).unwrap();
        assert_eq!(m.info_hash_hex(), "aabbccddee11223344556677889900aabbccddee");
        assert_eq!(m.display_name.as_deref(), Some("Test File"));
        assert_eq!(m.trackers.len(), 1);
        assert_eq!(m.trackers[0], "http://tracker.example.com/announce");
    }

    #[test]
    fn test_parse_magnet_multiple_trackers() {
        let uri = "magnet:?xt=urn:btih:aabbccddee11223344556677889900aabbccddee&tr=http%3A%2F%2Ft1.com&tr=http%3A%2F%2Ft2.com";
        let m = MagnetLink::parse(uri).unwrap();
        assert_eq!(m.trackers.len(), 2);
    }

    #[test]
    fn test_parse_magnet_no_tracker() {
        let uri = "magnet:?xt=urn:btih:aabbccddee11223344556677889900aabbccddee&dn=NoTracker";
        let m = MagnetLink::parse(uri).unwrap();
        assert_eq!(m.trackers.len(), 0);
        assert_eq!(m.display_name.as_deref(), Some("NoTracker"));
    }

    #[test]
    fn test_invalid_magnet() {
        assert!(MagnetLink::parse("http://example.com").is_err());
        assert!(MagnetLink::parse("magnet:?dn=NoHash").is_err());
        assert!(MagnetLink::parse("magnet:?xt=urn:btih:short").is_err());
    }

    #[test]
    fn test_parse_magnet_base32() {
        // Base32 of 20 zero bytes = AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA (32 chars)
        let uri = "magnet:?xt=urn:btih:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
        let m = MagnetLink::parse(uri).unwrap();
        assert_eq!(m.info_hash, [0u8; 20]);
    }
}
