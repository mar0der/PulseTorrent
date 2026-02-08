pub mod protocol;

pub use protocol::{Handshake, Message, PeerConnection, PeerError, STANDARD_BLOCK_SIZE};

use std::net::SocketAddrV4;

/// Bitfield tracking which pieces a peer has.
#[derive(Debug, Clone)]
pub struct Bitfield {
    bytes: Vec<u8>,
    num_pieces: usize,
}

impl Bitfield {
    pub fn new(num_pieces: usize) -> Self {
        let num_bytes = (num_pieces + 7) / 8;
        Self {
            bytes: vec![0u8; num_bytes],
            num_pieces,
        }
    }

    pub fn from_bytes(bytes: Vec<u8>, num_pieces: usize) -> Self {
        Self { bytes, num_pieces }
    }

    pub fn has_piece(&self, index: usize) -> bool {
        if index >= self.num_pieces {
            return false;
        }
        let byte_index = index / 8;
        let bit_index = 7 - (index % 8);
        self.bytes.get(byte_index).map_or(false, |b| b & (1 << bit_index) != 0)
    }

    pub fn set_piece(&mut self, index: usize) {
        if index >= self.num_pieces {
            return;
        }
        let byte_index = index / 8;
        let bit_index = 7 - (index % 8);
        if let Some(b) = self.bytes.get_mut(byte_index) {
            *b |= 1 << bit_index;
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn count_pieces(&self) -> usize {
        (0..self.num_pieces).filter(|&i| self.has_piece(i)).count()
    }
}

/// Represents a connected peer with state.
#[derive(Debug)]
pub struct PeerState {
    pub addr: SocketAddrV4,
    pub connection: PeerConnection,
    pub bitfield: Bitfield,
    /// We are choking this peer (not uploading to them).
    pub am_choking: bool,
    /// We are interested in this peer's pieces.
    pub am_interested: bool,
    /// This peer is choking us (not uploading to us).
    pub peer_choking: bool,
    /// This peer is interested in our pieces.
    pub peer_interested: bool,
    /// Bytes downloaded from this peer.
    pub downloaded: u64,
    /// Bytes uploaded to this peer.
    pub uploaded: u64,
}

impl PeerState {
    pub fn new(addr: SocketAddrV4, connection: PeerConnection, num_pieces: usize) -> Self {
        Self {
            addr,
            connection,
            bitfield: Bitfield::new(num_pieces),
            am_choking: true,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
            downloaded: 0,
            uploaded: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitfield() {
        let mut bf = Bitfield::new(10);
        assert!(!bf.has_piece(0));
        bf.set_piece(0);
        assert!(bf.has_piece(0));
        bf.set_piece(9);
        assert!(bf.has_piece(9));
        assert!(!bf.has_piece(5));
        assert_eq!(bf.count_pieces(), 2);
    }

    #[test]
    fn test_bitfield_from_bytes() {
        // 0b10100000 = piece 0 and 2 are set
        let bf = Bitfield::from_bytes(vec![0b10100000], 8);
        assert!(bf.has_piece(0));
        assert!(!bf.has_piece(1));
        assert!(bf.has_piece(2));
    }
}
