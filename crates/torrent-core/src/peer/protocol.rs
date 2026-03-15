use bytes::{Buf, BytesMut};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const PROTOCOL_STRING: &[u8] = b"BitTorrent protocol";
const BLOCK_SIZE: u32 = 16384; // 16 KiB standard block size

#[derive(Debug, Error)]
pub enum PeerError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid handshake")]
    InvalidHandshake,
    #[error("info hash mismatch")]
    InfoHashMismatch,
    #[error("invalid message")]
    InvalidMessage,
    #[error("connection timed out")]
    Timeout,
}

#[derive(Debug, Clone)]
pub struct Handshake {
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
}

impl Handshake {
    pub fn new(info_hash: [u8; 20], peer_id: [u8; 20]) -> Self {
        Self { info_hash, peer_id }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(68);
        buf.push(19); // protocol string length
        buf.extend_from_slice(PROTOCOL_STRING);
        let mut reserved = [0u8; 8];
        reserved[5] |= 0x10; // BEP 10: extension protocol support
        buf.extend_from_slice(&reserved);
        buf.extend_from_slice(&self.info_hash);
        buf.extend_from_slice(&self.peer_id);
        buf
    }

    /// Check if the peer's handshake indicates extension protocol support (BEP 10).
    pub fn supports_extensions(handshake_bytes: &[u8]) -> bool {
        handshake_bytes.len() >= 28 && (handshake_bytes[25] & 0x10) != 0
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self, PeerError> {
        if data.len() != 68 {
            return Err(PeerError::InvalidHandshake);
        }
        if data[0] != 19 || &data[1..20] != PROTOCOL_STRING {
            return Err(PeerError::InvalidHandshake);
        }
        let mut info_hash = [0u8; 20];
        info_hash.copy_from_slice(&data[28..48]);
        let mut peer_id = [0u8; 20];
        peer_id.copy_from_slice(&data[48..68]);
        Ok(Self { info_hash, peer_id })
    }
}

/// Peer wire protocol messages.
#[derive(Debug, Clone)]
pub enum Message {
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have(u32),
    Bitfield(Vec<u8>),
    Request {
        index: u32,
        begin: u32,
        length: u32,
    },
    Piece {
        index: u32,
        begin: u32,
        block: Vec<u8>,
    },
    Cancel {
        index: u32,
        begin: u32,
        length: u32,
    },
    /// BEP 10 extension message. ext_id=0 is the extension handshake.
    Extended {
        ext_id: u8,
        payload: Vec<u8>,
    },
}

impl Message {
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Message::KeepAlive => vec![0, 0, 0, 0],
            Message::Choke => {
                let mut buf = Vec::with_capacity(5);
                buf.extend_from_slice(&1u32.to_be_bytes());
                buf.push(0);
                buf
            }
            Message::Unchoke => {
                let mut buf = Vec::with_capacity(5);
                buf.extend_from_slice(&1u32.to_be_bytes());
                buf.push(1);
                buf
            }
            Message::Interested => {
                let mut buf = Vec::with_capacity(5);
                buf.extend_from_slice(&1u32.to_be_bytes());
                buf.push(2);
                buf
            }
            Message::NotInterested => {
                let mut buf = Vec::with_capacity(5);
                buf.extend_from_slice(&1u32.to_be_bytes());
                buf.push(3);
                buf
            }
            Message::Have(index) => {
                let mut buf = Vec::with_capacity(9);
                buf.extend_from_slice(&5u32.to_be_bytes());
                buf.push(4);
                buf.extend_from_slice(&index.to_be_bytes());
                buf
            }
            Message::Bitfield(data) => {
                let len = 1 + data.len() as u32;
                let mut buf = Vec::with_capacity(5 + data.len());
                buf.extend_from_slice(&len.to_be_bytes());
                buf.push(5);
                buf.extend_from_slice(data);
                buf
            }
            Message::Request {
                index,
                begin,
                length,
            } => {
                let mut buf = Vec::with_capacity(17);
                buf.extend_from_slice(&13u32.to_be_bytes());
                buf.push(6);
                buf.extend_from_slice(&index.to_be_bytes());
                buf.extend_from_slice(&begin.to_be_bytes());
                buf.extend_from_slice(&length.to_be_bytes());
                buf
            }
            Message::Piece {
                index,
                begin,
                block,
            } => {
                let len = 9 + block.len() as u32;
                let mut buf = Vec::with_capacity(4 + len as usize);
                buf.extend_from_slice(&len.to_be_bytes());
                buf.push(7);
                buf.extend_from_slice(&index.to_be_bytes());
                buf.extend_from_slice(&begin.to_be_bytes());
                buf.extend_from_slice(block);
                buf
            }
            Message::Cancel {
                index,
                begin,
                length,
            } => {
                let mut buf = Vec::with_capacity(17);
                buf.extend_from_slice(&13u32.to_be_bytes());
                buf.push(8);
                buf.extend_from_slice(&index.to_be_bytes());
                buf.extend_from_slice(&begin.to_be_bytes());
                buf.extend_from_slice(&length.to_be_bytes());
                buf
            }
            Message::Extended { ext_id, payload } => {
                let len = 2 + payload.len() as u32;
                let mut buf = Vec::with_capacity(4 + len as usize);
                buf.extend_from_slice(&len.to_be_bytes());
                buf.push(20); // extension message id
                buf.push(*ext_id);
                buf.extend_from_slice(payload);
                buf
            }
        }
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self, PeerError> {
        if data.is_empty() {
            return Ok(Message::KeepAlive);
        }

        let id = data[0];
        let payload = &data[1..];

        match id {
            0 => Ok(Message::Choke),
            1 => Ok(Message::Unchoke),
            2 => Ok(Message::Interested),
            3 => Ok(Message::NotInterested),
            4 => {
                if payload.len() < 4 {
                    return Err(PeerError::InvalidMessage);
                }
                let index = u32::from_be_bytes(payload[..4].try_into().unwrap());
                Ok(Message::Have(index))
            }
            5 => Ok(Message::Bitfield(payload.to_vec())),
            6 => {
                if payload.len() < 12 {
                    return Err(PeerError::InvalidMessage);
                }
                Ok(Message::Request {
                    index: u32::from_be_bytes(payload[..4].try_into().unwrap()),
                    begin: u32::from_be_bytes(payload[4..8].try_into().unwrap()),
                    length: u32::from_be_bytes(payload[8..12].try_into().unwrap()),
                })
            }
            7 => {
                if payload.len() < 8 {
                    return Err(PeerError::InvalidMessage);
                }
                Ok(Message::Piece {
                    index: u32::from_be_bytes(payload[..4].try_into().unwrap()),
                    begin: u32::from_be_bytes(payload[4..8].try_into().unwrap()),
                    block: payload[8..].to_vec(),
                })
            }
            8 => {
                if payload.len() < 12 {
                    return Err(PeerError::InvalidMessage);
                }
                Ok(Message::Cancel {
                    index: u32::from_be_bytes(payload[..4].try_into().unwrap()),
                    begin: u32::from_be_bytes(payload[4..8].try_into().unwrap()),
                    length: u32::from_be_bytes(payload[8..12].try_into().unwrap()),
                })
            }
            20 => {
                if payload.is_empty() {
                    return Err(PeerError::InvalidMessage);
                }
                Ok(Message::Extended {
                    ext_id: payload[0],
                    payload: payload[1..].to_vec(),
                })
            }
            _ => Err(PeerError::InvalidMessage),
        }
    }
}

/// A TCP connection to a peer with message framing.
#[derive(Debug)]
pub struct PeerConnection {
    stream: TcpStream,
    read_buf: BytesMut,
}

impl PeerConnection {
    /// Connect to a peer and perform the handshake.
    pub async fn connect(
        addr: std::net::SocketAddrV4,
        info_hash: [u8; 20],
        peer_id: [u8; 20],
    ) -> Result<(Self, Handshake), PeerError> {
        Self::connect_with_timeout(
            addr,
            info_hash,
            peer_id,
            std::time::Duration::from_secs(10),
        )
        .await
    }

    /// Connect with a custom timeout.
    pub async fn connect_with_timeout(
        addr: std::net::SocketAddrV4,
        info_hash: [u8; 20],
        peer_id: [u8; 20],
        timeout: std::time::Duration,
    ) -> Result<(Self, Handshake), PeerError> {
        let stream = tokio::time::timeout(timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| PeerError::Timeout)?
            .map_err(PeerError::Io)?;

        let mut conn = Self {
            stream,
            read_buf: BytesMut::with_capacity(65536),
        };

        // Send our handshake
        let handshake = Handshake::new(info_hash, peer_id);
        conn.stream
            .write_all(&handshake.to_bytes())
            .await
            .map_err(PeerError::Io)?;

        // Read their handshake
        let mut handshake_buf = [0u8; 68];
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            conn.stream.read_exact(&mut handshake_buf),
        )
        .await
        .map_err(|_| PeerError::Timeout)?
        .map_err(PeerError::Io)?;

        let peer_handshake = Handshake::from_bytes(&handshake_buf)?;

        // Verify info hash matches
        if peer_handshake.info_hash != info_hash {
            return Err(PeerError::InfoHashMismatch);
        }

        Ok((conn, peer_handshake))
    }

    /// Accept an incoming connection: read the peer's handshake first, verify info_hash,
    /// then send ours back.
    pub async fn accept(
        stream: TcpStream,
        info_hash: [u8; 20],
        peer_id: [u8; 20],
    ) -> Result<(Self, Handshake), PeerError> {
        let mut conn = Self {
            stream,
            read_buf: BytesMut::with_capacity(65536),
        };

        // Read their handshake first
        let mut handshake_buf = [0u8; 68];
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            conn.stream.read_exact(&mut handshake_buf),
        )
        .await
        .map_err(|_| PeerError::Timeout)?
        .map_err(PeerError::Io)?;

        let peer_handshake = Handshake::from_bytes(&handshake_buf)?;

        // Verify info hash matches our torrent
        if peer_handshake.info_hash != info_hash {
            return Err(PeerError::InfoHashMismatch);
        }

        // Send our handshake back
        let handshake = Handshake::new(info_hash, peer_id);
        conn.stream
            .write_all(&handshake.to_bytes())
            .await
            .map_err(PeerError::Io)?;

        Ok((conn, peer_handshake))
    }

    /// Send a message to the peer.
    pub async fn send(&mut self, msg: &Message) -> Result<(), PeerError> {
        self.stream
            .write_all(&msg.to_bytes())
            .await
            .map_err(PeerError::Io)
    }

    /// Read the next message from the peer.
    pub async fn receive(&mut self) -> Result<Message, PeerError> {
        // Read the 4-byte length prefix
        while self.read_buf.len() < 4 {
            let n = self.stream.read_buf(&mut self.read_buf).await.map_err(PeerError::Io)?;
            if n == 0 {
                return Err(PeerError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "peer disconnected",
                )));
            }
        }

        let length = u32::from_be_bytes(self.read_buf[..4].try_into().unwrap()) as usize;

        // Keep-alive
        if length == 0 {
            self.read_buf.advance(4);
            return Ok(Message::KeepAlive);
        }

        // Sanity check: allow piece data + extension metadata (up to ~256 KiB)
        if length > 262144 {
            return Err(PeerError::InvalidMessage);
        }

        // Read the full message
        while self.read_buf.len() < 4 + length {
            let n = self.stream.read_buf(&mut self.read_buf).await.map_err(PeerError::Io)?;
            if n == 0 {
                return Err(PeerError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "peer disconnected",
                )));
            }
        }

        let msg_data = &self.read_buf[4..4 + length];
        let msg = Message::from_bytes(msg_data)?;
        self.read_buf.advance(4 + length);
        Ok(msg)
    }
}

pub const STANDARD_BLOCK_SIZE: u32 = BLOCK_SIZE;
