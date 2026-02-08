# PulseTorrent

A free, open-source, multiplatform BitTorrent client built with Rust, Tauri, and React.

## Features

- **Torrent & Magnet support** — Add `.torrent` files or paste magnet links
- **Rarest-first piece selection** — Efficient downloading with SHA1 verification
- **Per-file skip/resume** — Right-click files to skip or resume individual downloads
- **Persistent state** — Torrents resume where they left off across app restarts
- **Real-time stats** — Download/upload speeds, peer count, seeders/leechers, per-file progress
- **Multi-file torrents** — Proper piece-to-file mapping with async disk I/O
- **BEP 9 metadata exchange** — Fetch torrent metadata from peers for magnet links
- **BEP 10 extension protocol** — Extended handshake support

## Architecture

```
crates/torrent-core/    Standalone Rust library
  src/bencode/            Bencode encoder/decoder
  src/torrent/            .torrent file parser
  src/tracker/            HTTP tracker client
  src/peer/               Peer wire protocol (all message types)
  src/piece/              Piece manager, block tracking, disk I/O
  src/engine/             Download engine, peer connection management
  src/magnet.rs           Magnet link parser
  src/metadata.rs         BEP 9 metadata fetching
  src/persistence.rs      JSON state save/load

ui/
  src-tauri/              Tauri v2 backend (IPC commands)
  src/                    React + TypeScript frontend (Vite)
```

## Building

### Prerequisites

- [Rust](https://rustup.rs/) (stable)
- [Node.js](https://nodejs.org/) (v18+)
- [Tauri v2 prerequisites](https://v2.tauri.app/start/prerequisites/)

### Development

```bash
cd ui
npm install
cargo tauri dev
```

### Production build

```bash
cargo tauri build
```

Output: `.app` bundle and `.dmg` installer in `target/release/bundle/`.

## Downloads

Pre-built binaries are available on the [Releases](https://github.com/mar0der/PulseTorrent/releases) page:

| Platform | Architecture | Format |
|----------|-------------|--------|
| macOS | Apple Silicon (arm64) | `.dmg` |
| macOS | Intel (x86_64) | `.dmg` |
| Windows | x86_64 | `.msi` |
| Linux | x86_64 | `.deb`, `.AppImage` |

Releases are built automatically via GitHub Actions on every tagged version.

## Roadmap

- [ ] DHT (BEP 5) — trackerless peer discovery
- [ ] Seeding / uploading to peers
- [ ] Choking algorithm (unchoke top 4 + optimistic)
- [ ] Endgame mode
- [ ] Multiple tracker tiers / announce-list cycling

## License

MIT
