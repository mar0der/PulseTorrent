# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project: PulseTorrent

Open-source BitTorrent client built with Rust (torrent-core library) + Tauri v2 + React/TypeScript frontend.

## Common Commands

### Development
```bash
cd ui && npm install   # first time setup
cargo tauri dev        # run dev server (starts both Vite and Tauri)
```

### Build
```bash
cargo tauri build      # production build → .app + .dmg in target/release/bundle/
```

### Tests
```bash
cargo test --lib                              # all unit tests (24 tests in torrent-core)
cargo test --lib -p torrent-core <name>       # run a single test by name
cargo test --lib -p torrent-core bencode      # run tests matching a module name
```

### Lint (frontend)
```bash
cd ui && npm run lint
```

## Architecture

### Crate Layout
```
Cargo.toml                 ← workspace (members: torrent-core, ui/src-tauri)
crates/torrent-core/       ← standalone Rust library, no Tauri dependency
ui/src-tauri/              ← Tauri v2 app crate, depends on torrent-core
ui/src/                    ← React + TypeScript frontend (Vite)
```

### torrent-core modules (`crates/torrent-core/src/`)
- `bencode/` — encoder/decoder (full spec compliance)
- `torrent/` — `.torrent` file parser, info_hash computation
- `tracker/` — HTTP tracker client (compact + dict peer formats)
- `peer/` — peer wire protocol (handshake, 9 message types + BEP 10 extension msgs)
- `piece/` — piece manager (rarest-first selection, SHA1 verification, async disk I/O)
- `engine/` — download engine, peer connection management
- `magnet.rs` — magnet link parser (hex + base32 info hash)
- `metadata.rs` — BEP 9 metadata exchange (ut_metadata)
- `persistence.rs` — JSON state save/load (atomic writes via tmp+rename)

### Tauri IPC layer (`ui/src-tauri/src/lib.rs`)
All IPC commands exposed to the frontend live here. Key types: `AppState`, `TorrentEntry`, `TorrentInfo`, `TorrentFileInfo`. State persists to `~/Library/Application Support/com.torrentrust.app/state/` (one JSON file per torrent).

Saves occur on: add, stop, remove, download-complete, every 30s during download, and app exit.

### Frontend (`ui/src/`)
Single-component React app (`App.tsx`). Polls Tauri IPC commands for torrent list and stats. No external state management library.

## Key Design Decisions
- `torrent-core` has **no Tauri dependency** — it can be used/tested as a standalone library.
- Piece resumption: SHA1 re-verification of all pieces on disk at resume time.
- File pre-allocation and skip-aware progress are supported.
- BEP 10 extension protocol is enabled (reserved bit set in handshake).

## Not Yet Implemented
- DHT (BEP 5) — trackerless peer discovery (magnet links without `&tr=` won't work)
- Seeding / uploading to peers
- Choking algorithm (unchoke top-4 + optimistic)
- Endgame mode
- Multiple tracker tiers / announce-list cycling

## CI/CD
GitHub Actions (`.github/workflows/release.yml`) auto-releases on version tags (`v*`) for: macOS Apple Silicon, macOS Intel, Linux x86_64, Windows x86_64. macOS builds include code signing and notarization.
