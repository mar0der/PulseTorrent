import { useState, useEffect, useCallback, useRef } from "react";
import { invoke } from "@tauri-apps/api/core";
import { open } from "@tauri-apps/plugin-dialog";
import "./App.css";

interface TorrentInfo {
  id: string;
  name: string;
  total_size: number;
  num_pieces: number;
  pieces_done: number;
  downloaded_bytes: number;
  uploaded_bytes: number;
  download_speed: number;
  upload_speed: number;
  num_peers: number;
  status: string;
  progress: number;
  seeders: number | null;
  leechers: number | null;
  connected_seeders: number;
  connected_leechers: number;
  download_dir: string;
  eta_secs: number | null;
  warning: string | null;
}

interface GlobalStatsInfo {
  total_downloaded: number;
  total_uploaded: number;
  ratio: number;
}

interface TorrentFileInfo {
  path: string;
  size: number;
  progress: number;
  skipped: boolean;
}

interface PeerInfoUI {
  addr: string;
  is_seeder: boolean;
  pieces_have: number;
  pieces_total: number;
  peer_interested: boolean;
  am_choking: boolean;
  peer_choking: boolean;
  client: string;
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return "0 B";
  const k = 1024;
  const sizes = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + " " + sizes[i];
}

function formatSpeed(bytesPerSec: number): string {
  return formatBytes(bytesPerSec) + "/s";
}

function formatEta(secs: number): string {
  if (secs <= 0) return "done";
  if (secs >= 8640000) return "\u221E";
  const d = Math.floor(secs / 86400);
  const h = Math.floor((secs % 86400) / 3600);
  const m = Math.floor((secs % 3600) / 60);
  const s = Math.floor(secs % 60);
  if (d > 0) return `${d}d ${h}h`;
  if (h > 0) return `${h}h ${m}m`;
  if (m > 0) return `${m}m ${s}s`;
  return `${s}s`;
}

type DetailTab = "files" | "info" | "peers" | "pieces";
type StatusFilter = "all" | "downloading" | "seeding" | "paused" | "verifying" | "complete" | "error";

const STATUS_FILTERS: { key: StatusFilter; label: string }[] = [
  { key: "all", label: "All" },
  { key: "downloading", label: "Downloading" },
  { key: "seeding", label: "Seeding" },
  { key: "paused", label: "Paused" },
  { key: "complete", label: "Complete" },
  { key: "verifying", label: "Verifying" },
  { key: "error", label: "Error" },
];

function matchesFilter(t: TorrentInfo, filter: StatusFilter): boolean {
  if (filter === "all") return true;
  if (filter === "error") return t.status.startsWith("error");
  if (filter === "complete") return t.status === "complete" || t.status === "seeding";
  return t.status === filter;
}

function App() {
  const [torrents, setTorrents] = useState<TorrentInfo[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [magnetInput, setMagnetInput] = useState("");
  const [showMagnetInput, setShowMagnetInput] = useState(false);
  const [magnetLoading, setMagnetLoading] = useState(false);
  const [defaultDownloadDir, setDefaultDownloadDir] = useState("");
  const [fileList, setFileList] = useState<TorrentFileInfo[]>([]);
  const [contextMenu, setContextMenu] = useState<{ x: number; y: number; fileIndex: number } | null>(null);
  const [globalStats, setGlobalStats] = useState<GlobalStatsInfo>({ total_downloaded: 0, total_uploaded: 0, ratio: 0 });
  const [appVersion, setAppVersion] = useState("");
  const [activeTab, setActiveTab] = useState<DetailTab>("files");
  const [peerList, setPeerList] = useState<PeerInfoUI[]>([]);
  const [pieceMap, setPieceMap] = useState<number[]>([]);
  const [statusFilter, setStatusFilter] = useState<StatusFilter>("all");
  const pollRef = useRef<number | null>(null);
  const torrentsRef = useRef<TorrentInfo[]>([]);
  const fileRequestGen = useRef(0);

  useEffect(() => {
    invoke<string>("get_download_dir").then(setDefaultDownloadDir).catch(() => {});
    invoke<string>("get_app_version").then(setAppVersion).catch(() => {});
  }, []);

  const refreshTorrents = useCallback(async () => {
    try {
      const list = await invoke<TorrentInfo[]>("get_torrents");
      setTorrents(list);
      torrentsRef.current = list;
    } catch (e) {
      console.error("Failed to get torrents:", e);
    }
  }, []);

  const refreshFiles = useCallback(async (id: string | null) => {
    const gen = ++fileRequestGen.current;
    if (!id) { setFileList([]); return; }
    try {
      const files = await invoke<TorrentFileInfo[]>("get_torrent_files", { id });
      if (fileRequestGen.current === gen) {
        setFileList(files);
      }
    } catch (e) {
      console.error("Failed to get torrent files:", e);
      if (fileRequestGen.current === gen) {
        setFileList([]);
      }
    }
  }, []);

  const refreshPeers = useCallback(async (id: string | null) => {
    if (!id) { setPeerList([]); return; }
    try {
      const peers = await invoke<PeerInfoUI[]>("get_peers", { id });
      setPeerList(peers);
    } catch {
      setPeerList([]);
    }
  }, []);

  const refreshPieceMap = useCallback(async (id: string | null) => {
    if (!id) { setPieceMap([]); return; }
    try {
      const pieces = await invoke<number[]>("get_piece_map", { id });
      setPieceMap(pieces);
    } catch {
      setPieceMap([]);
    }
  }, []);

  // Poll for updates
  useEffect(() => {
    refreshTorrents();
    pollRef.current = window.setInterval(async () => {
      for (const t of torrentsRef.current) {
        if (t.status === "downloading" || t.status === "verifying" || t.status === "seeding") {
          try {
            await invoke("poll_events", { id: t.id });
          } catch (_) {}
        }
      }
      refreshTorrents();
      invoke<GlobalStatsInfo>("get_global_stats").then(setGlobalStats).catch(() => {});
    }, 1000);

    return () => {
      if (pollRef.current) clearInterval(pollRef.current);
    };
  }, [refreshTorrents]);

  // Refresh tab data when selection or tab changes
  useEffect(() => {
    if (activeTab === "files") refreshFiles(selectedId);
    else if (activeTab === "peers") refreshPeers(selectedId);
    else if (activeTab === "pieces") refreshPieceMap(selectedId);
  }, [selectedId, activeTab, torrents, refreshFiles, refreshPeers, refreshPieceMap]);

  const pickDownloadFolder = async (): Promise<string | null> => {
    const folder = await open({
      directory: true,
      title: "Select download location",
      defaultPath: defaultDownloadDir || undefined,
    });
    if (typeof folder === "string") return folder;
    return null;
  };

  const handleAddTorrent = async () => {
    try {
      const file = await open({
        multiple: false,
        filters: [{ name: "Torrent", extensions: ["torrent"] }],
      });
      if (file) {
        const downloadDir = await pickDownloadFolder();
        if (!downloadDir) return;
        const info = await invoke<TorrentInfo>("add_torrent", {
          path: file,
          downloadDir,
        });
        setTorrents((prev) => [...prev, info]);
      }
    } catch (e) {
      console.error("Failed to add torrent:", e);
    }
  };

  const handleAddMagnet = async () => {
    const uri = magnetInput.trim();
    if (!uri) return;
    const downloadDir = await pickDownloadFolder();
    if (!downloadDir) return;
    setMagnetLoading(true);
    try {
      const info = await invoke<TorrentInfo>("add_magnet", { uri, downloadDir });
      setTorrents((prev) => {
        const existing = prev.find((t) => t.id === info.id);
        if (existing) {
          return prev.map((t) => (t.id === info.id ? info : t));
        }
        return [...prev, info];
      });
      setMagnetInput("");
      setShowMagnetInput(false);
    } catch (e) {
      console.error("Failed to add magnet:", e);
      refreshTorrents();
    } finally {
      setMagnetLoading(false);
    }
  };

  const handleStart = async (id: string) => {
    try { await invoke("start_torrent", { id }); } catch (e) { console.error(e); }
    refreshTorrents();
  };

  const handleStop = async (id: string) => {
    try { await invoke("stop_torrent", { id }); refreshTorrents(); } catch (e) { console.error(e); }
  };

  const handleToggleFileSkip = async (fileIndex: number) => {
    if (!selectedId) return;
    try {
      await invoke("toggle_file_skip", { id: selectedId, fileIndex });
      refreshFiles(selectedId);
    } catch (e) { console.error(e); }
    setContextMenu(null);
  };

  useEffect(() => {
    if (!contextMenu) return;
    const close = () => setContextMenu(null);
    window.addEventListener("click", close);
    return () => window.removeEventListener("click", close);
  }, [contextMenu]);

  const handleChangeDir = async (id: string) => {
    const folder = await pickDownloadFolder();
    if (!folder) return;
    try {
      await invoke("change_torrent_download_dir", { id, path: folder });
      refreshTorrents();
    } catch (e) {
      console.error(e);
      alert(String(e));
    }
  };

  const handleRemove = async (id: string, deleteFiles: boolean) => {
    if (deleteFiles) {
      const torrent = torrents.find((t) => t.id === id);
      const name = torrent?.name || id;
      if (!window.confirm(`Delete "${name}" and all downloaded files from disk?\n\nThis cannot be undone.`)) {
        return;
      }
    }
    try {
      await invoke("remove_torrent", { id, deleteFiles });
      setTorrents((prev) => prev.filter((t) => t.id !== id));
      if (selectedId === id) setSelectedId(null);
    } catch (e) { console.error(e); }
  };

  const filteredTorrents = torrents.filter((t) => matchesFilter(t, statusFilter));
  const selectedTorrent = torrents.find((t) => t.id === selectedId);

  // Count per status for sidebar badges
  const statusCounts: Record<StatusFilter, number> = {
    all: torrents.length,
    downloading: torrents.filter((t) => t.status === "downloading").length,
    seeding: torrents.filter((t) => t.status === "seeding").length,
    paused: torrents.filter((t) => t.status === "paused").length,
    complete: torrents.filter((t) => t.status === "complete" || t.status === "seeding").length,
    verifying: torrents.filter((t) => t.status === "verifying").length,
    error: torrents.filter((t) => t.status.startsWith("error")).length,
  };

  return (
    <div className="app">
      {/* Toolbar */}
      <header className="toolbar">
        <div className="toolbar-left">
          <h1 className="app-title">PulseTorrent {appVersion && <span className="app-version">v{appVersion}</span>}</h1>
        </div>
        <div className="toolbar-actions">
          <button className="btn btn-secondary" onClick={() => setShowMagnetInput(!showMagnetInput)}>
            Magnet Link
          </button>
          <button className="btn btn-primary" onClick={handleAddTorrent}>
            + Add .torrent
          </button>
        </div>
      </header>

      {showMagnetInput && (
        <div className="magnet-bar">
          <input
            className="magnet-input"
            type="text"
            placeholder="Paste magnet link here (magnet:?xt=urn:btih:...)"
            value={magnetInput}
            onChange={(e) => setMagnetInput(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && handleAddMagnet()}
            autoFocus
            disabled={magnetLoading}
          />
          <button
            className="btn btn-primary"
            onClick={handleAddMagnet}
            disabled={magnetLoading || !magnetInput.trim()}
          >
            {magnetLoading ? "Fetching..." : "Add"}
          </button>
        </div>
      )}

      {/* Main content: sidebar + (torrent table / detail tabs) */}
      <main className="content">
        {/* Sidebar filter */}
        <aside className="sidebar">
          <div className="sidebar-header">Torrents</div>
          {STATUS_FILTERS.map((f) => (
            <button
              key={f.key}
              className={`sidebar-item ${statusFilter === f.key ? "active" : ""}`}
              onClick={() => setStatusFilter(f.key)}
            >
              <span className="sidebar-label">{f.label}</span>
              <span className="sidebar-count">{statusCounts[f.key]}</span>
            </button>
          ))}
        </aside>

        {/* Right side: table + bottom panel */}
        <div className="main-panel">
        {/* Torrent table */}
        <div className="torrent-table-wrap">
          {filteredTorrents.length === 0 ? (
            <div className="empty-state">
              <div className="empty-icon">&#8595;</div>
              <p>{torrents.length === 0 ? "No torrents added yet" : "No torrents match this filter"}</p>
              {torrents.length === 0 && (
                <p className="empty-hint">Add a .torrent file or paste a magnet link</p>
              )}
            </div>
          ) : (
            <table className="torrent-table">
              <thead>
                <tr>
                  <th className="col-name">Name</th>
                  <th className="col-size">Size</th>
                  <th className="col-progress">Done</th>
                  <th className="col-status">Status</th>
                  <th className="col-down">Down</th>
                  <th className="col-up">Up</th>
                  <th className="col-eta">ETA</th>
                  <th className="col-seeds">Seeds / Peers</th>
                  <th className="col-actions"></th>
                </tr>
              </thead>
              <tbody>
                {filteredTorrents.map((t) => (
                  <tr
                    key={t.id}
                    className={`torrent-row ${selectedId === t.id ? "selected" : ""}`}
                    onClick={() => setSelectedId(t.id)}
                  >
                    <td className="col-name">
                      <div className="name-cell">
                        <span className="torrent-name" title={t.name}>{t.name}</span>
                        {t.warning && <span className="torrent-warning-dot" title={t.warning}>!</span>}
                      </div>
                      <div className="progress-bar">
                        <div
                          className={`progress-fill ${t.progress >= 1.0 ? "progress-complete" : ""}`}
                          style={{ width: `${Math.min(t.progress * 100, 100)}%` }}
                        />
                      </div>
                    </td>
                    <td className="col-size">{formatBytes(t.total_size)}</td>
                    <td className="col-progress">{(Math.min(t.progress * 100, 100)).toFixed(1)}%</td>
                    <td className="col-status">
                      <span className={`status-badge status-${t.status.startsWith("error") ? "error" : t.status.replace(/\s+/g, "-")}`}>
                        {t.status}
                      </span>
                    </td>
                    <td className="col-down">
                      {(t.status === "downloading" || t.status === "seeding") ? formatSpeed(t.download_speed) : ""}
                    </td>
                    <td className="col-up">
                      {(t.status === "downloading" || t.status === "seeding") ? formatSpeed(t.upload_speed) : ""}
                    </td>
                    <td className="col-eta">
                      {t.status === "downloading" && t.eta_secs !== null ? formatEta(t.eta_secs) : ""}
                    </td>
                    <td className="col-seeds">
                      {t.connected_seeders}S : {t.connected_leechers}L
                      {t.seeders !== null && <span className="swarm-info"> ({t.seeders}/{t.leechers})</span>}
                    </td>
                    <td className="col-actions" onClick={(e) => e.stopPropagation()}>
                      {t.status === "paused" && (
                        <button className="tbl-btn tbl-btn-start" onClick={() => handleStart(t.id)} title="Start">&#9654;</button>
                      )}
                      {(t.status === "downloading" || t.status === "seeding") && (
                        <button className="tbl-btn tbl-btn-pause" onClick={() => handleStop(t.id)} title="Pause">&#9646;&#9646;</button>
                      )}
                      <button className="tbl-btn tbl-btn-open" onClick={() => invoke("open_download_dir", { id: t.id })} title="Open folder">&#128194;</button>
                      <button className="tbl-btn tbl-btn-remove" onClick={() => handleRemove(t.id, false)} title="Remove">&#10005;</button>
                      <button className="tbl-btn tbl-btn-delete" onClick={() => handleRemove(t.id, true)} title="Delete files">&#128465;</button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>

        {/* Bottom detail panel with tabs */}
        {selectedTorrent && (
          <div className="detail-panel">
            <div className="tab-bar">
              <button className={`tab ${activeTab === "files" ? "active" : ""}`} onClick={() => setActiveTab("files")}>
                Files
              </button>
              <button className={`tab ${activeTab === "info" ? "active" : ""}`} onClick={() => setActiveTab("info")}>
                Info
              </button>
              <button className={`tab ${activeTab === "peers" ? "active" : ""}`} onClick={() => setActiveTab("peers")}>
                Peers ({peerList.length})
              </button>
              <button className={`tab ${activeTab === "pieces" ? "active" : ""}`} onClick={() => setActiveTab("pieces")}>
                Pieces
              </button>
            </div>

            <div className="tab-content">
              {/* Files tab */}
              {activeTab === "files" && (
                <div className="tab-files">
                  {fileList.length === 0 ? (
                    <p className="tab-empty">No files</p>
                  ) : (
                    <table className="file-table">
                      <thead>
                        <tr>
                          <th>Name</th>
                          <th>Size</th>
                          <th>Progress</th>
                          <th>Status</th>
                        </tr>
                      </thead>
                      <tbody>
                        {fileList.map((f, i) => (
                          <tr
                            key={i}
                            className={f.skipped ? "file-skipped" : ""}
                            onContextMenu={(e) => {
                              e.preventDefault();
                              setContextMenu({ x: e.clientX, y: e.clientY, fileIndex: i });
                            }}
                          >
                            <td className="file-name" title={f.path}>
                              {f.path.split("/").pop() || f.path}
                            </td>
                            <td className="file-size">{formatBytes(f.size)}</td>
                            <td className="file-progress-cell">
                              <div className="file-progress-bar">
                                <div className="file-progress-fill" style={{ width: `${f.progress * 100}%` }} />
                              </div>
                              <span className="file-pct">{(f.progress * 100).toFixed(1)}%</span>
                            </td>
                            <td className={`file-status ${f.progress >= 1.0 ? "file-done" : ""}`}>
                              {f.skipped ? "Skipped" : f.progress >= 1.0 ? "Done" : "Downloading"}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  )}
                  {contextMenu && (
                    <div
                      className="file-context-menu"
                      style={{ top: contextMenu.y, left: contextMenu.x }}
                      onClick={(e) => e.stopPropagation()}
                    >
                      <button className="context-menu-item" onClick={() => handleToggleFileSkip(contextMenu.fileIndex)}>
                        {fileList[contextMenu.fileIndex]?.skipped ? "Resume file" : "Skip file"}
                      </button>
                    </div>
                  )}
                </div>
              )}

              {/* Info tab */}
              {activeTab === "info" && (
                <div className="tab-info">
                  <div className="info-grid">
                    <div className="info-col">
                      <div className="info-row">
                        <span className="info-label">Name</span>
                        <span className="info-value">{selectedTorrent.name}</span>
                      </div>
                      <div className="info-row">
                        <span className="info-label">Size</span>
                        <span className="info-value">{formatBytes(selectedTorrent.total_size)}</span>
                      </div>
                      <div className="info-row">
                        <span className="info-label">Pieces</span>
                        <span className="info-value">{selectedTorrent.pieces_done} / {selectedTorrent.num_pieces}</span>
                      </div>
                      <div className="info-row">
                        <span className="info-label">Downloaded</span>
                        <span className="info-value">{formatBytes(Math.min(selectedTorrent.downloaded_bytes, selectedTorrent.total_size))}</span>
                      </div>
                      <div className="info-row">
                        <span className="info-label">Uploaded</span>
                        <span className="info-value">{formatBytes(selectedTorrent.uploaded_bytes)}</span>
                      </div>
                      <div className="info-row">
                        <span className="info-label">Ratio</span>
                        <span className="info-value">
                          {selectedTorrent.downloaded_bytes > 0
                            ? (selectedTorrent.uploaded_bytes / selectedTorrent.downloaded_bytes).toFixed(3)
                            : "0.000"}
                        </span>
                      </div>
                    </div>
                    <div className="info-col">
                      <div className="info-row">
                        <span className="info-label">Status</span>
                        <span className="info-value">{selectedTorrent.status}</span>
                      </div>
                      {selectedTorrent.eta_secs !== null && selectedTorrent.status === "downloading" && (
                        <div className="info-row">
                          <span className="info-label">ETA</span>
                          <span className="info-value">{formatEta(selectedTorrent.eta_secs)}</span>
                        </div>
                      )}
                      {selectedTorrent.seeders !== null && (
                        <div className="info-row">
                          <span className="info-label">Swarm</span>
                          <span className="info-value">
                            <span className="detail-seeders">{selectedTorrent.seeders} seeders</span>
                            {" / "}
                            <span className="detail-leechers">{selectedTorrent.leechers} leechers</span>
                          </span>
                        </div>
                      )}
                      <div className="info-row">
                        <span className="info-label">Peers</span>
                        <span className="info-value">
                          {selectedTorrent.num_peers} ({selectedTorrent.connected_seeders}S, {selectedTorrent.connected_leechers}L)
                        </span>
                      </div>
                      <div className="info-row">
                        <span className="info-label">Save to</span>
                        <span className="info-value info-path">
                          {selectedTorrent.download_dir}
                          {selectedTorrent.status === "paused" && (
                            <button
                              className="btn-inline"
                              onClick={() => handleChangeDir(selectedTorrent.id)}
                            >Change</button>
                          )}
                        </span>
                      </div>
                      <div className="info-row">
                        <span className="info-label">Hash</span>
                        <span className="info-value info-hash">{selectedTorrent.id}</span>
                      </div>
                    </div>
                  </div>
                </div>
              )}

              {/* Peers tab */}
              {activeTab === "peers" && (
                <div className="tab-peers">
                  {peerList.length === 0 ? (
                    <p className="tab-empty">No peers connected</p>
                  ) : (
                    <table className="peer-table">
                      <thead>
                        <tr>
                          <th>IP</th>
                          <th>Client</th>
                          <th>Flags</th>
                          <th>%</th>
                          <th>Pieces</th>
                        </tr>
                      </thead>
                      <tbody>
                        {peerList.map((p, i) => (
                          <tr key={i}>
                            <td className="peer-addr">{p.addr}</td>
                            <td>{p.client || "Unknown"}</td>
                            <td className="peer-flags">
                              {p.is_seeder ? "S" : "L"}
                              {p.peer_interested ? " I" : ""}
                              {!p.peer_choking ? " U" : ""}
                              {!p.am_choking ? " u" : ""}
                            </td>
                            <td>
                              {p.pieces_total > 0
                                ? `${((p.pieces_have / p.pieces_total) * 100).toFixed(1)}%`
                                : "?"}
                            </td>
                            <td>{p.pieces_have} / {p.pieces_total}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  )}
                </div>
              )}

              {/* Pieces tab */}
              {activeTab === "pieces" && (
                <div className="tab-pieces">
                  {pieceMap.length === 0 ? (
                    <p className="tab-empty">No piece data</p>
                  ) : (
                    <>
                      <div className="piecemap-grid">
                        {pieceMap.map((p, i) => (
                          <div
                            key={i}
                            className={`piecemap-cell ${p >= 1.0 ? "piece-done" : p > 0 ? "piece-partial" : "piece-missing"}`}
                            title={`Piece ${i}: ${p >= 1.0 ? "Complete" : p > 0 ? `${(p * 100).toFixed(0)}%` : "Missing"}`}
                          />
                        ))}
                      </div>
                      <div className="piecemap-legend">
                        <span><span className="legend-box piece-done" /> Complete ({pieceMap.filter(p => p >= 1.0).length})</span>
                        <span><span className="legend-box piece-partial" /> Partial ({pieceMap.filter(p => p > 0 && p < 1.0).length})</span>
                        <span><span className="legend-box piece-missing" /> Missing ({pieceMap.filter(p => p === 0).length})</span>
                        <span className="piecemap-total">{pieceMap.length} total</span>
                      </div>
                    </>
                  )}
                </div>
              )}
            </div>
          </div>
        )}
        </div>{/* end main-panel */}
      </main>

      {/* Status bar */}
      <footer className="statusbar">
        <span>{torrents.length} torrent(s)</span>
        <span>
          &#8595;{" "}
          {formatSpeed(torrents.reduce((acc, t) => acc + t.download_speed, 0))}
          {" | "}&#8593;{" "}
          {formatSpeed(torrents.reduce((acc, t) => acc + t.upload_speed, 0))}
        </span>
        <span>
          Total: &#8595; {formatBytes(globalStats.total_downloaded)} | &#8593; {formatBytes(globalStats.total_uploaded)} | Ratio: {globalStats.ratio.toFixed(3)}
        </span>
      </footer>
    </div>
  );
}

export default App;
