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
  download_dir: string;
}

interface TorrentFileInfo {
  path: string;
  size: number;
  progress: number;
  skipped: boolean;
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

function App() {
  const [torrents, setTorrents] = useState<TorrentInfo[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [magnetInput, setMagnetInput] = useState("");
  const [showMagnetInput, setShowMagnetInput] = useState(false);
  const [magnetLoading, setMagnetLoading] = useState(false);
  const [defaultDownloadDir, setDefaultDownloadDir] = useState("");
  const [fileList, setFileList] = useState<TorrentFileInfo[]>([]);
  const [contextMenu, setContextMenu] = useState<{ x: number; y: number; fileIndex: number } | null>(null);
  const pollRef = useRef<number | null>(null);
  const torrentsRef = useRef<TorrentInfo[]>([]);
  const fileRequestGen = useRef(0);

  // Load default download dir on mount
  useEffect(() => {
    invoke<string>("get_download_dir").then(setDefaultDownloadDir).catch(() => {});
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
      // Only update if no newer request has been issued (prevents stale responses
      // from overwriting correct data when switching torrent selection).
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

  // Poll for updates
  useEffect(() => {
    refreshTorrents();
    pollRef.current = window.setInterval(async () => {
      for (const t of torrentsRef.current) {
        if (t.status === "downloading") {
          try {
            await invoke("poll_events", { id: t.id });
          } catch (_) {}
        }
      }
      refreshTorrents();
    }, 1000);

    return () => {
      if (pollRef.current) clearInterval(pollRef.current);
    };
  }, [refreshTorrents]);

  // Refresh file list when selection or torrents change
  useEffect(() => {
    refreshFiles(selectedId);
  }, [selectedId, torrents, refreshFiles]);

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
        // Ask for download location
        const downloadDir = await pickDownloadFolder();
        if (!downloadDir) return; // User cancelled

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

    // Ask for download location first
    const downloadDir = await pickDownloadFolder();
    if (!downloadDir) return; // User cancelled

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
    try {
      await invoke("start_torrent", { id });
      refreshTorrents();
    } catch (e) {
      console.error("Failed to start torrent:", e);
    }
  };

  const handleStop = async (id: string) => {
    try {
      await invoke("stop_torrent", { id });
      refreshTorrents();
    } catch (e) {
      console.error("Failed to stop torrent:", e);
    }
  };

  const handleToggleFileSkip = async (fileIndex: number) => {
    if (!selectedId) return;
    try {
      await invoke("toggle_file_skip", { id: selectedId, fileIndex });
      refreshFiles(selectedId);
    } catch (e) {
      console.error("Failed to toggle file skip:", e);
    }
    setContextMenu(null);
  };

  // Close context menu on click anywhere
  useEffect(() => {
    if (!contextMenu) return;
    const close = () => setContextMenu(null);
    window.addEventListener("click", close);
    return () => window.removeEventListener("click", close);
  }, [contextMenu]);

  const handleRemove = async (id: string) => {
    try {
      await invoke("remove_torrent", { id });
      setTorrents((prev) => prev.filter((t) => t.id !== id));
      if (selectedId === id) setSelectedId(null);
    } catch (e) {
      console.error("Failed to remove torrent:", e);
    }
  };

  const selectedTorrent = torrents.find((t) => t.id === selectedId);

  return (
    <div className="app">
      <header className="toolbar">
        <div className="toolbar-left">
          <h1 className="app-title">PulseTorrent</h1>
        </div>
        <div className="toolbar-actions">
          <button
            className="btn btn-secondary"
            onClick={() => setShowMagnetInput(!showMagnetInput)}
          >
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

      <main className="content">
        <div className="torrent-list">
          {torrents.length === 0 ? (
            <div className="empty-state">
              <div className="empty-icon">&#8595;</div>
              <p>No torrents added yet</p>
              <p className="empty-hint">
                Add a .torrent file or paste a magnet link
              </p>
            </div>
          ) : (
            torrents.map((t) => (
              <div
                key={t.id}
                className={`torrent-item ${selectedId === t.id ? "selected" : ""}`}
                onClick={() => setSelectedId(t.id)}
              >
                <div className="torrent-header">
                  <span className="torrent-name">{t.name}</span>
                  <span
                    className={`torrent-status status-${t.status.startsWith("error") ? "error" : t.status.replace(/\s+/g, "-")}`}
                  >
                    {t.status}
                  </span>
                </div>

                <div className="progress-bar">
                  <div
                    className="progress-fill"
                    style={{
                      width: `${Math.min(t.progress * 100, 100)}%`,
                    }}
                  />
                </div>

                <div className="torrent-stats">
                  <span>
                    {Math.min(t.progress * 100, 100).toFixed(1)}%
                  </span>
                  <span>
                    {formatBytes(t.downloaded_bytes)} /{" "}
                    {formatBytes(t.total_size)}
                  </span>
                  {(t.status === "downloading" || t.status === "paused") && (
                    <>
                      {t.seeders !== null && (
                        <span className="stat-seeders">
                          S: {t.seeders}
                        </span>
                      )}
                      {t.leechers !== null && (
                        <span className="stat-leechers">
                          L: {t.leechers}
                        </span>
                      )}
                    </>
                  )}
                  {t.status === "downloading" && (
                    <>
                      <span>&#8595; {formatSpeed(t.download_speed)}</span>
                      <span>&#8593; {formatSpeed(t.upload_speed)}</span>
                      <span>{t.num_peers} peers</span>
                    </>
                  )}
                </div>

                <div className="torrent-actions">
                  {t.status === "paused" && (
                    <button
                      className="btn btn-small btn-success"
                      onClick={(e) => {
                        e.stopPropagation();
                        handleStart(t.id);
                      }}
                    >
                      {t.progress > 0 ? "Resume" : "Start"}
                    </button>
                  )}
                  {t.status === "downloading" && (
                    <button
                      className="btn btn-small btn-warning"
                      onClick={(e) => {
                        e.stopPropagation();
                        handleStop(t.id);
                      }}
                    >
                      Pause
                    </button>
                  )}
                  <button
                    className="btn btn-small btn-danger"
                    onClick={(e) => {
                      e.stopPropagation();
                      handleRemove(t.id);
                    }}
                  >
                    Remove
                  </button>
                </div>
              </div>
            ))
          )}
        </div>

        {selectedTorrent && (
          <div className="detail-panel">
            <h2>Details</h2>
            <div className="detail-grid">
              <div className="detail-row">
                <span className="detail-label">Name</span>
                <span className="detail-value">{selectedTorrent.name}</span>
              </div>
              <div className="detail-row">
                <span className="detail-label">Size</span>
                <span className="detail-value">
                  {formatBytes(selectedTorrent.total_size)}
                </span>
              </div>
              <div className="detail-row">
                <span className="detail-label">Pieces</span>
                <span className="detail-value">
                  {selectedTorrent.pieces_done} / {selectedTorrent.num_pieces}
                </span>
              </div>
              <div className="detail-row">
                <span className="detail-label">Downloaded</span>
                <span className="detail-value">
                  {formatBytes(selectedTorrent.downloaded_bytes)}
                </span>
              </div>
              <div className="detail-row">
                <span className="detail-label">Uploaded</span>
                <span className="detail-value">
                  {formatBytes(selectedTorrent.uploaded_bytes)}
                </span>
              </div>
              {selectedTorrent.seeders !== null && (
                <div className="detail-row">
                  <span className="detail-label">Seeders</span>
                  <span className="detail-value detail-seeders">
                    {selectedTorrent.seeders}
                  </span>
                </div>
              )}
              {selectedTorrent.leechers !== null && (
                <div className="detail-row">
                  <span className="detail-label">Leechers</span>
                  <span className="detail-value detail-leechers">
                    {selectedTorrent.leechers}
                  </span>
                </div>
              )}
              <div className="detail-row">
                <span className="detail-label">Peers</span>
                <span className="detail-value">
                  {selectedTorrent.num_peers}
                </span>
              </div>
              <div className="detail-row">
                <span className="detail-label">Save to</span>
                <span className="detail-value hash">
                  {selectedTorrent.download_dir}
                </span>
              </div>
              <div className="detail-row">
                <span className="detail-label">Hash</span>
                <span className="detail-value hash">{selectedTorrent.id}</span>
              </div>
            </div>

            {fileList.length > 0 && (
              <div className="file-list-section">
                <h3>Files ({fileList.length})</h3>
                <div className="file-list">
                  {fileList.map((f, i) => (
                    <div
                      key={i}
                      className={`file-item ${f.skipped ? "skipped" : ""}`}
                      onContextMenu={(e) => {
                        e.preventDefault();
                        setContextMenu({ x: e.clientX, y: e.clientY, fileIndex: i });
                      }}
                    >
                      <div className="file-header">
                        <span className="file-name" title={f.path}>
                          {f.path.split("/").pop() || f.path}
                        </span>
                        <span className="file-size">{formatBytes(f.size)}</span>
                      </div>
                      <div className="file-progress-bar">
                        <div
                          className="file-progress-fill"
                          style={{ width: `${f.progress * 100}%` }}
                        />
                      </div>
                      <span className={`file-progress-text ${f.progress >= 1.0 ? "file-done" : ""}`}>
                        {f.skipped ? "Skipped" : f.progress >= 1.0 ? "Done" : `${(f.progress * 100).toFixed(1)}%`}
                      </span>
                    </div>
                  ))}
                  {contextMenu && (
                    <div
                      className="file-context-menu"
                      style={{ top: contextMenu.y, left: contextMenu.x }}
                      onClick={(e) => e.stopPropagation()}
                    >
                      <button
                        className="context-menu-item"
                        onClick={() => handleToggleFileSkip(contextMenu.fileIndex)}
                      >
                        {fileList[contextMenu.fileIndex]?.skipped ? "Resume file" : "Skip file"}
                      </button>
                    </div>
                  )}
                </div>
              </div>
            )}
          </div>
        )}
      </main>

      <footer className="statusbar">
        <span>{torrents.length} torrent(s)</span>
        <span>
          &#8595;{" "}
          {formatSpeed(
            torrents.reduce((acc, t) => acc + t.download_speed, 0)
          )}{" "}
          | &#8593;{" "}
          {formatSpeed(torrents.reduce((acc, t) => acc + t.upload_speed, 0))}
        </span>
      </footer>
    </div>
  );
}

export default App;
