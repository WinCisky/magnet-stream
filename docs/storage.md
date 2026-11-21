# Piece storage in the browser

Verified pieces are kept in the origin private file system (OPFS). Nothing is
stored on any server, and piece data never leaves the browser except as
uploads to peers through the engine (M3-12, OQ-04).

## Lifecycle (decision OQ-13)

Every video opened is saved as it is recovered, including a partly
recovered one. Nothing is deleted when a session closes. Pieces are deleted
only when the user asks, for one video or for everything at once in the
"Saved" list. This replaces the "deleted on close unless kept" row of
docs/architecture.md §Storage and database.

## Layout

All files are in the OPFS directory `pieces/`, named only after the
validated hex info-hash (never after names from the torrent):

| File | Contents |
| --- | --- |
| `<info-hash>.data` | Sparse data file: piece `p` at byte `p × pieceLength`, the torrent's own layout, so a file of the torrent is one byte range |
| `<info-hash>.map` | Verified pieces as a bitfield (bit `p` of byte `p >> 3`, most significant bit first) |
| `<info-hash>.json` | Record of the files opened from the torrent: name, magnet, piece length, total length, and per file its index, path, byte range and when it was last opened |

A piece's bit is set only after its bytes are written and flushed, so an
interrupted write or a quota error leaves the piece missing. A partly
written piece is never marked verified.

## Who touches what

- **Engine worker** (`session.worker.ts`, `opfs-store.ts`, `piece-file.ts`)
  - Owns `.data` and `.map` through `FileSystemSyncAccessHandle`.
  - Writes each piece as soon as it is verified.
  - Serves the page's piece reads, and block reads for the upload path
    (16 KiB at most, verified pieces only; anything else throws).
  - A sync handle is exclusive, so the worker closes the files 5 s after
    the last saving recovery or read, and at once when the library needs
    them for a delete.
- **Library worker** (`library.worker.ts`, `library-core.ts`)
  - Lists, reads and deletes for the "Saved" list and downloads.
  - Reads with `getFile()`, which Chromium allows while the engine worker
    holds the files open.
  - Deleting one video clears the bits of the pieces no other saved video
    of the torrent covers, and truncates the data file when those pieces
    are at its end. Deleting the last saved video of a torrent, or
    "Delete all", removes its three files.
  - A delete refused because the files are in use (the video is being
    recovered) changes nothing and is reported to the user.

## Quota

Chromium counts the logical size of a sparse file against the origin's
quota: one piece written at 1.5 GiB costs 1.5 GiB of quota, even though the
disk holds only that piece. In a multi-file torrent, watching a late file
reserves quota for the files before it. OPFS cannot punch holes, so space
inside the data file is given back only by truncating its tail or removing
it.

When the browser refuses a write (`QuotaExceededError`), the piece stays
missing in the store and is kept in the page's memory, so playback goes on.
The page also shows that new pieces aren't being saved. The map and the
other pieces are untouched.

## Migration from Cache Storage

Versions up to M3-01 kept pieces in Cache Storage, one `mw-pieces-<info-hash>`
cache per torrent. On the library's first use after the upgrade:

- A cache with a record of its files is copied into OPFS (piece `p` at
  `p × pieceLength`, then the map, then the record), and the cache is
  deleted.
- A cache without a record ("unnamed", from before files were recorded) is
  deleted. Its piece length is unknown, so its pieces cannot be placed.
- A copy that fails (the quota, say) is removed. The cache is kept and the
  copy is tried again at the next start. "Delete all" also removes leftover
  caches.
- A torrent already present in OPFS keeps the OPFS copy, and its cache is
  deleted.

## Session memory (IndexedDB, M3-04)

Besides the pieces, the browser keeps, per torrent, what lets a reload or a
second paste of the same magnet start in one step
(`apps/web/src/lib/torrent/memory-db.ts`, database `magnet-stream-memory`):

| Store | Key | Contents |
| --- | --- | --- |
| `torrents` | info-hash | The verified info dict, whether a video of the torrent is saved, when it was last used |
| `peers` | info-hash | Notes on up to 200 peer addresses: bytes delivered, connected or not, failures and the last reason, banned, the scout's facts and when they came |
| `positions` | info-hash, file index | Playback position and duration of each file opened |

- **When it is written.** The session worker keeps the info dict as soon
  as it is verified, before any video is opened, and the peer notes every
  5 s while they change (and when a recovery ends or the session goes
  idle). The library marks the torrent saved when a video is opened. The
  video page keeps the playback position every 5 s while playing, and when
  it closes or the tab goes away.
- **How it is used.** The next session of the torrent offers the kept info
  dict to the metadata fetcher, which checks it against the info-hash like
  any other (a bad one is ignored and fetched again), and lists the files
  at once. The peer book restores the notes, validating each one: peers
  that delivered are dialled first and may take any tunnel slot, failed
  ones go last, banned ones stay out, and the scout's facts (if under 2 h
  old) are the prior again. With both the info dict and a peer that
  delivered, the `/peers` request waits: it starts only if nothing is left
  to dial, or when what is wanted goes 10 s without a peer delivering. A
  saved video resumes at its position (unless within 10 s of the start or
  30 s of the end).
- **Lifecycle.** The records of a torrent with a saved video follow its
  pieces: they stay until the user deletes the video. Deleting one video
  removes its position; deleting a torrent's last video removes its info
  dict, peer notes and positions; "Delete all" removes everything, cache
  included (except torrents whose delete was refused because they are in
  use). Torrents only looked up form a cache of at most 50 entries or
  32 MiB of info dicts, the least recently used leaving first with its
  notes; saved torrents don't count against it and are never evicted.
- **Trust and versions.** Everything read back is untrusted: rows of an
  unknown record version or shape read as missing. The schema is versioned
  (`MIGRATIONS`, one step per version); a database from a newer version of
  the site, or one without the expected stores, is deleted and recreated
  (it holds a cache and notes only, the pieces stay in OPFS).
- **Privacy.** Peer addresses and info-hashes stay in the browser. Nothing
  here is sent anywhere or logged.
