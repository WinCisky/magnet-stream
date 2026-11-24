# Magnet Watcher

Paste a magnet link, pick a video file, and the page recovers the file's
pieces from the BitTorrent swarm, verifying each one in the browser, and
plays the video while it downloads. Recovery is sequential (the file's head
and tail first, then in order from a movable front), and the player moves
that front to wherever playback needs data. Once everything past the front
is in, the front goes back to the file's start to fill the gaps.

- **Choosing a video.** A magnet's videos are shown as a folder tree (like
  `tree`): folders first, episodes in number order, folders holding a single
  folder merged into one row. Large folders start closed. With 8 or more
  videos, a filter box matches every typed word against the full path. Names
  are never cut off; they wrap at dots, dashes and slashes. On the video
  page, **Other videos** opens the same tree to switch to another video of
  the torrent. The house icon at its top goes back home, and the search
  icon looks up another magnet.
- **Saved videos.** Every video opened is saved in the browser as it's
  recovered. **Saved**, top-left of the home page (`?saved`), lists them,
  most recent first:
  - partly recovered ones are tagged **Partial** with their percentage;
  - opening one resumes where it left off;
  - each can be deleted on its own, or everything at once, with a second
    click to confirm.

  Deleting a video keeps the pieces at its edges that a neighbouring saved
  video still needs. Pieces saved before names were recorded show up by
  info-hash: open one to pick its video again, or delete it.

## How chunk recovery works

- **[magnet-seeders](https://github.com/WinCisky/magnet-seeders)** (VPS)
  streams peers on `/peers` (one request per lookup). The page learns the
  file list and piece hashes from those peers, over the tunnel.
- **The gateway** (Cloudflare Worker) does what the browser can't: it holds
  TCP connections to peers open. The page opens one to three WebSockets and
  speaks peer-wire itself; the worker only relays bytes after the handshake
  gate.
- The engine in `src/lib/torrent/` schedules the requests, assembles the
  pieces, and checks each piece's SHA-1. How it schedules:
  - At the start and after a seek, at most 6 requests run until 4 MiB are
    verified from the playback position: the first MiB in 256 KiB batches
    and the rest in 1 MiB batches, each from a different peer. A new
    connection starts slowly, and parallel requests share the link evenly,
    so this gets the first chunks in sooner.
  - Then requests run in parallel: up to 24 at once, fewer on a slow
    connection.
  - A seek cancels requests far from the new position.
  - Each request is a run of blocks sized to the peer's measured speed,
    because every request pays ~0.2–1 s of setup.
  - Near the playback front, it uses only peers that unchoke quickly.
  - Peers that serve fast get up to 3 parallel connections.
  - Peers that crawl have their blocks handed to others.
  - Peers that unchoke on a timer (Transmission) are given a longer wait
    instead of being marked as failures.
  - Once everything from the front to the file's end is in, the front goes
    back to the file's start and fills the gaps from there. That isn't a
    seek: nothing is cancelled, and requests run at full width, since
    playback isn't waiting.

  Verified pieces are stored in Cache Storage (one cache per torrent, with a
  record of the files opened from it: `src/lib/torrent/library.ts`), so a
  reload resumes where it left off. The view shows every piece's state
  (recovered, downloading, pending, no source, failed check) on a piece map
  and a seek-bar-style strip.

Service URLs default to production. Override them at build/dev time:

```sh
PUBLIC_SEEDERS_URL=http://127.0.0.1:8080 PUBLIC_WORKER_URL=http://127.0.0.1:8787 npm run dev
```

`PUBLIC_DEFAULT_GATEWAY` picks the primary tunnel gateway: `cloudflare`
(default, `PUBLIC_WORKER_URL`) or `vps` (`PUBLIC_VPS_GATEWAY_URL`, default
`PUBLIC_SEEDERS_URL`); the other one is the fallback. Any other value fails
the build (`src/lib/magnet/gateway-config.ts`, docs/operations.md › Default
data path).

On a thin swarm the session asks the VPS gateway to join it (fewer than 2
peers delivering, or a rate under the video's bitrate, for
`PUBLIC_ESCALATION_AFTER_SECONDS`, default 90) and the page says when the
rate stays under the bitrate (`PUBLIC_SLOW_NOTICE_SECONDS`, default 30);
`PUBLIC_VPS_GATEWAY_URL=off` turns escalation off (docs/operations.md ›
Escalation to the VPS gateway).

The file-list lookup waits as long as something is left to try (the
`/peers` stream still open, live or unknown addresses not dialled yet,
dials in flight, peers being asked for the metadata), up to a hard cap:
`PUBLIC_LOOKUP_CAP_SECONDS` (default 120). Meanwhile the page shows the
peers found, tried and connected. A failed lookup keeps its session for two
minutes: *Retry* reuses its open WebSockets and peer book
(`src/lib/torrent/lookup-wait.ts`, `session.worker.ts`).

To see every worker request in the browser console, run
`localStorage.setItem("mw-debug", "1")` and reload.

## Playback

The video page has a player above the recovery details. Press play once the
video's header is read (a few MiB from the file's start or end). A read of
data that isn't in yet waits for its piece and moves the recovery front
there. When the read jumps ahead of the front (a seek ahead), the front
starts one piece earlier, so what's just before the new position comes in
too. So seeking in the player, playing past the front, or a container
index at the end of the file all steer recovery. The piece strip at the
bottom marks where playback last read.

- **Formats** (Chrome as the baseline):

  | Kind | Supported |
  | :-- | :-- |
  | Containers | MKV/WebM, MP4/MOV, AVI, MPEG-TS |
  | Video | H.264, HEVC (8 and 10 bit), MPEG-4 Part 2 (Xvid, DivX), DivX 3, MPEG-2, VP9, AV1 |
  | Audio | AAC, MP3, AC3, EAC3, DTS, Opus, Vorbis, FLAC, PCM |
  | Extras | several audio tracks; text subtitles (SRT, ASS/SSA, WebVTT, TTML), in the video or as files in the torrent |

  There are no decoders for TrueHD, MP2 audio, WMV or RealVideo.
- **Native player and the streaming service worker.** `/stream-sw.js`
  (`src/lib/stream/sw.ts`, bundled by `scripts/stream-sw.mjs`) serves
  `/stream/<info-hash>/<file index>` with `Range` support: `206` with
  `Content-Range`, `200` without a range, `416` past the end, `404` for an
  invalid info-hash or index, or a file no page has open. It stores
  nothing: it asks the page that has the file open (`src/lib/stream/bridge.ts`)
  for the bytes, which reads them from the recovery like the libmedia
  player does, so a read of missing data moves the recovery there and
  waits. MP4 and WebM (`player/native.ts`) play in a plain `<video>` over
  that URL when their codecs allow it, with SRT and WebVTT subtitle files
  shown as WebVTT tracks. The codecs are read from the file's own index
  first (`player/probe.ts`: the `moov` sample descriptions, the WebM
  `Tracks`), and `player/codecs.ts` (`nativePlayable`) asks Chromium about
  each one: HEVC without hardware support, AC3/EAC3, several audio tracks,
  subtitle tracks inside the file, or an index that can't be read go to
  libmedia. If Chromium can't decode the file after all, libmedia takes
  over before anything played. The same URL with `?download=1` is the download link of a
  recovered video (a click opens it in a hidden frame: Chromium sends links
  with a `download` attribute past the service worker). Without the worker
  in control (a hard reload), libmedia plays and the download reads the
  pieces itself.
- **Start and rebuffer thresholds** (`player/buffering.ts`). Play starts
  once the player has read the container index and about 10 s of video
  are verified where it starts (the spinner says "Waiting for data…"
  meanwhile; a one-byte read there points the recovery at it). When
  playback runs dry (the browser's `waiting`, or a libmedia read held up
  for 400 ms), it resumes only with 5 s verified from there, not as each
  piece arrives. Seconds become bytes at the video's average bitrate (its
  size over its duration).
- **Bitrate.** Once the container is read, that bitrate goes to the session
  worker (`bitrate` message): the picker's playback window becomes 30 s of
  it in bytes, and another WebSocket opens after 30 s under twice it (until
  then 10 Mbit/s). The recovery details show the window.
- **Decoding.** Built on [libmedia](https://github.com/zhaohappy/libmedia)
  (`@libmedia/avplayer`). It uses the GPU (WebCodecs) where it can, or
  Chrome's own player (MSE) when the codecs are ones Chrome plays.
  Otherwise it decodes with FFmpeg compiled to wasm, one decoder per codec,
  fetched only when a video needs it: HEVC on machines without hardware
  support, DivX/Xvid, and AC3/EAC3/DTS audio. Nothing is re-encoded.
- **Cross-origin isolation.** libmedia needs SharedArrayBuffer to run on
  real threads. Without it, its workers can't pass subtitles to the page,
  and wasm decoding is single-threaded. The site is served by Cloudflare
  Pages, which sends the COOP/COEP headers from `public/_headers`; the dev
  server sends them from `astro.config.mjs`. No service worker is needed
  for it: the page is isolated on its first load.
  - Requests to other origins must allow CORS, as magnet-seeders and the
    worker do. Anything
    new loaded from another origin (images, fonts) must allow CORS or send
    `Cross-Origin-Resource-Policy`.
  - If isolation fails (headers missing), the Subtitles menu says to
    reload. The diagnostics record `isolated` in the
    environment.
- **Controls.** Seek bar with the downloaded parts shaded (bytes mapped
  linearly to time, so approximate), volume, full screen. Keys: space or
  `k`, `←`/`→` (10 s), `f`, `m`, and `c` for subtitles on/off.
- **Audio and subtitles.** With several audio tracks, the **Audio** menu
  picks one. Tracks are named by language and title, with codec and
  channels ("Italian · AC3 5.1"). The **Subtitles** menu lists:
  - **Off**;
  - the video's own subtitles. Image subtitles (PGS, VobSub) are listed but
    greyed out, since only text subtitles can be shown;
  - under **Subtitle files**, the torrent's `.srt/.ass/.ssa/.vtt/.ttml`
    files for this video. They are recovered only when picked, kept in
    memory and not saved. A file counts as the video's when:
    - its name starts with the video's (`Movie.it.srt`);
    - or it's in a `Subs` folder, under the video's name
      (`Subs/Show.S01E02/2_English.srt`);
    - or the torrent has a single video.
    The language and flags (forced, SDH) come from the file name.

  Subtitles start off unless the video flags one of its tracks as default
  (or titles one "forced").
- **Code.** The player and recovery are independent:
  - `src/lib/player/` reads from any `ByteSource` (`source.ts`) and knows
    nothing about torrents;
  - `src/lib/torrent/stream.ts` serves a file's bytes from its verified
    pieces, and knows nothing about players;
  - the video page joins them, and `src/lib/boundaries.test.ts` fails if
    either side imports the other.
- **Codec files.** `scripts/fetch-codecs.mjs` runs before `dev` and
  `build`. It downloads libmedia's wasm decoders (from jsDelivr, or GitHub
  if that fails) into `public/libmedia/` (gitignored), pinned to the
  version and SHA-256s in `scripts/codecs.json`, so the site serves them
  itself. After upgrading `@libmedia/avplayer`, set
  the new version there and run `node scripts/fetch-codecs.mjs --update`.
  `scripts/vite-libmedia.mjs` copies libmedia's lazily loaded format chunks
  next to its bundle.
- **Licenses.** libmedia is LGPL-3.0-or-later, used unmodified from npm.
  Its wasm decoders are built from [FFmpeg](https://ffmpeg.org) (LGPL).

## Diagnostics

The page records anonymous measurements of how each part of it performs.
They stay in the browser (IndexedDB) until the user exports them from the
home page's **Diagnostics** menu as JSON. The same menu deletes them.
Nothing is ever sent anywhere. The code is in `src/lib/diagnostics/`.

- **Visits.** One record per page load that did something:
  - what the user did (magnet submitted, file picked, saved video opened…);
  - magnet-seeders' `/files` (the magnet's file list) and `/peers`
    requests: outcomes, latency and errors;
  - uncaught errors;
  - the browser, OS and device class.
- **Recoveries.** One record per video opened:
  - milestones: piece hashes, first peers, first data, 1/4/16 MiB ready;
  - `/metadata` and `/swarm` outcomes, and the largest swarm size seen;
  - worker requests by how they ended, time to unchoke, why candidates
    failed, and which clients served the data;
  - speed per 10 s window, stalls (5 s or more with no data), time from a
    seek to 4 MiB ready, failed piece checks, and storage errors;
  - the player: how it decoded (the browser's player or libmedia's
    decoders) and the codecs, time to read the header and from play to the
    first picture, waits for data, seek times, decoder stutters, and errors;
    how many audio tracks, subtitles and subtitle files the video offered,
    which were picked, and how subtitle file downloads went.
- **Privacy.** There are no magnet links, file or torrent names, info-hashes
  or IP addresses. Error text is scrubbed of all of them, and sizes are
  rounded. A torrent appears only as a hash salted with a secret that never
  leaves the browser, so one torrent's recoveries can be grouped without
  revealing which torrent it is. `installId` is a random id that tells one
  browser's exports apart.
- **Cost.** Recording bumps counters in memory, at points that already run
  (a request ending, the 200 ms progress snapshot): about 0.3 µs per snapshot
  and 2 µs per request. The visit is written at most every 15 s, when the
  browser is idle, and when the page is hidden. A recovery's record is
  ~1.5 KB. At most 300 visits, up to 90 days old, are kept. The report code
  loads only on export.
- **The export.** `weakSpots` and `workingWell` summarize the verdicts:
  - `health` grades each part (lookup, seeder count, piece hashes, peer
    discovery, worker requests, startup, download speed, seeking, integrity,
    playback, storage, page errors) as good, fair or poor. The limits are in
    `thresholds`. Each part has a one-line summary and details;
  - `visits` holds the raw records;
  - histograms are counts per bucket, with the bounds in `buckets`.

## Commands

| Command | Action |
| :-- | :-- |
| `npm install` | Install dependencies |
| `npm run dev` | Dev server at `localhost:4321` |
| `npm run build` | Build to `./dist/` |
| `npm test` | Unit tests (vitest) |
| `npx astro check` | Type-check |
