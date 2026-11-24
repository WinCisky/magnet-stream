# Phase measurements

Each delivery phase is measured on the same three numbers
(docs/architecture.md §Risks, open questions and delivery):

| # | Number | Definition (as computed in `apps/web/src/lib/diagnostics/phase.ts`) |
|---|---|---|
| 1 | **Seconds to first verified piece** | magnet lookup (paste → file list) + time from opening the video to its first verified piece. The time spent choosing a file is not counted. Only videos not saved before. Median over videos. |
| 2 | **Sustained MB/s** | bytes received ÷ time spent recovering, over all videos (MB = 10^6 bytes). |
| 3 | **Billed requests per GB** | (WebSocket upgrades + WebSocket messages sent ÷ 20) ÷ (bytes received ÷ 10^9). Durable Object billing counts 20 incoming messages as 1 request. |

**Budget:** at most **40 billed requests per GB**, for every kind of swarm
(thin ones included; decision of 2026-11-18). The browser e2e fails above it.

Where the numbers live (decision OQ-19, option C):

- **In the browser.** The *Diagnostics* menu on the home page shows the
  three numbers over everything recorded in this browser. *Copy measurement
  row (last video)* copies one anonymous row for the real-world table
  below. The exported JSON report carries `phaseMetrics` and
  `measurementRows`, and per torrent session the lookup (where the file
  list came from, retries, why a lookup gave up), the tunnel's dials by
  class (scout-alive, untried, known) and outcome with the peers connected,
  unchoked, delivering and seeds, and the scout's `/peers` stream (alive,
  dead, unknown, the `info` line accepted or rejected): the `lookup`,
  `tunnel` and `scout` sections. The tunnel section also says what the
  session uploaded (bytes, peers unchoked, messages sent only for the
  upload, the setting), shown in the menu as *Sharing with peers*, and
  (from M4-08) the bytes by gateway path: received and sent through the
  Cloudflare gateway and through the VPS gateway, with the connections on
  each (`tunnel.paths`; totals and the VPS share in the `tunnel` health
  section), shown in the menu as *Data path (last session)*. Nothing is sent anywhere.
- **Synthetic, every CI run.** `npm run e2e` (browser session) recovers a
  256 MiB file (128 MiB until M3-07) from a fixed mixed swarm and fails if billed requests per GB
  exceed the budget. Its numbers are written to
  `tools/e2e/results/browser-session.json` (with `pathBytes`: bytes by
  gateway path as the page reports them, all on the primary gateway), kept
  as the `measurements` artifact of the CI run for 90 days.
- **Few seeders, every CI run** (M4-08). `npm run e2e:few-seeders -w
  tools/e2e` runs four scenarios on the full local stack (wrangler dev as
  the primary gateway, the VPS gateway with an inbound port, a fake tracker,
  fake peers; all on loopback, nothing announced to public trackers or the
  DHT), each ending with every piece verified against the source, and checks
  the bytes the diagnostics give for each path (table below). **Declared
  time: 120 s** for the four scenarios once the stack is up
  (`FEW_SEEDERS_BUDGET_S`); the run fails above it. `npm run e2e:few`
  checks the same split in the browser (Diagnostics menu and exported
  report).
- **Real-world, at each manual milestone review** (M2-11, M3-07, M4-09,
  M5-05). A few real torrents are watched and their rows are added below.

Rows carry no info-hash, magnet, file name or address. Sizes are rounded to
two significant digits.

## Synthetic (CI browser e2e)

Swarm: 2 fast seeds, 1 slow seed, 1 partial peer (first half), 2 peers that
never unchoke, 3 dead addresses; from M3-12 also a tit-for-tat leecher
(second half) that the page uploads to ("Share with peers" on, the
default), so the counted requests include the upload's flushes. The run
also checks that with the setting off the leecher gets no bitfield, have,
unchoke or piece. From M3-07 the file is 256 MiB: at 128 MiB the 3
WebSocket upgrades alone weighed ~22 billed requests per GB, and a slower
CI runner (10.5 MB/s) went over 40 (41.7); with fast peers kept 4 MiB
deep, 128 MiB also took only ~5 s locally, too short for the tit-for-tat
exchange (upload alone is flushed every 2 s). Everything runs locally
(fake peers,
`wrangler dev`, `astro dev`, Chromium), so only number 3 says much about
production; 1 and 2 catch regressions.

| Date | Milestone | Commit / where | File | First piece (s) | MB/s | Requests/GB (page) | Requests/GB (counted) | Budget |
|---|---|---|---|---|---|---|---|---|
| 2026-11-18 | M2 (M2-09) | working tree, local run | 128 MiB | 1.2 | 6.6 | 28.3 | 28.3 (1 WebSocket, 56 messages) | 40 |
| 2026-11-21 | M3 (M3-12) | working tree, local run | 128 MiB | 1.4 | 16.6 | 34.9 | 34.6 (3 WebSockets; 0.39 MB uploaded, 5 upload-only messages) | 40 |
| 2026-11-22 | M3 (M3-07) | working tree, local run | 256 MiB | 1.4 | 25.4 | 19.8 | 19.7 (3 WebSockets; 1.0 MB uploaded, 5 upload-only messages) | 40 |
| 2026-11-22 | M3 (M3-07) | working tree, local run on one CPU core | 256 MiB | 1.5 | 11 | 26.3 | 26.3 (3 WebSockets; 2.1 MB uploaded, 6 upload-only messages) | 40 |

### Few-seeders scenarios (CI e2e, M4-08)

Escalation after 2 s of few peers (the page's default is 90 s). Local run,
2026-11-24 (scenarios 1–3 run together, then 4):

| Scenario | Swarm | Through Cloudflare | Through the VPS | Time |
|---|---|---|---|---|
| nat-seed | 1 seed behind NAT (dials out to the tracker's addresses) | 0 | 4.0 MiB (1 inbound) | 4.4 s |
| late-unchoke | 1 reachable seed that unchokes after 20 s | 2.0 MiB (1 connection, kept) | 0 | 21.7 s |
| partial-mix | no seed: 2 reachable leechers (½ each, overlapping) + 1 NATed leecher (second ½) | 3.0 MiB | 1.0 MiB | 11.3 s |
| vps-crash | 1 reachable leecher (first ¼) + 1 NATed seed; VPS gateway killed at ¼ via the VPS, back 1 s later | 1.0 MiB | 3.0 MiB (2 inbound, 2 WebSockets) | 17.7 s |

Total 39 s against the declared 120 s.

## Real-world

Columns follow *Copy measurement row (last video)*: date, build, file size,
peers (known / delivered data), seconds to first verified piece, MB/s,
billed requests per GB, outcome. Add a *swarm* note (healthy, few seeders…)
and where it ran (production, or local stack against real peers).

| Date | Build | File | Peers (known / delivered) | First piece (s) | MB/s | Requests/GB | Outcome | Milestone | Swarm | Where |
|---|---|---|---|---|---|---|---|---|---|---|
| 2026-11-18 | 2026-11-18T16:46Z | 740 MB | 61 / 1 | 4.1 | 0.3 | 121.0 | left | M2 | few seeders | local stack, real peers |
| 2026-11-18 | 2026-11-18T17:08Z | 2600 MB | 59 / 2 | 68.2 | 0.2 | 333.3 | left | M2 | few seeders | local stack, real peers |
| 2026-11-18 | 2026-11-18T17:08Z | 1000 MB | 55 / 1 | 2.1 | 5.9 | 21.5 | left | M2 | many seeders | local stack, real peers |
| 2026-11-18 | 2026-11-18T17:08Z | 1900 MB | 302 / 2 | 51.6 | 2.3 | 45.8 | left | M2 | many seeders | local stack, real peers |
| 2026-11-18 | 2026-11-18T17:51Z | 740 MB | 51 / 1 | 4.4 | 0.1 | 1489.9 | left | M2 | few seeders | local stack, real peers |
| 2026-11-18 | 2026-11-18T17:51Z | 2600 MB | 55 / 1 | 61.8 | 0.5 | 288.4 | left | M2 | few seeders | local stack, real peers |
| 2026-11-18 | 2026-11-18T17:51Z | 1000 MB | 44 / 1 | n/a (resumed) | 5.6 | 20.7 | left | M2 | many seeders | local stack, real peers |
| 2026-11-18 | 2026-11-18T17:51Z | 1900 MB | 336 / 4 | 64.0 | 0.4 | 171.1 | left | M2 | many seeders | local stack, real peers |
| 2026-11-18 | 2026-11-18T22:48Z | 2600 MB | 48 / 2 | 18.0 | 1.7 | 41.1 | left | M2 | few seeders | local stack, real peers |
| 2026-11-18 | 2026-11-18T22:48Z | 1000 MB | 49 / 1 | 24.7 | 5.5 | 23.1 | left | M2 | many seeders | local stack, real peers |
| 2026-11-18 | 2026-11-18T22:48Z | 740 MB | 56 / 2 | 35.2 | 2.0 | 56.9 | left | M2 | few seeders | local stack, real peers |
| 2026-11-18 | 2026-11-18T22:48Z | 1900 MB | 406 / 2 | 4.2 | 4.4 | 22.6 | left | M2 | many seeders | local stack, real peers |
| 2026-11-22 | 2026-11-22T14:04Z | 740 MB | 62 / 2 | 1.8 | 4.1 | 14.1 | left | M3 | few seeders | local stack, real peers |
| 2026-11-22 | 2026-11-22T14:04Z | 2600 MB | 48 / 0 | n/a | 0.0 | n/a | left | M3 | few seeders (1 alive, stopped serving) | local stack, real peers |
| 2026-11-22 | 2026-11-22T14:04Z | 1000 MB | 36 / 2 | 2.0 | 9.3 | 14.3 | left | M3 | many seeders | local stack, real peers |
| 2026-11-22 | 2026-11-22T14:04Z | 1900 MB | 436 / 12 | 9.0 | 8.1 | 19.0 | left | M3 | many seeders | local stack, real peers |
| 2026-11-22 | 2026-11-22T14:04Z | 2600 MB | 48 / 0 | n/a | 0.0 | n/a | left | M3 | few seeders (1 alive, stopped serving) | local stack, real peers |
| 2026-11-22 | 2026-11-22T14:36Z | 88000 MB | 2104 / 11 | 25.9 | 9.4 | 20.4 | left | M3 | many seeders | local stack, real peers |
| 2026-11-22 | 2026-11-22T14:36Z | 4700 MB | 2576 / 17 | 13.9 | 8.9 | 20.9 | left | M3 | many seeders | local stack, real peers |
| 2026-11-22 | 2026-11-22T14:36Z | 3400 MB | 3000 / 20 | 3.9 | 10.5 | 16.7 | complete | M3 | many seeders | local stack, real peers |
| 2026-11-22 | 2026-11-22T14:36Z | 4400 MB | 61 / 3 | 12.8 | 8.5 | 17.9 | left | M3 | few seeders | local stack, real peers |
| 2026-11-22 | 2026-11-22T14:36Z | 9200 MB | 3000 / 11 | 15.0 | 9.1 | 20.3 | running (from the report) | M3 | many leechers, no seed connected | local stack, real peers |
| 2026-11-22 | 2026-11-22T14:36Z | 6100 MB | 3 / 1 | 45.9 | 0.07 | 436 | running (from the report) | M3 | one slow seed | local stack, real peers |

### M2 review notes (2026-11-18, the first series of rows above)

Totals of the second series (3 videos): first piece median 51.6 s, 1.23 MB/s,
75.2 billed requests per GB (58.4 requests for 776 MB): **over the budget**.
2 of 5 lookups ended in "metadata timed out". The gateway logs showed what
went wrong; these builds predate the fixes below.

- **Dials refused by the gateway.** Out of 1,729 `open`s in the second
  series, 18 connected; most of the rest were refused with
  `too_many_connections`. The gateway keeps the slot of a connect that timed
  out for 20 s (OQ-03), but the client counted it as free: each sweep
  re-dialled into a full socket and penalised good candidates as "busy".
  Only 1–2 peers ever delivered, even with 302 known.
  *Fixed:* the tunnel client counts the slots the gateway still holds (20 s
  after a timed-out connect, 1 s after a close); a refusal for lack of slots
  no longer counts against the peer.
- **One upstream message every 400 ms whatever the speed.** Each block
  received triggered a new request, so a 0.2 MB/s session paid as much per
  second as a 6 MB/s one (333 requests/GB). *Fixed:* a peer's queue is
  refilled only once half of it has drained (one message asks for
  megabytes); while a peer delivers, new dials go out at most every 5 s;
  keep-alives go out together in one message instead of one timer per peer.
  On a 0.4 MB/s test peer: 5 messages for 4 MiB instead of 16.
- **One WebSocket only.** No rate threshold was passed, so a slow session
  never opened a second socket (6 more slots). *Fixed provisionally:* a
  second socket after 30 s under 10 Mbit/s, until the player reports the
  bitrate (M3-06).
- **Counters.** The page counted 651 messages for a session the gateway saw
  786 for: counters only reached the page after a finished batch. They are
  now also sent every 5 s.

### Second series (build 2026-11-18T17:51Z, first round of fixes)

Same torrents. Totals: first piece median 61.8 s, 0.47 MB/s, 338.9 billed
requests per GB (267 requests for 788 MB). All 4 lookups succeeded (2 of 5
timed out before).

- **Better:** `too_many_connections` became rare (a few per minute instead of
  bursts of dozens); sessions opened up to 17 peers instead of 2–9, and up
  to 4 delivered; second and third WebSockets opened on slow sessions.
- **Worse: message storms from cancels.** The 740 MB session sent 3,311
  messages in 813 s (4 per second, above what the 400 ms batching allows):
  every block the page's scheduler gave up on (its 20 s request timeout,
  hedges) was cancelled with an "urgent" message of its own, then
  requested again. *Fixed:* no cancel is sent for a block the page dropped;
  it stays requested, and if it arrives with nobody waiting it is kept
  (up to 32 MiB) for the page's next request. Urgent frames sent in the same
  task now share one message.
- **Worse: one healthy swarm slowed to 0.4 MB/s** (2.3 before). Refilling
  every peer to half of its full `reqq` let a slow peer take hundreds of
  blocks of a batch the page then waited on. *Fixed:* each peer's queue is
  sized to 8 s of its own measured rate (16 blocks until known, at most its
  `reqq`), refilled at half: slow peers hold little, fast peers still get
  megabytes per message.
- **Slot accounting:** the remaining `too_many_connections` came from our
  slot release running slightly ahead of the gateway's. *Fixed:* a 1 s grace
  on every failed open and on the 20 s hold.

### Third series (build 2026-11-18T22:48Z, second round of fixes)

Same torrents. Totals: first piece median 18.0 s (p90 35.2 s), 2.55 MB/s,
**35.0 billed requests per GB** (41.3 requests for 1.18 GB): within the
budget for the first time. Every video recovered at 1.7–5.5 MB/s, the
thin swarms included; the gateway saw 80–301 upstream messages per session
instead of thousands. Two thin-swarm rows remain above 40/GB (41.1, 56.9);
the budget stays 40/GB for every swarm (decision of 2026-11-18).

- **Lookups that timed out.** 2 of 6 lookups ended in "metadata timed out"
  (both succeeded on retry, in ~24 s). The gateway logs show why: each
  failed lookup tried only 9 addresses in its 30 s, all dead. With every
  connect timing out at 5 s and holding its slot for 20 s, one WebSocket
  with at most 3 untried dials can try ~9 addresses in 30 s. *Fixed:* while
  nothing is connected, every slot is busy and untried addresses remain, the
  session opens another WebSocket every 5 s, up to 3 (the "exploration
  socket" of OQ-03): about three times as many addresses in the same time.
  The file-list timeout is now 60 s. *Superseded (M3-10, OQ-21, OQ-23):*
  the VPS scout ranks the addresses; ones it saw alive may take all 6 slots
  of a socket (untried ones at most 3, a timed-out connect's held slot
  counting as in flight), and while nothing has been delivered and the
  candidates outnumber the free slots the second and third WebSocket open
  at once. `npm run e2e:scout -w tools/e2e` (part of `npm run e2e`) prints
  the dials by class and outcome and the upstream messages.

### Fourth series (builds 2026-11-22T14:04Z and 14:36Z, M3 review M3-07)

M3 build (scout, VPS `info` line, deadline picker, OPFS + IndexedDB, upload
on), local stack against real peers, Chrome 158 on Linux, two diagnostics
reports: the four M2 torrents again (7 sessions), then six new torrents
(10 sessions), from 48 to 3,000 known peers. The last two rows above are
computed from the exported report with the definitions of this page (their
recoveries were still running, so the menu did not offer a row).

Totals of the second report (8 recoveries, 12.1 GB): first piece median
**13.9 s** (p90 45.9 s), **5.9 MB/s**, **20.8 billed requests per GB** (252
requests: 23 upgrades, 4,576 messages); first report: 2.0 s, 5.35 MB/s,
19.7/GB. No time without data for 98% of the recovering time.

| Torrent | M2, third series (known / delivered, MB/s, req/GB) | M3 |
|---|---|---|
| 740 MB, few seeders | 56 / 2, 2.0, 56.9 | 62 / 2, 4.1, 14.1 |
| 1000 MB, many seeders | 49 / 1, 5.5, 23.1 | 36 / 2, 9.3, 14.3 |
| 1900 MB, many seeders | 406 / 2, 4.4, 22.6 | 436 / 12, 8.1, 19.0 |
| 2600 MB, few seeders | 48 / 2, 1.7, 41.1 | 48 / 0 (9.9 MB, then the only seed left) |

Against the 2026-11-19 diagnostics that motivated OQ-21 (4400 MB, 1,582
known / 2 delivered, 0.07 MB/s): the large swarms now have 11–20 peers
delivering out of 2,104–3,000 known, at 8.9–10.5 MB/s; a 4400 MB few-seeder
swarm (61 known) ran at 8.5 MB/s with 3 delivering. 0.07 MB/s is now only
seen on a swarm with a single slow seed (6100 MB, 3 known).

- **Scout.** 15 of 15 `/peers` streams answered. Dials to scout-alive
  addresses connected 86% of the time (154/180), to remembered ones 85%
  (112/132), to untried ones 3% (6/201): the scout is what makes the
  dials work. Median on the second report: 1,963 peers known, 30 alive,
  17 connected, 8 delivering.
- **File list (12 distinct magnets, 17 lookups).** Listed for 10 of 12
  magnets (15 of 17 lookups). Sources: IndexedDB cache 5 (≤ 1 ms), VPS
  `info` line 9 (4 ms – 7.5 s, median 1.5 s), peers (BEP 9) 1 (4.5 s,
  the VPS had no `info` line for that swarm of 3). The two failures were
  swarms without a seed: one stopped at the 120 s cap after 163 untried
  dials (scout: 3,112 addresses, 0 alive, 1,313 unknown, first line after
  29 s); the other ended "exhausted" with 3 alive partial peers that never
  gave the info dict (2 retries, no reopened sockets).
- **Upload.** On the leecher swarm (9200 MB, no seed connected) the page
  uploaded 1.67 MB in 3 sessions to up to 4 unchoked peers, with 53
  messages counted for the upload alone; healthy swarms stayed at 17–21
  requests/GB with upload on. No upload when every peer is a seed, as
  expected. The setting off was checked by the e2e only.
- **Resume.** The 9200 MB video, reopened twice: file list from the cache
  (≤ 1 ms), 4 MiB ready at once, remembered peers dialled first (85%
  connected), recovery picked up at 17% and 22%.
- **Integrity.** 2 failed piece checks in 11.5 GB, nobody excluded (one
  failure each: the rule needs two).
- **2600 MB torrent.** One live seed among 48; it served 9.9 MB, then
  closed every connection. Effectively dead; the page waited 130 s without
  saying so.
- **VPS.** 58 MiB RSS after hours of use; systemd reported a 53.4 MiB peak
  over 1 day 16 h (budget ~150 MiB, OQ-22). Logs: no peer addresses,
  info-hashes or magnets.

Problems found:

1. **Thin swarms break the 40/GB budget** (*fixed in the review*, see
   below). The 6100 MB session (one seed,
   0.07 MB/s) cost ~436 requests/GB: the socket that delivered sent a
   message every ~2 s, and two more WebSockets stayed open for 14 minutes
   with no connection, ~28 keep-alive messages each (gateway log:
   `opens 0`, `messagesIn 28`, 858 s). Idle extra sockets are not closed,
   and at low rates the request refill (the picker's 4 s of the peer's
   rate, refilled at half) produces messages per unit of time, not per
   byte. *Fixed:* the fastest peers are kept at least 4 MiB deep (at most
   60 s of their rate), so a refill asks for 2 MiB; a window block held by
   a peer that is no longer among the fastest is asked of a fast one at
   once. In the picker simulation a lone 70 KB/s peer now costs 31
   messages for 64 MiB instead of 453 (~25 billed requests per GB instead
   of ~350). The low-rate rule adds a WebSocket only while candidates
   outnumber the free slots, and a surplus WebSocket left without any
   connection for 25 s is closed (never the last one).
2. **`astro dev` pages are not cross-origin isolated.** The page's HTML in
   dev has no COOP/COEP (`curl -I http://localhost:4321/`): `server.headers`
   in `astro.config.mjs` reaches Vite's assets, not Astro's pages. 8 of 9
   visits reported `isolated: false`. libmedia then runs without
   threads: the likely cause of the HEVC numbers (7.9 decoder stutters
   per minute, 38 s to first picture on the 88 GB HEVC/DTS file). The e2e
   serves the built site with `_headers`, so production is not affected;
   the local measurement stack is.
3. **Startup milestones are lost** (*fixed in the review*).
   `ready1MiB/4MiB/16MiB` were only marked while the recovery had had no
   seek, and reading a container index at the end of the file (MKV cues,
   AVI idx1) moves the cursor there and back, counted as a seek (e.g.
   4400 MB, 9200 MB: 1 recovery seek, 0 player seeks). *Fixed:* they are
   marked while the cursor is within 16 MiB of the file's start.
4. **The DHT library's log prints the host's own public address**
   (`mainline::rpc` at INFO, on the VPS too), and `DHT bootstrap complete`
   is logged right after `Could not bootstrap the routing table`; the VPS
   logs that error again every few hours.
5. **Quota.** Usage 110 GB: an 88 GB MKV with its index at the end reserves
   88 GB (sparse files, see docs/storage.md › Quota).

M3-07 criteria: large and thin swarms measured ✅; file list 10 of 12
magnets (83%) ❌ against 95%, accepted at the review: both failures are
swarms without a seed (no live address, or live partial peers that never
gave the info dict), every magnet with a reachable seed was listed; on
large swarms more than 2 peers deliver and data is missing 2% of the time
✅; upload observed ✅, setting off verified by the e2e; 17–21/GB on
healthy swarms ✅, the single-slow-seed swarm fixed in the review (problem
1, not re-measured on that swarm); VPS memory ✅; automatic save, resume
and deletion from *Saved* ✅. Problems 2, 4 and 5 stay open for M4.

## How to take real-world measurements

### Local stack against real swarms (before the first deploy)

Three terminals, from the repository root:

```sh
# 1. Discovery (VPS service) on :8080, answering the local site only
ALLOWED_ORIGINS=http://localhost:4321 BIND_ADDR=127.0.0.1:8080 \
  cargo run --release -p magnet-seeders

# 2. Tunnel gateway (wrangler dev) on :8787
cp -n apps/gateway/.dev.vars.example apps/gateway/.dev.vars
npm run dev -w apps/gateway

# 3. The site on :4321, pointed at both
PUBLIC_SEEDERS_URL=http://127.0.0.1:8080 PUBLIC_WORKER_URL=http://127.0.0.1:8787 \
  npm run dev -w apps/web
```

Note that `wrangler dev` dials peers from your machine, not from
Cloudflare's network. The billed-request count is the same; speed and
reachability are those of your connection.

### Production (after the first deploy, docs/operations.md)

Open the site's `SITE_URL`.

### For each torrent (3–5 per review: healthy, average, few seeders)

1. In a fresh Chromium profile (or after *Diagnostics › Delete
   diagnostics*), paste the magnet; pick the video if asked.
2. Let the recovery reach **complete**. Playing is optional; seek as little
   as possible.
3. Click **Home**, open **Diagnostics**, read the three numbers, then
   **Copy measurement row (last video)** and paste the row into the table
   above. Add the milestone, a swarm note and where it ran.
