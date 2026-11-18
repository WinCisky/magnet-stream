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
  `measurementRows`. Nothing is sent anywhere.
- **Synthetic, every CI run.** `npm run e2e` (browser session) recovers a
  128 MiB file from a fixed mixed swarm and fails if billed requests per GB
  exceed the budget. Its numbers are written to
  `tools/e2e/results/browser-session.json`, kept as the `measurements`
  artifact of the CI run for 90 days.
- **Real-world, at each manual milestone review** (M2-11, M3-07, M4-09,
  M5-05). A few real torrents are watched and their rows are added below.

Rows carry no info-hash, magnet, file name or address. Sizes are rounded to
two significant digits.

## Synthetic (CI browser e2e)

Swarm: 2 fast seeds, 1 slow seed, 1 partial peer (first half), 2 peers that
never unchoke, 3 dead addresses. Everything runs locally (fake peers,
`wrangler dev`, `astro dev`, Chromium), so only number 3 says much about
production; 1 and 2 catch regressions.

| Date | Milestone | Commit / where | File | First piece (s) | MB/s | Requests/GB (page) | Requests/GB (counted) | Budget |
|---|---|---|---|---|---|---|---|---|
| 2026-11-18 | M2 (M2-09) | working tree, local run | 128 MiB | 1.2 | 6.6 | 28.3 | 28.3 (1 WebSocket, 56 messages) | 40 |

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

### M2 review notes (2026-11-18, the four rows above)

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
  The file-list timeout is now 60 s.

To do: a fourth series on the current build, rows here.

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
