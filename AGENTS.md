# magnet-stream — agent guide

Magnet streaming in the browser through persistent tunnels: a full
BitTorrent client in the page, a tunnel gateway on Cloudflare Durable
Objects, discovery with peer scouting and metadata, and a fallback gateway
on a VPS; no database, no persistent server-side storage (bounded in-memory
caches on the VPS only, OQ-22). The browser uploads to peers (OQ-04). Design: `docs/architecture.md`. Work plan:
`implementation-plan.json` (milestones M1…M5, one task per session).

Before starting a task, read this file, `docs/architecture.md` and every
document the task cites.

## Layout

| path | what | origin |
|---|---|---|
| `apps/web` | Astro + Svelte site (player, UI, legacy recovery engine, streaming service worker `src/lib/stream/`) | PoC magnet-watcher |
| `apps/gateway` | Cloudflare Worker: tunnel Durable Object (`/v1/tunnel`) | PoC magnet-worker |
| `services/seeders` | Rust VPS service: peer discovery (trackers + DHT), scouting (probe + score), metadata; in-memory caches only; fallback tunnel gateway `/v1/tunnel` (`src/tunnel/`), with session announce and inbound peers (`src/tunnel/inbound.rs`); uTP (BEP 29) packet codec, sans-IO connection state machine, LEDBAT congestion control and the UDP socket multiplexer (`src/utp/`, M4-04a/b/c/d), used by the tunnel on the announced port: outbound uTP `open`s and inbound uTP peers (`src/tunnel/stream.rs`, M4-04e); the uTP dial holds the acceptor for the remote's address, for hole punching (`src/tunnel/punch.rs`, M4-05) | PoC magnet-seeders |
| `packages/tunnel-protocol` | frame codec of the browser ↔ gateway tunnel (no runtime deps) | new (M1-04) |
| `packages/engine` | browser BitTorrent engine: tunnel client, connection manager, peer-wire, metadata, peer exchange, verified download, upload (tit-for-tat choker), escalation to the VPS gateway on thin swarms (`escalation.ts`, M4-06), hole punching through relays, BEP 55 (`holepunch.ts`, M4-05) | new (M1-06) |
| `tools/fake-peer` | deterministic test torrents + scriptable BitTorrent peer (seed, partial, tit-for-tat leecher, outbound-only "behind NAT", …) | new (M1-03) |
| `tools/harness` | test helpers: free ports, swarms, `wrangler dev`, the VPS gateway (`startVpsGateway`, with `crash()`/`restart()`; hermetic: `DHT_BOOTSTRAP=none`, no DHT or default-tracker announces, checked at start; `startGateway` by `E2E_GATEWAY`), a fake HTTP tracker (`startFakeTracker`), the few-seeders stack (`startFewSeedersStack`: both gateways + tracker, loopback only) | new (M1-03) |
| `tools/e2e` | hermetic end-to-end download through the gateway (fixtures: a small VP9 MP4); `npm run stack -w tools/e2e [-- --nat]` local stack (`--nat`: only a NATed seed, escalation to the VPS); uTP interop with real clients (`e2e:interop`: libtorrent and Transmission in the Docker image of `tools/e2e/interop/`, M4-04f) | new (M1-07) |
| `tools/spike` | Phase 0 measurement CLI | new (M1-07) |
| `docs/` | architecture, tunnel protocol, discovery API, piece storage (OPFS) and session memory (IndexedDB), spike report, measurements, operations (deploy) | |

npm workspaces (`apps/*`, `packages/*`, `tools/*`) and a Cargo workspace
(`services/seeders`) share the root. Lockfiles (`package-lock.json`,
`Cargo.lock`) are versioned. Rust toolchain pinned in `rust-toolchain.toml`.
Node ≥ 22.18: packages and tools are TypeScript run directly by Node (type
stripping), so they use **erasable syntax only** (no enums, no parameter
properties, `.ts` extensions in relative imports).

## Commands (from the root)

```sh
npm ci                 # install every workspace
npm run verify         # lint + typecheck + TS tests + cargo fmt --check + clippy -D warnings + cargo test
npm run e2e            # tunnel e2e (fake peer → wrangler dev → engine), scout-guided dial, corrupting peer (v1 + v2), the same three through the VPS gateway (e2e:vps), an inbound seed behind NAT through the VPS announce (e2e:inbound), four few-seeders scenarios on the full local stack (NATed seed via announce + inbound, seed late to unchoke, partial leechers that together cover the file, the VPS gateway killed and restarted mid-download; bytes by path Cloudflare / VPS; declared time 120 s; e2e:few-seeders), uTP downloads from libtorrent and Transmission in both connection directions and a BEP 55 hole punch through a libtorrent relay to a libtorrent target (e2e:interop; needs Docker, skipped without it unless REQUIRE_INTEROP_E2E=1), gateway integration (incl. a peer that does not read) + legacy e2e, browser (upload to a tit-for-tat leecher, "Share with peers" off; again with the VPS as primary gateway, e2e:browser:vps), file-list lookup (VPS info line, forged line, retry in the same session, diagnostics report), few seeders (a seed late to unchoke is waited for; only a NATed seed: escalation to the VPS, the "slower than the video" notice; bytes by path in the Diagnostics menu and report; e2e:few), OPFS + resume after a reload (session memory), native MP4 playback through the streaming service worker, MKV through libmedia (start/rebuffer thresholds, bitrate → picker window)
npm run build          # apps/web build, apps/gateway typecheck
npm run lint | typecheck | test
npm run fake-peer -- --port 6881 --size 8MiB --seed 1
npm run spike -- --gateway ws://127.0.0.1:8787 --peer 127.0.0.1:6881 --bytes 64MiB
npm run dev -w apps/gateway        # wrangler dev on :8787 (copy .dev.vars.example to .dev.vars)
npm run test:integration -w apps/gateway
npm run dev -w apps/web            # astro dev
E2E_GATEWAY=vps npm run e2e -w tools/e2e   # an engine e2e through the VPS gateway (cargo build of services/seeders)
npm run e2e:interop -w tools/e2e   # uTP interop: libtorrent + Transmission (Docker) ↔ VPS gateway, both directions
PUBLIC_DEFAULT_GATEWAY=vps npm run e2e     # the browser sessions with the VPS as primary gateway (docs/operations.md › Default data path)
```

Astro dev server: prefer background mode (`astro dev --background`, then
`astro dev stop|status|logs`). Astro docs: https://docs.astro.build.

CI (`.github/workflows/ci.yml`): verify + web build (`check:dist`) + gateway
dry-run bundle, actionlint, e2e (the browser session enforces 40 billed
requests/GB and uploads `tools/e2e/results/` as the `measurements`
artifact; `docs/measurements.md`), dependency audit
(`node scripts/audit-npm.mjs`: high/critical npm advisories fail unless
accepted, with reason and expiry, in `audit-allowlist.json`; `cargo audit`),
gitleaks (working tree and history). Deploy (`.github/workflows/deploy.yml`,
setup in `docs/operations.md`): after a green CI on a push to `main`,
`wrangler deploy` of the gateway and `wrangler pages deploy` of the site
(served from `/` on Cloudflare Pages); the VPS is deployed by hand.

## Global rules (from the plan's `global_rules`)

- The original PoC folders (magnet-watcher, magnet-worker, magnet-seeders)
  are read-only references: never modify them.
- One task per session; every session starts without memory: read
  AGENTS.md, docs/architecture.md and the documents the task cites first.
- Never commit, push or deploy: leave changes in the working tree.
- At the end of every task `npm run verify` exits 0 (and `npm run e2e`
  from M1-07 on).
- No secrets in versioned code or config: environment variables or the
  secret store only (`.dev.vars`, `.env*` are git-ignored; only `*.example`
  files with placeholders are versioned).
- Validate every external input (frames, peer-wire messages, magnets, HTTP
  parameters, data read from browser storage).
- Logs carry no sensitive data: no client IPs, peer addresses, info-hashes,
  magnets or file names.
- If docs/architecture.md and the plan's `resolved_decisions` disagree, the
  `resolved_decisions` win.
- If a task is blocked by an open question, don't assume the answer: stop
  and report it.
- Tasks with `executor: human` are the manual end-of-milestone reviews.

## Gateway specifics

- `ALLOW_PRIVATE_PEERS=1` (tunnels may reach 127.0.0.1/private peers) is
  for `wrangler dev` only: put it in `.dev.vars` or pass `--var`; it is
  honoured only for requests to a loopback host and must never appear in
  `wrangler.jsonc` (a test enforces it).
- The Durable Object stores nothing.

## VPS gateway specifics

- `services/seeders` serves `/v1/tunnel` with the same protocol, checks and
  `TUNNEL_*` variables as the Cloudflare gateway (docs/tunnel-protocol.md);
  `ALLOW_PRIVATE_PEERS` (`1`/`true`) reaches tunnels only for direct
  loopback requests without the proxy's client-IP header.
- On a session's `announce` frame (M4-02) it announces its port to trackers
  and the DHT and accepts inbound peers (`TUNNEL_INBOUND_ADDR`; unset =
  off): only while a session asks, never before. The same port number
  takes uTP on UDP (M4-04e: inbound peers, and outbound `open`s with
  transport uTP of sessions that announce the info-hash), so it must
  differ from `DHT_PORT`; the Cloudflare gateway fails uTP `open`s with
  UNSUPPORTED_TRANSPORT.
- Hole punching (M4-05, BEP 55) adds no frame: the engine sends the
  rendezvous inside peer-wire `data` and then a uTP `open`; the gateway
  only dials (docs/tunnel-protocol.md › Hole punching). The engine never
  relays for other peers.
