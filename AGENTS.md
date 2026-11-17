# magnet-stream — agent guide

Magnet streaming in the browser through persistent tunnels: a full
BitTorrent client in the page, a tunnel gateway on Cloudflare Durable
Objects, discovery and a fallback gateway on a VPS; no database, no
server-side storage. Design: `docs/architecture.md`. Work plan:
`implementation-plan.json` (milestones M1…M5, one task per session).

Before starting a task, read this file, `docs/architecture.md` and every
document the task cites.

## Layout

| path | what | origin |
|---|---|---|
| `apps/web` | Astro + Svelte site (player, UI, legacy recovery engine) | PoC magnet-watcher |
| `apps/gateway` | Cloudflare Worker: tunnel Durable Object (`/v1/tunnel`) + legacy `/v1/blocks` | PoC magnet-worker |
| `services/seeders` | Rust VPS service: peer discovery (trackers + DHT), metadata | PoC magnet-seeders |
| `packages/tunnel-protocol` | frame codec of the browser ↔ gateway tunnel (no runtime deps) | new (M1-04) |
| `packages/engine` | browser BitTorrent engine: tunnel client, peer-wire, verified download | new (M1-06) |
| `tools/fake-peer` | deterministic test torrents + scriptable BitTorrent peer | new (M1-03) |
| `tools/harness` | test helpers: free ports, swarms, `wrangler dev` | new (M1-03) |
| `tools/e2e` | hermetic end-to-end download through the gateway | new (M1-07) |
| `tools/spike` | Phase 0 measurement CLI | new (M1-07) |
| `docs/` | architecture, tunnel protocol, spike report | |

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
npm run e2e            # tunnel e2e (fake peer → wrangler dev → engine), gateway integration + legacy e2e
npm run build          # apps/web build, apps/gateway typecheck
npm run lint | typecheck | test
npm run fake-peer -- --port 6881 --size 8MiB --seed 1
npm run spike -- --gateway ws://127.0.0.1:8787 --peer 127.0.0.1:6881 --bytes 64MiB
npm run dev -w apps/gateway        # wrangler dev on :8787 (copy .dev.vars.example to .dev.vars)
npm run test:integration -w apps/gateway
npm run dev -w apps/web            # astro dev
```

Astro dev server: prefer background mode (`astro dev --background`, then
`astro dev stop|status|logs`). Astro docs: https://docs.astro.build.

CI (`.github/workflows/ci.yml`): verify + web build, e2e, dependency audit
(`node scripts/audit-npm.mjs`: high/critical npm advisories fail unless
accepted, with reason and expiry, in `audit-allowlist.json`; `cargo audit`),
gitleaks (working tree and history).

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
