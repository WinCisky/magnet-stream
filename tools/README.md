# tools/

Development and test tooling (Node, never shipped to users).

| tool | what |
|---|---|
| `fake-peer` | deterministic test torrents (`makeTorrent`) and a scriptable BitTorrent peer (`startFakePeer`, CLI `npm run fake-peer`): seed, partial, unchoke delay, never unchoke, choke midway, close, corrupt data, `reqq`, `ut_metadata`, `ut_pex`. Loopback and ephemeral ports by default; payload is pseudo-random noise from the seed. |
| `harness` | free ports, groups of fake peers, `startWranglerDev` (own state dir per instance, killed with its workerd) |
| `e2e` | hermetic download of a 32 MiB torrent through `wrangler dev` with the engine (`npm run e2e`) |
| `spike` | Phase 0 measurement (`npm run spike`); see `docs/spike-report.md` |
