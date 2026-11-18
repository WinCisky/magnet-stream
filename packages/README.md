# packages/

Shared libraries, consumed as TypeScript sources through npm workspaces
(`exports` point at `src/*.ts`; erasable syntax only, so Node can run them
without a build step).

| package | what |
|---|---|
| `tunnel-protocol` | frame codec of the browser ↔ gateway tunnel; spec in `docs/tunnel-protocol.md`, binary vectors in `fixtures/` |
| `engine` | browser BitTorrent engine: `Transport`, `TunnelClient`, `ConnectionManager`, metadata (BEP 9), `ut_pex` / `PeerBook`, verified download (v1 and v2); also hosts the torrent codecs moved from apps/web (`bencode`, `metainfo`, legacy `blocks-stream`) |

Runtime code may only use web platform APIs (each package's `tsconfig.json`
has no Node types); tests use `tsconfig.test.json`.
