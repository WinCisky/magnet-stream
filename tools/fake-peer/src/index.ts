// @magnet-stream/fake-peer: deterministic test torrents and a scriptable
// BitTorrent peer (TCP, loopback by default). Test tooling only.

export * from "./torrent.ts";
export * from "./peer.ts";
export { bencode, bdecode, type BValue, type BDecoded } from "./bencode.ts";
