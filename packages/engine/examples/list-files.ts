#!/usr/bin/env node
// Print a torrent's files starting from its info-hash (or magnet) alone:
// connect to one peer, fetch the info dict over ut_metadata (BEP 9), check
// its hash, list the files as the page would.
//
//   npm run fake-peer -- --port 6881 --size 8MiB --seed 1
//   npm run list-files -w packages/engine -- --peer 127.0.0.1:6881 --ih <info-hash or magnet>
//   ... --gateway ws://127.0.0.1:8787 (through the tunnel instead of direct TCP)

import { randomBytes } from "node:crypto";
import { parseArgs } from "node:util";
import { MetadataFetcher, PeerConnection, TunnelClient, listing, parseMagnet, type Transport } from "../src/index.ts";
import { TcpTransport } from "./tcp-transport.ts";

const { values } = parseArgs({ options: { peer: { type: "string" }, ih: { type: "string" }, gateway: { type: "string" } } });
if (!values.peer || !values.ih) {
	console.error("usage: list-files --peer <ip:port> --ih <info-hash | magnet> [--gateway ws://host:port]");
	process.exit(2);
}
const magnet = parseMagnet(values.ih.startsWith("magnet:") ? values.ih : `magnet:?xt=urn:${values.ih.length === 64 ? `btmh:1220${values.ih}` : `btih:${values.ih}`}`);
const reserved = Uint8Array.of(0, 0, 0, 0, 0, 0x10, 0, 0x04);
const peerId = new Uint8Array([...new TextEncoder().encode("-MS0001-"), ...randomBytes(12)]);

let transport: Transport;
let tunnel: TunnelClient | undefined;
if (values.gateway) {
	tunnel = await TunnelClient.connect(`${values.gateway.replace(/\/$/, "")}/v1/tunnel`);
	transport = await tunnel.open({ address: values.peer, infoHash: magnet.infoHash, peerId, reserved });
} else {
	const [host, port] = [values.peer.slice(0, values.peer.lastIndexOf(":")).replace(/^\[|\]$/g, ""), Number(values.peer.slice(values.peer.lastIndexOf(":") + 1))];
	transport = await TcpTransport.connect({ host, port, infoHash: magnet.infoHash, peerId, reserved });
}

const fetcher = new MetadataFetcher({ expected: { v1: magnet.v1, v2: magnet.v2, wire: magnet.infoHashHex } });
const conn: PeerConnection = new PeerConnection(transport, {
	events: {
		extensions: () => fetcher.offer(conn),
		metadata: (m) => fetcher.onMessage(conn, m),
		close: () => fetcher.onClose(conn),
	},
});
const timer = setTimeout(() => {
	console.error("no metadata after 30 s");
	process.exit(1);
}, 30_000);
const { meta } = await fetcher.done;
clearTimeout(timer);
console.log(JSON.stringify({ version: meta.version, pieces: meta.pieceCount, ...listing(meta) }, null, 2));
conn.close();
tunnel?.close();
