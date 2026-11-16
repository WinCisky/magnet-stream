#!/usr/bin/env node
// npm run fake-peer -- --port 6881 --size 8MiB --seed 1 [--piece-length 256KiB]
//   [--host 127.0.0.1] [--behaviour key=value ...]
// Prints the magnet and info-hash, then serves until interrupted.

import { parseArgs } from "node:util";
import { startFakePeer, type PeerBehaviour } from "./peer.ts";
import { makeTorrent, parseSize } from "./torrent.ts";

const { values } = parseArgs({
	options: {
		port: { type: "string", default: "0" },
		host: { type: "string", default: "127.0.0.1" },
		size: { type: "string", default: "8MiB" },
		seed: { type: "string", default: "1" },
		"piece-length": { type: "string", default: "256KiB" },
		behaviour: { type: "string", multiple: true, default: [] },
		help: { type: "boolean", short: "h", default: false },
	},
});

if (values.help) {
	console.log(`usage: fake-peer [--port N] [--host IP] [--size 8MiB] [--seed N] [--piece-length 256KiB]
                 [--behaviour unchokeDelayMs=500] [--behaviour corrupt=true] [--behaviour have=0,2,4] ...`);
	process.exit(0);
}

const port = Number(values.port);
if (!Number.isInteger(port) || port < 0 || port > 65535) throw new RangeError("bad --port");
const seed = Number(values.seed);
if (!Number.isInteger(seed)) throw new RangeError("bad --seed");

const behaviour: Record<string, unknown> = {};
for (const kv of values.behaviour) {
	const [key, raw] = kv.split("=", 2);
	if (!key || raw === undefined) throw new RangeError(`bad --behaviour ${kv}`);
	behaviour[key] =
		raw === "true" ? true : raw === "false" ? false : /^\d+$/.test(raw) ? Number(raw) : raw.includes(",") || key === "have" ? raw.split(",").map(Number) : raw;
}

const torrent = makeTorrent({ seed, size: parseSize(values.size), pieceLength: parseSize(values["piece-length"]) });
const peer = await startFakePeer({ torrent, host: values.host, port, ...(behaviour as PeerBehaviour) });
console.log(
	JSON.stringify(
		{ addr: peer.addr, infoHash: torrent.infoHashHex, magnet: peer.magnet, size: torrent.totalLength, pieceLength: torrent.pieceLength, pieces: torrent.pieceCount },
		null,
		2,
	),
);
console.log("listening; Ctrl-C to stop");
const stop = () => void peer.close().then(() => process.exit(0));
process.on("SIGINT", stop);
process.on("SIGTERM", stop);
