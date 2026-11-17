#!/usr/bin/env node
// Phase 0 spike CLI (docs/spike-report.md).
//
// Against a fake peer (same --seed/--size/--piece-length as the peer):
//   npm run spike -- --gateway ws://127.0.0.1:8787 --peer 127.0.0.1:6881 --bytes 64MiB
// Against a real torrent and candidate peers:
//   npm run spike -- --gateway wss://<gw> --torrent file.torrent --peers-file peers.txt --bytes 1GiB --connections 6
//
// Gateway and peer can also come from SPIKE_GATEWAY / SPIKE_PEER. Prints a
// JSON report (no peer addresses, no info-hash).

import { readFileSync, writeFileSync } from "node:fs";
import { parseArgs } from "node:util";
import { parseSize } from "@magnet-stream/fake-peer";
import { fakeSource, runSpike, torrentSource } from "./spike.ts";

const { values } = parseArgs({
	options: {
		gateway: { type: "string", default: process.env.SPIKE_GATEWAY },
		peer: { type: "string", multiple: true, default: process.env.SPIKE_PEER ? [process.env.SPIKE_PEER] : [] },
		"peers-file": { type: "string" },
		torrent: { type: "string" },
		bytes: { type: "string", default: "64MiB" },
		seed: { type: "string", default: "1" },
		size: { type: "string", default: "8MiB" },
		"piece-length": { type: "string", default: "256KiB" },
		connections: { type: "string" },
		"max-seconds": { type: "string", default: "1800" },
		"no-probe": { type: "boolean", default: false },
		later: { type: "string", default: "10000" },
		"probe-open": { type: "string", default: "7" },
		out: { type: "string" },
		help: { type: "boolean", short: "h", default: false },
	},
});

const candidates = [...values.peer];
if (values["peers-file"]) {
	const text = readFileSync(values["peers-file"], "utf8").trim();
	candidates.push(...(text.startsWith("[") ? (JSON.parse(text) as string[]) : text.split(/\s+/)).filter(Boolean));
}

if (values.help || !values.gateway || candidates.length === 0) {
	console.error(`usage: spike --gateway <ws(s)://host[:port]> (--peer <ip:port> ... | --peers-file <file>) [--bytes 64MiB]
             fake peer:    [--seed 1 --size 8MiB --piece-length 256KiB]   same as the fake peer's
             real torrent: --torrent <file.torrent>
             [--connections N] [--max-seconds 1800] [--no-probe] [--later <ms>] [--probe-open 7] [--out file.json]
env: SPIKE_GATEWAY, SPIKE_PEER`);
	process.exit(values.help ? 0 : 2);
}

const int = (raw: string, name: string, min: number, max: number) => {
	const n = Number(raw);
	if (!Number.isInteger(n) || n < min || n > max) throw new RangeError(`--${name} must be an integer in ${min}..${max}`);
	return n;
};

const connections = int(values.connections ?? (values.torrent ? "6" : "1"), "connections", 1, 24);
const log = (line: string) => console.error(line);
log(values.torrent ? `reading ${values.torrent}…` : `building the fake peer's torrent (seed ${values.seed}, ${values.size})…`);
const source = values.torrent
	? await torrentSource(new Uint8Array(readFileSync(values.torrent)), candidates)
	: await fakeSource(candidates[0], int(values.seed, "seed", -(2 ** 31), 2 ** 31), parseSize(values.size), parseSize(values["piece-length"]), connections);

const report = await runSpike({
	gateway: values.gateway,
	source,
	bytes: parseSize(values.bytes),
	connections,
	maxSeconds: int(values["max-seconds"], "max-seconds", 10, 86_400),
	probe: !values["no-probe"],
	laterMs: int(values.later, "later", 0, 600_000),
	probeOpen: int(values["probe-open"], "probe-open", 1, 16),
	log,
});
const json = JSON.stringify(report, null, 2);
if (values.out) writeFileSync(values.out, json + "\n");
console.log(json);
process.exit(0);
