#!/usr/bin/env node
// npm run spike -- --gateway ws://127.0.0.1:8787 --peer 127.0.0.1:6881 --bytes 64MiB
//   [--seed 1] [--size 8MiB] [--piece-length 256KiB]   (must match the fake peer's)
//   [--connections 1] [--no-probe] [--later 10s] [--probe-open 7] [--out report.json]
// Gateway and peer can also come from SPIKE_GATEWAY / SPIKE_PEER. Prints a
// JSON report (docs/spike-report.md explains each field).

import { writeFileSync } from "node:fs";
import { parseArgs } from "node:util";
import { parseSize } from "@magnet-stream/fake-peer";
import { runSpike } from "./spike.ts";

const { values } = parseArgs({
	options: {
		gateway: { type: "string", default: process.env.SPIKE_GATEWAY },
		peer: { type: "string", default: process.env.SPIKE_PEER },
		bytes: { type: "string", default: "64MiB" },
		seed: { type: "string", default: "1" },
		size: { type: "string", default: "8MiB" },
		"piece-length": { type: "string", default: "256KiB" },
		connections: { type: "string", default: "1" },
		"no-probe": { type: "boolean", default: false },
		later: { type: "string", default: "10000" },
		"probe-open": { type: "string", default: "7" },
		out: { type: "string" },
		help: { type: "boolean", short: "h", default: false },
	},
});

if (values.help || !values.gateway || !values.peer) {
	console.error(`usage: spike --gateway <ws(s)://host[:port]> --peer <ip:port> [--bytes 64MiB]
             [--seed 1 --size 8MiB --piece-length 256KiB]   same as the fake peer's
             [--connections 1] [--no-probe] [--later <ms>] [--probe-open 7] [--out file.json]
env: SPIKE_GATEWAY, SPIKE_PEER`);
	process.exit(values.help ? 0 : 2);
}

const int = (raw: string, name: string, min: number, max: number) => {
	const n = Number(raw);
	if (!Number.isInteger(n) || n < min || n > max) throw new RangeError(`--${name} must be an integer in ${min}..${max}`);
	return n;
};

const report = await runSpike({
	gateway: values.gateway,
	peer: values.peer,
	bytes: parseSize(values.bytes),
	seed: int(values.seed, "seed", -(2 ** 31), 2 ** 31),
	size: parseSize(values.size),
	pieceLength: parseSize(values["piece-length"]),
	connections: int(values.connections, "connections", 1, 16),
	probe: !values["no-probe"],
	laterMs: int(values.later, "later", 0, 600_000),
	probeOpen: int(values["probe-open"], "probe-open", 1, 16),
	log: (line) => console.error(line),
});
const json = JSON.stringify(report, null, 2);
if (values.out) writeFileSync(values.out, json + "\n");
console.log(json);
