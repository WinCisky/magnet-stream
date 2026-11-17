#!/usr/bin/env node
// `npm audit` that fails on high/critical advisories, except those listed in
// audit-allowlist.json with a reason and a still-valid expiry date.
//
//   node scripts/audit-npm.mjs        (CI: dependency audit job)

import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";

const LEVELS = new Set(["high", "critical"]);
const allowlist = JSON.parse(readFileSync(new URL("../audit-allowlist.json", import.meta.url), "utf8")).advisories;
const today = new Date().toISOString().slice(0, 10);

let raw;
try {
	raw = execFileSync("npm", ["audit", "--json"], { encoding: "utf8", maxBuffer: 64 * 1024 * 1024 });
} catch (e) {
	raw = e.stdout; // npm audit exits non-zero when it finds anything
}
const report = JSON.parse(raw);
if (report.error) {
	console.error("npm audit failed:", report.error.summary ?? report.error);
	process.exit(2);
}

// Root advisories (objects in `via`), deduplicated by GHSA id.
const advisories = new Map();
for (const v of Object.values(report.vulnerabilities ?? {})) {
	for (const via of v.via) {
		if (typeof via !== "object" || !LEVELS.has(via.severity)) continue;
		const id = via.url?.split("/").pop() ?? String(via.source);
		advisories.set(id, { id, name: via.name, severity: via.severity, title: via.title, url: via.url });
	}
}

let failed = 0;
for (const a of advisories.values()) {
	const entry = allowlist.find((e) => e.id === a.id);
	if (entry && entry.expires >= today) {
		console.log(`allowed  ${a.severity.padEnd(8)} ${a.name}: ${a.title} (${a.id}, until ${entry.expires})`);
	} else {
		failed++;
		console.log(`FAIL     ${a.severity.padEnd(8)} ${a.name}: ${a.title} (${a.url})${entry ? ` — allowlist entry expired on ${entry.expires}` : ""}`);
	}
}
for (const e of allowlist) {
	if (!advisories.has(e.id)) console.log(`note     allowlist entry ${e.id} no longer matches any advisory: remove it`);
}
console.log(failed ? `\n${failed} high/critical advisories` : "\nno unaccepted high/critical advisories");
process.exit(failed ? 1 : 0);
