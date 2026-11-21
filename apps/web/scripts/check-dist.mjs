// Checks a production build for Cloudflare Pages (docs/operations.md):
// the isolation headers file, the page at the site root, and the assets the
// player needs. Run after `npm run build -w apps/web`; exits 1 on a problem.

import { existsSync, readFileSync, readdirSync } from "node:fs";
import { fileURLToPath } from "node:url";

const dist = fileURLToPath(new URL("../dist/", import.meta.url));
const problems = [];
const check = (ok, what) => {
	if (!ok) problems.push(what);
};

check(existsSync(dist), "dist/ is missing: run `npm run build -w apps/web` first");
if (problems.length === 0) {
	const headersPath = `${dist}_headers`;
	check(existsSync(headersPath), "dist/_headers is missing");
	if (existsSync(headersPath)) {
		const headers = readFileSync(headersPath, "utf8");
		check(/^\/\*\s*$/m.test(headers), "_headers has no `/*` rule");
		check(/Cross-Origin-Opener-Policy:\s*same-origin/.test(headers), "_headers lacks COOP same-origin");
		check(/Cross-Origin-Embedder-Policy:\s*require-corp/.test(headers), "_headers lacks COEP require-corp");
	}
	const indexPath = `${dist}index.html`;
	check(existsSync(indexPath), "dist/index.html is missing (the site is served from the root)");
	if (existsSync(indexPath)) {
		const html = readFileSync(indexPath, "utf8");
		check(!html.includes("/magnet-watcher"), "index.html still uses the GitHub Pages base path");
		check(!/(href|src)="\/\//.test(html), "index.html has a protocol-relative asset URL (double slash)");
		check(html.includes('href="/favicon.svg"'), "index.html does not link /favicon.svg");
	}
	check(existsSync(`${dist}_astro`) && readdirSync(`${dist}_astro`).some((f) => f.endsWith(".js")), "dist/_astro has no scripts");
	check(existsSync(`${dist}libmedia`), "dist/libmedia (player codecs) is missing");
	// The streaming service worker at the root (its scope is its folder), and no other.
	check(existsSync(`${dist}stream-sw.js`), "dist/stream-sw.js (streaming service worker) is missing");
	check(!existsSync(`${dist}coi-sw.js`), "dist/coi-sw.js is back: isolation comes from _headers");
}

if (problems.length > 0) {
	for (const p of problems) console.error(`FAIL ${p}`);
	process.exit(1);
}
console.log("dist ok: _headers, index.html at the root, assets");
