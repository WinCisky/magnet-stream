// Browser session on a mixed swarm: paste a magnet, recover the whole file
// through the tunnel, never call the legacy endpoints, and stay within the
// billed-request budget (docs/measurements.md, decision OQ-19).
//
//   chromium (system or CHROMIUM_PATH) + fake peers + /peers stub + wrangler dev + the site's production build
//
// The swarm: 2 fast seeds, 1 slow seed, 1 partial peer (first half), 2 peers
// that never unchoke, 3 dead addresses. Billed requests are counted from what
// the browser actually sent (WebSocket upgrades + messages / 20) and must stay
// at or under BUDGET_PER_GB per 10^9 bytes. The three phase numbers are read
// from the page's diagnostics menu and written to MEASUREMENTS_OUT (default
// tools/e2e/results/browser-session.json; CI keeps it as an artifact).

import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { createServer, type Server } from "node:http";
import { dirname, extname, join, resolve } from "node:path";
import { chromium } from "playwright-core";
import { makeTorrent, startFakePeer } from "@magnet-stream/fake-peer";
import { closedPort, freePort, startPeersStub, startWranglerDev, type WranglerDev } from "@magnet-stream/harness";

/** Agreed budget (OQ-19): billed Cloudflare requests per GB. */
const BUDGET_PER_GB = 40;
const MESSAGES_PER_REQUEST = 20;
const FILE_BYTES = 128 * 1024 * 1024;

const ROOT = new URL("../../../", import.meta.url).pathname;

/** Playwright's own Chromium (`npx playwright-core install chromium`), if installed. */
function bundledChromium(): string | undefined {
	try {
		return chromium.executablePath();
	} catch {
		return undefined;
	}
}

const chromiumPath = [process.env.CHROMIUM_PATH, "/usr/bin/chromium", "/usr/bin/chromium-browser", "/usr/bin/google-chrome", bundledChromium()].find(
	(p) => p && existsSync(p),
);
if (!chromiumPath) {
	// CI sets REQUIRE_BROWSER_E2E: there a missing browser must fail, not skip the budget check.
	console.log("skip browser e2e: no chromium (set CHROMIUM_PATH)");
	process.exit(process.env.REQUIRE_BROWSER_E2E ? 1 : 0);
}

let dev: WranglerDev | undefined;
let site: Server | undefined;
let siteDir: string | undefined;
let failed = false;

async function main(): Promise<void> {
	const torrent = makeTorrent({ seed: 90, name: "clip.mkv", size: FILE_BYTES, pieceLength: 256 * 1024 });
	const half = Array.from({ length: Math.floor(torrent.pieceCount / 2) }, (_, i) => i);
	const live = await Promise.all([
		startFakePeer({ torrent }),
		startFakePeer({ torrent }),
		startFakePeer({ torrent, blockGapMs: 20 }),
		startFakePeer({ torrent, have: half }),
		startFakePeer({ torrent, unchoke: false }),
		startFakePeer({ torrent, unchoke: false }),
	]);
	const peer = live[0];
	const dead = await Promise.all([0, 1, 2].map(() => closedPort()));
	// Dead and choked addresses first: the session must get past them.
	const addresses = [...dead.map((p) => `127.0.0.1:${p}`), live[4].addr, live[5].addr, live[3].addr, live[2].addr, live[1].addr, live[0].addr];
	const stub = await startPeersStub(addresses);
	try {
		// Our own port and a production build: a dev server already running in
		// apps/web (someone's local stack) must not answer for this test.
		const port = await freePort();
		const PAGE = `http://localhost:${port}/`;
		dev = await startWranglerDev({ vars: { ALLOW_PRIVATE_PEERS: "1", ALLOWED_ORIGINS: `http://localhost:${port}` } });
		// Inside the repository (git-ignored): Astro moves files into it, which fails across filesystems.
		mkdirSync(join(ROOT, "tools/e2e/results"), { recursive: true });
		siteDir = mkdtempSync(join(ROOT, "tools/e2e/results", "site-"));
		buildSite(stub.url, dev.url, siteDir);
		site = await serveSite(siteDir, port);
		await waitFor(PAGE, 30_000);

		const browser = await chromium.launch({ executablePath: chromiumPath, args: ["--no-sandbox", "--disable-dev-shm-usage"] });
		const page = await browser.newPage();
		const urls: string[] = [];
		const sockets: { url: string; closed: boolean; sent: number }[] = [];
		page.on("request", (req) => urls.push(req.url()));
		page.on("websocket", (ws) => {
			const entry = { url: ws.url(), closed: false, sent: 0 };
			sockets.push(entry);
			ws.on("framesent", () => entry.sent++);
			ws.on("close", () => (entry.closed = true));
		});
		await page.goto(PAGE, { waitUntil: "domcontentloaded" });
		// The isolation service worker reloads the first visit once.
		await page.waitForTimeout(1500);
		await page.reload({ waitUntil: "domcontentloaded" });
		const box = page.locator('textarea[aria-label="Magnet link"]');
		await box.waitFor({ timeout: 30_000 });
		await box.click();
		await page.keyboard.insertText(peer.magnet);
		const started = Date.now();
		await page.locator('button[aria-label="Search"]').click();
		try {
			await page.getByText("complete", { exact: false }).waitFor({ timeout: 180_000 });
		} catch (e) {
			console.log("URL", page.url());
			console.log("PAGE:\n", (await page.locator("body").innerText()).slice(0, 1500));
			throw e;
		}
		const recoveryMs = Date.now() - started;
		await page.waitForTimeout(1_000);
		// The three numbers as the page shows them (the menu is on the home step).
		await page.getByRole("button", { name: "Home" }).click();
		await page.getByRole("button", { name: "Diagnostics" }).click();
		const shown = page.getByTestId("phase-metrics");
		await shown.waitFor({ timeout: 10_000 });
		const metric = async (name: string) => Number.parseFloat((await shown.locator(`[data-metric="${name}"]`).innerText()).trim());
		const page3 = { firstPieceSeconds: await metric("first-piece-s"), sustainedMBps: await metric("mbps"), billedRequestsPerGB: await metric("requests-per-gb") };
		await browser.close();

		const legacy = urls.filter((u) => /\/(files|metadata|swarm|v1\/blocks|v1\/info)(\?|$)/.test(u));
		const peers = urls.filter((u) => {
			const url = new URL(u);
			return url.pathname === "/peers" && url.searchParams.has("ih");
		});
		const tunnels = sockets.filter((s) => s.url.includes("/v1/tunnel"));
		check(legacy.length === 0, `legacy calls: ${legacy.join(" ")}`);
		check(peers.length === 1, `expected 1 /peers request, got ${peers.length}`);
		check(tunnels.length >= 1 && tunnels.length <= 3, `expected 1–3 tunnels, got ${tunnels.length}`);
		check(tunnels.every((s) => s.closed), "tunnel WebSockets were not closed");
		check(stub.requests() === 1, `stub saw ${stub.requests()} /peers requests`);
		check(live.slice(0, 4).some((p) => p.stats.served > 0), "no live peer served a block");

		// Billed requests, counted from the browser's side of the tunnel.
		const messages = tunnels.reduce((n, s) => n + s.sent, 0);
		const billed = tunnels.length + messages / MESSAGES_PER_REQUEST;
		const perGB = billed / (torrent.totalLength / 1e9);
		check(perGB <= BUDGET_PER_GB, `billed requests per GB ${perGB.toFixed(1)} over the budget of ${BUDGET_PER_GB}`);
		for (const [name, value] of Object.entries(page3)) check(Number.isFinite(value) && value >= 0, `diagnostics menu shows no ${name}`);
		check(page3.billedRequestsPerGB <= BUDGET_PER_GB, `the page reports ${page3.billedRequestsPerGB} requests per GB, over ${BUDGET_PER_GB}`);

		const result = {
			kind: "synthetic",
			swarm: "2 fast seeds, 1 slow seed, 1 partial (half), 2 never unchoke, 3 dead; local wrangler dev",
			date: new Date().toISOString(),
			commit: process.env.GITHUB_SHA ?? null,
			fileBytes: torrent.totalLength,
			recoveryMs,
			measured: { websockets: tunnels.length, messagesSent: messages, billedRequests: Number(billed.toFixed(2)), billedRequestsPerGB: Number(perGB.toFixed(1)) },
			page: page3,
			budgetPerGB: BUDGET_PER_GB,
		};
		const out = resolve(ROOT, process.env.MEASUREMENTS_OUT ?? "tools/e2e/results/browser-session.json");
		mkdirSync(dirname(out), { recursive: true });
		writeFileSync(out, `${JSON.stringify(result, null, 1)}\n`);
		console.log(`browser session: ${peers.length} /peers, ${tunnels.length} websocket(s), no legacy calls`);
		console.log(
			`phase numbers (synthetic): first piece ${page3.firstPieceSeconds} s, ${page3.sustainedMBps} MB/s, ` +
				`${page3.billedRequestsPerGB} billed requests/GB on the page, ${perGB.toFixed(1)} counted (budget ${BUDGET_PER_GB}) → ${out}`,
		);
	} finally {
		await Promise.all(live.map((p) => p.close()));
		await stub.close();
		site?.close();
		if (siteDir) rmSync(siteDir, { recursive: true, force: true });
		await dev?.stop();
	}
}

/**
 * Build the site as it is deployed (Cloudflare Pages layout, the service
 * URLs baked in) into a temporary directory, so a dev server someone is
 * running in apps/web neither answers for this test nor gets in its way.
 */
function buildSite(seeders: string, gateway: string, outDir: string): void {
	const r = spawnSync("npm", ["run", "build", "-w", "apps/web", "--", "--outDir", outDir], {
		cwd: ROOT,
		encoding: "utf8",
		env: { ...process.env, PUBLIC_SEEDERS_URL: seeders, PUBLIC_WORKER_URL: gateway, NO_COLOR: "1" },
	});
	if (r.status !== 0) throw new Error(`site build failed:\n${(r.stdout + r.stderr).slice(-3000)}`);
}

const TYPES: Record<string, string> = {
	".html": "text/html; charset=utf-8",
	".js": "text/javascript",
	".mjs": "text/javascript",
	".css": "text/css",
	".json": "application/json",
	".wasm": "application/wasm",
	".svg": "image/svg+xml",
	".ico": "image/x-icon",
	".map": "application/json",
};

/** A static server for the build, sending the headers of its `_headers` `/*` rule (as Pages does). */
function serveSite(dir: string, port: number): Promise<Server> {
	const headers: Record<string, string> = {};
	const rules = readFileSync(join(dir, "_headers"), "utf8").split("\n");
	let inAll = false;
	for (const line of rules) {
		if (/^\S/.test(line)) inAll = line.trim() === "/*";
		else if (inAll && line.includes(":")) {
			const at = line.indexOf(":");
			headers[line.slice(0, at).trim()] = line.slice(at + 1).trim();
		}
	}
	const root = resolve(dir);
	const server = createServer((req, res) => {
		const path = decodeURIComponent(new URL(req.url ?? "/", "http://x").pathname);
		let file = resolve(root, `.${path}`);
		if (!file.startsWith(root)) {
			res.writeHead(403).end();
			return;
		}
		if (existsSync(file) && statSync(file).isDirectory()) file = join(file, "index.html");
		if (!existsSync(file)) {
			res.writeHead(404, headers).end();
			return;
		}
		res.writeHead(200, { ...headers, "content-type": TYPES[extname(file)] ?? "application/octet-stream" });
		res.end(readFileSync(file));
	});
	return new Promise((ok) => server.listen(port, "localhost", () => ok(server)));
}

async function waitFor(url: string, timeoutMs: number): Promise<void> {
	const until = Date.now() + timeoutMs;
	let last = "";
	while (Date.now() < until) {
		try {
			const res = await fetch(url);
			if (res.ok) return;
			last = `HTTP ${res.status}`;
		} catch (e) {
			last = e instanceof Error ? e.message : String(e);
		}
		await new Promise((r) => setTimeout(r, 300));
	}
	throw new Error(`page did not come up: ${last}`);
}

function check(ok: boolean, what: string): void {
	if (!ok) {
		failed = true;
		console.error(`FAIL ${what}`);
	}
}

main()
	.catch((e: unknown) => {
		failed = true;
		console.error(e);
	})
	.finally(() => process.exit(failed ? 1 : 0));
