// Browser session on a mixed swarm: paste a magnet, recover the whole file
// through the tunnel, never call the legacy endpoints, and stay within the
// billed-request budget (docs/measurements.md, decision OQ-19). The video is
// saved in OPFS, still listed after a reload, and gone once deleted.
//
//   chromium (system or CHROMIUM_PATH) + fake peers + /peers stub + wrangler dev + the site's production build
//
// The swarm: 2 fast seeds (one paced), 1 slow seed, 1 partial peer (first half), 2 peers
// that never unchoke, 1 tit-for-tat leecher (second half), 3 dead addresses.
// With "Share with peers" on (the default) the page uploads to the leecher
// and the diagnostics menu shows the bytes; turned off, a second session
// sends it no bitfield, have, unchoke or piece (M3-12). Billed requests are counted from what
// the browser actually sent (WebSocket upgrades + messages / 20) and must stay
// at or under BUDGET_PER_GB per 10^9 bytes. The three phase numbers are read
// from the page's diagnostics menu and written to MEASUREMENTS_OUT (default
// tools/e2e/results/browser-session.json; CI keeps it as an artifact).

import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import type { Server } from "node:http";
import { dirname, join, resolve } from "node:path";
import { chromium } from "playwright-core";
import { makeTorrent, startFakePeer } from "@magnet-stream/fake-peer";
import { closedPort, freePort, startPeersStub, startWranglerDev, type WranglerDev } from "@magnet-stream/harness";
import { ROOT, buildSite, findChromium, serveSite, waitFor } from "./site.ts";

/** Agreed budget (OQ-19): billed Cloudflare requests per GB. */
const BUDGET_PER_GB = 40;
const MESSAGES_PER_REQUEST = 20;
const FILE_BYTES = 128 * 1024 * 1024;

const chromiumPath = findChromium();
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
		// One fast seed paced: the session must last a few lazy upload flushes
		// (2 s each) for the tit-for-tat exchange to happen; with both unpaced,
		// 128 MiB took ~5 s once fast peers were kept 4 MiB deep (M3-07).
		startFakePeer({ torrent, blockGapMs: 2 }),
		startFakePeer({ torrent }),
		startFakePeer({ torrent, blockGapMs: 20 }),
		startFakePeer({ torrent, have: half }),
		startFakePeer({ torrent, unchoke: false }),
		startFakePeer({ torrent, unchoke: false }),
		startFakePeer({ torrent, titForTat: true, have: Array.from({ length: torrent.pieceCount - half.length }, (_, i) => half.length + i) }),
	]);
	const leecher = live[6];
	const peer = live[0];
	const dead = await Promise.all([0, 1, 2].map(() => closedPort()));
	// Dead and choked addresses first: the session must get past them.
	const addresses = [...dead.map((p) => `127.0.0.1:${p}`), leecher.addr, live[4].addr, live[5].addr, live[3].addr, live[2].addr, live[1].addr, live[0].addr];
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
		// Reciprocation (M3-12): what the page says it uploaded.
		const uploadShown = page.getByTestId("upload-metrics");
		await uploadShown.waitFor({ timeout: 10_000 });
		const uploaded = {
			bytes: Number(await uploadShown.locator('[data-metric="upload-mb"]').getAttribute("data-bytes")),
			unchoked: Number((await uploadShown.locator('[data-metric="upload-unchoked"]').innerText()).trim()),
			messages: Number((await uploadShown.locator('[data-metric="upload-messages"]').innerText()).trim()),
			setting: (await uploadShown.locator('[data-metric="upload-enabled"]').innerText()).trim(),
		};
		await page.keyboard.press("Escape");

		// Saved in OPFS (M3-03): the torrent's data, map and record, every piece verified.
		const opfs = () =>
			page.evaluate(async (ih) => {
				// Browser-side code: the DOM types aren't loaded here.
				type Dir = {
					getDirectoryHandle(name: string): Promise<Dir>;
					getFileHandle(name: string): Promise<{ getFile(): Promise<{ arrayBuffer(): Promise<ArrayBuffer> }> }>;
					keys(): AsyncIterable<string>;
				};
				const storage = (globalThis as unknown as { navigator: { storage: { getDirectory(): Promise<Dir> } } }).navigator.storage;
				const root = await storage.getDirectory();
				const names: string[] = [];
				let bits = 0;
				try {
					const dir = await root.getDirectoryHandle("pieces");
					for await (const name of dir.keys()) names.push(name);
					if (names.includes(`${ih}.map`)) {
						const map = new Uint8Array(await (await (await dir.getFileHandle(`${ih}.map`)).getFile()).arrayBuffer());
						for (const byte of map) for (let i = 0; i < 8; i++) bits += (byte >> i) & 1;
					}
				} catch {
					// No pieces directory.
				}
				return { names: names.sort(), bits };
			}, torrent.infoHashHex);
		const saved = await opfs();
		const ih = torrent.infoHashHex;
		check(saved.names.join() === [`${ih}.data`, `${ih}.json`, `${ih}.map`].join(), `OPFS files: ${saved.names.join(" ")}`);
		check(saved.bits === torrent.pieceCount, `OPFS map has ${saved.bits} of ${torrent.pieceCount} pieces`);
		// Still saved after a reload, without doing anything; deleted only when asked.
		await page.reload({ waitUntil: "domcontentloaded" });
		await page.getByRole("button", { name: "Saved" }).click();
		const entry = page.getByRole("button", { name: /clip\.mkv/ }).first();
		await entry.waitFor({ timeout: 15_000 });
		check((await opfs()).bits === torrent.pieceCount, "pieces gone after a reload");
		await page.getByRole("button", { name: "Delete clip.mkv" }).click();
		await page.getByRole("button", { name: "Delete", exact: true }).click();
		const deadline = Date.now() + 15_000;
		while ((await opfs()).names.length > 0 && Date.now() < deadline) await page.waitForTimeout(200);
		check((await opfs()).names.length === 0, `OPFS files left after the delete: ${(await opfs()).names.join(" ")}`);
		await page.getByText("Nothing saved yet").waitFor({ timeout: 10_000 });

		// The first session's traffic, before the "share off" session.
		const firstUrls = urls.slice();
		const firstSockets = sockets.slice();
		const firstStubRequests = stub.requests();
		const leecherBefore = { ...leecher.stats.received };
		const leecherConnections = leecher.stats.connections;

		// "Share with peers" off: a second session (the video was deleted, so a
		// full recovery) sends the leecher no bitfield, have, unchoke or piece.
		await page.goto(PAGE, { waitUntil: "domcontentloaded" });
		const toggle = page.getByRole("checkbox", { name: "Share with peers" });
		await toggle.waitFor({ timeout: 15_000 });
		check(await toggle.isChecked(), "Share with peers is not on by default");
		await toggle.uncheck();
		await page.reload({ waitUntil: "domcontentloaded" });
		// Read from localStorage once the island hydrates.
		const kept = page.getByRole("checkbox", { name: "Share with peers" });
		const keptBy = Date.now() + 10_000;
		while ((await kept.isChecked()) && Date.now() < keptBy) await page.waitForTimeout(100);
		check(!(await kept.isChecked()), "the Share with peers setting was not kept");
		const box2 = page.locator('textarea[aria-label="Magnet link"]');
		await box2.waitFor({ timeout: 30_000 });
		await box2.click();
		await page.keyboard.insertText(peer.magnet);
		await page.locator('button[aria-label="Search"]').click();
		await page.getByText("complete", { exact: false }).waitFor({ timeout: 180_000 });
		await page.waitForTimeout(2_500);
		const after = leecher.stats.received;
		check(leecher.stats.connections > leecherConnections, "the share-off session never connected to the leecher");
		for (const k of ["bitfield", "have", "haveAll", "unchoke", "piece"] as const) {
			check(after[k] === leecherBefore[k], `with sharing off the leecher received ${after[k] - leecherBefore[k]} ${k} message(s)`);
		}
		await browser.close();

		const legacy = firstUrls.filter((u) => /\/(files|metadata|swarm|v1\/blocks|v1\/info)(\?|$)/.test(u));
		const peers = firstUrls.filter((u) => {
			const url = new URL(u);
			return url.pathname === "/peers" && url.searchParams.has("ih");
		});
		const tunnels = firstSockets.filter((s) => s.url.includes("/v1/tunnel"));
		check(legacy.length === 0, `legacy calls: ${legacy.join(" ")}`);
		check(peers.length === 1, `expected 1 /peers request, got ${peers.length}`);
		check(tunnels.length >= 1 && tunnels.length <= 3, `expected 1–3 tunnels, got ${tunnels.length}`);
		check(tunnels.every((s) => s.closed), "tunnel WebSockets were not closed");
		check(firstStubRequests === 1, `stub saw ${firstStubRequests} /peers requests`);
		check(uploaded.setting === "on" && uploaded.bytes > 0, `the page uploaded ${uploaded.bytes} bytes (setting ${uploaded.setting})`);
		check(leecherBefore.bytes > 0 && leecherBefore.badBlocks === 0, `the leecher got ${leecherBefore.bytes} bytes, ${leecherBefore.badBlocks} bad blocks`);
		check(leecher.stats.titForTatUnchokes > 0, "the tit-for-tat leecher never unchoked the page");
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
			swarm: "2 fast seeds (one paced, 2 ms per block), 1 slow seed, 1 partial (half), 2 never unchoke, 1 tit-for-tat leecher (other half), 3 dead; local wrangler dev",
			date: new Date().toISOString(),
			commit: process.env.GITHUB_SHA ?? null,
			fileBytes: torrent.totalLength,
			recoveryMs,
			measured: { websockets: tunnels.length, messagesSent: messages, billedRequests: Number(billed.toFixed(2)), billedRequestsPerGB: Number(perGB.toFixed(1)) },
			page: page3,
			upload: { bytes: uploaded.bytes, peersUnchoked: uploaded.unchoked, uploadOnlyMessages: uploaded.messages, leecherReceived: leecherBefore.bytes },
			budgetPerGB: BUDGET_PER_GB,
		};
		const out = resolve(ROOT, process.env.MEASUREMENTS_OUT ?? "tools/e2e/results/browser-session.json");
		mkdirSync(dirname(out), { recursive: true });
		writeFileSync(out, `${JSON.stringify(result, null, 1)}\n`);
		console.log(`browser session: ${peers.length} /peers, ${tunnels.length} websocket(s), no legacy calls`);
		console.log(
			`phase numbers (synthetic): first piece ${page3.firstPieceSeconds} s, ${page3.sustainedMBps} MB/s, ` +
				`${page3.billedRequestsPerGB} billed requests/GB on the page, ${perGB.toFixed(1)} counted (budget ${BUDGET_PER_GB}); ` +
				`uploaded ${uploaded.bytes} bytes to ${uploaded.unchoked} peer(s), ${uploaded.messages} upload-only message(s) → ${out}`,
		);
	} finally {
		await Promise.all(live.map((p) => p.close()));
		await stub.close();
		site?.close();
		if (siteDir) rmSync(siteDir, { recursive: true, force: true });
		await dev?.stop();
	}
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
