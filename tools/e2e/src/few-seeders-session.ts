// Swarms with few seeders in the browser (M4-06, docs/architecture.md
// §Chunk recovery › Swarms with few seeders):
//
// 1. A single seed that unchokes only after ~30 s: the page stays connected
//    and interested, waits it out (one connection, never dropped nor
//    redialled) and recovers the whole file.
// 2. A swarm whose only seed sits behind NAT (it only dials out, finding
//    peers through the magnet's tracker): with nobody delivering, the page
//    opens the VPS tunnel and asks it to join the swarm; the VPS gateway
//    announces itself to the tracker, the seed connects in, and the file
//    comes through the VPS. The seed is slower than the video (a real MP4):
//    the page says so, offers to keep buffering, and the file completes.
//
//   chromium (system or CHROMIUM_PATH) + fake peers + fake tracker + /peers stub
//   + wrangler dev (primary) + the VPS gateway (escalation, inbound peers)
//   + the site's production build with short escalation thresholds
//
// 3. The diagnostics (menu and exported report) give the bytes of each
//    path: the late seed's session through Cloudflare, the NAT seed's
//    through the VPS (M4-08).
//
//   npm run e2e:few -w tools/e2e

import { mkdirSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import type { Server } from "node:http";
import { join } from "node:path";
import { chromium, type Page } from "playwright-core";
import { makeTorrent, startFakePeer, type FakePeer, type FakeTorrent } from "@magnet-stream/fake-peer";
import { freePort, startFakeTracker, startPeersStub, startVpsGateway, startWranglerDev, type FakeTracker, type StubLine, type WranglerDev } from "@magnet-stream/harness";
import { spreadSamples } from "./mp4-spread.ts";
import { ROOT, buildSite, findChromium, serveSite, waitFor, type SiteGateway } from "./site.ts";

const FIXTURE = new URL("../fixtures/clip-vp9.mp4", import.meta.url);
/** Short thresholds for the test (the defaults are 90 s and 30 s). */
const ESCALATE_AFTER_S = 6;
const NOTICE_AFTER_S = 3;
/** The late seed unchokes after this long (an optimistic rotation is ~30 s). */
const UNCHOKE_DELAY_MS = 30_000;

const chromiumPath = findChromium();
if (!chromiumPath) {
	console.log("skip few-seeders e2e: no chromium (set CHROMIUM_PATH)");
	process.exit(process.env.REQUIRE_BROWSER_E2E ? 1 : 0);
}

let primary: WranglerDev | undefined;
let vps: WranglerDev | undefined;
let tracker: FakeTracker | undefined;
let site: Server | undefined;
let siteDir: string | undefined;
const peers: FakePeer[] = [];
let failed = false;

async function main(): Promise<void> {
	// 1. One seed, late to unchoke (random bytes: no bitrate, nothing to play).
	const late = makeTorrent({ seed: 640, name: "late.mkv", size: 4 * 1024 * 1024, pieceLength: 256 * 1024 });
	const lateSeed = await startFakePeer({ torrent: late, unchokeDelayMs: UNCHOKE_DELAY_MS });
	peers.push(lateSeed);

	// 2. A real 40 s MP4 (samples spread apart: ~16 MiB, ~420 kB/s), seeded
	// only from behind "NAT", at ~270 kB/s: slower than the video.
	tracker = await startFakeTracker();
	const content = spreadSamples(new Uint8Array(readFileSync(FIXTURE)), 16 * 1024);
	const clip = makeTorrent({ content, name: "clip.mp4", pieceLength: 256 * 1024 });
	const natSeed = await startFakePeer({ torrent: clip, listen: false, tracker: tracker.url, announceIntervalMs: 300, blockGapMs: 60 });
	peers.push(natSeed);
	check(natSeed.addr === "", "the NAT seed does not listen");
	const bitrate = clip.totalLength / 40;

	const stub = await startPeersStub(({ ih }): StubLine[] => {
		if (ih === late.infoHashHex) return [{ info: late.info }, { peer: lateSeed.addr, score: 0.8, alive: true, seed: true, md: true }];
		// Nothing the VPS can reach: only the info dict (it could have come from session memory).
		if (ih === clip.infoHashHex) return [{ info: clip.info }];
		return [];
	});
	try {
		const port = await freePort();
		const origin = `http://localhost:${port}`;
		const PAGE = `${origin}/`;
		primary = await startWranglerDev({ vars: { ALLOW_PRIVATE_PEERS: "1", ALLOWED_ORIGINS: origin } });
		const inboundPort = await freePort();
		vps = await startVpsGateway({
			vars: {
				ALLOW_PRIVATE_PEERS: "1",
				ALLOWED_ORIGINS: origin,
				TUNNEL_INBOUND_ADDR: `127.0.0.1:${inboundPort}`,
				// Hermetic: no public DHT, no public trackers.
				TUNNEL_ANNOUNCE_DHT: "0",
				DEFAULT_TRACKERS: "",
			},
		});
		const gateway: SiteGateway = { ...primary, kind: "cloudflare", cloudflareUrl: primary.url, env: { PUBLIC_DEFAULT_GATEWAY: "cloudflare", PUBLIC_WORKER_URL: primary.url } };
		mkdirSync(join(ROOT, "tools/e2e/results"), { recursive: true });
		siteDir = mkdtempSync(join(ROOT, "tools/e2e/results", "site-"));
		buildSite(stub.url, gateway, siteDir, {
			PUBLIC_VPS_GATEWAY_URL: vps.url,
			PUBLIC_ESCALATION_AFTER_SECONDS: String(ESCALATE_AFTER_S),
			PUBLIC_SLOW_NOTICE_SECONDS: String(NOTICE_AFTER_S),
		});
		site = await serveSite(siteDir, port);
		await waitFor(PAGE, 30_000);

		const browser = await chromium.launch({ executablePath: chromiumPath, args: ["--no-sandbox", "--disable-dev-shm-usage"] });
		const page = await browser.newPage({ acceptDownloads: true });
		const sockets: string[] = [];
		page.on("websocket", (ws) => sockets.push(ws.url()));
		const vpsHost = new URL(vps.url).host;
		const toVps = () => sockets.filter((u) => new URL(u).host === vpsHost);
		await page.goto(PAGE, { waitUntil: "domcontentloaded" });

		// 1. The late seed is waited for.
		let started = Date.now();
		await paste(page, magnetOf(late));
		await complete(page, 120_000);
		const lateMs = Date.now() - started;
		check(lateMs >= UNCHOKE_DELAY_MS - 1_000, `the late seed's file completed in ${lateMs} ms, before it unchoked`);
		// One connection for the whole session (it closes with the session, once complete).
		check(lateSeed.stats.connections === 1, `the late seed was dropped and redialled (${lateSeed.stats.connections} connections)`);
		check(lateSeed.stats.served > 0, "the late seed served nothing");
		console.log(`late seed: waited ${(lateMs / 1000).toFixed(1)} s, 1 connection, ${lateSeed.stats.served} blocks`);
		await home(page);

		// 2. Only a NAT seed: escalation to the VPS, the notice, the file completes.
		const socketsBefore = toVps().length;
		started = Date.now();
		await paste(page, magnetOf(clip, tracker.url));
		// The big play button: the player reads the container and reports the bitrate.
		const play = page.locator('button.size-16[aria-label="Play"]');
		await play.waitFor({ timeout: 60_000 });
		const escalation = page.getByTestId("escalation");
		await escalation.waitFor({ timeout: 60_000 });
		const joined = page.locator('[data-testid="escalation"][data-state="joined"]');
		await joined.waitFor({ timeout: 30_000 });
		const escalatedMs = Date.now() - started;
		check(toVps().length > socketsBefore, "no WebSocket to the VPS gateway");
		check(toVps().every((u) => new URL(u).pathname === "/v1/tunnel"), `VPS WebSockets: ${toVps().join(" ")}`);
		await play.click();
		const notice = page.getByTestId("below-bitrate");
		try {
			await notice.waitFor({ timeout: 90_000 });
		} catch (e) {
			const at = await escalation.evaluate((el) => `${el.getAttribute("data-state")} rate ${el.getAttribute("data-rate")} bitrate ${el.getAttribute("data-bitrate")}`).catch(() => "");
			console.log(`health: ${at}\nPAGE:\n`, (await page.locator("body").innerText()).slice(0, 2000));
			throw e;
		}
		const noticeText = await notice.innerText();
		check(/slower|less than this video needs/.test(noticeText), `notice: ${noticeText}`);
		await notice.getByRole("button", { name: "Keep buffering" }).click();
		await page.getByTestId("buffering").waitFor({ timeout: 5_000 });
		const shown = await page.getByText(/connected through it/).innerText().catch(() => "");
		check(/1 peer connected through it/.test(shown), `escalation line: ${shown}`);
		await complete(page, 180_000);
		const clipMs = Date.now() - started;
		const body = await page.locator("body").innerText();
		check(!/\b\d{1,3}(?:\.\d{1,3}){3}(?::\d+)?\b/.test(body), "the page shows a peer address");
		check(!body.includes(clip.infoHashHex), "the page shows the info-hash");

		// The VPS joined the swarm for this torrent only, through the magnet's tracker, and the seed came in.
		const fromVps = tracker.announces.filter((a) => a.port === inboundPort);
		check(fromVps.some((a) => a.event === "started" && a.infoHash === clip.infoHashHex), "the VPS gateway never announced the torrent to the tracker");
		check(fromVps.every((a) => a.infoHash === clip.infoHashHex), "the VPS gateway announced another torrent");
		check(natSeed.stats.handshakes >= 1 && natSeed.stats.served > 0, `the NAT seed: ${natSeed.stats.handshakes} handshakes, ${natSeed.stats.served} blocks`);
		console.log(
			`NAT seed: escalated and joined after ${(escalatedMs / 1000).toFixed(1)} s, rate under the bitrate (${Math.round(bitrate)} B/s) shown, ` +
				`complete in ${(clipMs / 1000).toFixed(1)} s, ${natSeed.stats.served} blocks through the VPS`,
		);
		// The diagnostics say which path carried the bytes (M4-08): the late seed's
		// session through Cloudflare, the NAT seed's through the VPS.
		await home(page);
		await page.waitForTimeout(5_500);
		await page.getByRole("button", { name: "Diagnostics" }).click();
		const shownPaths = page.getByTestId("path-metrics");
		await shownPaths.waitFor({ timeout: 10_000 });
		const shownVps = Number(await shownPaths.locator('[data-metric="path-vps-mb"]').getAttribute("data-bytes"));
		const shownCf = Number(await shownPaths.locator('[data-metric="path-cloudflare-mb"]').getAttribute("data-bytes"));
		check(shownVps >= clip.totalLength && shownCf === 0, `the menu's data path (last session): ${shownCf} B through Cloudflare, ${shownVps} B through the VPS`);
		const [download] = await Promise.all([page.waitForEvent("download"), page.getByRole("menuitem", { name: "Export as JSON" }).click()]);
		await page.keyboard.press("Escape");
		const report = JSON.parse(readFileSync(await download.path(), "utf8")) as { visits: { sessions?: { tunnel: { paths?: Record<"cloudflare" | "vps", { downloaded: number }> } }[] }[]; health: { tunnel: { details: { paths?: Record<string, { downloaded: number }> } } } };
		const records = report.visits.flatMap((v) => v.sessions ?? []);
		const pathsOf = (i: number) => records.at(i)?.tunnel.paths;
		check((pathsOf(-2)?.cloudflare.downloaded ?? 0) >= late.totalLength && pathsOf(-2)?.vps.downloaded === 0, `late seed's session paths: ${JSON.stringify(pathsOf(-2))}`);
		check((pathsOf(-1)?.vps.downloaded ?? 0) >= clip.totalLength && pathsOf(-1)?.cloudflare.downloaded === 0, `NAT seed's session paths: ${JSON.stringify(pathsOf(-1))}`);
		const totals = report.health.tunnel.details.paths;
		check((totals?.cloudflare.downloaded ?? 0) > 0 && (totals?.vps.downloaded ?? 0) > 0, `the report's tunnel section by path: ${JSON.stringify(totals)}`);
		console.log(`diagnostics: ${shownCf} B through Cloudflare, ${shownVps} B through the VPS (last session); report totals ${JSON.stringify(totals)}`);
		await browser.close();
		const logs = vps.output();
		check(!logs.toLowerCase().includes(clip.infoHashHex), "info-hash in the VPS gateway's logs");
	} finally {
		await stub.close();
	}
}

function magnetOf(t: FakeTorrent, trackerUrl?: string): string {
	return `magnet:?xt=urn:btih:${t.infoHashHex}&dn=${t.name}${trackerUrl ? `&tr=${encodeURIComponent(trackerUrl)}` : ""}`;
}

async function paste(page: Page, link: string): Promise<void> {
	const box = page.locator('textarea[aria-label="Magnet link"]');
	await box.waitFor({ timeout: 30_000 });
	await box.fill("");
	await box.click();
	await page.keyboard.insertText(link);
	await page.locator('button[aria-label="Search"]').click();
}

async function complete(page: Page, timeout: number): Promise<void> {
	try {
		await page.getByText("· complete", { exact: false }).waitFor({ timeout });
	} catch (e) {
		console.log("PAGE:\n", (await page.locator("body").innerText()).slice(0, 2000));
		throw e;
	}
}

async function home(page: Page): Promise<void> {
	await page.getByRole("button", { name: "Home" }).click();
	await page.locator('textarea[aria-label="Magnet link"]').waitFor({ timeout: 10_000 });
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
		if (vps) console.error(vps.output().slice(-3000));
	})
	.finally(async () => {
		await Promise.all(peers.map((p) => p.close()));
		await tracker?.close();
		site?.close();
		if (siteDir) rmSync(siteDir, { recursive: true, force: true });
		await primary?.stop();
		await vps?.stop();
		process.exit(failed ? 1 : 0);
	});
