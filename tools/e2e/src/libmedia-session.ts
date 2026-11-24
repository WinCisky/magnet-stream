// libmedia playback (M3-06): an MKV recovered through the tunnel plays in
// libmedia (no native player for Matroska), from the file's bytes only:
// playback starts once its index is read and ~10 s are verified, a seek
// ahead to data not recovered yet holds playback until 5 s are verified
// there (the peer is paced, a slow one), and the picker's window in bytes
// follows the bitrate read from the container. The page requests nothing
// outside the site (libmedia's wasm decoders included), the gateway and
// the discovery service.
//
//   chromium (system or CHROMIUM_PATH) + a paced fake peer + /peers stub + wrangler dev + the site's production build
//
// The video is tools/e2e/fixtures/clip-vp9.mp4 remuxed to Matroska with
// 16 KiB of Void before each frame (mkv-from-mp4.ts): 40 s in about 16 MiB,
// cues at the end.

import { mkdirSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import type { Server } from "node:http";
import { join } from "node:path";
import { chromium, type Page } from "playwright-core";
import { makeTorrent, startFakePeer } from "@magnet-stream/fake-peer";
import { freePort, startPeersStub } from "@magnet-stream/harness";
import { mp4ToMkv } from "./mkv-from-mp4.ts";
import { ROOT, buildSite, findChromium, serveSite, startSiteGateway, waitFor, type SiteGateway } from "./site.ts";

const FIXTURE = new URL("../fixtures/clip-vp9.mp4", import.meta.url);
const DURATION_S = 40;
/** ArrowRight skips 10 s: three times, from the first seconds. */
const SKIPS = 3;

const chromiumPath = findChromium();
if (!chromiumPath) {
	console.log("skip libmedia e2e: no chromium (set CHROMIUM_PATH)");
	process.exit(process.env.REQUIRE_BROWSER_E2E ? 1 : 0);
}

let dev: SiteGateway | undefined;
let site: Server | undefined;
let siteDir: string | undefined;
let failed = false;

async function main(): Promise<void> {
	const content = mp4ToMkv(new Uint8Array(readFileSync(FIXTURE)), 16 * 1024);
	const torrent = makeTorrent({ content, name: "clip.mkv", pieceLength: 256 * 1024 });
	// One 16 KiB block every 20 ms (~800 kB/s, about twice the video's
	// bitrate): a seek soon after the start lands on data not recovered yet.
	const peer = await startFakePeer({ torrent, blockGapMs: 20 });
	const stub = await startPeersStub([peer.addr]);
	try {
		const port = await freePort();
		const PAGE = `http://localhost:${port}/`;
		dev = await startSiteGateway(`http://localhost:${port}`);
		mkdirSync(join(ROOT, "tools/e2e/results"), { recursive: true });
		siteDir = mkdtempSync(join(ROOT, "tools/e2e/results", "site-"));
		buildSite(stub.url, dev, siteDir);
		site = await serveSite(siteDir, port);
		await waitFor(PAGE, 30_000);

		const browser = await chromium.launch({ executablePath: chromiumPath, args: ["--no-sandbox", "--disable-dev-shm-usage"] });
		const context = await browser.newContext();
		const page = await context.newPage();
		const allowed = new Set([new URL(PAGE).origin, new URL(dev.url).origin, new URL(stub.url).origin]);
		const foreign: string[] = [];
		const wasm: string[] = [];
		context.on("request", (req) => {
			const url = new URL(req.url());
			if (url.pathname.endsWith(".wasm")) wasm.push(url.pathname);
			if (url.protocol !== "data:" && url.protocol !== "blob:" && !allowed.has(url.origin)) foreign.push(url.origin);
		});
		const logs: string[] = [];
		const t0 = Date.now();
		page.on("console", (m) => logs.push(`${((Date.now() - t0) / 1000).toFixed(1)} ${m.type()}: ${m.text()}`));
		page.on("pageerror", (e) => logs.push(`pageerror: ${e.message}`));

		if (process.env.E2E_DEBUG) await page.addInitScript(() => globalThis.localStorage.setItem("mw-debug", "1"));
		await page.goto(PAGE, { waitUntil: "domcontentloaded" });
		const box = page.locator('textarea[aria-label="Magnet link"]');
		await box.waitFor({ timeout: 30_000 });
		await box.click();
		await page.keyboard.insertText(torrent.magnet);
		await page.locator('button[aria-label="Search"]').click();
		const openedAt = Date.now();

		// libmedia, chosen for Matroska; play as soon as the button is there.
		const region = page.locator('[aria-label="Video player"]');
		await page.locator('[aria-label="Video player"][data-player="libmedia"]').waitFor({ state: "attached", timeout: 60_000 });
		await page.locator('button.size-16[aria-label="Play"]').click({ timeout: 60_000 });
		try {
			await waitUntil(async () => (await position(page)) >= 1, 90_000, "libmedia never played");
		} catch (e) {
			console.log("LOGS", logs.slice(-40).join("\n"));
			console.log("PAGE:\n", (await page.locator("body").innerText()).slice(0, 1500));
			throw e;
		}
		const startMs = Date.now() - openedAt;
		const startHold = ((await region.getAttribute("data-buffered")) ?? "").split(" ")[0];
		check(startHold === "start:end" || (startHold.startsWith("start:") && Number(startHold.slice(6)) >= 9.9), `start hold: ${startHold}`);
		check(!(await page.locator('[aria-label="Video player"] video[src^="/stream/"]').count()), "Matroska went to the native player");

		// The seek bar's time comes from libmedia: seek ahead past what's recovered.
		const before = await percent(page);
		const from = await position(page);
		for (let i = 0; i < SKIPS; i++) await page.keyboard.press("ArrowRight");
		const target = Math.min(DURATION_S - 4, from + SKIPS * 10);
		const seekedAt = Date.now();
		await waitUntil(async () => (await position(page)) >= target + 2, 90_000, `playback didn't resume past ${target + 2} s after the seek`);
		const resumeMs = Date.now() - seekedAt;
		check(before < (100 * target) / DURATION_S - 10, `the seek target was probably recovered already (${before}% at the seek, target ${target} s)`);
		const resumes = ((await region.getAttribute("data-buffered")) ?? "").split(" ").filter((h) => h.startsWith("resume:"));
		check(resumes.length >= 1 && resumes.every((h) => h === "resume:end" || Number(h.slice(7)) >= 4.9), `resume holds: ${resumes.join(" ") || "none"}`);
		const error = await page.getByText("This video can't be played here").count();
		check(error === 0, "the player failed");

		// The picker's window: 30 s at the bitrate libmedia read (size over duration).
		const bitrate = Math.round(torrent.totalLength / DURATION_S);
		const windowLine = page.getByTestId("playback-window");
		await waitUntil(async () => Number(await windowLine.getAttribute("data-bitrate")) > 0, 10_000, "the bitrate never reached the recovery");
		const windowBytes = Number(await windowLine.getAttribute("data-window-bytes"));
		check(Math.abs(windowBytes - 30 * bitrate) <= 30 * bitrate * 0.02, `window ${windowBytes} B, expected about ${30 * bitrate} B`);

		if (failed || process.env.E2E_DEBUG) {
			console.log("SLIDER max", await page.locator('[role="slider"][aria-label="Seek"]').getAttribute("aria-valuemax"));
			console.log("LOGS", logs.slice(-40).join("\n"));
		}
		await browser.close();
		check(foreign.length === 0, `requests outside the site and its services: ${[...new Set(foreign)].join(" ")}`);
		console.log(
			`libmedia: MKV playing ${startMs} ms after the paste (${startHold}), seek from ${from} s to ${target} s at ${before.toFixed(1)}% recovered resumed in ${resumeMs} ms (${resumes.join(" ")}), window ${windowBytes} B, wasm from the site: ${[...new Set(wasm)].join(" ") || "none"}`,
		);
	} finally {
		await peer.close();
		await stub.close();
		site?.close();
		if (siteDir) rmSync(siteDir, { recursive: true, force: true });
		await dev?.stop();
	}
}

/** Seconds played, from the seek bar. */
async function position(page: Page): Promise<number> {
	const value = await page
		.locator('[role="slider"][aria-label="Seek"]')
		.getAttribute("aria-valuenow", { timeout: 500 })
		.catch(() => null);
	return Number(value ?? 0);
}

/** "37.5% recovered" on the video page (0 until shown). */
async function percent(page: Page): Promise<number> {
	const text = await page
		.locator("p", { hasText: "recovered" })
		.first()
		.innerText({ timeout: 500 })
		.catch(() => "");
	const m = /([\d.]+)%/.exec(text);
	return m ? Number(m[1]) : 0;
}

async function waitUntil(test: () => Promise<boolean>, timeoutMs: number, what: string): Promise<void> {
	const until = Date.now() + timeoutMs;
	while (Date.now() < until) {
		if (await test()) return;
		await new Promise((r) => setTimeout(r, 250));
	}
	throw new Error(what);
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
