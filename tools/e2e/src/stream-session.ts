// Streaming service worker and native playback (M3-05): an MP4 recovered
// through the tunnel plays in a plain <video> over /stream/<ih>/<file>,
// a seek to data not recovered yet moves the recovery there and playback
// resumes, the worker answers ranges (206), plain requests (200),
// unsatisfiable ranges (416) and invalid targets (404), and the download
// link (the same URL, `?download=1`) saves the same bytes. The page is cross-origin isolated on its first
// load from the server's headers, and the streaming worker is the only one.
//
// Player integration (M3-06): the native player is chosen from the file's
// codecs (VP9 in MP4), playback starts only with ~10 s verified and resumes
// after the seek only with 5 s verified, the picker's window follows the
// video's bitrate, and the page requests nothing outside the site, the
// gateway and the discovery service.
//
//   chromium (system or CHROMIUM_PATH) + a paced fake peer + /peers stub + wrangler dev + the site's production build
//
// The video is tools/e2e/fixtures/clip-vp9.mp4 with its samples spread
// apart (mp4-spread.ts): 40 s of VP9 (which every
// Chromium build decodes, Playwright's included) in MP4, index at the end,
// made with
//   ffmpeg -f lavfi -i testsrc2=size=320x180:rate=24:duration=40 -c:v libvpx-vp9 -b:v 200k -g 48 \
//     -deadline good -cpu-used 4 -pix_fmt yuv420p -map_metadata -1 -fflags +bitexact -flags:v +bitexact -f mp4 clip-vp9.mp4

import { createHash } from "node:crypto";
import { mkdirSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import type { Server } from "node:http";
import { join } from "node:path";
import { chromium, type Page } from "playwright-core";
import { makeTorrent, startFakePeer } from "@magnet-stream/fake-peer";
import { freePort, startPeersStub } from "@magnet-stream/harness";
import { spreadSamples } from "./mp4-spread.ts";
import { ROOT, buildSite, findChromium, serveSite, startSiteGateway, waitFor, type SiteGateway } from "./site.ts";

const FIXTURE = new URL("../fixtures/clip-vp9.mp4", import.meta.url);
const DURATION_S = 40;
/** Where the seek lands (seconds): ArrowRight skips 10 s, three times from the first seconds. */
const SKIPS = 3;

const chromiumPath = findChromium();
if (!chromiumPath) {
	console.log("skip stream e2e: no chromium (set CHROMIUM_PATH)");
	process.exit(process.env.REQUIRE_BROWSER_E2E ? 1 : 0);
}

let dev: SiteGateway | undefined;
let site: Server | undefined;
let siteDir: string | undefined;
let failed = false;

// Browser-side code runs in page.evaluate: these are the few DOM shapes it uses.
type Video = { currentTime: number; paused: boolean; readyState: number; src: string; error: { code: number } | null };
type Win = {
	crossOriginIsolated: boolean;
	document: { querySelector(s: string): Video | null };
	navigator: { serviceWorker: { controller: { scriptURL: string } | null; getRegistrations(): Promise<{ active: { scriptURL: string } | null }[]> } };
	fetch(url: string, init?: { headers?: Record<string, string>; method?: string }): Promise<{
		status: number;
		headers: { get(name: string): string | null };
		arrayBuffer(): Promise<ArrayBuffer>;
	}>;
	crypto: { subtle: { digest(alg: string, data: ArrayBuffer): Promise<ArrayBuffer> } };
};

async function main(): Promise<void> {
	// 16 KiB between samples: about 16 MiB, so a seek of 30 s is a jump of
	// about 12 MiB and gets its own range request (mp4-spread.ts).
	const content = spreadSamples(new Uint8Array(readFileSync(FIXTURE)), 16 * 1024);
	const torrent = makeTorrent({ content, name: "clip.mp4", pieceLength: 256 * 1024 });
	const sha = createHash("sha256").update(content).digest("hex");
	// One 16 KiB block every 20 ms: about 20 s for the whole file, so a seek
	// soon after playback starts lands on data not recovered yet.
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
		const context = await browser.newContext({ acceptDownloads: true });
		const page = await context.newPage();
		const allowed = new Set([new URL(PAGE).origin, new URL(dev.url).origin, new URL(stub.url).origin]);
		const foreign: string[] = [];
		context.on("request", (req) => {
			const url = new URL(req.url());
			if (url.protocol !== "data:" && url.protocol !== "blob:" && !allowed.has(url.origin)) foreign.push(url.origin);
		});
		const streamed: { status: number; range: string | null; contentRange: string | null; type: string | null; ranges: string | null }[] = [];
		const logs: string[] = [];
		page.on("console", (m) => logs.push(`${m.type()}: ${m.text()}`));
		page.on("pageerror", (e) => logs.push(`pageerror: ${e.message}`));
		page.on("response", (res) => {
			if (!new URL(res.url()).pathname.startsWith("/stream/")) return;
			const h = res.headers();
			streamed.push({ status: res.status(), range: res.request().headers()["range"] ?? null, contentRange: h["content-range"] ?? null, type: h["content-type"] ?? null, ranges: h["accept-ranges"] ?? null });
		});

		// 1. Isolated on the first load, from the headers alone; the streaming worker takes control.
		await page.goto(PAGE, { waitUntil: "domcontentloaded" });
		check(await page.evaluate(() => (globalThis as unknown as Win).crossOriginIsolated), "the page isn't cross-origin isolated on its first load");
		await waitUntil(() => page.evaluate(() => (globalThis as unknown as Win).navigator.serviceWorker.controller !== null), 10_000, "the streaming worker never took control");
		const workers = await page.evaluate(async () => {
			const sw = (globalThis as unknown as Win).navigator.serviceWorker;
			return { controller: sw.controller?.scriptURL ?? "", all: (await sw.getRegistrations()).map((r) => r.active?.scriptURL ?? "") };
		});
		check(workers.controller.endsWith("/stream-sw.js"), `controlled by ${workers.controller}`);
		check(workers.all.length === 1 && workers.all[0].endsWith("/stream-sw.js"), `service workers: ${workers.all.join(" ")}`);

		// 2. Open the MP4: the native player plays it from /stream/.
		const box = page.locator('textarea[aria-label="Magnet link"]');
		await box.waitFor({ timeout: 30_000 });
		await box.click();
		await page.keyboard.insertText(torrent.magnet);
		await page.locator('button[aria-label="Search"]').click();
		const openedAt = Date.now();
		const video = page.locator('video[src^="/stream/"]');
		await video.waitFor({ state: "attached", timeout: 60_000 });
		const src = await video.getAttribute("src");
		check(src === `/stream/${torrent.infoHashHex}/0`, `video src ${src}`);
		// The big play button in the middle of the picture.
		await page.locator('button.size-16[aria-label="Play"]').click();
		try {
			await waitUntil(async () => (await state(page)).time > 1, 60_000, "the native player never played");
		} catch (e) {
			console.log("VIDEO", JSON.stringify(await state(page)), "\nSTREAMED", JSON.stringify(streamed), "\nLOGS", logs.join("\n"));
			console.log("PAGE:\n", (await page.locator("body").innerText()).slice(0, 1500));
			throw e;
		}

		const startMs = Date.now() - openedAt;
		const region = page.locator('[aria-label="Video player"]');
		check((await region.getAttribute("data-player")) === "native", `player: ${await region.getAttribute("data-player")}`);
		const startHold = ((await region.getAttribute("data-buffered")) ?? "").split(" ")[0];
		check(/^start:(end|\d+\.\d)$/.test(startHold) && (startHold === "start:end" || Number(startHold.slice(6)) >= 9.9), `start hold: ${startHold}`);
		// 3. Seek ahead, past what's recovered: the read moves the recovery there, playback resumes.
		const before = await percent(page);
		const from = (await state(page)).time;
		for (let i = 0; i < SKIPS; i++) await page.keyboard.press("ArrowRight");
		const target = Math.min(DURATION_S - 4, from + SKIPS * 10);
		const seekedAt = Date.now();
		await waitUntil(async () => (await state(page)).time >= target + 2, 90_000, `playback didn't resume past ${target + 2} s after the seek`);
		const after = await state(page);
		check(!after.paused && after.error === null, `after the seek: ${JSON.stringify(after)}`);
		check(before < (100 * target) / DURATION_S - 10, `the seek target was probably recovered already (${before}% at the seek, target ${target} s)`);
		const resumeMs = Date.now() - seekedAt;
		const resumes = ((await region.getAttribute("data-buffered")) ?? "").split(" ").filter((h) => h.startsWith("resume:"));
		check(resumes.length >= 1 && resumes.every((h) => h === "resume:end" || Number(h.slice(7)) >= 4.9), `resume holds: ${resumes.join(" ")}`);
		// The picker's window: 30 s at the video's bitrate (its size over its 40 s).
		const bitrate = Math.round(torrent.totalLength / DURATION_S);
		const windowLine = page.getByTestId("playback-window");
		await waitUntil(async () => Number(await windowLine.getAttribute("data-bitrate")) > 0, 10_000, "the bitrate never reached the recovery");
		const windowBytes = Number(await windowLine.getAttribute("data-window-bytes"));
		check(Math.abs(windowBytes - 30 * bitrate) <= 30 * bitrate * 0.02, `window ${windowBytes} B, expected about ${30 * bitrate} B (30 s at ${bitrate} B/s)`);

		// 4. Ranges from the worker, as the player asked them.
		const partial = streamed.filter((r) => r.status === 206);
		check(partial.length >= 2, `expected range responses, got ${JSON.stringify(streamed)}`);
		for (const r of partial) {
			check(r.ranges === "bytes" && r.type === "video/mp4" && /^bytes \d+-\d+\/\d+$/.test(r.contentRange ?? ""), `a 206 without its headers: ${JSON.stringify(r)}`);
		}
		await page.getByText("complete", { exact: false }).waitFor({ timeout: 120_000 });

		// 5. The worker's answers, from the page.
		const ih = torrent.infoHashHex;
		const size = torrent.totalLength;
		const answers = await page.evaluate(
			async ({ ih, size }) => {
				const w = globalThis as unknown as Win;
				const hex = async (b: ArrayBuffer) => [...new Uint8Array(await w.crypto.subtle.digest("SHA-256", b))].map((x) => x.toString(16).padStart(2, "0")).join("");
				const url = `/stream/${ih}/0`;
				const full = await w.fetch(url);
				const fullBody = await full.arrayBuffer();
				const range = await w.fetch(url, { headers: { Range: "bytes=100-199" } });
				const rangeBody = new Uint8Array(await range.arrayBuffer());
				const past = await w.fetch(url, { headers: { Range: `bytes=${size}-` } });
				const head = await w.fetch(`${url}?download=1`, { method: "HEAD" });
				const status = async (u: string) => (await w.fetch(u)).status;
				return {
					full: { status: full.status, length: fullBody.byteLength, sha: await hex(fullBody), type: full.headers.get("content-type"), nosniff: full.headers.get("x-content-type-options") },
					range: { status: range.status, contentRange: range.headers.get("content-range"), bytes: [...rangeBody.slice(0, 4)], length: rangeBody.length },
					past: { status: past.status, contentRange: past.headers.get("content-range") },
					disposition: head.headers.get("content-disposition"),
					invalidHash: await status(`/stream/${ih.slice(0, 39)}x/0`),
					shortHash: await status(`/stream/${ih.slice(2)}/0`),
					badIndex: await status(`/stream/${ih}/x1`),
					notOpen: await status(`/stream/${ih}/1`),
					otherTorrent: await status(`/stream/${"ab".repeat(20)}/0`),
				};
			},
			{ ih, size },
		);
		check(answers.full.status === 200 && answers.full.length === size && answers.full.sha === sha, `200: ${JSON.stringify(answers.full)}`);
		check(answers.full.type === "video/mp4" && answers.full.nosniff === "nosniff", `200 headers: ${JSON.stringify(answers.full)}`);
		check(
			answers.range.status === 206 && answers.range.contentRange === `bytes 100-199/${size}` && answers.range.length === 100 && answers.range.bytes.join() === [...content.slice(100, 104)].join(),
			`206: ${JSON.stringify(answers.range)}`,
		);
		check(answers.past.status === 416 && answers.past.contentRange === `bytes */${size}`, `416: ${JSON.stringify(answers.past)}`);
		check(answers.disposition === `attachment; filename="clip.mp4"; filename*=UTF-8''clip.mp4`, `download disposition: ${answers.disposition}`);
		for (const k of ["invalidHash", "shortHash", "badIndex", "notOpen", "otherTorrent"] as const) check(answers[k] === 404, `${k}: ${answers[k]}, expected 404`);

		// 6. The download link saves the same bytes.
		const [download] = await Promise.all([page.waitForEvent("download", { timeout: 30_000 }), page.getByRole("link", { name: "Download clip.mp4" }).click()]);
		const failure = await download.failure();
		check(failure === null, `download failed: ${failure}`);
		const saved = failure ? null : await download.path();
		const savedSha = saved ? createHash("sha256").update(readFileSync(saved)).digest("hex") : "";
		check(download.suggestedFilename() === "clip.mp4", `download named ${download.suggestedFilename()}`);
		check(savedSha === sha, "the downloaded file differs from the video");
		await browser.close();
		check(foreign.length === 0, `requests outside the site and its services: ${[...new Set(foreign)].join(" ")}`);
		console.log(
			`stream: native <video> over /stream/ playing ${startMs} ms after the paste (${startHold}), ${partial.length} range responses, seek from ${from.toFixed(1)} s to ${target.toFixed(1)} s at ${before.toFixed(1)}% recovered resumed in ${resumeMs} ms (${resumes.join(" ")}), window ${windowBytes} B; 200/206/416/404 and download ok`,
		);
	} finally {
		await peer.close();
		await stub.close();
		site?.close();
		if (siteDir) rmSync(siteDir, { recursive: true, force: true });
		await dev?.stop();
	}
}

async function state(page: Page): Promise<{ time: number; paused: boolean; error: number | null; ready: number }> {
	return page.evaluate(() => {
		const v = (globalThis as unknown as Win).document.querySelector('video[src^="/stream/"]');
		return { time: v?.currentTime ?? 0, paused: v?.paused ?? true, error: v?.error?.code ?? null, ready: v?.readyState ?? -1 };
	});
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
