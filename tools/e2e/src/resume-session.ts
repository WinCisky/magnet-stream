// Session memory in the browser (M3-04): a reload halfway through a
// download resumes from what IndexedDB kept, with no `/peers` request and
// no metadata fetch (BEP 9), and the download completes; a magnet only
// looked up before lists its files again from the cache within 1 s.
//
//   chromium (system or CHROMIUM_PATH) + fake peers + /peers stub + wrangler dev + the site's production build
//
// The magnets carry no peer hint (`x.pe`): after the reload, the only
// addresses the session knows are those it remembered.

import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
import type { Server } from "node:http";
import { join } from "node:path";
import { chromium, type Page } from "playwright-core";
import { makeTorrent, startFakePeer } from "@magnet-stream/fake-peer";
import { closedPort, freePort, startPeersStub } from "@magnet-stream/harness";
import { ROOT, buildSite, findChromium, serveSite, startSiteGateway, waitFor, type SiteGateway } from "./site.ts";

const FILE_BYTES = 64 * 1024 * 1024;
/** Reload once this much is recovered. */
const RELOAD_AT_PERCENT = 15;
/** The cached file list must show within this (acceptance criterion). */
const CACHED_LIST_MS = 1_000;

const chromiumPath = findChromium();
if (!chromiumPath) {
	console.log("skip resume e2e: no chromium (set CHROMIUM_PATH)");
	process.exit(process.env.REQUIRE_BROWSER_E2E ? 1 : 0);
}

let dev: SiteGateway | undefined;
let site: Server | undefined;
let siteDir: string | undefined;
let failed = false;

async function main(): Promise<void> {
	const film = makeTorrent({ seed: 140, name: "film.mkv", size: FILE_BYTES, pieceLength: 256 * 1024 });
	const show = makeTorrent({
		seed: 141,
		name: "show",
		pieceLength: 64 * 1024,
		files: [
			{ path: ["ep1.mkv"], length: 1024 * 1024 },
			{ path: ["ep2.mkv"], length: 1024 * 1024 },
		],
	});
	// Paced seeds: the download lasts long enough to reload in the middle.
	const seeds = await Promise.all([startFakePeer({ torrent: film, blockGapMs: 10 }), startFakePeer({ torrent: film, blockGapMs: 10 })]);
	const showPeer = await startFakePeer({ torrent: show });
	const dead = await closedPort();
	const stub = await startPeersStub([`127.0.0.1:${dead}`, ...seeds.map((p) => p.addr), showPeer.addr]);
	const magnet = (ih: string, name: string) => `magnet:?xt=urn:btih:${ih}&dn=${name}`;
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
		const page = await browser.newPage();
		const peersRequests: string[] = [];
		page.on("request", (req) => {
			const url = new URL(req.url());
			if (url.pathname === "/peers" && url.searchParams.has("ih")) peersRequests.push(url.searchParams.get("ih")!);
		});
		await page.goto(PAGE, { waitUntil: "domcontentloaded" });

		// 1. Open the film, recover part of it.
		await paste(page, magnet(film.infoHashHex, "film.mkv"));
		await waitUntil(async () => (await percent(page)) >= RELOAD_AT_PERCENT, 120_000, "the film never got going");
		// Notes are written every few seconds: wait for a peer that delivered.
		await waitUntil(async () => (await memory(page, film.infoHashHex)).delivering > 0, 30_000, "no peer note was kept");
		const before = await percent(page);
		check(before < 90, `the film went too fast to test a resume (${before}%)`);
		const kept = await memory(page, film.infoHashHex);
		check(kept.info && kept.saved, `metadata kept: ${kept.info}, saved: ${kept.saved}`);
		const peersBefore = stub.requests();
		const metadataBefore = seeds.reduce((n, p) => n + p.stats.metadataRequests, 0);
		check(peersBefore === 1, `expected 1 /peers request before the reload, got ${peersBefore}`);

		// 2. Reload in the middle: resume without discovery or BEP 9, and complete.
		await page.reload({ waitUntil: "domcontentloaded" });
		try {
			await page.getByText("complete", { exact: false }).waitFor({ timeout: 180_000 });
		} catch (e) {
			console.log("PAGE:\n", (await page.locator("body").innerText()).slice(0, 1500));
			throw e;
		}
		check(stub.requests() === peersBefore, `the resumed session asked /peers (${stub.requests() - peersBefore} more)`);
		check(peersRequests.filter((ih) => ih === film.infoHashHex).length === 1, `the browser sent ${peersRequests.length} /peers requests for the film`);
		const metadataAfter = seeds.reduce((n, p) => n + p.stats.metadataRequests, 0);
		check(metadataAfter === metadataBefore, `the resumed session fetched the metadata again (${metadataAfter - metadataBefore} requests)`);
		check((await mapBits(page, film.infoHashHex)) === film.pieceCount, "the film's pieces aren't all saved");

		// 3. A magnet only looked up: the second paste lists its files from the cache.
		await page.getByRole("button", { name: "Home" }).click();
		await paste(page, magnet(show.infoHashHex, "show"));
		const ep1 = page.getByRole("button", { name: /ep1\.mkv/ }).first();
		await ep1.waitFor({ timeout: 60_000 });
		await waitUntil(async () => (await memory(page, show.infoHashHex)).info, 10_000, "the looked-up metadata wasn't kept");
		const showMetadata = showPeer.stats.metadataRequests;
		await page.getByRole("button", { name: "Home" }).click();
		const started = Date.now();
		await paste(page, magnet(show.infoHashHex, "show"));
		await ep1.waitFor({ timeout: 10_000 });
		const listMs = Date.now() - started;
		check(listMs <= CACHED_LIST_MS, `the cached file list took ${listMs} ms (limit ${CACHED_LIST_MS})`);
		await page.waitForTimeout(1_000);
		check(showPeer.stats.metadataRequests === showMetadata, "the second paste fetched the metadata again");
		const saved = await memory(page, show.infoHashHex);
		check(!saved.saved, "a magnet only looked up is marked saved");

		// 4. Deleting the film deletes what was kept about it.
		await page.getByRole("button", { name: "Home" }).click();
		await page.getByRole("button", { name: "Saved" }).click();
		await page.getByRole("button", { name: "Delete film.mkv" }).click();
		await page.getByRole("button", { name: "Delete", exact: true }).click();
		await page.getByText("Nothing saved yet").waitFor({ timeout: 15_000 });
		const gone = await memory(page, film.infoHashHex);
		check(!gone.info && !gone.saved && gone.notes === 0, `records left after the delete: ${JSON.stringify(gone)}`);
		await browser.close();
		console.log(
			`resume: reload at ${before.toFixed(1)}% → complete with no /peers and no metadata request; cached file list in ${listMs} ms; delete cascades`,
		);
	} finally {
		await Promise.all([...seeds, showPeer].map((p) => p.close()));
		await stub.close();
		site?.close();
		if (siteDir) rmSync(siteDir, { recursive: true, force: true });
		await dev?.stop();
	}
}

async function paste(page: Page, link: string): Promise<void> {
	const box = page.locator('textarea[aria-label="Magnet link"]');
	await box.waitFor({ timeout: 30_000 });
	await box.fill("");
	await box.click();
	await page.keyboard.insertText(link);
	await page.locator('button[aria-label="Search"]').click();
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

/** What the session memory (IndexedDB) holds for a torrent. */
function memory(page: Page, infoHash: string): Promise<{ info: boolean; saved: boolean; notes: number; delivering: number }> {
	return page.evaluate(async (ih) => {
		// Browser-side code: the DOM types aren't loaded here.
		type Req<T> = { result: T; onsuccess: (() => void) | null; onerror: (() => void) | null; onupgradeneeded?: (() => void) | null };
		type Store = { get(key: string): Req<unknown> };
		type Db = { objectStoreNames: { contains(name: string): boolean }; transaction(names: string[]): { objectStore(name: string): Store }; close(): void };
		const idb = (globalThis as unknown as { indexedDB: { open(name: string): Req<Db>; databases(): Promise<{ name?: string }[]> } }).indexedDB;
		// Opening a database that doesn't exist would create it, empty.
		if (!(await idb.databases()).some((d) => d.name === "magnet-stream-memory")) return { info: false, saved: false, notes: 0, delivering: 0 };
		const wait = <T>(req: Req<T>) =>
			new Promise<T>((resolve, reject) => {
				req.onsuccess = () => resolve(req.result);
				req.onerror = () => reject(new Error("idb"));
			});
		const db = await wait(idb.open("magnet-stream-memory"));
		try {
			if (!db.objectStoreNames.contains("torrents")) return { info: false, saved: false, notes: 0, delivering: 0 };
			const tx = db.transaction(["torrents", "peers"]);
			const torrent = (await wait(tx.objectStore("torrents").get(ih))) as { info?: unknown; saved?: boolean } | undefined;
			const peers = (await wait(tx.objectStore("peers").get(ih))) as { notes?: { delivered?: number }[] } | undefined;
			const notes = peers?.notes ?? [];
			return {
				info: torrent?.info instanceof Uint8Array,
				saved: torrent?.saved === true,
				notes: notes.length,
				delivering: notes.filter((n) => (n.delivered ?? 0) > 0).length,
			};
		} finally {
			db.close();
		}
	}, infoHash);
}

/** Verified pieces in the torrent's OPFS map. */
function mapBits(page: Page, infoHash: string): Promise<number> {
	return page.evaluate(async (ih) => {
		type Dir = {
			getDirectoryHandle(name: string): Promise<Dir>;
			getFileHandle(name: string): Promise<{ getFile(): Promise<{ arrayBuffer(): Promise<ArrayBuffer> }> }>;
		};
		const storage = (globalThis as unknown as { navigator: { storage: { getDirectory(): Promise<Dir> } } }).navigator.storage;
		try {
			const dir = await (await storage.getDirectory()).getDirectoryHandle("pieces");
			const map = new Uint8Array(await (await (await dir.getFileHandle(`${ih}.map`)).getFile()).arrayBuffer());
			let bits = 0;
			for (const byte of map) for (let i = 0; i < 8; i++) bits += (byte >> i) & 1;
			return bits;
		} catch {
			return 0;
		}
	}, infoHash);
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
