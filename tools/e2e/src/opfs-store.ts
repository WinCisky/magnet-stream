// The OPFS piece store in Chromium (M3-03): the engine worker's code
// (apps/web/src/lib/torrent/opfs-store.ts, piece-file.ts), bundled as is,
// runs in a dedicated worker of a real page and
//   - writes and reads pieces at random places of a sparse file over 1 GiB,
//   - reads a 16 KiB block of a verified piece, and fails on an unverified one,
//   - reports the missing pieces of a range instead of returning bytes,
//   - keeps its map across a reopen (a reload), and
//   - the library (library-core.ts, in the page's library worker) lists,
//     reads and deletes it, refusing while the engine worker holds it open.
// Nothing leaves the browser: the page has no network access besides itself.

import { existsSync } from "node:fs";
import { createServer } from "node:http";
import { build } from "esbuild";
import { chromium } from "playwright-core";
import { freePort } from "@magnet-stream/harness";

const ROOT = new URL("../../../", import.meta.url).pathname;
const chromiumPath = [process.env.CHROMIUM_PATH, "/usr/bin/chromium", "/usr/bin/chromium-browser", "/usr/bin/google-chrome", bundled()].find(
	(p) => p && existsSync(p),
);

function bundled(): string | undefined {
	try {
		return chromium.executablePath();
	} catch {
		return undefined;
	}
}

if (!chromiumPath) {
	console.log("skip OPFS e2e: no chromium (set CHROMIUM_PATH)");
	process.exit(process.env.REQUIRE_BROWSER_E2E ? 1 : 0);
}

/** The worker: the store's own code plus a scripted scenario. */
const WORKER = `
import { openOpfsStore } from "${ROOT}apps/web/src/lib/torrent/opfs-store.ts";
import * as lib from "${ROOT}apps/web/src/lib/torrent/library-core.ts";

const PL = 4 * 1024 * 1024;
const COUNT = 320; // 1.25 GiB
const LAST = 1234567;
const HASH = "c".repeat(40);
const layout = { pieceLength: PL, pieceCount: COUNT, pieceSize: (p) => (p === COUNT - 1 ? LAST : PL) };
const fill = (p, size = layout.pieceSize(p)) => {
	const out = new Uint8Array(size);
	let x = (p + 1) * 2654435761;
	for (let i = 0; i < size; i += 4096) { x = (x * 1103515245 + 12345) >>> 0; out[i] = x & 0xff; out[size - 1 - (i % size)] ^= p & 0xff; }
	return out;
};
const same = (a, b) => a.length === b.length && a.every((v, i) => v === b[i]);
const check = (ok, what) => { if (!ok) throw new Error(what); };

onmessage = async ({ data: step }) => {
	try {
		const root = await navigator.storage.getDirectory();
		if (step === "write") {
			const store = await openOpfsStore(HASH, layout);
			// Random places, out of order, the last (short) piece included, well past 1 GiB.
			const order = [300, 3, COUNT - 1, 257, 0, 128, 299, 64, 290];
			for (const p of order) store.write(p, fill(p));
			for (const p of [...order].reverse()) check(same(store.readPiece(p), fill(p)), "piece " + p + " reads back");
			const block = store.readBlock(300, 16384, 16384);
			check(same(block, fill(300).subarray(16384, 32768)), "16 KiB block of a verified piece");
			let refused = "";
			try { store.readBlock(301, 0, 16384); } catch (e) { refused = e.code; }
			check(refused === "unverified", "an unverified block is refused: " + refused);
			const range = store.readRange(299 * PL - 10, PL + 20);
			check(range.bytes === null && range.missing.join() === "298", "a range over a missing piece reports it: " + range.missing);
			const ok = store.readRange(299 * PL + 5, PL);
			check(same(ok.bytes, new Uint8Array([...fill(299).subarray(5), ...fill(300).subarray(0, 5)])), "a verified range reads back");
			// The library can't delete what the engine worker holds open, and changes nothing.
			await lib.rememberFile(root, HASH, { name: "big", magnet: "magnet:?xt=urn:btih:" + HASH, pieceLength: PL, totalLength: (COUNT - 1) * PL + LAST }, { index: 0, path: "big/a.mkv", offset: 0, size: 300 * PL });
			await lib.rememberFile(root, HASH, { name: "big", magnet: "magnet:?xt=urn:btih:" + HASH, pieceLength: PL, totalLength: (COUNT - 1) * PL + LAST }, { index: 1, path: "big/b.mkv", offset: 300 * PL, size: 19 * PL + LAST });
			let busy = "";
			try { await lib.deleteSavedFile(root, HASH, 1); } catch (e) { busy = e.code; }
			check(busy === "busy", "delete refused while open: " + busy);
			const listed = await lib.listSaved(root);
			check(listed.files.length === 2, "both files still listed while open");
			const size = (await (await (await root.getDirectoryHandle("pieces")).getFileHandle(HASH + ".data")).getFile()).size;
			store.close();
			postMessage({ ok: true, size, pieces: order.length });
		} else if (step === "reopen") {
			const store = await openOpfsStore(HASH, layout);
			const pieces = store.pieces();
			check(pieces.join() === "0,3,64,128,257,290,299,300,319", "map survives a reload: " + pieces);
			check(same(store.readPiece(257), fill(257)), "bytes survive a reload");
			store.close();
			const list = await lib.listSaved(root);
			const b = list.files.find((f) => f.index === 1);
			check(b && !b.complete && b.savedBytes === PL + LAST, "partly saved file listed with its saved bytes");
			// Delete file b: its exclusive pieces go, the file shrinks back to a's last piece.
			await lib.deleteSavedFile(root, HASH, 1);
			const after = await lib.storedPieces(root, HASH);
			check([...after].sort((x, y) => x - y).join() === "0,3,64,128,257,290,299", "b's pieces forgotten: " + [...after]);
			const size = (await (await (await root.getDirectoryHandle("pieces")).getFileHandle(HASH + ".data")).getFile()).size;
			check(size === 300 * PL, "trailing space given back: " + size);
			await lib.deleteAllSaved(root);
			const names = [];
			for await (const name of (await root.getDirectoryHandle("pieces")).keys()) names.push(name);
			check(names.length === 0, "delete all leaves nothing: " + names);
			postMessage({ ok: true });
		}
	} catch (e) {
		postMessage({ ok: false, error: String(e && e.stack || e) });
	}
};
`;

async function main(): Promise<void> {
	const bundle = await build({
		stdin: { contents: WORKER, resolveDir: ROOT, loader: "js", sourcefile: "opfs-e2e-worker.js" },
		bundle: true,
		format: "esm",
		write: false,
		platform: "browser",
		target: "es2022",
	});
	const workerJs = bundle.outputFiles[0].text;
	const page = `<!doctype html><script type="module">
		const worker = new Worker("/worker.js", { type: "module" });
		window.step = (name) => new Promise((resolve) => { worker.onmessage = (e) => resolve(e.data); worker.postMessage(name); });
		window.ready = true;
	</script>`;
	const port = await freePort();
	const server = createServer((req, res) => {
		if (req.url === "/worker.js") {
			res.setHeader("content-type", "text/javascript");
			res.end(workerJs);
		} else {
			res.setHeader("content-type", "text/html");
			res.end(page);
		}
	});
	await new Promise<void>((r) => server.listen(port, "127.0.0.1", r));
	const browser = await chromium.launch({ executablePath: chromiumPath, args: ["--no-sandbox", "--disable-dev-shm-usage"] });
	try {
		const context = await browser.newContext();
		const tab = await context.newPage();
		const url = `http://localhost:${port}/`;
		await tab.goto(url);
		await tab.waitForFunction(() => (globalThis as unknown as { ready?: boolean }).ready === true);
		const run = async (name: string) => {
			const result = (await tab.evaluate((n) => (globalThis as unknown as { step(n: string): Promise<unknown> }).step(n), name)) as { ok: boolean; error?: string };
			if (!result.ok) throw new Error(`${name}: ${result.error}`);
			return result as { ok: true; size?: number };
		};
		const written = await run("write");
		if (!(written.size! > 2 ** 30)) throw new Error(`expected a sparse file over 1 GiB, got ${written.size}`);
		await tab.reload();
		await tab.waitForFunction(() => (globalThis as unknown as { ready?: boolean }).ready === true);
		await run("reopen");
		console.log(`OPFS store: random writes and reads on a ${(written.size! / 2 ** 30).toFixed(2)} GiB sparse file, block reads, reload, partial and full delete — ok`);
	} finally {
		await browser.close();
		server.close();
	}
}

main().catch((e: unknown) => {
	console.error(e);
	process.exit(1);
});
