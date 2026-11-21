// The file-list lookup in the browser (M3-11): the metadata from whoever
// answers first, always checked against the info-hash; a wait that follows
// progress; a retry that reuses the session; the diagnostics report.
//
//   chromium (system or CHROMIUM_PATH) + fake peers + /peers stub + wrangler dev + the site's production build
//
// 1. `info` line present, the only peer serves no metadata: the file list
//    comes from the VPS line, no BEP 9 request is made.
// 2. `info` line forged (another torrent's dict): the correct list comes
//    from the peer (BEP 9).
// 3. `info` line absent: the list comes from the peer.
// 4. Only dead addresses: the lookup fails at once, with a clear message,
//    once discovery is over. "Retry" reuses the session: no new WebSocket,
//    no redial of the addresses that failed (their back-off is not over),
//    and the second /peers answer's peer lists the files.
// 5. The exported diagnostics carry the new counters (lookup sources, dials
//    by class and outcome, the scout's stream) and no proof-of-concept
//    section, and stay anonymous.

import { mkdirSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import type { Server } from "node:http";
import { join } from "node:path";
import { chromium, type Page } from "playwright-core";
import { makeTorrent, startFakePeer, type FakePeer, type FakeTorrent } from "@magnet-stream/fake-peer";
import { closedPort, freePort, startPeersStub, startWranglerDev, type StubLine, type WranglerDev } from "@magnet-stream/harness";
import { ROOT, buildSite, findChromium, serveSite, waitFor } from "./site.ts";

/** A failed lookup with nothing left to try must say so within this (discovery over, dials failed, 5 s grace). */
const FAIL_FAST_MS = 15_000;

const chromiumPath = findChromium();
if (!chromiumPath) {
	console.log("skip lookup e2e: no chromium (set CHROMIUM_PATH)");
	process.exit(process.env.REQUIRE_BROWSER_E2E ? 1 : 0);
}

let dev: WranglerDev | undefined;
let site: Server | undefined;
let siteDir: string | undefined;
let failed = false;

/** Two small videos, so the page stops at the file list. */
function show(seed: number, prefix: string): FakeTorrent {
	return makeTorrent({
		seed,
		name: `${prefix}-show`,
		pieceLength: 64 * 1024,
		files: [
			{ path: [`${prefix}1.mkv`], length: 512 * 1024 },
			{ path: [`${prefix}2.mkv`], length: 512 * 1024 },
		],
	});
}

async function main(): Promise<void> {
	const fromVps = show(150, "vpsep");
	const forged = show(151, "rightep");
	const decoy = show(152, "decoyep");
	const absent = show(153, "peerep");
	const retried = show(154, "retryep");
	const peers: FakePeer[] = [
		// Serves pieces, never the metadata: only the info line can list the files.
		await startFakePeer({ torrent: fromVps, metadata: false }),
		await startFakePeer({ torrent: forged }),
		await startFakePeer({ torrent: absent }),
		await startFakePeer({ torrent: retried }),
		// Hangs up right after the handshake: a failure with a 60 s back-off; counts its connections.
		await startFakePeer({ torrent: retried, closeAfterHandshake: true }),
	];
	const [vpsPeer, forgedPeer, absentPeer, retryPeer, quitter] = peers;
	const dead = await Promise.all([0, 1].map(() => closedPort()));
	const deadLines = [...dead.map((p) => `127.0.0.1:${p}`), quitter.addr];
	const alive = (addr: string): StubLine => ({ peer: addr, score: 0.8, alive: true, seed: true, md: true });
	const stub = await startPeersStub(({ ih, n }) => {
		if (ih === fromVps.infoHashHex) return [{ info: fromVps.info }, alive(vpsPeer.addr)];
		if (ih === forged.infoHashHex) return [{ info: decoy.info }, alive(forgedPeer.addr)];
		if (ih === absent.infoHashHex) return [alive(absentPeer.addr)];
		// The retried magnet: dead addresses first, then (second request) its peer too.
		if (ih === retried.infoHashHex) return n === 0 ? deadLines : [...deadLines, alive(retryPeer.addr)];
		return [];
	});
	const magnet = (t: FakeTorrent) => `magnet:?xt=urn:btih:${t.infoHashHex}&dn=${t.name}`;
	try {
		const port = await freePort();
		const PAGE = `http://localhost:${port}/`;
		dev = await startWranglerDev({ vars: { ALLOW_PRIVATE_PEERS: "1", ALLOWED_ORIGINS: `http://localhost:${port}` } });
		mkdirSync(join(ROOT, "tools/e2e/results"), { recursive: true });
		siteDir = mkdtempSync(join(ROOT, "tools/e2e/results", "site-"));
		buildSite(stub.url, dev.url, siteDir);
		site = await serveSite(siteDir, port);
		await waitFor(PAGE, 30_000);

		const browser = await chromium.launch({ executablePath: chromiumPath, args: ["--no-sandbox", "--disable-dev-shm-usage"] });
		const page = await browser.newPage({ acceptDownloads: true });
		const sockets: { url: string; closed: boolean }[] = [];
		page.on("websocket", (ws) => {
			const entry = { url: ws.url(), closed: false };
			sockets.push(entry);
			ws.on("close", () => (entry.closed = true));
		});
		await page.goto(PAGE, { waitUntil: "domcontentloaded" });

		// 1. The info line lists the files; nobody is asked over BEP 9.
		await paste(page, magnet(fromVps));
		await listed(page, "vpsep1.mkv");
		check(vpsPeer.stats.metadataRequests === 0, `BEP 9 asked of the peer although the info line was there (${vpsPeer.stats.metadataRequests})`);
		await home(page);

		// 2. A forged info line is refused; the peer's dict lists the right files.
		await paste(page, magnet(forged));
		await listed(page, "rightep1.mkv");
		check((await page.getByText("decoyep1.mkv").count()) === 0, "the forged file list was shown");
		check(forgedPeer.stats.metadataRequests > 0, "the forged line's torrent was listed without asking the peer");
		await home(page);

		// 3. No info line: BEP 9 from the peer.
		await paste(page, magnet(absent));
		await listed(page, "peerep1.mkv");
		check(absentPeer.stats.metadataRequests > 0, "no BEP 9 request without an info line");
		await home(page);

		// 4. Nothing alive: a quick, clear failure; the retry reuses the session.
		const socketsBefore = sockets.length;
		const started = Date.now();
		await paste(page, magnet(retried));
		const error = page.getByTestId("lookup-error");
		await error.waitFor({ timeout: FAIL_FAST_MS + 5_000 });
		const failMs = Date.now() - started;
		const message = await error.innerText();
		check(failMs <= FAIL_FAST_MS, `the lookup took ${failMs} ms to give up with nothing to try (limit ${FAIL_FAST_MS})`);
		check(/None of the 3 peers found sent the file list/.test(message), `unclear failure message: ${message}`);
		check(!/\d+\.\d+\.\d+\.\d+|[0-9a-f]{40}/.test(message), "the failure message names an address or the info-hash");
		const opened = sockets.length - socketsBefore;
		const openNow = sockets.slice(socketsBefore).filter((s) => !s.closed).length;
		check(opened >= 1 && openNow === opened, `the failed session's WebSockets: ${opened} opened, ${openNow} still open`);
		const quitterConnections = quitter.stats.connections;
		check(quitterConnections >= 1, "the address that hangs up was never dialled");
		check(stub.requests() === 4, `expected 4 /peers requests so far, got ${stub.requests()}`);
		const beforeRetry = sessionsOf(await exportReport(page));

		await page.getByRole("button", { name: "Retry" }).click();
		await listed(page, "retryep1.mkv");
		check(sockets.length - socketsBefore === opened, `the retry opened ${sockets.length - socketsBefore - opened} new WebSocket(s)`);
		check(quitter.stats.connections === quitterConnections, `the retry redialled an address in back-off (${quitter.stats.connections - quitterConnections} more connections)`);
		check(stub.requests() === 5, `the retry should ask /peers once more (got ${stub.requests()} requests)`);

		// 5. The report: new counters, no proof-of-concept sections, anonymous.
		await home(page);
		const report = await exportReport(page);
		const text = JSON.stringify(report);
		const sessions = sessionsOf(report);
		check(sessions.length === beforeRetry.length, `the retry made a new session record (${beforeRetry.length} → ${sessions.length})`);
		const last = sessions.at(-1)!;
		check(last.lookup === "listed" && last.retries === 1 && (last.failures as Record<string, number>).exhausted === 1, `retried session: ${JSON.stringify({ lookup: last.lookup, retries: last.retries, failures: last.failures })}`);
		const tunnel = last.tunnel as { upgrades: number; dials: Record<string, { attempted: number; succeeded: number; failed: Record<string, number> }> };
		const earlier = beforeRetry.at(-1)!.tunnel as { upgrades: number };
		check(tunnel.upgrades === earlier.upgrades, `WebSocket upgrades went from ${earlier.upgrades} to ${tunnel.upgrades} on retry`);
		check(tunnel.dials.scout.succeeded >= 1 && Object.keys(tunnel.dials.untried.failed).length > 0, `dials by class and outcome: ${JSON.stringify(tunnel.dials)}`);
		const health = report.health as Record<string, { details: Record<string, unknown> }>;
		for (const part of ["lookup", "tunnel", "scout"]) check(part in health, `no ${part} section in the report`);
		for (const gone of ["seedersCount", "infoDict", "swarm", "worker", "metadataApi"]) check(!(gone in health), `the report still has the ${gone} section`);
		check(!/magnet-worker|magnet-seeders|\/swarm|\/metadata/.test(JSON.stringify(health)), "the report still names the proof of concept's services");
		const sources = health.lookup.details.sources as Record<string, number>;
		check(sources.vps === 1 && sources.peer === 3, `metadata sources: ${JSON.stringify(sources)}`);
		const info = health.scout.details.info as Record<string, number>;
		check(info.accepted === 1 && info.rejected === 1, `info lines: ${JSON.stringify(info)}`);
		const names = [fromVps, forged, decoy, absent, retried].flatMap((t) => [t.infoHashHex, t.name, ...t.files.map((f) => String(f.path).split("/").at(-1)!)]);
		for (const name of names) check(!text.includes(name), `the report names ${name.length === 40 ? "an info-hash" : `a file (${name})`}`);
		check(!/magnet:\?|\b\d{1,3}(?:\.\d{1,3}){3}:\d+\b|localhost:\d+/.test(text), "the report names a magnet or an address");

		await browser.close();
		const dials = tunnel.dials;
		console.log(
			`lookup: info line → list without BEP 9; forged line refused, list from the peer; failure in ${failMs} ms ("${message}"); ` +
				`retry on the same ${opened} WebSocket(s), no redial; dials scout ${dials.scout.succeeded}/${dials.scout.attempted}, ` +
				`untried ${dials.untried.succeeded}/${dials.untried.attempted} ${JSON.stringify(dials.untried.failed)}`,
		);
	} finally {
		await Promise.all(peers.map((p) => p.close()));
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

async function listed(page: Page, file: string): Promise<void> {
	try {
		await page.getByRole("button", { name: new RegExp(file.replace(".", "\\.")) }).first().waitFor({ timeout: 30_000 });
	} catch (e) {
		console.log("PAGE:\n", (await page.locator("body").innerText()).slice(0, 1500));
		throw e;
	}
}

async function home(page: Page): Promise<void> {
	await page.getByRole("button", { name: "Home" }).click();
	await page.locator('textarea[aria-label="Magnet link"]').waitFor({ timeout: 10_000 });
}

/** Export the diagnostics from the menu, as the user would. */
async function exportReport(page: Page): Promise<Record<string, unknown>> {
	// The session's counters reach the page every few seconds.
	await page.waitForTimeout(5_500);
	await page.getByRole("button", { name: "Diagnostics" }).click();
	const [download] = await Promise.all([page.waitForEvent("download"), page.getByRole("menuitem", { name: "Export as JSON" }).click()]);
	await page.keyboard.press("Escape");
	return JSON.parse(readFileSync(await download.path(), "utf8")) as Record<string, unknown>;
}

function sessionsOf(report: Record<string, unknown>): Record<string, unknown>[] {
	const visits = report.visits as { sessions?: Record<string, unknown>[] }[];
	return visits.flatMap((v) => v.sessions ?? []);
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
