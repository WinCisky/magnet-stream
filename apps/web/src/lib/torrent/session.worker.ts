// The torrent session, in a dedicated worker: one /peers stream, one to
// three tunnel WebSockets, metadata over BEP 9, and the blocks the page's
// scheduler asks for. Discovery is one streamed /peers request.

import { ConnectionManager, MetadataFetcher, listing, parseMagnet, readPeerStream } from "@magnet-stream/engine";
import { parsePageMessage, type WorkerMessage } from "./session-protocol.ts";

const scope = self as unknown as {
	onmessage: ((event: MessageEvent) => void) | null;
	postMessage(message: unknown, transfer?: Transferable[]): void;
};
/**
 * How long the file list may take. Most addresses do not answer and each
 * costs a slot for 20 s, so finding the peer that serves the metadata can
 * take a while on a thin swarm; the session also opens more sockets meanwhile.
 */
const METADATA_TIMEOUT_MS = 60_000;
/** 10 Mbit/s: about twice a typical 1080p stream. */
const PROVISIONAL_RATE_THRESHOLD = 1_250_000;
let manager: ConnectionManager | undefined;
let lookup: AbortController | undefined;
/** Wants still running, by id, so the page can cancel them. */
const wants = new Map<number, AbortController>();
/** Billing counters also go to the page on a timer: a stalled session has no want-done to carry them. */
const STATS_EVERY_MS = 5_000;
let statsTimer: ReturnType<typeof setInterval> | undefined;

scope.onmessage = (event: MessageEvent) => {
	const msg = parsePageMessage(event.data);
	if (!msg) return;
	if (msg.type === "stop") {
		stop();
		return;
	}
	if (msg.type === "start") {
		void start(msg.magnet, msg.gatewayUrl, msg.seedersUrl);
		return;
	}
	if (msg.type === "cancel") {
		wants.get(msg.id)?.abort();
		return;
	}
	if (msg.type === "idle") {
		manager?.suspend();
		postStats();
		return;
	}
	void want(msg.id, msg.blocks);
};

function post(message: WorkerMessage, transfer?: Transferable[]): void {
	scope.postMessage(message, transfer ?? []);
}

function fail(message: string): void {
	post({ type: "error", message: message.slice(0, 200) });
}

async function start(magnetUri: string, gatewayUrl: string, seedersUrl: string): Promise<void> {
	stop();
	let magnet;
	try {
		magnet = parseMagnet(magnetUri);
	} catch (e) {
		fail(e instanceof Error ? e.message : "bad magnet");
		return;
	}
	lookup = new AbortController();
	const signal = lookup.signal;
	const fetcher = new MetadataFetcher({ expected: { v1: magnet.v1, v2: magnet.v2, wire: magnet.infoHashHex } });
	const session = new ConnectionManager({
		gatewayUrl,
		infoHash: magnet.infoHash,
		allowPrivate: loopback(gatewayUrl),
		// Another WebSocket (6 more slots) after 30 s under this rate. A stand-in
		// for twice the video's bitrate until the player reports it (M3-06).
		rateThreshold: PROVISIONAL_RATE_THRESHOLD,
		onExtensions: (conn) => fetcher.offer(conn),
		onMetadata: (conn, message) => fetcher.onMessage(conn, message),
		onPeerClose: (conn) => fetcher.onClose(conn),
	});
	manager = session;
	statsTimer ??= setInterval(postStats, STATS_EVERY_MS);
	session.addPeers(magnet.peers, "magnet");
	const peersUrl = new URL("/peers", seedersUrl);
	peersUrl.searchParams.set("ih", magnet.infoHashHex);
	if (magnet.trackers.length > 0) peersUrl.searchParams.set("magnet", magnetUri);
	let count = 0;
	void readPeerStream(
		peersUrl.toString(),
		(address) => {
			count++;
			session.addPeers([address], "discovery");
			post({ type: "peers", count });
		},
		{ signal },
	).catch(() => 0);
	try {
		await session.connect();
		const result = await Promise.race([fetcher.done, timeout(METADATA_TIMEOUT_MS, signal)]);
		if (signal.aborted || manager !== session) return;
		session.setInfo(result.meta);
		const info = result.bytes.slice().buffer;
		post({ type: "files", listing: listing(result.meta), info }, [info]);
	} catch (e) {
		if (!signal.aborted) fail(e instanceof Error ? e.message : "metadata failed");
	}
}

async function want(id: number, blocks: { piece: number; begin: number; length: number }[]): Promise<void> {
	const session = manager;
	if (!session) {
		post({ type: "want-done", id, received: 0, missing: blocks.length });
		return;
	}
	if (wants.has(id)) return;
	const cancel = new AbortController();
	wants.set(id, cancel);
	try {
		const end = await session.requestBlocks(blocks, {
			signal: cancel.signal,
			timeoutMs: 20_000,
			onBlock: (piece, begin, data) => {
				const copy = data.slice();
				post({ type: "block", id, piece, begin, data: copy.buffer }, [copy.buffer]);
			},
		});
		post({ type: "want-done", id, received: end.received, missing: end.missing });
		postStats();
	} catch {
		post({ type: "want-done", id, received: 0, missing: blocks.length });
	} finally {
		wants.delete(id);
	}
}

/** Counters for the page's diagnostics (requests billed per GB). */
function postStats(): void {
	if (!manager) return;
	const { known, delivered } = manager.book.counts();
	post({ type: "stats", upgrades: manager.upgrades, messages: manager.messagesSent, known, delivered });
}

function stop(): void {
	postStats();
	if (statsTimer !== undefined) clearInterval(statsTimer);
	statsTimer = undefined;
	for (const cancel of wants.values()) cancel.abort();
	wants.clear();
	lookup?.abort();
	lookup = undefined;
	manager?.stop();
	manager = undefined;
}

function loopback(url: string): boolean {
	try {
		const host = new URL(url).hostname;
		return host === "localhost" || host === "127.0.0.1" || host === "::1";
	} catch {
		return false;
	}
}

function timeout(ms: number, signal: AbortSignal): Promise<never> {
	return new Promise((_, reject) => {
		const timer = setTimeout(() => reject(new Error("metadata timed out")), ms);
		signal.addEventListener("abort", () => clearTimeout(timer), { once: true });
	});
}
