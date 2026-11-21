// The torrent session, in a dedicated worker: one /peers stream, one to
// three tunnel WebSockets, the metadata from whoever answers first (session
// memory, the stream's `info` line, BEP 9 from peers: always checked
// against the info-hash), and the recoveries the page
// starts (a video, a subtitle file): the engine's deadline picker chooses
// the blocks, verifies the pieces, stores them in OPFS (opfs-store.ts,
// synchronous access handles: only a worker has them) and hands them over.
// Discovery is one streamed /peers request.
//
// Session memory (memory-db.ts, M3-04): the verified info dict is kept as
// soon as it is known, and notes on the peers every few seconds. The next
// session of the torrent (a reload, a second paste) lists the files from
// the kept dict (checked against the info-hash again), dials the peers
// that delivered first, and, with both, asks the VPS for peers only if
// those don't deliver.
//
// The file-list wait follows progress (lookup-wait.ts): it gives up only
// when nothing is left to try, or at a hard cap. A failed lookup keeps the
// session (sockets, peer book, fetcher): `retry` restarts the clock and
// asks the VPS again, and addresses that failed keep their back-off.
//
// Reciprocation (M3-12, OQ-04): with the "Share with peers" setting on
// (the default), the engine uploads verified pieces to the peers that
// trade with it, reading them from the OPFS store here, in this worker,
// and only while a recovery runs. The verified info dict is served to
// peers that ask for it. Off: nothing is announced or uploaded.

import {
	ConnectionManager,
	MetadataFetcher,
	listing,
	parseMagnet,
	pieceSize,
	readPeerBody,
	type Metainfo,
	type MetadataResult,
	type PeerBook,
	type PieceSource,
	type Recovery,
} from "@magnet-stream/engine";
import { MetadataRace } from "./metadata-race.ts";
import { LookupWait, failureMessage, type LookupFacts, type LookupFailure, type LookupProgress } from "./lookup-wait.ts";
import { sessionMemory } from "./memory-db.ts";
import { openOpfsStore } from "./opfs-store.ts";
import { toStoreError, type PieceFile, type StoreErrorCode } from "./piece-file.ts";
import { DIAL_CLASSES, parsePageMessage, type MetadataOrigin, type ScoutStreamStats, type SessionDiag, type WorkerMessage } from "./session-protocol.ts";

const scope = self as unknown as {
	onmessage: ((event: MessageEvent) => void) | null;
	postMessage(message: unknown, transfer?: Transferable[]): void;
};
/** The lookup's facts are sampled (and its progress sent to the page) this often. */
const LOOKUP_TICK_MS = 500;
/** Failed addresses whose back-off ends within this long still count as candidates. */
const SOON_MS = 15_000;
/**
 * Another WebSocket after 30 s under this rate: 10 Mbit/s, about twice a
 * typical 1080p stream, until the player reports the video's bitrate
 * (then twice that, see ConnectionManager.setBitrate).
 */
const PROVISIONAL_RATE_THRESHOLD = 1_250_000;
let manager: ConnectionManager | undefined;
let lookup: AbortController | undefined;

/** The session's file-list lookup. */
interface Lookup {
	session: ConnectionManager;
	fetcher: MetadataFetcher;
	race: MetadataRace;
	wait: LookupWait;
	scout: ScoutStreamStats;
	/** Ask the VPS (again): a no-op while a stream is open. */
	discover(): void;
	/** The first WebSocket could not be opened (nothing to dial through). */
	tunnelDown: boolean;
	lastConnectAt: number;
	connectsSeen: number;
	startedAt: number;
	timer: ReturnType<typeof setInterval> | undefined;
	/** The verified listing, once known (re-sent on retry). */
	files: { listing: ReturnType<typeof listing>; bytes: Uint8Array; source: MetadataOrigin } | undefined;
}
let current: Lookup | undefined;
/** Recoveries running, by the page's id. */
const recoveries = new Map<number, Recovery>();
/** Billing counters also go to the page on a timer, whatever the recoveries do. */
const STATS_EVERY_MS = 5_000;
let statsTimer: ReturnType<typeof setInterval> | undefined;
/** Progress of each recovery (pieces being assembled) for the page's piece strip. */
const PROGRESS_EVERY_MS = 500;
let progressTimer: ReturnType<typeof setInterval> | undefined;
/** The verified info dict of this session's torrent. */
let meta: Metainfo | undefined;
/**
 * The torrent's OPFS store. Only one handle per file can be open, and a
 * delete from the saved list needs them: closed this long after the last
 * saving recovery ended or the last read.
 */
const STORE_IDLE_MS = 5_000;
let store: PieceFile | undefined;
let storeOpening: Promise<PieceFile | undefined> | undefined;
let storeTimer: ReturnType<typeof setTimeout> | undefined;
/** Recoveries that store their pieces. */
const saving = new Set<number>();
/** Session memory (IndexedDB, M3-04): reads give up after this long rather than hold the lookup. */
const MEMORY_WAIT_MS = 1_500;
/** Remembered peers that delivered get this long to deliver again before discovery starts. */
const DISCOVERY_DEFER_MS = 10_000;
/** Peer notes are written at most this often (when they changed). */
const NOTES_EVERY_MS = 5_000;
let notesTimer: ReturnType<typeof setInterval> | undefined;
let deferTimer: ReturnType<typeof setInterval> | undefined;
/** The torrent whose notes are kept (its info dict verified), and the notes last written. */
let memoryHash: string | undefined;
let savedNotes = "";
/** The page said nothing is wanted (sockets closed on purpose). */
let idle = false;
/** The "Share with peers" setting. */
let share = true;

/**
 * What the engine may upload: verified pieces of the open store only (the
 * store checks again: a block of at most 16 KiB, inside a verified piece).
 */
const pieceSource: PieceSource = {
	has: (piece) => store !== undefined && !store.isClosed && store.has(piece),
	pieces: () => (store && !store.isClosed ? store.pieces() : []),
	read: (piece, begin, length) => {
		if (!store || store.isClosed) throw new Error("the store is closed");
		return store.readBlock(piece, begin, length);
	},
};

scope.onmessage = (event: MessageEvent) => {
	const msg = parsePageMessage(event.data);
	if (!msg) return;
	if (msg.type === "stop") {
		stop();
		return;
	}
	if (msg.type === "start") {
		share = msg.upload ?? true;
		void start(msg.magnet, msg.gatewayUrl, msg.seedersUrl, msg.capMs);
		return;
	}
	if (msg.type === "upload") {
		share = msg.enabled;
		manager?.setUpload(share);
		postStats();
		return;
	}
	if (msg.type === "retry") {
		retry();
		return;
	}
	if (msg.type === "idle") {
		idle = true;
		manager?.suspend();
		postStats();
		void saveNotes();
		return;
	}
	if (msg.type === "recover") {
		idle = false;
		void recover(msg.id, msg.offset, msg.size, msg.position, msg.have, msg.save);
		return;
	}
	if (msg.type === "read") {
		void readPiece(msg.req, msg.piece);
		return;
	}
	if (msg.type === "bitrate") {
		manager?.setBitrate(msg.bytesPerSecond);
		return;
	}
	if (msg.type === "release") {
		if (saving.size === 0) closeStore();
		post({ type: "released", req: msg.req });
		return;
	}
	const recovery = recoveries.get(msg.id);
	if (!recovery) return;
	if (msg.type === "seek") recovery.seek(msg.byte);
	else if (msg.type === "position") recovery.setPosition(msg.byte);
	else if (msg.type === "lost") recovery.forget(msg.piece);
	else if (msg.type === "end") {
		recoveries.delete(msg.id);
		unsave(msg.id);
		recovery.stop();
	}
};

function post(message: WorkerMessage, transfer?: Transferable[]): void {
	scope.postMessage(message, transfer ?? []);
}

function fail(message: string): void {
	post({ type: "error", message: message.slice(0, 200) });
}

async function start(magnetUri: string, gatewayUrl: string, seedersUrl: string, capMs: number | undefined): Promise<void> {
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
	const infoHash = magnet.infoHashHex;
	const startedAt = Date.now();
	// What earlier sessions kept (M3-04): the info dict and notes on the peers.
	const memory = await within(sessionMemory(), MEMORY_WAIT_MS, null);
	const [cached, notes] = memory
		? await Promise.all([
				within(memory.metadata(infoHash).catch(() => null), MEMORY_WAIT_MS, null),
				within(memory.peerNotes(infoHash).catch(() => [] as unknown[]), MEMORY_WAIT_MS, [] as unknown[]),
			])
		: [null, [] as unknown[]];
	if (signal.aborted) return;
	const fetcher = new MetadataFetcher({ expected: { v1: magnet.v1, v2: magnet.v2, wire: magnet.infoHashHex } });
	const session = new ConnectionManager({
		gatewayUrl,
		infoHash: magnet.infoHash,
		allowPrivate: loopback(gatewayUrl),
		// Another WebSocket (6 more slots) after 30 s under this rate, until
		// the player reports the video's bitrate (the `bitrate` message).
		rateThreshold: PROVISIONAL_RATE_THRESHOLD,
		onExtensions: (conn) => fetcher.offer(conn),
		onMetadata: (conn, message) => fetcher.onMessage(conn, message),
		onPeerClose: (conn) => fetcher.onClose(conn),
		upload: { source: pieceSource, enabled: share },
	});
	manager = session;
	statsTimer ??= setInterval(postStats, STATS_EVERY_MS);
	// Remembered peers first: those that delivered are dialled before anything else.
	session.book.restore(notes);
	session.addPeers(magnet.peers, "magnet");
	const peersUrl = new URL("/peers", seedersUrl);
	peersUrl.searchParams.set("ih", magnet.infoHashHex);
	if (magnet.trackers.length > 0) peersUrl.searchParams.set("magnet", magnetUri);
	const l: Lookup = {
		session,
		fetcher,
		race: new MetadataRace(fetcher),
		wait: new LookupWait(startedAt, capMs),
		scout: { state: "idle", streams: 0, peers: 0, alive: 0, seeds: 0, dead: 0, unknown: 0, rejected: 0, info: "none", firstPeerMs: null },
		discover: () => {},
		tunnelDown: false,
		lastConnectAt: 0,
		connectsSeen: 0,
		startedAt,
		timer: undefined,
		files: undefined,
	};
	current = l;
	// The kept info dict is checked against the info-hash like any other.
	const fromCache = cached ? await l.race.offer(cached, "cache") : false;
	if (signal.aborted || manager !== session) return;
	let count = 0;
	l.discover = () => {
		if (l.scout.state === "open" || signal.aborted || manager !== session) return;
		clearDeferral();
		const scout = l.scout;
		scout.state = "open";
		scout.streams++;
		void readStream(peersUrl.toString(), signal, {
			onPeer: (address, facts) => {
				count++;
				scout.peers++;
				scout.firstPeerMs ??= Date.now() - startedAt;
				if (facts?.alive === true) {
					scout.alive++;
					if (facts.seed === true) scout.seeds++;
				} else if (facts?.alive === false) scout.dead++;
				else scout.unknown++;
				// The scout's facts order the dials (OQ-21, OQ-23); the handshake gate still decides.
				session.addPeers([address], "discovery", [facts]);
				post({ type: "peers", count });
			},
			onInfo: (bytes) => {
				// The VPS fetched the info dict (step 3): unverified until it hashes right.
				if (fetcher.isDone) {
					scout.info = "late";
					return;
				}
				void l.race.offer(bytes, "vps").then((ok) => {
					if (scout.info === "none") scout.info = ok ? "accepted" : fetcher.isDone ? "late" : "rejected";
					postStats();
				});
			},
		}).then(
			(result) => {
				scout.rejected += result.rejected;
				if (manager === session) scout.state = "done";
			},
			() => {
				if (manager === session && !signal.aborted) scout.state = "failed";
			},
		);
	};
	// With the metadata and peers that delivered before, the session resumes
	// without discovery; it asks the VPS only if they don't deliver.
	const remembered = [...session.book.entries()].some((p) => p.remembered > 0 && !p.banned);
	if (fromCache && remembered) deferDiscovery(session, l.discover);
	else l.discover();
	session.connect().catch(() => {
		if (manager === session && session.socketsOpen === 0) l.tunnelDown = true;
	});
	void l.race.done.then(
		({ result, source }) => found(l, result, source, memory),
		() => {},
	);
	if (!fromCache) watch(l);
}

/** The metadata is verified: list the files, keep it (M3-04). */
function found(l: Lookup, result: MetadataResult, source: MetadataOrigin, memory: Awaited<ReturnType<typeof sessionMemory>>): void {
	const session = l.session;
	if (manager !== session || current !== l) return;
	stopWatching(l);
	if (source === "vps") l.scout.info = "accepted";
	session.setInfo(result.meta, source, result.bytes);
	meta = result.meta;
	memoryHash = result.meta.infoHash;
	if (source !== "cache") void memory?.rememberMetadata(result.meta.infoHash, result.bytes).catch(() => {});
	l.files = { listing: listing(result.meta), bytes: result.bytes, source };
	postFiles(l);
	postStats();
	notesTimer ??= setInterval(() => void saveNotes(), NOTES_EVERY_MS);
	void saveNotes();
}

function postFiles(l: Lookup): void {
	if (!l.files) return;
	const info = l.files.bytes.slice().buffer;
	post({ type: "files", listing: l.files.listing, info, source: l.files.source }, [info]);
}

/** Sample the lookup's facts until the metadata arrives or nothing is left to try. */
function watch(l: Lookup): void {
	stopWatching(l);
	l.timer = setInterval(() => judge(l), LOOKUP_TICK_MS);
}

function stopWatching(l: Lookup): void {
	if (l.timer !== undefined) clearInterval(l.timer);
	l.timer = undefined;
}

function judge(l: Lookup): void {
	if (current !== l || manager !== l.session || l.files) return stopWatching(l);
	const now = Date.now();
	const { facts, progress } = sample(l, now);
	const verdict = l.wait.judge(facts, now);
	if (verdict.state === "waiting") {
		post({ type: "lookup", progress });
		return;
	}
	stopWatching(l);
	const reason: LookupFailure = verdict.reason === "exhausted" && l.tunnelDown ? "tunnel" : verdict.reason;
	post({ type: "lookup-failed", reason, message: failureMessage(reason, progress), progress });
	postStats();
}

/** What the lookup can still hope for, and what the page shows (no addresses). */
function sample(l: Lookup, now: number): { facts: LookupFacts; progress: LookupProgress } {
	const { session, scout } = l;
	const counts = hope(session.book, now);
	const diag = session.diagnostics();
	const connects = DIAL_CLASSES.reduce((n, k) => n + diag.dials[k].succeeded, 0);
	if (connects > l.connectsSeen) {
		l.connectsSeen = connects;
		l.lastConnectAt = now;
	}
	const tunnel = !l.tunnelDown || session.socketsOpen > 0;
	if (session.socketsOpen > 0) l.tunnelDown = false;
	const facts: LookupFacts = {
		discoveryDone: scout.state === "done" || scout.state === "failed",
		candidates: tunnel ? counts.candidates : 0,
		dialling: counts.dialling,
		fetching: l.fetcher.pending,
		lastConnectAt: l.lastConnectAt,
		checking: l.race.checking,
	};
	const progress: LookupProgress = {
		found: session.book.size,
		tried: counts.tried,
		connected: session.liveCount,
		elapsedMs: l.wait.elapsed(now),
		discovering: scout.state === "open",
	};
	return { facts, progress };
}

/** Live or unknown addresses still to dial (now or soon), dials in flight, addresses tried. */
function hope(book: PeerBook, now: number): { candidates: number; dialling: number; tried: number } {
	let dialling = 0;
	let tried = 0;
	let soon = 0;
	for (const p of book.entries()) {
		if (p.state !== "new") tried++;
		if (p.state === "connecting") dialling++;
		else if (p.state === "failed" && !p.banned && p.retryAt > now && p.retryAt <= now + SOON_MS) soon++;
	}
	return { candidates: book.candidates(Infinity).length + soon, dialling, tried };
}

/**
 * Look again after a failed lookup, in the same session: the open
 * WebSockets and the peer book stay (addresses that failed keep their
 * back-off), the VPS is asked again, and the cap counts from now.
 */
function retry(): void {
	const l = current;
	if (!l || manager !== l.session) return;
	if (l.files) {
		postFiles(l);
		return;
	}
	l.wait.restart(Date.now());
	l.discover();
	if (l.session.socketsOpen === 0) {
		l.tunnelDown = false;
		l.session.connect().catch(() => {
			if (manager === l.session && l.session.socketsOpen === 0) l.tunnelDown = true;
		});
	}
	watch(l);
	judge(l);
}

/** One `/peers` stream, with its counts (a single request: no retry here). */
async function readStream(
	url: string,
	signal: AbortSignal,
	handlers: Parameters<typeof readPeerBody>[1],
): Promise<Awaited<ReturnType<typeof readPeerBody>>> {
	const res = await fetch(url, { signal, headers: { accept: "application/x-ndjson" } });
	if (!res.ok || !res.body) throw new Error(`peers request failed (${res.status})`);
	return readPeerBody(res.body, handlers);
}

/**
 * Discovery waits while remembered peers serve: it starts once nothing is
 * left to dial, or when what is wanted has gone DISCOVERY_DEFER_MS without
 * a peer delivering.
 */
function deferDiscovery(session: ConnectionManager, discover: () => void): void {
	let served = Date.now();
	clearDeferral();
	deferTimer = setInterval(() => {
		if (manager !== session) return clearDeferral();
		const now = Date.now();
		// Idle (nothing wanted, sockets closed on purpose): nothing to find.
		if (idle && recoveries.size === 0) {
			served = now;
			return;
		}
		// Delivering, or connected with nothing asked of the peers yet.
		if (session.diagnostics().delivering > 0 || (recoveries.size === 0 && session.liveCount > 0)) served = now;
		let dialling = false;
		for (const p of session.book.entries()) if (p.state === "connecting") dialling = true;
		const exhausted = session.liveCount === 0 && !dialling && session.book.candidates(1).length === 0;
		if (exhausted || now - served >= DISCOVERY_DEFER_MS) discover();
	}, 1_000);
}

function clearDeferral(): void {
	if (deferTimer !== undefined) clearInterval(deferTimer);
	deferTimer = undefined;
}

/** Keep the session's peer notes for the next one (only when they changed). */
async function saveNotes(): Promise<void> {
	const session = manager;
	const infoHash = memoryHash;
	if (!session || !infoHash || !meta) return;
	const notes = session.book.notes();
	const text = JSON.stringify(notes);
	if (text === savedNotes) return;
	const memory = await sessionMemory();
	if (!memory) return;
	try {
		if (await memory.rememberPeers(infoHash, notes)) savedNotes = text;
	} catch {
		// Storage refused: the session goes on, nothing is remembered.
	}
}

async function recover(id: number, offset: number, size: number, position: number, have: number[], save: boolean): Promise<void> {
	const session = manager;
	if (!session || recoveries.has(id) || saving.has(id)) return;
	const known = new Set(have);
	if (save) {
		saving.add(id);
		const opened = await openStore();
		if (manager !== session || !saving.has(id)) return;
		if (opened.store) for (const piece of opened.store.pieces()) known.add(piece);
		else post({ type: "store-error", id, code: opened.code });
	}
	const reported = new Set<StoreErrorCode>();
	let recovery: Recovery;
	try {
		recovery = session.recover(
			{ offset, size },
			{
				position,
				have: [...known],
				onPiece: (piece, data) => {
					if (recoveries.get(id) !== recovery) return;
					let stored = false;
					if (save && store && !store.isClosed) {
						try {
							store.write(piece, data);
							stored = true;
						} catch (e) {
							// The piece stays missing in the store; the page keeps it in memory.
							const code = toStoreError(e).code;
							if (!reported.has(code)) post({ type: "store-error", id, code });
							reported.add(code);
						}
					}
					const copy = data.slice();
					post({ type: "piece", id, piece, data: copy.buffer, stored }, [copy.buffer]);
				},
			},
		);
	} catch (e) {
		unsave(id);
		fail(e instanceof Error ? e.message : "recovery failed");
		return;
	}
	recoveries.set(id, recovery);
	progressTimer ??= setInterval(postProgress, PROGRESS_EVERY_MS);
	void recovery.done.then((complete) => {
		if (recoveries.get(id) !== recovery) return;
		recoveries.delete(id);
		unsave(id);
		postProgress();
		if (complete) post({ type: "recovered", id });
		postStats();
		void saveNotes();
	});
}

/** Open the torrent's store (once metadata is known), or say why it can't be. */
async function openStore(): Promise<{ store: PieceFile; code?: undefined } | { store?: undefined; code: StoreErrorCode }> {
	armStoreTimer();
	if (store && !store.isClosed) return { store };
	const m = meta;
	if (!m) return { code: "unavailable" };
	if (typeof navigator === "undefined" || typeof navigator.storage?.getDirectory !== "function") return { code: "unavailable" };
	let code: StoreErrorCode = "io";
	storeOpening ??= openOpfsStore(m.infoHash, { pieceLength: m.pieceLength, pieceCount: m.pieceCount, pieceSize: (p) => pieceSize(m, p) })
		.then(
			(opened) => {
				if (meta !== m) {
					opened.close();
					return undefined;
				}
				store = opened;
				return opened;
			},
			(e: unknown) => {
				code = toStoreError(e).code;
				return undefined;
			},
		)
		.finally(() => (storeOpening = undefined));
	const opened = await storeOpening;
	armStoreTimer();
	return opened ? { store: opened } : { code };
}

/** Close the store once nothing saves for a while (see STORE_IDLE_MS). */
function armStoreTimer(): void {
	if (storeTimer !== undefined) clearTimeout(storeTimer);
	storeTimer = setTimeout(() => {
		storeTimer = undefined;
		if (saving.size === 0) closeStore();
	}, STORE_IDLE_MS);
}

function unsave(id: number): void {
	if (saving.delete(id) && saving.size === 0) armStoreTimer();
}

function closeStore(): void {
	if (storeTimer !== undefined) clearTimeout(storeTimer);
	storeTimer = undefined;
	try {
		store?.close();
	} catch {
		// Already gone.
	}
	store = undefined;
}

/** A stored piece for the page's player. */
async function readPiece(req: number, piece: number): Promise<void> {
	const opened = await openStore();
	let data: ArrayBuffer | null = null;
	try {
		if (opened.store?.has(piece)) data = opened.store.readPiece(piece).buffer as ArrayBuffer;
	} catch {
		data = null;
	}
	post({ type: "read-result", req, piece, data }, data ? [data] : []);
}

function postProgress(): void {
	const session = manager;
	if (!session) return;
	for (const [id, recovery] of recoveries) {
		const assembling = recovery.assembling().slice(0, 4096).flat();
		post({
			type: "progress",
			id,
			assembling,
			bytes: session.downloadedBytes,
			live: session.liveCount,
			hashFailures: session.hashFailures,
			banned: session.bannedPeers,
			hedges: session.hedges,
			cancels: session.cancels,
			windowBytes: recovery.windowBytes(),
			rateThreshold: Math.round(session.rateThreshold ?? 0),
		});
	}
	if (recoveries.size === 0 && progressTimer !== undefined) {
		clearInterval(progressTimer);
		progressTimer = undefined;
	}
}

/** Counters for the page's diagnostics (requests billed per GB). */
function postStats(): void {
	if (!manager) return;
	const { known, delivered } = manager.book.counts();
	post({ type: "stats", upgrades: manager.upgrades, messages: manager.messagesSent, known, delivered });
	const l = current;
	if (l && l.session === manager) post({ type: "diag", diag: diagOf(l) });
}

/** The session's counters for the diagnostics report (no address, no info-hash). */
function diagOf(l: Lookup): SessionDiag {
	const d = l.session.diagnostics();
	const source = d.metadata?.source;
	const reasons = (failed: Record<string, number>) => {
		const out: Record<string, number> = {};
		for (const [k, v] of Object.entries(failed).slice(0, 16)) if (/^[a-z_]{1,32}$/.test(k)) out[k] = v;
		return out;
	};
	const dials = {} as SessionDiag["dials"];
	for (const kind of DIAL_CLASSES) dials[kind] = { attempted: d.dials[kind].attempted, succeeded: d.dials[kind].succeeded, failed: reasons(d.dials[kind].failed) };
	return {
		dials,
		connected: d.connected,
		unchoked: d.unchoked,
		delivering: d.delivering,
		seeds: d.seeds,
		firstUnchokeMs: d.firstUnchokeMs === null ? null : Math.max(0, Math.round(d.firstUnchokeMs)),
		metadata: d.metadata && (source === "cache" || source === "vps" || source === "peer") ? { source, ms: Math.max(0, Math.round(d.metadata.ms)) } : null,
		sockets: d.sockets,
		upgrades: d.upgrades,
		messages: d.messagesSent,
		recycles: d.recycles,
		untestedHighWater: d.untestedHighWater,
		scout: { ...l.scout },
		upload: { enabled: d.upload.enabled, bytes: d.upload.bytes, unchoked: d.upload.unchoked, messages: d.upload.messages },
	};
}

function stop(): void {
	postStats();
	void saveNotes();
	clearDeferral();
	if (notesTimer !== undefined) clearInterval(notesTimer);
	notesTimer = undefined;
	memoryHash = undefined;
	savedNotes = "";
	idle = false;
	if (statsTimer !== undefined) clearInterval(statsTimer);
	statsTimer = undefined;
	for (const recovery of recoveries.values()) recovery.stop();
	recoveries.clear();
	saving.clear();
	closeStore();
	meta = undefined;
	if (progressTimer !== undefined) clearInterval(progressTimer);
	progressTimer = undefined;
	lookup?.abort();
	lookup = undefined;
	if (current) stopWatching(current);
	current = undefined;
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

/** `promise`, or `fallback` if it takes longer than `ms`. */
function within<T>(promise: Promise<T>, ms: number, fallback: T): Promise<T> {
	return new Promise((resolve) => {
		const timer = setTimeout(() => resolve(fallback), ms);
		promise.then(
			(value) => {
				clearTimeout(timer);
				resolve(value);
			},
			() => {
				clearTimeout(timer);
				resolve(fallback);
			},
		);
	});
}
