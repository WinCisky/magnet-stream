// Messages between the page and the session worker. Both sides validate
// before trusting anything: a bad message is ignored, never interpreted.

import type { StoreErrorCode } from "./piece-file.ts";
import { MAX_LOOKUP_CAP_MS, MIN_LOOKUP_CAP_MS, type LookupFailure, type LookupProgress } from "./lookup-wait.ts";

const STORE_ERRORS: readonly StoreErrorCode[] = ["quota", "unverified", "range", "io", "unavailable", "busy"];

/** Largest piece a recovery accepts (the page keys blocks in 12 bits: 4096 × 16 KiB). */
export const MAX_PIECE = 64 * 1024 * 1024;
/** Stored pieces named in one `recover`. */
export const MAX_HAVE = 1_000_000;
/** Pieces being assembled reported in one `progress`. */
export const MAX_ASSEMBLING = 4096;
const MAX_BYTE = Number.MAX_SAFE_INTEGER;
/** Video bitrates accepted, bytes per second: 1 kB/s to 1 GB/s. */
export const MIN_BITRATE = 1_000;
export const MAX_BITRATE = 1_000_000_000;

export interface SessionFile {
	path: string;
	size: number;
	offset: number;
}

export interface SessionListing {
	infoHash: string;
	name: string;
	size: number;
	pieceLength: number;
	files: SessionFile[];
}

/** Where the verified info dict came from: session memory, the `/peers` stream's `info` line, or a peer (BEP 9). */
export type MetadataOrigin = "cache" | "vps" | "peer";
const METADATA_ORIGINS: readonly MetadataOrigin[] = ["cache", "vps", "peer"];

/** Dials of one class (ConnectionManager.diagnostics), failures by reason name. */
export interface DialStats {
	attempted: number;
	succeeded: number;
	failed: Record<string, number>;
}

export const DIAL_CLASSES = ["scout", "untried", "known"] as const;
export type DialClassName = (typeof DIAL_CLASSES)[number];

/** The `/peers` stream as the session read it (no addresses). */
export interface ScoutStreamStats {
	/** "idle": not asked (yet); "open": reading; "done": ended; "failed": the request failed. */
	state: "idle" | "open" | "done" | "failed";
	/** Streams read this session (a retry asks again). */
	streams: number;
	peers: number;
	/** What the scout said: alive (of which seeds), dead, not probed in time. */
	alive: number;
	seeds: number;
	dead: number;
	unknown: number;
	/** Lines dropped as malformed or out of range. */
	rejected: number;
	/** The `info` line: none yet, accepted (it ended the search), rejected (did not hash right), or late (the metadata was known already). */
	info: "none" | "accepted" | "rejected" | "late";
	/** Ms from the first request to its first peer line (null: none yet). */
	firstPeerMs: number | null;
}

/** What the session uploaded (M3-12): counts only. */
export interface UploadDiag {
	/** The "Share with peers" setting is on (and the engine can read the store). */
	enabled: boolean;
	/** Piece bytes sent to peers. */
	bytes: number;
	/** Peers we unchoke now. */
	unchoked: number;
	/** Upstream messages that carried only upload traffic. */
	messages: number;
}

/** Session counters for the page's diagnostics (anonymous: counts and reason names only). */
export interface SessionDiag {
	dials: Record<DialClassName, DialStats>;
	connected: number;
	unchoked: number;
	delivering: number;
	seeds: number;
	firstUnchokeMs: number | null;
	metadata: { source: MetadataOrigin; ms: number } | null;
	sockets: number;
	upgrades: number;
	messages: number;
	recycles: number;
	untestedHighWater: number;
	scout: ScoutStreamStats;
	upload: UploadDiag;
}

export type PageMessage =
	/**
	 * `capMs`: the lookup's hard cap (lookup-wait.ts; default 120 s).
	 * `upload`: the "Share with peers" setting (default on).
	 */
	| { type: "start"; magnet: string; gatewayUrl: string; seedersUrl: string; capMs?: number; upload?: boolean }
	/** The "Share with peers" setting changed. */
	| { type: "upload"; enabled: boolean }
	/** Look for the file list again after a failed lookup, in the same session. */
	| { type: "retry" }
	/**
	 * Recover torrent bytes [offset, offset + size) with the engine's
	 * deadline picker, from `position` (bytes into the range). `have`: pieces
	 * already stored. `save`: the worker stores verified pieces in OPFS.
	 */
	| { type: "recover"; id: number; offset: number; size: number; position: number; have: number[]; save: boolean }
	/** Bytes of a stored piece (answered by `read-result`). */
	| { type: "read"; req: number; piece: number }
	/** Close the store's files if nothing is being saved (a delete needs them), then answer `released`. */
	| { type: "release"; req: number }
	/** The viewer jumped to `byte` (into the range): requests outside the new window are cancelled. */
	| { type: "seek"; id: number; byte: number }
	/** Playback reached `byte`: the window follows (nothing is cancelled). */
	| { type: "position"; id: number; byte: number }
	/** A verified piece was lost from storage: fetch it again. */
	| { type: "lost"; id: number; piece: number }
	/**
	 * The video's average bitrate (bytes per second), once the player parsed
	 * the container: it sizes the picker's window and the socket threshold.
	 */
	| { type: "bitrate"; bytesPerSecond: number }
	/** Stop a recovery. */
	| { type: "end"; id: number }
	/** Nothing is being recovered: close the WebSockets, keep metadata and peers for a later want. */
	| { type: "idle" }
	| { type: "stop" };

export type WorkerMessage =
	| { type: "files"; listing: SessionListing; info: ArrayBuffer; source: MetadataOrigin }
	/** The lookup is still waiting: what it has tried so far. */
	| { type: "lookup"; progress: LookupProgress }
	/** The lookup gave up (the session stays: `retry` carries on). */
	| { type: "lookup-failed"; reason: LookupFailure; message: string; progress: LookupProgress }
	| { type: "diag"; diag: SessionDiag }
	| { type: "peers"; count: number }
	/** A verified piece of recovery `id`; `stored`: it is in OPFS too. */
	| { type: "piece"; id: number; piece: number; data: ArrayBuffer; stored: boolean }
	/** A stored piece's bytes, or null if it isn't stored. */
	| { type: "read-result"; req: number; piece: number; data: ArrayBuffer | null }
	| { type: "released"; req: number }
	/** Recovery `id` couldn't store a piece: "quota", "unavailable", "busy" or "io". */
	| { type: "store-error"; id: number; code: StoreErrorCode }
	/** Every piece of recovery `id` is verified. */
	| { type: "recovered"; id: number }
	/**
	 * Recovery `id` so far: pieces being assembled as flat
	 * [piece, blocks received, blocks] triples, session bytes received,
	 * peers connected, pieces that failed verification, duplicate requests
	 * and cancels sent, the picker's playback window in bytes and the rate
	 * under which another socket opens (0: none).
	 */
	| {
			type: "progress";
			id: number;
			assembling: number[];
			bytes: number;
			live: number;
			hashFailures: number;
			banned: number;
			hedges: number;
			cancels: number;
			windowBytes: number;
			rateThreshold: number;
	  }
	/**
	 * The tunnel so far: upgrades attempted and WebSocket messages sent
	 * (billing), peers known and peers that delivered data.
	 */
	| { type: "stats"; upgrades: number; messages: number; known: number; delivered: number }
	| { type: "error"; message: string };

function record(value: unknown): Record<string, unknown> | null {
	return value !== null && typeof value === "object" ? (value as Record<string, unknown>) : null;
}

/** A non-negative safe integer. */
function count(value: unknown): boolean {
	return Number.isInteger(value) && (value as number) >= 0 && (value as number) <= MAX_BYTE;
}

function httpUrl(value: unknown): string | null {
	if (typeof value !== "string" || value.length > 2048) return null;
	try {
		const url = new URL(value);
		if (url.protocol !== "http:" && url.protocol !== "https:" && url.protocol !== "ws:" && url.protocol !== "wss:") return null;
		return value;
	} catch {
		return null;
	}
}

export function parsePageMessage(value: unknown): PageMessage | null {
	const msg = record(value);
	if (!msg) return null;
	if (msg.type === "stop") return { type: "stop" };
	if (msg.type === "idle") return { type: "idle" };
	if (msg.type === "retry") return { type: "retry" };
	if (msg.type === "upload") return typeof msg.enabled === "boolean" ? { type: "upload", enabled: msg.enabled } : null;
	if (msg.type === "seek" || msg.type === "position") {
		if (!count(msg.id) || !count(msg.byte)) return null;
		return { type: msg.type, id: msg.id as number, byte: msg.byte as number };
	}
	if (msg.type === "lost") return count(msg.id) && count(msg.piece) ? { type: "lost", id: msg.id as number, piece: msg.piece as number } : null;
	if (msg.type === "end") return count(msg.id) ? { type: "end", id: msg.id as number } : null;
	if (msg.type === "bitrate") {
		const b = msg.bytesPerSecond;
		if (typeof b !== "number" || !Number.isFinite(b) || b < MIN_BITRATE || b > MAX_BITRATE) return null;
		return { type: "bitrate", bytesPerSecond: b };
	}
	if (msg.type === "start") {
		if (typeof msg.magnet !== "string" || msg.magnet.length === 0 || msg.magnet.length > 8 * 1024) return null;
		const gatewayUrl = httpUrl(msg.gatewayUrl);
		const seedersUrl = httpUrl(msg.seedersUrl);
		if (!gatewayUrl || !seedersUrl) return null;
		if (msg.upload !== undefined && typeof msg.upload !== "boolean") return null;
		const upload = msg.upload === undefined ? {} : { upload: msg.upload };
		if (msg.capMs === undefined) return { type: "start", magnet: msg.magnet, gatewayUrl, seedersUrl, ...upload };
		const cap = msg.capMs;
		if (!Number.isInteger(cap) || (cap as number) < MIN_LOOKUP_CAP_MS || (cap as number) > MAX_LOOKUP_CAP_MS) return null;
		return { type: "start", magnet: msg.magnet, gatewayUrl, seedersUrl, capMs: cap as number, ...upload };
	}
	if (msg.type === "recover") {
		if (![msg.id, msg.offset, msg.size, msg.position].every(count)) return null;
		if (!Array.isArray(msg.have) || msg.have.length > MAX_HAVE || !msg.have.every(count)) return null;
		if (typeof msg.save !== "boolean") return null;
		return { type: "recover", id: msg.id as number, offset: msg.offset as number, size: msg.size as number, position: msg.position as number, have: msg.have as number[], save: msg.save };
	}
	if (msg.type === "read") return count(msg.req) && count(msg.piece) ? { type: "read", req: msg.req as number, piece: msg.piece as number } : null;
	if (msg.type === "release") return count(msg.req) ? { type: "release", req: msg.req as number } : null;
	return null;
}

export function parseWorkerMessage(value: unknown): WorkerMessage | null {
	const msg = record(value);
	if (!msg || typeof msg.type !== "string") return null;
	if (msg.type === "error") return typeof msg.message === "string" && msg.message.length < 300 ? { type: "error", message: msg.message } : null;
	if (msg.type === "peers") return Number.isInteger(msg.count) && (msg.count as number) >= 0 ? { type: "peers", count: msg.count as number } : null;
	if (msg.type === "stats") {
		const counts = [msg.upgrades, msg.messages, msg.known, msg.delivered];
		if (!counts.every((n) => Number.isInteger(n) && (n as number) >= 0)) return null;
		const [upgrades, messages, known, delivered] = counts as number[];
		return { type: "stats", upgrades, messages, known, delivered };
	}
	if (msg.type === "recovered") return count(msg.id) ? { type: "recovered", id: msg.id as number } : null;
	if (msg.type === "piece") {
		if (!count(msg.id) || !count(msg.piece)) return null;
		if (!(msg.data instanceof ArrayBuffer) || msg.data.byteLength < 1 || msg.data.byteLength > MAX_PIECE || typeof msg.stored !== "boolean") return null;
		return { type: "piece", id: msg.id as number, piece: msg.piece as number, data: msg.data, stored: msg.stored };
	}
	if (msg.type === "read-result") {
		if (!count(msg.req) || !count(msg.piece)) return null;
		if (msg.data !== null && (!(msg.data instanceof ArrayBuffer) || msg.data.byteLength < 1 || msg.data.byteLength > MAX_PIECE)) return null;
		return { type: "read-result", req: msg.req as number, piece: msg.piece as number, data: msg.data as ArrayBuffer | null };
	}
	if (msg.type === "released") return count(msg.req) ? { type: "released", req: msg.req as number } : null;
	if (msg.type === "store-error") {
		if (!count(msg.id) || !STORE_ERRORS.includes(msg.code as StoreErrorCode)) return null;
		return { type: "store-error", id: msg.id as number, code: msg.code as StoreErrorCode };
	}
	if (msg.type === "progress") {
		const counts = [msg.id, msg.bytes, msg.live, msg.hashFailures, msg.banned, msg.hedges, msg.cancels, msg.windowBytes, msg.rateThreshold];
		if (!counts.every(count)) return null;
		const a = msg.assembling;
		if (!Array.isArray(a) || a.length % 3 !== 0 || a.length > 3 * MAX_ASSEMBLING || !a.every(count)) return null;
		const [id, bytes, live, hashFailures, banned, hedges, cancels, windowBytes, rateThreshold] = counts as number[];
		return { type: "progress", id, assembling: a as number[], bytes, live, hashFailures, banned, hedges, cancels, windowBytes, rateThreshold };
	}
	if (msg.type === "files") {
		const listing = parseListing(msg.listing);
		if (!listing || !(msg.info instanceof ArrayBuffer) || msg.info.byteLength < 1 || msg.info.byteLength > 16 * 1024 * 1024) return null;
		if (!METADATA_ORIGINS.includes(msg.source as MetadataOrigin)) return null;
		return { type: "files", listing, info: msg.info, source: msg.source as MetadataOrigin };
	}
	if (msg.type === "lookup") {
		const progress = parseProgress(msg.progress);
		return progress ? { type: "lookup", progress } : null;
	}
	if (msg.type === "lookup-failed") {
		const progress = parseProgress(msg.progress);
		if (!progress || (msg.reason !== "exhausted" && msg.reason !== "cap" && msg.reason !== "tunnel")) return null;
		if (typeof msg.message !== "string" || msg.message.length > 300) return null;
		return { type: "lookup-failed", reason: msg.reason, message: msg.message, progress };
	}
	if (msg.type === "diag") {
		const diag = parseDiag(msg.diag);
		return diag ? { type: "diag", diag } : null;
	}
	return null;
}

function parseProgress(value: unknown): LookupProgress | null {
	const p = record(value);
	if (!p) return null;
	const counts = [p.found, p.tried, p.connected, p.elapsedMs];
	if (!counts.every(count) || typeof p.discovering !== "boolean") return null;
	const [found, tried, connected, elapsedMs] = counts as number[];
	return { found, tried, connected, elapsedMs, discovering: p.discovering };
}

/** Reason names: what the engine and the gateway call a failure (never an address). */
const REASON = /^[a-z_]{1,32}$/;
const MAX_REASONS = 16;

function parseDials(value: unknown): DialStats | null {
	const d = record(value);
	if (!d || !count(d.attempted) || !count(d.succeeded)) return null;
	const failed = record(d.failed);
	if (!failed) return null;
	const entries = Object.entries(failed);
	if (entries.length > MAX_REASONS) return null;
	const out: Record<string, number> = {};
	for (const [k, v] of entries) {
		if (!REASON.test(k) || !count(v)) return null;
		out[k] = v as number;
	}
	return { attempted: d.attempted as number, succeeded: d.succeeded as number, failed: out };
}

const nullableCount = (v: unknown) => v === null || count(v);

function parseScout(value: unknown): ScoutStreamStats | null {
	const s = record(value);
	if (!s) return null;
	if (s.state !== "idle" && s.state !== "open" && s.state !== "done" && s.state !== "failed") return null;
	if (s.info !== "none" && s.info !== "accepted" && s.info !== "rejected" && s.info !== "late") return null;
	const counts = [s.streams, s.peers, s.alive, s.seeds, s.dead, s.unknown, s.rejected];
	if (!counts.every(count) || !nullableCount(s.firstPeerMs)) return null;
	const [streams, peers, alive, seeds, dead, unknown, rejected] = counts as number[];
	return { state: s.state, streams, peers, alive, seeds, dead, unknown, rejected, info: s.info, firstPeerMs: s.firstPeerMs as number | null };
}

export function parseDiag(value: unknown): SessionDiag | null {
	const d = record(value);
	if (!d) return null;
	const dialsIn = record(d.dials);
	if (!dialsIn) return null;
	const dials = {} as Record<DialClassName, DialStats>;
	for (const kind of DIAL_CLASSES) {
		const parsed = parseDials(dialsIn[kind]);
		if (!parsed) return null;
		dials[kind] = parsed;
	}
	const counts = [d.connected, d.unchoked, d.delivering, d.seeds, d.sockets, d.upgrades, d.messages, d.recycles, d.untestedHighWater];
	if (!counts.every(count) || !nullableCount(d.firstUnchokeMs)) return null;
	let metadata: SessionDiag["metadata"] = null;
	if (d.metadata !== null) {
		const m = record(d.metadata);
		if (!m || !METADATA_ORIGINS.includes(m.source as MetadataOrigin) || !count(m.ms)) return null;
		metadata = { source: m.source as MetadataOrigin, ms: m.ms as number };
	}
	const scout = parseScout(d.scout);
	if (!scout) return null;
	const u = record(d.upload);
	if (!u || typeof u.enabled !== "boolean" || ![u.bytes, u.unchoked, u.messages].every(count)) return null;
	const upload: UploadDiag = { enabled: u.enabled, bytes: u.bytes as number, unchoked: u.unchoked as number, messages: u.messages as number };
	const [connected, unchoked, delivering, seeds, sockets, upgrades, messages, recycles, untestedHighWater] = counts as number[];
	return {
		dials,
		connected,
		unchoked,
		delivering,
		seeds,
		firstUnchokeMs: d.firstUnchokeMs as number | null,
		metadata,
		sockets,
		upgrades,
		messages,
		recycles,
		untestedHighWater,
		scout,
		upload,
	};
}

function parseListing(value: unknown): SessionListing | null {
	const listing = record(value);
	if (!listing) return null;
	if (typeof listing.infoHash !== "string" || !/^[0-9a-f]{40}$/.test(listing.infoHash)) return null;
	if (typeof listing.name !== "string" || listing.name.length > 1024) return null;
	if (!Number.isInteger(listing.size) || (listing.size as number) < 0) return null;
	if (!Number.isInteger(listing.pieceLength) || (listing.pieceLength as number) < 1) return null;
	if (!Array.isArray(listing.files) || listing.files.length > 100_000) return null;
	const files: SessionFile[] = [];
	for (const raw of listing.files) {
		const file = record(raw);
		if (!file || typeof file.path !== "string" || file.path.length > 4096) return null;
		if (!Number.isInteger(file.size) || !Number.isInteger(file.offset)) return null;
		if ((file.size as number) < 0 || (file.offset as number) < 0) return null;
		files.push({ path: file.path, size: file.size as number, offset: file.offset as number });
	}
	return { infoHash: listing.infoHash, name: listing.name, size: listing.size as number, pieceLength: listing.pieceLength as number, files };
}
