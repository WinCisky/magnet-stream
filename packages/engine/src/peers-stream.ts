// One `GET /peers` NDJSON stream (docs/discovery-api.md, decision OQ-21).
// Each `peer` line is an address to dial, with what the VPS scout learned
// about it (score, alive, seed, have, md, unchoked, rtt); the `info` line
// carries the torrent's info dict, base64. The scout's facts are advice
// for the dial order only: the gateway's handshake gate and the metadata
// check against the info-hash stay mandatory.
//
// Every line is untrusted: a line that is not valid JSON, names a bad
// address, or carries a known field of the wrong type or out of range is
// dropped whole; unknown fields are ignored. Lines are bounded in length
// (an `info` line by its decoded size, 16 MiB) whatever the stream sends.
// The caller makes a single request: this reader does not retry.

/** What the VPS scout said about an address (docs/discovery-api.md › Response). */
export interface ScoutFacts {
	/** 0..1: about 0.5 for a handshake, more for seed/metadata/unchoke/rtt, ~0.02 dead, 0.2 unknown. */
	score: number;
	/** Probed: completed the handshake (true) or not (false). Absent: not probed. */
	alive?: boolean;
	seed?: boolean;
	/** Share of pieces it has, 0..1. */
	have?: number;
	/** It offers the metadata (BEP 9). */
	md?: boolean;
	unchoked?: boolean;
	/** TCP connect time from the VPS, ms. */
	rtt?: number;
}

export type StreamLine =
	| { kind: "peer"; peer: string; scout: ScoutFacts | undefined }
	| { kind: "info"; info: Uint8Array }
	| { kind: "done" };

/** Largest info dict accepted from the stream (decoded bytes). */
export const MAX_STREAM_INFO_BYTES = 16 * 1024 * 1024;
/** Longest `peer` / `done` line read. */
const MAX_PEER_LINE = 512;
/** Longest line kept in memory: an `info` line of MAX_STREAM_INFO_BYTES plus its JSON. */
const MAX_LINE = Math.ceil(MAX_STREAM_INFO_BYTES / 3) * 4 + 256;
/** Highest `rtt` taken as plausible (ms). */
const MAX_RTT_MS = 600_000;

const PEER_LINE = /^(?:(?:\d{1,3}\.){3}\d{1,3}|\[[0-9a-fA-F:.]+\]):\d{1,5}$/;
const BASE64 = /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/;

/** One NDJSON line, validated; null for anything unusable. */
export function parseStreamLine(line: string): StreamLine | null {
	const text = line.trim();
	if (text.length === 0 || text.length > MAX_LINE || text[0] !== "{") return null;
	// Only an info line may be long: don't parse a huge line that cannot be one.
	if (text.length > MAX_PEER_LINE && !text.includes('"info"')) return null;
	let value: unknown;
	try {
		value = JSON.parse(text);
	} catch {
		return null;
	}
	if (!value || typeof value !== "object" || Array.isArray(value)) return null;
	const v = value as Record<string, unknown>;
	if ("peer" in v) {
		if (text.length > MAX_PEER_LINE) return null;
		return parsePeer(v);
	}
	if ("info" in v) return parseInfo(v);
	if (v.done === true) return { kind: "done" };
	return null;
}

/** A peer line's address, or null (done, info, empty, malformed). */
export function parsePeerLine(line: string): string | null {
	const parsed = parseStreamLine(line);
	return parsed?.kind === "peer" ? parsed.peer : null;
}

function parsePeer(v: Record<string, unknown>): StreamLine | null {
	const peer = v.peer;
	if (typeof peer !== "string" || !PEER_LINE.test(peer)) return null;
	const port = Number(peer.slice(peer.lastIndexOf(":") + 1));
	if (!Number.isInteger(port) || port < 1 || port > 65535) return null;
	// A line with no score is a plain address (older stubs): no scout facts.
	if (v.score === undefined) {
		for (const k of ["alive", "seed", "have", "md", "unchoked", "rtt"]) if (v[k] !== undefined) return null;
		return { kind: "peer", peer, scout: undefined };
	}
	if (!unit(v.score)) return null;
	const scout: ScoutFacts = { score: v.score };
	for (const k of ["alive", "seed", "md", "unchoked"] as const) {
		const b = v[k];
		if (b === undefined) continue;
		if (typeof b !== "boolean") return null;
		scout[k] = b;
	}
	if (v.have !== undefined) {
		if (!unit(v.have)) return null;
		scout.have = v.have;
	}
	if (v.rtt !== undefined) {
		if (typeof v.rtt !== "number" || !Number.isFinite(v.rtt) || v.rtt < 0 || v.rtt > MAX_RTT_MS) return null;
		scout.rtt = v.rtt;
	}
	// Facts that contradict each other: the line is not trusted at all.
	if (scout.alive !== true && (scout.seed === true || scout.md === true || scout.unchoked === true)) return null;
	if (scout.seed === true && scout.have !== undefined && scout.have < 1) return null;
	return { kind: "peer", peer, scout };
}

function parseInfo(v: Record<string, unknown>): StreamLine | null {
	const info = v.info;
	if (typeof info !== "string" || info.length === 0 || info.length % 4 !== 0) return null;
	if (info.length > Math.ceil(MAX_STREAM_INFO_BYTES / 3) * 4 || !BASE64.test(info)) return null;
	const padding = info.endsWith("==") ? 2 : info.endsWith("=") ? 1 : 0;
	const length = (info.length / 4) * 3 - padding;
	if (length < 1 || length > MAX_STREAM_INFO_BYTES) return null;
	if (v.size !== undefined && v.size !== length) return null;
	const binary = atob(info);
	const bytes = new Uint8Array(binary.length);
	for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
	return { kind: "info", info: bytes };
}

function unit(x: unknown): x is number {
	return typeof x === "number" && Number.isFinite(x) && x >= 0 && x <= 1;
}

export interface PeerStreamHandlers {
	/** An address, with the scout's facts when the line had a score. */
	onPeer(address: string, scout: ScoutFacts | undefined): void;
	/** The info dict (at most once per stream). Unverified: check it against the info-hash. */
	onInfo?(info: Uint8Array): void;
}

export interface PeerStreamResult {
	/** Addresses accepted. */
	peers: number;
	/** An info line was read. */
	info: boolean;
	/** Lines dropped as malformed, out of range or too long. */
	rejected: number;
	/** The `done` line arrived (a stream that ends without it is a normal end too). */
	done: boolean;
}

/**
 * Read a peers stream. `fetchImpl` defaults to the platform `fetch` (one
 * request). Returns how many addresses were accepted.
 */
export async function readPeerStream(
	url: string,
	onPeer: PeerStreamHandlers["onPeer"],
	opts: { signal?: AbortSignal; fetch?: typeof fetch; onInfo?: PeerStreamHandlers["onInfo"] } = {},
): Promise<number> {
	const res = await (opts.fetch ?? fetch)(url, { signal: opts.signal, headers: { accept: "application/x-ndjson" } });
	if (!res.ok || !res.body) throw new Error(`peers request failed (${res.status})`);
	return (await readPeerBody(res.body, { onPeer, onInfo: opts.onInfo })).peers;
}

/** Pull NDJSON lines from a body; returns how many addresses were accepted. */
export async function consumePeerBody(body: ReadableStream<Uint8Array>, onPeer: PeerStreamHandlers["onPeer"], onInfo?: PeerStreamHandlers["onInfo"]): Promise<number> {
	return (await readPeerBody(body, { onPeer, onInfo })).peers;
}

/** Pull NDJSON lines from a body, with counts. */
export async function readPeerBody(body: ReadableStream<Uint8Array>, handlers: PeerStreamHandlers): Promise<PeerStreamResult> {
	const reader = body.getReader();
	const decode = new TextDecoder();
	const result: PeerStreamResult = { peers: 0, info: false, rejected: 0, done: false };
	let pending = "";
	// The current line grew past MAX_LINE: drop it up to its newline.
	let skipping = false;
	const line = (text: string) => {
		if (text.trim().length === 0) return;
		const parsed = parseStreamLine(text);
		if (!parsed) {
			result.rejected++;
			return;
		}
		if (parsed.kind === "peer") {
			result.peers++;
			handlers.onPeer(parsed.peer, parsed.scout);
		} else if (parsed.kind === "info") {
			// One info line per response: a second one is ignored.
			if (result.info) return;
			result.info = true;
			handlers.onInfo?.(parsed.info);
		} else result.done = true;
	};
	const feed = (text: string) => {
		let start = 0;
		let nl = text.indexOf("\n");
		while (nl >= 0) {
			if (skipping) skipping = false;
			else line(pending + text.slice(start, nl));
			pending = "";
			start = nl + 1;
			nl = text.indexOf("\n", start);
		}
		if (skipping) return;
		const rest = text.slice(start);
		if (pending.length + rest.length > MAX_LINE) {
			skipping = true;
			pending = "";
			result.rejected++;
		} else pending += rest;
	};
	for (;;) {
		const { done, value } = await reader.read();
		if (done) break;
		feed(decode.decode(value, { stream: true }));
	}
	feed(decode.decode());
	if (!skipping) line(pending);
	return result;
}
