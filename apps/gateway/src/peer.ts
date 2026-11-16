// Connecting to candidate peers and racing them: the first to unchoke us
// while holding some of the wanted pieces wins; the rest are closed.
//
// A request may carry dozens of candidates, most of which may be dead. They
// are tried in the given order through a window of at most MAX_OPEN sockets
// (the Workers limit on simultaneous outgoing connections): each failure
// starts the next candidate.

import { connect as cfConnect } from "cloudflare:sockets";
import { planBlocks, type BlocksRequest, type Candidate } from "./params";
import { handshake, message, MSG, peerId, SetupParser, WireError, type Block } from "./wire";

const CONNECT_TIMEOUT_MS = 3_000;
/** More than any sane pre-unchoke chatter (handshake + bitfield + haves). */
const MAX_SETUP_BYTES = 512 * 1024;
/**
 * Workers allow 6 simultaneous outgoing connections per invocation; further
 * connects queue silently until one closes, so we never ask for more.
 */
export const MAX_OPEN = 6;
/** How long a closed socket may take to release its connection slot. */
const CLOSE_WAIT_MS = 1_000;

export type Connector = (candidate: Candidate) => Socket;

export interface Outcome {
	peer: string;
	/**
	 * Why the candidate didn't win: a setup error, `lost` (still setting up
	 * when another won), `evicted` (handshaken but still choked after `hold`
	 * while others waited) or `untried` (the race ended before its turn).
	 */
	err?: string;
	/** It completed the handshake (so it's reachable and has the torrent). */
	hs?: 1;
}

export interface Winner {
	candidate: Candidate;
	socket: Socket;
	reader: ReadableStreamDefaultReader<Uint8Array>;
	writer: WritableStreamDefaultWriter<Uint8Array>;
	/** Everything the peer sent so far, forwarded to the browser verbatim. */
	setupBytes: Uint8Array[];
	parser: SetupParser;
	ms: { connect: number; handshake: number; unchoke: number };
}

export class SetupError extends Error {
	constructor(readonly code: string) {
		super(code);
	}
}

export class RaceLost extends Error {
	constructor(readonly outcomes: Outcome[]) {
		super("no candidate peer unchoked us with the wanted pieces");
	}
}

export interface RaceOptions {
	/** Nobody may unchoke later than this (epoch ms). */
	deadline: number;
	/**
	 * A handshaken peer still choked this long after we started connecting
	 * gives its slot to a waiting candidate.
	 */
	holdMs: number;
	/**
	 * Try the first candidate alone for this long before opening the other
	 * slots (0: all at once). A good first pick then isn't beaten by
	 * whichever fallback happens to unchoke first; the fallbacks only step
	 * in when it's slow, or at once when it fails.
	 */
	staggerMs?: number;
	connect?: Connector;
}

/**
 * Race the candidates. Resolves with the winner and the blocks to request
 * from it (the wanted blocks it has, capped at its queue depth).
 */
export function race(
	req: BlocksRequest,
	candidates: Candidate[],
	{ deadline, holdMs, staggerMs = 0, connect = (c) => cfConnect({ hostname: c.hostname, port: c.port }) }: RaceOptions,
): Promise<{ winner: Winner; blocks: Block[]; outcomes: Outcome[] }> {
	const outcomes: Outcome[] = candidates.map((c) => ({ peer: c.addr }));
	const aborts = candidates.map(() => new AbortController());
	const holdTimers: ReturnType<typeof setTimeout>[] = [];
	let settled = false;
	let next = 0;
	let open = 0;
	/** Evicted peers whose slots aren't free yet. */
	let evicting = 0;
	/** Slots open to candidates: just the first one while staggering. */
	let width = staggerMs > 0 ? 1 : MAX_OPEN;
	let widenTimer: ReturnType<typeof setTimeout> | undefined;

	return new Promise((resolve, reject) => {
		const finish = () => {
			settled = true;
			if (widenTimer !== undefined) clearTimeout(widenTimer);
			holdTimers.forEach(clearTimeout);
			for (let j = next; j < candidates.length; j++) outcomes[j].err ??= "untried";
		};
		const fill = () => {
			while (!settled && open < width && next < candidates.length && Date.now() < deadline) {
				start(next++);
			}
			if (!settled && open === 0) {
				finish();
				reject(new RaceLost(outcomes));
			}
		};
		// The slot is free once the socket has closed (or was never opened).
		const release = (i: number, closed: Promise<unknown>) => {
			void within(closed, CLOSE_WAIT_MS, "close")
				.catch(() => {})
				.then(() => {
					open--;
					if (outcomes[i].err === "evicted") evicting--;
					fill();
				});
		};
		const lose = (i: number, err: string, closed: Promise<unknown>) => {
			outcomes[i].err ??= err;
			// The first pick failed: no point waiting to open the other slots.
			if (!settled) width = MAX_OPEN;
			release(i, closed);
		};
		const start = (i: number) => {
			open++;
			const candidate = candidates[i];
			const started = Date.now();
			const onHandshaken = () => {
				outcomes[i].hs = 1;
				const left = started + holdMs - Date.now();
				holdTimers.push(
					setTimeout(() => {
						// Evict only for someone waiting (then all slots are taken),
						// one peer per waiting candidate.
						if (settled || candidates.length - next <= evicting) return;
						evicting++;
						outcomes[i].err ??= "evicted";
						aborts[i].abort();
					}, Math.max(0, left)),
				);
			};
			setUp(candidate, req, deadline, aborts[i].signal, connect, onHandshaken).then(
				(winner) => {
					if (settled) {
						lose(i, "lost", close(winner.socket));
						return;
					}
					const blocks = planBlocks(req, winner.parser);
					if (blocks.length === 0) {
						lose(i, "no_pieces", close(winner.socket));
						return;
					}
					finish();
					aborts.forEach((abort, j) => {
						if (j !== i) {
							outcomes[j].err ??= "lost";
							abort.abort();
						}
					});
					resolve({ winner, blocks, outcomes: outcomes.filter((_, j) => j !== i) });
				},
				(e: unknown) =>
					lose(
						i,
						e instanceof SetupFailure ? e.code : "error",
						e instanceof SetupFailure ? e.closed : Promise.resolve(),
					),
			);
		};
		if (staggerMs > 0) {
			widenTimer = setTimeout(() => {
				width = MAX_OPEN;
				fill();
			}, staggerMs);
		}
		fill();
	});
}

/** A failed setup, and when its socket has closed. */
class SetupFailure extends Error {
	constructor(
		readonly code: string,
		readonly closed: Promise<unknown>,
	) {
		super(code);
	}
}

async function setUp(
	candidate: Candidate,
	req: BlocksRequest,
	deadline: number,
	signal: AbortSignal,
	connect: Connector,
	onHandshaken: () => void,
): Promise<Winner> {
	const started = Date.now();
	let socket: Socket;
	try {
		socket = connect(candidate);
	} catch {
		throw new SetupFailure("connect_failed", Promise.resolve());
	}
	socket.closed.catch(() => {});
	const onAbort = () => void close(socket);
	signal.addEventListener("abort", onAbort, { once: true });

	try {
		await within(
			socket.opened.catch(() => {
				throw new SetupError("connect_failed");
			}),
			Math.min(CONNECT_TIMEOUT_MS, deadline - started),
			"connect_timeout",
		);
		const connectMs = Date.now() - started;

		const writer = socket.writable.getWriter();
		const reader = socket.readable.getReader();
		await writer.write(handshake(req.infoHash, peerId()));

		const parser = new SetupParser(req.infoHash);
		const setupBytes: Uint8Array[] = [];
		let received = 0;
		let handshakeMs = 0;
		while (!parser.unchoked) {
			const { value, done } = await within(
				reader.read(),
				deadline - Date.now(),
				parser.handshaken ? "choked" : "handshake_timeout",
			);
			if (done || signal.aborted) throw new SetupError("closed");
			received += value.byteLength;
			if (received > MAX_SETUP_BYTES) throw new SetupError("protocol");
			setupBytes.push(value);
			const wasHandshaken = parser.handshaken;
			parser.push(value);
			if (!wasHandshaken && parser.handshaken) {
				handshakeMs = Date.now() - started;
				onHandshaken();
				// BEP 6: with the fast extension on both sides, our first
				// message must be have-all/have-none/bitfield.
				const hello = parser.fast
					? concat(message(MSG.HAVE_NONE), message(MSG.INTERESTED))
					: message(MSG.INTERESTED);
				await writer.write(hello);
			}
		}

		signal.removeEventListener("abort", onAbort);
		return {
			candidate,
			socket,
			reader,
			writer,
			setupBytes,
			parser,
			ms: { connect: connectMs, handshake: handshakeMs, unchoke: Date.now() - started },
		};
	} catch (e) {
		const code = e instanceof SetupError || e instanceof WireError ? e.code : "error";
		throw new SetupFailure(code, close(socket));
	}
}

function within<T>(promise: Promise<T>, ms: number, code: string): Promise<T> {
	if (ms <= 0) return Promise.reject(new SetupError(code));
	let timer: ReturnType<typeof setTimeout>;
	return Promise.race([
		promise,
		new Promise<never>((_, reject) => {
			timer = setTimeout(() => reject(new SetupError(code)), ms);
		}),
	]).finally(() => clearTimeout(timer));
}

/** Close the socket; resolves (never rejects) once it has closed. */
export function close(socket: Socket): Promise<void> {
	try {
		return socket.close().then(
			() => {},
			() => {},
		);
	} catch {
		return Promise.resolve(); // already closed
	}
}

export function concat(...parts: Uint8Array[]): Uint8Array {
	const out = new Uint8Array(parts.reduce((n, p) => n + p.byteLength, 0));
	let at = 0;
	for (const p of parts) {
		out.set(p, at);
		at += p.byteLength;
	}
	return out;
}
