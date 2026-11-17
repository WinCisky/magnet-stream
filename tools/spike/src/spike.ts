// Phase 0 spike (docs/architecture.md §Risks, open questions and delivery ›
// Delivery, Phase 0; template in docs/spike-report.md): download N bytes
// through a tunnel gateway and report time to first verified piece,
// throughput, WebSocket messages, estimated billed requests, and what
// happens to a 7th connection and to connections opened later in the
// socket's life.
//
// Two sources:
//   - a fake peer (tools/fake-peer) started with the same --seed/--size/
//     --piece-length: the spike rebuilds the torrent to know its info-hash;
//   - a real torrent (--torrent file.torrent) and a list of candidate peers
//     (--peer, --peers-file): the spike keeps up to --connections peers
//     delivering, replacing the ones that fail, like the engine will.
// Every piece is SHA-1 verified. The report holds counts only: no peer
// addresses, no info-hash.

import { createHash, randomBytes } from "node:crypto";
import {
	PeerConnection,
	TunnelClient,
	TunnelOpenError,
	DownloadError,
	downloadAll,
	failReasonName,
	geometryOf,
	parseInfoDict,
	type Metainfo,
} from "@magnet-stream/engine";
import { bdecode, makeTorrent } from "@magnet-stream/fake-peer";
import { FailReason } from "@magnet-stream/tunnel-protocol";

export interface SpikeSource {
	kind: "fake" | "torrent";
	meta: Metainfo;
	infoHash: Uint8Array;
	/** Candidate peers, best first. A fake source repeats its one peer. */
	candidates: string[];
}

export interface SpikeOptions {
	/** ws(s):// or http(s):// URL of the gateway (…/v1/tunnel is added if missing). */
	gateway: string;
	source: SpikeSource;
	bytes: number;
	/** Peers kept delivering at once (spread over ceil(n/6) WebSockets). */
	connections: number;
	/** Give up after this long (the report says how far it got). */
	maxSeconds: number;
	/** Run the connection-limit probe. */
	probe: boolean;
	/** How long the probe waits before opening "later" connections. */
	laterMs: number;
	/** How many connections the probe tries to hold at once (6 + 1 to see the 7th). */
	probeOpen: number;
	log?: (line: string) => void;
}

export interface ConnectionAttempt {
	label: string;
	ok: boolean;
	/** Time until `opened` / `failed`. */
	openMs: number;
	/** Failure reason (gateway FailReason name, or local error). */
	reason?: string;
	/** Connections of the probe socket still open when this one was opened. */
	heldBefore?: number;
	/** A verified piece came through (time to get it). */
	dataMs?: number;
	dataError?: string;
}

export interface SpikeReport {
	version: 2;
	startedAt: string;
	params: { source: "fake" | "torrent"; bytes: number; torrentSize: number; pieceLength: number; connections: number; candidates: number };
	download: {
		/** From opening the first WebSocket to the first verified piece. */
		firstPieceMs: number;
		bytes: number;
		ms: number;
		mibPerSecond: number;
		piecesVerified: number;
		piecesCorrupt: number;
		complete: boolean;
	};
	peers: { tried: number; opened: number; delivered: number; failures: Record<string, number> };
	websocket: {
		upgrades: number;
		messagesSent: number;
		framesSent: number;
		bytesSent: number;
		messagesReceived: number;
		framesReceived: number;
		bytesReceived: number;
	};
	billing: { formula: string; estimatedRequests: number; estimatedRequestsPerGiB: number };
	connections?: { reachablePeers: number; initial: ConnectionAttempt[]; later: ConnectionAttempt[]; laterAfterMs: number; seventh: string };
}

const GiB = 1024 ** 3;
const MAX_PER_SOCKET = 6;

export function tunnelUrl(gateway: string): string {
	const url = new URL(gateway);
	if (url.protocol === "http:") url.protocol = "ws:";
	if (url.protocol === "https:") url.protocol = "wss:";
	if (url.protocol !== "ws:" && url.protocol !== "wss:") throw new Error("--gateway must be a ws(s):// or http(s):// URL");
	if (url.pathname === "/" || url.pathname === "") url.pathname = "/v1/tunnel";
	return url.toString();
}

/** The fake peer's torrent, rebuilt from its parameters. */
export async function fakeSource(peer: string, seed: number, size: number, pieceLength: number, connections: number): Promise<SpikeSource> {
	const torrent = makeTorrent({ seed, size, pieceLength });
	const meta = await parseInfoDict(torrent.info, torrent.infoHashHex);
	return { kind: "fake", meta, infoHash: torrent.infoHash, candidates: Array<string>(Math.max(connections, 1) * 4).fill(peer) };
}

/** A real .torrent file and candidate peers. */
export async function torrentSource(file: Uint8Array, candidates: string[]): Promise<SpikeSource> {
	const info = infoDictOf(file);
	const hex = createHash("sha1").update(info).digest("hex");
	const meta = await parseInfoDict(info, hex);
	return { kind: "torrent", meta, infoHash: Buffer.from(hex, "hex"), candidates: [...new Set(candidates)] };
}

/** The raw bytes of the `info` value of a .torrent (its SHA-1 is the info-hash). */
export function infoDictOf(torrent: Uint8Array): Uint8Array {
	if (torrent[0] !== 0x64 /* d */) throw new Error("not a torrent file");
	let at = 1;
	while (at < torrent.length && torrent[at] !== 0x65 /* e */) {
		const key = bdecode(torrent.subarray(at));
		if (!key || !(key.value instanceof Uint8Array)) break;
		at += key.end;
		const value = bdecode(torrent.subarray(at));
		if (!value) break;
		if (new TextDecoder().decode(key.value) === "info") return torrent.subarray(at, at + value.end);
		at += value.end;
	}
	throw new Error("torrent file has no info dict");
}

/** The first pieces covering `bytes`, repeated (rounds) if the torrent is smaller. */
export function piecesFor(meta: Metainfo, bytes: number): number[] {
	const out: number[] = [];
	let sum = 0;
	while (sum < bytes) {
		for (let p = 0; p < meta.pieceCount && sum < bytes; p++) {
			out.push(p);
			sum += pieceBytes(meta, p);
		}
	}
	return out;
}

export async function runSpike(opts: SpikeOptions): Promise<SpikeReport> {
	const log = opts.log ?? (() => {});
	const url = tunnelUrl(opts.gateway);
	const { meta, infoHash } = opts.source;
	const report: SpikeReport = {
		version: 2,
		startedAt: new Date().toISOString(),
		params: {
			source: opts.source.kind,
			bytes: opts.bytes,
			torrentSize: meta.totalLength,
			pieceLength: meta.pieceLength,
			connections: opts.connections,
			candidates: new Set(opts.source.candidates).size,
		},
		download: { firstPieceMs: 0, bytes: 0, ms: 0, mibPerSecond: 0, piecesVerified: 0, piecesCorrupt: 0, complete: false },
		peers: { tried: 0, opened: 0, delivered: 0, failures: {} },
		websocket: { upgrades: 0, messagesSent: 0, framesSent: 0, bytesSent: 0, messagesReceived: 0, framesReceived: 0, bytesReceived: 0 },
		billing: { formula: "upgrades + ceil(client→gateway messages / 20)", estimatedRequests: 0, estimatedRequestsPerGiB: 0 },
	};
	const clients: TunnelClient[] = [];
	const connect = async (maxConnections?: number) => {
		const client = await TunnelClient.connect(url, { maxConnections });
		clients.push(client);
		report.websocket.upgrades++;
		return client;
	};
	const fail = (why: string): void => {
		report.peers.failures[why] = (report.peers.failures[why] ?? 0) + 1;
	};
	const good: string[] = [];

	try {
		log(`downloading ${opts.bytes} bytes through ${new URL(url).host} with up to ${opts.connections} peers…`);
		const started = Date.now();
		const deadline = started + opts.maxSeconds * 1000;
		const sockets = await Promise.all(Array.from({ length: Math.ceil(opts.connections / MAX_PER_SOCKET) }, () => connect()));
		const queue = piecesFor(meta, opts.bytes);
		const candidates = [...opts.source.candidates];
		let active = 0;
		let lastLog = 0;

		const worker = async (conn: PeerConnection, addr: string) => {
			let delivered = false;
			try {
				for (;;) {
					if (Date.now() > deadline || queue.length === 0) return;
					// Next pieces this peer has (distinct), at most 8 at a time.
					const batch: number[] = [];
					for (let i = 0; i < queue.length && batch.length < 8; i++) {
						const p = queue[i];
						if (conn.has(p) && !batch.includes(p)) batch.push(...queue.splice(i--, 1));
					}
					if (batch.length === 0) {
						// Nothing it has (yet): give it a moment to announce, then drop it.
						if (conn.closed || conn.pieceCount > 0 || delivered) return fail("useless");
						await new Promise((r) => setTimeout(r, 2_000));
						if (conn.pieceCount === 0) return fail("no_pieces");
						continue;
					}
					try {
						const r = await downloadAll(conn, meta, {
							pieces: batch,
							stallTimeoutMs: 20_000,
							onPiece: (p) => {
								if (report.download.firstPieceMs === 0) report.download.firstPieceMs = Date.now() - started;
								report.download.piecesVerified++;
								report.download.bytes += pieceBytes(meta, p);
							},
						});
						if (!delivered && r.verified.length > 0) {
							delivered = true;
							report.peers.delivered++;
							good.push(addr);
						}
						report.download.piecesCorrupt += r.corrupt.length;
						queue.unshift(...r.missing, ...r.corrupt);
						if (r.corrupt.length > 0) return fail("corrupt");
					} catch (e) {
						const result = e instanceof DownloadError ? e.result : undefined;
						if (result && !delivered && result.verified.length > 0) {
							delivered = true;
							report.peers.delivered++;
							good.push(addr);
						}
						queue.unshift(...batch.filter((p) => !result?.verified.includes(p)));
						return fail((e as Error).message.startsWith("no progress") ? "stalled" : "closed");
					}
					if (Date.now() - lastLog > 5_000) {
						lastLog = Date.now();
						log(`  ${(report.download.bytes / 2 ** 20).toFixed(0)} MiB verified, ${active} peers, ${queue.length} pieces left`);
					}
				}
			} finally {
				conn.close();
			}
		};

		// Keep `connections` peers busy; replace whoever drops out.
		await new Promise<void>((resolve) => {
			const workers = new Set<Promise<void>>();
			let opening = 0;
			const pump = () => {
				const done = queue.length === 0 || Date.now() > deadline;
				if ((done || (candidates.length === 0 && opening === 0)) && active === 0 && opening === 0) return resolve();
				if (done) return;
				while (active + opening < opts.connections && candidates.length > 0) {
					const client = sockets.find((c) => !c.closed && c.connections < MAX_PER_SOCKET);
					if (!client) break;
					const addr = candidates.shift()!;
					opening++;
					report.peers.tried++;
					client
						.open({ address: addr, infoHash, peerId: peerId(), timeoutMs: 20_000 })
						.then(
							(transport) => {
								report.peers.opened++;
								active++;
								const w = worker(new PeerConnection(transport, { geometry: geometryOf(meta) }), addr).finally(() => {
									active--;
									workers.delete(w);
									pump();
								});
								workers.add(w);
							},
							(e: unknown) => {
								// The gateway frees a slot only once the failed socket has
								// closed (up to ~1 s): retry this candidate a bit later.
								if (e instanceof TunnelOpenError && e.reason === FailReason.TOO_MANY_CONNECTIONS) {
									report.peers.tried--;
									candidates.unshift(addr);
									return new Promise((r) => setTimeout(r, 500));
								}
								fail(e instanceof TunnelOpenError && e.reason ? failReasonName(e.reason) : "error");
							},
						)
						.finally(() => {
							opening--;
							pump();
						});
				}
			};
			pump();
		});

		report.download.ms = Date.now() - started;
		report.download.complete = report.download.bytes >= opts.bytes;
		report.download.mibPerSecond = round(report.download.bytes / 2 ** 20 / (report.download.ms / 1000));
		for (const c of sockets) c.close();

		if (opts.probe) {
			const peers = good.length > 0 ? good : opts.source.candidates.slice(0, 1);
			report.connections = await probe(connect, opts.source, peers, opts, log);
		}
	} finally {
		for (const c of clients) {
			c.close();
			report.websocket.messagesSent += c.stats.messagesSent;
			report.websocket.framesSent += c.stats.framesSent;
			report.websocket.bytesSent += c.stats.bytesSent;
			report.websocket.messagesReceived += c.stats.messagesReceived;
			report.websocket.framesReceived += c.stats.framesReceived;
			report.websocket.bytesReceived += c.stats.bytesReceived;
		}
	}
	report.billing.estimatedRequests = report.websocket.upgrades + Math.ceil(report.websocket.messagesSent / 20);
	report.billing.estimatedRequestsPerGiB = report.download.bytes > 0 ? round((report.billing.estimatedRequests * GiB) / report.download.bytes) : 0;
	return report;
}

/**
 * Connection-limit probe on a fresh WebSocket:
 *   1. find `probeOpen + 2` distinct reachable peers (on a scouting socket, one handshake each);
 *   2. on the probe socket, open them one after the other while holding the
 *      earlier ones: the 7th and later show what happens past 6 (the
 *      gateway's own limit, or the platform's if TUNNEL_MAX_CONNECTIONS is raised);
 *   3. check that every held connection delivers a verified piece;
 *   4. after `laterMs`, free two slots and open three more ("later in the socket's life").
 */
async function probe(
	connect: (max?: number) => Promise<TunnelClient>,
	source: SpikeSource,
	preferred: string[],
	opts: SpikeOptions,
	log: (l: string) => void,
) {
	log(`probing connection limits (${opts.probeOpen} held at once, then later after ${opts.laterMs} ms)…`);
	const want = opts.probeOpen + 2;
	const pool = [...new Set([...preferred, ...source.candidates])];
	const reachable: string[] = [];
	const scout = await connect();
	while (reachable.length < want && pool.length > 0) {
		const batch = pool.splice(0, Math.min(6, pool.length));
		await Promise.all(
			batch.map(async (addr) => {
				try {
					const t = await scout.open({ address: addr, infoHash: source.infoHash, peerId: peerId(), timeoutMs: 20_000 });
					t.close();
					reachable.push(addr);
				} catch {}
			}),
		);
		await new Promise((r) => setTimeout(r, 1_200)); // let the gateway free the slots
	}
	scout.close();
	if (source.kind === "fake") while (reachable.length < want) reachable.push(source.candidates[0]);
	log(`  ${reachable.length} reachable peers found`);

	const client = await connect(16);
	let next = 0;
	const attempt = async (label: string, held: number): Promise<{ a: ConnectionAttempt & { heldBefore: number }; conn?: PeerConnection }> => {
		const addr = reachable[next++ % reachable.length];
		const started = Date.now();
		try {
			const transport = await client.open({ address: addr, infoHash: source.infoHash, peerId: peerId(), timeoutMs: 20_000 });
			return { a: { label, heldBefore: held, ok: true, openMs: Date.now() - started }, conn: new PeerConnection(transport, { geometry: geometryOf(source.meta) }) };
		} catch (e) {
			const reason = e instanceof TunnelOpenError ? (e.reason ? failReasonName(e.reason) : e.message) : (e as Error).message;
			return { a: { label, heldBefore: held, ok: false, openMs: Date.now() - started, reason } };
		}
	};
	const checkData = async (a: ConnectionAttempt, conn: PeerConnection) => {
		const t0 = Date.now();
		try {
			for (let i = 0; i < 30 && conn.pieceCount === 0 && !conn.closed; i++) await new Promise((r) => setTimeout(r, 100));
			const piece = [...Array(source.meta.pieceCount).keys()].find((p) => conn.has(p));
			if (piece === undefined) throw new Error("the peer announced no pieces");
			const r = await downloadAll(conn, source.meta, { pieces: [piece], stallTimeoutMs: 15_000 });
			if (r.verified.length === 1) a.dataMs = Date.now() - t0;
			else a.dataError = "piece not verified";
		} catch (e) {
			a.dataError = (e as Error).message;
		}
	};

	const initial: ConnectionAttempt[] = [];
	const held: { a: ConnectionAttempt; conn: PeerConnection }[] = [];
	for (let i = 1; i <= opts.probeOpen; i++) {
		const { a, conn } = await attempt(`initial-${i}`, held.filter((h) => !h.conn.closed).length);
		initial.push(a);
		if (conn) held.push({ a, conn });
	}
	await Promise.all(held.map((h) => checkData(h.a, h.conn)));

	await new Promise((r) => setTimeout(r, opts.laterMs));
	held.splice(0, 2).forEach((h) => h.conn.close());
	await new Promise((r) => setTimeout(r, 1_500));
	const later: ConnectionAttempt[] = [];
	for (let i = 1; i <= 3; i++) {
		const { a, conn } = await attempt(`later-${i}`, held.filter((h) => !h.conn.closed).length);
		later.push(a);
		if (conn) {
			held.push({ a, conn });
			await checkData(a, conn);
		}
	}
	held.forEach((h) => h.conn.close());
	const seventh = initial[6];
	return {
		reachablePeers: reachable.length,
		initial,
		later,
		laterAfterMs: opts.laterMs,
		seventh: !seventh
			? "not tried"
			: seventh.ok
				? seventh.dataMs !== undefined
					? `opened with ${(seventh as { heldBefore?: number }).heldBefore} held, data in ${seventh.dataMs} ms`
					: `opened, no data (${seventh.dataError})`
				: `failed: ${seventh.reason}`,
	};
}

function pieceBytes(meta: Metainfo, p: number): number {
	return p < meta.pieceCount - 1 ? meta.pieceLength : meta.totalLength - meta.pieceLength * (meta.pieceCount - 1);
}

function peerId(): Uint8Array {
	return Buffer.concat([Buffer.from("-MS0001-", "latin1"), randomBytes(12)]);
}

function round(n: number): number {
	return Math.round(n * 100) / 100;
}
