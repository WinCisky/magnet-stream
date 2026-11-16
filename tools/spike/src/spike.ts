// Phase 0 spike (docs/architecture.md §Risks, open questions and delivery ›
// Delivery, Phase 0; template in docs/spike-report.md): download N bytes from a test
// peer through a tunnel gateway and report throughput, WebSocket messages,
// estimated billed requests, and what happens to a 7th connection and to
// connections opened later in the socket's life.
//
// The peer must be a fake peer (tools/fake-peer) started with the same
// --seed/--size/--piece-length: the spike rebuilds the torrent locally to
// know its info-hash and verify every piece.

import { randomBytes } from "node:crypto";
import { PeerConnection, TunnelClient, TunnelOpenError, downloadAll, failReasonName, parseInfoDict, type Metainfo } from "@magnet-stream/engine";
import { makeTorrent, type FakeTorrent } from "@magnet-stream/fake-peer";

export interface SpikeOptions {
	/** ws(s):// or http(s):// URL of the gateway (…/v1/tunnel is added if missing). */
	gateway: string;
	/** `ip:port` of the test peer. */
	peer: string;
	bytes: number;
	seed: number;
	size: number;
	pieceLength: number;
	/** Parallel connections for the throughput run (default 1). */
	connections: number;
	/** Run the connection-limit probe. */
	probe: boolean;
	/** How long the probe waits before opening "later" connections. */
	laterMs: number;
	/** How many connections the probe tries to hold (6 + 1 to see the 7th). */
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
	/** A block actually came through (time to get it). */
	dataMs?: number;
	dataError?: string;
}

export interface SpikeReport {
	version: 1;
	startedAt: string;
	params: { bytes: number; torrentSize: number; pieceLength: number; seed: number; connections: number };
	download: {
		/** From opening the WebSocket to the first verified piece (a phase metric of docs/architecture.md › Delivery). */
		firstPieceMs: number;
		bytes: number;
		ms: number;
		mibPerSecond: number;
		piecesVerified: number;
		piecesCorrupt: number;
		piecesMissing: number;
		rounds: number;
	};
	websocket: {
		upgrades: number;
		messagesSent: number;
		framesSent: number;
		bytesSent: number;
		messagesReceived: number;
		framesReceived: number;
		bytesReceived: number;
	};
	billing: {
		formula: string;
		estimatedRequests: number;
		estimatedRequestsPerGiB: number;
	};
	connections?: {
		initial: ConnectionAttempt[];
		later: ConnectionAttempt[];
		laterAfterMs: number;
		seventh: string;
	};
}

const GiB = 1024 ** 3;

export function tunnelUrl(gateway: string): string {
	const url = new URL(gateway);
	if (url.protocol === "http:") url.protocol = "ws:";
	if (url.protocol === "https:") url.protocol = "wss:";
	if (url.protocol !== "ws:" && url.protocol !== "wss:") throw new Error("--gateway must be a ws(s):// or http(s):// URL");
	if (url.pathname === "/" || url.pathname === "") url.pathname = "/v1/tunnel";
	return url.toString();
}

export async function runSpike(opts: SpikeOptions): Promise<SpikeReport> {
	const log = opts.log ?? (() => {});
	const url = tunnelUrl(opts.gateway);
	log(`building the test torrent (seed ${opts.seed}, ${opts.size} bytes)…`);
	const torrent = makeTorrent({ seed: opts.seed, size: opts.size, pieceLength: opts.pieceLength });
	const meta = await parseInfoDict(torrent.info, torrent.infoHashHex);
	const report: SpikeReport = {
		version: 1,
		startedAt: new Date().toISOString(),
		params: { bytes: opts.bytes, torrentSize: opts.size, pieceLength: opts.pieceLength, seed: opts.seed, connections: opts.connections },
		download: { firstPieceMs: 0, bytes: 0, ms: 0, mibPerSecond: 0, piecesVerified: 0, piecesCorrupt: 0, piecesMissing: 0, rounds: 0 },
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

	try {
		// Throughput: `connections` connections on one WebSocket, pieces split between them.
		log(`downloading ${opts.bytes} bytes through ${new URL(url).host}…`);
		const t0 = Date.now();
		const client = await connect(Math.max(6, opts.connections));
		const conns = await Promise.all(
			Array.from({ length: opts.connections }, async () => new PeerConnection(await client.open({ address: opts.peer, infoHash: torrent.infoHash, peerId: peerId() }), meta)),
		);
		const started = Date.now();
		let remaining = opts.bytes;
		while (remaining > 0) {
			const pieces = piecesFor(meta, remaining);
			const shares = conns.map((_, i) => pieces.filter((_, j) => j % conns.length === i));
			const onPiece = () => {
				if (report.download.firstPieceMs === 0) report.download.firstPieceMs = Date.now() - t0;
			};
			const results = await Promise.all(conns.map((c, i) => downloadAll(c, meta, { pieces: shares[i], onPiece })));
			for (const r of results) {
				report.download.piecesVerified += r.verified.length;
				report.download.piecesCorrupt += r.corrupt.length;
				report.download.piecesMissing += r.missing.length;
				report.download.bytes += r.verified.reduce((n, p) => n + pieceBytes(meta, p), 0);
			}
			report.download.rounds++;
			remaining -= pieces.reduce((n, p) => n + pieceBytes(meta, p), 0);
			log(`  round ${report.download.rounds}: ${(report.download.bytes / 2 ** 20).toFixed(1)} MiB verified`);
		}
		report.download.ms = Date.now() - started;
		report.download.mibPerSecond = round(report.download.bytes / 2 ** 20 / (report.download.ms / 1000));
		for (const c of conns) c.close();
		client.close();

		if (opts.probe) report.connections = await probe(await connect(opts.probeOpen), torrent, meta, opts, log);
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

/** 6 (+1) connections at once, then more after `laterMs`, each checked for real data. */
async function probe(client: TunnelClient, torrent: FakeTorrent, meta: Metainfo, opts: SpikeOptions, log: (l: string) => void) {
	log(`probing connection limits (${opts.probeOpen} at once, then later after ${opts.laterMs} ms)…`);
	const attempt = async (label: string): Promise<{ a: ConnectionAttempt; conn?: PeerConnection }> => {
		const started = Date.now();
		try {
			const transport = await client.open({ address: opts.peer, infoHash: torrent.infoHash, peerId: peerId(), timeoutMs: 15_000 });
			const a: ConnectionAttempt = { label, ok: true, openMs: Date.now() - started };
			const conn = new PeerConnection(transport, meta);
			const t0 = Date.now();
			try {
				await downloadAll(conn, meta, { pieces: [0], stallTimeoutMs: 10_000 });
				a.dataMs = Date.now() - t0;
			} catch (e) {
				a.dataError = (e as Error).message;
			}
			return { a, conn };
		} catch (e) {
			const reason = e instanceof TunnelOpenError ? (e.reason ? failReasonName(e.reason) : e.message) : (e as Error).message;
			return { a: { label, ok: false, openMs: Date.now() - started, reason } };
		}
	};

	const initial: ConnectionAttempt[] = [];
	const held: PeerConnection[] = [];
	for (let i = 1; i <= opts.probeOpen; i++) {
		const { a, conn } = await attempt(`initial-${i}`);
		initial.push(a);
		if (conn) held.push(conn);
	}
	await new Promise((r) => setTimeout(r, opts.laterMs));
	// Free two slots, then open two (and one more past the limit) later in the socket's life.
	held.splice(0, 2).forEach((c) => c.close());
	await new Promise((r) => setTimeout(r, 1_500));
	const later: ConnectionAttempt[] = [];
	for (let i = 1; i <= 3; i++) {
		const { a, conn } = await attempt(`later-${i}`);
		later.push(a);
		if (conn) held.push(conn);
	}
	held.forEach((c) => c.close());
	const seventh = initial[6];
	return {
		initial,
		later,
		laterAfterMs: opts.laterMs,
		seventh: !seventh ? "not tried" : seventh.ok ? (seventh.dataMs !== undefined ? `opened, data in ${seventh.dataMs} ms` : `opened, no data (${seventh.dataError})`) : `failed: ${seventh.reason}`,
	};
}

/** The first pieces covering `bytes` (the whole torrent at most). */
export function piecesFor(meta: Metainfo, bytes: number): number[] {
	const out: number[] = [];
	let sum = 0;
	for (let p = 0; p < meta.pieceCount && sum < bytes; p++) {
		out.push(p);
		sum += pieceBytes(meta, p);
	}
	return out;
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
