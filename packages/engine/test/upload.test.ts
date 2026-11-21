// Reciprocation (M3-12): the choker on a simulated clock (regular slots,
// optimistic slot, seeds never), request validation before any read, the
// token bucket, the uploader's rate cap and its stop when the download is
// not active, the upload-off behaviour, and the lazy flush policy.

import { describe, expect, it } from "vitest";
import type { Frame } from "@magnet-stream/tunnel-protocol";
import { BufferedTransport } from "../src/transport.ts";
import { PeerConnection } from "../src/peer-connection.ts";
import { MSG } from "../src/peer-wire.ts";
import { UpstreamBatcher, mergeData } from "../src/upstream.ts";
import { Choker, TokenBucket, Uploader, checkRequest, type ChokerPeer, type PieceSource, type RequestContext, type UploadPeer } from "../src/upload.ts";

const BLOCK = 16 * 1024;

describe("choker", () => {
	const peer = (id: string, delivered: number, over: Partial<ChokerPeer> = {}): ChokerPeer => ({ id, interested: true, seed: false, connectedAt: 0, delivered, ...over });

	it("unchokes the 4 interested peers that delivered most, every 10 s, plus one optimistic", () => {
		const c = new Choker({ random: () => 0 });
		const peers = [peer("a", 50), peer("b", 40), peer("c", 30), peer("d", 20), peer("e", 10), peer("f", 0)];
		const first = c.tick(peers, 100_000);
		expect([...c.regular].sort()).toEqual(["a", "b", "c", "d"]);
		expect(first.size).toBe(5);
		expect(["e", "f"]).toContain(c.optimistic);
		// e overtakes a, but regular slots only move at the next 10 s mark.
		peers[4] = peer("e", 1000);
		c.tick(peers, 105_000);
		expect(c.regular.has("e")).toBe(false);
		c.tick(peers, 110_000);
		expect(c.regular.has("e")).toBe(true);
		expect(c.regular.has("d")).toBe(false);
	});

	it("never unchokes seeds or peers that are not interested, and needs delivered bytes for a regular slot", () => {
		const c = new Choker({ random: () => 0 });
		const out = c.tick([peer("seed", 999, { seed: true }), peer("bored", 999, { interested: false }), peer("new", 0)], 0);
		expect([...out]).toEqual(["new"]);
		expect(c.regular.size).toBe(0);
		expect(c.optimistic).toBe("new");
	});

	it("rotates the optimistic slot every 30 s, and favours new connections", () => {
		const peers = [peer("old", 0, { connectedAt: 0 }), peer("fresh", 0, { connectedAt: 200_000 })];
		// Weights: old 1, fresh 3. 0.2 of 4 falls in old's share, 0.5 in fresh's.
		const draws = [0.5, 0.2];
		const c = new Choker({ random: () => draws.shift() ?? 0 });
		c.tick(peers, 210_000);
		expect(c.optimistic).toBe("fresh");
		c.tick(peers, 239_999);
		expect(c.optimistic).toBe("fresh");
		c.tick(peers, 240_000);
		// Rotation excludes the current holder.
		expect(c.optimistic).toBe("old");
		// Its peer leaves: redrawn at once.
		c.tick([peers[1]], 241_000);
		expect(c.optimistic).toBe("fresh");
	});

	it("draws new connections three times as often (statistically)", () => {
		let seed = 1;
		const random = () => ((seed = (seed * 16807) % 2147483647) / 2147483647);
		const counts: Record<string, number> = { old: 0, fresh: 0 };
		for (let i = 0; i < 2000; i++) {
			const c = new Choker({ random });
			c.tick([peer("old", 0, { connectedAt: 0 }), peer("fresh", 0, { connectedAt: 100_000 })], 120_000);
			counts[c.optimistic!]++;
		}
		expect(counts.fresh / counts.old).toBeGreaterThan(2.4);
		expect(counts.fresh / counts.old).toBeLessThan(3.8);
	});
});

describe("request validation", () => {
	const geometry = { pieceCount: 4, pieceLength: 32 * 1024, totalLength: 100 * 1024 };
	const ctx = (over: Partial<RequestContext> = {}): RequestContext => ({
		choking: false,
		geometry,
		available: (p) => p !== 1,
		queued: 0,
		queuedBytes: 0,
		maxQueued: 250,
		maxQueuedBytes: 2 * 1024 * 1024,
		...over,
	});

	it("serves only valid blocks of pieces we have, to peers we unchoked, within the queue", () => {
		expect(checkRequest({ piece: 0, begin: 0, length: BLOCK }, ctx())).toBe("serve");
		expect(checkRequest({ piece: 3, begin: 0, length: 4 * 1024 }, ctx())).toBe("serve");
		expect(checkRequest({ piece: 0, begin: 0, length: BLOCK }, ctx({ choking: true }))).toBe("choked");
		expect(checkRequest({ piece: 0, begin: 0, length: BLOCK }, ctx({ geometry: undefined }))).toBe("no_metadata");
		expect(checkRequest({ piece: 4, begin: 0, length: BLOCK }, ctx())).toBe("bad_piece");
		expect(checkRequest({ piece: 0, begin: 0, length: BLOCK + 1 }, ctx())).toBe("too_large");
		expect(checkRequest({ piece: 0, begin: 0, length: 0 }, ctx())).toBe("too_large");
		expect(checkRequest({ piece: 0, begin: 24 * 1024, length: BLOCK }, ctx())).toBe("bad_range");
		// The last piece is 4 KiB.
		expect(checkRequest({ piece: 3, begin: 0, length: 8 * 1024 }, ctx())).toBe("bad_range");
		expect(checkRequest({ piece: 1, begin: 0, length: BLOCK }, ctx())).toBe("missing");
		expect(checkRequest({ piece: 0, begin: 0, length: BLOCK }, ctx({ queued: 250 }))).toBe("queue_full");
		expect(checkRequest({ piece: 0, begin: 0, length: BLOCK }, ctx({ queuedBytes: 2 * 1024 * 1024 - 1 }))).toBe("queue_full");
	});
});

describe("token bucket", () => {
	it("allows the rate plus one burst, and refills over time", () => {
		const b = new TokenBucket(1000, 500, 0);
		expect(b.take(500, 0)).toBe(true);
		expect(b.take(1, 0)).toBe(false);
		expect(b.take(100, 100)).toBe(true);
		expect(b.take(1, 100)).toBe(false);
		expect(b.available(10_000)).toBe(500);
	});
});

/** A connection as the uploader sees it, recording what it sends. */
class FakePeer implements UploadPeer {
	fast = true;
	closed = false;
	isSeed = false;
	peerInterested = true;
	amChoking = true;
	pieces = new Set<number>();
	sent: string[] = [];
	bytes = 0;
	has(piece: number) {
		return this.pieces.has(piece);
	}
	chokePeer() {
		this.amChoking = true;
		this.sent.push("choke");
	}
	unchokePeer() {
		this.amChoking = false;
		this.sent.push("unchoke");
	}
	sendHave(piece: number) {
		this.sent.push(`have ${piece}`);
	}
	sendPiece(piece: number, begin: number, data: Uint8Array) {
		this.bytes += data.length;
		this.sent.push(`piece ${piece}:${begin}`);
	}
	rejectRequest(piece: number, begin: number) {
		this.sent.push(`reject ${piece}:${begin}`);
	}
	sendMetadataPiece(piece: number) {
		this.sent.push(`metadata ${piece}`);
		return true;
	}
	rejectMetadata(piece: number) {
		this.sent.push(`metadata-reject ${piece}`);
	}
}

function memorySource(pieces: number[], size = 64 * 1024): PieceSource & { reads: number } {
	const src = {
		reads: 0,
		has: (p: number) => pieces.includes(p),
		pieces: () => pieces,
		read(p: number, begin: number, length: number) {
			src.reads++;
			if (!pieces.includes(p) || begin + length > size) throw new Error("not readable");
			return new Uint8Array(length).fill(p);
		},
	};
	return src;
}

const geometry = { pieceCount: 64, pieceLength: 64 * 1024, totalLength: 64 * 64 * 1024 };
const flush = () => new Promise((r) => setTimeout(r, 0));

function setup(options: ConstructorParameters<typeof Uploader>[0] = {}, have = [0, 1, 2, 3]) {
	let now = 1_000_000;
	const source = memorySource(have);
	const up = new Uploader({ source, random: () => 0, ...options }, () => now);
	up.setInfo(geometry, new Uint8Array(40_000).fill(7));
	const peer = new FakePeer();
	up.add("p", peer, now);
	return { up, peer, source, advance: (ms: number) => (now += ms), now: () => now };
}

describe("uploader", () => {
	it("announces what the source holds, unchokes an interested peer, and serves its requests", async () => {
		const t = setup();
		expect([...(t.up.bitfield() ?? [])].slice(0, 1)).toEqual([0xf0]);
		expect(t.up.metadataSize).toBe(40_000);
		t.up.tick(true, () => 0);
		expect(t.peer.sent).toEqual(["unchoke"]);
		t.up.onRequest("p", 0, 0, BLOCK);
		t.up.onRequest("p", 1, BLOCK, BLOCK);
		await flush();
		expect(t.peer.sent.slice(1)).toEqual(["piece 0:0", `piece 1:${BLOCK}`]);
		expect(t.up.stats.bytes).toBe(2 * BLOCK);
		// A new verified piece is announced to the peers that lack it.
		t.peer.pieces.add(9);
		t.source.has = (p) => p <= 3 || p === 8 || p === 9;
		expect(t.up.offer(8)).toBe(true);
		expect(t.up.offer(9)).toBe(true);
		expect(t.up.offer(8)).toBe(false);
		expect(t.peer.sent.at(-1)).toBe("have 8");
		expect(t.peer.sent).not.toContain("have 9");
	});

	it("rejects bad requests without ever reading the store", async () => {
		const t = setup();
		// Choked: rejected (fast), not read.
		t.up.onRequest("p", 0, 0, BLOCK);
		t.up.tick(true, () => 0);
		t.up.onRequest("p", 7, 0, BLOCK); // missing
		t.up.onRequest("p", 0, 60 * 1024, BLOCK); // past the piece
		t.up.onRequest("p", 0, 0, 32 * 1024); // too large
		t.up.onRequest("p", 99, 0, BLOCK); // no such piece
		await flush();
		expect(t.source.reads).toBe(0);
		expect(t.peer.sent.filter((m) => m.startsWith("reject"))).toHaveLength(5);
		expect(t.up.stats.refused).toEqual({ choked: 1, missing: 1, bad_range: 1, too_large: 1, bad_piece: 1 });
		// Without the fast extension: ignored, no reject.
		t.peer.fast = false;
		t.up.onRequest("p", 7, 0, BLOCK);
		expect(t.peer.sent.filter((m) => m.startsWith("reject"))).toHaveLength(5);
		expect(t.up.stats.ignored).toBe(1);
	});

	it("bounds each peer's queue: 250 requests and 2 MiB", () => {
		const t = setup({ rate: BLOCK, maxReads: 1 });
		t.up.tick(true, () => 0);
		for (let i = 0; i < 300; i++) t.up.onRequest("p", i % 4, (i % 4) * BLOCK, BLOCK);
		// Duplicates are dropped; distinct blocks: 16 of them here.
		expect(t.up.queuedFor("p")).toBeLessThanOrEqual(16);
		const u = setup({ rate: BLOCK, maxReads: 1, maxQueuedRequests: 10 }, Array.from({ length: 64 }, (_, i) => i));
		u.up.tick(true, () => 0);
		for (let p = 0; p < 64; p++) u.up.onRequest("p", p, 0, BLOCK);
		expect(u.up.queuedFor("p")).toBe(10);
		expect(u.up.stats.refused.queue_full).toBe(54);
		const v = setup({ rate: BLOCK, maxReads: 1, maxQueuedBytes: 4 * BLOCK }, Array.from({ length: 64 }, (_, i) => i));
		v.up.tick(true, () => 0);
		for (let p = 0; p < 64; p++) v.up.onRequest("p", p, 0, BLOCK);
		expect(v.up.queuedFor("p")).toBe(4);
	});

	it("never exceeds the configured rate, and stops when the download is not active", async () => {
		const rate = 256 * 1024;
		const t = setup({ rate }, Array.from({ length: 64 }, (_, i) => i));
		t.up.tick(true, () => 0);
		let sentAt0 = 0;
		for (let second = 0; second < 10; second++) {
			for (let step = 0; step < 4; step++) {
				// Keep the queue full.
				for (let p = 0; p < 64; p++) for (let b = 0; b < 4; b++) t.up.onRequest("p", p, b * BLOCK, BLOCK);
				t.advance(250);
				t.up.tick(true, () => 0);
				await flush();
			}
			if (second === 0) sentAt0 = t.peer.bytes;
		}
		const burst = rate / 2;
		expect(t.peer.bytes).toBeLessThanOrEqual(rate * 10 + burst);
		expect(t.peer.bytes).toBeGreaterThan(rate * 8);
		expect(sentAt0).toBeLessThanOrEqual(rate + burst);
		// The download completes (or the session idles): everyone is choked and nothing more is sent.
		t.up.tick(false, () => 0);
		expect(t.peer.amChoking).toBe(true);
		const before = t.peer.bytes;
		t.up.onRequest("p", 5, 0, BLOCK);
		t.advance(5_000);
		t.up.tick(false, () => 0);
		await flush();
		expect(t.peer.bytes).toBe(before);
		expect(t.up.unchoked).toBe(0);
	});

	it("with the setting off: no bitfield, no have, no unchoke, no piece; requests ignored", async () => {
		const t = setup({ enabled: false });
		expect(t.up.bitfield()).toBeUndefined();
		expect(t.up.metadataSize).toBeUndefined();
		t.up.tick(true, () => 0);
		t.up.onRequest("p", 0, 0, BLOCK);
		t.source.has = () => true;
		t.up.offer(5);
		t.up.onMetadataRequest("p", 0);
		await flush();
		expect(t.peer.sent).toEqual(["metadata-reject 0"]);
		expect(t.source.reads).toBe(0);
		// Turned on: the pieces are announced; turned off again: choked.
		t.up.setEnabled(true);
		expect(t.peer.sent.filter((m) => m.startsWith("have")).length).toBe(5);
		t.up.tick(true, () => 0);
		expect(t.peer.amChoking).toBe(false);
		t.up.setEnabled(false);
		expect(t.peer.amChoking).toBe(true);
	});

	it("without a source it never uploads, whatever the setting", () => {
		const up = new Uploader({ enabled: true });
		up.setInfo(geometry);
		const peer = new FakePeer();
		up.add("p", peer);
		up.tick(true, () => 0);
		expect(up.on).toBe(false);
		expect(up.bitfield()).toBeUndefined();
		expect(peer.sent).toEqual([]);
	});

	it("choking a fast peer rejects its queued requests; a cancel removes one", async () => {
		const t = setup({ rate: BLOCK, maxReads: 1 });
		t.up.tick(true, () => 0);
		t.up.onRequest("p", 0, 0, BLOCK);
		t.up.onRequest("p", 1, 0, BLOCK);
		t.up.onRequest("p", 2, 0, BLOCK);
		t.up.onCancel("p", 2, 0, BLOCK);
		expect(t.peer.sent).toContain("reject 2:0");
		t.up.tick(false, () => 0);
		await flush();
		expect(t.peer.sent.filter((m) => m.startsWith("piece"))).toEqual([]);
		expect(t.peer.sent).toContain("reject 0:0");
		expect(t.peer.sent).toContain("reject 1:0");
	});

	it("serves the verified info dict over ut_metadata", () => {
		const t = setup();
		t.up.onMetadataRequest("p", 0);
		t.up.onMetadataRequest("p", 2);
		t.up.onMetadataRequest("p", 3);
		expect(t.peer.sent).toEqual(["metadata 0", "metadata 2", "metadata-reject 3"]);
	});

	it("gives regular slots to the peers that delivered most over the last 20 s", () => {
		const t = setup({ regularSlots: 1 });
		const other = new FakePeer();
		t.up.add("q", other, t.now());
		const totals: Record<string, number> = { p: 0, q: 0 };
		for (let i = 0; i < 12; i++) {
			totals.q += 100;
			t.up.tick(true, (id) => totals[id]);
			t.advance(1_000);
		}
		t.up.tick(true, (id) => totals[id]);
		expect(other.amChoking).toBe(false);
		expect(t.up.unchoked).toBe(2);
	});
});

class RecordingTransport extends BufferedTransport {
	readonly remote = { reserved: Uint8Array.of(0, 0, 0, 0, 0, 0x10, 0, 0x04), peerId: new Uint8Array(20) };
	readonly local = { reserved: this.remote.reserved };
	sent: Uint8Array[] = [];
	lazy: Uint8Array[] = [];
	send(bytes: Uint8Array): void {
		this.sent.push(bytes);
	}
	sendLazy(bytes: Uint8Array): void {
		this.lazy.push(bytes);
	}
	close(): void {
		this.finish();
	}
}

describe("peer connection upload messages", () => {
	const g = { pieceCount: 10, pieceLength: 32 * 1024, totalLength: 320 * 1024 };

	it("opens with our bitfield (have-all when complete, have-none when empty) and sends upload traffic lazily", () => {
		const bits = Uint8Array.of(0b1010_0000, 0);
		const t = new RecordingTransport();
		const conn = new PeerConnection(t, { geometry: g, keepAliveMs: 0, bitfield: bits, metadataSize: 1234 });
		expect(t.sent[0][4]).toBe(MSG.BITFIELD);
		expect([...t.sent[0].subarray(5, 7)]).toEqual([...bits]);
		expect(new TextDecoder().decode(t.sent[0])).toContain("13:metadata_sizei1234e");
		conn.unchokePeer();
		conn.sendHave(3);
		conn.sendPiece(0, 0, new Uint8Array(BLOCK));
		conn.rejectRequest(1, 0, BLOCK);
		expect(t.sent).toHaveLength(1);
		expect(t.lazy.map((m) => m[4])).toEqual([MSG.UNCHOKE, MSG.HAVE, MSG.PIECE, MSG.REJECT]);
		expect(conn.uploaded).toBe(BLOCK);
		conn.close();

		const all = new RecordingTransport();
		new PeerConnection(all, { geometry: g, keepAliveMs: 0, bitfield: Uint8Array.of(0xff, 0xc0) }).close();
		expect(all.sent[0][4]).toBe(MSG.HAVE_ALL);
		const none = new RecordingTransport();
		new PeerConnection(none, { geometry: g, keepAliveMs: 0 }).close();
		expect(none.sent[0][4]).toBe(MSG.HAVE_NONE);
	});

	it("hands requests, cancels and interest to the uploader", async () => {
		const t = new RecordingTransport();
		const seen: string[] = [];
		const conn = new PeerConnection(t, {
			geometry: g,
			keepAliveMs: 0,
			events: {
				request: (p, b, l) => seen.push(`request ${p}:${b}:${l}`),
				cancelRequest: (p, b) => seen.push(`cancel ${p}:${b}`),
				interest: (on) => seen.push(`interest ${on}`),
			},
		});
		await Promise.resolve();
		const msg = (id: number, ...words: number[]) => {
			const out = new Uint8Array(5 + 4 * words.length);
			const v = new DataView(out.buffer);
			v.setUint32(0, 1 + 4 * words.length);
			out[4] = id;
			words.forEach((w, i) => v.setUint32(5 + 4 * i, w));
			return out;
		};
		(t as unknown as { deliver(b: Uint8Array): void }).deliver(msg(MSG.INTERESTED));
		(t as unknown as { deliver(b: Uint8Array): void }).deliver(msg(MSG.REQUEST, 1, 0, BLOCK));
		(t as unknown as { deliver(b: Uint8Array): void }).deliver(msg(MSG.CANCEL, 1, 0, BLOCK));
		expect(seen).toEqual(["interest true", `request 1:0:${BLOCK}`, "cancel 1:0"]);
		expect(conn.peerInterested).toBe(true);
		conn.close();
	});
});

describe("lazy flush policy", () => {
	function clock() {
		let now = 0;
		const timers: ({ fn: () => void; at: number } | undefined)[] = [];
		return {
			now: () => now,
			advance(ms: number) {
				const end = now + ms;
				for (;;) {
					const due = timers.map((t, i) => [t, i] as const).filter(([t]) => t && t.at <= end).sort(([a], [b]) => a!.at - b!.at)[0];
					if (!due) break;
					now = Math.max(now, due[0]!.at);
					timers[due[1]] = undefined;
					due[0]!.fn();
				}
				now = end;
			},
			setTimer(fn: () => void, ms: number) {
				timers.push({ fn, at: now + ms });
				return (timers.length - 1) as unknown as ReturnType<typeof setTimeout>;
			},
			clearTimer(id: ReturnType<typeof setTimeout>) {
				timers[id as unknown as number] = undefined;
			},
		};
	}
	const data = (conn: number, n: number): Frame => ({ type: "data", conn, payload: new Uint8Array(n).fill(conn) });

	it("lazy frames alone go at most once every 2 s; they ride on any message due sooner", () => {
		const c = clock();
		const sent: Frame[][] = [];
		const b = new UpstreamBatcher({ intervalMs: 400, lazyMs: 2_000, now: c.now, setTimer: c.setTimer, clearTimer: c.clearTimer, send: (f) => sent.push(f) });
		// Upload only, for 10 s, a frame every 100 ms: at most 5 messages.
		for (let i = 0; i < 100; i++) {
			b.enqueue([data(1, 10)], "lazy");
			c.advance(100);
		}
		expect(sent.length).toBeLessThanOrEqual(5);
		expect(sent.length).toBeGreaterThanOrEqual(4);
		expect(b.lazyMessages).toBe(sent.length);
		// A normal frame takes the waiting lazy ones along, within 400 ms.
		const before = sent.length;
		b.enqueue([data(2, 5)], "lazy");
		b.enqueue([{ type: "ping", conn: 0, payload: new Uint8Array(0) }]);
		c.advance(400);
		expect(sent.length).toBe(before + 1);
		expect(sent.at(-1)!.map((f) => f.type)).toEqual(["data", "ping"]);
		expect(b.lazyMessages).toBe(before);
	});

	it("merges each connection's data frames, in order, and splits past the frame cap", () => {
		const merged = mergeData([data(1, 2), data(2, 1), data(1, 3), { type: "close", conn: 2, reason: 0 }, data(2, 4)]);
		expect(merged.map((f) => [f.type, f.conn, f.type === "data" ? f.payload.length : 0])).toEqual([
			["data", 1, 5],
			["data", 2, 1],
			["close", 2, 0],
			["data", 2, 4],
		]);
		const sent: Frame[][] = [];
		const b = new UpstreamBatcher({ maxFrames: 3, send: (f) => sent.push(f) });
		b.enqueue([1, 2, 3, 4, 5].map((n) => ({ type: "close", conn: n, reason: 0 }) as Frame));
		b.flush();
		expect(sent.map((m) => m.length)).toEqual([3, 2]);
	});
});
