// Swarms with few seeders (M4-06): the escalation state machine on a
// simulated clock, and the connection manager against a scripted gateway
// (no network): a healthy swarm never opens the VPS tunnel, few peers or a
// rate under the bitrate do, a choked seed is waited for, and without a
// VPS gateway nothing escalates.

import { afterEach, describe, expect, it } from "vitest";
import { AnnounceStatus, SUBPROTOCOL, decodeMessage, encodeFrames, formatAddress, type Frame } from "@magnet-stream/tunnel-protocol";
import { makeTorrent, type FakeTorrent } from "@magnet-stream/fake-peer";
import { ConnectionManager, ESCALATION_DEFAULTS, Escalation, escalationConfig, parseInfoDict, type EscalationInput, type Metainfo, type WebSocketLike } from "../src/index.ts";

describe("escalation state machine (simulated clock)", () => {
	const base: EscalationInput = { now: 0, active: true, delivering: 3, scoutExhausted: true, rate: 2_000_000, bitrate: 1_000_000 };
	/** Feed one input per second from `from` to `to` (inclusive), returning the first escalation and when the warning first showed. */
	function run(e: Escalation, from: number, to: number, input: Partial<EscalationInput>) {
		let escalated: { at: number; reason: string } | undefined;
		let warnedAt: number | undefined;
		for (let t = from; t <= to; t += 1_000) {
			const v = e.update({ ...base, ...input, now: t });
			if (v.escalate && !escalated) escalated = { at: t, reason: v.escalate };
			if (v.belowBitrate && warnedAt === undefined) warnedAt = t;
		}
		return { escalated, warnedAt };
	}

	it("has demanding defaults: 2 peers, 90 s to escalate, 30 s to warn, 10 min healthy to withdraw", () => {
		expect(ESCALATION_DEFAULTS).toEqual({ minDelivering: 2, escalateAfterMs: 90_000, warnAfterMs: 30_000, withdrawAfterMs: 600_000 });
		expect(new Escalation().config).toEqual(ESCALATION_DEFAULTS);
	});

	it("never escalates nor warns on a healthy swarm", () => {
		const e = new Escalation();
		const r = run(e, 0, 3_600_000, {});
		expect(r.escalated).toBeUndefined();
		expect(r.warnedAt).toBeUndefined();
		expect(e.reason).toBeUndefined();
	});

	it("escalates after 90 s in a row with fewer than 2 peers delivering, once", () => {
		const e = new Escalation();
		const r = run(e, 0, 300_000, { delivering: 1 });
		expect(r.escalated).toEqual({ at: 90_000, reason: "few_peers" });
		expect(e.reason).toBe("few_peers");
		// Never again.
		expect(run(e, 301_000, 600_000, { delivering: 0 }).escalated).toBeUndefined();
	});

	it("counts few peers only once the scout's live candidates are used up", () => {
		const e = new Escalation();
		expect(run(e, 0, 200_000, { delivering: 0, scoutExhausted: false }).escalated).toBeUndefined();
		const r = run(e, 201_000, 400_000, { delivering: 0, scoutExhausted: true });
		expect(r.escalated).toEqual({ at: 291_000, reason: "few_peers" });
	});

	it("restarts the clock when the streak breaks", () => {
		const e = new Escalation();
		expect(run(e, 0, 89_000, { delivering: 1 }).escalated).toBeUndefined();
		run(e, 90_000, 90_000, { delivering: 2 });
		const r = run(e, 91_000, 200_000, { delivering: 1 });
		expect(r.escalated?.at).toBe(181_000);
	});

	it("warns after 30 s under the bitrate and escalates after 90 s; the warning clears when the rate recovers", () => {
		const e = new Escalation();
		const r = run(e, 0, 120_000, { rate: 500_000 });
		expect(r.warnedAt).toBe(30_000);
		expect(r.escalated).toEqual({ at: 90_000, reason: "slow" });
		expect(e.update({ ...base, now: 121_000 }).belowBitrate).toBe(false);
	});

	it("never judges the rate while the bitrate is unknown", () => {
		const e = new Escalation();
		const r = run(e, 0, 600_000, { rate: 1, bitrate: 0 });
		expect(r).toEqual({ escalated: undefined, warnedAt: undefined });
	});

	it("judges nothing while no data is wanted, and starts over afterwards", () => {
		const e = new Escalation();
		run(e, 0, 80_000, { delivering: 0, rate: 0 });
		expect(e.update({ ...base, now: 81_000, active: false, delivering: 0, rate: 0 })).toEqual({ belowBitrate: false, escalate: undefined, withdraw: false });
		const r = run(e, 82_000, 200_000, { delivering: 0, rate: 0 });
		expect(r.escalated?.at).toBe(172_000);
		expect(r.warnedAt).toBe(112_000);
	});

	it("takes configured thresholds, and ignores nonsense", () => {
		const e = new Escalation({ minDelivering: 3, escalateAfterMs: 10_000, warnAfterMs: 2_000 });
		const r = run(e, 0, 60_000, { delivering: 2 });
		expect(r.escalated).toEqual({ at: 10_000, reason: "few_peers" });
		expect(escalationConfig({ minDelivering: Number.NaN, escalateAfterMs: -5, warnAfterMs: Infinity, withdrawAfterMs: Number.NaN })).toEqual({
			minDelivering: 2,
			escalateAfterMs: 0,
			warnAfterMs: 30_000,
			withdrawAfterMs: 600_000,
		});
	});

	/** Feed one input per second, returning the times of the withdraws and escalations. */
	function events(e: Escalation, from: number, to: number, input: Partial<EscalationInput>) {
		const withdrawn: number[] = [];
		const escalated: number[] = [];
		for (let t = from; t <= to; t += 1_000) {
			const v = e.update({ ...base, ...input, now: t });
			if (v.withdraw) withdrawn.push(t);
			if (v.escalate) escalated.push(t);
		}
		return { withdrawn, escalated };
	}

	it("withdraws once the session's own peers have been enough for 10 min, then may escalate again", () => {
		const e = new Escalation();
		run(e, 0, 90_000, { delivering: 1, ownDelivering: 1 });
		expect(e.escalated).toBe(true);
		// The escalation's peers make the swarm look healthy: not a reason to withdraw.
		expect(events(e, 91_000, 2_000_000, { delivering: 4, ownDelivering: 1 }).withdrawn).toEqual([]);
		// The session's own peers suffice: 10 min in a row, a streak broken once.
		expect(events(e, 2_001_000, 2_300_000, { delivering: 4, ownDelivering: 2 }).withdrawn).toEqual([]);
		events(e, 2_301_000, 2_301_000, { delivering: 4, ownDelivering: 1 });
		const w = events(e, 2_302_000, 3_000_000, { delivering: 4, ownDelivering: 2 });
		expect(w.withdrawn).toEqual([2_902_000]);
		expect(e.escalated).toBe(false);
		expect(e.reason).toBe("few_peers");
		expect(e.withdrawals).toBe(1);
		// Thin again: 90 s later it escalates again.
		const again = events(e, 3_001_000, 3_200_000, { delivering: 1, ownDelivering: 1 });
		expect(again.escalated).toEqual([3_091_000]);
		expect(again.withdrawn).toEqual([]);
	});

	it("with a known bitrate, withdraws only when the session's own peers are fast enough", () => {
		const e = new Escalation({ withdrawAfterMs: 60_000 });
		run(e, 0, 90_000, { rate: 500_000 });
		expect(e.reason).toBe("slow");
		expect(events(e, 91_000, 400_000, { rate: 2_000_000, ownRate: 900_000 }).withdrawn).toEqual([]);
		expect(events(e, 401_000, 500_000, { rate: 2_000_000, ownRate: 1_100_000 }).withdrawn).toEqual([461_000]);
	});

	it("does not count healthy time while no data is wanted", () => {
		const e = new Escalation({ withdrawAfterMs: 60_000 });
		run(e, 0, 90_000, { delivering: 0 });
		events(e, 91_000, 140_000, {});
		e.update({ ...base, now: 141_000, active: false });
		expect(events(e, 142_000, 201_000, {}).withdrawn).toEqual([]);
		expect(events(e, 202_000, 202_000, {}).withdrawn).toEqual([202_000]);
	});
});

// ---------------------------------------------------------------------------
// A scripted gateway: tunnels to "peers" that live in the test.

interface PeerScript {
	/** Unchoke this long after connecting (default 0); Infinity: never. */
	unchokeAfterMs?: number;
	/** Each requested block is answered this long after the previous one (default 5 ms). */
	gapMs?: number;
}

class FakeSocket implements WebSocketLike {
	binaryType = "blob";
	readyState = 0;
	protocol = "";
	sent: Frame[] = [];
	private listeners: Record<string, ((e: { data?: unknown; code?: number }) => void)[]> = {};
	onFrames: ((frames: Frame[]) => void) | undefined;
	readonly url: string;
	constructor(url: string) {
		this.url = url;
	}
	addEventListener(type: string, l: (e: { data?: unknown; code?: number }) => void) {
		(this.listeners[type] ??= []).push(l);
	}
	emit(type: string, e: { data?: unknown; code?: number } = {}) {
		for (const l of this.listeners[type] ?? []) l(e);
	}
	send(data: Uint8Array) {
		const r = decodeMessage(data);
		if (!r.ok) throw new Error("garbage");
		this.sent.push(...r.frames);
		this.onFrames?.(r.frames);
	}
	close(code = 1000) {
		if (this.readyState === 3) return;
		this.readyState = 3;
		this.emit("close", { code });
	}
	deliver(...frames: Frame[]) {
		if (this.readyState !== 1) return;
		const bytes = encodeFrames(frames);
		this.emit("message", { data: bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) });
	}
}

const msg = (id: number, payload: Uint8Array = new Uint8Array(0)) => {
	const out = new Uint8Array(5 + payload.length);
	new DataView(out.buffer).setUint32(0, 1 + payload.length);
	out[4] = id;
	out.set(payload, 5);
	return out;
};

class FakeGateways {
	readonly sockets: FakeSocket[] = [];
	readonly peers = new Map<string, PeerScript>();
	/** Gateways (by host:port) that answer `announce` with this status (others: UNSUPPORTED). */
	readonly announceStatus = new Map<string, number>();
	readonly timers = new Set<ReturnType<typeof setTimeout>>();
	readonly torrent: FakeTorrent;
	constructor(torrent: FakeTorrent) {
		this.torrent = torrent;
	}

	factory = (url: string, protocol: string): WebSocketLike => {
		const ws = new FakeSocket(url);
		this.sockets.push(ws);
		ws.onFrames = (frames) => this.onFrames(ws, frames);
		queueMicrotask(() => {
			ws.readyState = 1;
			ws.protocol = protocol === SUBPROTOCOL ? SUBPROTOCOL : "";
			ws.emit("open");
		});
		return ws;
	};

	on(host: string): FakeSocket[] {
		return this.sockets.filter((s) => new URL(s.url).host === host);
	}

	private later(fn: () => void, ms: number): void {
		const t = setTimeout(() => {
			this.timers.delete(t);
			fn();
		}, ms);
		this.timers.add(t);
	}

	private onFrames(ws: FakeSocket, frames: Frame[]): void {
		for (const f of frames) {
			if (f.type === "open") {
				const script = this.peers.get(formatAddress(f.address));
				if (!script) {
					this.later(() => ws.deliver({ type: "failed", conn: f.conn, reason: 1 }), 1);
					continue;
				}
				this.later(() => this.connect(ws, f.conn, script), 1);
			} else if (f.type === "announce") {
				const status = this.announceStatus.get(new URL(ws.url).host) ?? AnnounceStatus.UNSUPPORTED;
				this.later(() => ws.deliver({ type: "announced", conn: 0, infoHash: f.infoHash, status }), 1);
			} else if (f.type === "data") {
				this.onData(ws, f.conn, f.payload);
			}
		}
	}

	private readonly conns = new Map<string, { buf: Uint8Array; script: PeerScript; unchoked: boolean; queue: number[][]; busy: boolean }>();

	/** A peer "connected" through `ws` as `conn` (outbound open, or inbound accept). */
	connect(ws: FakeSocket, conn: number, script: PeerScript, accept?: Frame): void {
		const key = `${this.sockets.indexOf(ws)}:${conn}`;
		const state = { buf: new Uint8Array(0), script, unchoked: false, queue: [] as number[][], busy: false };
		this.conns.set(key, state);
		const bits = new Uint8Array(Math.ceil(this.torrent.pieceCount / 8));
		for (let p = 0; p < this.torrent.pieceCount; p++) bits[p >> 3] |= 0x80 >> (p & 7);
		const first: Frame[] = accept ? [accept] : [{ type: "opened", conn, reserved: new Uint8Array(8), peerId: new Uint8Array(20).fill(7) }];
		ws.deliver(...first, { type: "data", conn, payload: msg(5, bits) });
		const after = script.unchokeAfterMs ?? 0;
		if (Number.isFinite(after))
			this.later(() => {
				state.unchoked = true;
				ws.deliver({ type: "data", conn, payload: msg(1) });
				this.pump(ws, conn, key);
			}, after);
	}

	private onData(ws: FakeSocket, conn: number, payload: Uint8Array): void {
		const key = `${this.sockets.indexOf(ws)}:${conn}`;
		const state = this.conns.get(key);
		if (!state) return;
		const buf = new Uint8Array(state.buf.length + payload.length);
		buf.set(state.buf);
		buf.set(payload, state.buf.length);
		let at = 0;
		while (buf.length - at >= 4) {
			const len = new DataView(buf.buffer, buf.byteOffset + at).getUint32(0);
			if (buf.length - at < 4 + len) break;
			if (len >= 13 && buf[at + 4] === 6) {
				const v = new DataView(buf.buffer, buf.byteOffset + at + 5);
				state.queue.push([v.getUint32(0), v.getUint32(4), v.getUint32(8)]);
			}
			at += 4 + len;
		}
		state.buf = buf.slice(at);
		this.pump(ws, conn, key);
	}

	private pump(ws: FakeSocket, conn: number, key: string): void {
		const state = this.conns.get(key);
		if (!state || !state.unchoked || state.busy || state.queue.length === 0) return;
		state.busy = true;
		this.later(() => {
			state.busy = false;
			const next = state.queue.shift();
			if (!next) return;
			const [piece, begin, length] = next;
			const start = piece * this.torrent.pieceLength + begin;
			const payload = new Uint8Array(8 + length);
			const v = new DataView(payload.buffer);
			v.setUint32(0, piece);
			v.setUint32(4, begin);
			payload.set(this.torrent.data.subarray(start, start + length), 8);
			ws.deliver({ type: "data", conn, payload: msg(7, payload) });
			this.pump(ws, conn, key);
		}, state.script.gapMs ?? 5);
	}

	stop(): void {
		for (const t of this.timers) clearTimeout(t);
		this.timers.clear();
	}
}

const PRIMARY = "ws://primary.test:8787";
const VPS = "ws://vps.test:8080";
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function until(pred: () => boolean, ms = 5_000): Promise<void> {
	const deadline = Date.now() + ms;
	while (!pred()) {
		if (Date.now() > deadline) throw new Error("timed out");
		await sleep(10);
	}
}

describe("connection manager escalation (scripted gateway)", () => {
	let world: FakeGateways | undefined;
	let mgr: ConnectionManager | undefined;
	afterEach(() => {
		mgr?.stop();
		world?.stop();
		mgr = undefined;
		world = undefined;
	});

	async function setup(opts: {
		peers: PeerScript[];
		escalation?: ConstructorParameters<typeof ConnectionManager>[0]["escalation"];
		size?: number;
		extra?: Partial<ConstructorParameters<typeof ConnectionManager>[0]>;
	}): Promise<{ meta: Metainfo; torrent: FakeTorrent; addresses: string[] }> {
		const torrent = makeTorrent({ seed: 606, size: opts.size ?? 1024 * 1024, pieceLength: 64 * 1024 });
		const meta = await parseInfoDict(torrent.info, torrent.infoHashHex);
		world = new FakeGateways(torrent);
		const addresses = opts.peers.map((script, i) => {
			const addr = `127.0.0.1:${20_000 + i}`;
			world!.peers.set(addr, script);
			return addr;
		});
		mgr = new ConnectionManager({
			gatewayUrl: PRIMARY,
			infoHash: torrent.infoHash,
			allowPrivate: true,
			factory: world.factory,
			sweepMs: 20,
			batchMs: 5,
			escalation: opts.escalation,
			...opts.extra,
		});
		mgr.setInfo(meta);
		mgr.addPeers(addresses);
		return { meta, torrent, addresses };
	}

	it("never opens the VPS tunnel on a healthy swarm", async () => {
		const { meta } = await setup({
			peers: [{ gapMs: 25 }, { gapMs: 25 }, { gapMs: 25 }],
			size: 4 * 1024 * 1024,
			escalation: { gatewayUrl: VPS, escalateAfterMs: 300, warnAfterMs: 100 },
		});
		mgr!.setBitrate(10_000);
		const started = Date.now();
		const rec = mgr!.recover({ offset: 0, size: meta.totalLength });
		expect(await rec.done).toBe(true);
		expect(Date.now() - started).toBeGreaterThan(900);
		expect(world!.on("vps.test:8080")).toHaveLength(0);
		expect(world!.on("vps.test:8080")).toHaveLength(0);
		expect(mgr!.swarmHealth()).toMatchObject({ escalation: "idle", reason: null, belowBitrate: false });
		expect(mgr!.diagnostics().escalation).toMatchObject({ state: "idle", upgrades: 0, afterMs: null });
	});

	it("with fewer than 2 peers delivering, opens the VPS tunnel and announces the info-hash and the magnet's trackers only", async () => {
		const trackers = ["udp://tracker.test:1337/announce", "https://t.test/announce"];
		const piecesWeHave = new Set([0]);
		const { meta, torrent } = await setup({
			peers: [{ gapMs: 40 }],
			escalation: { gatewayUrl: VPS, trackers, escalateAfterMs: 300 },
			extra: { upload: { source: { has: (p) => piecesWeHave.has(p), pieces: () => piecesWeHave, read: () => new Uint8Array(0) } } },
		});
		world!.announceStatus.set("vps.test:8080", AnnounceStatus.ACTIVE);
		const rec = mgr!.recover({ offset: 0, size: meta.totalLength });
		await until(() => mgr!.swarmHealth().escalation === "joined");
		const vps = world!.on("vps.test:8080");
		expect(vps).toHaveLength(1);
		expect(new URL(vps[0].url).pathname).toBe("/v1/tunnel");
		const announce = vps[0].sent.find((f) => f.type === "announce");
		expect(announce).toMatchObject({ type: "announce", conn: 0, infoHash: torrent.infoHash, trackers });
		// Nothing else went to the VPS: no dial, no data.
		expect(vps[0].sent.every((f) => f.type === "announce" || f.type === "ping")).toBe(true);
		expect(mgr!.swarmHealth()).toMatchObject({ escalation: "joined", reason: "few_peers" });
		expect(mgr!.diagnostics().escalation).toMatchObject({ state: "joined", reason: "few_peers", upgrades: 1 });

		// An inbound peer: a connection like the others (picker, choker and upload).
		const accept: Frame = { type: "accept", conn: 0x8000, infoHash: torrent.infoHash, reserved: new Uint8Array(8), peerId: new Uint8Array(20).fill(3), address: { family: 4, ip: Uint8Array.of(127, 0, 0, 1), port: 40_000 } };
		world!.connect(vps[0], 0x8000, { gapMs: 2 }, accept);
		await until(() => mgr!.swarmHealth().inbound === 1);
		expect(await rec.done).toBe(true);
		const toInbound = vps[0].sent.filter((f) => f.type === "data" && f.conn === 0x8000);
		const ids = toInbound.flatMap((f) => (f.type === "data" ? [...splitIds(f.payload)] : []));
		expect(ids).toContain(5); // our bitfield: it is offered pieces like any peer
		expect(ids).toContain(6); // and asked for blocks
		// Bytes by path (M4-08): the dialled seed through Cloudflare, the inbound peer through the VPS.
		const paths = mgr!.diagnostics().paths;
		expect(paths.cloudflare.connections).toBe(1);
		expect(paths.vps.connections).toBe(1);
		expect(paths.cloudflare.downloaded).toBeGreaterThan(0);
		expect(paths.vps.downloaded).toBeGreaterThan(0);
		expect(paths.cloudflare.downloaded + paths.vps.downloaded).toBe(mgr!.downloadedBytes);
	});

	it("counts the primary gateway's bytes as the VPS's when it is the VPS", async () => {
		const { meta } = await setup({ peers: [{ gapMs: 1 }], extra: { gatewayPath: "vps" } });
		expect(await mgr!.recover({ offset: 0, size: meta.totalLength }).done).toBe(true);
		const paths = mgr!.diagnostics().paths;
		expect(paths.vps).toMatchObject({ downloaded: mgr!.downloadedBytes, connections: 1 });
		expect(paths.cloudflare).toEqual({ downloaded: 0, uploaded: 0, connections: 0 });
	});

	it("does not count few peers before the scout's stream is over", async () => {
		await setup({ peers: [{ gapMs: 40 }], escalation: { gatewayUrl: VPS, escalateAfterMs: 200 } });
		world!.announceStatus.set("vps.test:8080", AnnounceStatus.ACTIVE);
		mgr!.setDiscovering(true);
		const meta = await parseInfoDict(world!.torrent.info, world!.torrent.infoHashHex);
		mgr!.recover({ offset: 0, size: meta.totalLength });
		await sleep(600);
		expect(world!.on("vps.test:8080")).toHaveLength(0);
		mgr!.setDiscovering(false);
		await until(() => world!.on("vps.test:8080").length === 1);
	});

	it("escalates when the rate stays under the bitrate, and the page hears it is under", async () => {
		const { meta } = await setup({
			peers: [{ gapMs: 20 }, { gapMs: 20 }, { gapMs: 20 }],
			size: 4 * 1024 * 1024,
			escalation: { gatewayUrl: VPS, escalateAfterMs: 500, warnAfterMs: 150 },
		});
		mgr!.setBitrate(500_000_000);
		mgr!.recover({ offset: 0, size: meta.totalLength });
		await until(() => mgr!.swarmHealth().belowBitrate);
		await until(() => mgr!.swarmHealth().reason !== null);
		expect(mgr!.swarmHealth().reason).toBe("slow");
		await until(() => world!.on("vps.test:8080").length === 1);
		// UNSUPPORTED: the tunnel is closed again, and the session says so.
		await until(() => mgr!.swarmHealth().escalation === "unavailable");
		expect(world!.on("vps.test:8080")[0].readyState).toBe(3);
	});

	it("without a VPS gateway, never escalates and raises no error", async () => {
		for (const gatewayUrl of [undefined, "", "ftp://nope", "not a url"]) {
			const { meta } = await setup({ peers: [{ gapMs: 30 }], escalation: { gatewayUrl, escalateAfterMs: 100, warnAfterMs: 50 } });
			mgr!.setBitrate(500_000_000);
			mgr!.recover({ offset: 0, size: meta.totalLength });
			await until(() => mgr!.swarmHealth().belowBitrate);
			await sleep(300);
			expect(mgr!.swarmHealth()).toMatchObject({ escalation: "off" });
			expect(world!.sockets.every((s) => s.url.startsWith(PRIMARY))).toBe(true);
			mgr!.stop();
			world!.stop();
		}
	}, 20_000);

	it("withdraws the announce once its own peers are enough again, and closes the idle VPS tunnel", async () => {
		const { meta } = await setup({
			peers: [{ gapMs: 20 }, { gapMs: 20 }, { gapMs: 20 }],
			size: 8 * 1024 * 1024,
			escalation: { gatewayUrl: VPS, escalateAfterMs: 300, withdrawAfterMs: 400 },
		});
		world!.announceStatus.set("vps.test:8080", AnnounceStatus.ACTIVE);
		// Under the bitrate: escalates ("slow")...
		mgr!.setBitrate(500_000_000);
		const rec = mgr!.recover({ offset: 0, size: meta.totalLength });
		await until(() => mgr!.swarmHealth().escalation === "joined");
		const vps = world!.on("vps.test:8080")[0];
		expect(vps.sent.some((f) => f.type === "withdraw")).toBe(false);
		// ...then the session's own peers are fast enough (a lower bitrate): withdraws after 400 ms.
		const healthyAt = Date.now();
		mgr!.setBitrate(10_000);
		await until(() => mgr!.swarmHealth().escalation === "left");
		expect(Date.now() - healthyAt).toBeGreaterThanOrEqual(350);
		// No peer on it: the VPS WebSocket is closed, which ends the announce on the gateway.
		await until(() => vps.readyState === 3);
		expect(mgr!.diagnostics().escalation).toMatchObject({ state: "left", reason: "slow", withdrawals: 1 });
		// Slow again: a new escalation, a new VPS WebSocket.
		mgr!.setBitrate(500_000_000);
		await until(() => mgr!.swarmHealth().escalation === "joined");
		expect(world!.on("vps.test:8080")).toHaveLength(2);
		rec.stop();
	});

	it("withdraws on a VPS tunnel that still carries an inbound peer, which stays", async () => {
		const { meta, torrent } = await setup({
			peers: [{ gapMs: 20 }, { gapMs: 20 }, { gapMs: 20 }],
			size: 8 * 1024 * 1024,
			escalation: { gatewayUrl: VPS, escalateAfterMs: 300, withdrawAfterMs: 400 },
		});
		world!.announceStatus.set("vps.test:8080", AnnounceStatus.ACTIVE);
		mgr!.setBitrate(500_000_000);
		const rec = mgr!.recover({ offset: 0, size: meta.totalLength });
		await until(() => mgr!.swarmHealth().escalation === "joined");
		const vps = world!.on("vps.test:8080")[0];
		const accept: Frame = { type: "accept", conn: 0x8000, infoHash: torrent.infoHash, reserved: new Uint8Array(8), peerId: new Uint8Array(20).fill(3), address: { family: 4, ip: Uint8Array.of(127, 0, 0, 1), port: 40_000 } };
		world!.connect(vps, 0x8000, { gapMs: 30 }, accept);
		await until(() => mgr!.swarmHealth().inbound === 1);
		// The inbound peer's bytes do not count: the three dialled seeds are enough on their own.
		mgr!.setBitrate(10_000);
		await until(() => mgr!.swarmHealth().escalation === "left");
		await until(() => vps.sent.some((f) => f.type === "withdraw"));
		expect(vps.sent.filter((f) => f.type === "withdraw")).toHaveLength(1);
		expect(vps.readyState).toBe(1);
		expect(mgr!.swarmHealth().inbound).toBe(1);
		rec.stop();
	});

	it("with the VPS as the primary gateway, announces on the session's own tunnel", async () => {
		const { meta } = await setup({ peers: [{ gapMs: 40 }], escalation: { gatewayUrl: `${PRIMARY}/v1/tunnel`, escalateAfterMs: 200 } });
		world!.announceStatus.set("primary.test:8787", AnnounceStatus.ACTIVE);
		mgr!.recover({ offset: 0, size: meta.totalLength });
		await until(() => mgr!.swarmHealth().escalation === "joined");
		expect(world!.sockets.every((s) => s.url.startsWith(PRIMARY))).toBe(true);
		expect(world!.sockets.some((s) => s.sent.some((f) => f.type === "announce"))).toBe(true);
	});

	it("waits for a choked seed past the choke grace instead of replacing it", async () => {
		// One socket, full: 5 seeds that never unchoke and one that unchokes late; a candidate waits.
		const late = 450;
		const { meta, addresses } = await setup({
			peers: [{ unchokeAfterMs: late, gapMs: 2 }, ...Array.from({ length: 5 }, () => ({ unchokeAfterMs: Infinity })), { gapMs: 2 }],
			extra: { maxSockets: 1, chokeReplaceAfterMs: 100, seedPatienceMs: 1_000, earlyWidth: false },
		});
		const rec = mgr!.recover({ offset: 0, size: meta.totalLength });
		expect(await rec.done).toBe(true);
		// The late seed stayed connected (never redialled) and delivered.
		const opens = world!.sockets.flatMap((s) => s.sent.filter((f) => f.type === "open" && formatAddress(f.address) === addresses[0]));
		expect(opens).toHaveLength(1);
		expect(mgr!.book.get(addresses[0])?.delivered ?? 0).toBeGreaterThan(0);
	});
});

function* splitIds(payload: Uint8Array): Generator<number> {
	let at = 0;
	while (payload.length - at >= 4) {
		const len = new DataView(payload.buffer, payload.byteOffset + at).getUint32(0);
		if (len > 0 && at + 4 < payload.length) yield payload[at + 4];
		at += 4 + len;
	}
}
