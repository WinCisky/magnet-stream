// Scout-guided dialling on a simulated clock (docs/architecture.md
// §Connection management, revision of 2026-11-19; OQ-03, OQ-21, OQ-23).
// A fake gateway answers `opened` at once for the addresses it calls alive
// and `failed` TIMEOUT after 5 s for the others, whose slot it keeps 20 s
// from the connect, as Cloudflare does. It records, per WebSocket, how many
// dead connects held a slot at once.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { FailReason, SUBPROTOCOL, decodeMessage, encodeFrames, formatAddress, type Frame } from "@magnet-stream/tunnel-protocol";
import { ConnectionManager, type ConnectionManagerOptions, type ScoutFacts, type WebSocketLike } from "../src/index.ts";

const HOLD_MS = 20_000;
type Fate = "ok" | "unchoke" | "refuse";

class FakeGateway implements WebSocketLike {
	binaryType = "blob";
	readyState = 0;
	protocol = "";
	opens: string[] = [];
	/** Until when each dead connect holds a slot. */
	private deadHolds: number[] = [];
	deadHighWater = 0;
	private listeners: Record<string, ((e: { data?: unknown; code?: number }) => void)[]> = {};
	/** Dead connects started after this time (ms), for the "carrying socket" check. */
	deadOpensAt: number[] = [];
	private readonly alive: Map<string, Fate>;
	constructor(alive: Map<string, Fate>) {
		this.alive = alive;
		setTimeout(() => {
			this.readyState = 1;
			this.protocol = SUBPROTOCOL;
			this.emit("open");
		}, 0);
	}
	addEventListener(type: string, l: (e: { data?: unknown; code?: number }) => void) {
		(this.listeners[type] ??= []).push(l);
	}
	private emit(type: string, e: { data?: unknown; code?: number } = {}) {
		for (const l of this.listeners[type] ?? []) l(e);
	}
	send(data: Uint8Array) {
		const r = decodeMessage(data);
		if (!r.ok) throw new Error("garbage");
		for (const f of r.frames) {
			if (f.type !== "open") continue;
			const address = formatAddress(f.address);
			this.opens.push(address);
			const conn = f.conn;
			const fate = this.alive.get(address);
			if (fate === "ok" || fate === "unchoke") {
				setTimeout(() => this.deliver({ type: "opened", conn, reserved: new Uint8Array(8), peerId: new Uint8Array(20).fill(7) }), 30);
				// An unchoke message: this socket carries data from then on.
				if (fate === "unchoke") setTimeout(() => this.deliver({ type: "data", conn, payload: Uint8Array.of(0, 0, 0, 1, 1) }), 60);
			} else if (fate === "refuse") {
				setTimeout(() => this.deliver({ type: "failed", conn, reason: FailReason.REFUSED }), 30);
			} else {
				const now = Date.now();
				this.deadOpensAt.push(now);
				this.deadHolds = this.deadHolds.filter((t) => t > now);
				this.deadHolds.push(now + HOLD_MS);
				this.deadHighWater = Math.max(this.deadHighWater, this.deadHolds.length);
				setTimeout(() => this.deliver({ type: "failed", conn, reason: FailReason.TIMEOUT }), 5_000);
			}
		}
	}
	close(code = 1000) {
		if (this.readyState === 3) return;
		this.readyState = 3;
		this.emit("close", { code });
	}
	private deliver(...frames: Frame[]) {
		if (this.readyState !== 1) return;
		const bytes = encodeFrames(frames);
		this.emit("message", { data: bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) });
	}
}

beforeEach(() => {
	vi.useFakeTimers();
});
afterEach(() => {
	vi.useRealTimers();
});

function addresses(n: number, base = 0): string[] {
	return Array.from({ length: n }, (_, i) => `8.8.${Math.floor((base + i) / 200)}.${((base + i) % 200) + 1}:6881`);
}

function manager(alive = new Map<string, Fate>(), options: Partial<ConnectionManagerOptions> = {}) {
	const sockets: FakeGateway[] = [];
	const mgr = new ConnectionManager({
		gatewayUrl: "http://gateway.test",
		infoHash: new Uint8Array(20).fill(1),
		...options,
		factory: () => {
			const ws = new FakeGateway(alive);
			sockets.push(ws);
			return ws;
		},
	});
	return { mgr, sockets };
}

async function start(mgr: ConnectionManager): Promise<void> {
	const connected = mgr.connect();
	await vi.advanceTimersByTimeAsync(10);
	await connected;
}

const ALIVE_SEED: ScoutFacts = { score: 0.9, alive: true, seed: true, have: 1, md: true, unchoked: true, rtt: 40 };
const ALIVE_PARTIAL: ScoutFacts = { score: 0.6, alive: true, seed: false, have: 0.4 };
const DEAD: ScoutFacts = { score: 0.02, alive: false };

describe("scout-guided dialling", () => {
	it("connects the 2 live peers within 3 s of their lines, behind 30 dead addresses", async () => {
		const live = ["9.9.9.1:6881", "9.9.9.2:6881"];
		const { mgr, sockets } = manager(new Map(live.map((a) => [a, "ok" as const])));
		await start(mgr);
		// 30 dead: half unknown to the scout (no facts), half seen dead.
		const dead = addresses(30);
		for (let i = 0; i < dead.length; i++) mgr.addPeers([dead[i]], "discovery", [i % 2 === 0 ? undefined : DEAD]);
		await vi.advanceTimersByTimeAsync(2_000);
		// Every slot an untried address may take is held by a dead connect.
		expect(mgr.liveCount).toBe(0);

		mgr.addPeers([live[0]], "discovery", [ALIVE_SEED]);
		mgr.addPeers([live[1]], "discovery", [ALIVE_PARTIAL]);
		const arrived = Date.now();
		while (!live.every((a) => mgr.book.get(a)?.state === "connected")) {
			expect(Date.now() - arrived).toBeLessThan(3_000);
			await vi.advanceTimersByTimeAsync(50);
		}
		expect(Date.now() - arrived).toBeLessThan(3_000);
		expect(mgr.liveCount).toBe(2);

		await vi.advanceTimersByTimeAsync(60_000);
		// Never more than 3 untried connects holding slots on one socket.
		for (const s of sockets) expect(s.deadHighWater).toBeLessThanOrEqual(3);
		expect(mgr.untestedHighWater).toBeLessThanOrEqual(3);
		const d = mgr.diagnostics();
		expect(d.dials.scout.attempted).toBe(2);
		expect(d.dials.scout.succeeded).toBe(2);
		expect(d.dials.untried.attempted).toBeGreaterThan(0);
		expect(d.dials.untried.failed.timeout).toBe(d.dials.untried.attempted - (d.dials.untried.failed.busy ?? 0) - inFlight(mgr));
		expect(d.connected).toBe(2);
		mgr.stop();
	});

	it("lets scout-alive addresses take every slot of a socket, untried ones at most 3", async () => {
		const live = addresses(6, 100);
		const { mgr, sockets } = manager(new Map(live.map((a) => [a, "ok" as const])));
		const connected = mgr.connect();
		mgr.addPeers(addresses(10), "discovery");
		mgr.addPeers(live, "discovery", live.map(() => ALIVE_SEED));
		await vi.advanceTimersByTimeAsync(10);
		await connected;
		await vi.advanceTimersByTimeAsync(1_000);
		// The 6 live ones, best first, fill the first socket; untried ones never pass 3 per socket.
		expect(live.every((a) => mgr.book.get(a)?.state === "connected")).toBe(true);
		expect(sockets[0].opens.slice(0, 6).sort()).toEqual([...live].sort());
		await vi.advanceTimersByTimeAsync(45_000);
		for (const s of sockets) expect(s.deadHighWater).toBeLessThanOrEqual(3);
		mgr.stop();
	});

	it("opens the second and third WebSocket at once while nothing is delivered and candidates exceed the free slots", async () => {
		const { mgr, sockets } = manager();
		mgr.addPeers(addresses(30), "discovery");
		await start(mgr);
		await vi.advanceTimersByTimeAsync(200);
		expect(mgr.socketsOpen).toBe(3);
		expect(mgr.upgrades).toBe(3);
		// Each socket took its share of untried addresses at once (one batched message each).
		await vi.advanceTimersByTimeAsync(500);
		expect(sockets.map((s) => s.opens.length)).toEqual([3, 3, 3]);
		await vi.advanceTimersByTimeAsync(60_000);
		expect(mgr.socketsOpen).toBe(3);
		for (const s of sockets) expect(s.deadHighWater).toBeLessThanOrEqual(3);
		mgr.stop();
	});

	it("stays at one WebSocket when the candidates fit its free slots, or there are none", async () => {
		const none = manager();
		await start(none.mgr);
		await vi.advanceTimersByTimeAsync(30_000);
		expect(none.mgr.socketsOpen).toBe(1);
		none.mgr.stop();

		const few = manager(new Map(addresses(2, 50).map((a) => [a, "ok" as const])));
		few.mgr.addPeers(addresses(3), "discovery");
		few.mgr.addPeers(addresses(2, 50), "discovery", [ALIVE_SEED, ALIVE_SEED]);
		await start(few.mgr);
		await vi.advanceTimersByTimeAsync(1_000);
		expect(few.mgr.socketsOpen).toBe(1);
		few.mgr.stop();
	});

	it("treats a scout-alive address that fails through the tunnel as untried, with the normal back-off", async () => {
		const addr = "9.9.9.9:6881";
		const { mgr } = manager(new Map([[addr, "refuse" as const]]));
		mgr.addPeers([addr], "discovery", [ALIVE_SEED]);
		expect(mgr.book.dialClass(mgr.book.get(addr)!)).toBe("scout");
		await start(mgr);
		await vi.advanceTimersByTimeAsync(1_000);
		const entry = mgr.book.get(addr)!;
		expect(entry.state).toBe("failed");
		expect(mgr.book.dialClass(entry)).toBe("untried");
		expect(entry.retryAt).toBeGreaterThan(Date.now() + 60_000);
		expect(mgr.diagnostics().dials.scout.failed.refused).toBe(1);
		mgr.stop();
	});
});

describe("dead connects kept off the sockets that carry data", () => {
	it("sends untried dials only to sockets none of whose peers has unchoked us", async () => {
		const live = "9.9.9.1:6881";
		const { mgr, sockets } = manager(new Map([[live, "unchoke" as const]]));
		mgr.addPeers(addresses(60), "discovery");
		await start(mgr);
		await vi.advanceTimersByTimeAsync(2_000);
		expect(mgr.socketsOpen).toBe(3);
		mgr.addPeers([live], "discovery", [ALIVE_SEED]);
		await vi.advanceTimersByTimeAsync(1_000);
		expect(mgr.diagnostics().unchoked).toBe(1);
		const carrying = sockets.find((s) => s.opens.includes(live))!;
		const unchokedAt = Date.now();
		await vi.advanceTimersByTimeAsync(60_000);
		// The others went on exploring; the carrying one got no new dead connect.
		expect(carrying.deadOpensAt.filter((t) => t > unchokedAt)).toEqual([]);
		const others = sockets.filter((s) => s !== carrying);
		expect(others.some((s) => s.deadOpensAt.some((t) => t > unchokedAt))).toBe(true);
		mgr.stop();
	});

	it("replaces an exploring socket jammed by dead connects, within its limits", async () => {
		const { mgr, sockets } = manager(new Map(), { maxSockets: 1, maxRecycles: 2, recycleIntervalMs: 20_000 });
		mgr.addPeers(addresses(60), "discovery");
		await start(mgr);
		// The first 3 time out at 5 s and hold their slots to ~21 s: recycled at once.
		await vi.advanceTimersByTimeAsync(6_000);
		expect(mgr.diagnostics().recycles).toBe(1);
		expect(mgr.upgrades).toBe(2);
		expect(mgr.socketsOpen).toBe(1);
		expect(sockets[0].readyState).toBe(3);
		// Not before the interval, and never more than maxRecycles.
		await vi.advanceTimersByTimeAsync(10_000);
		expect(mgr.diagnostics().recycles).toBe(1);
		await vi.advanceTimersByTimeAsync(120_000);
		expect(mgr.diagnostics().recycles).toBe(2);
		expect(mgr.upgrades).toBe(3);
		for (const s of sockets) expect(s.deadHighWater).toBeLessThanOrEqual(3);
		mgr.stop();
	});

	it("does not recycle with recycling off", async () => {
		const { mgr } = manager(new Map(), { maxSockets: 1, maxRecycles: 0 });
		mgr.addPeers(addresses(60), "discovery");
		await start(mgr);
		await vi.advanceTimersByTimeAsync(60_000);
		expect(mgr.diagnostics().recycles).toBe(0);
		expect(mgr.upgrades).toBe(1);
		mgr.stop();
	});
});

/** Untried dials still waiting for an answer (not failed yet). */
describe("width that nothing uses is not paid for (M3-07: thin swarms)", () => {
	it("closes the surplus WebSockets left without any connection, keeping one", async () => {
		const { mgr, sockets } = manager(new Map(), { maxRecycles: 0 });
		mgr.addPeers(addresses(30), "discovery");
		await start(mgr);
		await vi.advanceTimersByTimeAsync(200);
		expect(mgr.socketsOpen).toBe(3);
		// Every address tried and backed off (5 min), held slots released, then IDLE_SOCKET_MS.
		await vi.advanceTimersByTimeAsync(160_000);
		expect(mgr.socketsOpen).toBe(1);
		expect(mgr.upgrades).toBe(3);
		expect(sockets.filter((s) => s.readyState === 3)).toHaveLength(2);
		// Nothing reopened them, and the last one stays.
		await vi.advanceTimersByTimeAsync(60_000);
		expect(mgr.socketsOpen).toBe(1);
		expect(mgr.upgrades).toBe(3);
		mgr.stop();
	});

	it("keeps a socket that carries a connection, however long", async () => {
		const live = addresses(2, 50);
		const { mgr } = manager(new Map(live.map((a) => [a, "unchoke" as const])));
		mgr.addPeers(addresses(30), "discovery");
		await start(mgr);
		await vi.advanceTimersByTimeAsync(200);
		mgr.addPeers(live, "discovery", [ALIVE_SEED, ALIVE_SEED]);
		await vi.advanceTimersByTimeAsync(300_000);
		expect(mgr.liveCount).toBe(2);
		expect(mgr.diagnostics().connected).toBe(2);
		expect(mgr.socketsOpen).toBeGreaterThanOrEqual(1);
		mgr.stop();
	});

	it("does not add a WebSocket for a low rate when nobody is left to dial", async () => {
		const live = "9.9.9.1:6881";
		const { mgr } = manager(new Map([[live, "unchoke" as const]]), { rateThreshold: 1e12, rateWindowMs: 1_000 });
		mgr.addPeers([live], "discovery", [ALIVE_SEED]);
		await start(mgr);
		await vi.advanceTimersByTimeAsync(120_000);
		expect(mgr.liveCount).toBe(1);
		expect(mgr.upgrades).toBe(1);
		expect(mgr.socketsOpen).toBe(1);
		mgr.stop();
	});

	it("still adds one for a low rate when candidates outnumber the free slots", async () => {
		const live = "9.9.9.1:6881";
		const { mgr } = manager(new Map([[live, "unchoke" as const]]), { rateThreshold: 1e12, rateWindowMs: 1_000, earlyWidth: false });
		mgr.addPeers([live], "discovery", [ALIVE_SEED]);
		mgr.addPeers(addresses(30), "discovery");
		await start(mgr);
		await vi.advanceTimersByTimeAsync(5_000);
		expect(mgr.upgrades).toBeGreaterThan(1);
		mgr.stop();
	});
});

function inFlight(mgr: ConnectionManager): number {
	let n = 0;
	for (const p of mgr.book.entries()) if (p.state === "connecting") n++;
	return n;
}
