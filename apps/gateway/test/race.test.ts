import { describe, expect, it } from "vitest";
import { Geometry, type BlocksRequest, type Candidate } from "../src/params";
import { MAX_OPEN, RaceLost, race, type Connector } from "../src/peer";
import { handshake, message, MSG } from "../src/wire";

const INFO_HASH = Uint8Array.from({ length: 20 }, (_, i) => i + 1);

const req: BlocksRequest = {
	infoHash: INFO_HASH,
	infoHashHex: "",
	geometry: new Geometry(32_768, 65_536),
	want: [{ piece: 0, first: 0, last: 1 }],
	candidates: [],
	waitMs: 2_000,
	holdMs: 2_000,
	staggerMs: 0,
};

/** How each fake peer behaves once connected to. */
type Kind =
	| { kind: "refuse"; afterMs?: number }
	| { kind: "dead" }
	| { kind: "unchoke"; afterMs: number }
	| { kind: "choke" };

function candidate(i: number): Candidate {
	return { addr: `10.0.0.${i}:6881`, hostname: `10.0.0.${i}`, port: 6881, exp: 0, token: "" };
}

/** Fake sockets; records how many are open at once and the connect order. */
function network(kinds: Kind[]) {
	const stats = { open: 0, peak: 0, connected: [] as number[] };
	const connect: Connector = (c) => {
		const i = Number(c.hostname.split(".")[3]);
		const kind = kinds[i];
		stats.connected.push(i);
		stats.open++;
		stats.peak = Math.max(stats.peak, stats.open);
		let closed = false;
		let resolveClosed!: () => void;
		const closedP = new Promise<void>((r) => (resolveClosed = r));
		let controller!: ReadableStreamDefaultController<Uint8Array>;
		const readable = new ReadableStream<Uint8Array>({ start: (c) => void (controller = c) });
		const doClose = () => {
			if (closed) return;
			closed = true;
			stats.open--;
			try {
				controller.close();
			} catch {}
			resolveClosed();
		};
		let opened: Promise<unknown>;
		if (kind.kind === "dead") opened = new Promise(() => {});
		else if (kind.kind === "refuse") {
			opened = new Promise((_, reject) =>
				setTimeout(() => {
					reject(new Error("refused"));
					doClose();
				}, kind.afterMs ?? 5),
			);
		} else {
			opened = Promise.resolve();
			setTimeout(() => {
				if (closed) return;
				controller.enqueue(handshake(INFO_HASH, new Uint8Array(20)));
				controller.enqueue(message(MSG.HAVE_ALL));
			}, 5);
			if (kind.kind === "unchoke") {
				setTimeout(() => !closed && controller.enqueue(message(MSG.UNCHOKE)), kind.afterMs);
			}
		}
		opened.catch(() => {});
		return {
			opened,
			closed: closedP,
			readable,
			writable: new WritableStream<Uint8Array>(),
			close: () => {
				doClose();
				return Promise.resolve();
			},
		} as unknown as Socket;
	};
	return { stats, connect };
}

function candidates(n: number): Candidate[] {
	return Array.from({ length: n }, (_, i) => candidate(i));
}

describe("race", () => {
	it("works through many dead candidates, never more than 6 at once", async () => {
		const kinds: Kind[] = Array.from({ length: 40 }, (_, i): Kind => (i < 25 ? { kind: "refuse", afterMs: 10 } : { kind: "dead" }));
		kinds[25] = { kind: "unchoke", afterMs: 20 };
		const { stats, connect } = network(kinds);
		const { winner, outcomes } = await race(req, candidates(40), { deadline: Date.now() + 2_000, holdMs: 2_000, connect });
		expect(winner.candidate.addr).toBe("10.0.0.25:6881");
		expect(stats.peak).toBe(MAX_OPEN);
		// Tried in order.
		expect(stats.connected.slice(0, 26)).toEqual([...Array(26).keys()]);
		const byPeer = Object.fromEntries(outcomes.map((o) => [o.peer, o.err]));
		expect(byPeer["10.0.0.0:6881"]).toBe("connect_failed");
		// Whoever never got a turn is reported as such.
		expect(Object.values(byPeer).filter((e) => e === "untried").length).toBe(40 - 31);
		winner.socket.close();
	});

	it("evicts choked peers after `hold` so waiting candidates get a turn", async () => {
		const kinds: Kind[] = [
			...Array.from({ length: 6 }, (): Kind => ({ kind: "choke" })),
			{ kind: "unchoke", afterMs: 10 },
		];
		const { stats, connect } = network(kinds);
		const started = Date.now();
		const { winner, outcomes } = await race(req, candidates(7), { deadline: Date.now() + 2_000, holdMs: 100, connect });
		expect(winner.candidate.addr).toBe("10.0.0.6:6881");
		expect(Date.now() - started).toBeLessThan(1_000);
		expect(stats.peak).toBe(MAX_OPEN);
		// One waiting candidate: exactly one peer made room for it.
		expect(outcomes.filter((o) => o.err === "evicted")).toHaveLength(1);
		expect(outcomes.every((o) => o.hs === 1)).toBe(true);
		winner.socket.close();
	});

	it("keeps choked peers until the deadline when nobody is waiting", async () => {
		const kinds: Kind[] = [{ kind: "choke" }, { kind: "unchoke", afterMs: 300 }];
		const { connect } = network(kinds);
		const { winner, outcomes } = await race(req, candidates(2), { deadline: Date.now() + 2_000, holdMs: 100, connect });
		expect(winner.candidate.addr).toBe("10.0.0.1:6881");
		expect(outcomes).toEqual([{ peer: "10.0.0.0:6881", err: "lost", hs: 1 }]);
		winner.socket.close();
	});

	it("with a stagger, tries the first candidate alone before the rest", async () => {
		// A fallback that unchokes faster doesn't beat a good first pick...
		let net = network([{ kind: "unchoke", afterMs: 100 }, { kind: "unchoke", afterMs: 10 }]);
		let r = await race(req, candidates(2), { deadline: Date.now() + 2_000, holdMs: 2_000, staggerMs: 300, connect: net.connect });
		expect(r.winner.candidate.addr).toBe("10.0.0.0:6881");
		expect(r.outcomes).toEqual([{ peer: "10.0.0.1:6881", err: "untried" }]);
		r.winner.socket.close();
		// ...but steps in when it's slow...
		net = network([{ kind: "unchoke", afterMs: 1_000 }, { kind: "unchoke", afterMs: 10 }]);
		r = await race(req, candidates(2), { deadline: Date.now() + 2_000, holdMs: 2_000, staggerMs: 300, connect: net.connect });
		expect(r.winner.candidate.addr).toBe("10.0.0.1:6881");
		r.winner.socket.close();
		// ...and at once when it fails.
		net = network([{ kind: "refuse" }, { kind: "unchoke", afterMs: 10 }]);
		const started = Date.now();
		r = await race(req, candidates(2), { deadline: Date.now() + 2_000, holdMs: 2_000, staggerMs: 1_000, connect: net.connect });
		expect(r.winner.candidate.addr).toBe("10.0.0.1:6881");
		expect(Date.now() - started).toBeLessThan(500);
		r.winner.socket.close();
	});

	it("rejects with every outcome when nobody unchokes in time", async () => {
		const kinds: Kind[] = [{ kind: "choke" }, { kind: "dead" }, { kind: "refuse" }];
		const { stats, connect } = network(kinds);
		const err = await race(req, candidates(3), { deadline: Date.now() + 300, holdMs: 300, connect }).catch((e) => e);
		expect(err).toBeInstanceOf(RaceLost);
		expect((err as RaceLost).outcomes.map((o) => o.err)).toEqual(["choked", "connect_timeout", "connect_failed"]);
		expect(stats.open).toBe(0);
	});
});
