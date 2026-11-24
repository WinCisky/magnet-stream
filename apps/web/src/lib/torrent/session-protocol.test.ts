import { describe, expect, it } from "vitest";
import { MAX_PIECE, parsePageMessage, parseWorkerMessage } from "./session-protocol";

describe("session messages", () => {
	it("accepts a start and the recovery messages, and rejects anything else", () => {
		expect(parsePageMessage({ type: "start", magnet: "magnet:?xt=urn:btih:aa", gatewayUrl: "http://127.0.0.1:8787", seedersUrl: "http://127.0.0.1:8080" })?.type).toBe("start");
		expect(parsePageMessage({ type: "stop" })).toEqual({ type: "stop" });
		expect(parsePageMessage({ type: "idle" })).toEqual({ type: "idle" });
		expect(parsePageMessage({ type: "recover", id: 1, offset: 0, size: 100, position: 0, have: [0, 3], save: true })).toEqual({
			type: "recover",
			id: 1,
			offset: 0,
			size: 100,
			position: 0,
			have: [0, 3],
			save: true,
		});
		expect(parsePageMessage({ type: "recover", id: 1, offset: 0, size: 100, position: 0, have: [], save: "yes" })).toBeNull();
		expect(parsePageMessage({ type: "read", req: 4, piece: 9 })).toEqual({ type: "read", req: 4, piece: 9 });
		expect(parsePageMessage({ type: "read", req: 4, piece: -9 })).toBeNull();
		expect(parsePageMessage({ type: "release", req: 2 })).toEqual({ type: "release", req: 2 });
		expect(parsePageMessage({ type: "recover", id: 1, offset: -1, size: 100, position: 0, have: [] })).toBeNull();
		expect(parsePageMessage({ type: "recover", id: 1, offset: 0, size: 100, position: 0, have: [1.5] })).toBeNull();
		expect(parsePageMessage({ type: "recover", id: 1, offset: 0, size: 100, position: 0 })).toBeNull();
		expect(parsePageMessage({ type: "seek", id: 3, byte: 1 << 20 })).toEqual({ type: "seek", id: 3, byte: 1 << 20 });
		expect(parsePageMessage({ type: "position", id: 3, byte: 5 })).toEqual({ type: "position", id: 3, byte: 5 });
		expect(parsePageMessage({ type: "seek", id: 3, byte: -5 })).toBeNull();
		expect(parsePageMessage({ type: "seek", id: "3", byte: 5 })).toBeNull();
		expect(parsePageMessage({ type: "lost", id: 3, piece: 7 })).toEqual({ type: "lost", id: 3, piece: 7 });
		expect(parsePageMessage({ type: "end", id: 3 })).toEqual({ type: "end", id: 3 });
		expect(parsePageMessage({ type: "end", id: -1 })).toBeNull();
		expect(parsePageMessage({ type: "bitrate", bytesPerSecond: 412_345.5 })).toEqual({ type: "bitrate", bytesPerSecond: 412_345.5 });
		for (const bad of [0, 999, -5, 2e9, Number.NaN, Number.POSITIVE_INFINITY, "500000", null]) {
			expect(parsePageMessage({ type: "bitrate", bytesPerSecond: bad })).toBeNull();
		}
		expect(parsePageMessage({ type: "want", id: 1, blocks: [{ piece: 0, begin: 0, length: 16 }] })).toBeNull();
		expect(parsePageMessage({ type: "start", magnet: "magnet:?xt=urn:btih:aa", gatewayUrl: "ftp://x", seedersUrl: "http://127.0.0.1" })).toBeNull();
		expect(parsePageMessage({ type: "files" })).toBeNull();
		expect(parsePageMessage({ type: "retry" })).toEqual({ type: "retry" });
		const start = { type: "start", magnet: "magnet:?xt=urn:btih:aa", gatewayUrl: "http://127.0.0.1:8787", seedersUrl: "http://127.0.0.1:8080" };
		expect(parsePageMessage({ ...start, capMs: 30_000 })).toEqual({ ...start, capMs: 30_000 });
		for (const bad of [10, 1e9, 1.5, "30000", null]) expect(parsePageMessage({ ...start, capMs: bad })).toBeNull();
		expect(parsePageMessage({ ...start, upload: false })).toEqual({ ...start, upload: false });
		expect(parsePageMessage({ ...start, upload: "no" })).toBeNull();
		expect(parsePageMessage({ type: "upload", enabled: true })).toEqual({ type: "upload", enabled: true });
		expect(parsePageMessage({ type: "upload", enabled: 1 })).toBeNull();
	});

	it("validates the lookup's progress, its failure and the session's counters", () => {
		const progress = { found: 12, tried: 4, connected: 1, elapsedMs: 3_500, discovering: true };
		expect(parseWorkerMessage({ type: "lookup", progress })).toEqual({ type: "lookup", progress });
		expect(parseWorkerMessage({ type: "lookup", progress: { ...progress, found: -1 } })).toBeNull();
		expect(parseWorkerMessage({ type: "lookup", progress: { ...progress, discovering: 1 } })).toBeNull();
		const failed = { type: "lookup-failed", reason: "exhausted", message: "None of the 12 peers found sent the file list", progress };
		expect(parseWorkerMessage(failed)).toEqual(failed);
		expect(parseWorkerMessage({ ...failed, reason: "timeout" })).toBeNull();
		expect(parseWorkerMessage({ ...failed, message: "x".repeat(400) })).toBeNull();

		const dials = (failedBy: Record<string, number> = {}) => ({ attempted: 5, succeeded: 2, failed: failedBy });
		const diag = {
			dials: { scout: dials({ timeout: 1 }), untried: dials({ refused: 2 }), known: dials() },
			connected: 2,
			unchoked: 1,
			delivering: 1,
			seeds: 1,
			firstUnchokeMs: null,
			metadata: { source: "vps", ms: 800 },
			sockets: 1,
			upgrades: 1,
			messages: 12,
			recycles: 0,
			untestedHighWater: 3,
			scout: { state: "done", streams: 1, peers: 30, alive: 4, seeds: 1, dead: 20, unknown: 6, rejected: 0, info: "rejected", firstPeerMs: 120 },
			upload: { enabled: true, bytes: 1_048_576, unchoked: 2, messages: 3 },
			escalation: { state: "joined", reason: "few_peers", afterMs: 91_000, upgrades: 1, accepted: 2 },
			paths: { cloudflare: { downloaded: 4_194_304, uploaded: 65_536, connections: 2 }, vps: { downloaded: 1_048_576, uploaded: 0, connections: 1 } },
		};
		expect(parseWorkerMessage({ type: "diag", diag })).toEqual({ type: "diag", diag });
		// Anything that could carry an address or is out of shape is refused whole.
		expect(parseWorkerMessage({ type: "diag", diag: { ...diag, dials: { ...diag.dials, scout: dials({ "1.2.3.4:6881": 1 }) } } })).toBeNull();
		expect(parseWorkerMessage({ type: "diag", diag: { ...diag, metadata: { source: "magnet-seeders", ms: 1 } } })).toBeNull();
		expect(parseWorkerMessage({ type: "diag", diag: { ...diag, scout: { ...diag.scout, info: "forged" } } })).toBeNull();
		expect(parseWorkerMessage({ type: "diag", diag: { ...diag, dials: { scout: dials(), untried: dials() } } })).toBeNull();
		expect(parseWorkerMessage({ type: "diag", diag: { ...diag, upload: { ...diag.upload, bytes: -1 } } })).toBeNull();
		expect(parseWorkerMessage({ type: "diag", diag: { ...diag, upload: { ...diag.upload, enabled: "yes" } } })).toBeNull();
		expect(parseWorkerMessage({ type: "diag", diag: { ...diag, upload: undefined } })).toBeNull();
		expect(parseWorkerMessage({ type: "diag", diag: { ...diag, escalation: undefined } })).toBeNull();
		expect(parseWorkerMessage({ type: "diag", diag: { ...diag, escalation: { ...diag.escalation, state: "maybe" } } })).toBeNull();
		expect(parseWorkerMessage({ type: "diag", diag: { ...diag, escalation: { ...diag.escalation, reason: "1.2.3.4" } } })).toBeNull();
		expect(parseWorkerMessage({ type: "diag", diag: { ...diag, escalation: { ...diag.escalation, accepted: -1 } } })).toBeNull();
		// Bytes by path (M4-08): both paths, non-negative integers.
		expect(parseWorkerMessage({ type: "diag", diag: { ...diag, paths: undefined } })).toBeNull();
		expect(parseWorkerMessage({ type: "diag", diag: { ...diag, paths: { cloudflare: diag.paths.cloudflare } } })).toBeNull();
		expect(parseWorkerMessage({ type: "diag", diag: { ...diag, paths: { ...diag.paths, vps: { ...diag.paths.vps, downloaded: 1.5 } } } })).toBeNull();
		expect(parseWorkerMessage({ type: "diag", diag: { ...diag, paths: { ...diag.paths, vps: { ...diag.paths.vps, uploaded: -1 } } } })).toBeNull();
		// Unknown path names are dropped, not kept.
		const extra = parseWorkerMessage({ type: "diag", diag: { ...diag, paths: { ...diag.paths, "1.2.3.4": { downloaded: 1, uploaded: 0, connections: 1 } } } });
		expect(extra).toEqual({ type: "diag", diag });
		const many = Object.fromEntries(Array.from({ length: 20 }, (_, i) => [`r${String.fromCharCode(97 + i)}`, 1]));
		expect(parseWorkerMessage({ type: "diag", diag: { ...diag, dials: { ...diag.dials, known: dials(many) } } })).toBeNull();

		const info = new ArrayBuffer(8);
		const listing = { infoHash: "a".repeat(40), name: "x", size: 1, pieceLength: 16384, files: [{ path: "x", size: 1, offset: 0 }] };
		expect(parseWorkerMessage({ type: "files", listing, info, source: "peer" })?.type).toBe("files");
		expect(parseWorkerMessage({ type: "files", listing, info, source: "external" })).toBeNull();
	});

	it("validates what the worker sends", () => {
		expect(parseWorkerMessage({ type: "error", message: "metadata timed out" })?.type).toBe("error");
		expect(parseWorkerMessage({ type: "peers", count: 2 })).toEqual({ type: "peers", count: 2 });
		expect(parseWorkerMessage({ type: "stats", upgrades: 1, messages: 40, known: 9, delivered: 2 })).toEqual({ type: "stats", upgrades: 1, messages: 40, known: 9, delivered: 2 });
		expect(parseWorkerMessage({ type: "stats", upgrades: 1, messages: -1, known: 0, delivered: 0 })).toBeNull();
		expect(parseWorkerMessage({ type: "stats", upgrades: 1, messages: 2 })).toBeNull();
		expect(parseWorkerMessage({ type: "error", message: "1.2.3.4:6881" })).toEqual({ type: "error", message: "1.2.3.4:6881" });
		const data = new ArrayBuffer(8);
		expect(parseWorkerMessage({ type: "piece", id: 1, piece: 4, data, stored: true })).toEqual({ type: "piece", id: 1, piece: 4, data, stored: true });
		expect(parseWorkerMessage({ type: "piece", id: 1, piece: 4, data })).toBeNull();
		expect(parseWorkerMessage({ type: "piece", id: 1, piece: 4, data: new ArrayBuffer(0), stored: false })).toBeNull();
		expect(parseWorkerMessage({ type: "piece", id: 1, piece: 4, data: new Uint8Array(8), stored: false })).toBeNull();
		expect(parseWorkerMessage({ type: "read-result", req: 3, piece: 4, data: null })).toEqual({ type: "read-result", req: 3, piece: 4, data: null });
		expect(parseWorkerMessage({ type: "read-result", req: 3, piece: 4, data })).toEqual({ type: "read-result", req: 3, piece: 4, data });
		expect(parseWorkerMessage({ type: "read-result", req: 3, piece: 4, data: "x" })).toBeNull();
		expect(parseWorkerMessage({ type: "released", req: 3 })).toEqual({ type: "released", req: 3 });
		expect(parseWorkerMessage({ type: "store-error", id: 1, code: "quota" })).toEqual({ type: "store-error", id: 1, code: "quota" });
		expect(parseWorkerMessage({ type: "store-error", id: 1, code: "boom" })).toBeNull();
		expect(MAX_PIECE).toBe(64 * 1024 * 1024);
		expect(parseWorkerMessage({ type: "recovered", id: 2 })).toEqual({ type: "recovered", id: 2 });
		const progress = { type: "progress", id: 1, assembling: [4, 2, 4], bytes: 10, live: 3, hashFailures: 0, banned: 1, hedges: 1, cancels: 2, windowBytes: 18_750_000, rateThreshold: 1_250_000 };
		expect(parseWorkerMessage(progress)).toEqual(progress);
		expect(parseWorkerMessage({ ...progress, assembling: [4, 2] })).toBeNull();
		expect(parseWorkerMessage({ ...progress, assembling: [4, -2, 4] })).toBeNull();
		expect(parseWorkerMessage({ ...progress, live: undefined })).toBeNull();
		expect(parseWorkerMessage({ ...progress, windowBytes: -1 })).toBeNull();
		expect(parseWorkerMessage({ ...progress, rateThreshold: 1.5 })).toBeNull();
	});

	it("validates the escalation settings of a start and the swarm's health (M4-06)", () => {
		const start = { type: "start", magnet: "magnet:?xt=urn:btih:aa", gatewayUrl: "http://127.0.0.1:8787", seedersUrl: "http://127.0.0.1:8080" };
		const escalation = { gatewayUrl: "https://vps.example.org", minDelivering: 2, escalateAfterMs: 90_000, warnAfterMs: 30_000 };
		expect(parsePageMessage({ ...start, escalation })).toEqual({ ...start, escalation });
		// The withdraw delay is optional (the engine's default without it).
		expect(parsePageMessage({ ...start, escalation: { ...escalation, withdrawAfterMs: 600_000 } })).toEqual({ ...start, escalation: { ...escalation, withdrawAfterMs: 600_000 } });
		// Off: no VPS gateway.
		expect(parsePageMessage({ ...start, escalation: { ...escalation, gatewayUrl: null } })).toEqual({ ...start, escalation: { ...escalation, gatewayUrl: null } });
		for (const bad of [
			{ ...escalation, gatewayUrl: "ftp://x" },
			{ ...escalation, gatewayUrl: undefined },
			{ ...escalation, minDelivering: 0 },
			{ ...escalation, minDelivering: 1.5 },
			{ ...escalation, escalateAfterMs: -1 },
			{ ...escalation, warnAfterMs: 1e12 },
			{ ...escalation, withdrawAfterMs: -5 },
			{ ...escalation, withdrawAfterMs: "600" },
			"on",
		])
			expect(parsePageMessage({ ...start, escalation: bad })).toBeNull();

		const health = { escalation: "joined", reason: "slow", belowBitrate: true, inbound: 1, rate: 120_000, bitrate: 400_000 };
		expect(parseWorkerMessage({ type: "health", health })).toEqual({ type: "health", health });
		expect(parseWorkerMessage({ type: "health", health: { ...health, reason: null, escalation: "off" } })?.type).toBe("health");
		expect(parseWorkerMessage({ type: "health", health: { ...health, escalation: "left" } })?.type).toBe("health");
		for (const bad of [{ ...health, escalation: "vps" }, { ...health, reason: "1.2.3.4:6881" }, { ...health, belowBitrate: 1 }, { ...health, rate: -1 }, { ...health, inbound: undefined }]) {
			expect(parseWorkerMessage({ type: "health", health: bad })).toBeNull();
		}
	});
});
