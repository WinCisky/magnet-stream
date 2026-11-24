// Hole punching, BEP 55 `ut_holepunch` (docs/architecture.md §Chunk
// recovery › Swarms with few seeders, step 4; M4-05).
//
// A peer behind NAT never answers a dial, and one that does not re-announce
// soon never connects to the VPS gateway's announced port either. A peer we
// are already connected to (the relay) may be connected to it, though: we
// send the relay a `rendezvous` naming the target, the relay sends a
// `connect` to both of us, each with the other's address, and both sides
// open a uTP connection to each other at once. The NAT in front of the
// target lets our packets in once the target's own packet has left towards
// us. The result is a tunnel like any other: the engine asks the VPS gateway
// for a uTP `open` to the target (the gateway dials from its announced port
// and takes the target's SYN for that open, services/seeders/src/tunnel/
// punch.rs).
//
// For the relay to tell the target the right address, it must see us at the
// announced port: only connections that reached it from there qualify
// (inbound peers, and uTP connections, which leave from that port); a TCP
// dial from the gateway leaves from an ephemeral port. The target must
// support the extension (the relay's ut_pex flag 0x08) and be one the
// relay is connected to (it named it in ut_pex and has not dropped it).
//
// This file is the message codec and the bookkeeping (which relay knows
// which target, punches in flight, limits); connection-manager.ts does the
// I/O. We never act as a relay (out of M4-05's scope): a `rendezvous` sent
// to us is answered with the error NotConnected (peer-connection.ts), and a
// `connect` we did not ask for is ignored, so a peer cannot make the gateway
// dial an address of its choosing.

import { formatAddress, type PeerAddress } from "@magnet-stream/tunnel-protocol";
import { ExtensionError, type PexAddress } from "./extension.ts";

export const HolepunchType = { RENDEZVOUS: 0, CONNECT: 1, ERROR: 2 } as const;

/** BEP 55 error codes. */
export const HolepunchError = {
	/** The relay does not know the target. */
	NO_SUCH_PEER: 1,
	/** The relay is not connected to the target. */
	NOT_CONNECTED: 2,
	/** The target does not support ut_holepunch. */
	NO_SUPPORT: 3,
	/** The target is the initiator itself. */
	NO_SELF: 4,
} as const;

export type HolepunchMessage =
	| { type: "rendezvous"; address: PeerAddress }
	| { type: "connect"; address: PeerAddress }
	| { type: "error"; address: PeerAddress; error: number };

/** BEP 11 `added.f`: the peer supports ut_holepunch. */
export const PEX_HOLEPUNCH = 0x08;

/** The snake_case name of an error code (unknown codes: `error`). */
export function holepunchErrorName(code: number): string {
	const found = Object.entries(HolepunchError).find(([, v]) => v === code);
	return found ? found[0].toLowerCase() : "error";
}

/** `msg_type u8, addr_type u8 (0 IPv4, 1 IPv6), addr, port u16, err_code u32`, big-endian. */
export function encodeHolepunch(message: HolepunchMessage): Uint8Array {
	const { address } = message;
	const ipLength = address.family === 4 ? 4 : 16;
	if (address.ip.length !== ipLength || !Number.isInteger(address.port) || address.port < 1 || address.port > 65535) {
		throw new RangeError("bad holepunch address");
	}
	const out = new Uint8Array(2 + ipLength + 2 + 4);
	const view = new DataView(out.buffer);
	out[0] = HolepunchType[message.type.toUpperCase() as "RENDEZVOUS" | "CONNECT" | "ERROR"];
	out[1] = address.family === 4 ? 0 : 1;
	out.set(address.ip, 2);
	view.setUint16(2 + ipLength, address.port);
	view.setUint32(4 + ipLength, message.type === "error" ? message.error >>> 0 : 0);
	return out;
}

/**
 * Exactly one BEP 55 message: a known type, an IPv4 or IPv6 address, a
 * port above 0, then the error code. The error code is kept for `error`
 * messages and ignored for the others (BEP 55: only meaningful there);
 * libtorrent leaves it out of `rendezvous` and `connect` (8 or 20 bytes
 * instead of 12 or 24), so there it may be absent; an `error` must carry it.
 */
export function decodeHolepunch(payload: Uint8Array): HolepunchMessage {
	if (payload.length < 2) throw new ExtensionError("ut_holepunch message too short");
	const type = payload[0];
	const family = payload[1] === 0 ? 4 : payload[1] === 1 ? 6 : 0;
	if (family === 0) throw new ExtensionError("bad ut_holepunch addr_type");
	const ipLength = family === 4 ? 4 : 16;
	const withCode = payload.length === 2 + ipLength + 2 + 4;
	const withoutCode = payload.length === 2 + ipLength + 2 && type !== HolepunchType.ERROR;
	if (!withCode && !withoutCode) throw new ExtensionError("bad ut_holepunch length");
	const view = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
	const address: PeerAddress = { family, ip: payload.slice(2, 2 + ipLength), port: view.getUint16(2 + ipLength) };
	if (address.port === 0) throw new ExtensionError("ut_holepunch port 0");
	const error = withCode ? view.getUint32(4 + ipLength) : 0;
	if (type === HolepunchType.RENDEZVOUS) return { type: "rendezvous", address };
	if (type === HolepunchType.CONNECT) return { type: "connect", address };
	if (type === HolepunchType.ERROR) return { type: "error", address, error };
	throw new ExtensionError("unknown ut_holepunch msg_type");
}

// ------------------------------------------------------------ bookkeeping

export interface HolepunchConfig {
	/** Punches in flight at once (rendezvous sent, or the uTP open pending). */
	maxInFlight: number;
	/** A rendezvous the relay answers neither way within this fails (`timeout`). */
	answerTimeoutMs: number;
	/** Punches per target in a session (each through another relay). */
	maxAttempts: number;
	/** Least time between two rendezvous. */
	intervalMs: number;
	/** Rendezvous per session, at most. */
	maxPerSession: number;
	/** Targets remembered per relay (from its ut_pex). */
	maxKnownPerRelay: number;
}

export const HOLEPUNCH_DEFAULTS: Readonly<HolepunchConfig> = {
	maxInFlight: 2,
	answerTimeoutMs: 10_000,
	maxAttempts: 2,
	intervalMs: 1_000,
	maxPerSession: 64,
	maxKnownPerRelay: 200,
};

function sane(value: number | undefined, fallback: number, min: number, max: number): number {
	if (value === undefined || !Number.isFinite(value)) return fallback;
	return Math.min(max, Math.max(min, Math.round(value)));
}

export function holepunchConfig(over: Partial<HolepunchConfig> = {}): HolepunchConfig {
	const d = HOLEPUNCH_DEFAULTS;
	return {
		maxInFlight: sane(over.maxInFlight, d.maxInFlight, 1, 6),
		answerTimeoutMs: sane(over.answerTimeoutMs, d.answerTimeoutMs, 100, 120_000),
		maxAttempts: sane(over.maxAttempts, d.maxAttempts, 1, 8),
		intervalMs: sane(over.intervalMs, d.intervalMs, 0, 600_000),
		maxPerSession: sane(over.maxPerSession, d.maxPerSession, 0, 10_000),
		maxKnownPerRelay: sane(over.maxKnownPerRelay, d.maxKnownPerRelay, 1, 2_000),
	};
}

export interface Punch {
	/** `ip:port` of the peer behind NAT. */
	target: string;
	/** Connection key of the relay. */
	relay: string;
	/** `rendezvous`: waiting for the relay's answer; `dialling`: the uTP open is out. */
	phase: "rendezvous" | "dialling";
	startedAt: number;
}

/** Counters (no addresses). */
export interface HolepunchStats {
	/** Rendezvous sent. */
	attempted: number;
	/** Punches that gave a connection. */
	connected: number;
	/** Failed punches by reason: BEP 55 error names, `timeout`, `relay_closed`, `busy`, `open_<failed reason>`. */
	failed: Record<string, number>;
	/** `connect` messages we did not ask for (ignored). */
	unsolicited: number;
}

/** Errors after which no relay is asked about the target again. */
const FINAL = new Set(["no_support", "no_self"]);
/** Errors that say this relay does not (or no longer) know the target. */
const UNKNOWN_TO_RELAY = new Set(["no_such_peer", "not_connected"]);

export class HolePuncher {
	readonly config: HolepunchConfig;
	readonly stats: HolepunchStats = { attempted: 0, connected: 0, failed: {}, unsolicited: 0 };
	/** Relay key → targets it named with the holepunch flag (and has not dropped), oldest first. */
	private readonly known = new Map<string, Set<string>>();
	private readonly punches = new Map<string, Punch>();
	/** Target → relays already asked about it, and how many punches it had. */
	private readonly tried = new Map<string, Set<string>>();
	private readonly never = new Set<string>();
	private lastAt = -Infinity;

	constructor(config: Partial<HolepunchConfig> = {}) {
		this.config = holepunchConfig(config);
	}

	get inFlight(): number {
		return this.punches.size;
	}

	pending(target: string): Punch | undefined {
		return this.punches.get(target);
	}

	/** A relay's ut_pex: the peers it is connected to that support ut_holepunch, minus those it dropped. */
	learn(relay: string, added: readonly PexAddress[], dropped: readonly PexAddress[] = []): void {
		let set = this.known.get(relay);
		for (const a of dropped) set?.delete(formatAddress(a));
		for (const a of added) {
			if ((a.flags & PEX_HOLEPUNCH) === 0) continue;
			const key = formatAddress(a);
			set ??= new Set();
			set.delete(key);
			set.add(key);
			if (set.size > this.config.maxKnownPerRelay) set.delete(set.values().next().value!);
		}
		if (set && set.size > 0) this.known.set(relay, set);
	}

	/** The relay's connection ended: what it knew goes, and the punches waiting for its answer are returned (they failed). */
	forgetRelay(relay: string): Punch[] {
		this.known.delete(relay);
		const lost: Punch[] = [];
		for (const p of this.punches.values()) if (p.relay === relay && p.phase === "rendezvous") lost.push(p);
		for (const p of lost) this.fail(p.target, "relay_closed");
		return lost;
	}

	/**
	 * The next punch to start, if the limits allow one: a relay `isRelay`
	 * accepts and a target it knows that `isTarget` accepts, not punched
	 * now, not asked of this relay before, under the attempts. Registered
	 * as sent (the caller sends the rendezvous).
	 */
	next(now: number, isRelay: (relay: string) => boolean, isTarget: (target: string) => boolean): Punch | undefined {
		if (this.punches.size >= this.config.maxInFlight || this.stats.attempted >= this.config.maxPerSession) return undefined;
		if (now - this.lastAt < this.config.intervalMs) return undefined;
		let best: { relay: string; target: string; tries: number } | undefined;
		for (const [relay, targets] of this.known) {
			if (!isRelay(relay)) continue;
			for (const target of targets) {
				if (this.punches.has(target) || this.never.has(target)) continue;
				const asked = this.tried.get(target);
				const tries = asked?.size ?? 0;
				if (tries >= this.config.maxAttempts || asked?.has(relay)) continue;
				if (best && best.tries <= tries) continue;
				if (!isTarget(target)) continue;
				best = { relay, target, tries };
				if (tries === 0) break;
			}
			if (best?.tries === 0) break;
		}
		if (!best) return undefined;
		const punch: Punch = { target: best.target, relay: best.relay, phase: "rendezvous", startedAt: now };
		this.punches.set(best.target, punch);
		let asked = this.tried.get(best.target);
		if (!asked) this.tried.set(best.target, (asked = new Set()));
		asked.add(best.relay);
		this.stats.attempted++;
		this.lastAt = now;
		return punch;
	}

	/** The relay's `connect` for `target`: true (now dialling) if we asked that relay about it, else ignored. */
	onConnect(relay: string, target: string, now: number): boolean {
		const p = this.punches.get(target);
		if (!p || p.relay !== relay || p.phase !== "rendezvous") {
			this.stats.unsolicited++;
			return false;
		}
		p.phase = "dialling";
		p.startedAt = now;
		return true;
	}

	/** The relay's `error` about `target`: the failure's name if it answers our rendezvous, else undefined (ignored). */
	onError(relay: string, target: string, code: number): string | undefined {
		const p = this.punches.get(target);
		if (!p || p.relay !== relay || p.phase !== "rendezvous") return undefined;
		const name = holepunchErrorName(code);
		if (UNKNOWN_TO_RELAY.has(name)) this.known.get(relay)?.delete(target);
		if (FINAL.has(name)) this.never.add(target);
		this.fail(target, name);
		return name;
	}

	/** Rendezvous left unanswered past the timeout: removed (failed `timeout`) and returned. */
	expired(now: number): Punch[] {
		const out: Punch[] = [];
		for (const p of this.punches.values()) if (p.phase === "rendezvous" && now - p.startedAt >= this.config.answerTimeoutMs) out.push(p);
		for (const p of out) this.fail(p.target, "timeout");
		return out;
	}

	/** The punch of `target` gave a connection. */
	succeeded(target: string): void {
		if (this.punches.delete(target)) this.stats.connected++;
	}

	/** The punch of `target` failed for `reason`. */
	fail(target: string, reason: string): void {
		if (!this.punches.delete(target)) return;
		this.stats.failed[reason] = (this.stats.failed[reason] ?? 0) + 1;
	}
}
