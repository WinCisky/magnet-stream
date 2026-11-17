// Magnet links (BEP 9, BEP 52): untrusted input, parsed strictly.
//
// The torrent is named by `xt=urn:btih:` (v1: 40 hex or 32 base32) and/or
// `xt=urn:btmh:1220…` (v2: SHA-256 multihash). The 20 bytes used on the
// wire (handshake, DHT, trackers) are the v1 hash, or for a v2-only torrent
// the v2 hash truncated to 20 bytes.

export const MAX_MAGNET_LENGTH = 8 * 1024;
const MAX_TRACKERS = 50;
const MAX_PEER_HINTS = 50;
const MAX_NAME = 1024;

export interface Magnet {
	/** The 20-byte info-hash used on the wire. */
	infoHash: Uint8Array;
	/** Its hex form (storage key, discovery `ih`). */
	infoHashHex: string;
	/** v1 SHA-1 info-hash, hex (v1 and hybrid). */
	v1?: string;
	/** v2 SHA-256 info-hash, hex (v2 and hybrid). */
	v2?: string;
	/** `dn`, if any (display only: untrusted text). */
	name?: string;
	/** `tr=` trackers (at most 50). */
	trackers: string[];
	/** `x.pe=` peer hints, `ip:port` (at most 50). */
	peers: string[];
}

export class MagnetError extends Error {}

export function parseMagnet(input: string): Magnet {
	const text = input.trim();
	if (text.length === 0) throw new MagnetError("Empty magnet link");
	if (text.length > MAX_MAGNET_LENGTH) throw new MagnetError("Magnet link too long");
	if (!/^magnet:\?/i.test(text)) throw new MagnetError("Not a magnet link");
	let params: URLSearchParams;
	try {
		params = new URLSearchParams(text.slice(text.indexOf("?") + 1));
	} catch {
		throw new MagnetError("Malformed magnet link");
	}
	let v1: string | undefined;
	let v2: string | undefined;
	for (const xt of params.getAll("xt")) {
		const lower = xt.trim();
		if (/^urn:btih:/i.test(lower)) v1 ??= decodeBtih(lower.slice(9));
		else if (/^urn:btmh:/i.test(lower)) v2 ??= decodeBtmh(lower.slice(9));
	}
	if (!v1 && !v2) throw new MagnetError("The magnet link has no BitTorrent info-hash (urn:btih or urn:btmh)");
	const wire = v1 ?? v2!.slice(0, 40);
	const trackers = params
		.getAll("tr")
		.map((t) => t.trim())
		.filter((t) => /^(udp|https?|wss?):\/\/[^\s]+$/i.test(t) && t.length <= 1024)
		.slice(0, MAX_TRACKERS);
	const peers = params
		.getAll("x.pe")
		.map((p) => p.trim())
		.filter((p) => p.length <= 64)
		.slice(0, MAX_PEER_HINTS);
	const dn = params.get("dn");
	return {
		infoHash: hexToBytes(wire),
		infoHashHex: wire,
		...(v1 ? { v1 } : {}),
		...(v2 ? { v2 } : {}),
		...(dn ? { name: dn.slice(0, MAX_NAME) } : {}),
		trackers: [...new Set(trackers)],
		peers: [...new Set(peers)],
	};
}

/** 40 hex or 32 base32 → lowercase hex; undefined if neither. */
export function decodeBtih(text: string): string | undefined {
	if (/^[0-9a-f]{40}$/i.test(text)) return text.toLowerCase();
	if (/^[a-z2-7]{32}$/i.test(text)) return bytesToHex(base32(text.toUpperCase()));
	return undefined;
}

/** `1220` + 64 hex (SHA-256 multihash) → lowercase hex digest. */
export function decodeBtmh(text: string): string | undefined {
	const m = /^1220([0-9a-f]{64})$/i.exec(text);
	return m ? m[1].toLowerCase() : undefined;
}

function base32(text: string): Uint8Array {
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
	const out: number[] = [];
	let bits = 0;
	let value = 0;
	for (const c of text) {
		value = (value << 5) | alphabet.indexOf(c);
		bits += 5;
		if (bits >= 8) {
			out.push((value >>> (bits - 8)) & 0xff);
			bits -= 8;
		}
	}
	return Uint8Array.from(out);
}

export function hexToBytes(hex: string): Uint8Array {
	const out = new Uint8Array(hex.length / 2);
	for (let i = 0; i < out.length; i++) out[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
	return out;
}

export function bytesToHex(bytes: Uint8Array): string {
	return Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
}
