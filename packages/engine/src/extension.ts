// BEP 10 extension protocol: the extended handshake, ut_metadata (BEP 9)
// and ut_pex (BEP 11) messages. Everything a peer sends is untrusted and
// bounded here.

import { BencodeError, decodeBencode, decodeBencodePrefix, encodeBencode, getBytes, getDict, getInt, type BencodeDict } from "./bencode.ts";

/** Extension message ids we assign (what peers must use when sending to us). */
export const LOCAL_EXTENSIONS = { ut_metadata: 1, ut_pex: 2 } as const;
export type ExtensionName = keyof typeof LOCAL_EXTENSIONS;

/** BEP 9: the info dict travels in 16 KiB pieces. */
export const METADATA_PIECE_SIZE = 16 * 1024;
/** Largest extension message we parse (a metadata piece plus its dict). */
export const MAX_EXTENSION_MESSAGE = METADATA_PIECE_SIZE + 1024;
const CLIENT = "magnet-stream/0.1";

export interface ExtensionHandshake {
	/** Extension name → the id the peer wants us to use (0 / absent: unsupported). */
	m: Map<string, number>;
	/** BEP 9: size of the info dict, if the peer has it. */
	metadataSize?: number;
	/** Request queue depth the peer accepts. */
	reqq?: number;
	/** Client name and version (display only). */
	client?: string;
	/** The peer's listen port. */
	port?: number;
}

export class ExtensionError extends Error {}

/** Our extended handshake: what we accept, no metadata to offer (we never upload). */
export function encodeOurHandshake(options: { reqq?: number } = {}): Uint8Array {
	return encodeBencode({ m: { ...LOCAL_EXTENSIONS }, reqq: options.reqq ?? 250, v: CLIENT });
}

export function decodeHandshake(payload: Uint8Array): ExtensionHandshake {
	const dict = decodeDict(payload);
	const m = new Map<string, number>();
	const mDict = getDict(dict, "m");
	if (mDict) {
		for (const [name, id] of mDict) {
			if (m.size >= 64) break;
			if (typeof id === "number" && Number.isInteger(id) && id > 0 && id < 256 && name.length <= 64) m.set(name, id);
		}
	}
	const out: ExtensionHandshake = { m };
	const size = getInt(dict, "metadata_size");
	if (size !== undefined && size > 0) out.metadataSize = size;
	const reqq = getInt(dict, "reqq");
	if (reqq !== undefined && reqq > 0) out.reqq = Math.min(reqq, 100_000);
	const v = getBytes(dict, "v");
	if (v) out.client = new TextDecoder().decode(v.subarray(0, 64)).replace(/[^\x20-\x7e]/g, "?");
	const p = getInt(dict, "p");
	if (p !== undefined && p > 0 && p < 65536) out.port = p;
	return out;
}

// ------------------------------------------------------------ ut_metadata

export type MetadataMessage =
	| { type: "request"; piece: number }
	| { type: "data"; piece: number; totalSize: number; data: Uint8Array }
	| { type: "reject"; piece: number };

export function encodeMetadataRequest(piece: number): Uint8Array {
	return encodeBencode({ msg_type: 0, piece });
}

export function encodeMetadataReject(piece: number): Uint8Array {
	return encodeBencode({ msg_type: 2, piece });
}

export function decodeMetadataMessage(payload: Uint8Array): MetadataMessage {
	if (payload.length > MAX_EXTENSION_MESSAGE) throw new ExtensionError("ut_metadata message too large");
	let decoded;
	try {
		decoded = decodeBencodePrefix(payload, { maxDepth: 4, maxItems: 64 });
	} catch (e) {
		throw new ExtensionError(`bad ut_metadata message: ${(e as BencodeError).message}`);
	}
	if (!(decoded.value instanceof Map)) throw new ExtensionError("ut_metadata message is not a dict");
	const dict = decoded.value;
	const type = getInt(dict, "msg_type");
	const piece = getInt(dict, "piece");
	if (piece === undefined || piece < 0 || piece > 1 << 16) throw new ExtensionError("bad ut_metadata piece");
	if (type === 0) return { type: "request", piece };
	if (type === 2) return { type: "reject", piece };
	if (type === 1) {
		const totalSize = getInt(dict, "total_size");
		if (totalSize === undefined || totalSize <= 0) throw new ExtensionError("ut_metadata data without total_size");
		const data = payload.slice(decoded.length);
		if (data.length === 0 || data.length > METADATA_PIECE_SIZE) throw new ExtensionError("bad ut_metadata data length");
		return { type: "data", piece, totalSize, data };
	}
	throw new ExtensionError("unknown ut_metadata msg_type");
}

// ------------------------------------------------------------ ut_pex

export interface PexAddress {
	family: 4 | 6;
	ip: Uint8Array;
	port: number;
	/** `added.f` flags (0x01 encryption, 0x02 seed, 0x04 utp, 0x08 holepunch, 0x10 reachable). */
	flags: number;
}

/** Per message, at most this many addresses are read (BEP 11 says peers send at most 50). */
export const MAX_PEX_PER_MESSAGE = 200;

export interface PexMessage {
	added: PexAddress[];
	dropped: PexAddress[];
}

export function decodePex(payload: Uint8Array): PexMessage {
	if (payload.length > 64 * 1024) throw new ExtensionError("ut_pex message too large");
	const dict = decodeDict(payload);
	const added = [
		...compact(getBytes(dict, "added"), getBytes(dict, "added.f"), 4),
		...compact(getBytes(dict, "added6"), getBytes(dict, "added6.f"), 6),
	].slice(0, MAX_PEX_PER_MESSAGE);
	const dropped = [...compact(getBytes(dict, "dropped"), undefined, 4), ...compact(getBytes(dict, "dropped6"), undefined, 6)].slice(0, MAX_PEX_PER_MESSAGE);
	return { added, dropped };
}

function compact(bytes: Uint8Array | undefined, flags: Uint8Array | undefined, family: 4 | 6): PexAddress[] {
	if (!bytes) return [];
	const size = family === 4 ? 6 : 18;
	const out: PexAddress[] = [];
	const n = Math.min(Math.floor(bytes.length / size), MAX_PEX_PER_MESSAGE);
	for (let i = 0; i < n; i++) {
		const at = i * size;
		const ip = bytes.slice(at, at + size - 2);
		const port = (bytes[at + size - 2] << 8) | bytes[at + size - 1];
		out.push({ family, ip, port, flags: flags?.[i] ?? 0 });
	}
	return out;
}

function decodeDict(payload: Uint8Array): BencodeDict {
	let value;
	try {
		value = decodeBencode(payload, { maxDepth: 8, maxItems: 4096 });
	} catch (e) {
		throw new ExtensionError(`bad extension message: ${(e as Error).message}`);
	}
	if (!(value instanceof Map)) throw new ExtensionError("extension message is not a dict");
	return value;
}
