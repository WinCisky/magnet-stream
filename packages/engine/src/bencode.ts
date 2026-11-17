// Bencode (BEP 3): a bounded decoder for untrusted input (info dicts,
// extension messages) and an encoder for the messages we send.

export type Bencoded = number | Uint8Array | Bencoded[] | BencodeDict;
export type BencodeDict = Map<string, Bencoded>;

export class BencodeError extends Error {}

export interface BencodeLimits {
	/** Nesting depth (default 64). */
	maxDepth?: number;
	/** Values (integers, strings, lists, dicts) in the input (default 1,000,000). */
	maxItems?: number;
}

const MAX_DEPTH = 64;
const MAX_ITEMS = 1_000_000;
const utf8 = new TextDecoder();

/** Decode a whole buffer; trailing bytes are an error. */
export function decodeBencode(data: Uint8Array, limits: BencodeLimits = {}): Bencoded {
	const { value, length } = decodeBencodePrefix(data, limits);
	if (length !== data.length) throw new BencodeError(`trailing data at byte ${length}`);
	return value;
}

/**
 * Decode one value from the start of `data` and say where it ended
 * (ut_metadata `data` messages carry raw bytes after the dict).
 */
export function decodeBencodePrefix(data: Uint8Array, limits: BencodeLimits = {}): { value: Bencoded; length: number } {
	const maxDepth = limits.maxDepth ?? MAX_DEPTH;
	let items = limits.maxItems ?? MAX_ITEMS;
	let at = 0;

	const fail = (why: string): never => {
		throw new BencodeError(`${why} at byte ${at}`);
	};

	const integerUntil = (end: number): number => {
		if (end - at > 20) fail("integer too long");
		const text = utf8.decode(data.subarray(at, end));
		if (!/^(0|-?[1-9]\d*)$/.test(text)) fail("bad integer");
		const n = Number(text);
		if (!Number.isSafeInteger(n)) fail("integer out of range");
		return n;
	};

	const value = (depth: number): Bencoded => {
		if (depth > maxDepth) fail("nested too deeply");
		if (--items < 0) fail("too many values");
		if (at >= data.length) fail("unexpected end");
		const c = data[at];
		if (c === 0x69 /* i */) {
			const end = data.indexOf(0x65 /* e */, at);
			if (end < 0) fail("unterminated integer");
			at++;
			const n = integerUntil(end);
			at = end + 1;
			return n;
		}
		if (c === 0x6c /* l */) {
			at++;
			const list: Bencoded[] = [];
			for (;;) {
				if (at >= data.length) fail("unterminated list");
				if (data[at] === 0x65) break;
				list.push(value(depth + 1));
			}
			at++;
			return list;
		}
		if (c === 0x64 /* d */) {
			at++;
			const dict: BencodeDict = new Map();
			for (;;) {
				if (at >= data.length) fail("unterminated dict");
				if (data[at] === 0x65) break;
				const key = value(depth + 1);
				if (!(key instanceof Uint8Array)) fail("dict key is not a string");
				const name = utf8.decode(key as Uint8Array);
				if (dict.has(name)) fail("duplicate dict key");
				dict.set(name, value(depth + 1));
			}
			at++;
			return dict;
		}
		if (c < 0x30 || c > 0x39) fail("unexpected byte");
		const colon = data.indexOf(0x3a /* : */, at);
		if (colon < 0) fail("bad string length");
		const length = integerUntil(colon);
		if (length < 0 || colon + 1 + length > data.length) fail("string runs past the end");
		at = colon + 1 + length;
		return data.subarray(colon + 1, at);
	};

	const result = value(0);
	return { value: result, length: at };
}

export type BencodeInput = number | string | Uint8Array | BencodeInput[] | { [key: string]: BencodeInput | undefined } | Map<string, BencodeInput>;

const text = new TextEncoder();

/** Encode, with dict keys sorted as the spec requires (undefined values are skipped). */
export function encodeBencode(value: BencodeInput): Uint8Array {
	const parts: Uint8Array[] = [];
	const walk = (v: BencodeInput): void => {
		if (typeof v === "number") {
			if (!Number.isSafeInteger(v)) throw new BencodeError("bencode: integers only");
			parts.push(text.encode(`i${v}e`));
		} else if (typeof v === "string") walk(text.encode(v));
		else if (v instanceof Uint8Array) parts.push(text.encode(`${v.length}:`), v);
		else if (Array.isArray(v)) {
			parts.push(text.encode("l"));
			v.forEach(walk);
			parts.push(text.encode("e"));
		} else {
			const entries: [string, BencodeInput][] = v instanceof Map ? [...v] : (Object.entries(v).filter(([, x]) => x !== undefined) as [string, BencodeInput][]);
			entries.sort(([a], [b]) => compareBytes(text.encode(a), text.encode(b)));
			parts.push(text.encode("d"));
			for (const [k, x] of entries) {
				walk(k);
				walk(x);
			}
			parts.push(text.encode("e"));
		}
	};
	walk(value);
	const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
	let at = 0;
	for (const p of parts) {
		out.set(p, at);
		at += p.length;
	}
	return out;
}

function compareBytes(a: Uint8Array, b: Uint8Array): number {
	for (let i = 0; i < Math.min(a.length, b.length); i++) if (a[i] !== b[i]) return a[i] - b[i];
	return a.length - b.length;
}

/** Helpers for reading decoded dicts. */
export function getBytes(dict: BencodeDict, key: string): Uint8Array | undefined {
	const v = dict.get(key);
	return v instanceof Uint8Array ? v : undefined;
}

export function getInt(dict: BencodeDict, key: string): number | undefined {
	const v = dict.get(key);
	return typeof v === "number" ? v : undefined;
}

export function getDict(dict: BencodeDict, key: string): BencodeDict | undefined {
	const v = dict.get(key);
	return v instanceof Map ? v : undefined;
}
