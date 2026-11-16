// Minimal bencode (BEP 3) for the fake peer: encode info dicts and
// extension messages, decode what test clients send.

export type BValue = number | string | Uint8Array | BValue[] | { [key: string]: BValue };

const text = new TextEncoder();

/** Encode with dictionary keys sorted, as the spec requires. */
export function bencode(value: BValue): Uint8Array {
	const parts: Uint8Array[] = [];
	const walk = (v: BValue): void => {
		if (typeof v === "number") {
			if (!Number.isSafeInteger(v)) throw new RangeError("bencode: integers only");
			parts.push(text.encode(`i${v}e`));
		} else if (typeof v === "string") walk(text.encode(v));
		else if (v instanceof Uint8Array) {
			parts.push(text.encode(`${v.length}:`), v);
		} else if (Array.isArray(v)) {
			parts.push(text.encode("l"));
			v.forEach(walk);
			parts.push(text.encode("e"));
		} else {
			parts.push(text.encode("d"));
			for (const key of Object.keys(v).sort()) {
				walk(key);
				walk(v[key]);
			}
			parts.push(text.encode("e"));
		}
	};
	walk(value);
	return concat(parts);
}

export type BDecoded = number | Uint8Array | BDecoded[] | Map<string, BDecoded>;

/**
 * Decode one value from the start of `data`; `end` is where it stopped
 * (extension messages carry raw bytes after the dict). Undefined when
 * malformed.
 */
export function bdecode(data: Uint8Array): { value: BDecoded; end: number } | undefined {
	let at = 0;
	const dec = new TextDecoder();
	const int = (end: number): number | undefined => {
		const s = dec.decode(data.subarray(at, end));
		if (!/^(0|-?[1-9]\d*)$/.test(s)) return undefined;
		const n = Number(s);
		return Number.isSafeInteger(n) ? n : undefined;
	};
	const value = (depth: number): BDecoded | undefined => {
		if (depth > 32 || at >= data.length) return undefined;
		const c = data[at];
		if (c === 0x69) {
			const end = data.indexOf(0x65, at);
			if (end < 0) return undefined;
			at++;
			const n = int(end);
			at = end + 1;
			return n;
		}
		if (c === 0x6c || c === 0x64) {
			at++;
			const list: BDecoded[] = [];
			const dict = new Map<string, BDecoded>();
			while (at < data.length && data[at] !== 0x65) {
				if (c === 0x6c) {
					const item = value(depth + 1);
					if (item === undefined) return undefined;
					list.push(item);
				} else {
					const key = value(depth + 1);
					if (!(key instanceof Uint8Array)) return undefined;
					const item = value(depth + 1);
					if (item === undefined) return undefined;
					dict.set(dec.decode(key), item);
				}
			}
			if (at >= data.length) return undefined;
			at++;
			return c === 0x6c ? list : dict;
		}
		const colon = data.indexOf(0x3a, at);
		if (colon < 0) return undefined;
		const len = int(colon);
		if (len === undefined || len < 0 || colon + 1 + len > data.length) return undefined;
		at = colon + 1 + len;
		return data.subarray(colon + 1, at);
	};
	const v = value(0);
	return v === undefined ? undefined : { value: v, end: at };
}

export function concat(parts: readonly Uint8Array[]): Uint8Array {
	const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
	let at = 0;
	for (const p of parts) {
		out.set(p, at);
		at += p.length;
	}
	return out;
}
