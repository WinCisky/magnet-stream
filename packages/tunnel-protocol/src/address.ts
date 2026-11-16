// Peer addresses as carried by `open` and `accept`: an IPv4 or IPv6 literal
// and a port. No hostnames: the gateway never resolves names, so a tunnel
// can't be pointed at an internal host through DNS.

export interface PeerAddress {
	family: 4 | 6;
	/** 4 bytes (IPv4) or 16 bytes (IPv6), network order. */
	ip: Uint8Array;
	/** 1..65535 */
	port: number;
}

/** Parse an IPv4 (`1.2.3.4`) or IPv6 (`2001:db8::1`, `::ffff:1.2.3.4`) literal. */
export function parseIp(text: string): { family: 4 | 6; ip: Uint8Array } | undefined {
	if (text.length > 64) return undefined;
	const v4 = parseIpv4(text);
	if (v4) return { family: 4, ip: v4 };
	const v6 = parseIpv6(text);
	if (v6) return { family: 6, ip: v6 };
	return undefined;
}

function parseIpv4(text: string): Uint8Array | undefined {
	const parts = text.split(".");
	if (parts.length !== 4) return undefined;
	const out = new Uint8Array(4);
	for (let i = 0; i < 4; i++) {
		if (!/^(0|[1-9]\d{0,2})$/.test(parts[i])) return undefined;
		const n = Number(parts[i]);
		if (n > 255) return undefined;
		out[i] = n;
	}
	return out;
}

function parseIpv6(text: string): Uint8Array | undefined {
	let head = text;
	let tailV4: Uint8Array | undefined;
	const lastColon = text.lastIndexOf(":");
	if (lastColon >= 0 && text.includes(".", lastColon)) {
		tailV4 = parseIpv4(text.slice(lastColon + 1));
		if (!tailV4) return undefined;
		head = text.slice(0, lastColon + 1) + "0:0";
	}
	const halves = head.split("::");
	if (halves.length > 2) return undefined;
	const groups = (s: string) => (s === "" ? [] : s.split(":"));
	const left = groups(halves[0]);
	const right = halves.length === 2 ? groups(halves[1]) : [];
	const total = left.length + right.length;
	if (halves.length === 1 ? total !== 8 : total > 7) return undefined;
	const words = [...left, ...Array<string>(8 - total).fill("0"), ...right];
	const out = new Uint8Array(16);
	for (let i = 0; i < 8; i++) {
		if (!/^[0-9a-fA-F]{1,4}$/.test(words[i])) return undefined;
		const w = parseInt(words[i], 16);
		out[i * 2] = w >> 8;
		out[i * 2 + 1] = w & 0xff;
	}
	if (tailV4) out.set(tailV4, 12);
	return out;
}

/** `1.2.3.4` or `2001:db8::1` (RFC 5952 compressed form). */
export function formatIp(family: 4 | 6, ip: Uint8Array): string {
	if (family === 4) return Array.from(ip.subarray(0, 4)).join(".");
	const words: number[] = [];
	for (let i = 0; i < 16; i += 2) words.push((ip[i] << 8) | ip[i + 1]);
	// Longest run (>= 2) of zero words becomes "::".
	let bestAt = -1;
	let bestLen = 1;
	for (let i = 0; i < 8; ) {
		if (words[i] !== 0) {
			i++;
			continue;
		}
		let j = i;
		while (j < 8 && words[j] === 0) j++;
		if (j - i > bestLen) {
			bestAt = i;
			bestLen = j - i;
		}
		i = j;
	}
	const hex = (ws: number[]) => ws.map((w) => w.toString(16)).join(":");
	if (bestAt < 0) return hex(words);
	return `${hex(words.slice(0, bestAt))}::${hex(words.slice(bestAt + bestLen))}`;
}

/** `1.2.3.4:6881` or `[2001:db8::1]:6881`. */
export function formatAddress(address: PeerAddress): string {
	const ip = formatIp(address.family, address.ip);
	return address.family === 6 ? `[${ip}]:${address.port}` : `${ip}:${address.port}`;
}

/** Inverse of `formatAddress`. */
export function parseAddress(text: string): PeerAddress | undefined {
	const m = /^(?:\[([0-9a-fA-F:.]+)\]|(\d{1,3}(?:\.\d{1,3}){3})):(\d{1,5})$/.exec(text);
	if (!m) return undefined;
	const port = Number(m[3]);
	if (port < 1 || port > 65535) return undefined;
	const parsed = parseIp(m[1] ?? m[2]);
	if (!parsed || (m[1] !== undefined) !== (parsed.family === 6)) return undefined;
	return { family: parsed.family, ip: parsed.ip, port };
}
