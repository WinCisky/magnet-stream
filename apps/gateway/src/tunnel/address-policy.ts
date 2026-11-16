// Which addresses a tunnel may connect to (SSRF guard). Only public
// unicast addresses on ports >= 1024: no private, loopback, link-local,
// CGNAT, multicast, documentation or otherwise reserved ranges, for IPv4,
// IPv6 and IPv4 embedded in IPv6 (mapped, compatible, NAT64, 6to4).
// Addresses are literals (the protocol carries no hostnames), so there is
// no DNS lookup to rebind.

import type { PeerAddress } from "@magnet-stream/tunnel-protocol";

export const MIN_PORT = 1024;

export type AddressVerdict = "ok" | "forbidden_address" | "forbidden_port";

/**
 * `allowPrivate` lifts the address check only (local tests against peers
 * on 127.0.0.1); the port rule always applies.
 */
export function checkAddress(address: PeerAddress, allowPrivate = false): AddressVerdict {
	if (!Number.isInteger(address.port) || address.port < MIN_PORT || address.port > 65535) return "forbidden_port";
	if (allowPrivate) return "ok";
	const publicIp = address.family === 4 ? isPublicIpv4(address.ip) : isPublicIpv6(address.ip);
	return publicIp ? "ok" : "forbidden_address";
}

/** [network as u32, prefix length] */
const IPV4_BLOCKED: readonly (readonly [number, number])[] = [
	[0x00000000, 8], // 0.0.0.0/8 "this network"
	[0x0a000000, 8], // 10.0.0.0/8 private
	[0x64400000, 10], // 100.64.0.0/10 CGNAT
	[0x7f000000, 8], // 127.0.0.0/8 loopback
	[0xa9fe0000, 16], // 169.254.0.0/16 link-local
	[0xac100000, 12], // 172.16.0.0/12 private
	[0xc0000000, 24], // 192.0.0.0/24 IETF protocol assignments
	[0xc0000200, 24], // 192.0.2.0/24 TEST-NET-1
	[0xc0586300, 24], // 192.88.99.0/24 6to4 relay anycast
	[0xc0a80000, 16], // 192.168.0.0/16 private
	[0xc6120000, 15], // 198.18.0.0/15 benchmarking
	[0xc6336400, 24], // 198.51.100.0/24 TEST-NET-2
	[0xcb007100, 24], // 203.0.113.0/24 TEST-NET-3
	[0xe0000000, 4], // 224.0.0.0/4 multicast
	[0xf0000000, 4], // 240.0.0.0/4 reserved + 255.255.255.255 broadcast
];

export function isPublicIpv4(ip: Uint8Array): boolean {
	if (ip.length !== 4) return false;
	const n = ((ip[0] << 24) | (ip[1] << 16) | (ip[2] << 8) | ip[3]) >>> 0;
	for (const [base, bits] of IPV4_BLOCKED) {
		const mask = bits === 0 ? 0 : (0xffffffff << (32 - bits)) >>> 0;
		if ((n & mask) >>> 0 === base) return false;
	}
	return true;
}

function prefixEquals(ip: Uint8Array, prefix: readonly number[], bits: number): boolean {
	for (let i = 0; i < bits; i++) {
		const byte = i >> 3;
		const bit = 0x80 >> (i & 7);
		if ((ip[byte] & bit) !== ((prefix[byte] ?? 0) & bit)) return false;
	}
	return true;
}

export function isPublicIpv6(ip: Uint8Array): boolean {
	if (ip.length !== 16) return false;
	const zeros = (from: number, to: number) => ip.subarray(from, to).every((b) => b === 0);
	// ::/96 — unspecified, loopback, IPv4-compatible (deprecated): never a peer.
	if (zeros(0, 12)) return false;
	// ::ffff:0:0/96 IPv4-mapped → judge the IPv4 address.
	if (zeros(0, 10) && ip[10] === 0xff && ip[11] === 0xff) return isPublicIpv4(ip.subarray(12));
	// 64:ff9b::/96 NAT64 → judge the embedded IPv4.
	if (prefixEquals(ip, [0x00, 0x64, 0xff, 0x9b], 32) && zeros(4, 12)) return isPublicIpv4(ip.subarray(12));
	// 64:ff9b:1::/48 local-use NAT64.
	if (prefixEquals(ip, [0x00, 0x64, 0xff, 0x9b, 0x00, 0x01], 48)) return false;
	// 2002::/16 6to4 → judge the embedded IPv4.
	if (ip[0] === 0x20 && ip[1] === 0x02) return isPublicIpv4(ip.subarray(2, 6));
	const blocked: readonly (readonly [readonly number[], number])[] = [
		[[0x01, 0x00], 64], // 100::/64 discard
		[[0x20, 0x01, 0x00, 0x00], 32], // 2001::/32 Teredo (embeds arbitrary IPv4)
		[[0x20, 0x01, 0x00, 0x02, 0x00, 0x00], 48], // 2001:2::/48 benchmarking
		[[0x20, 0x01, 0x0d, 0xb8], 32], // 2001:db8::/32 documentation
		[[0x20, 0x01, 0x00, 0x10], 28], // 2001:10::/28 ORCHID
		[[0x20, 0x01, 0x00, 0x20], 28], // 2001:20::/28 ORCHIDv2
		[[0x3f, 0xff], 20], // 3fff::/20 documentation
		[[0x5f, 0x00], 16], // 5f00::/16 SRv6 SIDs
		[[0xfc, 0x00], 7], // fc00::/7 unique local
		[[0xfe, 0x80], 10], // fe80::/10 link-local
		[[0xfe, 0xc0], 10], // fec0::/10 site-local (deprecated)
		[[0xff, 0x00], 8], // ff00::/8 multicast
	];
	for (const [prefix, bits] of blocked) if (prefixEquals(ip, prefix, bits)) return false;
	// Only global unicast (2000::/3) is reachable on the Internet.
	return (ip[0] & 0xe0) === 0x20;
}
