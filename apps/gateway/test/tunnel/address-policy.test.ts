import { describe, expect, it } from "vitest";
import { parseIp } from "@magnet-stream/tunnel-protocol";
import { checkAddress } from "../../src/tunnel/address-policy";

function verdict(ip: string, port = 6881, allowPrivate = false) {
	const parsed = parseIp(ip);
	if (!parsed) throw new Error(`bad ip ${ip}`);
	return checkAddress({ ...parsed, port }, allowPrivate);
}

describe("address policy", () => {
	it("allows public IPv4 and IPv6 unicast", () => {
		for (const ip of ["1.1.1.1", "8.8.8.8", "93.184.216.34", "100.63.255.255", "100.128.0.1", "172.32.0.1", "2a00:1450:4001::1", "2606:4700::1111"]) {
			expect(verdict(ip), ip).toBe("ok");
		}
	});

	it("forbids private, loopback, link-local, CGNAT, multicast, reserved IPv4", () => {
		for (const ip of [
			"0.0.0.0", "10.1.2.3", "100.64.0.1", "127.0.0.1", "127.255.255.254", "169.254.169.254", "172.16.0.1", "172.31.255.255",
			"192.0.0.1", "192.0.2.1", "192.168.1.1", "198.18.0.1", "198.51.100.1", "203.0.113.9", "224.0.0.1", "239.255.255.250",
			"240.0.0.1", "255.255.255.255",
		]) {
			expect(verdict(ip), ip).toBe("forbidden_address");
		}
	});

	it("forbids special IPv6 ranges", () => {
		for (const ip of ["::", "::1", "::2", "fc00::1", "fd12:3456::1", "fe80::1", "fec0::1", "ff02::1", "2001:db8::1", "2001::1", "100::1", "3fff::1", "64:ff9b:1::1"]) {
			expect(verdict(ip), ip).toBe("forbidden_address");
		}
	});

	it("judges IPv4 embedded in IPv6 by the IPv4 address", () => {
		expect(verdict("::ffff:127.0.0.1")).toBe("forbidden_address");
		expect(verdict("::ffff:10.0.0.1")).toBe("forbidden_address");
		expect(verdict("::ffff:169.254.169.254")).toBe("forbidden_address");
		expect(verdict("::ffff:8.8.8.8")).toBe("ok");
		expect(verdict("64:ff9b::192.168.0.1")).toBe("forbidden_address");
		expect(verdict("64:ff9b::8.8.8.8")).toBe("ok");
		expect(verdict("2002:c0a8:0101::1")).toBe("forbidden_address"); // 6to4 of 192.168.1.1
		expect(verdict("2002:0808:0808::1")).toBe("ok");
	});

	it("forbids ports below 1024, even with ALLOW_PRIVATE_PEERS", () => {
		expect(verdict("8.8.8.8", 80)).toBe("forbidden_port");
		expect(verdict("8.8.8.8", 1023)).toBe("forbidden_port");
		expect(verdict("8.8.8.8", 1024)).toBe("ok");
		expect(verdict("127.0.0.1", 22, true)).toBe("forbidden_port");
	});

	it("ALLOW_PRIVATE_PEERS lifts the address check only", () => {
		expect(verdict("127.0.0.1", 6881, true)).toBe("ok");
		expect(verdict("::1", 6881, true)).toBe("ok");
	});
});
