import { describe, expect, it } from "vitest";
import { encodeBencode } from "../src/bencode.ts";
import { ExtensionError, LOCAL_EXTENSIONS, decodeHandshake, decodeMetadataMessage, decodePex, encodeMetadataRequest, encodeOurHandshake } from "../src/extension.ts";

describe("extension protocol", () => {
	it("announces ut_metadata and ut_pex, and no metadata_size (we have nothing to give)", () => {
		const h = decodeHandshake(encodeOurHandshake());
		expect(Object.fromEntries(h.m)).toEqual(LOCAL_EXTENSIONS);
		expect(h.metadataSize).toBeUndefined();
	});

	it("reads a peer's handshake defensively", () => {
		const h = decodeHandshake(encodeBencode({ m: { ut_metadata: 3, ut_pex: 0, bogus: -1, big: 999 }, metadata_size: 31_234, reqq: 500, v: "qBittorrent/4.6\u0001", p: 51413 }));
		expect(Object.fromEntries(h.m)).toEqual({ ut_metadata: 3 });
		expect(h).toMatchObject({ metadataSize: 31_234, reqq: 500, port: 51413 });
		expect(h.client).toBe("qBittorrent/4.6?");
		expect(() => decodeHandshake(new Uint8Array([1, 2, 3]))).toThrow(ExtensionError);
	});

	it("encodes requests and decodes data, reject and request messages", () => {
		expect(decodeMetadataMessage(encodeMetadataRequest(2))).toEqual({ type: "request", piece: 2 });
		expect(decodeMetadataMessage(encodeBencode({ msg_type: 2, piece: 1 }))).toEqual({ type: "reject", piece: 1 });
		const data = new Uint8Array([...encodeBencode({ msg_type: 1, piece: 0, total_size: 5 }), 1, 2, 3, 4, 5]);
		expect(decodeMetadataMessage(data)).toEqual({ type: "data", piece: 0, totalSize: 5, data: Uint8Array.of(1, 2, 3, 4, 5) });
	});

	it("rejects malformed and oversized metadata messages", () => {
		const big = new Uint8Array([...encodeBencode({ msg_type: 1, piece: 0, total_size: 1 }), ...new Uint8Array(16 * 1024 + 1)]);
		for (const bad of [big, encodeBencode({ msg_type: 1, piece: 0, total_size: 5 }), encodeBencode({ msg_type: 7, piece: 0 }), encodeBencode({ msg_type: 0 }), encodeBencode([1])]) {
			expect(() => decodeMetadataMessage(bad)).toThrow(ExtensionError);
		}
	});

	it("decodes compact pex addresses, IPv4 and IPv6, with flags", () => {
		const v6 = new Uint8Array(18);
		v6[0] = 0x20;
		v6[1] = 0x01;
		v6[15] = 1;
		v6[16] = 0x1a;
		v6[17] = 0xe1;
		const msg = decodePex(encodeBencode({ added: Uint8Array.of(1, 2, 3, 4, 0x1a, 0xe1, 5, 6, 7, 8, 0, 80, 9), "added.f": Uint8Array.of(2), added6: v6, dropped: new Uint8Array(0) }));
		expect(msg.added.map((a) => [a.family, Array.from(a.ip).join("."), a.port, a.flags])).toEqual([
			[4, "1.2.3.4", 6881, 2],
			[4, "5.6.7.8", 80, 0],
			[6, "32.1.0.0.0.0.0.0.0.0.0.0.0.0.0.1", 6881, 0],
		]);
	});
});
