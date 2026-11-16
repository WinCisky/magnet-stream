import { describe, expect, it } from "vitest";
import { signToken, verifyToken } from "../src/token";

const IH = "c9e15763f722f23e98a29decdfae341b98d53056";

// Same vectors as magnet-seeders' src/token.rs: the two must agree.
describe("tokens", () => {
	it("match the vectors shared with magnet-seeders", async () => {
		expect(await signToken("test-secret", IH, "1.2.3.4:6881", 1_700_000_000)).toBe(
			"sRR_H4vDqUIPeXLWwQoGjkaZxnZT1mEvVTqga86kR18",
		);
		expect(await signToken("test-secret", IH, "[2001:db8::1]:51413", 1_700_000_000)).toBe(
			"QVnEb2t6E7ja-qO9EejYkj76CkPpvAiRV1XvntzHUVY",
		);
	});

	it("verify only the exact peer, torrent and expiry they were signed for", async () => {
		const token = "sRR_H4vDqUIPeXLWwQoGjkaZxnZT1mEvVTqga86kR18"; // gitleaks:allow (test vector)
		const now = 1_600_000_000;
		expect(await verifyToken("test-secret", IH, "1.2.3.4:6881", 1_700_000_000, token, now)).toBe(true);
		expect(await verifyToken("test-secret", IH, "1.2.3.4:6882", 1_700_000_000, token, now)).toBe(false);
		expect(await verifyToken("test-secret", IH, "1.2.3.4:6881", 1_700_000_001, token, now)).toBe(false);
		expect(await verifyToken("other-secret", IH, "1.2.3.4:6881", 1_700_000_000, token, now)).toBe(false);
		expect(await verifyToken("test-secret", "0".repeat(40), "1.2.3.4:6881", 1_700_000_000, token, now)).toBe(false);
	});

	it("reject expired tokens", async () => {
		const token = "sRR_H4vDqUIPeXLWwQoGjkaZxnZT1mEvVTqga86kR18"; // gitleaks:allow (test vector)
		expect(await verifyToken("test-secret", IH, "1.2.3.4:6881", 1_700_000_000, token, 1_700_000_001)).toBe(false);
	});
});
