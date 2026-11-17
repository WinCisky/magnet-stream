import { describe, expect, it } from "vitest";
import { CloseReason, MAX_CLOSE_REASON, formatSessionEnd, parseSessionEnd } from "../src/index.ts";

describe("session end reason", () => {
	it("round-trips reason and counters", () => {
		const end = { reason: "session_timeout", counters: { messagesIn: 1234, bytesUp: 56_789, bytesDown: 3_221_225_472 } };
		const text = formatSessionEnd(end);
		expect(text).toBe("session_timeout in=1234 up=56789 down=3221225472");
		expect(parseSessionEnd(text)).toEqual(end);
		expect(parseSessionEnd("normal")).toEqual({ reason: "normal" });
	});

	it("stays within the WebSocket close reason limit", () => {
		const max = Number.MAX_SAFE_INTEGER;
		const text = formatSessionEnd({ reason: "x".repeat(32), counters: { messagesIn: max, bytesUp: max, bytesDown: max } });
		expect(new TextEncoder().encode(text).length).toBeLessThanOrEqual(MAX_CLOSE_REASON);
	});

	it("sanitises odd input and rejects garbage", () => {
		expect(formatSessionEnd({ reason: "Bad Reason!" })).toBe("internal_error");
		expect(formatSessionEnd({ reason: "idle", counters: { messagesIn: -1, bytesUp: 1.5, bytesDown: NaN } })).toBe("idle in=0 up=0 down=0");
		for (const bad of ["", "IDLE", "idle in=1", "idle in=a up=1 down=2", "x".repeat(200)]) expect(parseSessionEnd(bad)).toBeNull();
	});

	it("has close reasons for idle and session timeout", () => {
		expect(CloseReason.IDLE).toBe(6);
		expect(CloseReason.SESSION_TIMEOUT).toBe(7);
	});
});
