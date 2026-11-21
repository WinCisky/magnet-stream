// Session-level rules shared by gateways and the client: WebSocket close
// codes, the counters a gateway reports when a WebSocket ends, timeouts and
// the `ping` cadence. Spec: docs/tunnel-protocol.md § Session lifetime.

/** WebSocket close codes used by gateways. */
export const SessionCloseCode = {
	/** The client closed the WebSocket, or the gateway answered its close. */
	NORMAL: 1000,
	/** A frame did not decode, or the client sent a gateway-only frame. */
	PROTOCOL_ERROR: 1002,
	/** A text message. */
	UNSUPPORTED_DATA: 1003,
	/** No message from the client for the idle timeout. */
	IDLE: 4000,
	/** The session reached its maximum length. */
	SESSION_TIMEOUT: 4001,
	/** The gateway is shutting down or failed. */
	SHUTDOWN: 4002,
} as const;

/** Defaults (each gateway may configure its own). */
export const DEFAULT_IDLE_TIMEOUT_MS = 90_000;
export const DEFAULT_SESSION_TIMEOUT_S = 7_200;
/**
 * Bytes a gateway holds for one peer that were not written to it yet: past
 * this, the peer is not reading and its connection is closed with
 * `close` WRITE_OVERFLOW (the browser caps its upload too, OQ-04).
 */
export const DEFAULT_MAX_PENDING_WRITE_BYTES = 4 * 1024 * 1024;
/** WebSockets per session (one Durable Object); each carries its own 6 connections. */
export const DEFAULT_SOCKETS_PER_SESSION = 3;
/**
 * A connect that timed out keeps its slot taken until this long after it
 * started: the platform keeps it pending that long and queues new connects
 * behind it (docs/spike-report.md, OQ-03). Gateways hold the slot; clients
 * count it as taken too, or their next `open` gets TOO_MANY_CONNECTIONS.
 */
export const FAILED_CONNECT_HOLD_MS = 20_000;
/** A slot frees only once the gateway's socket has closed: clients wait this long after a close. */
export const CLOSE_SLOT_GRACE_MS = 1_000;
/**
 * The client sends `ping` when it has sent nothing else for this long, so a
 * quiet but live session never hits the gateway's idle timeout (90 s).
 * Three per idle timeout: one lost or late message does not end the session.
 */
export const PING_INTERVAL_MS = 30_000;

/** Counters a gateway reports in the WebSocket close reason. */
export interface SessionCounters {
	/** WebSocket messages received from the client (billed at 20:1 on a Durable Object). */
	messagesIn: number;
	/** Bytes forwarded client → peers. */
	bytesUp: number;
	/** Bytes forwarded peers → client. */
	bytesDown: number;
}

export interface SessionEnd {
	/** Lowercase reason name, e.g. `normal`, `idle`, `session_timeout`, `protocol_error`. */
	reason: string;
	counters?: SessionCounters;
}

/** Max bytes of a WebSocket close reason (RFC 6455: 125 minus the code). */
export const MAX_CLOSE_REASON = 123;

/** `"<reason> in=<n> up=<n> down=<n>"`, always within MAX_CLOSE_REASON. */
export function formatSessionEnd(end: SessionEnd): string {
	const reason = /^[a-z0-9_]{1,32}$/.test(end.reason) ? end.reason : "internal_error";
	if (!end.counters) return reason;
	const { messagesIn, bytesUp, bytesDown } = end.counters;
	return `${reason} in=${count(messagesIn)} up=${count(bytesUp)} down=${count(bytesDown)}`;
}

/** Inverse of formatSessionEnd; null if the text is not in that format. */
export function parseSessionEnd(text: string): SessionEnd | null {
	if (text.length > MAX_CLOSE_REASON) return null;
	const m = /^([a-z0-9_]{1,32})(?: in=(\d{1,16}) up=(\d{1,16}) down=(\d{1,16}))?$/.exec(text);
	if (!m) return null;
	if (m[2] === undefined) return { reason: m[1] };
	return { reason: m[1], counters: { messagesIn: Number(m[2]), bytesUp: Number(m[3]), bytesDown: Number(m[4]) } };
}

function count(n: number): number {
	return Number.isSafeInteger(n) && n >= 0 ? n : 0;
}
