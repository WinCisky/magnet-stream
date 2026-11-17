// Wire constants of the tunnel protocol. Byte-level specification:
// docs/tunnel-protocol.md (which refers to docs/architecture.md §Tunnel
// protocol and request budget › Frames).

/** Protocol version, negotiated as the WebSocket subprotocol. */
export const PROTOCOL_VERSION = 1;
/** `Sec-WebSocket-Protocol` value both sides must agree on. */
export const SUBPROTOCOL = `magnet-tunnel.v${PROTOCOL_VERSION}`;

/** `[type u8][conn u16][length u32]` */
export const HEADER_LENGTH = 7;

export const FrameType = {
	OPEN: 0x01,
	OPENED: 0x02,
	FAILED: 0x03,
	DATA: 0x04,
	CLOSE: 0x05,
	ACCEPT: 0x06,
	PING: 0x07,
} as const;
export type FrameTypeCode = (typeof FrameType)[keyof typeof FrameType];

export const FRAME_TYPE_NAMES: Readonly<Record<number, string>> = {
	[FrameType.OPEN]: "open",
	[FrameType.OPENED]: "opened",
	[FrameType.FAILED]: "failed",
	[FrameType.DATA]: "data",
	[FrameType.CLOSE]: "close",
	[FrameType.ACCEPT]: "accept",
	[FrameType.PING]: "ping",
};

/** `conn` 0 is the session itself: only `ping` uses it. */
export const SESSION_CONN = 0;
/** Connections opened by the client (`open`) use 1..0x7fff. */
export const CLIENT_CONN_MIN = 0x0001;
export const CLIENT_CONN_MAX = 0x7fff;
/** Connections announced by the gateway (`accept`, VPS only) use 0x8000..0xffff. */
export const GATEWAY_CONN_MIN = 0x8000;
export const GATEWAY_CONN_MAX = 0xffff;

/** Simultaneous connections (open, opening, or closing) per WebSocket. */
export const MAX_CONNECTIONS_PER_SOCKET = 6;

/** Why an `open` did not produce an `opened`. */
export const FailReason = {
	/** The TCP connection was refused or could not be made. */
	REFUSED: 1,
	/** No TCP connection, or no remote handshake, in time. */
	TIMEOUT: 2,
	/** The remote answered with something that is not a BitTorrent handshake. */
	NOT_BITTORRENT: 3,
	/** The remote handshake names another info-hash. */
	INFOHASH_MISMATCH: 4,
	/** The address is private/reserved/loopback/... or the port is below 1024. */
	FORBIDDEN_ADDRESS: 5,
	/** The WebSocket already has MAX_CONNECTIONS_PER_SOCKET connections. */
	TOO_MANY_CONNECTIONS: 6,
	/** The `conn` id is still in use on this WebSocket. */
	CONN_IN_USE: 7,
	/** The remote closed the connection before completing its handshake. */
	REMOTE_CLOSED: 8,
	/** Anything else (gateway bug, runtime limit). */
	INTERNAL_ERROR: 9,
} as const;
export type FailReasonCode = (typeof FailReason)[keyof typeof FailReason];

/** Why a connection ended. */
export const CloseReason = {
	/** The sender of the `close` asked for it. */
	NORMAL: 0,
	/** The remote peer closed the TCP connection. */
	REMOTE_CLOSED: 1,
	/** The TCP connection failed (reset, write error). */
	REMOTE_ERROR: 2,
	/** The other side broke the protocol for this connection (e.g. `data` before `opened`). */
	PROTOCOL_ERROR: 3,
	/** The gateway is ending the session (limits, shutdown). */
	SESSION_ENDING: 4,
	/** Anything else. */
	INTERNAL_ERROR: 5,
	/** No byte in either direction for the idle timeout (default 90 s). */
	IDLE: 6,
	/** The session reached its maximum length (default 2 h). */
	SESSION_TIMEOUT: 7,
} as const;
export type CloseReasonCode = (typeof CloseReason)[keyof typeof CloseReason];

export const INFO_HASH_LENGTH = 20;
export const PEER_ID_LENGTH = 20;
export const RESERVED_LENGTH = 8;
/** Max opaque payload of a `ping`. */
export const MAX_PING_PAYLOAD = 8;

/** Defaults for the decoder's limits (overridable per decoder). */
export const DEFAULT_MAX_PAYLOAD = 1 << 20;
export const DEFAULT_MAX_FRAMES = 1024;
