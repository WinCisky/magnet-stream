// @magnet-stream/engine: the BitTorrent engine that runs in the browser
// (in a worker) and in Node for tests. Platform APIs only: WebSocket,
// WebCrypto, timers. M1 scope: one peer-wire connection over a tunnel and a
// verified download from a single peer.

export * from "./transport.ts";
export * from "./peer-wire.ts";
export * from "./peer-connection.ts";
export * from "./tunnel-client.ts";
export * from "./download.ts";
export {
	BLOCK_SIZE,
	MetainfoError,
	blockCount,
	blockLength,
	fileRange,
	parseInfoDict,
	pieceSize,
	sha1Hex,
	verifyPiece,
	type FileRange,
	type Metainfo,
	type MetainfoFile,
} from "./metainfo.ts";
export { BencodeError, decodeBencode, type BencodeDict, type Bencoded } from "./bencode.ts";
