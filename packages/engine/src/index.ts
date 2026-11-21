// @magnet-stream/engine: the BitTorrent engine that runs in the browser
// (in a worker) and in Node for tests. Platform APIs only: WebSocket,
// WebCrypto, timers.

export * from "./transport.ts";
export * from "./peer-wire.ts";
export * from "./peer-connection.ts";
export * from "./tunnel-client.ts";
export * from "./download.ts";
export * from "./extension.ts";
export * from "./peer-book.ts";
export * from "./score.ts";
export * from "./slots.ts";
export * from "./upstream.ts";
export * from "./picker.ts";
export * from "./connection-manager.ts";
export * from "./peers-stream.ts";
export * from "./magnet.ts";
export * from "./metadata.ts";
export * from "./verify.ts";
export * from "./hashes.ts";
export * from "./attribution.ts";
export {
	BLOCK_SIZE,
	MetainfoError,
	blockCount,
	blockLength,
	fileRange,
	geometryOf,
	listing,
	parseInfoDict,
	pieceSize,
	sanitizeComponent,
	sha1Hex,
	verifyPiece,
	type ExpectedHashes,
	type FileRange,
	type ListedFile,
	type Listing,
	type Metainfo,
	type MetainfoFile,
	type MetainfoLimits,
	type TorrentVersion,
} from "./metainfo.ts";
export {
	BencodeError,
	decodeBencode,
	decodeBencodePrefix,
	encodeBencode,
	type BencodeDict,
	type BencodeInput,
	type Bencoded,
} from "./bencode.ts";
export * from "./upload.ts";
