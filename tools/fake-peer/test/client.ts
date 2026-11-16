// A tiny TCP leecher for testing the fake peer: handshake, extension
// handshake, interested, then requests every block it may and checks each
// piece's SHA-1.

import { createHash } from "node:crypto";
import net from "node:net";
import { bdecode, bencode, MSG, type FakeTorrent } from "../src/index.ts";

const BLOCK = 16 * 1024;
const OUR_EXT = { ut_metadata: 3, ut_pex: 4 };

export interface ClientResult {
	handshake?: Buffer;
	extHandshake?: Map<string, unknown>;
	bitfield?: Buffer;
	haveAll: boolean;
	haveNone: boolean;
	unchoked: boolean;
	choked: boolean;
	rejects: number;
	pieces: Map<number, boolean>; // piece -> hash ok
	metadata?: Buffer;
	metadataRejected: boolean;
	pex?: Map<string, unknown>;
	closed: boolean;
}

export interface ClientOptions {
	port: number;
	host?: string;
	infoHash?: Uint8Array;
	/** Fetch the info dict over ut_metadata instead of blocks. */
	metadata?: boolean;
	/** Stop after this long (ms). */
	timeoutMs?: number;
	/** Only request these pieces (default: every piece the peer has). */
	pieces?: number[];
}

export function download(torrent: FakeTorrent, opts: ClientOptions): Promise<ClientResult> {
	const result: ClientResult = { haveAll: false, haveNone: false, unchoked: false, choked: false, rejects: 0, pieces: new Map(), metadataRejected: false, closed: false };
	return new Promise((resolve) => {
		const socket = net.connect({ port: opts.port, host: opts.host ?? "127.0.0.1" });
		let buf = Buffer.alloc(0);
		const blocks = new Map<number, Buffer[]>();
		const metaChunks: Buffer[] = [];
		let wanted: number[] = [];
		const done = () => {
			clearTimeout(timer);
			socket.destroy();
			resolve(result);
		};
		const timer = setTimeout(done, opts.timeoutMs ?? 5_000);
		socket.on("error", () => {});
		socket.on("close", () => {
			result.closed = true;
			done();
		});
		const send = (id: number, payload: Uint8Array = new Uint8Array(0)) => {
			const b = Buffer.alloc(5 + payload.length);
			b.writeUInt32BE(1 + payload.length);
			b[4] = id;
			b.set(payload, 5);
			socket.write(b);
		};
		const has = (p: number) => result.haveAll || (result.bitfield ? (result.bitfield[p >> 3] & (0x80 >> (p & 7))) !== 0 : false);
		const pieceSize = (p: number) => Math.min(torrent.pieceLength, torrent.totalLength - p * torrent.pieceLength);
		const requestAll = () => {
			wanted = (opts.pieces ?? [...Array(torrent.pieceCount).keys()]).filter(has);
			if (wanted.length === 0) return done();
			for (const p of wanted) {
				for (let begin = 0; begin < pieceSize(p); begin += BLOCK) {
					const r = Buffer.alloc(12);
					r.writeUInt32BE(p, 0);
					r.writeUInt32BE(begin, 4);
					r.writeUInt32BE(Math.min(BLOCK, pieceSize(p) - begin), 8);
					send(MSG.REQUEST, r);
				}
			}
		};
		socket.on("connect", () => {
			const hs = Buffer.alloc(68);
			hs[0] = 19;
			hs.write("BitTorrent protocol", 1, "latin1");
			hs[25] = 0x10;
			hs[27] = 0x04;
			Buffer.from(opts.infoHash ?? torrent.infoHash).copy(hs, 28);
			Buffer.from("-TC0001-000000000000").copy(hs, 48);
			socket.write(hs);
		});
		socket.on("data", (chunk) => {
			buf = Buffer.concat([buf, chunk]);
			if (!result.handshake) {
				if (buf.length < 68) return;
				result.handshake = buf.subarray(0, 68);
				buf = buf.subarray(68);
				// BEP 6: with fast on both sides our first message is have-none.
				if (result.handshake[27] & 0x04) send(MSG.HAVE_NONE);
				send(MSG.EXTENDED, Buffer.concat([Buffer.of(0), Buffer.from(bencode({ m: OUR_EXT }))]));
				if (!opts.metadata) send(MSG.INTERESTED);
			}
			while (buf.length >= 4 && buf.length >= 4 + buf.readUInt32BE(0)) {
				const len = buf.readUInt32BE(0);
				const id = len ? buf[4] : -1;
				const p = buf.subarray(5, 4 + len);
				buf = buf.subarray(4 + len);
				if (id === MSG.BITFIELD) result.bitfield = Buffer.from(p);
				else if (id === MSG.HAVE_ALL) result.haveAll = true;
				else if (id === MSG.HAVE_NONE) result.haveNone = true;
				else if (id === MSG.UNCHOKE) {
					result.unchoked = true;
					requestAll();
				} else if (id === MSG.CHOKE) result.choked = true;
				else if (id === MSG.REJECT) result.rejects++;
				else if (id === MSG.PIECE) {
					const piece = p.readUInt32BE(0);
					const begin = p.readUInt32BE(4);
					const list = blocks.get(piece) ?? [];
					list[begin / BLOCK] = Buffer.from(p.subarray(8));
					blocks.set(piece, list);
					const received = list.filter(Boolean).reduce((n, b) => n + b.length, 0);
					if (received === pieceSize(piece)) {
						const hash = createHash("sha1").update(Buffer.concat(list)).digest();
						result.pieces.set(piece, hash.equals(torrent.hashes[piece]));
						if (result.pieces.size === wanted.length) done();
					}
				} else if (id === MSG.EXTENDED) {
					const extId = p[0];
					const decoded = bdecode(p.subarray(1));
					if (!decoded || !(decoded.value instanceof Map)) continue;
					if (extId === 0) {
						result.extHandshake = decoded.value;
						if (opts.metadata) {
							const m = decoded.value.get("m") as Map<string, number>;
							const size = decoded.value.get("metadata_size") as number;
							for (let i = 0; i < Math.ceil(size / BLOCK); i++) {
								send(MSG.EXTENDED, Buffer.concat([Buffer.of(m.get("ut_metadata") ?? 0), Buffer.from(bencode({ msg_type: 0, piece: i }))]));
							}
						}
					} else if (extId === OUR_EXT.ut_metadata) {
						const type = decoded.value.get("msg_type");
						if (type === 2) {
							result.metadataRejected = true;
							done();
							continue;
						}
						const piece = decoded.value.get("piece") as number;
						const total = decoded.value.get("total_size") as number;
						metaChunks[piece] = Buffer.from(p.subarray(1 + decoded.end));
						const have = metaChunks.filter(Boolean).reduce((n, b) => n + b.length, 0);
						if (have === total) {
							result.metadata = Buffer.concat(metaChunks);
							done();
						}
					} else if (extId === OUR_EXT.ut_pex) {
						result.pex = decoded.value;
					}
				}
			}
		});
	});
}
