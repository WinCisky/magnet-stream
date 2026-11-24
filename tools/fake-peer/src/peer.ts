// A scriptable BitTorrent peer for tests: serves a FakeTorrent over the
// peer-wire protocol on TCP, with configurable misbehaviour. Listens on
// loopback and an ephemeral port unless told otherwise. With `listen:
// false` it is reachable only by its own outbound connections (a peer
// behind NAT): it dials `connectTo` and the peers an HTTP `tracker` names,
// and starts the handshake itself.

import { randomBytes } from "node:crypto";
import net from "node:net";
import { bdecode, bencode, concat, type BDecoded } from "./bencode.ts";
import type { FakeTorrent } from "./torrent.ts";

export const MSG = {
	CHOKE: 0,
	UNCHOKE: 1,
	INTERESTED: 2,
	NOT_INTERESTED: 3,
	HAVE: 4,
	BITFIELD: 5,
	REQUEST: 6,
	PIECE: 7,
	CANCEL: 8,
	HAVE_ALL: 0x0e,
	HAVE_NONE: 0x0f,
	REJECT: 0x10,
	EXTENDED: 20,
	HASH_REQUEST: 21,
	HASHES: 22,
	HASH_REJECT: 23,
} as const;

/** Extension ids this peer assigns in its BEP 10 handshake (`m`). */
export const OUR_EXTENSIONS = { ut_metadata: 1, ut_pex: 2 } as const;
export const METADATA_PIECE = 16 * 1024;
const MAX_MESSAGE = 1 << 20;
const MAX_REQUEST = 128 * 1024;
const PROTOCOL = Buffer.from("BitTorrent protocol", "latin1");

export interface PeerBehaviour {
	/** Listen address (default 127.0.0.1). */
	host?: string;
	/** Listen port (default 0: ephemeral). */
	port?: number;
	/** Pieces held: all (default), none, or a list (partial peer, sent as a bitfield). */
	have?: "all" | "none" | readonly number[];
	/** BEP 6 fast extension (default true): have-all/none, reject. */
	fast?: boolean;
	/** BEP 10 extension protocol (default true): reqq, ut_metadata, ut_pex. */
	extensions?: boolean;
	/** Request queue depth advertised in the extension handshake (default 500). */
	reqq?: number;
	/** Unchoke on interested (default true). false: never unchoke. */
	unchoke?: boolean;
	/** Delay before the unchoke (default 0). */
	unchokeDelayMs?: number;
	/** Minimum gap between served blocks, so a download lasts long enough to interrupt. */
	blockGapMs?: number;
	/** Choke (and reject, with fast) once this many blocks were served. */
	chokeAfterBlocks?: number;
	/** Hang up right after the handshake. */
	closeAfterHandshake?: boolean;
	/** Hang up once this many blocks were served. */
	closeAfterBlocks?: number;
	/** Serve corrupted bytes: for every piece (true) or the listed ones. */
	corrupt?: boolean | readonly number[];
	/** Answer the handshake with another info-hash. */
	wrongInfoHash?: boolean;
	/** Wait this long before answering the handshake. */
	handshakeDelayMs?: number;
	/** Never answer the handshake (the connection stays open, silent). */
	silent?: boolean;
	/** Answer with these bytes instead of a handshake (a non-BitTorrent server). */
	respondWith?: Uint8Array;
	/** Serve the info dict over ut_metadata, BEP 9 (default true). */
	metadata?: boolean;
	/** Serve an info dict with one byte flipped (its hash no longer matches). */
	metadataCorrupt?: boolean;
	/** Hang up after serving this many ut_metadata pieces. */
	closeAfterMetadataPieces?: number;
	/** Advertise this `metadata_size` instead of the real one. */
	metadataSize?: number;
	/** BEP 52 hash requests: answer (default), reject them, or answer with corrupted hashes. */
	hashes?: "serve" | "reject" | "corrupt";
	/** Addresses (`ip:port`, `[ip6]:port`) to announce with ut_pex, BEP 11. */
	pex?: readonly string[];
	/** 20-byte peer id (default `-FP0001-` + random). */
	peerId?: Uint8Array;
	/**
	 * Tit-for-tat leecher (M3-12): never unchoke on interest alone; unchoke a
	 * connection once it uploaded `titForTatBytes` of good blocks to us. It
	 * is interested in what the remote has and it lacks (`have`), and
	 * requests it while unchoked.
	 */
	titForTat?: boolean;
	/** Bytes a connection must upload to a tit-for-tat leecher before it unchokes it (default 16 KiB). */
	titForTatBytes?: number;
	/** Requests a tit-for-tat leecher keeps outstanding per connection (default 8). */
	leechQueue?: number;
	/** Stop reading right after answering the handshake (a peer that never reads: its TCP window fills). */
	noRead?: boolean;
	/** Accept inbound connections (default true). false: outbound only, like a peer behind NAT. */
	listen?: boolean;
	/** Dial these addresses (`ip:port`) and serve them, starting the handshake. */
	connectTo?: readonly string[];
	/** HTTP tracker announce URL: announce, then dial every peer it returns (once per address). */
	tracker?: string;
	/** Re-announce period to `tracker` (default 500 ms). */
	announceIntervalMs?: number;
}

/** What a peer received from the other side (all connections). */
export interface ReceivedStats {
	bitfield: number;
	have: number;
	haveAll: number;
	haveNone: number;
	unchoke: number;
	choke: number;
	interested: number;
	piece: number;
	reject: number;
	/** Payload bytes of `piece` messages received. */
	bytes: number;
	/** Blocks received that differ from the torrent's data. */
	badBlocks: number;
	/** ut_metadata pieces received (data). */
	metadataPieces: number;
}

export interface PeerStats {
	connections: number;
	closed: number;
	handshakes: number;
	requests: number;
	served: number;
	rejected: number;
	/** Ids of the first two messages each connection received after the handshake. */
	firstMessages: number[];
	metadataRequests: number;
	hashRequests: number;
	pexSent: number;
	/** Messages received that this peer never expects from a leecher that doesn't upload (piece, unchoke...). */
	unexpected: number[];
	/** `cancel` messages received. */
	cancels: number;
	/** Delayed blocks (blockGapMs) not sent because they were cancelled first. */
	cancelled: number;
	/** Piece of every block sent, in order (the first 100,000). */
	servedPieces: number[];
	/** Messages received from the other side. */
	received: ReceivedStats;
	/** Tit-for-tat leecher: connections it unchoked because they uploaded to it. */
	titForTatUnchokes: number;
}

export interface FakePeer {
	host: string;
	/** 0 when it does not listen. */
	port: number;
	/** `127.0.0.1:6881` or `[::1]:6881`; empty when it does not listen. */
	addr: string;
	/** Magnet with an `x.pe` hint pointing at this peer. */
	magnet: string;
	torrent: FakeTorrent;
	stats: PeerStats;
	close(): Promise<void>;
}

export interface StartOptions extends PeerBehaviour {
	torrent: FakeTorrent;
}

export function frame(id: number, payload: Uint8Array = new Uint8Array(0)): Buffer {
	const buf = Buffer.alloc(5 + payload.length);
	buf.writeUInt32BE(payload.length + 1, 0);
	buf[4] = id;
	buf.set(payload, 5);
	return buf;
}

export function extended(extId: number, dict: Parameters<typeof bencode>[0], trailer?: Uint8Array): Buffer {
	const body = bencode(dict);
	return frame(MSG.EXTENDED, concat([Uint8Array.of(extId), body, trailer ?? new Uint8Array(0)]));
}

/** Start a fake peer serving `torrent`. */
export async function startFakePeer(options: StartOptions): Promise<FakePeer> {
	const { torrent } = options;
	const b = {
		host: "127.0.0.1",
		port: 0,
		have: "all" as const,
		fast: true,
		extensions: true,
		reqq: 500,
		unchoke: true,
		unchokeDelayMs: 0,
		metadata: true,
		...options,
	};
	const peerId = b.peerId ?? concat([Buffer.from("-FP0001-", "latin1"), randomBytes(12)]);
	if (peerId.length !== 20) throw new RangeError("peerId must be 20 bytes");
	const holds = havePredicate(b.have, torrent.pieceCount);
	const servedInfo = b.metadataCorrupt ? Uint8Array.from(torrent.info, (x, i) => (i === torrent.info.length >> 1 ? x ^ 0xff : x)) : torrent.info;
	const corrupt =
		b.corrupt === true ? () => true : Array.isArray(b.corrupt) ? (p: number) => (b.corrupt as readonly number[]).includes(p) : () => false;

	const stats: PeerStats = {
		connections: 0,
		closed: 0,
		handshakes: 0,
		requests: 0,
		served: 0,
		rejected: 0,
		firstMessages: [],
		metadataRequests: 0,
		hashRequests: 0,
		pexSent: 0,
		unexpected: [],
		cancels: 0,
		cancelled: 0,
		servedPieces: [],
		received: { bitfield: 0, have: 0, haveAll: 0, haveNone: 0, unchoke: 0, choke: 0, interested: 0, piece: 0, reject: 0, bytes: 0, badBlocks: 0, metadataPieces: 0 },
		titForTatUnchokes: 0,
	};
	const received = stats.received;
	const sockets = new Set<net.Socket>();

	const handle = (socket: net.Socket, initiator: boolean) => {
		stats.connections++;
		sockets.add(socket);
		socket.setNoDelay(true);
		socket.on("close", () => {
			stats.closed++;
			sockets.delete(socket);
		});
		socket.on("error", () => {});
		const timers = new Set<ReturnType<typeof setTimeout>>();
		socket.on("close", () => timers.forEach(clearTimeout));
		const later = (ms: number, fn: () => void) => {
			const t = setTimeout(() => {
				timers.delete(t);
				fn();
			}, ms);
			timers.add(t);
		};

		let buf: Buffer = Buffer.alloc(0);
		let handshaken = false;
		let unchoked = false;
		let messages = 0;
		let remoteExt: Map<string, BDecoded> | undefined;
		let served = 0;
		let metadataServed = 0;
		/** Paced blocks (blockGapMs) not sent yet, in request order. */
		const queue: { key: string; deliver: () => void }[] = [];
		let pumping = false;
		// Tit-for-tat leecher: what the remote has, what we asked of it, what it gave us.
		let remoteInterested = false;
		let remoteAll = false;
		let remoteBits: Uint8Array | undefined;
		const remoteHaves = new Set<number>();
		let remoteChoking = true;
		let leechInterested = false;
		const asked = new Set<string>();
		const got = new Set<string>();
		let uploadedToUs = 0;

		const send = (data: Uint8Array) => {
			if (!socket.destroyed && socket.writable) socket.write(data);
		};

		const sendSetup = () => {
			if (b.extensions) {
				send(
					extended(0, {
						m: {
							...(b.metadata ? { ut_metadata: OUR_EXTENSIONS.ut_metadata } : {}),
							...(b.pex ? { ut_pex: OUR_EXTENSIONS.ut_pex } : {}),
						},
						reqq: b.reqq,
						v: "FakePeer/1.0",
						...(b.metadata ? { metadata_size: b.metadataSize ?? torrent.info.length } : {}),
					}),
				);
			}
			if (b.have === "all" && b.fast) send(frame(MSG.HAVE_ALL));
			else if (b.have === "none" && b.fast) send(frame(MSG.HAVE_NONE));
			else if (b.have !== "none") {
				const bits = Buffer.alloc(Math.ceil(torrent.pieceCount / 8));
				for (let p = 0; p < torrent.pieceCount; p++) if (holds(p)) bits[p >> 3] |= 0x80 >> (p & 7);
				send(frame(MSG.BITFIELD, bits));
			}
		};

		const writeHandshake = () => {
			const reply = Buffer.alloc(68);
			reply[0] = 19;
			PROTOCOL.copy(reply, 1);
			if (b.extensions) reply[25] |= 0x10;
			if (b.fast) reply[27] |= 0x04;
			if (torrent.version !== 1) reply[27] |= 0x10; // BEP 52 v2 bit
			(b.wrongInfoHash ? randomBytes(20) : Buffer.from(torrent.infoHash)).copy(reply, 28);
			Buffer.from(peerId).copy(reply, 48);
			send(reply);
		};

		const afterHandshake = () => {
			if (b.closeAfterHandshake) {
				socket.end();
				return;
			}
			sendSetup();
			if (b.noRead) socket.pause();
		};

		const answerHandshake = () => {
			if (b.respondWith) {
				send(b.respondWith);
				return;
			}
			writeHandshake();
			afterHandshake();
		};

		// The initiator speaks first; the remote's handshake then completes it.
		if (initiator) writeHandshake();

		const onExtended = (payload: Buffer) => {
			if (payload.length < 1) return;
			const id = payload[0];
			const decoded = bdecode(payload.subarray(1));
			if (!decoded || !(decoded.value instanceof Map)) return;
			const dict = decoded.value;
			if (id === 0) {
				const m = dict.get("m");
				remoteExt = m instanceof Map ? m : new Map();
				const pexId = remoteExt.get("ut_pex");
				if (b.pex && typeof pexId === "number" && pexId > 0) {
					send(extended(pexId, pexMessage(b.pex)));
					stats.pexSent++;
				}
				return;
			}
			if (id === OUR_EXTENSIONS.ut_metadata && dict.get("msg_type") === 1) {
				received.metadataPieces++;
				return;
			}
			if (id === OUR_EXTENSIONS.ut_metadata) {
				const replyId = remoteExt?.get("ut_metadata");
				const msgType = dict.get("msg_type");
				const piece = dict.get("piece");
				if (typeof replyId !== "number" || replyId <= 0 || msgType !== 0 || typeof piece !== "number") return;
				stats.metadataRequests++;
				const pieces = Math.ceil(torrent.info.length / METADATA_PIECE);
				if (!b.metadata || piece < 0 || piece >= pieces) {
					send(extended(replyId, { msg_type: 2, piece }));
					return;
				}
				const chunk = servedInfo.subarray(piece * METADATA_PIECE, (piece + 1) * METADATA_PIECE);
				send(extended(replyId, { msg_type: 1, piece, total_size: torrent.info.length }, chunk));
				metadataServed++;
				if (b.closeAfterMetadataPieces !== undefined && metadataServed >= b.closeAfterMetadataPieces) socket.end();
			}
		};

		const reject = (payload: Buffer) => {
			stats.rejected++;
			if (b.fast) send(frame(MSG.REJECT, payload.subarray(0, 12)));
		};

		/** BEP 52 hash request: `pieces root[32] base u32 index u32 length u32 proof layers u32`. */
		const onHashRequest = (payload: Buffer) => {
			if (payload.length !== 48) return;
			stats.hashRequests++;
			const answer = hashAnswer(torrent, payload, b.hashes ?? "serve");
			send(answer ? frame(MSG.HASHES, answer) : frame(MSG.HASH_REJECT, payload));
		};

		const onRequest = (payload: Buffer) => {
			if (payload.length !== 12) return;
			stats.requests++;
			const piece = payload.readUInt32BE(0);
			const begin = payload.readUInt32BE(4);
			const length = payload.readUInt32BE(8);
			const pieceSize = piece < torrent.pieceCount ? torrent.pieceSize(piece) : 0;
			if (!unchoked || !holds(piece) || length === 0 || length > MAX_REQUEST || begin + length > pieceSize) {
				reject(payload);
				return;
			}
			if (b.chokeAfterBlocks !== undefined && served >= b.chokeAfterBlocks) {
				unchoked = false;
				send(frame(MSG.CHOKE));
				reject(payload);
				return;
			}
			served++;
			stats.served++;
			const start = piece * torrent.pieceLength + begin;
			let block = torrent.data.subarray(start, start + length);
			if (corrupt(piece)) {
				block = Uint8Array.from(block);
				for (let i = 0; i < block.length; i += 997) block[i] ^= 0xff;
			}
			const header = Buffer.alloc(8);
			header.writeUInt32BE(piece, 0);
			header.writeUInt32BE(begin, 4);
			const pieceFrame = frame(MSG.PIECE, concat([header, block]));
			const deliver = () => {
				if (stats.servedPieces.length < 100_000) stats.servedPieces.push(piece);
				send(pieceFrame);
				if (b.closeAfterBlocks !== undefined && served >= b.closeAfterBlocks) socket.end();
			};
			// Paced: one block per gap, in request order; a cancelled block
			// leaves the queue without using a slot.
			if ((b.blockGapMs ?? 0) > 0) {
				queue.push({ key: `${piece}:${begin}`, deliver });
				if (!pumping) pump();
			} else deliver();
		};

		const pump = () => {
			const next = queue.shift();
			pumping = next !== undefined;
			if (!next) return;
			next.deliver();
			later(b.blockGapMs ?? 0, pump);
		};

		const remoteHas = (p: number) => remoteAll || remoteHaves.has(p) || (remoteBits !== undefined && (remoteBits[p >> 3] & (0x80 >> (p & 7))) !== 0);
		const wanted = (p: number) => !holds(p) && remoteHas(p);

		/** Tit-for-tat leecher: unchoke a connection that uploaded enough to us. */
		const titForTat = () => {
			if (!b.titForTat || unchoked || !remoteInterested || uploadedToUs < (b.titForTatBytes ?? 16 * 1024)) return;
			unchoked = true;
			stats.titForTatUnchokes++;
			send(frame(MSG.UNCHOKE));
		};

		/** Tit-for-tat leecher: be interested in what the remote has and ask for it while unchoked. */
		const leech = () => {
			if (!b.titForTat || socket.destroyed) return;
			let any = false;
			for (let p = 0; p < torrent.pieceCount && !any; p++) any = wanted(p);
			if (any !== leechInterested) {
				leechInterested = any;
				send(frame(any ? MSG.INTERESTED : MSG.NOT_INTERESTED));
			}
			if (remoteChoking || !any) return;
			const limit = b.leechQueue ?? 8;
			const parts: Buffer[] = [];
			for (let p = 0; p < torrent.pieceCount && asked.size < limit; p++) {
				if (!wanted(p)) continue;
				const size = torrent.pieceSize(p);
				for (let begin = 0; begin < size && asked.size < limit; begin += 16 * 1024) {
					const key = `${p}:${begin}`;
					if (asked.has(key) || got.has(key)) continue;
					asked.add(key);
					const req = Buffer.alloc(12);
					req.writeUInt32BE(p, 0);
					req.writeUInt32BE(begin, 4);
					req.writeUInt32BE(Math.min(16 * 1024, size - begin), 8);
					parts.push(frame(MSG.REQUEST, req));
				}
			}
			if (parts.length > 0) send(Buffer.concat(parts));
		};

		const onPiece = (payload: Buffer) => {
			received.piece++;
			if (payload.length < 8) return;
			const piece = payload.readUInt32BE(0);
			const begin = payload.readUInt32BE(4);
			const data = payload.subarray(8);
			received.bytes += data.length;
			const start = piece * torrent.pieceLength + begin;
			const good = piece < torrent.pieceCount && Buffer.from(torrent.data.subarray(start, start + data.length)).equals(data);
			if (!good) received.badBlocks++;
			const key = `${piece}:${begin}`;
			if (!asked.delete(key)) return;
			if (!good) return;
			got.add(key);
			uploadedToUs += data.length;
			titForTat();
			leech();
		};

		const onCancel = (payload: Buffer) => {
			if (payload.length !== 12) return;
			stats.cancels++;
			const key = `${payload.readUInt32BE(0)}:${payload.readUInt32BE(4)}`;
			const at = queue.findIndex((q) => q.key === key);
			if (at < 0) return;
			queue.splice(at, 1);
			stats.cancelled++;
		};

		socket.on("data", (chunk: Buffer) => {
			buf = buf.length === 0 ? chunk : Buffer.concat([buf, chunk]);
			if (!handshaken) {
				if (buf.length < 68) return;
				if (buf[0] !== 19 || !buf.subarray(1, 20).equals(PROTOCOL) || !buf.subarray(28, 48).equals(Buffer.from(torrent.infoHash))) {
					socket.destroy();
					return;
				}
				handshaken = true;
				stats.handshakes++;
				buf = buf.subarray(68);
				if (initiator) afterHandshake();
				else if (b.silent) return;
				else if (b.handshakeDelayMs) later(b.handshakeDelayMs, answerHandshake);
				else answerHandshake();
			}
			while (buf.length >= 4 && !socket.writableEnded && !socket.destroyed) {
				const len = buf.readUInt32BE(0);
				if (len > MAX_MESSAGE) {
					socket.destroy();
					return;
				}
				if (buf.length < 4 + len) break;
				const id = len > 0 ? buf[4] : -1;
				const payload = buf.subarray(5, 4 + len);
				buf = buf.subarray(4 + len);
				if (len === 0) continue;
				if (messages++ < 2) stats.firstMessages.push(id);
				switch (id) {
					case MSG.INTERESTED:
						received.interested++;
						remoteInterested = true;
						if (b.titForTat) titForTat();
						else if (b.unchoke && !unchoked) {
							later(b.unchokeDelayMs, () => {
								unchoked = true;
								send(frame(MSG.UNCHOKE));
							});
						}
						break;
					case MSG.REQUEST:
						onRequest(payload);
						break;
					case MSG.CANCEL:
						onCancel(payload);
						break;
					case MSG.EXTENDED:
						onExtended(payload);
						break;
					case MSG.HASH_REQUEST:
						onHashRequest(payload);
						break;
					case MSG.NOT_INTERESTED:
						remoteInterested = false;
						break;
					case MSG.UNCHOKE:
						stats.unexpected.push(id);
						received.unchoke++;
						remoteChoking = false;
						leech();
						break;
					case MSG.CHOKE:
						received.choke++;
						remoteChoking = true;
						if (!b.fast) asked.clear();
						break;
					case MSG.PIECE:
						stats.unexpected.push(id);
						onPiece(payload);
						break;
					case MSG.REJECT:
						received.reject++;
						if (payload.length === 12) asked.delete(`${payload.readUInt32BE(0)}:${payload.readUInt32BE(4)}`);
						leech();
						break;
					case MSG.HAVE:
						received.have++;
						if (payload.length === 4) remoteHaves.add(payload.readUInt32BE(0));
						leech();
						break;
					case MSG.BITFIELD:
						received.bitfield++;
						remoteBits = Buffer.from(payload);
						leech();
						break;
					case MSG.HAVE_ALL:
						received.haveAll++;
						remoteAll = true;
						leech();
						break;
					case MSG.HAVE_NONE:
						received.haveNone++;
						break;
				}
			}
		});
	};

	const listening = b.listen !== false;
	const server = net.createServer((socket) => handle(socket, false));
	if (listening) {
		await new Promise<void>((resolve, reject) => {
			server.once("error", reject);
			server.listen(b.port, b.host, () => {
				server.off("error", reject);
				resolve();
			});
		});
	}
	const port = listening ? (server.address() as net.AddressInfo).port : 0;
	const addr = !listening ? "" : b.host.includes(":") ? `[${b.host}]:${port}` : `${b.host}:${port}`;

	// Outbound: one connection per address at a time.
	let stopped = false;
	const dialled = new Set<string>();
	const dial = (address: string) => {
		if (stopped || dialled.has(address)) return;
		const m = /^(?:\[([0-9a-fA-F:.]+)\]|([^:]+)):(\d+)$/.exec(address);
		if (!m) return;
		dialled.add(address);
		const socket = net.connect({ host: m[1] ?? m[2], port: Number(m[3]) });
		sockets.add(socket);
		socket.on("error", () => {});
		socket.on("close", () => {
			sockets.delete(socket);
			dialled.delete(address);
		});
		socket.once("connect", () => handle(socket, true));
	};
	for (const a of b.connectTo ?? []) dial(a);
	let announceTimer: ReturnType<typeof setTimeout> | undefined;
	const announce = async () => {
		if (stopped || !b.tracker) return;
		try {
			for (const a of await trackerPeers(b.tracker, torrent.infoHash, peerId, port || 1)) dial(a);
		} catch {}
		if (!stopped) announceTimer = setTimeout(() => void announce(), b.announceIntervalMs ?? 500);
	};
	void announce();

	let closing: Promise<void> | undefined;
	return {
		host: b.host,
		port,
		addr,
		magnet: addr ? `${torrent.magnet}&x.pe=${encodeURIComponent(addr)}` : torrent.magnet,
		torrent,
		stats,
		close: () =>
			(closing ??= new Promise((resolve) => {
				stopped = true;
				clearTimeout(announceTimer);
				for (const s of sockets) s.destroy();
				if (listening) server.close(() => resolve());
				else resolve();
			})),
	};
}

/** One HTTP tracker announce (BEP 3, compact): the IPv4 peers it returns. */
export async function trackerPeers(tracker: string, infoHash: Uint8Array, peerId: Uint8Array, port: number): Promise<string[]> {
	const enc = (bytes: Uint8Array) => Array.from(bytes, (x) => `%${x.toString(16).padStart(2, "0")}`).join("");
	const sep = tracker.includes("?") ? "&" : "?";
	const url = `${tracker}${sep}info_hash=${enc(infoHash)}&peer_id=${enc(peerId)}&port=${port}&uploaded=0&downloaded=0&left=0&compact=1&event=started`;
	const res = await fetch(url, { signal: AbortSignal.timeout(5_000) });
	const body = new Uint8Array(await res.arrayBuffer());
	const decoded = bdecode(body);
	if (!decoded || !(decoded.value instanceof Map)) return [];
	const peers = decoded.value.get("peers");
	if (!(peers instanceof Uint8Array)) return [];
	const out: string[] = [];
	for (let i = 0; i + 6 <= peers.length; i += 6) out.push(`${peers[i]}.${peers[i + 1]}.${peers[i + 2]}.${peers[i + 3]}:${(peers[i + 4] << 8) | peers[i + 5]}`);
	return out;
}

/**
 * The `hashes` payload for a hash request, or undefined to reject it: the
 * request header, `length` hashes of layer `base` from `index`, then up to
 * `proof layers` uncle hashes from the requested subtree's root upwards
 * (the root itself is never sent).
 */
export function hashAnswer(torrent: FakeTorrent, request: Uint8Array, mode: "serve" | "reject" | "corrupt"): Uint8Array | undefined {
	if (mode === "reject" || request.length !== 48) return undefined;
	const view = new DataView(request.buffer, request.byteOffset, request.byteLength);
	const root = Buffer.from(request.subarray(0, 32)).toString("hex");
	const base = view.getUint32(32);
	const index = view.getUint32(36);
	const length = view.getUint32(40);
	const proofLayers = view.getUint32(44);
	const tree = torrent.trees.get(root);
	if (!tree || base >= tree.layers.length - 1) return undefined;
	const layer = tree.layers[base];
	if (length < 2 || length > 512 || (length & (length - 1)) !== 0 || index % length !== 0 || index + length > layer.length) return undefined;
	const out: Uint8Array[] = [request.subarray(0, 48), ...layer.slice(index, index + length)];
	let level = base + Math.log2(length);
	let at = index / length;
	for (let i = 0; i < proofLayers && level < tree.layers.length - 1; i++, level++, at >>= 1) out.push(tree.layers[level][at ^ 1]);
	const bytes = concat(out);
	if (mode === "corrupt") bytes[48] ^= 0xff;
	return bytes;
}

function havePredicate(have: "all" | "none" | readonly number[], pieceCount: number): (piece: number) => boolean {
	if (have === "all") return (p) => p < pieceCount;
	if (have === "none") return () => false;
	const set = new Set(have);
	return (p) => set.has(p);
}

/** ut_pex payload announcing `addrs` (IPv4 in `added`, IPv6 in `added6`). */
export function pexMessage(addrs: readonly string[]): Parameters<typeof bencode>[0] {
	const v4: Uint8Array[] = [];
	const v6: Uint8Array[] = [];
	for (const a of addrs) {
		const m = /^(?:\[([0-9a-fA-F:]+)\]|(\d+\.\d+\.\d+\.\d+)):(\d+)$/.exec(a);
		if (!m) throw new RangeError(`bad pex address ${a}`);
		const port = Buffer.alloc(2);
		port.writeUInt16BE(Number(m[3]));
		if (m[2]) v4.push(concat([Uint8Array.from(m[2].split(".").map(Number)), port]));
		else v6.push(concat([ipv6Bytes(m[1]), port]));
	}
	return {
		added: concat(v4),
		"added.f": new Uint8Array(v4.length).fill(0x02),
		added6: concat(v6),
		"added6.f": new Uint8Array(v6.length).fill(0x02),
		dropped: new Uint8Array(0),
	};
}

function ipv6Bytes(text: string): Uint8Array {
	const [l, r = ""] = text.split("::");
	const left = l ? l.split(":") : [];
	const right = r ? r.split(":") : [];
	const words = text.includes("::") ? [...left, ...Array<string>(8 - left.length - right.length).fill("0"), ...right] : left;
	const out = Buffer.alloc(16);
	words.forEach((w, i) => out.writeUInt16BE(parseInt(w, 16), i * 2));
	return out;
}
