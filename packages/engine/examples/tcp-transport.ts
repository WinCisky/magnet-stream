// Node-only Transport over a direct TCP socket (tests and examples). It does the 68-byte
// handshake itself (the gateway's job in production) and then hands the
// stream to PeerConnection.

import net from "node:net";
import { BufferedTransport, type HandshakeInfo } from "../src/index.ts";

const PROTOCOL = Buffer.from("\x13BitTorrent protocol", "latin1");

export class TcpTransport extends BufferedTransport {
	readonly remote: HandshakeInfo;
	readonly local: { reserved: Uint8Array };
	/** Every byte we sent after the handshake (to check what we upload). */
	readonly sent: Uint8Array[] = [];
	private readonly socket: net.Socket;

	private constructor(socket: net.Socket, remote: HandshakeInfo, reserved: Uint8Array, rest: Uint8Array) {
		super();
		this.socket = socket;
		this.remote = remote;
		this.local = { reserved };
		if (rest.length > 0) this.deliver(rest);
		socket.on("data", (chunk) => this.deliver(new Uint8Array(chunk)));
		socket.on("close", () => this.finish());
		socket.on("error", (e) => this.finish(e));
	}

	static connect(opts: { port: number; host?: string; infoHash: Uint8Array; peerId?: Uint8Array; reserved?: Uint8Array }): Promise<TcpTransport> {
		const reserved = opts.reserved ?? Uint8Array.of(0, 0, 0, 0, 0, 0, 0, 0x04);
		const peerId = opts.peerId ?? Buffer.from("-MS0001-tcptransport", "latin1");
		return new Promise((resolve, reject) => {
			const socket = net.connect({ port: opts.port, host: opts.host ?? "127.0.0.1" });
			let buf = Buffer.alloc(0);
			const onData = (chunk: Buffer) => {
				buf = Buffer.concat([buf, chunk]);
				if (buf.length < 68) return;
				socket.off("data", onData);
				if (!buf.subarray(0, 20).equals(PROTOCOL) || !buf.subarray(28, 48).equals(Buffer.from(opts.infoHash))) {
					socket.destroy();
					reject(new Error("bad handshake"));
					return;
				}
				resolve(new TcpTransport(socket, { reserved: new Uint8Array(buf.subarray(20, 28)), peerId: new Uint8Array(buf.subarray(48, 68)) }, reserved, new Uint8Array(buf.subarray(68))));
			};
			socket.on("data", onData);
			socket.once("error", reject);
			socket.once("connect", () => socket.write(Buffer.concat([PROTOCOL, reserved, opts.infoHash, peerId])));
		});
	}

	send(bytes: Uint8Array): void {
		if (this.closed) return;
		this.sent.push(bytes.slice());
		this.socket.write(bytes);
	}

	close(): void {
		this.socket.destroy();
		this.finish();
	}
}
