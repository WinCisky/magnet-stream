// A Transport is a bidirectional byte stream to one peer, positioned right
// after the BitTorrent handshakes (both sides' 68 bytes are already
// exchanged and checked). The tunnel (TunnelClient) and, in tests, a
// direct TCP socket both provide one; PeerConnection runs on either.

export interface HandshakeInfo {
	/** The remote's 8 reserved bytes (extension bits). */
	reserved: Uint8Array;
	/** The remote's 20-byte peer id. */
	peerId: Uint8Array;
}

export interface TransportHandlers {
	data(bytes: Uint8Array): void;
	/** The stream ended; `error` when it failed rather than closed. */
	close(error?: Error): void;
}

export interface Transport {
	/** The remote's handshake. */
	readonly remote: HandshakeInfo;
	/** The reserved bytes we sent in our handshake. */
	readonly local: { reserved: Uint8Array };
	readonly closed: boolean;
	/** Queue bytes to the peer. */
	send(bytes: Uint8Array): void;
	/** Close the stream (idempotent). */
	close(): void;
	/** Start receiving. Bytes that arrived earlier are delivered first. */
	attach(handlers: TransportHandlers): void;
}

/**
 * Base for transports: buffers input until `attach`, then forwards it, and
 * reports the close exactly once.
 */
export abstract class BufferedTransport implements Transport {
	abstract readonly remote: HandshakeInfo;
	abstract readonly local: { reserved: Uint8Array };
	private handlers: TransportHandlers | undefined;
	private early: Uint8Array[] = [];
	private ended: { error?: Error } | undefined;
	private notified = false;

	get closed(): boolean {
		return this.ended !== undefined;
	}

	abstract send(bytes: Uint8Array): void;
	abstract close(): void;

	attach(handlers: TransportHandlers): void {
		if (this.handlers) throw new Error("transport already attached");
		this.handlers = handlers;
		const early = this.early;
		this.early = [];
		for (const chunk of early) handlers.data(chunk);
		this.notifyClose();
	}

	/** For subclasses: bytes from the peer. */
	protected deliver(bytes: Uint8Array): void {
		if (this.ended) return;
		if (this.handlers) this.handlers.data(bytes);
		else this.early.push(bytes);
	}

	/** For subclasses: the stream is over. */
	protected finish(error?: Error): void {
		if (this.ended) return;
		this.ended = { error };
		this.notifyClose();
	}

	private notifyClose(): void {
		if (!this.ended || !this.handlers || this.notified) return;
		this.notified = true;
		this.handlers.close(this.ended.error);
	}
}
