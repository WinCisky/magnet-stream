// Hermetic end-to-end test of the M1 slice: a fake peer on loopback, the
// gateway under `wrangler dev` (ALLOW_PRIVATE_PEERS=1, local only) and the
// engine downloading a whole torrent through the tunnel, every piece
// verified by SHA-1.
//
//   npm run e2e                     (from the repo root: this, then the gateway suites)
//   E2E_SIZE=64MiB npm run e2e -w tools/e2e
//   E2E_GATEWAY_URL=http://127.0.0.1:8787 npm run e2e -w tools/e2e   (a running wrangler dev with ALLOW_PRIVATE_PEERS=1)
//   E2E_GATEWAY=vps npm run e2e -w tools/e2e   (through the VPS gateway, services/seeders, instead of wrangler dev)

import { randomBytes } from "node:crypto";
import { PeerConnection, TunnelClient, downloadAll, geometryOf, parseInfoDict } from "@magnet-stream/engine";
import { makeTorrent, parseSize, startFakePeer } from "@magnet-stream/fake-peer";
import { gatewayKind, startGateway, type WranglerDev } from "@magnet-stream/harness";

const MiB = 1024 * 1024;
const size = parseSize(process.env.E2E_SIZE ?? "32MiB");
if (size < 32 * MiB && !process.env.E2E_SIZE) throw new Error("the e2e torrent must be at least 32 MiB");

let dev: WranglerDev | undefined;
let failed = false;

async function main(): Promise<void> {
	const torrent = makeTorrent({ seed: 42, size, pieceLength: 256 * 1024 });
	const meta = await parseInfoDict(torrent.info, torrent.infoHashHex);
	const peer = await startFakePeer({ torrent });
	try {
		const base = process.env.E2E_GATEWAY_URL ?? (dev = await startGateway({ vars: { ALLOW_PRIVATE_PEERS: "1" } })).url;
		const client = await TunnelClient.connect(`${base.replace(/^http/, "ws")}/v1/tunnel`);
		const started = Date.now();
		const transport = await client.open({ address: peer.addr, infoHash: torrent.infoHash, peerId: peerId() });
		const conn = new PeerConnection(transport, { geometry: geometryOf(meta) });
		let delivered = 0;
		const result = await downloadAll(conn, meta, {
			onPiece: (piece, data) => {
				// Belt and braces: the engine already checked the SHA-1.
				const expected = torrent.data.subarray(piece * meta.pieceLength, piece * meta.pieceLength + data.length);
				if (!Buffer.from(data).equals(Buffer.from(expected))) throw new Error(`piece ${piece} differs from the source`);
				delivered++;
			},
		});
		const seconds = (Date.now() - started) / 1000;
		conn.close();
		client.close();

		const n = meta.pieceCount;
		console.log(`verified ${result.verified.length}/${n} pieces (${(size / MiB).toFixed(1)} MiB in ${seconds.toFixed(2)} s, ${(size / MiB / seconds).toFixed(1)} MiB/s)`);
		const blocks = Math.ceil(size / 16384);
		console.log(
			`websocket messages: ${client.stats.messagesSent} upstream (${client.stats.framesSent} frames, billed as ~${(1 + client.stats.messagesSent / 20).toFixed(0)} requests), ` +
				`${client.stats.messagesReceived} downstream, for ${blocks} blocks`,
		);
		check(result.verified.length === n && delivered === n, `expected ${n} verified pieces`);
		check(result.corrupt.length === 0 && result.missing.length === 0, "no corrupt or missing pieces");
		// Requests go out in batches: far fewer upstream messages than blocks.
		check(client.stats.messagesSent <= Math.ceil(blocks / 16) + 8, `upstream messages ${client.stats.messagesSent} not batched`);
		check(peer.stats.unexpected.length === 0, "without a piece source the engine uploads nothing (no unchoke/piece sent to the peer)");
		if (dev) {
			// Log hygiene: neither the info-hash nor the peer's address appear in the gateway's logs.
			const logs = dev.output();
			check(!logs.toLowerCase().includes(torrent.infoHashHex), `info-hash absent from ${gatewayKind()} gateway logs`);
			check(!logs.includes(peer.addr), `peer address absent from ${gatewayKind()} gateway logs`);
		}
	} finally {
		await peer.close();
	}
}

function peerId(): Uint8Array {
	return Buffer.concat([Buffer.from("-MS0001-", "latin1"), randomBytes(12)]);
}

function check(ok: boolean, what: string): void {
	if (!ok) {
		failed = true;
		console.error(`FAIL ${what}`);
	}
}

main()
	.catch((e: unknown) => {
		failed = true;
		console.error(e);
		if (dev) console.error(dev.output().slice(-4000));
	})
	.finally(async () => {
		await dev?.stop();
		process.exit(failed ? 1 : 0);
	});
