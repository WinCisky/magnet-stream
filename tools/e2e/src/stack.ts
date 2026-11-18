// Local stack for a manual session: fake peer, /peers stub, wrangler dev.
// Prints the magnet and the URLs the page needs. Ctrl-C stops everything.
//
//   npm run stack
//   PUBLIC_SEEDERS_URL=... PUBLIC_WORKER_URL=... npm run dev -w apps/web

import { makeTorrent, startFakePeer } from "@magnet-stream/fake-peer";
import { startPeersStub, startWranglerDev } from "@magnet-stream/harness";

const torrent = makeTorrent({ seed: 1, name: "clip.mkv", size: 8 * 1024 * 1024, pieceLength: 256 * 1024 });
const peer = await startFakePeer({ torrent });
const stub = await startPeersStub([peer.addr]);
const dev = await startWranglerDev({ vars: { ALLOW_PRIVATE_PEERS: "1" } });
console.log(
	JSON.stringify(
		{
			magnet: peer.magnet,
			peers: stub.url,
			gateway: dev.url,
			peer: peer.addr,
		},
		null,
		2,
	),
);
console.log("stack up; Ctrl-C to stop");
const stop = async () => {
	await peer.close();
	await stub.close();
	await dev.stop();
	process.exit(0);
};
process.on("SIGINT", () => void stop());
process.on("SIGTERM", () => void stop());
