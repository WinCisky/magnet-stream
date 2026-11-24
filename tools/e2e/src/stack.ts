// Local stack for a manual session: fake peer, /peers stub, wrangler dev.
// Prints the magnet and the URLs the page needs. Ctrl-C stops everything.
//
//   npm run stack
//   PUBLIC_SEEDERS_URL=... PUBLIC_WORKER_URL=... npm run dev -w apps/web
//
// `npm run stack -w tools/e2e -- --nat`: the "only NATed peers" scenario (M4-06). The
// only seed of a real MP4 sits behind "NAT" (it dials out to the peers a
// fake tracker names, slower than the video); the /peers stub gives only
// the info dict; the VPS gateway (services/seeders) accepts inbound peers.
// Open the magnet in the dev server started with the printed variables:
// after a few seconds the page asks the VPS to join the swarm, the seed
// connects in, and the page says the swarm is slower than the video.

import { readFileSync } from "node:fs";
import { makeTorrent, startFakePeer } from "@magnet-stream/fake-peer";
import { freePort, startFakeTracker, startPeersStub, startVpsGateway, startWranglerDev } from "@magnet-stream/harness";
import { spreadSamples } from "./mp4-spread.ts";

const nat = process.argv.includes("--nat");
const stops: (() => Promise<void>)[] = [];

if (!nat) {
	const torrent = makeTorrent({ seed: 1, name: "clip.mkv", size: 8 * 1024 * 1024, pieceLength: 256 * 1024 });
	const peer = await startFakePeer({ torrent });
	const stub = await startPeersStub([peer.addr]);
	const dev = await startWranglerDev({ vars: { ALLOW_PRIVATE_PEERS: "1" } });
	stops.push(() => peer.close(), () => stub.close(), () => dev.stop());
	console.log(JSON.stringify({ magnet: peer.magnet, peers: stub.url, gateway: dev.url, peer: peer.addr }, null, 2));
} else {
	const origin = "http://localhost:4321";
	const tracker = await startFakeTracker();
	const content = spreadSamples(new Uint8Array(readFileSync(new URL("../fixtures/clip-vp9.mp4", import.meta.url))), 16 * 1024);
	const torrent = makeTorrent({ content, name: "clip.mp4", pieceLength: 256 * 1024 });
	const seed = await startFakePeer({ torrent, listen: false, tracker: tracker.url, announceIntervalMs: 1_000, blockGapMs: 60 });
	const stub = await startPeersStub([{ info: torrent.info }]);
	const dev = await startWranglerDev({ vars: { ALLOW_PRIVATE_PEERS: "1", ALLOWED_ORIGINS: origin } });
	const inbound = await freePort();
	const vps = await startVpsGateway({
		vars: { ALLOW_PRIVATE_PEERS: "1", ALLOWED_ORIGINS: origin, TUNNEL_INBOUND_ADDR: `127.0.0.1:${inbound}`, TUNNEL_ANNOUNCE_DHT: "0", DEFAULT_TRACKERS: "" },
	});
	stops.push(() => seed.close(), () => stub.close(), () => tracker.close(), () => dev.stop(), () => vps.stop());
	const magnet = `magnet:?xt=urn:btih:${torrent.infoHashHex}&dn=clip.mp4&tr=${encodeURIComponent(tracker.url)}`;
	console.log(JSON.stringify({ scenario: "only NATed peers", magnet, peers: stub.url, gateway: dev.url, vpsGateway: vps.url, tracker: tracker.url }, null, 2));
	console.log(
		`\nPUBLIC_SEEDERS_URL=${stub.url} PUBLIC_WORKER_URL=${dev.url} PUBLIC_VPS_GATEWAY_URL=${vps.url} ` +
			"PUBLIC_ESCALATION_AFTER_SECONDS=10 PUBLIC_SLOW_NOTICE_SECONDS=5 npm run dev -w apps/web",
	);
}
console.log("stack up; Ctrl-C to stop");
const stop = async () => {
	for (const s of stops) await s().catch(() => {});
	process.exit(0);
};
process.on("SIGINT", () => void stop());
process.on("SIGTERM", () => void stop());
