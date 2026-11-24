// The full local stack of the few-seeders path (docs/architecture.md
// §Chunk recovery › Swarms with few seeders), all on loopback:
//
//   - the Cloudflare gateway under `wrangler dev` (the primary data path)
//   - the VPS gateway (services/seeders) with an inbound address: it
//     announces itself to the trackers a session names and hands inbound
//     peers over; crash() and restart() for "the VPS goes down mid-film"
//   - a fake HTTP tracker, where "behind NAT" fake peers (`listen: false`,
//     `tracker`) find the VPS gateway's announced port and connect in
//
// Nothing reaches the public DHT or public trackers (HERMETIC_VPS_VARS).

import { assertLoopbackUrl, freePort, startVpsGateway, startWranglerDev, type VpsGateway, type WranglerDev } from "./index.ts";
import { startFakeTracker, type FakeTracker } from "./tracker.ts";

export interface FewSeedersStackOptions {
	/** The page's origin, for ALLOWED_ORIGINS on both gateways (unset: none, as for Node clients). */
	origin?: string;
	/** Tracker announce interval it answers, in seconds (default 60). */
	trackerInterval?: number;
	/** More environment for the VPS gateway. */
	vpsVars?: Record<string, string>;
}

export interface FewSeedersStack {
	primary: WranglerDev;
	vps: VpsGateway;
	tracker: FakeTracker;
	/** The VPS gateway's announced port (TCP and uTP), on 127.0.0.1. */
	inboundPort: number;
	/** Announces the VPS gateway made (its announced port) for `infoHashHex`, or for any torrent. */
	vpsAnnounces(infoHashHex?: string): FakeTracker["announces"];
	close(): Promise<void>;
}

export async function startFewSeedersStack(opts: FewSeedersStackOptions = {}): Promise<FewSeedersStack> {
	const common: Record<string, string> = { ALLOW_PRIVATE_PEERS: "1", ...(opts.origin ? { ALLOWED_ORIGINS: opts.origin } : {}) };
	const closers: (() => Promise<void>)[] = [];
	const close = async () => {
		for (const c of closers.reverse()) await c().catch(() => {});
	};
	try {
		const tracker = await startFakeTracker({ interval: opts.trackerInterval });
		assertLoopbackUrl(tracker.url);
		closers.push(() => tracker.close());
		const inboundPort = await freePort();
		const vps = await startVpsGateway({ vars: { ...common, TUNNEL_INBOUND_ADDR: `127.0.0.1:${inboundPort}`, ...opts.vpsVars } });
		closers.push(() => vps.stop());
		const primary = await startWranglerDev({ vars: common });
		closers.push(() => primary.stop());
		return {
			primary,
			vps,
			tracker,
			inboundPort,
			vpsAnnounces: (ih) => tracker.announces.filter((a) => a.port === inboundPort && (ih === undefined || a.infoHash === ih)),
			close,
		};
	} catch (e) {
		await close();
		throw e;
	}
}
