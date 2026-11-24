// The site as deployed, for the browser e2e sessions: a production build
// with the service URLs baked in, served the way Cloudflare Pages serves it,
// and the Chromium to open it with.

import { spawnSync } from "node:child_process";
import { existsSync, readFileSync, statSync } from "node:fs";
import { createServer, type Server } from "node:http";
import { extname, join, resolve } from "node:path";
import { chromium } from "playwright-core";
import { closedPort, startVpsGateway, startWranglerDev, type WranglerDev } from "@magnet-stream/harness";

export const ROOT = new URL("../../../", import.meta.url).pathname;

/** Playwright's own Chromium (`npx playwright-core install chromium`), if installed. */
function bundledChromium(): string | undefined {
	try {
		return chromium.executablePath();
	} catch {
		return undefined;
	}
}

/** A Chromium to drive: CHROMIUM_PATH, the system's, or Playwright's. */
export function findChromium(): string | undefined {
	return [process.env.CHROMIUM_PATH, "/usr/bin/chromium", "/usr/bin/chromium-browser", "/usr/bin/google-chrome", bundledChromium()].find((p) => p && existsSync(p));
}

export type SiteGatewayKind = "cloudflare" | "vps";

/**
 * The site's primary gateway for the browser sessions: PUBLIC_DEFAULT_GATEWAY
 * as the build reads it (docs/operations.md › Default data path), `cloudflare`
 * when unset.
 */
export function siteGatewayKind(): SiteGatewayKind {
	const kind = (process.env.PUBLIC_DEFAULT_GATEWAY ?? "").trim() || "cloudflare";
	if (kind !== "cloudflare" && kind !== "vps") throw new Error(`PUBLIC_DEFAULT_GATEWAY must be cloudflare or vps, not ${kind}`);
	return kind;
}

/** The gateway a built site opens its tunnels to (same interface as wrangler dev). */
export interface SiteGateway extends WranglerDev {
	kind: SiteGatewayKind;
	/** The Cloudflare gateway the site knows; with `vps` a closed port nobody may open a WebSocket to. */
	cloudflareUrl: string;
	/** The site's gateway variables (PUBLIC_DEFAULT_GATEWAY, PUBLIC_WORKER_URL, PUBLIC_VPS_GATEWAY_URL). */
	env: Record<string, string>;
}

/**
 * Start the primary gateway siteGatewayKind() names, accepting tunnels from
 * `pageOrigin` and reaching the loopback fake peers: wrangler dev, or the VPS
 * service (services/seeders, its `/v1/tunnel`). With `vps` the site still
 * gets a Cloudflare URL (the fallback), on a closed port, so a WebSocket the
 * page opens there shows up (`cloudflareSockets`).
 */
export async function startSiteGateway(pageOrigin: string): Promise<SiteGateway> {
	const kind = siteGatewayKind();
	const vars = { ALLOW_PRIVATE_PEERS: "1", ALLOWED_ORIGINS: pageOrigin };
	if (kind === "cloudflare") {
		const dev = await startWranglerDev({ vars });
		return { ...dev, kind, cloudflareUrl: dev.url, env: { PUBLIC_DEFAULT_GATEWAY: "cloudflare", PUBLIC_WORKER_URL: dev.url } };
	}
	const vps = await startVpsGateway({ vars });
	const cloudflareUrl = `http://127.0.0.1:${await closedPort()}`;
	return { ...vps, kind, cloudflareUrl, env: { PUBLIC_DEFAULT_GATEWAY: "vps", PUBLIC_WORKER_URL: cloudflareUrl, PUBLIC_VPS_GATEWAY_URL: vps.url } };
}

/** WebSocket URLs (of those the page opened) that went to the Cloudflare gateway although the VPS is primary. */
export function cloudflareSockets(gateway: SiteGateway, urls: readonly string[]): string[] {
	if (gateway.kind !== "vps") return [];
	const cf = new URL(gateway.cloudflareUrl);
	return urls.filter((u) => {
		const url = new URL(u);
		return url.hostname === cf.hostname && url.port === cf.port;
	});
}

/**
 * Build the site as it is deployed (Cloudflare Pages layout, the service
 * URLs baked in) into a temporary directory, so a dev server someone is
 * running in apps/web neither answers for this test nor gets in its way.
 * `extraEnv`: more public build variables (e.g. the escalation's thresholds).
 */
export function buildSite(seeders: string, gateway: SiteGateway, outDir: string, extraEnv: Record<string, string> = {}): void {
	const r = spawnSync("npm", ["run", "build", "-w", "apps/web", "--", "--outDir", outDir], {
		cwd: ROOT,
		encoding: "utf8",
		env: { ...process.env, PUBLIC_VPS_GATEWAY_URL: "", PUBLIC_SEEDERS_URL: seeders, ...gateway.env, ...extraEnv, NO_COLOR: "1" },
	});
	if (r.status !== 0) throw new Error(`site build failed:\n${(r.stdout + r.stderr).slice(-3000)}`);
}

const TYPES: Record<string, string> = {
	".html": "text/html; charset=utf-8",
	".js": "text/javascript",
	".mjs": "text/javascript",
	".css": "text/css",
	".json": "application/json",
	".wasm": "application/wasm",
	".svg": "image/svg+xml",
	".ico": "image/x-icon",
	".map": "application/json",
};

/** A static server for the build, sending the headers of its `_headers` `/*` rule (as Pages does). */
export function serveSite(dir: string, port: number): Promise<Server> {
	const headers: Record<string, string> = {};
	const rules = readFileSync(join(dir, "_headers"), "utf8").split("\n");
	let inAll = false;
	for (const line of rules) {
		if (/^\S/.test(line)) inAll = line.trim() === "/*";
		else if (inAll && line.includes(":")) {
			const at = line.indexOf(":");
			headers[line.slice(0, at).trim()] = line.slice(at + 1).trim();
		}
	}
	const root = resolve(dir);
	const server = createServer((req, res) => {
		const path = decodeURIComponent(new URL(req.url ?? "/", "http://x").pathname);
		let file = resolve(root, `.${path}`);
		if (!file.startsWith(root)) {
			res.writeHead(403).end();
			return;
		}
		if (existsSync(file) && statSync(file).isDirectory()) file = join(file, "index.html");
		if (!existsSync(file)) {
			res.writeHead(404, headers).end();
			return;
		}
		res.writeHead(200, { ...headers, "content-type": TYPES[extname(file)] ?? "application/octet-stream" });
		res.end(readFileSync(file));
	});
	return new Promise((ok) => server.listen(port, "localhost", () => ok(server)));
}

export async function waitFor(url: string, timeoutMs: number): Promise<void> {
	const until = Date.now() + timeoutMs;
	let last = "";
	while (Date.now() < until) {
		try {
			const res = await fetch(url);
			if (res.ok) return;
			last = `HTTP ${res.status}`;
		} catch (e) {
			last = e instanceof Error ? e.message : String(e);
		}
		await new Promise((r) => setTimeout(r, 300));
	}
	throw new Error(`page did not come up: ${last}`);
}

