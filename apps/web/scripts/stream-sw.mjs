// Astro integration for the streaming service worker (src/lib/stream/sw.ts).
// A service worker's scope can't be wider than its script's folder, so it
// must be served from the site root, not from _astro/: it is bundled on its
// own (one classic script, no imports) to <base>stream-sw.js, written into
// the build output and, in dev, served by a middleware.

import { writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { rolldown } from "rolldown";

export const STREAM_SW = "stream-sw.js";
const ENTRY = fileURLToPath(new URL("../src/lib/stream/sw.ts", import.meta.url));

/** The worker as one minified classic script. */
export async function bundleStreamWorker() {
	const bundle = await rolldown({ input: ENTRY, platform: "browser", logLevel: "warn" });
	try {
		const { output } = await bundle.generate({ format: "iife", minify: true });
		return output[0].code;
	} finally {
		await bundle.close();
	}
}

/** @returns {import("astro").AstroIntegration} */
export function streamWorker() {
	let base = "/";
	return {
		name: "stream-sw",
		hooks: {
			"astro:config:done": ({ config }) => {
				base = config.base.replace(/\/?$/, "/");
			},
			"astro:server:setup": ({ server }) => {
				server.middlewares.use((req, res, next) => {
					if ((req.url ?? "").split("?")[0] !== `${base}${STREAM_SW}`) return next();
					bundleStreamWorker().then(
						(code) => {
							res.setHeader("Content-Type", "text/javascript; charset=utf-8");
							res.setHeader("Cache-Control", "no-cache");
							res.end(code);
						},
						(e) => next(e),
					);
				});
			},
			"astro:build:done": async ({ dir }) => {
				await writeFile(new URL(STREAM_SW, dir), await bundleStreamWorker());
			},
		},
	};
}
