// The removed block route stays gone: wrangler dev answers 404.

import { startWranglerDev } from "@magnet-stream/harness";

const dev = await startWranglerDev();
let failed = false;
try {
	for (const path of ["/v1/" + "blocks", "/v1/" + "info"]) {
		const res = await fetch(`${dev.url}${path}`);
		if (res.status !== 404) {
			failed = true;
			console.error(`FAIL ${path} → ${res.status}, expected 404`);
		} else {
			console.log(`ok   ${path} → 404`);
		}
	}
} finally {
	await dev.stop();
}
if (failed) process.exit(1);
console.log("all passed");
