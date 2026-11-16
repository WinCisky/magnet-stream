import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

export default defineConfig({
	test: {
		// test/integration needs wrangler dev: npm run test:integration.
		include: ["test/**/*.test.ts"],
		exclude: ["test/integration/**", "**/node_modules/**"],
	},
	resolve: {
		// Unit tests inject their own sockets; this only has to resolve.
		alias: { "cloudflare:sockets": fileURLToPath(new URL("./test/no-sockets.ts", import.meta.url)) },
	},
});
