// Integration tests: the real worker under `wrangler dev` against fake
// peers on loopback (npm run test:integration -w apps/gateway).
import { defineConfig } from "vitest/config";

export default defineConfig({
	test: {
		include: ["test/integration/**/*.test.ts"],
		testTimeout: 60_000,
		hookTimeout: 120_000,
	},
});
