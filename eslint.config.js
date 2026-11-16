// Root lint config for every TypeScript/JavaScript workspace.
// Svelte and Astro components are covered by `astro check` (npm run typecheck).

import js from "@eslint/js";
import globals from "globals";
import tseslint from "typescript-eslint";

export default tseslint.config(
	{
		ignores: [
			"**/node_modules/**",
			"**/dist/**",
			"**/.astro/**",
			"**/.wrangler/**",
			"target/**",
			"**/worker-configuration.d.ts",
			"apps/web/public/**",
		],
	},
	js.configs.recommended,
	...tseslint.configs.recommended,
	{
		languageOptions: {
			ecmaVersion: "latest",
			sourceType: "module",
			globals: { ...globals.browser, ...globals.node, ...globals.worker },
		},
		rules: {
			// No eval, ever (security rules of the plan).
			"no-eval": "error",
			"no-empty": ["error", { allowEmptyCatch: true }],
			"no-implied-eval": "error",
			"no-new-func": "error",
			"@typescript-eslint/no-unused-vars": ["error", { argsIgnorePattern: "^_", varsIgnorePattern: "^_", caughtErrors: "none" }],
		},
	},
);
