// @ts-check
import { defineConfig } from 'astro/config';

import svelte from '@astrojs/svelte';
import tailwindcss from '@tailwindcss/vite';
import { libmediaChunks } from './scripts/vite-libmedia.mjs';

// https://astro.build/config
export default defineConfig({
  // Served from the root of a Cloudflare Pages project (docs/operations.md).
  // SITE_URL is the public address, set by the deploy workflow; unset in dev.
  site: process.env.SITE_URL || undefined,
  base: '/',
  integrations: [svelte()],

  vite: {
    server: {
      // Dev has no _headers file. Without CORP the isolated page cannot
      // load the session worker (ERR_BLOCKED_BY_RESPONSE).
      headers: {
        "Cross-Origin-Opener-Policy": "same-origin",
        "Cross-Origin-Embedder-Policy": "require-corp",
        "Cross-Origin-Resource-Policy": "cross-origin",
      },
    },
    plugins: [tailwindcss(), libmediaChunks()],
    define: {
      // Which build a diagnostics export came from (each deploy builds anew).
      __MW_BUILD__: JSON.stringify(new Date().toISOString().slice(0, 16) + 'Z'),
    },
    resolve: {
      alias: {
        $lib: new URL('./src/lib', import.meta.url).pathname
      }
    }
  }
});