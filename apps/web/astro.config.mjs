// @ts-check
import { defineConfig } from 'astro/config';

import svelte from '@astrojs/svelte';
import tailwindcss from '@tailwindcss/vite';
import { libmediaChunks } from './scripts/vite-libmedia.mjs';
import { streamWorker } from './scripts/stream-sw.mjs';
import { resolveEscalation } from './src/lib/magnet/gateway-config.ts';

/**
 * PUBLIC_DEFAULT_GATEWAY picks the primary tunnel gateway (docs/operations.md
 * › Default data path): resolved here from the same variables the bundle
 * reads (.env files included), so an invalid value fails the build (and
 * `astro dev`) with a clear message instead of shipping a broken site.
 * @returns {import('vite').Plugin}
 */
function defaultGateway() {
  return {
    name: 'magnet-stream:default-gateway',
    configResolved(config) {
      try {
        // The gateways and the escalation's thresholds (M4-06).
        resolveEscalation(config.env);
      } catch (e) {
        throw new Error(`invalid gateway configuration: ${e instanceof Error ? e.message : String(e)}`, { cause: e });
      }
    },
  };
}

// https://astro.build/config
export default defineConfig({
  // Served from the root of a Cloudflare Pages project (docs/operations.md).
  // SITE_URL is the public address, set by the deploy workflow; unset in dev.
  site: process.env.SITE_URL || undefined,
  base: '/',
  // streamWorker: /stream-sw.js, the range-serving service worker (M3-05).
  integrations: [svelte(), streamWorker()],

  vite: {
    server: {
      // Dev has no _headers file (public/_headers is what Pages sends):
      // the same isolation headers, so the page is crossOriginIsolated in
      // dev too. Without CORP the isolated page cannot load the session
      // worker (ERR_BLOCKED_BY_RESPONSE).
      headers: {
        "Cross-Origin-Opener-Policy": "same-origin",
        "Cross-Origin-Embedder-Policy": "require-corp",
        "Cross-Origin-Resource-Policy": "cross-origin",
      },
    },
    plugins: [tailwindcss(), libmediaChunks(), defaultGateway()],
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