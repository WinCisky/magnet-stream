// Cross-origin isolation for GitHub Pages, which can't send the headers
// itself: this service worker adds COOP/COEP to the site's own responses.
// Isolated, the page gets SharedArrayBuffer, and the player (libmedia) runs
// its decoders on real threads; without it, subtitles can't reach the page.
// Requests to other origins pass through untouched (they must allow CORS,
// which the app's APIs do). Registered from src/pages/index.astro.

self.addEventListener("install", () => self.skipWaiting());
self.addEventListener("activate", (event) => event.waitUntil(self.clients.claim()));

self.addEventListener("fetch", (event) => {
	const request = event.request;
	const url = new URL(request.url);
	if (url.origin !== self.location.origin) return;
	// Module workers are blocked if a service worker synthesises their script response.
	if (request.destination === "worker" || url.searchParams.has("worker_file")) return;
	if (request.cache === "only-if-cached" && request.mode !== "same-origin") return;
	event.respondWith(
		fetch(request).then((response) => {
			if (response.status === 0) return response;
			const headers = new Headers(response.headers);
			headers.set("Cross-Origin-Resource-Policy", "same-origin");
			headers.set("Cross-Origin-Opener-Policy", "same-origin");
			headers.set("Cross-Origin-Embedder-Policy", "require-corp");
			return new Response(response.body, { status: response.status, statusText: response.statusText, headers });
		}),
	);
});
