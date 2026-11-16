// Peer tokens minted by magnet-seeders (see its `src/token.rs`): the worker
// only connects to a peer the browser holds a valid, unexpired token for.
//
//   token = base64url_nopad(HMAC-SHA256(secret, "mw1|<info-hash hex>|<addr>|<exp>"))

const encoder = new TextEncoder();
let cached: { secret: string; key: Promise<CryptoKey> } | undefined;

function verifyKey(secret: string): Promise<CryptoKey> {
	if (cached?.secret !== secret) {
		const key = crypto.subtle.importKey(
			"raw",
			encoder.encode(secret),
			{ name: "HMAC", hash: "SHA-256" },
			false,
			["verify"],
		);
		cached = { secret, key };
	}
	return cached.key;
}

function message(infoHashHex: string, addr: string, exp: number): Uint8Array {
	return encoder.encode(`mw1|${infoHashHex}|${addr}|${exp}`);
}

export async function verifyToken(
	secret: string,
	infoHashHex: string,
	addr: string,
	exp: number,
	token: string,
	nowSeconds: number,
): Promise<boolean> {
	if (exp < nowSeconds) return false;
	const signature = fromBase64Url(token);
	if (!signature) return false;
	return crypto.subtle.verify("HMAC", await verifyKey(secret), signature, message(infoHashHex, addr, exp));
}

/** Mint a token (tests and local tooling; production tokens come from magnet-seeders). */
export async function signToken(
	secret: string,
	infoHashHex: string,
	addr: string,
	exp: number,
): Promise<string> {
	const key = await crypto.subtle.importKey(
		"raw",
		encoder.encode(secret),
		{ name: "HMAC", hash: "SHA-256" },
		false,
		["sign"],
	);
	const mac = new Uint8Array(await crypto.subtle.sign("HMAC", key, message(infoHashHex, addr, exp)));
	return btoa(String.fromCharCode(...mac))
		.replaceAll("+", "-")
		.replaceAll("/", "_")
		.replace(/=+$/, "");
}

function fromBase64Url(s: string): Uint8Array | undefined {
	try {
		const bin = atob(s.replaceAll("-", "+").replaceAll("_", "/"));
		return Uint8Array.from(bin, (c) => c.charCodeAt(0));
	} catch {
		return undefined;
	}
}
