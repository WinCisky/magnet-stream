# Operations: automatic deploy of the gateway and the site

The tunnel gateway (`apps/gateway`, a Cloudflare Worker with a Durable
Object) and the site (`apps/web`, static files on Cloudflare Pages) are
deployed by `.github/workflows/deploy.yml`. The VPS service
(`services/seeders`) is deployed by hand: see `services/seeders/README.md`
(the full runbook arrives with M5-04).

## When a deploy runs

- Only after a **push to `main`** of this repository whose **CI run
  succeeded** (verify, web build, gateway bundle, workflow lint, e2e,
  dependency audit, secret scan). Never for pull requests or forks: the
  workflow is triggered by `workflow_run` and checks the event, the branch
  and the head repository.
- It checks out exactly the commit CI tested (`head_sha`), builds the site,
  checks the build (`npm run check:dist -w apps/web`), deploys the gateway
  (`wrangler deploy`), then the site (`wrangler pages deploy`). The gateway
  goes first, so a new site never talks to an old gateway.
- One deploy at a time (`concurrency: deploy-production`); a newer one waits.
- The job token is read-only (`permissions: contents: read`); actions are
  pinned to commit SHAs.

## One-time setup

### 1. Cloudflare Pages project

In the Cloudflare dashboard, *Workers & Pages › Create › Pages › Upload
assets* (direct upload, **not** Git integration), or from a shell:

```sh
npx wrangler pages project create <project-name> --production-branch main
```

The site is then served at `https://<project-name>.pages.dev` (or at a
custom domain added to the project). It is served from the root path `/`.
Response headers (cross-origin isolation for the player) come from
`apps/web/public/_headers`, which the build copies to `dist/_headers`.
The build also writes `dist/stream-sw.js`, the streaming service worker,
which must stay at the root (its scope is `/`).

### 2. API token

*My Profile › API Tokens › Create Token › Custom token*, with only:

| Permission | Access |
|---|---|
| Account › Workers Scripts | Edit |
| Account › Cloudflare Pages | Edit |

Account resources: only the account that hosts the project. No zone
permissions are needed (the gateway runs on `workers.dev`). Set an expiry
and rotate it before then. Note the **Account ID** (dashboard sidebar).

### 3. GitHub environment `production`

*Settings › Environments › New environment* `production`:

- **Deployment branches and tags:** *Selected branches* → `main` only.
- Optionally, *Required reviewers* to approve each deploy by hand.
- **Environment secrets** (never in the repository):

  | Secret | Value |
  |---|---|
  | `CLOUDFLARE_API_TOKEN` | the token from step 2 |
  | `CLOUDFLARE_ACCOUNT_ID` | the account ID |

- **Environment variables** (not secret; public URLs, checked by the
  workflow before it builds anything):

  | Variable | Example | Used for |
  |---|---|---|
  | `PAGES_PROJECT` | `magnet-stream` | `wrangler pages deploy --project-name` |
  | `SITE_URL` | `https://magnet-stream.pages.dev` | Astro `site`; its origin becomes the gateway's `ALLOWED_ORIGINS` |
  | `PUBLIC_WORKER_URL` | `https://magnet-stream-gateway.<subdomain>.workers.dev` | the gateway the site opens tunnels to |
  | `PUBLIC_SEEDERS_URL` | `https://magnet-stream.example.org` | the VPS discovery API (`GET /peers`) |
  | `PUBLIC_DEFAULT_GATEWAY` | `cloudflare` | optional: the primary tunnel gateway, `cloudflare` (unset) or `vps` (see *Default data path*) |
  | `PUBLIC_VPS_GATEWAY_URL` | `https://magnet-stream.example.org` | optional: the VPS tunnel gateway (`/v1/tunnel`) when it is not at `PUBLIC_SEEDERS_URL` |

### 4. VPS origin

The VPS only answers browsers from allowed origins (CORS, decision OQ-09):
add the site's origin (the `SITE_URL` origin) to `ALLOWED_ORIGINS` in
`/etc/magnet-stream/magnet-stream.env` and restart the service
(`sudo systemctl restart magnet-stream`). The VPS install is named
*magnet-stream* so it can run next to the PoC's magnet-seeders (files in
`services/seeders/deploy/magnet-stream.*` and `nginx-magnet-stream.conf`).

The same service is the fallback tunnel gateway (`GET /v1/tunnel`, M4-01):
its `Origin` check reads the same `ALLOWED_ORIGINS`, and its limits are the
`TUNNEL_*` variables of `magnet-stream.env.example` (Cloudflare's names and
defaults). It runs as the dedicated unprivileged user of the systemd unit
and needs no secret. Forwarding WebSocket upgrades through nginx is part of
the production setup (M5-04).

For swarms whose seeders sit behind NAT, a session may ask this gateway to
announce itself and accept inbound peers (M4-02, docs/tunnel-protocol.md ›
Announce and inbound connections). Set `TUNNEL_INBOUND_ADDR` (for example
`0.0.0.0:6883`: a port of at least 1024, free on the host for TCP and UDP,
and not `DHT_PORT`) and open that port in the VPS firewall for both, since
inbound peers connect over TCP and over uTP on UDP (M4-04e; e.g.
`sudo ufw allow 6883/tcp` and `sudo ufw allow 6883/udp`); behind a port
forward, `TUNNEL_ANNOUNCE_PORT` is the public port (forward TCP and UDP).
The port listens only while a session announces and closes when the last
one ends; without `TUNNEL_INBOUND_ADDR` the feature is off and sessions
are told so. A `TUNNEL_INBOUND_ADDR` on `DHT_PORT` turns it off too
(logged at startup). The
announce goes to the session's trackers, `DEFAULT_TRACKERS` and the DHT
(`TUNNEL_ANNOUNCE_DHT=0` to skip it). Leave `DHT_BOOTSTRAP` unset in
production (the DHT library's public routers); `none` keeps the node off
the public DHT, which is what the tests do (tools/harness
`HERMETIC_VPS_VARS`).

**Exposure of the VPS address.** While a session announces, the VPS's IP
is a peer of that torrent's swarm for trackers, the DHT and anyone who
watches them, so complaints about a torrent (copyright notices, abuse
reports) reach the VPS provider. The browser is not authenticated
(OQ-09): the `Origin` check stops other sites' pages, not a script that
forges the header, so anyone can make the gateway announce an info-hash
of their choosing. The limits bound it, not prevent it: per WebSocket
(`TUNNEL_ANNOUNCES_PER_SOCKET`, 2), per client IP or IPv6 /64
(`TUNNEL_ANNOUNCES_PER_IP`, 4) and in total (`TUNNEL_ANNOUNCES_MAX`, 64)
info-hashes at once, and announce rounds per minute per client IP
(`TUNNEL_ANNOUNCE_ROUNDS_PER_IP_PER_MINUTE`, 6) and in total
(`TUNNEL_ANNOUNCE_ROUNDS_PER_MINUTE`, 60). Pick a provider whose terms
accept BitTorrent traffic, and know that a few clients that fill
`TUNNEL_ANNOUNCES_MAX` leave the other sessions without escalation (they
are told `LIMIT_REACHED` and go on with what they have). Unset
`TUNNEL_INBOUND_ADDR` to turn the whole feature off.

Which path carried a session's bytes shows in the page's diagnostics
(M4-08): *Data path (last session)* in the Diagnostics menu, and in the
exported report `tunnel.paths` per session (bytes received and sent, and
connections, through Cloudflare and through the VPS) with totals and the
VPS share in the `tunnel` health section.

The VPS scouts the peers it streams (docs/discovery-api.md › Scouting,
decisions OQ-21, OQ-22): it keeps scores and discovery results **in memory
only**, weighed in bytes. Defaults: `SCOUT_CACHE_MB=16`, `PEERS_CACHE_MB=16`
(twice: streamed and legacy caches), `PROBE_CONCURRENCY=256` sockets,
`SCOUT_PER_STREAM=64`, `MAX_PROBE_PEERS=1000` probes per info-hash per
`SCOUT_REPROBE_SECS=600` window (an address probed more recently is answered
from memory), `SCOUT_MAX_ADDRS=1000` addresses kept per info-hash, scores
fading toward 0.2 with `SCOUT_HALF_LIFE_SECS=1800` and forgotten after
`SCOUT_IDLE_SECS=7200` without use. Probe timings:
`PROBE_CONNECT_TIMEOUT_MS=2500`, `PROBE_UNCHOKE_WAIT_MS=2500`,
`PROBE_TIMEOUT_MS=5000` in all. Info dicts (`/peers` `info` line and
`GET /metadata?ih=`): `METADATA_CACHE_MB=32`, entries over
`METADATA_CACHE_MAX_ENTRY_MB=4` not cached, `METADATA_FETCHES=4` fetches in
flight (buffers bounded to 4 × 8 MiB), `METADATA_DEADLINE_MS=15000`. All are
listed in `services/seeders/README.md` › Configuration. Check the service's
memory with `ps -o rss= -p $(systemctl show -p MainPID --value magnet-stream)`
after some use, and lower the budgets on a small host.

## Default data path

Which gateway carries a session's tunnels is a build setting, not code
(docs/architecture.md §Risks: *switching the default path is a
configuration change*). The public variable `PUBLIC_DEFAULT_GATEWAY`
names the **primary** gateway; the other one is the **fallback** (used
by escalation, M4-06):

| `PUBLIC_DEFAULT_GATEWAY` | Primary (every tunnel opens here) | Fallback |
|---|---|---|
| unset or `cloudflare` (default, decision OQ-02) | `PUBLIC_WORKER_URL` (Durable Object) | the VPS gateway |
| `vps` | the VPS gateway: `PUBLIC_VPS_GATEWAY_URL`, or `PUBLIC_SEEDERS_URL` when unset (the same service serves `/v1/tunnel`); one of them must be set | `PUBLIC_WORKER_URL` |

With `vps` a session opens no WebSocket to Cloudflare (the browser e2e
`npm run e2e:browser:vps -w tools/e2e` checks it). Any other value fails
`astro build` (and `astro dev`) with `invalid gateway configuration:
PUBLIC_DEFAULT_GATEWAY must be one of "cloudflare", "vps"`; so does a
gateway URL that is not http(s)/ws(s) or carries credentials, a query or a
fragment. The resolution is `apps/web/src/lib/magnet/gateway-config.ts`.
These are public URLs baked into the site's bundle: never put a credential
in a `PUBLIC_*` variable.

To switch the default path to the VPS (for example if Cloudflare starts
objecting to BitTorrent traffic or peers block its ranges):

1. Make sure the VPS gateway answers browsers: `ALLOWED_ORIGINS` in
   `/etc/magnet-stream/magnet-stream.env` contains the site's origin, and
   nginx forwards WebSocket upgrades on `/v1/tunnel` (M5-04).
   `curl -i https://<vps>/health` → `200`.
2. Set the environment variable `PUBLIC_DEFAULT_GATEWAY=vps` in the
   GitHub environment `production` (and `PUBLIC_VPS_GATEWAY_URL` if the
   tunnel endpoint is not on the `PUBLIC_SEEDERS_URL` host).
3. Re-run the last *Deploy* (or push to `main`): the site is rebuilt with
   the new primary. Check DevTools › Network: the `/v1/tunnel` WebSockets
   go to the VPS host, none to `workers.dev`.

To switch back, delete the variable (or set `cloudflare`) and deploy again.
The gateway Worker keeps being deployed either way, so the switch is
instant in both directions. VPS bandwidth: with `vps` every byte crosses
the VPS (docs/architecture.md §Cost model).

Locally: `PUBLIC_DEFAULT_GATEWAY=vps PUBLIC_SEEDERS_URL=http://127.0.0.1:8080 npm run dev -w apps/web`
(the VPS service started with `ALLOWED_ORIGINS=http://localhost:4321` and
`ALLOW_PRIVATE_PEERS=1` for loopback peers).

## Escalation to the VPS gateway

On a thin swarm a session asks the VPS gateway to join it (M4-06,
docs/architecture.md §Chunk recovery › Swarms with few seeders): when fewer
than 2 peers deliver (counted once the `/peers` stream is over and its live
addresses are used up), or the download rate stays under the video's
bitrate, for 90 s in a row, it opens one WebSocket to the VPS gateway and
sends `announce` with the info-hash and the magnet's trackers (nothing
else). Inbound peers then take part in the download and in the upload like
any other. With the VPS primary, the announce goes on the session's own
tunnels. After 30 s under the bitrate the page says so and offers to keep
buffering; the file completes either way. The announce lasts only while it
is needed: once the peers the session found on its own (not the inbound or
uTP peers the escalation brought) deliver enough, and at the video's
bitrate when it is known, for 10 min in a row, the session withdraws it
(the gateway sends `stopped` and leaves the swarm; peers already connected
stay) and a thin swarm later escalates again. Nothing escalates while only the
file list is being looked up.

| Public build variable | Default | Meaning |
|---|---|---|
| `PUBLIC_VPS_GATEWAY_URL` | `PUBLIC_SEEDERS_URL` if set | the escalation target; `off`, or neither variable set: no VPS gateway, escalation off (a build error with `PUBLIC_DEFAULT_GATEWAY=vps`; the built-in seeders address of builds without variables is never taken for a tunnel gateway) |
| `PUBLIC_ESCALATION_AFTER_SECONDS` | `90` | few peers or a rate under the bitrate this long: escalate (1–3600) |
| `PUBLIC_SLOW_NOTICE_SECONDS` | `30` | a rate under the bitrate this long: the page says so (1–3600) |
| `PUBLIC_ESCALATION_MIN_PEERS` | `2` | "few peers" is fewer than this many delivering (1–64) |
| `PUBLIC_ESCALATION_WITHDRAW_SECONDS` | `600` | escalated, and the session's own peers enough this long: withdraw the announce (1–3600) |

An invalid value fails `astro build` with `invalid gateway configuration:
…`. The VPS needs `TUNNEL_INBOUND_ADDR` (see *VPS origin*); without it the
announce is refused and the page shows that the relay could not join. The
diagnostics report counts escalated sessions, by reason, and the inbound
peers (`health.tunnel.details.escalation`).

Locally, the "only NATed peers" scenario: `npm run stack -w tools/e2e -- --nat` prints a
magnet and the `npm run dev -w apps/web` command with the variables (short
thresholds); the page shows the escalation, the seed connects in through
the VPS, and the notice that the swarm is slower than the video.

## Configuration the deploy sets

- **Gateway `ALLOWED_ORIGINS`.** `wrangler.jsonc` lists only
  `http://localhost:4321` (local development). The workflow replaces it at
  deploy time with the origin of `SITE_URL` (`--var ALLOWED_ORIGINS:<origin>`).
  A manual `npm run deploy -w apps/gateway` must pass the same `--var`, or
  the site gets 403 on every tunnel.
- `ALLOW_PRIVATE_PEERS` is never deployed (a test keeps it out of
  `wrangler.jsonc`).
- The Durable Object stores nothing, so deploys need no data migration;
  open tunnels drop on deploy and the browser reconnects (docs/architecture.md
  §Risks).

## Checking a deploy

- *Actions › Deploy*: the run lists the gateway upload and the Pages
  deployment URL.
- `curl -i https://<gateway>/health` → `200 ok`; `curl -i https://<gateway>/v1/blocks` → `404`.
- `curl -sI https://<site>/ | grep -i cross-origin` → COOP `same-origin`,
  COEP `require-corp`.
- `curl -sI https://<site>/stream-sw.js` → `200`, a JavaScript type.
- Open the site, paste a magnet: the file list appears; DevTools › Network
  shows one `/peers` request and 1–3 WebSockets to the primary gateway
  (`PUBLIC_DEFAULT_GATEWAY`).

## Rollback

- **Site.** *Workers & Pages › <project> › Deployments*, pick the previous
  production deployment › *Rollback to this deployment*. Instant, no build.
- **Gateway.** List versions and roll back:

  ```sh
  cd apps/gateway
  npx wrangler deployments list
  npx wrangler rollback <version-id> --message "rollback: <reason>"
  ```

  (or *Workers & Pages › magnet-stream-gateway › Deployments › Rollback*).
  A rollback keeps the variables of the version it restores.
- Then revert the faulty commit on `main`; the next green CI deploys the
  fix. To stop deploys meanwhile, disable the *Deploy* workflow
  (*Actions › Deploy › ⋯ › Disable workflow*) or add a required reviewer
  to the `production` environment.

## Local checks (what CI runs)

```sh
npm run build -w apps/web && npm run check:dist -w apps/web
(cd apps/gateway && npx wrangler deploy --dry-run --outdir /tmp/gateway-bundle)
actionlint .github/workflows/*.yml
```
