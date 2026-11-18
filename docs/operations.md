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
  | `PUBLIC_SEEDERS_URL` | `https://magnet-seeders.example.org` | the VPS discovery API (`GET /peers`) |

### 4. VPS origin

The VPS only answers browsers from allowed origins (CORS, decision OQ-09):
add the site's origin (the `SITE_URL` origin) to `ALLOWED_ORIGINS` in
`/etc/magnet-seeders/magnet-seeders.env` and restart the service.

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
- Open the site, paste a magnet: the file list appears; DevTools › Network
  shows one `/peers` request and 1–3 WebSockets to the gateway.

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
