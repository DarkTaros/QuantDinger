# Arbitrage workspace

QuantDinger retains its existing strategy/backtest system. This module adds isolated,
pre-funded **Binance Spot + OKX Spot** taker/taker simulation for BTC/USDT, ETH/USDT, and SOL/USDT.
It never registers a real exchange execution client. Triangular arbitrage, borrowing,
transfers, leverage and live execution are outside this release.

## Local setup

The HTTP API stays on Python 3.12. The engine uses Python **3.14** and
**nautilus_trader==2.0.0rc9** in its own environment:

```sh
scripts/arbitrage.sh install
(cd backend_api_python && SKIP_AUTO_MIGRATE=false .venv/bin/python -m app.commands.migrate)
scripts/arbitrage.sh supervisor
scripts/arbitrage.sh import-worker
scripts/arbitrage.sh backtest-worker
```

Run the last three commands in separate terminals. PostgreSQL and the durable jobs
Redis must be available. Configure the root `.env` (never commit it):

```dotenv
ARBITRAGE_ENABLED=true
CRYPTOHFTDATA_API_KEY=
# Optional; default is <repository>/data/arbitrage
ARB_DATA_DIR=/absolute/path/to/data/arbitrage
ARB_QUOTA_BYTES=107374182400
# Optional existing HTTP proxy, used for public REST and WebSocket connections
ARB_HTTP_PROXY=
```

Restart the API after changing the feature flag. The Vue source in the sibling
`QuantDinger-Vue` repository exposes `/arbitrage`. The capability flag hides its
menu when disabled, and the API returns 404. Existing backtests are unaffected.

## Public real-time data: CCXT compatibility decision

The rc9 Binance official data adapter currently fails to decode the public REST
SBE exchange-info response (`Invalid block length: expected 19, was 22`). Following
user authorization, real-time data uses **ccxt==4.5.92** (CCXT Pro) through rc9's supported
Python `DataClient` / `DataClientFactory` interface and `LiveNode.run_async()`.
Execution remains the native **SandboxExecutionClientFactory**, with independent
multi-currency CASH accounts. The engine version is not changed or patched.

Binance uses its documented public-data-only endpoints:
`https://data-api.binance.vision/api/v3` and
`wss://data-stream.binance.vision:443/ws`. These do not expose user data or order
submission. See [Binance public-data documentation](https://developers.binance.com/en/docs/products/spot/faqs/market_data_only).
OKX uses its public spot `books` channel. No exchange keys are required or read.

The adapter preserves decimal strings using Decimal book parsers and raw-message
hooks. CCXT handles initial REST synchronization and WebSocket transport. The shared
validator independently checks Binance/OKX sequence chains. Buffered bootstrap
messages become one synchronized snapshot before trading can resume. Subsequent
messages are atomic L2 delta batches, including zero-size deletes; they are not
replaced with a fresh full snapshot on every update. Empty OKX keepalives do not
replenish displayed liquidity. A failed/stale/disconnected feed blocks new cycles.

## Processes and durable state

- HTTP: authentication, strict JSON validation and tenant-scoped reads/writes only.
- Supervisor: durable PostgreSQL outbox -> dedicated Celery queues; one paper child.
- Import worker: one finite import, two concurrent hourly downloads.
- Backtest worker: one isolated child per run; a PostgreSQL advisory lock enforces
  global concurrency one, even if multiple worker replicas are deployed.
- PostgreSQL: shared `qd_agent_jobs` kinds `arb_import`, `arb_backtest`, `arb_paper`;
  dedicated datasets/access grants/runs/control leases/cycles/events tables.
- Redis: delivery, never the source of truth. Broker outages retain queued jobs;
  supervisor retries dispatch. Duplicate deliveries cannot claim a running job.

Claims carry unique fencing tokens and 90-second leases. Parent processes renew
leases; stale runs become `interrupted` and never automatically reset/restart.
Cancellation moves an active job to `stopping`; a late success becomes `cancelled`.
Queued cancellation prevents a subsequent claim. Paper stop retains initial
inventory, cancels outstanding orders and handles confirmed imbalances once.

All human API queries constrain the authenticated user. Idempotency keys are scoped
by user and job kind and reject reuse with a different config. No credit billing or
Agent Gateway/MCP endpoints are added.

## Contract and data

Human endpoints are under `/api/arbitrage`, with the existing `code: 1` success
wrapper and user JWT authentication. Request schemas are generated into
[`../api/openapi.yaml`](../api/openapi.yaml). The shared contract is implemented in
`backend_api_python/qd_arbitrage/`, with no Flask or Nautilus imports.

The report UI reads downsampled chart data from
`GET /runs/{run_id}/series?metric=equity|drawdown|spread|fees|exposure&resolution=600`.
For local files, `POST /datasets/upload` accepts provider-named Parquet or
Zstandard-wrapped Parquet files and sends them through the same strict importer;
it never bypasses sequence, timestamp or instrument validation.

The run envelope uses `schemaVersion`, exact `engineVersion`, template/version,
mode, pair, two venue balances/fee/latency maps, strategy/risk parameters, seed,
and (for historical runs) an immutable `datasetId` plus UTC `[start,end)`.
Amounts are decimal strings. Timezones are required; no whole-day expansion is
performed. Default duration is 24 hours in the UI; the API rejects intervals over
7 days. The template is deterministic; the seed is recorded but no stochastic
fill/slippage model is enabled.

Raw files, normalized Zstandard JSONL shards and complete event shards live below
`data/arbitrage/`. Objects are SHA-256 addressed. Manifests bind source paths,
download timestamps, original/decoded hashes, converter version, frozen instrument
rules, sequence quality and normalized shard hashes. `COMPLETE` is published only
after successful conversion. Revised vendor files create new immutable versions.
An old run keeps referring to its original dataset. Do not delete referenced files.
The default 100 GiB soft quota prevents new downloads; nothing is automatically
pruned. Back up the database and the entire data directory together.

Imports read Parquet in batches, preserve original message order, detect Parquet
versus outer Zstandard by magic bytes, and handle bounded retries/429 Retry-After.
Missing requested hours, unexplained timestamp reversals, sequence gaps and absent
reliable snapshots fail with file/range diagnostics. Warmup searches at most 24
hours. Equal receive-time events are ordered BINANCE before OKX, book before trade,
then original message order. Vendor receive timestamps describe its collector,
not the user's future trading network. Instrument rules are observed at import
time and frozen in the manifest; historical rule changes are not reconstructed.

## Trading and accounting

Both directions are depth-priced after fees. The same base quantity satisfies both
venue lot steps using their least common multiple. Cash/inventory never cross venues.
Orders are IOC limit orders rounded inside the price-protection boundary. Native
L2 matching enables liquidity consumption; no K-line slippage is layered on top.
Only inbound order latency is modeled; execution-report latency is zero.

The state machine records all order events, actual filled quantities, fees and
VWAPs. One cycle may be unresolved. After both legs reach terminal states, a
confirmed difference gets at most one reverse IOC on the original excess-fill
venue. Unconfirmed orders are cancelled first. Untradeable dust remains explicit;
failed compensation pauses new trading and retains exposure for manual handling.
The default exposure cap is 200 USDT (configurable), in addition to the 100 USDT
nominal order preset; this allows the temporarily unmatched protected leg.

Reports separate fully balanced cycle cash profits, initial inventory holding P&L,
unresolved exposure and total venue-marked equity. Inventory/open-exposure
revaluation is reported separately to reconcile equity with realized profit.
Invalid/stale quotes produce unavailable valuations. PostgreSQL cycle records and
immutable events are the accounting source; browser pagination does not calculate
profit. Report event digests exclude non-economic UUIDs and are reproducible.

## Linux Docker

```sh
docker compose -f docker-compose.yml -f docker-compose.arbitrage.yml \
  --profile arbitrage config --quiet
docker compose -f docker-compose.yml -f docker-compose.arbitrage.yml \
  --profile arbitrage up -d --build arbitrage-supervisor arbitrage-import arbitrage-backtest
```

The optional services use Python 3.14, a locked dependency set and a non-root user.
The named `arbitrage-data` volume must be shared by all three services. The backend
only accesses PostgreSQL, so it does not need to mount engine files. This service
group does not depend on Kafka trading workers. Deploy the companion Vue changes
with the normal frontend build/image workflow. A prebuilt upstream frontend image
will not include this workspace until rebuilt from those changes.

## Verification and diagnosis

```sh
scripts/arbitrage.sh test
cd backend_api_python
python -m pytest tests/test_arbitrage.py -q
# Set ARB_TEST_DATABASE_URL to exercise temporary-schema PostgreSQL tests.
```

[Acceptance status](ACCEPTANCE.md) records actual runs, pending checks and limits.
Paper process logs are under `data/arbitrage/logs/<jobId>.log`; API job polling
returns sanitized failures. Never log provider request URLs containing `api_key`.
For a controlled disconnect drill, send `SIGUSR1` to the paper child PID: its CCXT
WebSocket sessions close, both books are invalidated, and fresh synchronized
snapshots are required. Send API stop instead of killing a healthy paper session.
An unexpected process exit preserves the last confirmed checkpoint and is marked
`interrupted`; starting a new session is an explicit separate action.
