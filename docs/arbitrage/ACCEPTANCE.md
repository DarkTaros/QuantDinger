# Arbitrage acceptance record

Updated 2026-11-15 UTC. This file records observed results; pending items are not counted as passes.

## Passed

- Nautilus runtime tests: **35 passed** on the local Python 3.14.2 environment with `nautilus_trader==2.0.0rc9` and `ccxt==4.5.92`.
- Linux container runtime tests: **25 passed** with the final `quantdinger-nautilus:2.0.0rc9` image.
- Backend regression in a Python 3.12 Linux container: **65 passed, 3 skipped**. The skipped tests are opt-in integrations. The local macOS run has one existing Strategy V2 sandbox-memory failure because macOS cannot apply the Linux `RLIMIT_AS` policy; the policy and original test were not changed.
- Backend PostgreSQL lifecycle tests in an isolated temporary schema: **7 passed**.
- Vue lint and production build passed. OpenAPI was regenerated after the arbitrage request schemas were added.
- BTC/USDT 24-hour import and final-code backtest: succeeded, 2,757,307 normalized messages, peak resident memory 268,288,000 bytes, zero cycles at the configured 5 bps net threshold.
- ETH/USDT 24-hour import and final-code backtest: succeeded, 2,455,930 normalized messages, peak resident memory 201,261,056 bytes, zero cycles at the configured 5 bps net threshold.
- A real CCXT public-feed paper process connected to Binance and OKX, used only native Sandbox execution clients, and recorded independent balances. A SIGUSR1 drill produced disconnect events for both venues; both feeds became invalid and then recovered with fresh snapshots. The process has remained healthy after recovery.
- Final Linux Nautilus image built successfully and Compose configuration validates.

## Strict failures and pending checks

- A requested 7-day BTC interval (2026-11-01 through 2026-11-08 UTC) was rejected during normalization because CryptoHFTData Binance trades for 2026-11-04 13:00 contains `received_time` reversals (the observed first reversal is about 596 ms). The importer reports the source path and keeps strict failure semantics. A second candidate interval (2026-10-25 through 2026-11-01 UTC) is being tested with the final importer; it is not accepted until its manifest is complete.
- The live paper acceptance requires two continuous wall-clock hours. The current corrected process is left running after the successful disconnect/recovery drill; its start time and health checkpoints are recorded in PostgreSQL. At the latest check both venues were valid with one recorded disconnect each; the two-hour wall-clock requirement is still pending. This is an observation requirement, not simulated time.
- A separate unexpected-child-exit check remains pending. Existing lease-expiry tests prove that a stale worker becomes `interrupted` while preserving its last checkpoint; the process-kill drill still needs to be run on a separate short-lived session.

## Interpretation

The configured 5 bps threshold and 10 bps taker fee on each venue produced no historical arbitrage cycles in the two accepted 24-hour datasets. This is an outcome of the selected data, fees and threshold, not a profitability claim. The reports separate realized cycle profit from inventory holding P&L and marked equity.
