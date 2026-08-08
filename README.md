# Maker

Rust multi-level post-only market maker for Binance USD-M Futures.

This program uses real API credentials and places live orders. On startup,
graceful shutdown, stream loss, and recovery it cancels **every open order for
the configured symbol**, including orders created by other programs. Ordinary
stream recovery first resolves known orders and preserves the local grid
purposes; an instrument-rule change explicitly rebuilds the grid.

## Behavior

The initial bid and ask ladders are built around the current best-bid/ask
midpoint. All price distances are configured as exchange ticks.

When a Quote ask level fills completely, the engine creates a bid
TakeProfit level at `filled_price - strategy.take_profit_ticks`. When a Quote
bid fills, it creates the mirror ask TakeProfit at
`filled_price + strategy.take_profit_ticks`. If that same-side, same-price
exchange order already exists, only its local grid purpose changes from
`Quote` to `TakeProfit`; no remote cancel or replace is sent. A missing
TakeProfit is submitted, and the farthest Quote on that side is removed to
keep the configured level count.

When a TakeProfit fills, it closes that grid transition and only restores a
far-side Quote. `Quote`/`TakeProfit` is grid metadata and is independent of
the exchange order status. Partial fills do not roll the grid.
The private Binance WebSocket drives fills; a periodic reconciler only repairs
failed order mutations. Order placement, single-order cancellation,
symbol-wide cancellation, and uncertain-order lookup all use the Binance
USD-M WebSocket API. REST is limited to exchange metadata, initial snapshots,
position-mode validation, clock synchronization, and listen-key lifecycle.
Exchange trading rules are loaded before the first grid is built. The adapter then keeps the single configured instrument snapshot in the
move-only strategy session; every
`runtime.instrument_refresh_interval_secs` it fetches a candidate update. A
rule change cancels the symbol orders and rebuilds the grid so old tick or step
semantics cannot remain live.

The configured `levels_per_side` is also the hard size of each desired ladder.
If a side has no Quote level left to replace a new fill, or if pending
take-profit transitions would make the ladder invalid, the engine stops and
cancels the symbol rather than inventing an unbounded position. Configure
enough levels for the intended inventory range.

## Configuration

Create the live configuration and restrict its permissions:

```sh
cp config.example.toml config.toml
chmod 600 config.toml
```

Set a Binance Ed25519 API key with USD-M Futures trading permission and place
its PKCS#8 PEM private key in `exchange.private_key_pem`. Withdrawal permission
should remain disabled, and an IP allowlist should be used.

Every runtime setting and both credentials live in `config.toml`; environment
variables are not read. The real configuration is ignored by Git. The
WebSocket idle timeout is a continuity guard. When either critical stream
becomes silent, the engine resolves every known order, preserves the current
grid revision and its Quote/TakeProfit purposes, cancels any remaining symbol
orders, then rebuilds the subscriptions. Trading-rule changes still rebuild a
fresh grid.

Logs are written to the configured `logging.directory` (default `logs/`). The
terminal receives compact control-plane text, while `maker.log.YYYY-MM-DD`
files receive structured JSON through an asynchronous writer. Files older than
`logging.retention_days` are removed when the process starts. Dedicated
strategy and network runtime paths do not call tracing or perform synchronous
output; lifecycle logging remains outside the covered low-latency boundary.
API keys, private keys, and signatures are never logged.

## Run

```sh
cargo run --release -p maker-cli -- --config config.toml
```

There is no dry-run or live-confirmation flag. Press `Ctrl-C` for graceful
shutdown and symbol-wide order cancellation.

## Architecture

```text
maker-domain            exact values and pure rolling-grid transitions
maker-ports             exchange-neutral async capabilities
maker-engine            single-owner lifecycle and reconciliation loop
exchange-binance-usdm   Binance REST adapter plus isolated network runtimes
maker-cli               configuration and dependency composition
```

The Binance adapter owns two isolated network OS threads, each running a
current-thread Tokio runtime. `maker-network-market-data` handles instrument
metadata, book-ticker REST, and the public book-ticker WebSocket.
`maker-network-trading` handles position mode, the private user-data stream and
listen-key keepalive, trading WebSocket API requests, and clock synchronization.
Both share the same cloneable REST client state, but public market bursts cannot
consume scheduler time on the latency-critical private/trading runtime.

Both network runtimes and the strategy runtime are always busy-polled. Project
control flow therefore never selects an event-driven parking mode. Each runtime
continuously self-wakes so Tokio uses non-blocking driver polls; this consumes
one CPU core per dedicated thread and can reduce local wake-up jitter, but it
does not bypass Tokio, socket readiness, or the kernel TCP stack.
`runtime.market_data_cpu_core` and `runtime.trading_cpu_core` independently
select logical CPUs.

The engine owns a separate `maker-strategy` OS thread and a current-thread
Tokio runtime. Recurring FIFO communication between strategy and the two network
threads uses fixed-capacity SPSC rings: capacity 64 for each network task inbox,
capacity 256 for trading WebSocket API commands, and capacity 256 for ordered
private order updates. The move-only exchange session directly owns each raw
SPSC producer, so Rust ownership permits exactly one physical producer without
locks or a runtime ownership guard. Queue saturation never
blocks or overwrites: task/command admission fails immediately, while private
order-event saturation drains accepted events and then forces stream recovery.
Public BBO is intentionally different: the market thread writes a two-slot
latest-value mailbox and strategy reads the newest coherent snapshot directly.

All three dedicated runtimes use the same busy-poll policy and therefore consume
one CPU core continuously. The strategy, market-data, and trading CPU selectors are resolved by `core_affinity`; they should name separate logical
CPUs. On a four-core Linux low-latency host, the example reserves core 1 for strategy,
core 2 for private trading, core 3 for public market data, and leaves core 0 for
the OS, logging, and kernel network work. On Linux each selector indexes the
CPUs allowed to the process and installs a hard single-CPU affinity mask. CPU
affinity is rejected as unavailable on macOS because the project uses safe Rust
only and Mach affinity tags do not provide hard CPU pinning.

The no-blocking guarantee applies to project-owned production code executing on
`maker-strategy`, `maker-network-market-data`, and `maker-network-trading`:
there are no project mutexes, read/write locks, blocking waits, runtime parking
modes, watch/MPSC control channels, or synchronous output calls on those paths.
Bounded SPSC rings, atomics, `AtomicWaker`, bounded startup spinning, and
asynchronous socket/HTTP I/O are permitted. Synchronization inside Tokio,
reqwest/hyper, rustls, DNS, the allocator, the OS, and other dependencies is
outside this project-owned proof boundary. A source-policy integration test
rejects forbidden primitives in covered production modules.

## Verify

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```
