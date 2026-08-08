# Fixed-capacity container follow-ups

Hot cached elements now use inline/numeric identities. The following dynamic containers remain intentionally unchanged until their hard capacities and overflow semantics are selected.

## Engine fixed-capacity state (completed)

- Engine configuration now rejects more than 64 levels per side (128 total).
- `OrderRegistry` and all client-order lifecycle maps/sets use 256 direct-addressed slots indexed by the low `ClientOrderId` byte. Every lookup, mutation, and removal validates the complete ID; a collision reports capacity exhaustion and never evicts live or stale state.
- Deferred placements, placement priority, desired/blocked reconciliation levels, cancellation snapshots, recovery snapshots, and pending-fill FIFO storage use inline fixed arrays.
- Placement and cancellation slots are reserved before the exchange future is dispatched. Partial reservation rolls back without sending a command.
- Pending fills retain strict arrival FIFO: a blocked head fill prevents later fills from overtaking it. Overflow enters recovery without eviction.
- Reconciliation and recovery no longer allocate temporary `Vec`/`HashSet` snapshots. Their local inline arrays are rebuilt on the stack for each pass; `GridModel`'s internal `BTreeMap`s, boxed exchange futures, and `FuturesUnordered` command nodes remain intentionally out of scope.

## Adapter and trading runtime fixed-capacity state (completed)

- `BinanceUsdm::instrument: Option<InstrumentSpec>` stores the intentionally single-symbol snapshot directly. Multi-symbol support requires a separate ownership and capacity design.
- WebSocket API pending requests use 256 direct-addressed transport slots indexed by the low request-ID byte and validated against the complete transport ID.
- The worker owns absolute response deadlines beginning at strategy-side enqueue, bounds socket writes, expires abandoned requests, skips closed replies, and preserves deadlines across retries.
- At full pending capacity the worker stops admitting commands until a response or timeout frees a slot. No resident request is overwritten or evicted; unexpected below-capacity collisions reset the connection and fail pending requests so exchange mutations remain explicit.

## Temporary reconciliation allocations (completed)

Engine desired, unresolved, recovery, cancellation, and blocked-level snapshots now use local inline fixed arrays bounded by the validated 128 total grid levels or 256 lifecycle slots. Sorting and deduplication operate in place. `GridModel` now exposes a non-allocating level visitor while retaining its existing snapshot API.

## Completed cross-thread paths

- Public BBO uses a cache-line-aligned, two-slot latest-value mailbox. The market-data thread is the single writer; strategy reads the newest coherent snapshot directly, and stale intermediate books are overwritten.
- BBO wakeups use an atomic generation plus `AtomicWaker`; terminal health uses a capacity-one SPSC separate from snapshot storage, so strategy decisions do not wait for queued book payloads.
- Market-data and trading runtime task inboxes are independent capacity-64 SPSC rings. One strategy OS thread owns their physical producers; each named network thread owns one consumer.
- WebSocket API requests use a capacity-256 SPSC command ring from strategy to the persistent trading worker. Concurrent strategy futures share one thread-affine physical producer; per-request results remain one-shot replies.
- Private order updates use a capacity-256 SPSC FIFO from the trading thread to strategy. Full capacity never overwrites lifecycle events: buffered updates drain first, then an out-of-band terminal error forces stream recovery.
- Ring producer/consumer indices are cache-separated by the underlying SPSC implementation. Ordinary compact event values are not individually cache-line aligned.
