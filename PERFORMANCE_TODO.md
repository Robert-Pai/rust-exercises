# Fixed-capacity container follow-ups

Hot cached elements now use inline/numeric identities. The following dynamic containers remain intentionally unchanged until their hard capacities and overflow semantics are selected.

## Engine persistent state

- `OrderRegistry::orders: HashMap<ClientOrderId, RegisteredOrder>`
  - Expected bound: active grid orders plus terminal entries awaiting cleanup.
  - Candidate: fixed-capacity hash map or slot table indexed by compact client ID sequence.
  - Decision needed: whether terminal orders are removed immediately and what recovery does on capacity exhaustion.

- `MakerEngine::placement_attempts: HashMap<ClientOrderId, GridLevel>`
- `inflight_placements`, `filled_before_ack`, `unresolved_attempts`, `inflight_cancels`, `deferred_cancels: HashSet<ClientOrderId>`
  - Expected bound: a small multiple of configured `levels_per_side`.
  - Candidate: fixed-capacity maps/sets or sequence-indexed bitsets.
  - Decision needed: exact multiplier covering placement/cancel races and recovery overlap; overflow should stop quoting and enter recovery.

- `pre_ack_updates: HashMap<ClientOrderId, OrderUpdate>`
  - Expected bound: concurrent placement acknowledgements in flight.
  - Candidate: fixed-capacity map sized from the placement concurrency bound.
  - Decision needed: whether overflow is fatal or triggers stream recovery.

- `pending_fills: HashMap<ClientOrderId, PendingFill>` and `pending_fill_order: VecDeque<ClientOrderId>`
  - Expected bound: fills deferred while grid transitions are temporarily blocked.
  - Candidate: one fixed-capacity ordered table/ring to avoid duplicate key storage.
  - Decision needed: deterministic eviction is not safe; overflow must enter recovery.

- `deferred_placements: HashSet<GridLevel>` and `placement_priority: Vec<GridLevel>`
  - Expected bound: configured grid level count (`2 * levels_per_side`).
  - Candidate: fixed array/bitset indexed by grid slot.
  - Decision needed: expose a maximum grid level count in configuration validation.

## Adapter and trading runtime

- `BinanceUsdm::instrument: Option<InstrumentSpec>`
  - The move-only adapter session is intentionally single-symbol, so the snapshot is stored directly without ArcSwap or a map.
  - Multi-symbol support would require a separate ownership and capacity design rather than silently restoring shared cache state.

- WebSocket API `pending: HashMap<u64, PendingRequest>`
  - Upper pressure is related to `COMMAND_BUFFER` (256), but timed-out `Forget` commands and reconnect handling must be included.
  - Candidate: fixed slab indexed by request ID modulo capacity with generation validation.
  - Decision needed: maximum concurrent requests and collision behavior.

## Temporary reconciliation allocations

The engine creates temporary `Vec`/`HashSet` values for desired, unresolved, recovery, and cancellation snapshots. They are not long-lived cached state but can still cause latency jitter.

- Candidate: reusable scratch buffers owned by `MakerEngine`, preallocated from the validated maximum grid size.
- Decision needed: maximum grid size and whether sorting/dedup can be replaced by sequence-indexed bitsets.

## Completed cross-thread paths

- Public BBO uses a cache-line-aligned, two-slot latest-value mailbox. The market-data thread is the single writer; strategy reads the newest coherent snapshot directly, and stale intermediate books are overwritten.
- BBO wakeups use an atomic generation plus `AtomicWaker`; terminal health uses a capacity-one SPSC separate from snapshot storage, so strategy decisions do not wait for queued book payloads.
- Market-data and trading runtime task inboxes are independent capacity-64 SPSC rings. One strategy OS thread owns their physical producers; each named network thread owns one consumer.
- WebSocket API requests use a capacity-256 SPSC command ring from strategy to the persistent trading worker. Concurrent strategy futures share one thread-affine physical producer; per-request results remain one-shot replies.
- Private order updates use a capacity-256 SPSC FIFO from the trading thread to strategy. Full capacity never overwrites lifecycle events: buffered updates drain first, then an out-of-band terminal error forces stream recovery.
- Ring producer/consumer indices are cache-separated by the underlying SPSC implementation. Ordinary compact event values are not individually cache-line aligned.
