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

- `BinanceUsdm::instruments: ArcSwap<HashMap<Symbol, InstrumentSpec>>`
  - Current deployment is single-symbol; logical bound is one.
  - Candidate: `ArcSwapOption<InstrumentSpec>` or fixed array for future multi-symbol support.
  - Decision needed: whether the adapter remains permanently single-symbol.

- WebSocket API `pending: HashMap<u64, PendingRequest>`
  - Upper pressure is related to `COMMAND_BUFFER` (256), but timed-out `Forget` commands and reconnect handling must be included.
  - Candidate: fixed slab indexed by request ID modulo capacity with generation validation.
  - Decision needed: maximum concurrent requests and collision behavior.

## Temporary reconciliation allocations

The engine creates temporary `Vec`/`HashSet` values for desired, unresolved, recovery, and cancellation snapshots. They are not long-lived cached state but can still cause latency jitter.

- Candidate: reusable scratch buffers owned by `MakerEngine`, preallocated from the validated maximum grid size.
- Decision needed: maximum grid size and whether sorting/dedup can be replaced by sequence-indexed bitsets.

## Cross-thread channels

- BBO stream: replace FIFO MPSC with a latest-value mailbox/seqlock because stale books should be overwritten.
- Private order updates: replace MPSC with a bounded SPSC ring; queue full must trigger recovery, never overwrite.
- Cache-line alignment should be applied to independently mutated producer/consumer indices and mailbox publication state, not to ordinary event structs.
