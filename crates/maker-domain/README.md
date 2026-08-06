# maker-domain

`maker-domain` contains the exchange-neutral model for the market maker. It is
deliberately synchronous and deterministic: it has no networking, async
runtime, configuration parsing, logging, retry policy, or exchange-specific
data structures.

## Numeric model

Prices are represented as positive integer ticks and order quantities as
positive integer lots. `InstrumentSpec` is the only decimal conversion
boundary. Conversions are exact and reject values that would require implicit
rounding.

## Grid model

`GridModel` owns the desired price levels, not live exchange orders. A unique
fully filled level is applied through `GridModel::apply_fill`, which returns a
`GridTransition` describing:

- the consumed level;
- the farthest opposite level to cancel;
- the opposite level to place at the consumed price;
- the new far level to place on the consumed side.

The transition is transactional. The candidate grid is validated before it
replaces the current grid, so a rejected transition cannot partially mutate
state.

The application layer is responsible for:

- deduplicating order updates;
- converting only terminal `FILLED` updates into `FilledLevel`;
- associating desired levels with client and exchange order IDs;
- executing and retrying the work described by `GridTransition`;
- buffering an out-of-order fill when applying it would temporarily cross the
  desired grid.

If a known Quote order fills after an earlier transition retired it and sent
its cancellation, the application layer may use
`GridModel::apply_late_quote_fill` to roll the ladder without treating the
late exchange event as an unknown order. TakeProfit fills still require the
level to be present, because silently recreating a missing hedge would hide a
state error.

## Invariants

After initialization and every successful transition:

- each side has exactly the configured number of levels;
- prices are unique on each side;
- the highest bid is below the lowest ask;
- new far levels extend by exactly the configured tick spacing;
- the opposite replacement uses the original filled price.
