use std::collections::BTreeMap;

use crate::{BestBidAsk, PriceTicks, Side, Symbol};

use super::{
    FilledLevel, GridConfig, GridError, GridLevel, GridPurpose, GridReassignment, GridRevision,
    GridTransition,
};

/// The desired bid and ask ladder, independent of actual exchange orders.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GridModel {
    symbol: Symbol,
    config: GridConfig,
    bids: BTreeMap<PriceTicks, GridPurpose>,
    asks: BTreeMap<PriceTicks, GridPurpose>,
    revision: GridRevision,
}

impl GridModel {
    pub fn initialize(config: GridConfig, book: &BestBidAsk) -> Result<Self, GridError> {
        let nearest_bid = checked_sub(Side::Buy, book.floor_mid(), config.inner_offset().get())?;
        let nearest_ask = checked_add(Side::Sell, book.ceil_mid(), config.inner_offset().get())?;

        let count = config.levels_per_side().get();
        let mut bids = BTreeMap::new();
        let mut asks = BTreeMap::new();
        for index in 0..count {
            let index_ticks = u64::try_from(index)
                .ok()
                .and_then(|index| config.spacing().get().checked_mul(index))
                .ok_or(GridError::TickOffsetOverflow { level: index })?;
            bids.insert(
                checked_sub(Side::Buy, nearest_bid, index_ticks)?,
                GridPurpose::Quote,
            );
            asks.insert(
                checked_add(Side::Sell, nearest_ask, index_ticks)?,
                GridPurpose::Quote,
            );
        }

        let model = Self {
            symbol: *book.symbol(),
            config,
            bids,
            asks,
            revision: GridRevision::ZERO,
        };
        model.validate()?;
        Ok(model)
    }

    pub fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    pub const fn config(&self) -> GridConfig {
        self.config
    }

    pub const fn revision(&self) -> GridRevision {
        self.revision
    }

    /// Returns levels in executable priority order: best price to farthest price.
    pub fn levels(&self, side: Side) -> Vec<GridLevel> {
        let quantity = self.config.quantity();
        match side {
            Side::Buy => self
                .bids
                .iter()
                .rev()
                .map(|(&price, &purpose)| GridLevel::new(side, price, quantity, purpose))
                .collect(),
            Side::Sell => self
                .asks
                .iter()
                .map(|(&price, &purpose)| GridLevel::new(side, price, quantity, purpose))
                .collect(),
        }
    }

    pub fn contains(&self, side: Side, price: PriceTicks) -> bool {
        match side {
            Side::Buy => self.bids.contains_key(&price),
            Side::Sell => self.asks.contains_key(&price),
        }
    }

    pub fn level(&self, side: Side, price: PriceTicks) -> Option<GridLevel> {
        let purpose = match side {
            Side::Buy => self.bids.get(&price),
            Side::Sell => self.asks.get(&price),
        }?;
        Some(GridLevel::new(
            side,
            price,
            self.config.quantity(),
            *purpose,
        ))
    }

    /// Applies a unique, fully-filled active level as one atomic grid transition.
    pub fn apply_fill(&mut self, fill: FilledLevel) -> Result<GridTransition, GridError> {
        let mut candidate = self.clone();
        let current =
            candidate
                .level(fill.side(), fill.price())
                .ok_or(GridError::LevelNotFound {
                    side: fill.side(),
                    price: fill.price(),
                })?;
        if current.purpose() != fill.purpose() {
            return Err(GridError::PurposeMismatch {
                side: fill.side(),
                price: fill.price(),
                expected: current.purpose(),
                actual: fill.purpose(),
            });
        }
        let revision = candidate
            .revision
            .checked_next()
            .ok_or(GridError::RevisionOverflow)?;
        let work = match fill.purpose() {
            GridPurpose::Quote => match fill.side() {
                Side::Sell => candidate.roll_quote_ask_fill(fill.price())?,
                Side::Buy => candidate.roll_quote_bid_fill(fill.price())?,
            },
            GridPurpose::TakeProfit => {
                candidate.roll_take_profit_fill(fill.side(), fill.price())?
            }
        };

        candidate.revision = revision;
        candidate.validate()?;
        *self = candidate;

        Ok(GridTransition::new(
            revision,
            current,
            work.cancel,
            work.placements,
            work.reassignment,
        ))
    }

    /// Applies a fill for a Quote order that was removed from the desired
    /// ladder by an earlier transition but filled before its cancellation was
    /// acknowledged. The caller must have verified the physical order
    /// identity; TakeProfit fills are never accepted through this path.
    pub fn apply_late_quote_fill(
        &mut self,
        fill: FilledLevel,
    ) -> Result<GridTransition, GridError> {
        if fill.purpose() != GridPurpose::Quote || self.contains(fill.side(), fill.price()) {
            return Err(GridError::LevelNotFound {
                side: fill.side(),
                price: fill.price(),
            });
        }

        let mut candidate = self.clone();
        let current = GridLevel::new(
            fill.side(),
            fill.price(),
            candidate.config.quantity(),
            GridPurpose::Quote,
        );
        let revision = candidate
            .revision
            .checked_next()
            .ok_or(GridError::RevisionOverflow)?;
        let work = match fill.side() {
            Side::Sell => candidate.roll_late_quote_ask_fill(fill.price())?,
            Side::Buy => candidate.roll_late_quote_bid_fill(fill.price())?,
        };

        candidate.revision = revision;
        candidate.validate()?;
        *self = candidate;

        Ok(GridTransition::new(
            revision,
            current,
            work.cancel,
            work.placements,
            work.reassignment,
        ))
    }

    fn roll_quote_ask_fill(&mut self, price: PriceTicks) -> Result<GridWork, GridError> {
        let quantity = self.config.quantity();
        let farthest_ask = *self
            .asks
            .keys()
            .next_back()
            .expect("a valid grid always contains an ask");
        let new_far_ask = checked_add(Side::Sell, farthest_ask, self.config.spacing().get())?;

        self.asks.remove(&price);
        insert_unique(&mut self.asks, Side::Sell, new_far_ask, GridPurpose::Quote)?;
        let far_quote = GridLevel::new(Side::Sell, new_far_ask, quantity, GridPurpose::Quote);
        self.insert_take_profit_for_ask(price, far_quote)
    }

    fn roll_late_quote_ask_fill(&mut self, price: PriceTicks) -> Result<GridWork, GridError> {
        let quantity = self.config.quantity();
        let farthest_quote = self
            .asks
            .iter()
            .rev()
            .find_map(|(&candidate, &purpose)| (purpose == GridPurpose::Quote).then_some(candidate))
            .ok_or(GridError::NoQuoteCapacity { side: Side::Sell })?;
        self.asks.remove(&farthest_quote);
        let new_far_ask = checked_add(Side::Sell, price, self.config.spacing().get())?;
        insert_unique(&mut self.asks, Side::Sell, new_far_ask, GridPurpose::Quote)?;
        let far_quote = GridLevel::new(Side::Sell, new_far_ask, quantity, GridPurpose::Quote);
        self.insert_take_profit_for_ask(price, far_quote)
    }

    fn roll_quote_bid_fill(&mut self, price: PriceTicks) -> Result<GridWork, GridError> {
        let quantity = self.config.quantity();
        let farthest_bid = *self
            .bids
            .keys()
            .next()
            .expect("a valid grid always contains a bid");
        let new_far_bid = checked_sub(Side::Buy, farthest_bid, self.config.spacing().get())?;

        self.bids.remove(&price);
        insert_unique(&mut self.bids, Side::Buy, new_far_bid, GridPurpose::Quote)?;
        let far_quote = GridLevel::new(Side::Buy, new_far_bid, quantity, GridPurpose::Quote);
        self.insert_take_profit_for_bid(price, far_quote)
    }

    fn roll_late_quote_bid_fill(&mut self, price: PriceTicks) -> Result<GridWork, GridError> {
        let quantity = self.config.quantity();
        let farthest_quote = self
            .bids
            .iter()
            .find_map(|(&candidate, &purpose)| (purpose == GridPurpose::Quote).then_some(candidate))
            .ok_or(GridError::NoQuoteCapacity { side: Side::Buy })?;
        self.bids.remove(&farthest_quote);
        let new_far_bid = checked_sub(Side::Buy, price, self.config.spacing().get())?;
        insert_unique(&mut self.bids, Side::Buy, new_far_bid, GridPurpose::Quote)?;
        let far_quote = GridLevel::new(Side::Buy, new_far_bid, quantity, GridPurpose::Quote);
        self.insert_take_profit_for_bid(price, far_quote)
    }

    fn insert_take_profit_for_ask(
        &mut self,
        filled_price: PriceTicks,
        far_quote: GridLevel,
    ) -> Result<GridWork, GridError> {
        let quantity = self.config.quantity();
        let take_profit_price =
            checked_sub(Side::Buy, filled_price, self.config.take_profit().get())?;

        if let Some(existing) = self.bids.get(&take_profit_price).copied() {
            if existing == GridPurpose::TakeProfit {
                return Err(GridError::TakeProfitCollision {
                    side: Side::Buy,
                    price: take_profit_price,
                });
            }
            self.bids.insert(take_profit_price, GridPurpose::TakeProfit);
            let previous =
                GridLevel::new(Side::Buy, take_profit_price, quantity, GridPurpose::Quote);
            return Ok(GridWork {
                cancel: None,
                placements: [Some(far_quote), None],
                reassignment: Some(GridReassignment::new(
                    previous,
                    previous.with_purpose(GridPurpose::TakeProfit),
                )),
            });
        }

        let farthest_quote_bid = self
            .bids
            .iter()
            .find_map(|(&candidate, &purpose)| (purpose == GridPurpose::Quote).then_some(candidate))
            .ok_or(GridError::NoQuoteCapacity { side: Side::Buy })?;
        self.bids.remove(&farthest_quote_bid);
        insert_unique(
            &mut self.bids,
            Side::Buy,
            take_profit_price,
            GridPurpose::TakeProfit,
        )?;

        Ok(GridWork {
            cancel: Some(GridLevel::new(
                Side::Buy,
                farthest_quote_bid,
                quantity,
                GridPurpose::Quote,
            )),
            placements: [
                Some(GridLevel::new(
                    Side::Buy,
                    take_profit_price,
                    quantity,
                    GridPurpose::TakeProfit,
                )),
                Some(far_quote),
            ],
            reassignment: None,
        })
    }

    fn insert_take_profit_for_bid(
        &mut self,
        filled_price: PriceTicks,
        far_quote: GridLevel,
    ) -> Result<GridWork, GridError> {
        let quantity = self.config.quantity();
        let take_profit_price =
            checked_add(Side::Sell, filled_price, self.config.take_profit().get())?;

        if let Some(existing) = self.asks.get(&take_profit_price).copied() {
            if existing == GridPurpose::TakeProfit {
                return Err(GridError::TakeProfitCollision {
                    side: Side::Sell,
                    price: take_profit_price,
                });
            }
            self.asks.insert(take_profit_price, GridPurpose::TakeProfit);
            let previous =
                GridLevel::new(Side::Sell, take_profit_price, quantity, GridPurpose::Quote);
            return Ok(GridWork {
                cancel: None,
                placements: [Some(far_quote), None],
                reassignment: Some(GridReassignment::new(
                    previous,
                    previous.with_purpose(GridPurpose::TakeProfit),
                )),
            });
        }

        let farthest_quote_ask = self
            .asks
            .iter()
            .rev()
            .find_map(|(&candidate, &purpose)| (purpose == GridPurpose::Quote).then_some(candidate))
            .ok_or(GridError::NoQuoteCapacity { side: Side::Sell })?;
        self.asks.remove(&farthest_quote_ask);
        insert_unique(
            &mut self.asks,
            Side::Sell,
            take_profit_price,
            GridPurpose::TakeProfit,
        )?;

        Ok(GridWork {
            cancel: Some(GridLevel::new(
                Side::Sell,
                farthest_quote_ask,
                quantity,
                GridPurpose::Quote,
            )),
            placements: [
                Some(GridLevel::new(
                    Side::Sell,
                    take_profit_price,
                    quantity,
                    GridPurpose::TakeProfit,
                )),
                Some(far_quote),
            ],
            reassignment: None,
        })
    }

    fn roll_take_profit_fill(
        &mut self,
        side: Side,
        price: PriceTicks,
    ) -> Result<GridWork, GridError> {
        let quantity = self.config.quantity();
        let far_quote = match side {
            Side::Buy => {
                self.bids.remove(&price);
                let anchor = self.bids.keys().next().copied().unwrap_or(price);
                let new_far = checked_sub(Side::Buy, anchor, self.config.spacing().get())?;
                insert_unique(&mut self.bids, Side::Buy, new_far, GridPurpose::Quote)?;
                GridLevel::new(Side::Buy, new_far, quantity, GridPurpose::Quote)
            }
            Side::Sell => {
                self.asks.remove(&price);
                let anchor = self.asks.keys().next_back().copied().unwrap_or(price);
                let new_far = checked_add(Side::Sell, anchor, self.config.spacing().get())?;
                insert_unique(&mut self.asks, Side::Sell, new_far, GridPurpose::Quote)?;
                GridLevel::new(Side::Sell, new_far, quantity, GridPurpose::Quote)
            }
        };

        Ok(GridWork {
            cancel: None,
            placements: [Some(far_quote), None],
            reassignment: None,
        })
    }

    fn validate(&self) -> Result<(), GridError> {
        let expected = self.config.levels_per_side().get();
        validate_count(Side::Buy, expected, self.bids.len())?;
        validate_count(Side::Sell, expected, self.asks.len())?;

        let highest_bid = *self
            .bids
            .keys()
            .next_back()
            .expect("level count validation guarantees a bid");
        let lowest_ask = *self
            .asks
            .keys()
            .next()
            .expect("level count validation guarantees an ask");
        if highest_bid >= lowest_ask {
            return Err(GridError::CrossedGrid {
                highest_bid,
                lowest_ask,
            });
        }
        Ok(())
    }
}

fn validate_count(side: Side, expected: usize, actual: usize) -> Result<(), GridError> {
    if actual != expected {
        return Err(GridError::InvalidLevelCount {
            side,
            expected,
            actual,
        });
    }
    Ok(())
}

fn insert_unique(
    levels: &mut BTreeMap<PriceTicks, GridPurpose>,
    side: Side,
    price: PriceTicks,
    purpose: GridPurpose,
) -> Result<(), GridError> {
    if levels.insert(price, purpose).is_some() {
        return Err(GridError::PriceCollision { side, price });
    }
    Ok(())
}

struct GridWork {
    cancel: Option<GridLevel>,
    placements: [Option<GridLevel>; 2],
    reassignment: Option<GridReassignment>,
}

fn checked_add(side: Side, base: PriceTicks, ticks: u64) -> Result<PriceTicks, GridError> {
    base.checked_add(ticks)
        .ok_or(GridError::PriceOverflow { side, base, ticks })
}

fn checked_sub(side: Side, base: PriceTicks, ticks: u64) -> Result<PriceTicks, GridError> {
    base.checked_sub(ticks)
        .ok_or(GridError::PriceUnderflow { side, base, ticks })
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use crate::{NonZeroTickCount, QuantityLots};

    use super::*;

    fn config(levels: usize, inner: u64, spacing: u64, take_profit: u64) -> GridConfig {
        GridConfig::new(
            NonZeroUsize::new(levels).unwrap(),
            NonZeroTickCount::new(inner).unwrap(),
            NonZeroTickCount::new(spacing).unwrap(),
            NonZeroTickCount::new(take_profit).unwrap(),
            QuantityLots::new(2).unwrap(),
        )
    }

    fn book(bid: u64, ask: u64) -> BestBidAsk {
        BestBidAsk::new(
            Symbol::new("BTCUSDT").unwrap(),
            PriceTicks::new(bid).unwrap(),
            PriceTicks::new(ask).unwrap(),
        )
        .unwrap()
    }

    fn prices(model: &GridModel, side: Side) -> Vec<u64> {
        model
            .levels(side)
            .into_iter()
            .map(|level| level.price().get())
            .collect()
    }

    #[test]
    fn initializes_symmetric_tick_ladders() {
        let model = GridModel::initialize(config(3, 1, 1, 1), &book(99, 100)).unwrap();
        assert_eq!(prices(&model, Side::Buy), vec![98, 97, 96]);
        assert_eq!(prices(&model, Side::Sell), vec![101, 102, 103]);
        assert!(
            model
                .levels(Side::Buy)
                .into_iter()
                .chain(model.levels(Side::Sell))
                .all(|level| level.purpose() == GridPurpose::Quote)
        );
        assert_eq!(model.revision(), GridRevision::ZERO);
    }

    #[test]
    fn quote_ask_fill_places_take_profit_bid_and_far_quote_ask() {
        let mut model = GridModel::initialize(config(3, 1, 1, 1), &book(99, 100)).unwrap();
        let transition = model
            .apply_fill(FilledLevel::new(
                Side::Sell,
                PriceTicks::new(101).unwrap(),
                GridPurpose::Quote,
            ))
            .unwrap();

        assert_eq!(prices(&model, Side::Buy), vec![100, 98, 97]);
        assert_eq!(prices(&model, Side::Sell), vec![102, 103, 104]);
        assert_eq!(
            model
                .level(Side::Buy, PriceTicks::new(100).unwrap())
                .unwrap()
                .purpose(),
            GridPurpose::TakeProfit
        );
        assert_eq!(transition.cancel().unwrap().price().get(), 96);
        let placements: Vec<_> = transition.placements().collect();
        assert_eq!(placements[0].purpose(), GridPurpose::TakeProfit);
        assert_eq!(placements[0].price().get(), 100);
        assert_eq!(placements[1].purpose(), GridPurpose::Quote);
        assert_eq!(placements[1].price().get(), 104);
        assert_eq!(transition.revision().get(), 1);
    }

    #[test]
    fn bid_fill_is_the_exact_mirror_transition() {
        let mut model = GridModel::initialize(config(3, 1, 1, 1), &book(99, 100)).unwrap();
        let transition = model
            .apply_fill(FilledLevel::new(
                Side::Buy,
                PriceTicks::new(98).unwrap(),
                GridPurpose::Quote,
            ))
            .unwrap();

        assert_eq!(prices(&model, Side::Buy), vec![97, 96, 95]);
        assert_eq!(prices(&model, Side::Sell), vec![99, 101, 102]);
        assert_eq!(transition.cancel().unwrap().price().get(), 103);
        let placements: Vec<_> = transition.placements().collect();
        assert_eq!(placements[0].price().get(), 99);
        assert_eq!(placements[0].purpose(), GridPurpose::TakeProfit);
        assert_eq!(placements[1].price().get(), 95);
        assert_eq!(placements[1].purpose(), GridPurpose::Quote);
    }

    #[test]
    fn same_price_quote_is_reassigned_to_take_profit_without_remote_replacement() {
        let mut model = GridModel::initialize(config(3, 1, 1, 3), &book(99, 100)).unwrap();
        let transition = model
            .apply_fill(FilledLevel::new(
                Side::Sell,
                PriceTicks::new(101).unwrap(),
                GridPurpose::Quote,
            ))
            .unwrap();

        assert_eq!(prices(&model, Side::Buy), vec![98, 97, 96]);
        assert!(transition.cancel().is_none());
        assert_eq!(transition.placements().count(), 1);
        let reassignment = transition.reassignment().unwrap();
        assert_eq!(reassignment.previous().purpose(), GridPurpose::Quote);
        assert_eq!(reassignment.current().purpose(), GridPurpose::TakeProfit);
        assert_eq!(reassignment.current().price().get(), 98);
    }

    #[test]
    fn take_profit_fill_restores_a_far_quote_on_the_same_side() {
        let mut model = GridModel::initialize(config(3, 1, 1, 1), &book(99, 100)).unwrap();
        model
            .apply_fill(FilledLevel::new(
                Side::Sell,
                PriceTicks::new(101).unwrap(),
                GridPurpose::Quote,
            ))
            .unwrap();
        let transition = model
            .apply_fill(FilledLevel::new(
                Side::Buy,
                PriceTicks::new(100).unwrap(),
                GridPurpose::TakeProfit,
            ))
            .unwrap();

        assert_eq!(prices(&model, Side::Buy), vec![98, 97, 96]);
        assert_eq!(prices(&model, Side::Sell), vec![102, 103, 104]);
        assert!(transition.cancel().is_none());
        assert!(transition.reassignment().is_none());
        let placements: Vec<_> = transition.placements().collect();
        assert_eq!(placements.len(), 1);
        assert_eq!(placements[0].price().get(), 96);
        assert_eq!(placements[0].purpose(), GridPurpose::Quote);
    }

    #[test]
    fn late_quote_fill_is_applied_after_its_level_was_retired() {
        let mut model = GridModel::initialize(config(3, 1, 1, 10), &book(99, 100)).unwrap();
        model
            .apply_fill(FilledLevel::new(
                Side::Sell,
                PriceTicks::new(101).unwrap(),
                GridPurpose::Quote,
            ))
            .unwrap();

        let transition = model
            .apply_late_quote_fill(FilledLevel::new(
                Side::Buy,
                PriceTicks::new(96).unwrap(),
                GridPurpose::Quote,
            ))
            .unwrap();

        assert_eq!(prices(&model, Side::Buy), vec![98, 95, 91]);
        assert_eq!(prices(&model, Side::Sell), vec![102, 103, 106]);
        assert_eq!(transition.consumed().price().get(), 96);
        assert_eq!(transition.revision().get(), 2);
        assert_eq!(
            model
                .level(Side::Sell, PriceTicks::new(106).unwrap())
                .unwrap()
                .purpose(),
            GridPurpose::TakeProfit
        );
    }

    #[test]
    fn rejected_transition_does_not_mutate_the_grid() {
        let mut model = GridModel::initialize(config(3, 1, 1, 1), &book(99, 100)).unwrap();
        let before = model.clone();
        let result = model.apply_fill(FilledLevel::new(
            Side::Sell,
            PriceTicks::new(102).unwrap(),
            GridPurpose::Quote,
        ));

        assert!(matches!(result, Err(GridError::CrossedGrid { .. })));
        assert_eq!(model, before);
    }

    #[test]
    fn duplicate_fill_is_rejected_without_mutation() {
        let mut model = GridModel::initialize(config(3, 1, 1, 1), &book(99, 100)).unwrap();
        let fill = FilledLevel::new(
            Side::Sell,
            PriceTicks::new(101).unwrap(),
            GridPurpose::Quote,
        );
        model.apply_fill(fill).unwrap();
        let before = model.clone();

        assert!(matches!(
            model.apply_fill(fill),
            Err(GridError::LevelNotFound { .. })
        ));
        assert_eq!(model, before);
    }

    #[test]
    fn detects_initial_bid_underflow() {
        assert!(matches!(
            GridModel::initialize(config(5, 2, 2, 1), &book(2, 3)),
            Err(GridError::PriceUnderflow { .. })
        ));
    }
}
