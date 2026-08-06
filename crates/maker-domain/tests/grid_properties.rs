use std::num::NonZeroUsize;

use maker_domain::{
    BestBidAsk, FilledLevel, GridConfig, GridModel, GridPurpose, NonZeroTickCount, PriceTicks,
    QuantityLots, Side, Symbol,
};
use proptest::prelude::*;
use proptest::test_runner::TestCaseResult;

fn assert_invariants(model: &GridModel, levels: usize) -> TestCaseResult {
    let bids = model.levels(Side::Buy);
    let asks = model.levels(Side::Sell);

    prop_assert_eq!(bids.len(), levels);
    prop_assert_eq!(asks.len(), levels);
    prop_assert!(
        bids.windows(2)
            .all(|pair| pair[0].price() > pair[1].price())
    );
    prop_assert!(
        asks.windows(2)
            .all(|pair| pair[0].price() < pair[1].price())
    );
    prop_assert!(bids[0].price() < asks[0].price());
    Ok(())
}

proptest! {
    #[test]
    fn repeated_nearest_ask_fills_preserve_grid_invariants(
        levels in 1usize..20,
        inner in 1u64..50,
        spacing in 1u64..20,
        fills in 1usize..30,
    ) {
        let config = GridConfig::new(
            NonZeroUsize::new(levels).unwrap(),
            NonZeroTickCount::new(inner).unwrap(),
            NonZeroTickCount::new(spacing).unwrap(),
            NonZeroTickCount::new(1).unwrap(),
            QuantityLots::new(1).unwrap(),
        );
        let book = BestBidAsk::new(
            Symbol::new("BTCUSDT").unwrap(),
            PriceTicks::new(1_000_000).unwrap(),
            PriceTicks::new(1_000_001).unwrap(),
        ).unwrap();
        let mut model = GridModel::initialize(config, &book).unwrap();

        for _ in 0..fills {
            let nearest_ask = model.levels(Side::Sell)[0];
            model.apply_fill(FilledLevel::from(nearest_ask)).unwrap();
            let take_profit = model
                .levels(Side::Buy)
                .into_iter()
                .find(|level| level.purpose() == GridPurpose::TakeProfit)
                .unwrap();
            model.apply_fill(FilledLevel::from(take_profit)).unwrap();
            assert_invariants(&model, levels)?;
        }
        prop_assert_eq!(model.revision().get(), (fills * 2) as u64);
    }

    #[test]
    fn repeated_nearest_bid_fills_preserve_grid_invariants(
        levels in 1usize..20,
        inner in 1u64..50,
        spacing in 1u64..20,
        fills in 1usize..30,
    ) {
        let config = GridConfig::new(
            NonZeroUsize::new(levels).unwrap(),
            NonZeroTickCount::new(inner).unwrap(),
            NonZeroTickCount::new(spacing).unwrap(),
            NonZeroTickCount::new(1).unwrap(),
            QuantityLots::new(1).unwrap(),
        );
        let book = BestBidAsk::new(
            Symbol::new("BTCUSDT").unwrap(),
            PriceTicks::new(1_000_000).unwrap(),
            PriceTicks::new(1_000_001).unwrap(),
        ).unwrap();
        let mut model = GridModel::initialize(config, &book).unwrap();

        for _ in 0..fills {
            let nearest_bid = model.levels(Side::Buy)[0];
            model.apply_fill(FilledLevel::from(nearest_bid)).unwrap();
            let take_profit = model
                .levels(Side::Sell)
                .into_iter()
                .find(|level| level.purpose() == GridPurpose::TakeProfit)
                .unwrap();
            model.apply_fill(FilledLevel::from(take_profit)).unwrap();
            assert_invariants(&model, levels)?;
        }
        prop_assert_eq!(model.revision().get(), (fills * 2) as u64);
    }

    #[test]
    fn mixed_nearest_fills_preserve_grid_invariants(
        levels in 1usize..20,
        inner in 1u64..50,
        spacing in 1u64..20,
        sell_fills in prop::collection::vec(any::<bool>(), 1..50),
    ) {
        let config = GridConfig::new(
            NonZeroUsize::new(levels).unwrap(),
            NonZeroTickCount::new(inner).unwrap(),
            NonZeroTickCount::new(spacing).unwrap(),
            NonZeroTickCount::new(1).unwrap(),
            QuantityLots::new(1).unwrap(),
        );
        let book = BestBidAsk::new(
            Symbol::new("BTCUSDT").unwrap(),
            PriceTicks::new(1_000_000).unwrap(),
            PriceTicks::new(1_000_001).unwrap(),
        ).unwrap();
        let mut model = GridModel::initialize(config, &book).unwrap();

        for sell_fill in &sell_fills {
            let side = if *sell_fill { Side::Sell } else { Side::Buy };
            let nearest_quote = model
                .levels(side)
                .into_iter()
                .find(|level| level.purpose() == GridPurpose::Quote)
                .unwrap();
            model.apply_fill(FilledLevel::from(nearest_quote)).unwrap();
            let take_profit_side = match side {
                Side::Buy => Side::Sell,
                Side::Sell => Side::Buy,
            };
            let take_profit = model
                .levels(take_profit_side)
                .into_iter()
                .find(|level| level.purpose() == GridPurpose::TakeProfit)
                .unwrap();
            model.apply_fill(FilledLevel::from(take_profit)).unwrap();
            assert_invariants(&model, levels)?;
        }
        prop_assert_eq!(model.revision().get(), (sell_fills.len() * 2) as u64);
    }
}
