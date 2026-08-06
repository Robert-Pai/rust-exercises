use maker_domain::{ClientOrderId, ExchangeOrderId, OrderUpdate, Symbol};

/// The account position accounting mode relevant to order placement.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PositionMode {
    OneWay,
    Hedge,
}

/// Identity assigned to a successfully accepted order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PlaceOrderAck {
    client_order_id: ClientOrderId,
    exchange_order_id: ExchangeOrderId,
    symbol: Symbol,
}

impl PlaceOrderAck {
    pub fn new(
        symbol: Symbol,
        client_order_id: ClientOrderId,
        exchange_order_id: ExchangeOrderId,
    ) -> Self {
        Self {
            symbol,
            client_order_id,
            exchange_order_id,
        }
    }

    pub fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    pub fn client_order_id(&self) -> &ClientOrderId {
        &self.client_order_id
    }

    pub fn exchange_order_id(&self) -> &ExchangeOrderId {
        &self.exchange_order_id
    }
}

/// The terminal resolution of a single-order cancellation request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CancelOutcome {
    /// The exchange acknowledged cancellation but supplied no full update.
    Canceled,

    /// The exchange supplied the final state, including cancellation or a fill
    /// that raced with cancellation.
    Terminal(OrderUpdate),

    /// The exchange no longer recognizes either identifier for this order.
    NotFound,
}

impl CancelOutcome {
    pub fn terminal_update(&self) -> Option<&OrderUpdate> {
        match self {
            Self::Terminal(update) => Some(update),
            Self::Canceled | Self::NotFound => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use maker_domain::{FilledLots, OrderStatus, PriceTicks, QuantityLots, Side};

    use super::*;

    #[test]
    fn exposes_terminal_cancel_update() {
        let update = OrderUpdate::new(
            Symbol::new("BTCUSDT").unwrap(),
            ClientOrderId::new(1).unwrap(),
            ExchangeOrderId::new(42).unwrap(),
            Side::Buy,
            PriceTicks::new(100).unwrap(),
            QuantityLots::new(2).unwrap(),
            FilledLots::new(2),
            OrderStatus::Filled,
        )
        .unwrap();
        let outcome = CancelOutcome::Terminal(update);

        assert_eq!(outcome.terminal_update(), Some(&update));
        assert_eq!(CancelOutcome::Canceled.terminal_update(), None);
    }
}
