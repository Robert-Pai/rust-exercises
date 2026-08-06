use crate::{ClientOrderId, PriceTicks, QuantityLots, Side, Symbol};

/// Exchange-neutral execution semantics requested for an order.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ExecutionPolicy {
    PostOnly,
}

/// A validated request to create one limit order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OrderIntent {
    price: PriceTicks,
    quantity: QuantityLots,
    client_order_id: ClientOrderId,
    symbol: Symbol,
    side: Side,
    execution: ExecutionPolicy,
}

impl OrderIntent {
    pub fn post_only(
        symbol: Symbol,
        client_order_id: ClientOrderId,
        side: Side,
        price: PriceTicks,
        quantity: QuantityLots,
    ) -> Self {
        Self {
            symbol,
            client_order_id,
            side,
            price,
            quantity,
            execution: ExecutionPolicy::PostOnly,
        }
    }

    pub fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    pub fn client_order_id(&self) -> &ClientOrderId {
        &self.client_order_id
    }

    pub const fn side(&self) -> Side {
        self.side
    }

    pub const fn price(&self) -> PriceTicks {
        self.price
    }

    pub const fn quantity(&self) -> QuantityLots {
        self.quantity
    }

    pub const fn execution(&self) -> ExecutionPolicy {
        self.execution
    }
}
