use thiserror::Error;

use crate::{ClientOrderId, ExchangeOrderId, FilledLots, PriceTicks, QuantityLots, Side, Symbol};

use super::OrderStatus;

/// A normalized exchange order update.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderUpdate {
    symbol: Symbol,
    client_order_id: ClientOrderId,
    exchange_order_id: ExchangeOrderId,
    side: Side,
    price: PriceTicks,
    original_quantity: QuantityLots,
    cumulative_filled: FilledLots,
    status: OrderStatus,
}

impl OrderUpdate {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        symbol: Symbol,
        client_order_id: ClientOrderId,
        exchange_order_id: ExchangeOrderId,
        side: Side,
        price: PriceTicks,
        original_quantity: QuantityLots,
        cumulative_filled: FilledLots,
        status: OrderStatus,
    ) -> Result<Self, OrderUpdateError> {
        let filled = cumulative_filled.get();
        let original = original_quantity.get();
        if filled > original {
            return Err(OrderUpdateError::Overfilled { filled, original });
        }
        match status {
            OrderStatus::Accepted if filled != 0 => {
                return Err(OrderUpdateError::InvalidFilledAmount { status, filled });
            }
            OrderStatus::PartiallyFilled if filled == 0 || filled == original => {
                return Err(OrderUpdateError::InvalidFilledAmount { status, filled });
            }
            OrderStatus::Filled if filled != original => {
                return Err(OrderUpdateError::InvalidFilledAmount { status, filled });
            }
            _ => {}
        }

        Ok(Self {
            symbol,
            client_order_id,
            exchange_order_id,
            side,
            price,
            original_quantity,
            cumulative_filled,
            status,
        })
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

    pub const fn side(&self) -> Side {
        self.side
    }

    pub const fn price(&self) -> PriceTicks {
        self.price
    }

    pub const fn original_quantity(&self) -> QuantityLots {
        self.original_quantity
    }

    pub const fn cumulative_filled(&self) -> FilledLots {
        self.cumulative_filled
    }

    pub const fn status(&self) -> OrderStatus {
        self.status
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum OrderUpdateError {
    #[error("cumulative fill {filled} exceeds original quantity {original}")]
    Overfilled { filled: u64, original: u64 },

    #[error("filled amount {filled} is inconsistent with status {status:?}")]
    InvalidFilledAmount { status: OrderStatus, filled: u64 },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn update(status: OrderStatus, filled: u64) -> Result<OrderUpdate, OrderUpdateError> {
        OrderUpdate::new(
            Symbol::new("BTCUSDT").unwrap(),
            ClientOrderId::new("maker-1").unwrap(),
            ExchangeOrderId::new("42").unwrap(),
            Side::Buy,
            PriceTicks::new(100).unwrap(),
            QuantityLots::new(10).unwrap(),
            FilledLots::new(filled),
            status,
        )
    }

    #[test]
    fn enforces_status_quantity_consistency() {
        assert!(update(OrderStatus::Accepted, 0).is_ok());
        assert!(update(OrderStatus::PartiallyFilled, 5).is_ok());
        assert!(update(OrderStatus::Filled, 10).is_ok());
        assert!(update(OrderStatus::PartiallyFilled, 0).is_err());
        assert!(update(OrderStatus::Filled, 9).is_err());
        assert!(update(OrderStatus::Canceled, 5).is_ok());
    }
}
