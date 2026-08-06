use std::collections::HashMap;

use maker_domain::{ClientOrderId, ExchangeOrderId, GridLevel, OrderStatus, OrderUpdate, Symbol};
use maker_ports::PlaceOrderAck;
use thiserror::Error;

/// Whether an exchange order still contributes to the live side count.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RegistryState {
    Active,
    Terminal,
}

/// One accepted exchange order and the grid level it represents.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegisteredOrder {
    symbol: Symbol,
    client_order_id: ClientOrderId,
    exchange_order_id: ExchangeOrderId,
    level: GridLevel,
    status: OrderStatus,
    state: RegistryState,
}

impl RegisteredOrder {
    pub fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    pub fn client_order_id(&self) -> &ClientOrderId {
        &self.client_order_id
    }

    pub fn exchange_order_id(&self) -> &ExchangeOrderId {
        &self.exchange_order_id
    }

    pub const fn level(&self) -> GridLevel {
        self.level
    }

    pub const fn status(&self) -> OrderStatus {
        self.status
    }

    pub const fn state(&self) -> RegistryState {
        self.state
    }
}

/// Actual exchange orders known to the current engine session.
#[derive(Debug, Default)]
pub struct OrderRegistry {
    orders: HashMap<ClientOrderId, RegisteredOrder>,
}

impl OrderRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.orders.len()
    }

    pub fn is_empty(&self) -> bool {
        self.orders.is_empty()
    }

    pub fn get(&self, client_order_id: &ClientOrderId) -> Option<&RegisteredOrder> {
        self.orders.get(client_order_id)
    }

    pub fn active_orders(&self) -> Vec<&RegisteredOrder> {
        self.orders
            .values()
            .filter(|order| order.state == RegistryState::Active)
            .collect()
    }

    pub fn active_levels(&self) -> Vec<GridLevel> {
        self.active_orders()
            .into_iter()
            .map(RegisteredOrder::level)
            .collect()
    }

    pub fn active_count(&self, side: maker_domain::Side) -> usize {
        self.orders
            .values()
            .filter(|order| order.state == RegistryState::Active && order.level.side() == side)
            .count()
    }

    pub fn has_active_level(&self, level: GridLevel) -> bool {
        self.orders
            .values()
            .any(|order| order.state == RegistryState::Active && order.level.same_order(level))
    }

    pub(crate) fn reassign_matching_level(
        &mut self,
        previous: GridLevel,
        current: GridLevel,
    ) -> Result<Option<ClientOrderId>, RegistryError> {
        let client_order_id = self
            .orders
            .values()
            .find(|order| order.state == RegistryState::Active && order.level.same_order(previous))
            .map(|order| order.client_order_id.clone());
        let Some(client_order_id) = client_order_id else {
            return Ok(None);
        };
        let order = self
            .orders
            .get_mut(&client_order_id)
            .expect("the matching order was found in the same registry");
        order.level = current;
        Ok(Some(client_order_id))
    }

    pub(crate) fn register(
        &mut self,
        level: GridLevel,
        ack: PlaceOrderAck,
    ) -> Result<(), RegistryError> {
        if self.orders.contains_key(ack.client_order_id()) {
            return Err(RegistryError::DuplicateClientOrderId(
                ack.client_order_id().clone(),
            ));
        }
        if self.has_active_level(level) {
            return Err(RegistryError::DuplicateActiveLevel(level));
        }

        let order = RegisteredOrder {
            symbol: ack.symbol().clone(),
            client_order_id: ack.client_order_id().clone(),
            exchange_order_id: ack.exchange_order_id().clone(),
            level,
            status: OrderStatus::Accepted,
            state: RegistryState::Active,
        };
        self.orders.insert(order.client_order_id.clone(), order);
        Ok(())
    }

    pub(crate) fn validate_update(
        &self,
        update: &OrderUpdate,
    ) -> Result<Option<GridLevel>, RegistryError> {
        let Some(order) = self.orders.get(update.client_order_id()) else {
            return Ok(None);
        };
        if order.symbol != *update.symbol()
            || order.exchange_order_id != *update.exchange_order_id()
            || !order.level.same_order(GridLevel::new(
                update.side(),
                update.price(),
                update.original_quantity(),
                order.level.purpose(),
            ))
        {
            return Err(RegistryError::UpdateIdentityMismatch {
                client_order_id: update.client_order_id().clone(),
            });
        }
        Ok(Some(order.level))
    }

    pub(crate) fn apply_update(
        &mut self,
        update: &OrderUpdate,
    ) -> Result<Option<GridLevel>, RegistryError> {
        let level = self.validate_update(update)?;
        let Some(order) = self.orders.get_mut(update.client_order_id()) else {
            return Ok(None);
        };
        order.status = update.status();
        if update.status().is_terminal() {
            order.state = RegistryState::Terminal;
        }
        Ok(level)
    }

    pub(crate) fn mark_canceled(
        &mut self,
        client_order_id: &ClientOrderId,
    ) -> Result<(), RegistryError> {
        let order = self
            .orders
            .get_mut(client_order_id)
            .ok_or_else(|| RegistryError::UnknownOrder(client_order_id.clone()))?;
        order.status = OrderStatus::Canceled;
        order.state = RegistryState::Terminal;
        Ok(())
    }

    pub(crate) fn clear(&mut self) {
        self.orders.clear();
    }

    pub(crate) fn discard(&mut self, client_order_id: &ClientOrderId) -> Option<RegisteredOrder> {
        self.orders.remove(client_order_id)
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum RegistryError {
    #[error("client order ID {0} is already registered")]
    DuplicateClientOrderId(ClientOrderId),

    #[error("an active order already represents {0:?}")]
    DuplicateActiveLevel(GridLevel),

    #[error("order update identity does not match registered order {client_order_id}")]
    UpdateIdentityMismatch { client_order_id: ClientOrderId },

    #[error("client order ID {0} is not registered")]
    UnknownOrder(ClientOrderId),
}

#[cfg(test)]
mod tests {
    use maker_domain::{ExchangeOrderId, PriceTicks, QuantityLots, Side, Symbol};

    use super::*;

    #[test]
    fn terminal_orders_stop_counting_as_live() {
        let symbol = Symbol::new("BTCUSDT").unwrap();
        let client_order_id = ClientOrderId::new("mk-1").unwrap();
        let level = GridLevel::new(
            Side::Buy,
            PriceTicks::new(100).unwrap(),
            QuantityLots::new(2).unwrap(),
            maker_domain::GridPurpose::Quote,
        );
        let mut registry = OrderRegistry::new();
        registry
            .register(
                level,
                PlaceOrderAck::new(
                    symbol,
                    client_order_id.clone(),
                    ExchangeOrderId::new("1").unwrap(),
                ),
            )
            .unwrap();

        assert_eq!(registry.active_count(Side::Buy), 1);
        registry.mark_canceled(&client_order_id).unwrap();
        assert_eq!(registry.active_count(Side::Buy), 0);
        assert!(registry.discard(&client_order_id).is_some());
        assert!(registry.is_empty());
    }
}
