/// The normalized lifecycle state reported by an exchange.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum OrderStatus {
    Accepted,
    PartiallyFilled,
    Filled,
    Canceled,
    Rejected,
    Expired,
}

impl OrderStatus {
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Filled | Self::Canceled | Self::Rejected | Self::Expired
        )
    }
}
