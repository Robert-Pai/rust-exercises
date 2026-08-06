use std::{fmt, num::NonZeroU64};

use super::ValueError;

/// A positive order quantity expressed in instrument quantity steps.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct QuantityLots(NonZeroU64);

impl QuantityLots {
    pub fn new(value: u64) -> Result<Self, ValueError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(ValueError::ZeroQuantity)
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

impl fmt::Display for QuantityLots {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.get().fmt(formatter)
    }
}

/// A cumulative filled quantity, which may be zero.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FilledLots(u64);

impl FilledLots {
    pub const ZERO: Self = Self(0);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}
