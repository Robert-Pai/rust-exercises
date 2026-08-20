use std::{fmt, num::NonZeroU64};

use super::ValueError;

/// A positive price expressed as an integer number of instrument ticks.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PriceTicks(NonZeroU64);

impl PriceTicks {
    pub fn new(value: u64) -> Result<Self, ValueError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(ValueError::ZeroPrice)
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }

    pub(crate) fn checked_add(self, offset: u64) -> Option<Self> {
        self.get()
            .checked_add(offset)
            .and_then(NonZeroU64::new)
            .map(Self)
    }

    pub(crate) fn checked_sub(self, offset: u64) -> Option<Self> {
        self.get()
            .checked_sub(offset)
            .and_then(NonZeroU64::new)
            .map(Self)
    }
}

impl fmt::Display for PriceTicks {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.get().fmt(formatter)
    }
}

/// A tick distance that may be zero.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TickCount(u64);

impl TickCount {
    pub const ZERO: Self = Self(0);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

/// A strictly positive tick distance.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NonZeroTickCount(NonZeroU64);

impl NonZeroTickCount {
    pub fn new(value: u64) -> Result<Self, ValueError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(ValueError::ZeroTickCount)
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

impl From<NonZeroTickCount> for TickCount {
    fn from(value: NonZeroTickCount) -> Self {
        Self(value.get())
    }
}
