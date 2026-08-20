use super::GridLevel;

/// A monotonically increasing version of the desired grid.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct GridRevision(u64);

impl GridRevision {
    pub const ZERO: Self = Self(0);

    pub const fn get(self) -> u64 {
        self.0
    }

    pub(crate) fn checked_next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

/// A local purpose change for an existing same-side, same-price order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GridReassignment {
    previous: GridLevel,
    current: GridLevel,
}

impl GridReassignment {
    pub(crate) const fn new(previous: GridLevel, current: GridLevel) -> Self {
        Self { previous, current }
    }

    pub const fn previous(self) -> GridLevel {
        self.previous
    }

    pub const fn current(self) -> GridLevel {
        self.current
    }
}

/// The exact local and exchange-facing work implied by one successful fill.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GridTransition {
    revision: GridRevision,
    consumed: GridLevel,
    cancel: Option<GridLevel>,
    placements: [Option<GridLevel>; 2],
    reassignment: Option<GridReassignment>,
}

impl GridTransition {
    pub(crate) const fn new(
        revision: GridRevision,
        consumed: GridLevel,
        cancel: Option<GridLevel>,
        placements: [Option<GridLevel>; 2],
        reassignment: Option<GridReassignment>,
    ) -> Self {
        Self {
            revision,
            consumed,
            cancel,
            placements,
            reassignment,
        }
    }

    pub const fn revision(self) -> GridRevision {
        self.revision
    }

    pub const fn consumed(self) -> GridLevel {
        self.consumed
    }

    pub const fn cancel(self) -> Option<GridLevel> {
        self.cancel
    }

    pub fn placements(self) -> impl Iterator<Item = GridLevel> {
        self.placements.into_iter().flatten()
    }

    pub const fn reassignment(self) -> Option<GridReassignment> {
        self.reassignment
    }
}
