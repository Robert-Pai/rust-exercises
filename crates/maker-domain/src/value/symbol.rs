use std::{fmt, str::FromStr};

use super::ValueError;

/// An opaque exchange-neutral instrument symbol.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Symbol(String);

impl Symbol {
    pub fn new(value: impl Into<String>) -> Result<Self, ValueError> {
        let value = value.into();
        if value.is_empty() {
            return Err(ValueError::EmptySymbol);
        }
        if value.trim() != value {
            return Err(ValueError::SymbolWhitespace(value));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Symbol {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for Symbol {
    type Err = ValueError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty_and_padded_symbols() {
        assert_eq!(Symbol::new(""), Err(ValueError::EmptySymbol));
        assert!(matches!(
            Symbol::new(" BTCUSDT"),
            Err(ValueError::SymbolWhitespace(_))
        ));
    }
}
