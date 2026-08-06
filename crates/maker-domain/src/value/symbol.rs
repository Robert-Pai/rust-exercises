use std::{fmt, str::FromStr};

use super::ValueError;

pub const MAX_SYMBOL_LEN: usize = 16;

/// An exchange-neutral ASCII instrument symbol stored entirely inline.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Symbol {
    bytes: [u8; MAX_SYMBOL_LEN],
}

impl Symbol {
    pub fn new(value: impl AsRef<str>) -> Result<Self, ValueError> {
        let value = value.as_ref();
        if value.is_empty() {
            return Err(ValueError::EmptySymbol);
        }
        if !value.is_ascii() || value.as_bytes().contains(&0) {
            return Err(ValueError::NonAsciiSymbol);
        }
        if value.trim() != value {
            return Err(ValueError::SymbolWhitespace);
        }
        if value.len() > MAX_SYMBOL_LEN {
            return Err(ValueError::SymbolTooLong {
                length: value.len(),
                maximum: MAX_SYMBOL_LEN,
            });
        }

        let mut bytes = [0; MAX_SYMBOL_LEN];
        bytes[..value.len()].copy_from_slice(value.as_bytes());
        Ok(Self { bytes })
    }

    pub fn as_str(&self) -> &str {
        let len = self
            .bytes
            .iter()
            .position(|byte| *byte == 0)
            .unwrap_or(MAX_SYMBOL_LEN);
        std::str::from_utf8(&self.bytes[..len]).expect("Symbol construction guarantees ASCII bytes")
    }
}

impl fmt::Display for Symbol {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
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
    use std::mem::size_of;

    use super::*;

    #[test]
    fn stores_symbols_inline() {
        let symbol = Symbol::new("BTCUSDT").unwrap();
        assert_eq!(symbol.as_str(), "BTCUSDT");
        assert_eq!(size_of::<Symbol>(), 16);
    }

    #[test]
    fn rejects_invalid_symbols() {
        assert_eq!(Symbol::new(""), Err(ValueError::EmptySymbol));
        assert_eq!(Symbol::new(" BTCUSDT"), Err(ValueError::SymbolWhitespace));
        assert_eq!(Symbol::new("比特币"), Err(ValueError::NonAsciiSymbol));
        assert!(matches!(
            Symbol::new("ABCDEFGHIJKLMNOPQ"),
            Err(ValueError::SymbolTooLong { .. })
        ));
    }
}
