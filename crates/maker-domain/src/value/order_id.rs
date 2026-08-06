use std::fmt;

use super::ValueError;

macro_rules! numeric_identifier {
    ($name:ident, $kind:literal) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(u64);

        impl $name {
            pub const fn new(value: u64) -> Result<Self, ValueError> {
                if value == 0 {
                    return Err(ValueError::ZeroIdentifier { kind: $kind });
                }
                Ok(Self(value))
            }

            pub const fn get(self) -> u64 {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

numeric_identifier!(ClientOrderId, "client order ID");
numeric_identifier!(ExchangeOrderId, "exchange order ID");

#[cfg(test)]
mod tests {
    use std::mem::size_of;

    use super::*;

    #[test]
    fn identifiers_are_compact_numeric_values() {
        assert_eq!(size_of::<ClientOrderId>(), 8);
        assert_eq!(size_of::<ExchangeOrderId>(), 8);
        assert_eq!(ClientOrderId::new(42).unwrap().to_string(), "42");
        assert_eq!(ExchangeOrderId::new(7).unwrap().get(), 7);
    }

    #[test]
    fn rejects_zero_identifiers() {
        assert!(ClientOrderId::new(0).is_err());
        assert!(ExchangeOrderId::new(0).is_err());
    }
}
