use std::fmt;

use super::ValueError;

macro_rules! identifier {
    ($name:ident, $kind:literal) => {
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, ValueError> {
                let value = value.into();
                if value.is_empty() {
                    return Err(ValueError::EmptyIdentifier { kind: $kind });
                }
                if value.trim() != value {
                    return Err(ValueError::IdentifierWhitespace { kind: $kind });
                }
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }
    };
}

identifier!(ClientOrderId, "client order ID");
identifier!(ExchangeOrderId, "exchange order ID");
