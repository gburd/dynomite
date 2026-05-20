//! Token list parsing.
//!
//! `tokens:` and `dyn_seeds[*].tokens` are comma-separated big-int
//! strings. The C reference's `derive_tokens` accepts an optional
//! leading `-` per component and any number of decimal digits; the
//! actual ring math is in `hashkit::token` (Stage 3). At configuration
//! time we only need to validate the syntax and remember the original
//! components so they can be re-emitted.

use std::fmt;

use serde::de::{self, Deserializer, Visitor};
use serde::{Deserialize, Serialize};

use super::error::ConfError;

/// One element of a comma-separated token list.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct TokenComponent {
    /// `-1` for negative, `0` for zero, `1` for positive.
    pub signum: i8,
    /// Decimal digits without the optional leading sign.
    pub digits: String,
}

impl TokenComponent {
    /// Parse a single component (no commas, no surrounding whitespace).
    pub fn parse(raw: &str) -> Result<Self, ConfError> {
        if raw.is_empty() {
            return Err(ConfError::BadToken {
                value: raw.to_string(),
                reason: "empty token component".to_string(),
            });
        }
        let (signum, digits): (i8, &str) = if let Some(rest) = raw.strip_prefix('-') {
            if rest.is_empty() {
                return Err(ConfError::BadToken {
                    value: raw.to_string(),
                    reason: "lone minus sign".to_string(),
                });
            }
            (-1, rest)
        } else if raw == "0" {
            return Ok(Self {
                signum: 0,
                digits: "0".to_string(),
            });
        } else {
            (1, raw)
        };
        if !digits.bytes().all(|b| b.is_ascii_digit()) {
            return Err(ConfError::BadToken {
                value: raw.to_string(),
                reason: "non-digit character in token".to_string(),
            });
        }
        Ok(Self {
            signum,
            digits: digits.to_string(),
        })
    }
}

impl fmt::Display for TokenComponent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.signum < 0 {
            f.write_str("-")?;
        }
        f.write_str(&self.digits)
    }
}

/// A list of [`TokenComponent`]s parsed from a comma-separated string.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Default)]
pub struct TokenList {
    components: Vec<TokenComponent>,
    raw: String,
}

impl TokenList {
    /// Parse a comma-separated list. Leading or trailing whitespace
    /// inside each component is rejected to mirror the C parser.
    pub fn parse(raw: &str) -> Result<Self, ConfError> {
        if raw.is_empty() {
            return Err(ConfError::BadToken {
                value: raw.to_string(),
                reason: "empty token list".to_string(),
            });
        }
        let mut components = Vec::new();
        for piece in raw.split(',') {
            components.push(TokenComponent::parse(piece)?);
        }
        Ok(Self {
            components,
            raw: raw.to_string(),
        })
    }

    /// Borrow the parsed components.
    pub fn components(&self) -> &[TokenComponent] {
        &self.components
    }

    /// Number of components in the list.
    pub fn len(&self) -> usize {
        self.components.len()
    }

    /// Whether the list is empty (only constructible via the
    /// `Default` impl).
    pub fn is_empty(&self) -> bool {
        self.components.is_empty()
    }

    /// The original input string.
    pub fn raw(&self) -> &str {
        &self.raw
    }
}

impl fmt::Display for TokenList {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, c) in self.components.iter().enumerate() {
            if i > 0 {
                f.write_str(",")?;
            }
            c.fmt(f)?;
        }
        Ok(())
    }
}

impl Serialize for TokenList {
    fn serialize<S: serde::Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        ser.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for TokenList {
    fn deserialize<D: Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        struct V;
        impl Visitor<'_> for V {
            type Value = TokenList;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a comma-separated big-integer token list")
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                TokenList::parse(v).map_err(|e| E::custom(e.to_string()))
            }
            fn visit_string<E: de::Error>(self, v: String) -> Result<Self::Value, E> {
                self.visit_str(&v)
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
                self.visit_str(&v.to_string())
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
                self.visit_str(&v.to_string())
            }
        }
        de.deserialize_any(V)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_token() {
        let t = TokenList::parse("101134286").unwrap();
        assert_eq!(t.len(), 1);
        assert_eq!(t.components()[0].signum, 1);
        assert_eq!(t.components()[0].digits, "101134286");
    }

    #[test]
    fn comma_separated() {
        let t = TokenList::parse("0,1,2,4294967295").unwrap();
        assert_eq!(t.len(), 4);
        assert_eq!(t.to_string(), "0,1,2,4294967295");
    }

    #[test]
    fn negative_token() {
        let t = TokenList::parse("-7").unwrap();
        assert_eq!(t.components()[0].signum, -1);
        assert_eq!(t.components()[0].digits, "7");
        assert_eq!(t.to_string(), "-7");
    }

    #[test]
    fn zero_normalised() {
        let t = TokenList::parse("0").unwrap();
        assert_eq!(t.components()[0].signum, 0);
    }

    #[test]
    fn empty_rejected() {
        assert!(TokenList::parse("").is_err());
    }

    #[test]
    fn non_digit_rejected() {
        assert!(TokenList::parse("12a").is_err());
    }

    #[test]
    fn lone_minus_rejected() {
        assert!(TokenList::parse("-").is_err());
    }

    #[test]
    fn empty_component_rejected() {
        assert!(TokenList::parse("1,,2").is_err());
    }
}
