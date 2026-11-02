//! Token parsing.
//!
//! `tokens:` and the token field of each `dyn_seeds` entry name the one
//! ring token a node owns: a decimal integer in `0..=4294967295`.
//!
//! A comma list of several tokens is rejected. One token per node is the
//! supported contract; a multi-token list never worked (it overran the
//! continuum and crashed the node).
//!
//! Negative tokens are rejected because they sort above every positive
//! token, the reverse of their numeric value. Tokens above `u32::MAX` are
//! rejected because they would silently wrap to a different ring
//! position. The type keeps a list shape (`components`) so the wire and
//! display forms are unchanged.

use std::fmt;

use serde::de::{self, Deserializer, Visitor};
use serde::{Deserialize, Serialize};

use super::error::ConfError;

/// One element of a comma-separated token list.
///
/// # Examples
///
/// ```
/// use dynomite::conf::TokenComponent;
/// let pos = TokenComponent::parse("42").unwrap();
/// assert_eq!(pos.signum, 1);
/// assert_eq!(pos.digits, "42");
/// let zero = TokenComponent::parse("0").unwrap();
/// assert_eq!(zero.signum, 0);
/// ```
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct TokenComponent {
    /// `-1` for negative, `0` for zero, `1` for positive.
    pub signum: i8,
    /// Decimal digits without the optional leading sign.
    pub digits: String,
}

impl TokenComponent {
    /// Parse a single component (no commas, no surrounding whitespace).
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::TokenComponent;
    /// let neg = TokenComponent::parse("-7").unwrap();
    /// assert_eq!(neg.signum, -1);
    /// assert_eq!(neg.digits, "7");
    /// assert!(TokenComponent::parse("").is_err());
    /// assert!(TokenComponent::parse("-").is_err());
    /// assert!(TokenComponent::parse("12a").is_err());
    /// ```
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
///
/// # Examples
///
/// ```
/// use dynomite::conf::TokenList;
/// let t = TokenList::parse("101134286").unwrap();
/// assert_eq!(t.len(), 1);
/// assert_eq!(t.to_string(), "101134286");
/// ```
#[derive(Debug, Clone, Eq, PartialEq, Hash, Default)]
pub struct TokenList {
    components: Vec<TokenComponent>,
    raw: String,
}

impl TokenList {
    /// Parse a node's token: exactly one decimal integer in
    /// `0..=u32::MAX`, with no sign, no whitespace, and no comma list.
    ///
    /// # Errors
    ///
    /// [`ConfError::BadToken`] for an empty string, a comma list of more
    /// than one token, a negative token, a non-digit character, or a
    /// value above `u32::MAX`.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::TokenList;
    /// assert_eq!(TokenList::parse("4294967295").unwrap().value(), u32::MAX);
    /// assert!(TokenList::parse("").is_err());
    /// assert!(TokenList::parse("1,2").is_err());
    /// assert!(TokenList::parse("-7").is_err());
    /// assert!(TokenList::parse("4294967296").is_err());
    /// ```
    pub fn parse(raw: &str) -> Result<Self, ConfError> {
        let bad = |reason: &str| ConfError::BadToken {
            value: raw.to_string(),
            reason: reason.to_string(),
        };
        if raw.is_empty() {
            return Err(bad("empty token"));
        }
        if raw.contains(',') {
            return Err(bad(
                "only one token per node is supported; a comma list of tokens is rejected",
            ));
        }
        let component = TokenComponent::parse(raw)?;
        if component.signum < 0 {
            return Err(bad("negative tokens are not supported; use 0..=4294967295"));
        }
        if component.digits.parse::<u32>().is_err() {
            return Err(bad("token exceeds the 32-bit ring (0..=4294967295)"));
        }
        Ok(Self {
            components: vec![component],
            raw: raw.to_string(),
        })
    }

    /// The token value. Only meaningful on a parsed list, which holds
    /// exactly one in-range token; `0` for the empty `Default` value.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::TokenList;
    /// assert_eq!(TokenList::parse("101134286").unwrap().value(), 101_134_286);
    /// assert_eq!(TokenList::default().value(), 0);
    /// ```
    #[must_use]
    pub fn value(&self) -> u32 {
        self.components
            .first()
            .and_then(|c| c.digits.parse::<u32>().ok())
            .unwrap_or(0)
    }

    /// Borrow the parsed components.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::TokenList;
    /// let t = TokenList::parse("7").unwrap();
    /// assert_eq!(t.components().len(), 1);
    /// ```
    pub fn components(&self) -> &[TokenComponent] {
        &self.components
    }

    /// Number of components in the list.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::TokenList;
    /// assert_eq!(TokenList::parse("5").unwrap().len(), 1);
    /// ```
    pub fn len(&self) -> usize {
        self.components.len()
    }

    /// Whether the list is empty (only constructible via the
    /// `Default` impl).
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::TokenList;
    /// assert!(TokenList::default().is_empty());
    /// assert!(!TokenList::parse("1").unwrap().is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.components.is_empty()
    }

    /// The original input string.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::TokenList;
    /// assert_eq!(TokenList::parse("12").unwrap().raw(), "12");
    /// ```
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
                f.write_str("a single ring token in 0..=4294967295")
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
    fn regression_multi_token_list_rejected() {
        // DEV-H4: a multi-token list overran the C continuum and crashed
        // the node; it is rejected at parse time.
        for raw in [
            "0,1",
            "0,1,2,4294967295",
            "101,2147483648,3000000000",
            "7,7",
        ] {
            assert!(TokenList::parse(raw).is_err(), "{raw}");
        }
    }

    #[test]
    fn regression_negative_token_rejected() {
        // A negative token sorts above every positive one.
        assert!(TokenList::parse("-7").is_err());
        assert!(TokenList::parse("-0").is_err());
    }

    #[test]
    fn regression_out_of_range_token_rejected() {
        assert_eq!(TokenList::parse("4294967295").unwrap().value(), u32::MAX);
        assert!(TokenList::parse("4294967296").is_err());
        assert!(TokenList::parse("99999999999999999999").is_err());
    }

    #[test]
    fn leading_zeros_parse_to_their_value() {
        assert_eq!(TokenList::parse("007").unwrap().value(), 7);
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
        assert!(TokenList::parse(",").is_err());
    }
}
