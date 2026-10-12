//! Pre-serialized arguments for the Nix bootstrap entry point.
//!
//! The framework owns the schema (today: [`crate::nix_args::NixArgs<'a>`]),
//! serializes it once via [`crate::nix_args::NixArgs::bootstrap_args`], and
//! shares the resulting value with the backend. Backends never see
//! [`crate::nix_args::NixArgs`] — the payload is opaque Nix code by the
//! time it crosses the seam.

use miette::Result;
use std::sync::Arc;

/// Pre-serialized argument attrset for `import bootstrap/default.nix <args>`.
///
/// Cheaply cloneable: the underlying serialized string is shared via
/// reference counting, so callers that need their own handle can `.clone()`
/// without duplicating the payload.
#[derive(Clone)]
pub struct BootstrapArgs {
    serialized: Arc<str>,
    cache_key_seed: Arc<str>,
}

impl BootstrapArgs {
    /// Serialize any [`serde::Serialize`] value via `ser_nix`.
    pub fn from_serializable<T: serde::Serialize>(value: &T) -> Result<Self> {
        let serialized: Arc<str> = Arc::from(serialize(value)?);
        Ok(Self {
            cache_key_seed: serialized.clone(),
            serialized,
        })
    }

    /// Like [`Self::from_serializable`], but the eval cache keys on
    /// `cache_key_value` rather than on the payload.
    pub fn with_cache_key_seed<T: serde::Serialize, K: serde::Serialize>(
        value: &T,
        cache_key_value: &K,
    ) -> Result<Self> {
        Ok(Self {
            serialized: Arc::from(serialize(value)?),
            cache_key_seed: Arc::from(serialize(cache_key_value)?),
        })
    }

    /// Borrow the serialized Nix expression: the payload spliced into the
    /// bootstrap import call.
    pub fn as_str(&self) -> &str {
        &self.serialized
    }

    /// What the eval cache keys on in place of the payload.
    pub fn cache_key_seed(&self) -> &str {
        &self.cache_key_seed
    }
}

fn serialize<T: serde::Serialize>(value: &T) -> Result<String> {
    ser_nix::to_string(value)
        .map_err(|e| miette::miette!("Failed to serialize bootstrap args: {}", e))
}

impl AsRef<str> for BootstrapArgs {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    #[derive(Serialize)]
    struct TinyArgs<'a> {
        version: &'a str,
        system: &'a str,
    }

    #[test]
    fn from_serializable_round_trips_a_simple_attrset() {
        let args = TinyArgs {
            version: "1.0.0",
            system: "x86_64-linux",
        };
        let b = BootstrapArgs::from_serializable(&args).expect("serialize");
        assert!(b.as_str().contains("\"1.0.0\""));
        assert!(b.as_str().contains("\"x86_64-linux\""));
    }

    #[test]
    fn clone_shares_the_underlying_payload() {
        let args = TinyArgs {
            version: "1.0.0",
            system: "x86_64-linux",
        };
        let a = BootstrapArgs::from_serializable(&args).expect("serialize");
        let b = a.clone();
        assert_eq!(a.as_str().as_ptr(), b.as_str().as_ptr());
    }
}
