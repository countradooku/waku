//! Id allocation (RFC 0001 R6). `decide` takes ids from an [`IdSource`], so
//! tests can make every id deterministic.

use uuid::Uuid;

/// The fixed namespace for derived (v5) ids.
pub const WAKU_NAMESPACE: Uuid = Uuid::from_u128(0x5f0d_7a6e_2b3c_4d1e_9a8b_7c6d_5e4f_3a2b);

/// Where `decide` gets new ids.
pub trait IdSource: Send {
    /// A fresh id: UUIDv7 in production, a counter in tests.
    fn new_id(&mut self) -> Uuid;

    /// A stable id derived from inputs (UUIDv5 over [`WAKU_NAMESPACE`]), so a
    /// retried decision allocates the same id.
    fn derived(&self, scope: &str, key: &str) -> Uuid {
        derived_id(scope, key)
    }
}

/// UUIDv5 of `"<scope>:<key>"` in the Waku namespace.
pub fn derived_id(scope: &str, key: &str) -> Uuid {
    let mut name = String::with_capacity(scope.len() + key.len() + 1);
    name.push_str(scope);
    name.push(':');
    name.push_str(key);
    Uuid::new_v5(&WAKU_NAMESPACE, name.as_bytes())
}

/// Time-ordered UUIDv7 ids, for production.
#[derive(Clone, Copy, Debug, Default)]
pub struct UuidV7Ids;

impl IdSource for UuidV7Ids {
    fn new_id(&mut self) -> Uuid {
        Uuid::now_v7()
    }
}

/// Deterministic ids `prefix:counter`, for tests.
#[derive(Clone, Debug)]
pub struct SequentialIds {
    prefix: u64,
    next: u64,
}

impl SequentialIds {
    pub fn new(prefix: u64) -> Self {
        Self { prefix, next: 1 }
    }
}

impl Default for SequentialIds {
    fn default() -> Self {
        Self::new(0x7e57)
    }
}

impl IdSource for SequentialIds {
    fn new_id(&mut self) -> Uuid {
        let id = Uuid::from_u128(((self.prefix as u128) << 64) | self.next as u128);
        self.next += 1;
        id
    }
}
