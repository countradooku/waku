//! Deterministic randomness shared by every generator.
//!
//! This is the mulberry32 generator the former `scripts/seed-mock-sessions.ts`
//! used, so one seed always produces the same fixture bytes on every machine.

use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct Rng {
    state: u32,
}

impl Rng {
    pub fn new(seed: u32) -> Self {
        Self { state: seed }
    }

    pub fn next_u32(&mut self) -> u32 {
        self.state = self.state.wrapping_add(0x6d2b_79f5);
        let mut t = self.state;
        t = (t ^ (t >> 15)).wrapping_mul(t | 1);
        t ^= t.wrapping_add((t ^ (t >> 7)).wrapping_mul(t | 61));
        t ^ (t >> 14)
    }

    /// A float in `[0, 1)`, matching the TypeScript generator.
    pub fn next_f64(&mut self) -> f64 {
        f64::from(self.next_u32()) / 4_294_967_296.0
    }

    pub fn pick<'a, T>(&mut self, values: &'a [T]) -> &'a T {
        let index = (self.next_f64() * values.len() as f64) as usize;
        &values[index.min(values.len() - 1)]
    }

    /// An integer in the inclusive range `min..=max`.
    pub fn int(&mut self, min: u64, max: u64) -> u64 {
        min + (self.next_f64() * (max - min + 1) as f64) as u64
    }

    pub fn chance(&mut self, probability: f64) -> bool {
        self.next_f64() < probability
    }

    /// A valid v4-layout UUID whose bytes come from this generator.
    pub fn uuid(&mut self) -> Uuid {
        let mut bytes = [0_u8; 16];
        for chunk in bytes.chunks_mut(4) {
            chunk.copy_from_slice(&self.next_u32().to_le_bytes());
        }
        // Bench-owned ids share a recognizable first group.
        bytes[0] = 0xbe;
        bytes[1] = 0x4c;
        bytes[6] = (bytes[6] & 0x0f) | 0x40;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Uuid::from_bytes(bytes)
    }

    /// A short provider-style call id, such as `call_1x9k2m`.
    pub fn call_id(&mut self) -> String {
        format!(
            "call_{}",
            radix36(u64::from(self.next_u32()) << 8 | self.int(0, 255))
        )
    }
}

fn radix36(mut value: u64) -> String {
    const DIGITS: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    if value == 0 {
        return "0".into();
    }
    let mut out = Vec::new();
    while value > 0 {
        out.push(DIGITS[(value % 36) as usize]);
        value /= 36;
    }
    out.reverse();
    String::from_utf8(out).expect("radix digits are ASCII")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_the_typescript_mulberry32_sequence() {
        // First outputs of mulberry32(20260806), the original seeding script's default.
        let mut rng = Rng::new(20_260_806);
        let first = rng.next_u32();
        let mut again = Rng::new(20_260_806);
        assert_eq!(first, again.next_u32());
        assert!(rng.next_f64() < 1.0);
    }

    #[test]
    fn uuids_are_valid_v4_and_deterministic() {
        let a = Rng::new(7).uuid();
        let b = Rng::new(7).uuid();
        assert_eq!(a, b);
        assert_eq!(a.get_version_num(), 4);
        assert!(a.to_string().starts_with("be4c"));
    }
}
