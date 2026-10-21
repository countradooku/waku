//! Memory bounds for per-session desktop state (#24).
//!
//! Three things grew with every session a window visited: hydrated
//! transcripts, parsed Markdown and decoded images. Each now has a bound:
//!
//! - [`ByteBudgetLru`] keeps parsed Markdown for the most recently drawn
//!   messages and reasoning blocks up to a source-byte budget, instead of
//!   clearing the whole cache on a session switch once it is too large;
//! - [`ImageCache`] keeps decoded images up to a decoded-byte budget and
//!   hands evicted images back so GPUI's asset cache and sprite atlas can drop
//!   them too;
//! - transcripts are bounded by the projection store, which keeps at most a
//!   handful of threads open and drops the rest (#55).

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Source bytes of parsed message Markdown kept across frames and sessions.
///
/// Measured at ~17x expansion into parsed structures, plus flattened text and
/// shaped runs on top, so this is bounded by source size rather than entry
/// count — one long message costs far more than several short ones.
pub(super) const MESSAGE_MARKDOWN_BUDGET: usize = 512 * 1024;
/// Source bytes of parsed reasoning Markdown, keyed by (session, activity).
pub(super) const ACTIVITY_MARKDOWN_BUDGET: usize = 256 * 1024;
/// Entries a Markdown cache never trims below, whatever their size: the most
/// recently drawn rows, which include everything on screen, so a budget
/// smaller than one frame's rows never makes a visible row re-parse.
pub(super) const MARKDOWN_MIN_ENTRIES: usize = 48;
/// A cache insert re-checks the budget once per this many inserts, so a fast
/// scroll through new rows does not sum the cache on every row.
const TRIM_EVERY_INSERTS: u32 = 16;

/// Decoded (RGBA) bytes of daemon-owned and inline images kept in memory.
pub(super) const IMAGE_DECODED_BUDGET: usize = 96 * 1024 * 1024;
/// Images never evicted for size: the ones most recently drawn.
const IMAGE_MIN_ENTRIES: usize = 4;
/// Upper bound on entries of any kind, including failures and in-flight
/// fetches, which cost no decoded bytes but still occupy the map.
const IMAGE_MAX_ENTRIES: usize = 512;

/// A map that evicts its least recently used entries once their combined
/// size exceeds a budget. Sizes are measured at trim time, because a cached
/// value (a streaming Markdown view) keeps growing after insertion.
pub(super) struct ByteBudgetLru<K, V> {
    entries: HashMap<K, LruEntry<V>>,
    clock: u64,
    budget: usize,
    min_entries: usize,
    size_of: fn(&V) -> usize,
    inserts_since_trim: u32,
}

struct LruEntry<V> {
    value: V,
    last_used: u64,
}

impl<K: Clone + Eq + Hash, V> ByteBudgetLru<K, V> {
    pub(super) fn new(budget: usize, min_entries: usize, size_of: fn(&V) -> usize) -> Self {
        Self {
            entries: HashMap::new(),
            clock: 0,
            budget,
            min_entries,
            size_of,
            inserts_since_trim: 0,
        }
    }

    fn tick(&mut self) -> u64 {
        self.clock += 1;
        self.clock
    }

    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(super) fn contains_key(&self, key: &K) -> bool {
        self.entries.contains_key(key)
    }

    pub(super) fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        let now = self.tick();
        let entry = self.entries.get_mut(key)?;
        entry.last_used = now;
        Some(&mut entry.value)
    }

    /// The entry for `key`, created with `create` on a miss. A miss may first
    /// trim the cache, never evicting `key` itself.
    pub(super) fn get_or_insert_with(&mut self, key: K, create: impl FnOnce() -> V) -> &mut V {
        let now = self.tick();
        if !self.entries.contains_key(&key) {
            self.inserts_since_trim += 1;
            if self.inserts_since_trim >= TRIM_EVERY_INSERTS {
                self.trim();
            }
        }
        let entry = self.entries.entry(key).or_insert_with(|| LruEntry {
            value: create(),
            last_used: now,
        });
        entry.last_used = now;
        &mut entry.value
    }

    pub(super) fn insert(&mut self, key: K, value: V) {
        let now = self.tick();
        self.entries.insert(
            key,
            LruEntry {
                value,
                last_used: now,
            },
        );
    }

    pub(super) fn remove(&mut self, key: &K) -> Option<V> {
        self.entries.remove(key).map(|entry| entry.value)
    }

    /// The combined size of every entry.
    pub(super) fn bytes(&self) -> usize {
        self.entries
            .values()
            .map(|entry| (self.size_of)(&entry.value))
            .sum()
    }

    /// Evicts least recently used entries until the cache fits its budget or
    /// only `min_entries` remain. Returns the evicted values.
    pub(super) fn trim(&mut self) -> Vec<V> {
        self.inserts_since_trim = 0;
        let mut total = self.bytes();
        if total <= self.budget || self.entries.len() <= self.min_entries {
            return Vec::new();
        }
        let mut by_age = self
            .entries
            .iter()
            .map(|(key, entry)| (entry.last_used, key.clone()))
            .collect::<Vec<_>>();
        by_age.sort_unstable_by_key(|(last_used, _)| *last_used);
        let evictable = by_age.len().saturating_sub(self.min_entries);
        let mut evicted = Vec::new();
        for (_, key) in by_age.into_iter().take(evictable) {
            if total <= self.budget {
                break;
            }
            if let Some(entry) = self.entries.remove(&key) {
                total = total.saturating_sub((self.size_of)(&entry.value));
                evicted.push(entry.value);
            }
        }
        evicted
    }
}

/// One cached image.
#[derive(Clone, Debug)]
pub(super) enum RemoteImageState {
    Loading,
    Ready {
        image: Arc<gpui::Image>,
        /// Estimated RGBA size once decoded; what the budget counts.
        decoded_bytes: usize,
    },
    Unavailable,
}

impl RemoteImageState {
    pub(super) fn ready(image: Arc<gpui::Image>) -> Self {
        let decoded_bytes = estimated_decoded_bytes(&image.bytes);
        Self::Ready {
            image,
            decoded_bytes,
        }
    }

    fn decoded_bytes(&self) -> usize {
        match self {
            Self::Ready { decoded_bytes, .. } => *decoded_bytes,
            Self::Loading | Self::Unavailable => 0,
        }
    }
}

/// Decoded images bounded by [`IMAGE_DECODED_BUDGET`] and
/// [`IMAGE_MAX_ENTRIES`], keyed by [`ImageCache::reference_key`] or
/// [`ImageCache::data_url_key`].
pub(super) struct ImageCache {
    entries: ByteBudgetLru<u64, RemoteImageState>,
}

impl Default for ImageCache {
    fn default() -> Self {
        Self::with_budget(IMAGE_DECODED_BUDGET)
    }
}

impl ImageCache {
    pub(super) fn with_budget(budget: usize) -> Self {
        Self {
            entries: ByteBudgetLru::new(budget, IMAGE_MIN_ENTRIES, RemoteImageState::decoded_bytes),
        }
    }

    pub(super) fn reference_key(reference: &str) -> u64 {
        let mut hasher = fixed_hasher();
        0_u8.hash(&mut hasher);
        reference.hash(&mut hasher);
        hasher.finish()
    }

    /// Key for an inline `data:` URL. Those can be megabytes and the key is
    /// computed for every visible row on every frame, so it hashes the length
    /// and a sample (both ends plus a stride through the middle) instead of
    /// every byte. Two different base64 payloads of one length that agree at
    /// every sampled byte do not occur in practice.
    pub(super) fn data_url_key(url: &str) -> u64 {
        const EDGE: usize = 256;
        const STRIDE: usize = 4096;
        let bytes = url.as_bytes();
        let mut hasher = fixed_hasher();
        1_u8.hash(&mut hasher);
        bytes.len().hash(&mut hasher);
        if bytes.len() <= 2 * EDGE {
            bytes.hash(&mut hasher);
        } else {
            bytes[..EDGE].hash(&mut hasher);
            bytes[bytes.len() - EDGE..].hash(&mut hasher);
            for byte in bytes[EDGE..bytes.len() - EDGE].iter().step_by(STRIDE) {
                byte.hash(&mut hasher);
            }
        }
        hasher.finish()
    }

    /// The cached state, counting as a use.
    pub(super) fn get(&mut self, key: u64) -> Option<&RemoteImageState> {
        self.entries.get_mut(&key).map(|state| &*state)
    }

    pub(super) fn contains(&self, key: u64) -> bool {
        self.entries.contains_key(&key)
    }

    /// Stores `state` and returns the images evicted to make room. Their GPUI
    /// asset-cache entries and atlas tiles must be released by the caller
    /// (see `Waku::retire_images`).
    #[must_use]
    pub(super) fn insert(&mut self, key: u64, state: RemoteImageState) -> Vec<Arc<gpui::Image>> {
        self.entries.insert(key, state);
        let mut evicted = self.entries.trim();
        if self.entries.len() > IMAGE_MAX_ENTRIES {
            // Over the count bound with nothing large left to evict: drop the
            // oldest placeholders and failures, which a visible row refetches.
            let excess = self.entries.len() - IMAGE_MAX_ENTRIES;
            let mut stale = self
                .entries
                .entries
                .iter()
                .map(|(key, entry)| (entry.last_used, *key))
                .collect::<Vec<_>>();
            stale.sort_unstable_by_key(|(last_used, _)| *last_used);
            for (_, key) in stale.into_iter().take(excess) {
                if let Some(state) = self.entries.remove(&key) {
                    evicted.push(state);
                }
            }
        }
        evicted
            .into_iter()
            .filter_map(|state| match state {
                RemoteImageState::Ready { image, .. } => Some(image),
                RemoteImageState::Loading | RemoteImageState::Unavailable => None,
            })
            .collect()
    }

    #[cfg(test)]
    fn decoded_bytes(&self) -> usize {
        self.entries.bytes()
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.len()
    }
}

fn fixed_hasher() -> std::hash::DefaultHasher {
    // Keys only live in memory; fixed keys keep them reproducible in tests.
    std::hash::DefaultHasher::new()
}

/// The RGBA size `bytes` decodes to, from the image header alone. Falls back
/// to a conservative multiple of the encoded size when the header cannot be
/// read.
pub(super) fn estimated_decoded_bytes(bytes: &[u8]) -> usize {
    if let Ok(size) = imagesize::blob_size(bytes) {
        return size.width.saturating_mul(size.height).saturating_mul(4);
    }
    bytes.len().saturating_mul(8)
}

/// Decodes an inline `data:` image URL, as older transcripts stored tool
/// images before the daemon externalized them to blobs.
pub(super) fn decode_data_url_image(url: &str) -> Option<Arc<gpui::Image>> {
    use base64::Engine as _;
    let (header, encoded) = url.split_once(',')?;
    let mime_type = header.strip_prefix("data:")?.split(';').next()?;
    let format = gpui::ImageFormat::from_mime_type(mime_type)?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .ok()?;
    (!bytes.is_empty()).then(|| Arc::new(gpui::Image::from_bytes(format, bytes)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn string_lru(budget: usize, min_entries: usize) -> ByteBudgetLru<u32, String> {
        ByteBudgetLru::new(budget, min_entries, String::len)
    }

    #[test]
    fn lru_trims_least_recently_used_entries_to_the_byte_budget() {
        let mut cache = string_lru(10, 0);
        cache.insert(1, "aaaa".into());
        cache.insert(2, "bbbb".into());
        cache.insert(3, "cccc".into());
        // Using 1 makes 2 the oldest.
        cache.get_mut(&1);

        let evicted = cache.trim();

        assert_eq!(evicted, vec!["bbbb".to_owned()]);
        assert!(cache.contains_key(&1) && cache.contains_key(&3));
        assert_eq!(cache.bytes(), 8);
        assert!(cache.trim().is_empty(), "a fitting cache is left alone");
    }

    #[test]
    fn lru_measures_entries_that_grew_after_insertion() {
        let mut cache = string_lru(10, 0);
        cache.insert(1, String::new());
        cache.insert(2, String::new());
        // A streaming view grows in place after it was cached.
        cache.get_mut(&1).unwrap().push_str("0123456789");
        cache.get_mut(&2).unwrap().push_str("0123");

        cache.trim();

        assert!(!cache.contains_key(&1), "the grown, older entry goes");
        assert!(cache.contains_key(&2));
    }

    #[test]
    fn lru_keeps_its_most_recent_entries_even_over_budget() {
        let mut cache = string_lru(1, 2);
        for key in 0..5 {
            cache.insert(key, "large".into());
        }

        cache.trim();

        assert_eq!(cache.len(), 2);
        assert!(cache.contains_key(&3) && cache.contains_key(&4));
    }

    #[test]
    fn lru_trims_on_insert_without_evicting_the_new_key() {
        let mut cache = string_lru(4, 0);
        for key in 0..TRIM_EVERY_INSERTS {
            cache.get_or_insert_with(key, || "xxxx".into());
        }
        // The trim ran before the last insert: only that entry survived the
        // budget, plus the new one.
        assert!(cache.contains_key(&(TRIM_EVERY_INSERTS - 1)));
        assert!(cache.len() <= 2, "{} entries after trimming", cache.len());
    }

    fn png(width: u32, height: u32) -> Arc<gpui::Image> {
        let mut encoded = Vec::new();
        image::RgbaImage::new(width, height)
            .write_to(
                &mut std::io::Cursor::new(&mut encoded),
                image::ImageFormat::Png,
            )
            .unwrap();
        Arc::new(gpui::Image::from_bytes(gpui::ImageFormat::Png, encoded))
    }

    #[test]
    fn image_cache_counts_decoded_bytes_and_returns_evicted_images() {
        let image = png(64, 32);
        assert_eq!(estimated_decoded_bytes(&image.bytes), 64 * 32 * 4);

        let budget = 64 * 32 * 4 * (IMAGE_MIN_ENTRIES + 1);
        let mut cache = ImageCache::with_budget(budget);
        let keys = (0..IMAGE_MIN_ENTRIES as u64 + 3).collect::<Vec<_>>();
        let mut evicted = Vec::new();
        for key in &keys {
            evicted.extend(cache.insert(*key, RemoteImageState::ready(png(64, 32))));
        }

        assert!(cache.decoded_bytes() <= budget);
        assert_eq!(evicted.len(), 2, "the two oldest images are handed back");
        assert!(!cache.contains(keys[0]) && !cache.contains(keys[1]));
        assert!(cache.contains(*keys.last().unwrap()));
    }

    #[test]
    fn image_cache_bounds_placeholder_entries_by_count() {
        let mut cache = ImageCache::default();
        for key in 0..(IMAGE_MAX_ENTRIES as u64 + 10) {
            assert!(cache.insert(key, RemoteImageState::Unavailable).is_empty());
        }
        assert_eq!(cache.len(), IMAGE_MAX_ENTRIES);
        assert!(!cache.contains(0));
    }

    #[test]
    fn data_url_keys_are_stable_and_distinguish_content() {
        let small = "data:image/png;base64,AAAA";
        assert_eq!(
            ImageCache::data_url_key(small),
            ImageCache::data_url_key(small)
        );
        let large = format!("data:image/png;base64,{}", "A".repeat(100_000));
        assert_ne!(
            ImageCache::data_url_key(&large),
            ImageCache::data_url_key(&format!("{large}A")),
            "the length is part of the key"
        );
        for position in [40, large.len() / 2 + 7, large.len() - 3] {
            let mut changed = large.clone();
            changed.replace_range(position..=position, "B");
            let sampled = position < 256
                || position >= large.len() - 256
                || (position - 256).is_multiple_of(4096);
            assert_eq!(
                ImageCache::data_url_key(&large) != ImageCache::data_url_key(&changed),
                sampled,
                "a change at {position} is detected exactly when it is sampled"
            );
        }
        assert_ne!(
            ImageCache::data_url_key("blob:x"),
            ImageCache::reference_key("blob:x")
        );
    }

    #[test]
    fn data_url_images_decode() {
        use base64::Engine as _;
        let image = png(2, 2);
        let url = format!(
            "data:image/png;base64,{}",
            base64::engine::general_purpose::STANDARD.encode(&image.bytes)
        );
        let decoded = decode_data_url_image(&url).unwrap();
        assert_eq!(decoded.bytes, image.bytes);
        assert!(decode_data_url_image("data:image/png;base64,").is_none());
        assert!(decode_data_url_image("https://example.com/a.png").is_none());
    }
}
