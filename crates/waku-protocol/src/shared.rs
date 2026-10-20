//! Copy-on-write collections for transcript data.
//!
//! A session's transcript is the largest thing a client holds, and both the
//! desktop's save path and its row builders take snapshots of it. Storing each
//! element behind an [`Arc`] makes a snapshot O(len) reference-count bumps
//! instead of a deep copy of every message body, while mutation clones only
//! the elements that a snapshot still shares ([`Arc::make_mut`]).
//!
//! On the wire and on disk a [`SharedVec<T>`] is exactly a `Vec<T>`.

use std::fmt;
use std::ops::{Deref, RangeBounds};
use std::sync::Arc;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A `Vec<T>` whose elements are individually reference counted.
///
/// Reads go through the `[Arc<T>]` slice it derefs to (`iter`, `len`,
/// `last`, indexing, ranges). There is deliberately no `DerefMut`: every
/// write goes through a method that uses [`Arc::make_mut`], so mutating one
/// element never disturbs a snapshot taken by [`Clone`] and never deep-copies
/// the elements that did not change.
pub struct SharedVec<T>(Vec<Arc<T>>);

impl<T> SharedVec<T> {
    pub const fn new() -> Self {
        Self(Vec::new())
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self(Vec::with_capacity(capacity))
    }

    pub fn push(&mut self, value: T) {
        self.0.push(Arc::new(value));
    }

    pub fn push_shared(&mut self, value: Arc<T>) {
        self.0.push(value);
    }

    pub fn insert(&mut self, index: usize, value: T) {
        self.0.insert(index, Arc::new(value));
    }

    pub fn remove(&mut self, index: usize) -> Arc<T> {
        self.0.remove(index)
    }

    pub fn pop(&mut self) -> Option<Arc<T>> {
        self.0.pop()
    }

    pub fn truncate(&mut self, len: usize) {
        self.0.truncate(len);
    }

    pub fn clear(&mut self) {
        self.0.clear();
    }

    /// Releases the backing allocation too, unlike [`Self::clear`].
    pub fn release(&mut self) {
        self.0 = Vec::new();
    }

    pub fn retain(&mut self, mut keep: impl FnMut(&T) -> bool) {
        self.0.retain(|value| keep(value));
    }

    pub fn drain(&mut self, range: impl RangeBounds<usize>) -> std::vec::Drain<'_, Arc<T>> {
        self.0.drain(range)
    }

    pub fn split_off(&mut self, at: usize) -> Self {
        Self(self.0.split_off(at))
    }

    pub fn reserve(&mut self, additional: usize) {
        self.0.reserve(additional);
    }

    /// Whether `self` and `other` hold the very same elements, compared by
    /// pointer rather than by value.
    pub fn ptr_eq(&self, other: &Self) -> bool {
        self.0.len() == other.0.len()
            && self
                .0
                .iter()
                .zip(&other.0)
                .all(|(left, right)| Arc::ptr_eq(left, right))
    }
}

impl<T: Clone> SharedVec<T> {
    /// Mutable access to one element, cloning it first only when a snapshot
    /// still shares it.
    pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        self.0.get_mut(index).map(Arc::make_mut)
    }

    /// Like [`Self::get_mut`], panicking on an out-of-bounds index the way
    /// `IndexMut` would.
    pub fn make_mut(&mut self, index: usize) -> &mut T {
        Arc::make_mut(&mut self.0[index])
    }

    pub fn first_mut(&mut self) -> Option<&mut T> {
        self.0.first_mut().map(Arc::make_mut)
    }

    pub fn last_mut(&mut self) -> Option<&mut T> {
        self.0.last_mut().map(Arc::make_mut)
    }

    /// Mutable iteration. Every visited element is unshared first, so prefer
    /// [`Self::get_mut`] or [`Self::last_mut`] when only a few change.
    pub fn iter_mut(&mut self) -> impl DoubleEndedIterator<Item = &mut T> + ExactSizeIterator {
        self.0.iter_mut().map(Arc::make_mut)
    }

    /// [`Self::iter_mut`] restricted to `range`.
    pub fn range_mut(
        &mut self,
        range: impl std::slice::SliceIndex<[Arc<T>], Output = [Arc<T>]>,
    ) -> impl DoubleEndedIterator<Item = &mut T> + ExactSizeIterator {
        self.0[range].iter_mut().map(Arc::make_mut)
    }

    /// The last element matching `matches`, made mutable. Elements that are
    /// only inspected stay shared.
    pub fn rfind_mut(&mut self, mut matches: impl FnMut(&T) -> bool) -> Option<&mut T> {
        let index = self.0.iter().rposition(|value| matches(value))?;
        Some(Arc::make_mut(&mut self.0[index]))
    }

    /// Applies `update` to every element matching `matches`, unsharing only
    /// those. Returns how many were updated.
    pub fn update_where(
        &mut self,
        mut matches: impl FnMut(&T) -> bool,
        mut update: impl FnMut(&mut T),
    ) -> usize {
        let mut updated = 0;
        for value in &mut self.0 {
            if matches(value) {
                update(Arc::make_mut(value));
                updated += 1;
            }
        }
        updated
    }

    /// The elements as owned values, cloning only those still shared.
    pub fn into_vec(self) -> Vec<T> {
        self.0.into_iter().map(Arc::unwrap_or_clone).collect()
    }

    /// Clones the elements out into a plain `Vec`.
    pub fn to_vec(&self) -> Vec<T> {
        self.0.iter().map(|value| T::clone(value)).collect()
    }
}

impl<T> Deref for SharedVec<T> {
    type Target = [Arc<T>];

    fn deref(&self) -> &[Arc<T>] {
        &self.0
    }
}

/// Cheap: clones the element pointers, not the elements.
impl<T> Clone for SharedVec<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> Default for SharedVec<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: fmt::Debug> fmt::Debug for SharedVec<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_list().entries(self.0.iter()).finish()
    }
}

impl<T: PartialEq> PartialEq for SharedVec<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0.len() == other.0.len()
            && self
                .0
                .iter()
                .zip(&other.0)
                .all(|(left, right)| Arc::ptr_eq(left, right) || **left == **right)
    }
}

impl<T: Eq> Eq for SharedVec<T> {}

impl<T> From<Vec<T>> for SharedVec<T> {
    fn from(values: Vec<T>) -> Self {
        values.into_iter().collect()
    }
}

impl<T> From<Vec<Arc<T>>> for SharedVec<T> {
    fn from(values: Vec<Arc<T>>) -> Self {
        Self(values)
    }
}

impl<T: Clone> From<SharedVec<T>> for Vec<T> {
    fn from(values: SharedVec<T>) -> Self {
        values.into_vec()
    }
}

impl<T> FromIterator<T> for SharedVec<T> {
    fn from_iter<I: IntoIterator<Item = T>>(values: I) -> Self {
        Self(values.into_iter().map(Arc::new).collect())
    }
}

impl<T> FromIterator<Arc<T>> for SharedVec<T> {
    fn from_iter<I: IntoIterator<Item = Arc<T>>>(values: I) -> Self {
        Self(values.into_iter().collect())
    }
}

impl<T> Extend<T> for SharedVec<T> {
    fn extend<I: IntoIterator<Item = T>>(&mut self, values: I) {
        self.0.extend(values.into_iter().map(Arc::new));
    }
}

impl<T> Extend<Arc<T>> for SharedVec<T> {
    fn extend<I: IntoIterator<Item = Arc<T>>>(&mut self, values: I) {
        self.0.extend(values);
    }
}

impl<'a, T> IntoIterator for &'a SharedVec<T> {
    type Item = &'a Arc<T>;
    type IntoIter = std::slice::Iter<'a, Arc<T>>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

/// Unshares every element it visits; see [`SharedVec::iter_mut`].
impl<'a, T: Clone> IntoIterator for &'a mut SharedVec<T> {
    type Item = &'a mut T;
    type IntoIter = std::iter::Map<std::slice::IterMut<'a, Arc<T>>, fn(&mut Arc<T>) -> &mut T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0
            .iter_mut()
            .map(Arc::make_mut as fn(&mut Arc<T>) -> &mut T)
    }
}

impl<T> IntoIterator for SharedVec<T> {
    type Item = Arc<T>;
    type IntoIter = std::vec::IntoIter<Arc<T>>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<T: Serialize> Serialize for SharedVec<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.0.iter().map(|value| &**value))
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for SharedVec<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Vec::<T>::deserialize(deserializer).map(Self::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clone_shares_elements_and_mutation_unshares_only_the_written_one() {
        let mut values: SharedVec<String> = vec!["a".to_owned(), "b".to_owned()].into();
        let snapshot = values.clone();
        assert!(values.ptr_eq(&snapshot));

        values.last_mut().unwrap().push('!');

        assert!(Arc::ptr_eq(&values[0], &snapshot[0]));
        assert!(!Arc::ptr_eq(&values[1], &snapshot[1]));
        assert_eq!(*snapshot[1], "b");
        assert_eq!(*values[1], "b!");
    }

    #[test]
    fn serializes_exactly_like_a_vec() {
        let values: SharedVec<u32> = vec![1, 2, 3].into();
        let encoded = serde_json::to_string(&values).unwrap();
        assert_eq!(encoded, serde_json::to_string(&vec![1, 2, 3]).unwrap());
        let decoded: SharedVec<u32> = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn targeted_updates_leave_inspected_elements_shared() {
        let mut values: SharedVec<String> =
            vec!["keep".to_owned(), "edit".to_owned(), "keep".to_owned()].into();
        let snapshot = values.clone();

        assert_eq!(
            values.update_where(|value| value == "edit", |value| value.push('!')),
            1
        );
        values.rfind_mut(|value| value == "nothing");

        assert!(Arc::ptr_eq(&values[0], &snapshot[0]));
        assert!(!Arc::ptr_eq(&values[1], &snapshot[1]));
        assert!(Arc::ptr_eq(&values[2], &snapshot[2]));
        assert_eq!(*values[1], "edit!");
    }

    #[test]
    fn unshared_mutation_does_not_reallocate() {
        let mut values: SharedVec<String> = vec!["a".to_owned()].into();
        let before = Arc::as_ptr(&values[0]);
        values.make_mut(0).push('b');
        assert_eq!(Arc::as_ptr(&values[0]), before);
    }
}
