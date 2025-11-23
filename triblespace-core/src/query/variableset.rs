use std::fmt;

/// A fixed size bitset for up to 128 variables.
/// The set is represented as a 128-bit integer where each bit
/// represents the presence of the corresponding element in the set.
#[derive(Eq, PartialEq, Clone, Copy)]
#[repr(transparent)]
pub struct VariableSet {
    bits: u128,
}

impl VariableSet {
    /// Create a new empty set.
    #[must_use]
    pub const fn new_empty() -> Self {
        VariableSet { bits: 0 }
    }
    /// Create a new set with every value of the domain set.
    #[must_use]
    pub const fn new_full() -> Self {
        VariableSet { bits: !0 }
    }

    /// Create a new set with a single value from the domain set.
    #[must_use]
    pub fn new_singleton(index: usize) -> Self {
        let mut set = Self::new_empty();
        set.set(index);
        set
    }

    /// Check if the set is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bits == 0
    }
    /// Count the number of elements in the set.
    #[must_use]
    pub fn count(&self) -> usize {
        self.bits.count_ones() as usize
    }
    /// Set the given value in the set.
    pub fn set(&mut self, index: usize) {
        self.bits |= 1 << index;
    }
    /// Remove the given value from the set.
    pub fn unset(&mut self, index: usize) {
        self.bits &= !(1 << index);
    }
    /// Sets or removes the given element into or from the set
    /// depending on the passed value.
    pub fn set_value(&mut self, index: usize, value: bool) {
        if value {
            self.set(index);
        } else {
            self.unset(index);
        }
    }
    /// Include every value in the domain in the set.
    pub fn set_all(&mut self) {
        self.bits = !0;
    }
    /// Remove all values from the set.
    pub fn unset_all(&mut self) {
        self.bits = 0;
    }
    /// Check if the given value is in the set.
    #[must_use]
    pub fn is_set(&self, index: usize) -> bool {
        0 != (self.bits & (1 << index))
    }
    /// Finds the index of the first set bit.
    /// If no bits are set, returns `None`.
    #[must_use]
    pub fn find_first_set(&self) -> Option<usize> {
        if self.bits != 0 {
            return Some(self.bits.trailing_zeros() as usize);
        }
        None
    }
    /// Finds the index of the last set bit.
    /// If no bits are set, returns `None`.
    #[must_use]
    pub fn find_last_set(&self) -> Option<usize> {
        if self.bits != 0 {
            return Some(127 - (self.bits.leading_zeros() as usize));
        }
        None
    }
    /// Returns the index of the next set bit
    /// in the bit set, in ascending order, while unseting it.
    pub fn drain_next_ascending(&mut self) -> Option<usize> {
        if let Some(next_index) = self.find_first_set() {
            self.unset(next_index);
            Some(next_index)
        } else {
            None
        }
    }
    /// Returns the index of the next set bit
    /// in the bit set, in descending order, while unseting it.
    pub fn drain_next_descending(&mut self) -> Option<usize> {
        if let Some(next_index) = self.find_last_set() {
            self.unset(next_index);
            Some(next_index)
        } else {
            None
        }
    }
    /// Checks if the set is a superset of the passed set.
    #[must_use]
    pub fn is_superset_of(&self, other: &Self) -> bool {
        ((self.bits & other.bits) ^ other.bits) == 0
    }
    /// Checks if the set is a subset of the passed set.
    #[must_use]
    pub fn is_subset_of(&self, other: &Self) -> bool {
        ((self.bits & other.bits) ^ self.bits) == 0
    }
    /// Compute the set intersection between the two given sets.
    #[must_use]
    pub fn intersect(self, other: Self) -> Self {
        Self {
            bits: self.bits & other.bits,
        }
    }
    /// Compute the set union between the two given sets.
    #[must_use]
    pub fn union(self, other: Self) -> Self {
        Self {
            bits: self.bits | other.bits,
        }
    }
    /// Compute the set subtraction between the two given sets.
    #[must_use]
    pub fn subtract(self, other: Self) -> Self {
        Self {
            bits: self.bits & !other.bits,
        }
    }
    /// Compute the set difference between the two given sets.
    #[must_use]
    pub fn difference(self, other: Self) -> Self {
        Self {
            bits: self.bits ^ other.bits,
        }
    }
    /// Compute a set complement, removing every element that was in the set
    /// and inserting every element from the domain that wasn't in the set.
    #[must_use]
    pub fn complement(self) -> Self {
        Self { bits: !self.bits }
    }
    /// Remove all elements from the set except the one passed.
    /// Equal to an intersection with a set containing only the passed element.
    pub fn keep_single(&mut self, index: usize) {
        let had_bit = self.is_set(index);
        self.unset_all();
        if had_bit {
            self.set(index);
        }
    }

    /// Similar to keep_single, except that only values in the
    /// specified range are kept. Both range ends are inclusive.
    pub fn keep_range(&mut self, from_index: usize, to_index: usize) {
        self.bits &= !0 << from_index;
        self.bits &= !(!1 << to_index);
    }
}

impl fmt::Debug for VariableSet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "VariableSet: {{")?;
        let mut needs_comma = false;
        for byte in 0..128 {
            if self.is_set(byte) {
                if needs_comma {
                    write!(f, ", ",)?;
                }
                write!(f, "{byte}")?;
                needs_comma = true;
            }
        }
        writeln!(f, "}}")?;
        Ok(())
    }
}

pub struct VariableSetIterator(VariableSet);

impl Iterator for VariableSetIterator {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.drain_next_ascending()
    }
}

impl IntoIterator for VariableSet {
    type Item = usize;
    type IntoIter = VariableSetIterator;

    fn into_iter(self) -> Self::IntoIter {
        VariableSetIterator(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn new_empty_is_empty() {
        let set = VariableSet::new_empty();
        assert!(set.is_empty());
    }
    #[test]
    fn new_full_is_not_empty() {
        let set = VariableSet::new_full();
        assert!(!set.is_empty());
    }
    #[test]
    fn after_set_is_not_empty() {
        let mut set = VariableSet::new_empty();
        set.set(5);
        assert!(!set.is_empty());
    }
    #[test]
    fn after_set_unset_is_empty() {
        let mut set = VariableSet::new_empty();
        set.set(5);
        set.unset(5);
        assert!(set.is_empty());
    }
    #[test]
    fn after_set_is_set() {
        let mut set = VariableSet::new_empty();
        set.set(5);
        assert!(set.is_set(5));
    }
    #[test]
    fn after_unset_is_not_set() {
        let mut set = VariableSet::new_full();
        set.unset(5);
        assert!(!set.is_set(5));
    }
    #[test]
    fn find_first_set_none() {
        let set = VariableSet::new_empty();
        assert_eq!(None, set.find_first_set());
    }
    #[test]
    fn find_last_set_none() {
        let set = VariableSet::new_empty();
        assert_eq!(None, set.find_last_set());
    }
    #[test]
    fn superset_full_of_empty() {
        let empty = VariableSet::new_empty();
        let full = VariableSet::new_full();
        assert!(full.is_superset_of(&empty));
    }
    #[test]
    fn superset_not_empty_of_full() {
        let empty = VariableSet::new_empty();
        let full = VariableSet::new_full();
        assert!(!empty.is_superset_of(&full));
    }
    #[test]
    fn subset_empty_of_full() {
        let empty = VariableSet::new_empty();
        let full = VariableSet::new_full();
        assert!(empty.is_subset_of(&full));
    }
    #[test]
    fn subset_not_full_of_empty() {
        let empty = VariableSet::new_empty();
        let full = VariableSet::new_full();
        assert!(!full.is_subset_of(&empty));
    }
    proptest! {
        #[test]
        fn find_first_set(n in 0..128usize) {
            let mut set = VariableSet::new_empty();
            set.set(n);
            prop_assert_eq!(Some(n), set.find_first_set());
        }
        #[test]
        fn find_last_set(n in 0..128usize) {
            let mut set = VariableSet::new_empty();
            set.set(n);
            prop_assert_eq!(Some(n), set.find_last_set());
        }
        #[test]
        fn drain_ascending_drains(n in 0..128usize) {
            let mut set = VariableSet::new_empty();
            set.set(n);
            prop_assert_eq!(Some(n), set.drain_next_ascending());
            prop_assert!(!set.is_set(n));
        }
        #[test]
        fn drain_descending_drains(n in 0..128usize) {
            let mut set = VariableSet::new_empty();
            set.set(n);
            prop_assert_eq!(Some(n), set.drain_next_descending());
            prop_assert!(!set.is_set(n));
        }
        #[test]
        fn intersect(n in 0..128usize, m in 0..128usize) {
            let mut left = VariableSet::new_empty();
            let mut right = VariableSet::new_empty();
            left.set(n);
            right.set(m);

            let out = left.intersect(right);
            prop_assert_eq!(n == m, out.is_set(n));
        }
        #[test]
        fn union(n in 0..128usize, m in 0..128usize) {
            let mut left = VariableSet::new_empty();
            let mut right = VariableSet::new_empty();
            left.set(n);
            right.set(m);

            let out = left.union(right);
            prop_assert!(out.is_set(n));
            prop_assert!(out.is_set(m));
        }
        #[test]
        fn subtract(n in 0..128usize, m in 0..128usize) {
            let mut left = VariableSet::new_empty();
            let mut right = VariableSet::new_empty();
            left.set(n);
            right.set(m);

            let out = left.subtract(right);
            prop_assert_eq!(n != m, out.is_set(n));
        }
        #[test]
        fn difference(n in 0..128usize, m in 0..128usize) {
            let mut left = VariableSet::new_empty();
            let mut right = VariableSet::new_empty();
            left.set(n);
            right.set(m);

            let out = left.difference(right);
            prop_assert_eq!(n != m, out.is_set(n));
            prop_assert_eq!(n != m, out.is_set(m));
        }
        #[test]
        fn complement(n in 0..128usize, m in 0..128usize) {
            let mut input = VariableSet::new_empty();
            input.set(n);

            let out = input.complement();
            prop_assert!(!out.is_set(n));
            if n != m {
                prop_assert!(out.is_set(m));
            }
        }
        #[test]
        fn keep_single(n in 0..128usize, m in 0..128usize) {
            let mut set = VariableSet::new_full();
            set.keep_single(n);
            prop_assert!(set.is_set(n));
            if n != m {
                prop_assert!(!set.is_set(m));
            }
        }
        #[test]
        fn keep_range(from in 0..128usize, to in 0..128usize) {
            let mut set = VariableSet::new_full();
            set.keep_range(from, to);

            for n in 0..128 {
                prop_assert_eq!(from <= n && n <= to, set.is_set(n));
            }
        }
    }
}
