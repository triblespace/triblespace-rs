use crate::id::Id;
use crate::id::RawId;
use crate::patch::{IdentitySchema, PATCHIntoOrderedIterator};

use super::Fragment;

/// Trait for types that can be "spread" into an `entity!` repeated attribute.
///
/// A spread produces an iterator of attribute values, plus a Fragment
/// of extras (facts + blobs) that gets merged into the entity's result
/// fragment.
///
/// Plain iterators return an empty extras Fragment. A [`Fragment`] returns
/// its exported ids as the values and its contained facts + blobs as the
/// extras.
pub trait Spread {
    /// The type of each yielded value.
    type Item;
    /// The iterator type returned by [`spread`](Spread::spread).
    type Iter: IntoIterator<Item = Self::Item>;
    /// Decomposes the value into an iterator of items and extras (facts +
    /// blobs) to merge.
    fn spread(self) -> (Self::Iter, Fragment);
}

impl<I: IntoIterator> Spread for I {
    type Item = I::Item;
    type Iter = I;
    fn spread(self) -> (Self::Iter, Fragment) {
        (self, Fragment::empty())
    }
}

/// Free function (not a closure) so `Map`'s type is nameable in
/// `Spread::Iter` below — keeps `Fragment::spread` allocation-free.
fn raw_to_id(raw: RawId) -> Id {
    Id::new(raw).expect("export ids are non-nil")
}

impl Spread for Fragment {
    type Item = Id;
    type Iter = std::iter::Map<
        PATCHIntoOrderedIterator<16, IdentitySchema, ()>,
        fn(RawId) -> Id,
    >;
    fn spread(self) -> (Self::Iter, Fragment) {
        let (exports, facts, blobs) = self.into_parts();
        // Wrap the remaining facts + blobs as an extras fragment with
        // no exports — the exports are consumed lazily as the spread
        // values via the mapping iterator below.
        let extras = Fragment::from_facts_and_blobs(facts, blobs);
        let iter = exports.into_iter_ordered().map(raw_to_id as fn(_) -> _);
        (iter, extras)
    }
}
