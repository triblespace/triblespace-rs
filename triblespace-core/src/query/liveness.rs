//! Candidate storage and liveness for one search level — the two
//! representations of [`ProposalBuffer`] / [`Candidates`], selected at
//! compile time.
//!
//! The engine's confirm protocol is **kill-only**: a proposer appends
//! candidate values, confirmers clear liveness, and the engine iterates the
//! survivors. Nothing is ever compacted, so a candidate's index is stable for
//! as long as a binding can hold it. *How* liveness is stored is invisible to
//! every constraint in the tree — they only ever say `is_live(i)`, `kill(i)`,
//! `retain(..)` — which is what makes the representation swappable.
//!
//! Two representations live here:
//!
//! * **`words`** (default): one `u32` per candidate, `0` = dead. Every lane —
//!   CPU or GPU — writes its own word, so there is no read-modify-write
//!   contention anywhere, at 32x the memory of the information content. This
//!   is the baseline every alternative is measured against.
//! * **`bitmask`** (`liveness-bitmask` feature): 32 candidates packed per
//!   `u32`. 32x denser, `count_live`/`next_live` become `count_ones`/
//!   `trailing_zeros` over whole words — but a kill is now a
//!   read-modify-write on a word shared with 31 neighbours, and a region no
//!   longer starts on a word boundary.
//!
//! The selection is a cargo feature and **not** a runtime branch on purpose:
//! a predictable-but-present branch in the kill path would contaminate the
//! very measurement this exists to enable.
//!
//! # Building both
//!
//! ```text
//! cargo test --workspace                          # baseline
//! cargo test --workspace --features liveness-bitmask
//! cargo test -p triblespace-core --features liveness-bitmask
//! ```
//!
//! The workspace form works because `triblespace`, `triblespace-core` and
//! `triblespace-gpu` all declare a feature of that name; the last two must
//! agree, and `triblespace-gpu` const-asserts it (cargo features are per-crate,
//! so nothing else would catch a half-enabled build).
//!
//! # What the two share
//!
//! [`and_words`] and [`or_words`], and the [`Candidates::live_words`] /
//! [`Candidates::set_live_words`] pair they compose over, are already an
//! abstraction over *the liveness words* rather than over candidates:
//! `*w &= *o` intersects two live sets whether a word carries one boolean or
//! 32 packed bits. They are shared verbatim.
//!
//! The one thing callers must not assume is that there is one word **per
//! candidate**. [`Candidates::live_word_len`] is the only source of truth for
//! how many words a region's liveness occupies.
//!
//! # The region-boundary trap (bitmask only)
//!
//! A [`Candidates`] region is `[base..]` of a buffer for an arbitrary `base`.
//! With one word per candidate that is a plain sub-slice. Bit-packed, it is
//! not: the region's first and last words also carry bits belonging to the
//! *neighbouring* regions of the same buffer. So:
//!
//! * a write that touches a whole word silently kills or resurrects
//!   candidates the region does not own;
//! * a word handed out to a caller carries neighbours' liveness into whatever
//!   the caller composes it with.
//!
//! There is no compile-time signal for either bug — both show up only as
//! wrong query answers. The invariant is therefore enforced at the
//! **boundary of the type**: every write path (`kill`, `retain`, `kill_all`,
//! `set_live_words`) masks to the owned bits, and every read path
//! (`live_words`) zeroes the bits it does not own. See
//! the module-private `region_mask` in the bitmask module for the mask
//! itself.

/// Whether this build packs 32 candidates into each liveness word.
///
/// Exists so crates that *do* depend on the layout — chiefly `triblespace-gpu`,
/// whose confirm kernels write one verdict word per candidate — can assert at
/// compile time that their own mirror feature agrees with core's. Cargo
/// features are per-crate; nothing else stops a build from enabling
/// `triblespace-core/liveness-bitmask` alone and getting a clean compile with
/// nonsense results.
pub const LIVENESS_BITMASK: bool = cfg!(feature = "liveness-bitmask");

pub use repr::{Candidates, ProposalBuffer};

/// Word-wise AND of `other` into `words` (conjunction of live sets).
///
/// Representation-independent: intersecting two live sets is the same
/// operation whether a word holds one boolean or 32 packed bits.
pub fn and_words(words: &mut [u32], other: &[u32]) {
    debug_assert_eq!(words.len(), other.len());
    for (w, o) in words.iter_mut().zip(other.iter()) {
        *w &= *o;
    }
}

/// Word-wise OR of `other` into `words` (disjunction of live sets).
///
/// Representation-independent, as [`and_words`].
pub fn or_words(words: &mut [u32], other: &[u32]) {
    debug_assert_eq!(words.len(), other.len());
    for (w, o) in words.iter_mut().zip(other.iter()) {
        *w |= *o;
    }
}

// ---------------------------------------------------------------------------
// Representation A: one u32 per candidate (default).
// ---------------------------------------------------------------------------

#[cfg(not(feature = "liveness-bitmask"))]
mod repr {
    use crate::inline::RawInline;

    /// Growable buffer of candidate values for one variable at one search
    /// level — the write target of
    /// [`Constraint::propose`](crate::query::Constraint::propose) and the
    /// engine's per-level candidate store.
    ///
    /// Entries are plain `RawInline` (fixed-stride 32-byte POD), stored
    /// contiguously and **write-once**: nothing ever moves or rewrites a
    /// stored value. Each entry carries a parallel liveness word (`u32`,
    /// nonzero = live): confirmers kill entries instead of removing them, and
    /// the engine iterates live entries directly — there is no compaction.
    /// The pairing of value and liveness is structural (one type owns both),
    /// and the buffer derefs to `[RawInline]` for reading.
    ///
    /// The word-per-entry liveness layout is the deliberate baseline: every
    /// lane — CPU or GPU — writes its own word with no read-modify-write
    /// contention. The bit-packed alternative lives behind this same API,
    /// under the `liveness-bitmask` feature, to be justified against this
    /// baseline.
    #[derive(Clone, Debug, Default)]
    pub struct ProposalBuffer {
        entries: Vec<RawInline>,
        live: Vec<u32>,
    }

    impl ProposalBuffer {
        /// Creates an empty buffer.
        ///
        /// `const` so the empty level array the empty
        /// [`Binding`](crate::query::Binding) borrows can be a `static`.
        pub const fn new() -> Self {
            ProposalBuffer {
                entries: Vec::new(),
                live: Vec::new(),
            }
        }

        /// Appends a candidate, live.
        pub fn push(&mut self, value: RawInline) {
            self.entries.push(value);
            self.live.push(1);
        }

        /// Appends every candidate from `iter`, live.
        pub fn extend(&mut self, iter: impl IntoIterator<Item = RawInline>) {
            for value in iter {
                self.push(value);
            }
        }

        /// Appends every candidate from `slice`, live.
        pub fn extend_from_slice(&mut self, slice: &[RawInline]) {
            self.entries.extend_from_slice(slice);
            self.live.resize(self.entries.len(), 1);
        }

        /// Drops all candidates, keeping capacity.
        pub fn clear(&mut self) {
            self.entries.clear();
            self.live.clear();
        }

        /// Current capacity in entries.
        pub fn capacity(&self) -> usize {
            self.entries.capacity()
        }

        /// Reserves capacity for exactly `additional` further entries.
        pub fn reserve_exact(&mut self, additional: usize) {
            self.entries.reserve_exact(additional);
            self.live.reserve_exact(additional);
        }

        /// Index of the first live entry at or after `from`, if any.
        pub fn next_live(&self, from: usize) -> Option<usize> {
            self.live[from.min(self.live.len())..]
                .iter()
                .position(|w| *w != 0)
                .map(|offset| from + offset)
        }

        /// Number of live entries at or after `from`.
        pub fn count_live(&self, from: usize) -> usize {
            self.live[from.min(self.live.len())..]
                .iter()
                .filter(|w| **w != 0)
                .count()
        }

        /// Whether entry `i` is live.
        pub fn is_live(&self, i: usize) -> bool {
            self.live[i] != 0
        }

        /// Iterates the live entry values at or after `from` — the engine's own
        /// consumption view, also handy for inspecting survivors in tests.
        pub fn live_values(&self, from: usize) -> impl Iterator<Item = &RawInline> {
            self.entries[from..]
                .iter()
                .zip(self.live[from..].iter())
                .filter(|(_, w)| **w != 0)
                .map(|(v, _)| v)
        }

        /// The confirmable region from `base` onward: entry values paired with
        /// their killable liveness words.
        pub fn region(&mut self, base: usize) -> Candidates<'_> {
            Candidates {
                values: &self.entries[base..],
                live: &mut self.live[base..],
            }
        }

        /// Moves every entry of `other` (values and liveness together) onto
        /// the end of this buffer, leaving `other` empty. Existing entries —
        /// and therefore every index a binding holds into them — are
        /// untouched.
        pub fn append(&mut self, other: &mut ProposalBuffer) {
            self.entries.append(&mut other.entries);
            self.live.append(&mut other.live);
        }

        /// Splits off and returns the tail starting at `at` (values and
        /// liveness together).
        pub fn split_off(&mut self, at: usize) -> ProposalBuffer {
            ProposalBuffer {
                entries: self.entries.split_off(at),
                live: self.live.split_off(at),
            }
        }

        /// Rewrites the freshly-proposed region `[base..]` with `values`, all
        /// live. Only a proposer may call this, and only on the region it
        /// appended in the current call, before returning — after that,
        /// indices are frozen because kills bind to them. Used by
        /// [`UnionConstraint`](crate::query::unionconstraint::UnionConstraint)
        /// for its sort-dedup.
        pub fn rewrite_region(&mut self, base: usize, values: Vec<RawInline>) {
            self.entries.truncate(base);
            self.live.truncate(base);
            for value in values {
                self.push(value);
            }
        }
    }

    impl std::ops::Deref for ProposalBuffer {
        type Target = [RawInline];

        fn deref(&self) -> &[RawInline] {
            &self.entries
        }
    }

    impl<'a> IntoIterator for &'a ProposalBuffer {
        type Item = &'a RawInline;
        type IntoIter = std::slice::Iter<'a, RawInline>;

        fn into_iter(self) -> Self::IntoIter {
            self.entries.iter()
        }
    }

    /// A confirmable view over one proposal region: entry values read-only,
    /// liveness killable — the argument of
    /// [`Constraint::confirm`](crate::query::Constraint::confirm).
    ///
    /// Confirmers may only kill entries, never revive them, so any number of
    /// confirmers writing into the same region compute their conjunction —
    /// sequentially (each skipping already-dead entries) or in parallel over
    /// copies merged with [`and_words`](crate::query::and_words) /
    /// [`or_words`](crate::query::or_words). Index `i` refers to `values()[i]`.
    pub struct Candidates<'a> {
        values: &'a [RawInline],
        live: &'a mut [u32],
    }

    impl<'a> Candidates<'a> {
        /// The candidate values of this region.
        pub fn values(&self) -> &[RawInline] {
            self.values
        }

        /// Number of entries (live and dead) in this region.
        pub fn len(&self) -> usize {
            self.values.len()
        }

        /// True when the region has no entries.
        pub fn is_empty(&self) -> bool {
            self.values.is_empty()
        }

        /// Whether entry `i` is still live.
        pub fn is_live(&self, i: usize) -> bool {
            self.live[i] != 0
        }

        /// Marks entry `i` dead.
        pub fn kill(&mut self, i: usize) {
            self.live[i] = 0;
        }

        /// Kills every live entry whose value fails `keep` — the region-side
        /// equivalent of `Vec::retain`, skipping entries already dead. Lives on
        /// the pair type deliberately: values and liveness are one object here,
        /// so there is no loose pairing to misalign.
        pub fn retain(&mut self, mut keep: impl FnMut(&RawInline) -> bool) {
            for (i, value) in self.values.iter().enumerate() {
                if self.live[i] != 0 && !keep(value) {
                    self.live[i] = 0;
                }
            }
        }

        /// Kills every entry (a confirmer that can prove total inconsistency).
        pub fn kill_all(&mut self) {
            self.live.fill(0);
        }

        /// Reborrows this region mutably (for passing to several confirmers in
        /// sequence).
        pub fn reborrow(&mut self) -> Candidates<'_> {
            Candidates {
                values: self.values,
                live: self.live,
            }
        }

        /// Number of `u32` liveness words this region occupies — the length
        /// [`live_words`](Self::live_words) returns and
        /// [`set_live_words`](Self::set_live_words) expects.
        ///
        /// Here it happens to equal [`len`](Self::len); under
        /// `liveness-bitmask` it does not. Word-composition code must size its
        /// accumulators from this, never from the candidate count.
        pub fn live_word_len(&self) -> usize {
            self.live.len()
        }

        /// Bit position of entry 0 within the first word
        /// [`live_words`](Self::live_words) hands out.
        ///
        /// Always `0` here: one word per candidate makes a region a plain
        /// sub-slice, so entry `i` is word `i`. Under `liveness-bitmask` it is
        /// `base % 32` and entry `i` is bit `bit_offset() + i`. Only code that
        /// indexes liveness *bits* rather than *candidates* — i.e. a device
        /// kernel writing packed verdict words — has any business reading it;
        /// every CPU-side access goes through `is_live`/`kill`/`retain`.
        pub fn bit_offset(&self) -> usize {
            0
        }

        /// Copies the liveness words out (scratch for OR-composition).
        pub fn live_words(&self) -> Vec<u32> {
            self.live.to_vec()
        }

        /// Replaces the liveness words (merge result of OR-composition).
        pub fn set_live_words(&mut self, words: &[u32]) {
            debug_assert_eq!(words.len(), self.live.len());
            self.live.copy_from_slice(words);
        }

        /// A detached scratch region over `words` with the same values —
        /// used by OR-composition to collect per-variant verdicts.
        ///
        /// `words` must be [`live_word_len`](Self::live_word_len) long, and
        /// index `i` addresses the same bit position in it as in the real
        /// region, so the two can be merged word-wise.
        pub fn scratch<'b>(&self, words: &'b mut [u32]) -> Candidates<'b>
        where
            'a: 'b,
        {
            debug_assert_eq!(words.len(), self.live.len());
            Candidates {
                values: self.values,
                live: words,
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Representation B: 32 candidates packed per u32 (`liveness-bitmask`).
// ---------------------------------------------------------------------------

#[cfg(feature = "liveness-bitmask")]
mod repr {
    use crate::inline::RawInline;

    /// Candidates per liveness word.
    const BITS: usize = 32;

    /// Number of words needed to hold `bits` liveness bits.
    fn words_for(bits: usize) -> usize {
        bits.div_ceil(BITS)
    }

    /// Mask with bits `[lo, hi)` set. Requires `lo < hi <= 32`, which keeps
    /// both shifts below 32 (a 32-bit shift of a `u32` panics in debug and is
    /// undefined-ish in release).
    fn bit_range_mask(lo: usize, hi: usize) -> u32 {
        debug_assert!(lo < hi && hi <= BITS);
        (u32::MAX >> (BITS - (hi - lo))) << lo
    }

    /// Sets bits `[from, to)` of `live`. `live` must already be long enough.
    fn set_bits(live: &mut [u32], from: usize, to: usize) {
        if from >= to {
            return;
        }
        let first = from / BITS;
        let last = (to - 1) / BITS;
        if first == last {
            live[first] |= bit_range_mask(from - first * BITS, to - first * BITS);
            return;
        }
        live[first] |= bit_range_mask(from - first * BITS, BITS);
        for w in &mut live[first + 1..last] {
            *w = u32::MAX;
        }
        live[last] |= bit_range_mask(0, to - last * BITS);
    }

    /// The bits of word `k` that belong to a region with this `bit_offset` and
    /// candidate count — where `k` counts words from the region's own first
    /// word, as [`Candidates`] stores them.
    ///
    /// THE masking primitive, and the reason this module is not a mechanical
    /// port. A region covers bits `[bit_offset, bit_offset + len)` of its word
    /// slice; everything outside that in the first and last word is the
    /// liveness of *neighbouring* regions of the same buffer. Reads that hand
    /// words out neutralise everything outside the mask, writes leave
    /// everything outside it alone. Do not "optimise" a caller into a
    /// whole-word operation — a whole-word write is exactly the bug, and it has
    /// no symptom other than wrong query answers.
    fn region_mask(bit_offset: usize, len: usize, k: usize) -> u32 {
        let word_lo = k * BITS;
        let word_hi = word_lo + BITS;
        let lo = bit_offset.clamp(word_lo, word_hi);
        let hi = (bit_offset + len).clamp(word_lo, word_hi);
        if lo >= hi {
            0
        } else {
            bit_range_mask(lo - word_lo, hi - word_lo)
        }
    }

    /// Shrinks `live` to cover exactly `len` entries, clearing the bits of the
    /// dropped entries from the now-last partial word.
    ///
    /// **Tail-zero invariant.** Bits at or above `entries.len()` are always
    /// zero. That is what lets `count_live`/`next_live` fold whole words
    /// without a tail mask, and it is why a truncation cannot just call
    /// `Vec::truncate`: the stale high bits of the last word would resurrect
    /// candidates that no longer exist.
    fn truncate_live(live: &mut Vec<u32>, len: usize) {
        live.truncate(words_for(len));
        let tail = len % BITS;
        if tail != 0 {
            let last = live.len() - 1;
            live[last] &= bit_range_mask(0, tail);
        }
    }

    /// Growable buffer of candidate values for one variable at one search
    /// level — the write target of
    /// [`Constraint::propose`](crate::query::Constraint::propose) and the
    /// engine's per-level candidate store.
    ///
    /// Entries are plain `RawInline` (fixed-stride 32-byte POD), stored
    /// contiguously and **write-once**: nothing ever moves or rewrites a
    /// stored value. Liveness is a **bit per entry**, packed 32 to a `u32`:
    /// entry `i` is bit `i % 32` of `live[i / 32]`, set = live. Confirmers
    /// kill entries instead of removing them, and the engine iterates live
    /// entries directly — there is no compaction. The buffer derefs to
    /// `[RawInline]` for reading.
    ///
    /// # Invariants
    ///
    /// 1. `live.len() == words_for(entries.len())`.
    /// 2. **Tail-zero**: every bit at or above `entries.len()` is `0`. Whole
    ///    words can therefore be folded with `count_ones`/`trailing_zeros`
    ///    without a tail mask. Every path that shortens `entries` must go
    ///    through the module-private `truncate_live`.
    #[derive(Clone, Debug, Default)]
    pub struct ProposalBuffer {
        entries: Vec<RawInline>,
        live: Vec<u32>,
    }

    impl ProposalBuffer {
        /// Creates an empty buffer.
        ///
        /// `const` so the empty level array the empty
        /// [`Binding`](crate::query::Binding) borrows can be a `static`.
        pub const fn new() -> Self {
            ProposalBuffer {
                entries: Vec::new(),
                live: Vec::new(),
            }
        }

        /// Appends a candidate, live.
        pub fn push(&mut self, value: RawInline) {
            let i = self.entries.len();
            self.entries.push(value);
            // A new word is needed exactly when the entry opens one — which
            // includes the very first entry. It is pushed dead so the
            // tail-zero invariant holds for the 31 bits above `i`.
            if i % BITS == 0 {
                self.live.push(0);
            }
            self.live[i / BITS] |= 1 << (i % BITS);
        }

        /// Appends every candidate from `iter`, live.
        pub fn extend(&mut self, iter: impl IntoIterator<Item = RawInline>) {
            for value in iter {
                self.push(value);
            }
        }

        /// Appends every candidate from `slice`, live.
        pub fn extend_from_slice(&mut self, slice: &[RawInline]) {
            let from = self.entries.len();
            self.entries.extend_from_slice(slice);
            let to = self.entries.len();
            // `resize` only ever grows here and fills with 0, so the words
            // already in place (including the partial one holding `from`) keep
            // their bits.
            self.live.resize(words_for(to), 0);
            set_bits(&mut self.live, from, to);
        }

        /// Drops all candidates, keeping capacity.
        pub fn clear(&mut self) {
            self.entries.clear();
            self.live.clear();
        }

        /// Current capacity in entries.
        pub fn capacity(&self) -> usize {
            self.entries.capacity()
        }

        /// Reserves capacity for exactly `additional` further entries.
        pub fn reserve_exact(&mut self, additional: usize) {
            self.entries.reserve_exact(additional);
            // `Vec::reserve_exact` counts *beyond the current length*, so ask
            // for the shortfall between the words we hold and the words the
            // grown buffer will need.
            let needed = words_for(self.entries.len() + additional);
            self.live.reserve_exact(needed.saturating_sub(self.live.len()));
        }

        /// Index of the first live entry at or after `from`, if any.
        pub fn next_live(&self, from: usize) -> Option<usize> {
            if from >= self.entries.len() {
                return None;
            }
            let mut w = from / BITS;
            // Mask off the bits *below* `from` in the first word: they belong
            // to entries the caller has already consumed. Above the region
            // there is nothing to mask — the tail-zero invariant guarantees
            // bits past `entries.len()` are clear.
            let mut word = self.live[w] & (u32::MAX << (from % BITS));
            loop {
                if word != 0 {
                    return Some(w * BITS + word.trailing_zeros() as usize);
                }
                w += 1;
                if w >= self.live.len() {
                    return None;
                }
                word = self.live[w];
            }
        }

        /// Number of live entries at or after `from`.
        pub fn count_live(&self, from: usize) -> usize {
            if from >= self.entries.len() {
                return 0;
            }
            let first = from / BITS;
            // Same asymmetry as `next_live`: mask the head, trust the
            // tail-zero invariant for the tail.
            let head = self.live[first] & (u32::MAX << (from % BITS));
            head.count_ones() as usize
                + self.live[first + 1..]
                    .iter()
                    .map(|w| w.count_ones() as usize)
                    .sum::<usize>()
        }

        /// Whether entry `i` is live.
        pub fn is_live(&self, i: usize) -> bool {
            debug_assert!(i < self.entries.len(), "liveness read past the buffer");
            (self.live[i / BITS] >> (i % BITS)) & 1 != 0
        }

        /// Iterates the live entry values at or after `from` — the engine's own
        /// consumption view, also handy for inspecting survivors in tests.
        pub fn live_values(&self, from: usize) -> impl Iterator<Item = &RawInline> {
            // Capture the word slice rather than `self` so the returned
            // iterator borrows exactly what it reads.
            let live: &[u32] = &self.live;
            self.entries[from..]
                .iter()
                .enumerate()
                .filter_map(move |(k, value)| {
                    let i = from + k;
                    ((live[i / BITS] >> (i % BITS)) & 1 != 0).then_some(value)
                })
        }

        /// The confirmable region from `base` onward: entry values paired with
        /// their killable liveness words.
        ///
        /// Unlike the word-per-entry representation this is **not** a clean
        /// sub-slice: `base` lands mid-word, so the region borrows from
        /// `base / 32` and remembers `base % 32` as its bit offset. The bits
        /// below that offset in the first word — and above the last entry in
        /// the last word — belong to neighbouring regions of this same buffer.
        /// [`Candidates`] masks every access accordingly.
        pub fn region(&mut self, base: usize) -> Candidates<'_> {
            let first = base / BITS;
            Candidates {
                values: &self.entries[base..],
                words: &mut self.live[first..],
                bit_offset: base % BITS,
            }
        }

        /// Moves every entry of `other` (values and liveness together) onto
        /// the end of this buffer, leaving `other` empty. Existing entries —
        /// and therefore every index a binding holds into them — are
        /// untouched.
        ///
        /// # Cost
        ///
        /// The word-per-entry representation concatenates with a `memcpy`.
        /// Packed, `other`'s entry `k` has to land at absolute bit
        /// `base + k`, so unless the join is word-aligned every one of
        /// `other`'s words is shifted left by `base % 32` and its overflow
        /// carried into the next word: still O(words) — 1/32 of the word count
        /// the baseline copies — but a read-modify-write per word instead of a
        /// straight copy, and not vectorisable as a `memcpy`.
        pub fn append(&mut self, other: &mut ProposalBuffer) {
            let base = self.entries.len();
            let add = other.entries.len();
            self.entries.append(&mut other.entries);
            self.live.resize(words_for(base + add), 0);
            let shift = base % BITS;
            let first = base / BITS;
            for (k, w) in other.live.iter().enumerate() {
                let dst = first + k;
                if shift == 0 {
                    // Word-aligned join: `other`'s words drop straight in.
                    self.live[dst] |= *w;
                } else {
                    self.live[dst] |= *w << shift;
                    // The high `shift` bits spill into the next word. When the
                    // spill is nonzero there provably *is* a next word (the bit
                    // it carries belongs to an entry below `base + add`), so an
                    // out-of-range index here would mean invariant 1 is broken
                    // — let it panic rather than silently drop live bits.
                    let carry = *w >> (BITS - shift);
                    if carry != 0 {
                        self.live[dst + 1] |= carry;
                    }
                }
            }
            // `Vec::append` already emptied `other.entries`; restore invariant
            // 1 for `other` by dropping its now-orphaned words.
            other.live.clear();
        }

        /// Splits off and returns the tail starting at `at` (values and
        /// liveness together).
        ///
        /// # Cost
        ///
        /// The mirror image of [`append`](Self::append): the tail has to be
        /// re-based to bit 0, so unless `at` is a multiple of 32 every word is
        /// shifted right by `at % 32` with the low bits of the following word
        /// pulled into its top. O(words), read-modify-write, not a `memcpy`.
        pub fn split_off(&mut self, at: usize) -> ProposalBuffer {
            let entries = self.entries.split_off(at);
            let n = entries.len();
            let shift = at % BITS;
            let first = at / BITS;
            let mut live = vec![0u32; words_for(n)];
            for (k, out) in live.iter_mut().enumerate() {
                // `first + k` is always in range: floor(at/32) + ceil(n/32)
                // <= ceil((at + n)/32) == self.live.len().
                let low = self.live[first + k] >> shift;
                let high = if shift == 0 {
                    // Shifting a u32 by 32 is not allowed; word-aligned splits
                    // have nothing to pull down anyway.
                    0
                } else {
                    // `BITS - shift` is 1..=31 here, so the shift is legal.
                    // A missing successor word means there are no further
                    // entries, so the bits it would contribute are zero.
                    self.live.get(first + k + 1).copied().unwrap_or(0) << (BITS - shift)
                };
                *out = low | high;
            }
            // Drop the tail's words from the head and clear the tail's bits out
            // of the head's now-last partial word (tail-zero invariant).
            truncate_live(&mut self.live, at);
            ProposalBuffer { entries, live }
        }

        /// Rewrites the freshly-proposed region `[base..]` with `values`, all
        /// live. Only a proposer may call this, and only on the region it
        /// appended in the current call, before returning — after that,
        /// indices are frozen because kills bind to them. Used by
        /// [`UnionConstraint`](crate::query::unionconstraint::UnionConstraint)
        /// for its sort-dedup.
        pub fn rewrite_region(&mut self, base: usize, values: Vec<RawInline>) {
            self.entries.truncate(base);
            // Not `live.truncate(base)`: `base` is a *bit* index, and the word
            // holding it keeps the discarded entries' bits unless they are
            // masked off. See the tail-zero invariant on `truncate_live`.
            truncate_live(&mut self.live, base);
            for value in values {
                self.push(value);
            }
        }
    }

    impl std::ops::Deref for ProposalBuffer {
        type Target = [RawInline];

        fn deref(&self) -> &[RawInline] {
            &self.entries
        }
    }

    impl<'a> IntoIterator for &'a ProposalBuffer {
        type Item = &'a RawInline;
        type IntoIter = std::slice::Iter<'a, RawInline>;

        fn into_iter(self) -> Self::IntoIter {
            self.entries.iter()
        }
    }

    /// A confirmable view over one proposal region: entry values read-only,
    /// liveness killable — the argument of
    /// [`Constraint::confirm`](crate::query::Constraint::confirm).
    ///
    /// Confirmers may only kill entries, never revive them, so any number of
    /// confirmers writing into the same region compute their conjunction —
    /// sequentially (each skipping already-dead entries) or in parallel over
    /// copies merged with [`and_words`](crate::query::and_words) /
    /// [`or_words`](crate::query::or_words). Index `i` refers to `values()[i]`.
    ///
    /// # Layout
    ///
    /// Entry `i` is bit `bit_offset + i` of `words`, i.e. bit
    /// `(bit_offset + i) % 32` of `words[(bit_offset + i) / 32]`, with
    /// `bit_offset < 32` and
    /// `words.len() == words_for(bit_offset + values.len())`.
    ///
    /// # The bits this region does *not* own
    ///
    /// `words[0]`'s bits below `bit_offset`, and `words.last()`'s bits at or
    /// above `bit_offset + values.len()`, are the liveness of *neighbouring*
    /// regions of the same buffer. Touching them kills or resurrects
    /// candidates this region has no business deciding about, and nothing in
    /// the type system says so — the only symptom is wrong query answers.
    /// Every method below goes through the module-private `region_mask`.
    pub struct Candidates<'a> {
        values: &'a [RawInline],
        words: &'a mut [u32],
        /// Bit position of entry 0 within `words[0]`; `0..32`.
        bit_offset: usize,
    }

    impl<'a> Candidates<'a> {
        /// The candidate values of this region.
        pub fn values(&self) -> &[RawInline] {
            self.values
        }

        /// Number of entries (live and dead) in this region.
        pub fn len(&self) -> usize {
            self.values.len()
        }

        /// True when the region has no entries.
        pub fn is_empty(&self) -> bool {
            self.values.is_empty()
        }

        /// Whether entry `i` is still live.
        pub fn is_live(&self, i: usize) -> bool {
            debug_assert!(i < self.values.len(), "liveness read past the region");
            let b = self.bit_offset + i;
            (self.words[b / BITS] >> (b % BITS)) & 1 != 0
        }

        /// Marks entry `i` dead.
        pub fn kill(&mut self, i: usize) {
            debug_assert!(i < self.values.len(), "kill past the region");
            let b = self.bit_offset + i;
            self.words[b / BITS] &= !(1u32 << (b % BITS));
        }

        /// Kills every live entry whose value fails `keep` — the region-side
        /// equivalent of `Vec::retain`, skipping entries already dead. Lives on
        /// the pair type deliberately: values and liveness are one object here,
        /// so there is no loose pairing to misalign.
        pub fn retain(&mut self, mut keep: impl FnMut(&RawInline) -> bool) {
            for i in 0..self.values.len() {
                if self.is_live(i) && !keep(&self.values[i]) {
                    self.kill(i);
                }
            }
        }

        /// Kills every entry (a confirmer that can prove total inconsistency).
        pub fn kill_all(&mut self) {
            // NOT `words.fill(0)`. The word-per-entry representation could do
            // that because its slice *was* the region; here the first and last
            // words are shared with the neighbouring regions, and zeroing them
            // wholesale would kill candidates this region does not own.
            let (offset, len) = (self.bit_offset, self.values.len());
            for (k, w) in self.words.iter_mut().enumerate() {
                *w &= !region_mask(offset, len, k);
            }
        }

        /// Reborrows this region mutably (for passing to several confirmers in
        /// sequence).
        pub fn reborrow(&mut self) -> Candidates<'_> {
            Candidates {
                values: self.values,
                words: self.words,
                bit_offset: self.bit_offset,
            }
        }

        /// Number of `u32` liveness words this region occupies — the length
        /// [`live_words`](Self::live_words) returns and
        /// [`set_live_words`](Self::set_live_words) expects.
        ///
        /// This is *not* [`len`](Self::len): one word covers up to 32
        /// candidates, and a region that starts mid-word needs one more word
        /// than its candidate count alone implies. Word-composition code must
        /// size its accumulators from this.
        pub fn live_word_len(&self) -> usize {
            self.words.len()
        }

        /// Bit position of entry 0 within the first word
        /// [`live_words`](Self::live_words) hands out — `base % 32`, where
        /// `base` is the buffer index this region starts at.
        ///
        /// Entry `i` is bit `(bit_offset() + i) % 32` of word
        /// `(bit_offset() + i) / 32`. Only code that indexes liveness *bits*
        /// rather than *candidates* — i.e. a device kernel writing packed
        /// verdict words — has any business reading it; every CPU-side access
        /// goes through the masked methods above, which is what keeps the
        /// neighbouring regions' bits out of reach.
        pub fn bit_offset(&self) -> usize {
            self.bit_offset
        }

        /// Copies the liveness words out (scratch for OR-composition).
        ///
        /// NEUTRAL MASKING (read side): the bits this region does not own are
        /// zeroed. Zero is the identity of [`or_words`](crate::query::or_words),
        /// which is what this copy is documented to feed, so a neighbour's live
        /// candidate can never be OR-ed back into an accumulator and
        /// resurrected. It also makes `live_words().iter().all(|w| *w == 0)`
        /// mean "this region is entirely dead", which is otherwise false.
        /// Do not "simplify" this to `self.words.to_vec()`.
        pub fn live_words(&self) -> Vec<u32> {
            let (offset, len) = (self.bit_offset, self.values.len());
            self.words
                .iter()
                .enumerate()
                .map(|(k, w)| *w & region_mask(offset, len, k))
                .collect()
        }

        /// Replaces the liveness words (merge result of OR-composition).
        ///
        /// NEUTRAL MASKING (write side, and the load-bearing one): only the
        /// bits this region owns are taken from `words`; the neighbours' bits
        /// in the first and last word are preserved from the region's current
        /// liveness. Without this a single `copy_from_slice` — which is what
        /// the word-per-entry representation does, correctly — would overwrite
        /// up to 62 candidates belonging to other regions with whatever the
        /// caller's scratch happened to hold.
        ///
        /// Because the mask is applied here, a caller may pass a scratch whose
        /// out-of-region bits are arbitrary; combined with the zeroing in
        /// [`live_words`](Self::live_words) neither AND- nor OR-composition can
        /// leak across a region boundary.
        pub fn set_live_words(&mut self, words: &[u32]) {
            debug_assert_eq!(words.len(), self.words.len());
            let (offset, len) = (self.bit_offset, self.values.len());
            for (k, (word, incoming)) in self.words.iter_mut().zip(words.iter()).enumerate() {
                let mask = region_mask(offset, len, k);
                *word = (*word & !mask) | (*incoming & mask);
            }
        }

        /// A detached scratch region over `words` with the same values —
        /// used by OR-composition to collect per-variant verdicts.
        ///
        /// `words` must be [`live_word_len`](Self::live_word_len) long. The
        /// scratch keeps this region's `bit_offset`, so entry `i` sits at the
        /// same bit position in both and the two can be merged word-wise by
        /// [`and_words`](crate::query::and_words) /
        /// [`or_words`](crate::query::or_words) — that alignment is the whole
        /// point of carrying the offset instead of re-basing the copy.
        ///
        /// The scratch's out-of-region bits are the caller's business:
        /// [`live_words`](Self::live_words) hands them out as zeros, and
        /// [`set_live_words`](Self::set_live_words) ignores them on the way
        /// back in.
        pub fn scratch<'b>(&self, words: &'b mut [u32]) -> Candidates<'b>
        where
            'a: 'b,
        {
            debug_assert_eq!(words.len(), self.words.len());
            Candidates {
                values: self.values,
                words,
                bit_offset: self.bit_offset,
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Shared tests.
//
// These run under BOTH representations and use only the public API, so they
// are a differential spec rather than a test of one layout: anything asserted
// here is behaviour a liveness representation must have, and the bit-packed
// one is exactly as correct as the baseline or the suite says so.
//
// The cases that matter are the ones where the two layouts genuinely differ:
// regions that start mid-word (so their first word is shared with a
// neighbour), `append`/`split_off`/`rewrite_region` at non-word-aligned
// indices (so words have to be shifted or masked), and anything that hands
// liveness words in or out.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inline::RawInline;

    /// A distinguishable candidate value; the low byte of `i` lands in the
    /// last byte, which `retain` below keys off.
    fn v(i: usize) -> RawInline {
        let mut x = [0u8; 32];
        x[30] = (i >> 8) as u8;
        x[31] = i as u8;
        x
    }

    fn filled(n: usize) -> ProposalBuffer {
        let mut b = ProposalBuffer::new();
        for i in 0..n {
            b.push(v(i));
        }
        b
    }

    fn live_indices(b: &ProposalBuffer) -> Vec<usize> {
        (0..b.len()).filter(|&i| b.is_live(i)).collect()
    }

    #[test]
    fn push_is_live_across_word_boundaries() {
        let b = filled(70);
        assert_eq!(b.len(), 70);
        assert_eq!(b.count_live(0), 70);
        assert_eq!(b.count_live(33), 37);
        assert_eq!(b.count_live(70), 0);
        assert_eq!(b.next_live(0), Some(0));
        assert_eq!(b.next_live(32), Some(32));
        assert_eq!(b.next_live(69), Some(69));
        assert_eq!(b.next_live(70), None);
    }

    #[test]
    fn extend_from_slice_starts_mid_word() {
        let values: Vec<RawInline> = (0..70).map(v).collect();
        let mut b = ProposalBuffer::new();
        b.push(v(1000));
        b.extend_from_slice(&values);
        assert_eq!(b.len(), 71);
        assert_eq!(b.count_live(0), 71);
        assert_eq!(&b[1..], &values[..]);
    }

    #[test]
    fn kills_are_skipped_by_next_and_counted_out() {
        let mut b = filled(70);
        {
            let mut r = b.region(0);
            for i in [0usize, 1, 31, 32, 33, 69] {
                r.kill(i);
            }
        }
        assert_eq!(b.count_live(0), 64);
        assert_eq!(b.next_live(0), Some(2));
        assert_eq!(b.next_live(31), Some(34));
        assert_eq!(b.next_live(69), None);
        assert!(!b.is_live(31));
        assert!(b.is_live(34));
    }

    #[test]
    fn live_values_agrees_with_is_live() {
        let mut b = filled(70);
        {
            let mut r = b.region(0);
            r.kill(1);
            r.kill(33);
            r.kill(69);
        }
        let dead = [1usize, 33, 69];
        let got: Vec<RawInline> = b.live_values(0).copied().collect();
        let want: Vec<RawInline> = (0..70).filter(|i| !dead.contains(i)).map(v).collect();
        assert_eq!(got, want);

        let got_from: Vec<RawInline> = b.live_values(33).copied().collect();
        let want_from: Vec<RawInline> = (33..70).filter(|i| !dead.contains(i)).map(v).collect();
        assert_eq!(got_from, want_from);
    }

    /// THE TRAP: a region starting mid-word shares its first word with the
    /// entries before it. `kill_all` must not take them with it.
    #[test]
    fn kill_all_spares_the_entries_below_the_region() {
        let mut b = filled(70);
        {
            let mut r = b.region(5);
            assert_eq!(r.len(), 65);
            r.kill_all();
        }
        assert_eq!(live_indices(&b), (0..5).collect::<Vec<_>>());
    }

    /// The same trap from the other end: `region(40)` shares word 1 with
    /// entries 32..40.
    #[test]
    fn kill_all_spares_a_shared_word_below_a_later_base() {
        let mut b = filled(70);
        {
            let mut r = b.region(40);
            r.kill_all();
        }
        assert_eq!(b.count_live(0), 40);
        assert!(b.is_live(39));
        assert!(!b.is_live(40));
    }

    #[test]
    fn region_indices_are_relative_to_the_base() {
        let mut b = filled(70);
        {
            let mut r = b.region(5);
            r.kill(0);
            r.kill(27);
        }
        assert!(!b.is_live(5));
        assert!(!b.is_live(32));
        assert!(b.is_live(4));
        assert!(b.is_live(31));
        assert_eq!(b.count_live(0), 68);
    }

    #[test]
    fn retain_spares_dead_neighbours_and_skips_dead_entries() {
        let mut b = filled(70);
        {
            let mut r = b.region(5);
            r.retain(|value| value[31] % 2 == 0);
        }
        let expected: Vec<usize> = (0..5).chain((5..70).filter(|i| i % 2 == 0)).collect();
        assert_eq!(live_indices(&b), expected);
    }

    /// Read-side neutrality: entries 0..5 are live but are not this region's
    /// to report, so the words handed out must be all-zero.
    #[test]
    fn live_words_are_zero_outside_the_region() {
        let mut b = filled(70);
        {
            let mut r = b.region(5);
            r.kill_all();
        }
        assert!(b.is_live(0));
        let r = b.region(5);
        assert!(r.live_words().iter().all(|w| *w == 0));
    }

    /// Write-side neutrality: writing an all-dead word set back into a region
    /// must not touch the entries below its base.
    #[test]
    fn set_live_words_spares_the_entries_below_the_region() {
        let mut b = filled(70);
        {
            let mut r = b.region(5);
            let zeros = vec![0u32; r.live_word_len()];
            r.set_live_words(&zeros);
        }
        assert_eq!(live_indices(&b), (0..5).collect::<Vec<_>>());
    }

    #[test]
    fn live_word_len_describes_the_copies() {
        let mut b = filled(70);
        for base in [0usize, 1, 5, 31, 32, 33, 63, 64, 69, 70] {
            let r = b.region(base);
            assert_eq!(r.live_words().len(), r.live_word_len());
        }
    }

    /// The word geometry a device kernel has to reproduce: the region's words
    /// hold `bit_offset() + len()` bits, and `bit_offset()` is below one word.
    #[test]
    fn bit_offset_describes_the_word_geometry() {
        let mut b = filled(70);
        for base in [0usize, 1, 5, 31, 32, 33, 63, 64, 69, 70] {
            let r = b.region(base);
            let offset = r.bit_offset();
            assert!(offset < 32, "bit offset {offset} is not below a word");
            if LIVENESS_BITMASK {
                assert_eq!(offset, base % 32);
                assert_eq!(r.live_word_len(), (offset + r.len()).div_ceil(32));
            } else {
                assert_eq!(offset, 0);
                assert_eq!(r.live_word_len(), r.len());
            }
        }
    }

    /// The reconstruction a packed device kernel performs, pinned on the host:
    /// entry `i` of a region is bit `bit_offset() + i` of the words
    /// `live_words` hands out (word `i` in the baseline layout). Getting this
    /// wrong is a silent wrong-answer bug, so spell it here — this is the
    /// *host-side contract* the kernel is written against, not a test of any
    /// kernel.
    #[test]
    fn live_words_reconstruct_is_live_bit_by_bit() {
        let mut b = filled(70);
        {
            let mut r = b.region(0);
            for i in [0usize, 1, 30, 31, 32, 33, 40, 63, 64, 69] {
                r.kill(i);
            }
        }
        for base in [0usize, 1, 5, 31, 32, 33, 63, 64, 69, 70] {
            let r = b.region(base);
            let words = r.live_words();
            let offset = r.bit_offset();
            for i in 0..r.len() {
                let reconstructed = if LIVENESS_BITMASK {
                    let bit = offset + i;
                    (words[bit / 32] >> (bit % 32)) & 1 != 0
                } else {
                    words[i] != 0
                };
                assert_eq!(
                    reconstructed,
                    r.is_live(i),
                    "entry {i} of region({base}) reconstructs wrong"
                );
            }
        }
    }

    /// The exact shape `UnionConstraint::confirm` composes: per-variant votes
    /// on scratch copies, OR-ed together, written back.
    #[test]
    fn or_composition_over_scratch_copies() {
        let mut b = filled(70);
        {
            let mut r = b.region(5);
            let mut any = vec![0u32; r.live_word_len()];
            for step in [2usize, 3] {
                let mut scratch = r.live_words();
                {
                    let mut s = r.scratch(&mut scratch);
                    for i in 0..s.len() {
                        if i % step != 0 {
                            s.kill(i);
                        }
                    }
                }
                or_words(&mut any, &scratch);
            }
            r.set_live_words(&any);
        }
        let expected: Vec<usize> = (0..5)
            .chain((5..70).filter(|i| (i - 5) % 2 == 0 || (i - 5) % 3 == 0))
            .collect();
        assert_eq!(live_indices(&b), expected);
    }

    /// AND-composition over two scratch copies — the conjunction half of the
    /// same word abstraction.
    #[test]
    fn and_composition_over_scratch_copies() {
        let mut b = filled(70);
        {
            let mut r = b.region(5);
            let mut all = r.live_words();
            for step in [2usize, 3] {
                let mut scratch = r.live_words();
                {
                    let mut s = r.scratch(&mut scratch);
                    for i in 0..s.len() {
                        if i % step != 0 {
                            s.kill(i);
                        }
                    }
                }
                and_words(&mut all, &scratch);
            }
            r.set_live_words(&all);
        }
        let expected: Vec<usize> = (0..5)
            .chain((5..70).filter(|i| (i - 5) % 6 == 0))
            .collect();
        assert_eq!(live_indices(&b), expected);
    }

    #[test]
    fn append_at_a_non_aligned_base() {
        let mut a = filled(5);
        let mut tail = filled(70);
        {
            let mut r = tail.region(0);
            r.kill(0);
            r.kill(31);
            r.kill(69);
        }
        a.append(&mut tail);
        assert_eq!(a.len(), 75);
        assert_eq!(tail.len(), 0);
        assert_eq!(tail.count_live(0), 0);
        assert_eq!(a[36], v(31));
        assert!(!a.is_live(5));
        assert!(!a.is_live(36));
        assert!(!a.is_live(74));
        assert!(a.is_live(4));
        assert!(a.is_live(35));
        assert_eq!(a.count_live(0), 72);
    }

    #[test]
    fn append_at_a_word_aligned_base() {
        let mut a = filled(32);
        let mut tail = filled(40);
        {
            let mut r = tail.region(0);
            r.kill(7);
        }
        a.append(&mut tail);
        assert_eq!(a.len(), 72);
        assert_eq!(a[39], v(7));
        assert!(!a.is_live(39));
        assert_eq!(a.count_live(0), 71);
    }

    #[test]
    fn append_onto_an_empty_buffer() {
        let mut a = ProposalBuffer::new();
        let mut tail = filled(70);
        {
            let mut r = tail.region(0);
            r.kill(33);
        }
        a.append(&mut tail);
        assert_eq!(a.len(), 70);
        assert_eq!(a.count_live(0), 69);
        assert!(!a.is_live(33));
    }

    #[test]
    fn split_off_rebases_the_tail_and_trims_the_head() {
        let mut b = filled(70);
        {
            let mut r = b.region(0);
            r.kill(0);
            r.kill(4);
            r.kill(37);
            r.kill(69);
        }
        let tail = b.split_off(5);
        assert_eq!(b.len(), 5);
        assert_eq!(tail.len(), 65);
        // The head must not keep the tail's liveness bits in its last word.
        assert_eq!(live_indices(&b), vec![1, 2, 3]);
        assert_eq!(b.count_live(0), 3);
        assert_eq!(tail[0], v(5));
        assert_eq!(tail[64], v(69));
        assert!(!tail.is_live(32));
        assert!(!tail.is_live(64));
        assert_eq!(tail.count_live(0), 63);
    }

    #[test]
    fn split_off_at_a_word_boundary() {
        let mut b = filled(70);
        {
            let mut r = b.region(0);
            r.kill(32);
        }
        let tail = b.split_off(32);
        assert_eq!(b.len(), 32);
        assert_eq!(b.count_live(0), 32);
        assert_eq!(tail.len(), 38);
        assert!(!tail.is_live(0));
        assert_eq!(tail.count_live(0), 37);
    }

    #[test]
    fn split_off_at_zero_and_at_the_end() {
        let mut b = filled(70);
        let all = b.split_off(0);
        assert_eq!(b.len(), 0);
        assert_eq!(b.count_live(0), 0);
        assert_eq!(all.len(), 70);
        assert_eq!(all.count_live(0), 70);

        let mut c = filled(70);
        let empty = c.split_off(70);
        assert_eq!(empty.len(), 0);
        assert_eq!(empty.count_live(0), 0);
        assert_eq!(c.count_live(0), 70);
    }

    /// A split followed by an append must round-trip, which only holds if the
    /// right-shift and the left-shift agree.
    #[test]
    fn split_off_then_append_round_trips() {
        let mut b = filled(70);
        {
            let mut r = b.region(0);
            r.kill(3);
            r.kill(35);
            r.kill(64);
        }
        let before = live_indices(&b);
        let mut tail = b.split_off(13);
        b.append(&mut tail);
        assert_eq!(b.len(), 70);
        assert_eq!(live_indices(&b), before);
    }

    #[test]
    fn rewrite_region_drops_the_old_tails_liveness() {
        let mut b = filled(70);
        {
            let mut r = b.region(0);
            r.kill(3);
        }
        b.rewrite_region(5, vec![v(100), v(101)]);
        assert_eq!(b.len(), 7);
        assert_eq!(b[5], v(100));
        assert_eq!(b[6], v(101));
        assert_eq!(b.count_live(0), 6);
        assert_eq!(live_indices(&b), vec![0, 1, 2, 4, 5, 6]);
    }

    #[test]
    fn rewrite_region_at_zero_and_at_a_word_boundary() {
        let mut b = filled(70);
        b.rewrite_region(0, vec![v(1), v(2)]);
        assert_eq!(b.len(), 2);
        assert_eq!(b.count_live(0), 2);

        let mut c = filled(70);
        c.rewrite_region(32, vec![v(1)]);
        assert_eq!(c.len(), 33);
        assert_eq!(c.count_live(0), 33);
    }

    #[test]
    fn empty_region_at_the_end_is_inert() {
        let mut b = filled(64);
        {
            let mut r = b.region(64);
            assert_eq!(r.len(), 0);
            assert!(r.is_empty());
            r.kill_all();
            assert!(r.live_words().iter().all(|w| *w == 0));
        }
        assert_eq!(b.count_live(0), 64);
    }

    #[test]
    fn clear_resets_liveness() {
        let mut b = filled(70);
        {
            let mut r = b.region(0);
            r.kill_all();
        }
        b.clear();
        assert_eq!(b.len(), 0);
        assert_eq!(b.count_live(0), 0);
        for i in 0..70 {
            b.push(v(i));
        }
        assert_eq!(b.count_live(0), 70);
    }

    #[test]
    fn reserve_exact_keeps_contents() {
        let mut b = filled(70);
        {
            let mut r = b.region(0);
            r.kill(5);
        }
        b.reserve_exact(1000);
        assert_eq!(b.count_live(0), 69);
        assert!(!b.is_live(5));
        b.push(v(70));
        assert_eq!(b.count_live(0), 70);
        assert!(b.is_live(70));
    }
}
