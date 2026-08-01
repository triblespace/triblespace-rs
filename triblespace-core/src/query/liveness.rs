//! Candidate storage and liveness for one search level — [`ProposalBuffer`]
//! and [`Candidates`].
//!
//! The engine's confirm protocol is **kill-only**: a proposer appends
//! candidate values, confirmers clear liveness, and the engine iterates the
//! survivors. Nothing is ever compacted once the region is visible to a
//! caller, so a candidate's index is stable for as long as a binding can hold
//! it. *How* liveness is stored is invisible to every constraint in the tree —
//! they only ever say `is_live(i)`, `kill(i)`, `retain(..)`.
//!
//! Liveness is **bit-packed**: 32 candidates per `u32`, set = live. That is
//! 32x denser than the information content needs, and `count_live` /
//! `next_live` fold whole words through `count_ones` / `trailing_zeros`
//! instead of scanning per candidate. The price is paid in this module and
//! nowhere else: a kill is a read-modify-write on a word shared with 31
//! neighbours, and a region no longer starts on a word boundary. (The obvious
//! alternative — one `u32` per candidate, no shared words, no masks — was the
//! baseline this replaced; it lives in git history if it ever needs
//! remeasuring.)
//!
//! Parent tags are *not* packed and never will be: they are `u32` frontier row
//! numbers, one per entry, and every reader wants random access to them. Only
//! `live` changes representation here.
//!
//! # The word abstraction
//!
//! [`and_words`] and [`or_words`], and the [`Candidates::live_words`] /
//! [`Candidates::set_live_words`] pair they compose over, are an abstraction
//! over *the liveness words* rather than over candidates: `*w &= *o`
//! intersects two live sets however many candidates a word carries.
//!
//! The one thing callers must not assume is that there is one word **per
//! candidate**. [`Candidates::live_word_len`] is the only source of truth for
//! how many words a region's liveness occupies.
//!
//! # The region-boundary trap
//!
//! A [`Candidates`] region is `[base..]` of a buffer for an arbitrary `base`
//! — and [`Candidates::for_each_parent`] cuts *that* into per-parent runs at
//! arbitrary further offsets. Bit-packed, neither is a plain sub-slice: the
//! region's first and last words also carry bits belonging to the
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
//! (`live_words`) zeroes the bits it does not own. See the module-private
//! `region_mask` below for the mask itself.

use crate::inline::RawInline;

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

/// Candidates carried by one `u32` liveness word — **the** core/device
/// geometry contract.
///
/// A device kernel that writes liveness has to reproduce this number
/// exactly: it decides which ballot lane lands in which bit, how a region's
/// `bit_offset` is turned into a word index, and how many verdict words a
/// region needs. Nothing in either crate's type system relates the two, so
/// widening the word here without widening the kernel would not fail to
/// compile and would not crash — it would return **silently wrong query
/// answers**, because verdict bits would be written for the wrong candidates.
///
/// Hence this is `pub` and const-asserted on the device side (see
/// `triblespace-gpu`'s `batch_confirm`), so the mismatch becomes a compile
/// error that names the reason.
pub const LIVENESS_WORD_BITS: usize = 32;

/// Candidates per liveness word — the module-internal spelling of
/// [`LIVENESS_WORD_BITS`].
const BITS: usize = LIVENESS_WORD_BITS;

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
/// THE masking primitive. A region covers bits `[bit_offset, bit_offset +
/// len)` of its word slice; everything outside that in the first and last word
/// is the liveness of *neighbouring* regions of the same buffer. Reads that
/// hand words out neutralise everything outside the mask, writes leave
/// everything outside it alone. Do not "optimise" a caller into a whole-word
/// operation — a whole-word write is exactly the bug, and it has no symptom
/// other than wrong query answers.
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
/// stored value once its region is visible to a caller. Liveness is a
/// **bit per entry**, packed 32 to a `u32`: entry `i` is bit `i % 32` of
/// `live[i / 32]`, set = live. Confirmers kill entries instead of removing
/// them, and the engine iterates live entries directly — there is no
/// compaction. The buffer derefs to `[RawInline]` for reading.
///
/// The buffer is **segmented**: a proposer expanding a
/// [`Frontier`](crate::query::Frontier) calls [`open`](ProposalBuffer::open)
/// with the row it is about to expand, and every candidate appended
/// afterwards carries that row as its **parent tag**. The tag is what lets
/// one region hold the candidates of a whole batch: confirmers read it to
/// find which binding a candidate belongs to, and the engine reads it to
/// reconstruct the child row. Tags are stored per entry rather than as
/// segment offsets because that is the form both readers want — random
/// access, and no requirement that a proposer emit its segments
/// contiguously (see
/// [`UnionConstraint`](crate::query::unionconstraint::UnionConstraint),
/// which interleaves variants and then sorts).
///
/// # Invariants
///
/// 1. `live.len() == words_for(entries.len())`, and
///    `parents.len() == entries.len()`.
/// 2. **Tail-zero**: every bit at or above `entries.len()` is `0`. Whole
///    words can therefore be folded with `count_ones`/`trailing_zeros`
///    without a tail mask. Every path that shortens `entries` must go
///    through the module-private `truncate_live`.
#[derive(Clone, Debug, Default)]
pub struct ProposalBuffer {
    entries: Vec<RawInline>,
    live: Vec<u32>,
    parents: Vec<u32>,
    parent: u32,
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
            parents: Vec::new(),
            parent: 0,
        }
    }

    /// Opens the segment for frontier row `parent`: every candidate
    /// appended until the next `open` is tagged as belonging to it.
    ///
    /// A proposer handed a frontier of `n` rows calls this once per row.
    /// The default tag is row 0, so a proposer that only ever sees a
    /// frontier of one needs no call at all.
    ///
    /// Packing does not reach here. `open` moves no data and touches no
    /// liveness — it only sets which tag the *next* `push` records, and the
    /// tags are a plain `u32` per entry. What the packed layout changes is
    /// the appending itself: the segment a proposer opens starts at whatever
    /// bit `entries.len()` happens to be, so a segment is a bit range and not
    /// a word range, and the entries it appends share their first and last
    /// word with the segments either side of it. Every method that writes
    /// liveness below is written for that; `open` needs nothing.
    pub fn open(&mut self, parent: u32) {
        self.parent = parent;
    }

    /// The frontier row entry `i` was proposed for.
    pub fn parent_of(&self, i: usize) -> u32 {
        self.parents[i]
    }

    /// Appends a candidate, live, tagged with the open segment.
    pub fn push(&mut self, value: RawInline) {
        let i = self.entries.len();
        self.entries.push(value);
        self.parents.push(self.parent);
        // A new word is needed exactly when the entry opens one — which
        // includes the very first entry. It is pushed dead so the
        // tail-zero invariant holds for the 31 bits above `i`.
        if i % BITS == 0 {
            self.live.push(0);
        }
        self.live[i / BITS] |= 1 << (i % BITS);
    }

    /// Appends every candidate from `iter`, live, tagged with the open
    /// segment.
    pub fn extend(&mut self, iter: impl IntoIterator<Item = RawInline>) {
        for value in iter {
            self.push(value);
        }
    }

    /// Appends every candidate from `slice`, live, tagged with the open
    /// segment.
    pub fn extend_from_slice(&mut self, slice: &[RawInline]) {
        let from = self.entries.len();
        self.entries.extend_from_slice(slice);
        let to = self.entries.len();
        self.parents.resize(to, self.parent);
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
        self.parents.clear();
        self.parent = 0;
    }

    /// Current capacity in entries.
    pub fn capacity(&self) -> usize {
        self.entries.capacity()
    }

    /// Reserves capacity for exactly `additional` further entries.
    pub fn reserve_exact(&mut self, additional: usize) {
        self.entries.reserve_exact(additional);
        self.parents.reserve_exact(additional);
        // `Vec::reserve_exact` counts *beyond the current length*, so ask
        // for the shortfall between the words we hold and the words the
        // grown buffer will need.
        let needed = words_for(self.entries.len() + additional);
        self.live
            .reserve_exact(needed.saturating_sub(self.live.len()));
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

    /// Cursor immediately after the first `count` live entries in
    /// `start..end`, provided that at least one further live entry remains.
    ///
    /// This is the exact boundary that a sequential `take_chunk(count)` would
    /// produce. Dead candidates stay in their immutable buffer positions but
    /// do not count toward the page width.
    #[cfg(feature = "parallel")]
    pub(crate) fn live_prefix_split(
        &self,
        start: usize,
        end: usize,
        count: usize,
    ) -> Option<usize> {
        let end = end.min(self.entries.len());
        if count == 0 || start >= end {
            return None;
        }

        let first = start / BITS;
        let last = (end - 1) / BITS;
        let mut needed = count;
        for word_index in first..=last {
            let lo = if word_index == first { start % BITS } else { 0 };
            let hi = if word_index == last {
                (end - 1) % BITS + 1
            } else {
                BITS
            };
            let mut word = self.live[word_index] & bit_range_mask(lo, hi);
            let in_word = word.count_ones() as usize;
            if in_word < needed {
                needed -= in_word;
                continue;
            }

            for _ in 1..needed {
                word &= word - 1;
            }
            let split = word_index * BITS + word.trailing_zeros() as usize + 1;
            return self
                .next_live(split)
                .is_some_and(|candidate| candidate < end)
                .then_some(split);
        }
        None
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

    /// The confirmable region from `base` onward: entry values and their
    /// parent tags paired with their killable liveness words.
    ///
    /// This is **not** a clean sub-slice: `base` lands mid-word, so the region
    /// borrows from `base / 32` and remembers `base % 32` as its bit offset.
    /// The bits below that offset in the first word — and above the last entry
    /// in the last word — belong to neighbouring regions of this same buffer.
    /// [`Candidates`] masks every access accordingly.
    pub fn region(&mut self, base: usize) -> Candidates<'_> {
        let first = base / BITS;
        Candidates {
            values: &self.entries[base..],
            parents: &self.parents[base..],
            words: &mut self.live[first..],
            bit_offset: base % BITS,
        }
    }

    /// The **live** entries of the freshly-proposed region `[base..]` as
    /// `(parent tag, value)` pairs — the form
    /// [`rewrite_region`](ProposalBuffer::rewrite_region) takes back.
    ///
    /// Killed entries are skipped, which is what makes the round trip
    /// through `rewrite_region` safe: that call republishes everything it is
    /// handed as live, so yielding the dead here would resurrect them. The
    /// buffer is kill-only, and this pair of methods is the only place that
    /// invariant could be broken — a variant is free to kill inside its own
    /// propose, and those kills must survive the region being rebuilt.
    pub fn tagged(&self, base: usize) -> impl Iterator<Item = (u32, RawInline)> + '_ {
        let live: &[u32] = &self.live;
        self.parents[base..]
            .iter()
            .copied()
            .zip(self.entries[base..].iter().copied())
            .enumerate()
            .filter_map(move |(k, pair)| {
                let i = base + k;
                ((live[i / BITS] >> (i % BITS)) & 1 != 0).then_some(pair)
            })
    }

    /// Drops entries of the freshly-proposed region `[base..]` for which
    /// `keep` returns `false`, compacting the survivors.
    ///
    /// Only a proposer may call this, and only on the region it appended
    /// in the current call, before returning — see
    /// [`rewrite_region`](ProposalBuffer::rewrite_region) for why.
    ///
    /// Packed, the survivor's liveness bit has to be *moved* from bit `read`
    /// to bit `write` rather than copied word to word. That is safe in this
    /// direction only: `write <= read` always, and bit `write` was already
    /// read on an earlier iteration, so the read-modify-write can never
    /// clobber a bit the loop still needs.
    pub fn retain_region(&mut self, base: usize, mut keep: impl FnMut(u32, &RawInline) -> bool) {
        let mut write = base;
        for read in base..self.entries.len() {
            if keep(self.parents[read], &self.entries[read]) {
                self.entries[write] = self.entries[read];
                self.parents[write] = self.parents[read];
                let bit = (self.live[read / BITS] >> (read % BITS)) & 1;
                let w = write / BITS;
                let b = write % BITS;
                self.live[w] = (self.live[w] & !(1u32 << b)) | (bit << b);
                write += 1;
            }
        }
        self.entries.truncate(write);
        self.parents.truncate(write);
        // Not `live.truncate(write)`: `write` is a *bit* index, and the word
        // holding it keeps the dropped entries' bits unless they are masked
        // off. See the tail-zero invariant on `truncate_live`.
        truncate_live(&mut self.live, write);
    }

    /// Rewrites the freshly-proposed region `[base..]` with `values`, all
    /// live, each carrying its own parent tag. Only a proposer may call
    /// this, and only on the region it appended in the current call,
    /// before returning — after that, indices are frozen because kills
    /// bind to them. Used by
    /// [`UnionConstraint`](crate::query::unionconstraint::UnionConstraint)
    /// for its per-row sort-dedup.
    pub fn rewrite_region(&mut self, base: usize, values: Vec<(u32, RawInline)>) {
        self.entries.truncate(base);
        self.parents.truncate(base);
        // Not `live.truncate(base)`: same bit-index trap as in
        // `retain_region`.
        truncate_live(&mut self.live, base);
        for (parent, value) in values {
            let i = self.entries.len();
            self.entries.push(value);
            self.parents.push(parent);
            if i % BITS == 0 {
                self.live.push(0);
            }
            self.live[i / BITS] |= 1 << (i % BITS);
        }
    }

    /// Rewrites the parent tags of the freshly-proposed region `[base..]`
    /// through `map`, which translates the sub-frontier row numbers a
    /// child wrote into this proposer's own frontier coordinates.
    ///
    /// This is the counterpart of [`Frontier::compose`](crate::query::Frontier::compose):
    /// a composite that hands a child a sub-batch gets tags in the
    /// sub-batch's numbering back and must lift them before the region
    /// reaches its own caller.
    pub fn remap_region(&mut self, base: usize, map: &[u32]) {
        for parent in &mut self.parents[base..] {
            *parent = map[*parent as usize];
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

/// A confirmable view over one proposal region: entry values and parent
/// tags read-only, liveness killable — the argument of
/// [`Constraint::confirm`](crate::query::Constraint::confirm).
///
/// Confirmers may only kill entries, never revive them, so any number of
/// confirmers writing into the same region compute their conjunction —
/// sequentially (each skipping already-dead entries) or in parallel over
/// copies merged with [`and_words`](crate::query::and_words) /
/// [`or_words`](crate::query::or_words). Index `i` refers to `values()[i]`.
///
/// A region spans a whole [`Frontier`](crate::query::Frontier): entry `i`'s
/// [`parent`](Candidates::parent) is the frontier row it was proposed for.
/// Confirmers whose verdict depends on the parent binding walk the region
/// with [`for_each_parent`](Candidates::for_each_parent); those whose
/// verdict is parent-independent (set membership, a range, a constant)
/// ignore the tags entirely and use [`retain`](Candidates::retain).
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
    parents: &'a [u32],
    words: &'a mut [u32],
    /// Bit position of entry 0 within `words[0]`; `0..32`.
    bit_offset: usize,
}

impl<'a> Candidates<'a> {
    /// The candidate values of this region.
    pub fn values(&self) -> &[RawInline] {
        self.values
    }

    /// The frontier row entry `i` was proposed for.
    pub fn parent(&self, i: usize) -> u32 {
        self.parents[i]
    }

    /// The parent tags of this region, one per entry.
    pub fn parents(&self) -> &[u32] {
        self.parents
    }

    /// Splits the region into maximal runs of equal parent tag and calls
    /// `confirm` on each with the run's own sub-region.
    ///
    /// This is how a parent-dependent confirmer amortises its per-binding
    /// setup across a batch: one setup per run instead of one per
    /// candidate. Correct for any tag order — an interleaved region just
    /// yields more, shorter runs — and each sub-region's kills land
    /// directly in this region's liveness words.
    ///
    /// Packed, a run is a *bit* range and almost never a word range: it
    /// starts at bit `bit_offset + start`, so it takes the words from
    /// `(bit_offset + start) / 32` through `(bit_offset + end - 1) / 32`
    /// and carries its own offset into the first of them. Both end words are
    /// then shared with the runs either side, which is exactly the boundary
    /// every `Candidates` method already masks for — so a run's kills stay
    /// inside the run without any further care here.
    pub fn for_each_parent(&mut self, mut confirm: impl FnMut(u32, &mut Candidates<'_>)) {
        let values = self.values;
        let parents = self.parents;
        let bit_offset = self.bit_offset;
        let mut start = 0;
        while start < parents.len() {
            let parent = parents[start];
            let mut end = start + 1;
            while end < parents.len() && parents[end] == parent {
                end += 1;
            }
            // `end > start`, so the run is non-empty and `last` is a real
            // word index; `bit_offset + end <= bit_offset + len` keeps it
            // inside `words`.
            let first = (bit_offset + start) / BITS;
            let last = (bit_offset + end - 1) / BITS;
            let mut run = Candidates {
                values: &values[start..end],
                parents: &parents[start..end],
                words: &mut self.words[first..=last],
                bit_offset: (bit_offset + start) % BITS,
            };
            confirm(parent, &mut run);
            start = end;
        }
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
        // NOT `words.fill(0)`: the first and last words are shared with the
        // neighbouring regions of the same buffer, and zeroing them wholesale
        // would kill candidates this region does not own.
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
            parents: self.parents,
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

    /// Consumes this region and divides it at candidate index `mid`, where
    /// the division falls exactly between two packed liveness words.
    ///
    /// The alignment requirement is `(bit_offset() + mid) % 32 == 0`, not
    /// merely `mid % 32 == 0`: a region may itself start in the middle of a
    /// word. At an aligned division the two returned regions borrow disjoint
    /// `&mut [u32]` slices, so separate workers may kill candidates in them
    /// concurrently without atomics, scratch masks, or a merge pass. The
    /// left region keeps this region's bit offset; the right begins at bit
    /// zero of its first word.
    ///
    /// This is deliberately crate-private. It is an ownership primitive for
    /// CPU constraint implementations, not another way for the query engine
    /// to fragment a logical proposal batch.
    ///
    /// # Panics
    ///
    /// Panics when `mid` is an endpoint or does not lie on a packed-word
    /// boundary in this region's coordinate system.
    #[cfg(feature = "parallel")]
    #[track_caller]
    pub(crate) fn split_at_word_boundary(self, mid: usize) -> (Candidates<'a>, Candidates<'a>) {
        assert!(mid > 0 && mid < self.values.len(), "split must be interior");
        let word_mid_bits = self.bit_offset + mid;
        assert!(
            word_mid_bits.is_multiple_of(BITS),
            "candidate split does not fall on a liveness-word boundary"
        );
        let word_mid = word_mid_bits / BITS;

        let Candidates {
            values,
            parents,
            words,
            bit_offset,
        } = self;
        let (left_values, right_values) = values.split_at(mid);
        let (left_parents, right_parents) = parents.split_at(mid);
        let (left_words, right_words) = words.split_at_mut(word_mid);

        (
            Candidates {
                values: left_values,
                parents: left_parents,
                words: left_words,
                bit_offset,
            },
            Candidates {
                values: right_values,
                parents: right_parents,
                words: right_words,
                bit_offset: 0,
            },
        )
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
    /// liveness. Without this a single `copy_from_slice` would overwrite up
    /// to 62 candidates belonging to other regions with whatever the caller's
    /// scratch happened to hold.
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

    /// A detached scratch region over `words` with the same values and
    /// parent tags — used by OR-composition to collect per-variant
    /// verdicts.
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
            parents: self.parents,
            words,
            bit_offset: self.bit_offset,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests.
//
// These use only the public API, so they are the *spec* of candidate liveness
// rather than a test of its internals: anything asserted here is behaviour the
// representation must have, and a replacement layout would have to pass this
// same suite unchanged. They were written as a differential suite while a
// word-per-candidate baseline still existed alongside this one, which is why
// the cases concentrate on exactly what packing makes hard: regions that start
// mid-word (so their first word is shared with a neighbour), per-parent runs
// that start mid-word inside such a region, `retain_region` / `rewrite_region`
// at non-word-aligned indices (so words have to be shifted or masked), and
// anything that hands liveness words in or out.
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

    #[cfg(feature = "parallel")]
    #[test]
    fn live_prefix_split_counts_live_entries_and_requires_a_remainder() {
        let mut b = filled(100);
        {
            let mut all = b.region(0);
            for i in [0usize, 2, 31, 32, 63, 64, 70, 98] {
                all.kill(i);
            }
        }

        assert_eq!(b.live_prefix_split(2, 99, 4), Some(7));
        assert_eq!(b.live_prefix_split(2, 8, 4), Some(7));
        assert_eq!(b.live_prefix_split(2, 7, 4), None);
        assert_eq!(b.live_prefix_split(2, 99, 0), None);
        assert_eq!(b.live_prefix_split(99, 99, 1), None);
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
            assert!(
                offset < LIVENESS_WORD_BITS,
                "bit offset {offset} is not below a word"
            );
            assert_eq!(offset, base % LIVENESS_WORD_BITS);
            assert_eq!(
                r.live_word_len(),
                (offset + r.len()).div_ceil(LIVENESS_WORD_BITS)
            );
        }
    }

    /// An aligned split is an ownership split, not a copied verdict: kills in
    /// either half land directly in the original buffer while neither half
    /// can reach the other's packed words (or the prefix below the region).
    #[cfg(feature = "parallel")]
    #[test]
    fn word_boundary_split_yields_disjoint_killable_regions() {
        let mut b = ProposalBuffer::new();
        for row in 0..5u32 {
            b.open(row);
            for i in 0..20 {
                b.push(v(row as usize * 100 + i));
            }
        }
        // Preserve a dead bit through the split as well as the live ones.
        {
            let mut all = b.region(0);
            all.kill(40);
        }
        {
            let region = b.region(5);
            // Region index 27 is absolute buffer index 32: exactly the next
            // packed-word boundary despite `27` itself not being aligned.
            let (mut left, mut right) = region.split_at_word_boundary(27);
            assert_eq!((left.len(), left.bit_offset()), (27, 5));
            assert_eq!((right.len(), right.bit_offset()), (68, 0));
            assert_eq!((left.live_word_len(), right.live_word_len()), (1, 3));
            assert_eq!(left.parent(0), 0);
            assert_eq!(left.parent(14), 0);
            assert_eq!(left.parent(15), 1);
            assert_eq!(right.parent(0), 1);
            assert_eq!(right.parent(67), 4);

            for i in (0..left.len()).step_by(2) {
                left.kill(i);
            }
            for i in (0..right.len()).step_by(3) {
                right.kill(i);
            }
        }

        for i in 0..100 {
            let want = if i < 5 {
                true
            } else if i < 32 {
                (i - 5) % 2 != 0
            } else {
                i != 40 && (i - 32) % 3 != 0
            };
            assert_eq!(b.is_live(i), want, "entry {i}");
        }
    }

    #[cfg(feature = "parallel")]
    #[test]
    #[should_panic(expected = "liveness-word boundary")]
    fn word_boundary_split_rejects_a_shared_boundary_word() {
        let mut b = filled(70);
        let region = b.region(5);
        let _ = region.split_at_word_boundary(32);
    }

    /// The reconstruction a packed device kernel performs, pinned on the host:
    /// entry `i` of a region is bit `bit_offset() + i` of the words
    /// `live_words` hands out. Getting this wrong is a silent wrong-answer
    /// bug, so spell it here — this is the *host-side contract* the kernel is
    /// written against, not a test of any kernel.
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
                let bit = offset + i;
                let reconstructed =
                    (words[bit / LIVENESS_WORD_BITS] >> (bit % LIVENESS_WORD_BITS)) & 1 != 0;
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
        let expected: Vec<usize> = (0..5).chain((5..70).filter(|i| (i - 5) % 6 == 0)).collect();
        assert_eq!(live_indices(&b), expected);
    }

    #[test]
    fn rewrite_region_drops_the_old_tails_liveness() {
        let mut b = filled(70);
        {
            let mut r = b.region(0);
            r.kill(3);
        }
        b.rewrite_region(5, vec![(0, v(100)), (0, v(101))]);
        assert_eq!(b.len(), 7);
        assert_eq!(b[5], v(100));
        assert_eq!(b[6], v(101));
        assert_eq!(b.count_live(0), 6);
        assert_eq!(live_indices(&b), vec![0, 1, 2, 4, 5, 6]);
    }

    #[test]
    fn rewrite_region_at_zero_and_at_a_word_boundary() {
        let mut b = filled(70);
        b.rewrite_region(0, vec![(0, v(1)), (0, v(2))]);
        assert_eq!(b.len(), 2);
        assert_eq!(b.count_live(0), 2);

        let mut c = filled(70);
        c.rewrite_region(32, vec![(3, v(1))]);
        assert_eq!(c.len(), 33);
        assert_eq!(c.count_live(0), 33);
        assert_eq!(c.parent_of(32), 3);
    }

    /// `rewrite_region` republishes what it is handed, so `tagged` must not
    /// hand back the dead — checked at a base that lands mid-word.
    #[test]
    fn tagged_skips_the_dead_at_a_non_aligned_base() {
        let mut b = ProposalBuffer::new();
        for row in 0..3u32 {
            b.open(row);
            for i in 0..25 {
                b.push(v(row as usize * 100 + i));
            }
        }
        {
            let mut r = b.region(5);
            for i in [0usize, 27, 40, 69] {
                r.kill(i);
            }
        }
        let fresh: Vec<(u32, RawInline)> = b.tagged(5).collect();
        assert_eq!(fresh.len(), 70 - 4);
        let dead = [5usize, 32, 45, 74];
        let want: Vec<(u32, RawInline)> = (5..75)
            .filter(|i| !dead.contains(i))
            .map(|i| ((i / 25) as u32, v((i / 25) * 100 + i % 25)))
            .collect();
        assert_eq!(fresh, want);

        b.rewrite_region(5, fresh);
        assert_eq!(b.len(), 5 + 66);
        assert_eq!(b.count_live(0), 5 + 66);
    }

    /// `retain_region` compacts survivors, which packed means *moving* each
    /// survivor's liveness bit down — including across a word boundary — and
    /// clearing the vacated tail.
    #[test]
    fn retain_region_moves_liveness_bits_down() {
        let mut b = ProposalBuffer::new();
        for row in 0..4u32 {
            b.open(row);
            for i in 0..20 {
                b.push(v(row as usize * 100 + i));
            }
        }
        // Pre-kill a few, so the compaction has to carry dead bits too.
        {
            let mut r = b.region(0);
            for i in [1usize, 34, 60, 79] {
                r.kill(i);
            }
        }
        // Keep rows 1 and 3 from index 7 onward.
        b.retain_region(7, |row, _| row % 2 == 1);
        let kept: Vec<usize> = (7..80).filter(|i| (i / 20) % 2 == 1).collect();
        assert_eq!(b.len(), 7 + kept.len());
        for (k, &source) in kept.iter().enumerate() {
            let i = 7 + k;
            assert_eq!(b[i], v((source / 20) * 100 + source % 20));
            assert_eq!(b.parent_of(i), (source / 20) as u32);
            assert_eq!(
                b.is_live(i),
                ![34usize, 60, 79].contains(&source),
                "entry {i} (from {source}) has the wrong liveness"
            );
        }
        // The prefix below the base is untouched, dead entry 1 included.
        assert_eq!(live_indices(&b)[..6], [0, 2, 3, 4, 5, 6]);
    }

    /// `retain_region` that keeps nothing must leave no stale bits behind.
    #[test]
    fn retain_region_dropping_everything_clears_the_tail() {
        let mut b = filled(70);
        b.retain_region(5, |_, _| false);
        assert_eq!(b.len(), 5);
        assert_eq!(b.count_live(0), 5);
        b.push(v(999));
        assert_eq!(b.len(), 6);
        assert_eq!(b.count_live(0), 6);
    }

    /// Per-parent runs of a region that itself starts mid-word: each run's
    /// kills must stay inside the run, so the runs either side are untouched.
    #[test]
    fn for_each_parent_runs_do_not_leak_across_their_boundaries() {
        let mut b = ProposalBuffer::new();
        for row in 0..6u32 {
            b.open(row);
            for i in 0..11 {
                b.push(v(row as usize * 100 + i));
            }
        }
        {
            let mut r = b.region(5);
            let mut seen = Vec::new();
            r.for_each_parent(|row, run| {
                seen.push((row, run.len()));
                // Kill everything in the odd rows' runs only.
                if row % 2 == 1 {
                    run.kill_all();
                }
            });
            assert_eq!(
                seen,
                vec![(0, 6), (1, 11), (2, 11), (3, 11), (4, 11), (5, 11)]
            );
        }
        let expected: Vec<usize> = (0..66).filter(|i| (i / 11) % 2 == 0).collect();
        assert_eq!(live_indices(&b), expected);
    }

    /// A run reports its own bit geometry, and a scratch over it merges
    /// word-wise with the run's own liveness.
    #[test]
    fn for_each_parent_runs_carry_their_own_bit_offset() {
        let mut b = ProposalBuffer::new();
        for row in 0..4u32 {
            b.open(row);
            for i in 0..17 {
                b.push(v(row as usize * 100 + i));
            }
        }
        {
            let mut r = b.region(3);
            let mut base = 3usize;
            r.for_each_parent(|_, run| {
                assert_eq!(run.bit_offset(), base % LIVENESS_WORD_BITS);
                assert_eq!(
                    run.live_word_len(),
                    (run.bit_offset() + run.len()).div_ceil(LIVENESS_WORD_BITS)
                );
                // Round-trip the run's own words through a scratch: kill the
                // even entries there, OR nothing else in, write back.
                let mut scratch = run.live_words();
                {
                    let mut s = run.scratch(&mut scratch);
                    for i in (0..s.len()).step_by(2) {
                        s.kill(i);
                    }
                }
                run.set_live_words(&scratch);
                base += run.len();
            });
        }
        // Region-relative starts of the four runs. The first is *short*: the
        // region's base cuts into row 0's segment, so run 0 holds 14 of its
        // 17 entries and every later run starts at an index congruent to
        // nothing in particular — which is the point.
        let starts = [0usize, 14, 31, 48];
        for i in 0..68 {
            let want = if i < 3 {
                true
            } else {
                let r = i - 3;
                let run = starts.iter().rposition(|&s| s <= r).unwrap();
                (r - starts[run]) % 2 == 1
            };
            assert_eq!(b.is_live(i), want, "entry {i}");
        }
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
