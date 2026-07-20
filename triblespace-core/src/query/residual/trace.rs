//! Environment-gated diagnostics for residual scheduler investigations.
//!
//! This module is intentionally private and observational.  The production
//! hot path pays one cached boolean check when no trace is armed.  A traced
//! process writes bounded, power-of-two samples to stderr so progress remains
//! visible even when the first public iterator pull has not returned.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

use super::{CandidatePayload, RawInline};

static ENABLED: OnceLock<bool> = OnceLock::new();
static NEXT_QUERY: AtomicU64 = AtomicU64::new(1);
thread_local! {
    static CURRENT_QUERY: Cell<u64> = const { Cell::new(0) };
    static EVENT_COUNTS: RefCell<BTreeMap<(u64, &'static str), usize>> =
        const { RefCell::new(BTreeMap::new()) };
}

pub(crate) fn enabled() -> bool {
    *ENABLED.get_or_init(|| {
        std::env::var_os("TRIBLESPACE_RESIDUAL_TRACE")
            .is_some_and(|value| !value.is_empty() && value != std::ffi::OsStr::new("0"))
    })
}

pub(super) fn new_query() -> u64 {
    if !enabled() {
        return 0;
    }
    let query = NEXT_QUERY.fetch_add(1, Ordering::Relaxed);
    emit_for(query, format_args!("query start"));
    query
}

pub(super) struct Scope {
    previous: u64,
}

pub(super) fn enter(query: u64) -> Scope {
    let previous = CURRENT_QUERY.with(|current| current.replace(query));
    Scope { previous }
}

impl Drop for Scope {
    fn drop(&mut self) {
        CURRENT_QUERY.with(|current| current.set(self.previous));
    }
}

pub(crate) fn current_query() -> u64 {
    CURRENT_QUERY.with(Cell::get)
}

pub(crate) fn emit(args: fmt::Arguments<'_>) {
    emit_for(current_query(), args);
}

pub(super) fn emit_for(query: u64, args: fmt::Arguments<'_>) {
    if query != 0 {
        eprintln!("[residual-trace q{query}] {args}");
    }
}

/// Returns an event-local sequence number and whether this sample should be
/// printed.  The dense prefix captures startup transients; powers of two keep
/// a runaway loop observable without making stderr its dominant workload.
pub(crate) fn event(kind: &'static str) -> (usize, bool) {
    let query = current_query();
    if query == 0 {
        return (0, false);
    }
    EVENT_COUNTS.with(|counts| {
        let mut counts = counts.borrow_mut();
        let count = counts.entry((query, kind)).or_default();
        *count = count.checked_add(1).expect("residual trace event overflow");
        (*count, *count <= 16 || (*count).is_power_of_two())
    })
}

#[derive(Clone, Copy)]
pub(super) struct CandidateSummary {
    pub(super) len: usize,
    representation: &'static str,
    parent_runs: usize,
    first_parent: Option<u32>,
    last_parent: Option<u32>,
    first: Option<RawInline>,
    last: Option<RawInline>,
    ascending_within_parent: bool,
    descending_within_parent: bool,
    adjacent_duplicates: usize,
}

pub(super) fn candidates(payload: &CandidatePayload) -> CandidateSummary {
    let representation = match payload {
        CandidatePayload::Values(_) => "values",
        CandidatePayload::Tagged(_) => "tagged",
        CandidatePayload::Deferred(_) => "deferred",
    };
    let mut iter = payload.iter();
    let Some((first_parent, first)) = iter.next() else {
        return CandidateSummary {
            len: 0,
            representation,
            parent_runs: 0,
            first_parent: None,
            last_parent: None,
            first: None,
            last: None,
            ascending_within_parent: true,
            descending_within_parent: true,
            adjacent_duplicates: 0,
        };
    };

    let mut len = 1usize;
    let mut parent_runs = 1usize;
    let mut previous_parent = first_parent;
    let mut previous = first;
    let mut last_parent = first_parent;
    let mut last = first;
    let mut ascending_within_parent = true;
    let mut descending_within_parent = true;
    let mut adjacent_duplicates = 0usize;
    for (parent, value) in iter {
        len += 1;
        if parent == previous_parent {
            ascending_within_parent &= previous <= value;
            descending_within_parent &= previous >= value;
            adjacent_duplicates += usize::from(previous == value);
        } else {
            parent_runs += 1;
        }
        previous_parent = parent;
        previous = value;
        last_parent = parent;
        last = value;
    }
    debug_assert_eq!(len, payload.len());
    CandidateSummary {
        len,
        representation,
        parent_runs,
        first_parent: Some(first_parent),
        last_parent: Some(last_parent),
        first: Some(first),
        last: Some(last),
        ascending_within_parent,
        descending_within_parent,
        adjacent_duplicates,
    }
}

struct RawHex(RawInline);

impl fmt::Display for RawHex {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Display for CandidateSummary {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "len={} repr={} parent_runs={} parents={:?}..{:?} asc={} desc={} adjacent_dups={}",
            self.len,
            self.representation,
            self.parent_runs,
            self.first_parent,
            self.last_parent,
            self.ascending_within_parent,
            self.descending_within_parent,
            self.adjacent_duplicates,
        )?;
        if let (Some(first), Some(last)) = (self.first, self.last) {
            write!(formatter, " range={}..{}", RawHex(first), RawHex(last))?;
        }
        Ok(())
    }
}
