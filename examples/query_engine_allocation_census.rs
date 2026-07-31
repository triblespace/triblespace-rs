#![allow(unexpected_cfgs)]

//! Phase-scoped allocation census for the literal scalar DFS and frontier engines.
//!
//! This is deliberately an engine instrument, not a benchmark of a storage
//! backend.  Two allocation-free synthetic constraints expose the same
//! one-result lookup and 4x4 bound-star result sets to both engine generations.
//! Each is measured directly and behind a one-child `IntersectionConstraint`,
//! making the cost of an explicit identity intersection visible.
//!
//! The global allocator counts requested bytes, not allocator size classes or
//! resident memory.  Query construction, dropping an unstarted query, fetching
//! the first result, and draining the full result set get separate snapshots.
//! Result classification, exact-set gates, record storage, and printing all
//! happen outside those snapshots.

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};

use triblespace::core::inline::RawInline;
use triblespace::core::query::intersectionconstraint::IntersectionConstraint;
#[cfg(not(allocation_census_old))]
use triblespace::core::query::Frontier;
use triblespace::core::query::{
    Binding, Candidates, Constraint, ProposalBuffer, Query, VariableId, VariableSet,
};

const PROTOCOL: &str = "query-engine-allocation-census-v1";
const ENGINE_REVISION: &str = match option_env!("ALLOCATION_CENSUS_ENGINE_REVISION") {
    Some(value) => value,
    None => "unbaked",
};
const ENGINE_VARIANT: &str = match option_env!("ALLOCATION_CENSUS_ENGINE_VARIANT") {
    Some(value) => value,
    None => "unbaked",
};
const HARNESS_SHA256: &str = match option_env!("ALLOCATION_CENSUS_HARNESS_SHA256") {
    Some(value) => value,
    None => "unbaked",
};
const LOCK_SHA256: &str = match option_env!("ALLOCATION_CENSUS_LOCK_SHA256") {
    Some(value) => value,
    None => "unbaked",
};

static ALLOC_OPS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
static DEALLOC_OPS: AtomicU64 = AtomicU64::new(0);
static DEALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);

struct CountingAllocator;

impl CountingAllocator {
    #[inline]
    fn allocated(size: usize) {
        ALLOC_OPS.fetch_add(1, Ordering::Relaxed);
        ALLOCATED_BYTES.fetch_add(size as u64, Ordering::Relaxed);
    }

    #[inline]
    fn deallocated(size: usize) {
        DEALLOC_OPS.fetch_add(1, Ordering::Relaxed);
        DEALLOCATED_BYTES.fetch_add(size as u64, Ordering::Relaxed);
    }
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            Self::allocated(layout.size());
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            Self::allocated(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        Self::deallocated(layout.size());
        unsafe { System.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let replacement = unsafe { System.realloc(pointer, layout, new_size) };
        if !replacement.is_null() {
            // A successful realloc retires one requested allocation and
            // returns another.  Counting both sides makes net requested bytes
            // and operation counts compositional across phase boundaries.
            Self::deallocated(layout.size());
            Self::allocated(new_size);
        }
        replacement
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

#[derive(Clone, Copy)]
struct Snapshot {
    alloc_ops: u64,
    allocated_bytes: u64,
    dealloc_ops: u64,
    deallocated_bytes: u64,
}

impl Snapshot {
    #[inline]
    fn now() -> Self {
        Self {
            alloc_ops: ALLOC_OPS.load(Ordering::Relaxed),
            allocated_bytes: ALLOCATED_BYTES.load(Ordering::Relaxed),
            dealloc_ops: DEALLOC_OPS.load(Ordering::Relaxed),
            deallocated_bytes: DEALLOCATED_BYTES.load(Ordering::Relaxed),
        }
    }

    fn since(self, before: Self) -> AllocationDelta {
        AllocationDelta {
            alloc_ops: self.alloc_ops - before.alloc_ops,
            allocated_bytes: self.allocated_bytes - before.allocated_bytes,
            dealloc_ops: self.dealloc_ops - before.dealloc_ops,
            deallocated_bytes: self.deallocated_bytes - before.deallocated_bytes,
        }
    }
}

#[derive(Clone, Copy)]
struct AllocationDelta {
    alloc_ops: u64,
    allocated_bytes: u64,
    dealloc_ops: u64,
    deallocated_bytes: u64,
}

impl AllocationDelta {
    fn net_bytes(self) -> i128 {
        self.allocated_bytes as i128 - self.deallocated_bytes as i128
    }
}

#[derive(Clone, Copy)]
enum Shape {
    Unique,
    BoundStar4x4,
}

impl Shape {
    fn label(self) -> &'static str {
        match self {
            Self::Unique => "unique",
            Self::BoundStar4x4 => "bound-star-4x4",
        }
    }

    fn variables(self) -> VariableSet {
        let mut variables = VariableSet::new_empty();
        variables.set(0);
        if matches!(self, Self::BoundStar4x4) {
            variables.set(1);
        }
        variables
    }

    fn estimate(self, variable: VariableId) -> Option<usize> {
        match (self, variable) {
            (Self::Unique, 0) => Some(1),
            (Self::BoundStar4x4, 0 | 1) => Some(4),
            _ => None,
        }
    }

    fn values(self, variable: VariableId) -> &'static [RawInline] {
        match (self, variable) {
            (Self::Unique, 0) => &UNIQUE,
            (Self::BoundStar4x4, 0) => &STAR_LEFT,
            (Self::BoundStar4x4, 1) => &STAR_RIGHT,
            _ => &[],
        }
    }
}

#[derive(Clone, Copy)]
struct SyntheticConstraint {
    shape: Shape,
}

impl SyntheticConstraint {
    fn new(shape: Shape) -> Self {
        Self { shape }
    }
}

#[cfg(allocation_census_old)]
impl<'a> Constraint<'a> for SyntheticConstraint {
    fn variables(&self) -> VariableSet {
        self.shape.variables()
    }

    fn estimate(&self, variable: VariableId, _binding: &Binding) -> Option<usize> {
        self.shape.estimate(variable)
    }

    fn propose(&self, variable: VariableId, _binding: &Binding, proposals: &mut ProposalBuffer) {
        proposals.extend_from_slice(self.shape.values(variable));
    }

    fn confirm(&self, _variable: VariableId, _binding: &Binding, _candidates: &mut Candidates<'_>) {
    }
}

#[cfg(not(allocation_census_old))]
impl<'a> Constraint<'a> for SyntheticConstraint {
    fn variables(&self) -> VariableSet {
        self.shape.variables()
    }

    fn estimate(&self, variable: VariableId, _binding: &Binding) -> Option<usize> {
        self.shape.estimate(variable)
    }

    fn propose(
        &self,
        variable: VariableId,
        frontier: &Frontier<'_>,
        proposals: &mut ProposalBuffer,
    ) {
        let values = self.shape.values(variable);
        for parent in 0..frontier.len() {
            proposals.open(parent as u32);
            proposals.extend_from_slice(values);
        }
    }

    fn confirm(
        &self,
        _variable: VariableId,
        _frontier: &Frontier<'_>,
        _candidates: &mut Candidates<'_>,
    ) {
    }
}

const fn raw(tag: u8) -> RawInline {
    let mut value = [0; 32];
    value[0] = tag;
    value
}

const UNIQUE: [RawInline; 1] = [raw(1)];
const STAR_LEFT: [RawInline; 4] = [raw(10), raw(11), raw(12), raw(13)];
const STAR_RIGHT: [RawInline; 4] = [raw(20), raw(21), raw(22), raw(23)];

fn project_unique(binding: &Binding<'_>) -> Option<RawInline> {
    binding.get(0).copied()
}

fn project_star(binding: &Binding<'_>) -> Option<(RawInline, RawInline)> {
    Some((binding.get(0).copied()?, binding.get(1).copied()?))
}

trait ExactRow: Sized {
    const EXPECTED_ROWS: usize;
    const EXPECTED_MASK: u64;

    fn bit(self) -> Option<u64>;
}

impl ExactRow for RawInline {
    const EXPECTED_ROWS: usize = 1;
    const EXPECTED_MASK: u64 = 1;

    fn bit(self) -> Option<u64> {
        (self == UNIQUE[0]).then_some(1)
    }
}

impl ExactRow for (RawInline, RawInline) {
    const EXPECTED_ROWS: usize = 16;
    const EXPECTED_MASK: u64 = 0xffff;

    fn bit(self) -> Option<u64> {
        let left = STAR_LEFT.iter().position(|value| value == &self.0)?;
        let right = STAR_RIGHT.iter().position(|value| value == &self.1)?;
        Some(1u64 << (left * 4 + right))
    }
}

#[derive(Clone, Copy)]
struct Record {
    shape: &'static str,
    representation: &'static str,
    phase: &'static str,
    delta: AllocationDelta,
    rows: usize,
    exact_mask: u64,
}

fn measure<R, I, Make>(
    shape: Shape,
    representation: &'static str,
    mut make: Make,
    records: &mut Vec<Record>,
) where
    R: ExactRow,
    I: Iterator<Item = R>,
    Make: FnMut() -> I,
{
    let before = Snapshot::now();
    let query = black_box(make());
    let after = Snapshot::now();
    records.push(Record {
        shape: shape.label(),
        representation,
        phase: "construct",
        delta: after.since(before),
        rows: 0,
        exact_mask: 0,
    });

    let before = Snapshot::now();
    drop(query);
    let after = Snapshot::now();
    records.push(Record {
        shape: shape.label(),
        representation,
        phase: "drop-unstarted",
        delta: after.since(before),
        rows: 0,
        exact_mask: 0,
    });

    let mut query = make();
    let before = Snapshot::now();
    let first = black_box(query.next());
    let after = Snapshot::now();
    let first = first.expect("every census shape is nonempty");
    let bit = first
        .bit()
        .expect("first result is outside the exact oracle");
    records.push(Record {
        shape: shape.label(),
        representation,
        phase: "first-result",
        delta: after.since(before),
        rows: 1,
        exact_mask: bit,
    });
    drop(query);

    let mut query = make();
    let before = Snapshot::now();
    let mut rows = 0usize;
    let mut mask = 0u64;
    let mut invalid = false;
    let mut duplicate = false;
    for row in &mut query {
        rows += 1;
        match black_box(row).bit() {
            Some(bit) => {
                duplicate |= mask & bit != 0;
                mask |= bit;
            }
            None => invalid = true,
        }
    }
    let after = Snapshot::now();
    assert!(!invalid, "full drain produced a foreign row");
    assert!(!duplicate, "full drain produced a duplicate row");
    assert_eq!(rows, R::EXPECTED_ROWS, "full drain row count");
    assert_eq!(mask, R::EXPECTED_MASK, "full drain exact set");
    records.push(Record {
        shape: shape.label(),
        representation,
        phase: "full-drain",
        delta: after.since(before),
        rows,
        exact_mask: mask,
    });
    drop(query);
}

fn main() {
    assert_ne!(ENGINE_REVISION, "unbaked", "runner must bake revision");
    assert_ne!(ENGINE_VARIANT, "unbaked", "runner must bake variant");
    assert_ne!(HARNESS_SHA256, "unbaked", "runner must bake harness hash");
    assert_ne!(LOCK_SHA256, "unbaked", "runner must bake lock hash");

    // All record storage is reserved before the first allocation snapshot.
    let mut records = Vec::with_capacity(16);

    measure::<RawInline, _, _>(
        Shape::Unique,
        "direct",
        || {
            Query::new(
                SyntheticConstraint::new(Shape::Unique),
                project_unique as fn(&Binding<'_>) -> Option<RawInline>,
            )
        },
        &mut records,
    );
    measure::<RawInline, _, _>(
        Shape::Unique,
        "intersection-1",
        || {
            Query::new(
                IntersectionConstraint::new(vec![SyntheticConstraint::new(Shape::Unique)]),
                project_unique as fn(&Binding<'_>) -> Option<RawInline>,
            )
        },
        &mut records,
    );
    measure::<(RawInline, RawInline), _, _>(
        Shape::BoundStar4x4,
        "direct",
        || {
            Query::new(
                SyntheticConstraint::new(Shape::BoundStar4x4),
                project_star as fn(&Binding<'_>) -> Option<(RawInline, RawInline)>,
            )
        },
        &mut records,
    );
    measure::<(RawInline, RawInline), _, _>(
        Shape::BoundStar4x4,
        "intersection-1",
        || {
            Query::new(
                IntersectionConstraint::new(vec![SyntheticConstraint::new(Shape::BoundStar4x4)]),
                project_star as fn(&Binding<'_>) -> Option<(RawInline, RawInline)>,
            )
        },
        &mut records,
    );

    assert_eq!(records.len(), records.capacity());
    println!(
        "protocol\tengine_revision\tengine_variant\tharness_sha256\tlock_sha256\tshape\trepresentation\tphase\talloc_ops\tallocated_bytes\tdealloc_ops\tdeallocated_bytes\tnet_bytes\trows\texact_mask"
    );
    for record in records {
        println!(
            "{PROTOCOL}\t{ENGINE_REVISION}\t{ENGINE_VARIANT}\t{HARNESS_SHA256}\t{LOCK_SHA256}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{:016x}",
            record.shape,
            record.representation,
            record.phase,
            record.delta.alloc_ops,
            record.delta.allocated_bytes,
            record.delta.dealloc_ops,
            record.delta.deallocated_bytes,
            record.delta.net_bytes(),
            record.rows,
            record.exact_mask,
        );
    }
}
