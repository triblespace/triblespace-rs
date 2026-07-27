//! Exact regular-path relations kept outside the core query solver.
//!
//! The reference implementation deliberately has one execution strategy:
//! compile graph edges and a fixed epsilon-free automaton into a product graph,
//! then maintain its reflexive transitive closure. Segment merge closes the
//! union of complete product relations, so a path may move between the same
//! segments arbitrarily often.
//!
//! This crate initially optimizes for a small, falsifiable semantic nucleus.
//! It retains every product state and reports its growth. Persistence,
//! boundary elimination, and accelerator-specific storage can be added behind
//! the same algebra once measurements justify them.

mod automaton;
mod closure;
mod constraint;
mod index;

pub use automaton::{Automaton, AutomatonError, StateId, Step, Transition};
pub use constraint::PathConstraint;
pub use index::{
    BuildStats, GraphEdge, IndexMetrics, MergeError, PathIndex, ProductPoint,
    RECTANGLE_LOG2_BUCKETS,
};
