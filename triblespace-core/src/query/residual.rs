//! Canonical residual-state execution.
//!
//! A bucket is identified by its remaining computation rather than its
//! history. The engine can lower any root [`Constraint`]. An exposed
//! associative AND region becomes deterministic preorder leaf occurrences; an
//! opaque root is one leaf at the empty path. Union and regular-path constraints
//! therefore remain ordinary indivisible leaves, as do custom constraints
//! unless they explicitly expose structure. An ignored conjunction may expose
//! its candidate children while retaining the wrapper as one atomic Support
//! action.
//! [`FormulaScope`] selects a chain of formula boundaries. `UnionLeaves`
//! executes exposed Unions as arbitrary finite AND/OR trees through a canonical
//! program counter and affine payload-frame stack. Candidate actions descend to
//! Atom nodes; scoped conjunction Support invokes its owner as one explicit
//! action. `WholeRoot` absorbs that scope and instead makes the maximal exposed
//! root one synthetic formula occurrence after outer
//! variable selection. It flattens only the maximal root AND region and retains
//! candidate-occurrence paging once that AND's exact remaining confirmation
//! suffix is page-local. The independent transition-program axis admits both
//! terminating finite automata and repeated least-fixpoint programs.
//!
//! Ready and Candidate descriptors are pure planning states: they estimate,
//! partition rows by a uniform semantic action, and file explicit Propose or
//! Confirm descriptors without invoking either protocol verb. The action state
//! is what calls one flattened leaf. Exact row-local variable choices are the
//! leaves of the same topology-scaled agglomerative merge hierarchy used by the
//! DAG engine; after a compatible group is reassigned, each row still chooses
//! its tightest proposer for that scheduled variable. Occupancy scheduling
//! chooses the deepest live bucket able to fill the desired actionable width;
//! if none can, it drains the minimum-rank bucket through the strict readiness
//! gate. When a full Propose or Confirm action advances to an underfilled
//! successor, an exact physical filing token keeps at most that newly appended
//! tail hot until it emits or dies. Readiness pops and planning-state splits do
//! not themselves activate a sprint, so planning-created underfill still uses
//! ordinary batch assembly. Once an action lineage is hot, however, it may
//! intentionally defer reconvergence with an older cohort in exchange for
//! first-result latency. The token is not part of canonical state identity and
//! never consumes that older cohort. Ready and
//! Propose states measure parent rows. Candidate and Confirm states remain
//! parent-atomic while any unchecked whole-group confirmer remains; once the
//! residual continuation contains only page-local confirms, they measure and
//! split candidate occurrences. Thus width one can confirm one value and
//! descend while preserving group-global Union/custom semantics at their
//! atomic boundary. Proposal remains eager for each selected parent block.
//! Execution classifies every pop as `Advanced`, `Dead`, or terminal `Emit`.
//! Lazy width is unchanged while nonempty successors advance. Once a partial
//! action activates an exact continuation cohort, that lineage outranks cold
//! siblings—even when it merges into an already-live bucket—until it emits or
//! dies. Width grows geometrically after an action dies or raw rows reach
//! projection, so a negative prefix can widen within a single pull.
//!
//! Ordered cyclic sources retain an affine cursor per activation while a
//! separate physical layer cohorts activations with the same row schema,
//! candidate mode, and cursor family. One same-schema block-native hook gets
//! ragged per-parent limits whose sum is the current global width, so batching
//! does not multiply the geometric work budget or refine canonical state
//! identity. A singleton activation inherited from a stable continuation is a
//! latency exception: its directed source/transition steps stay scalar even
//! after the global width grows. The preference still ends at its first stable
//! yield; ordinary cold harvesting remains geometrically batched.
//!
//! As with the other batched engines, flattened leaves must obey the
//! [`Constraint::estimate`] protocol: relevance is a structural answer,
//! uniform across every row with the same bound-variable schema. Constraint
//! behavior, residual shape, child ordering, and the query's planning metadata
//! must also remain unchanged for the duration of a solve. Those laws make the
//! canonical descriptor and its stored paths a total description of the future
//! computation while row values remain payload.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use indexmap::IndexSet;
use smallvec::SmallVec;

use super::*;

mod delta;
use delta::{
    ActiveDeltaContinuation, ActiveDeltaStatus, DeltaDesc, DeltaScheduler, DeltaSeedOutcome,
    DeltaStepOutcome,
};

/// A physically focused cyclic activation advances one source or transition
/// item at a time. This directed latency quantum is independent of the global
/// geometric harvest width and does not retain focus across a stable yield.
const ACTIVE_DELTA_STEP_WIDTH: usize = 1;

/// One deterministic route from the owned root to an opaque residual leaf.
#[derive(Clone, Debug, Eq, PartialEq)]
struct ConstraintPath(Box<[usize]>);

/// One structural step below a residual leaf occurrence. Connective tags make
/// the path self-describing and fail closed if a constraint changes shape
/// during a solve.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum FormulaStep {
    And(usize),
    ScopedAnd(usize),
    Or(usize),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct FormulaPath(Box<[FormulaStep]>);

/// Execution capabilities captured at one opaque formula occurrence.
#[derive(Clone, Debug, Eq, PartialEq)]
struct FormulaNodeCapabilities {
    confirm_page_local: bool,
    grouped_delta_confirm_requirements: Box<[(VariableId, VariableSet)]>,
}

impl FormulaNodeCapabilities {
    fn grouped_delta_confirm(&self, variable: VariableId, bound: VariableSet) -> bool {
        grouped_delta_confirm_is_active(&self.grouped_delta_confirm_requirements, variable, bound)
    }
}

fn compile_grouped_delta_confirm_requirements<'a>(
    constraint: &dyn Constraint<'a>,
    transition_programs: bool,
) -> Box<[(VariableId, VariableSet)]> {
    if !transition_programs {
        return Box::new([]);
    }
    constraint
        .variables()
        .into_iter()
        .filter_map(|variable| {
            constraint
                .residual_delta_confirm_grouping_requirements(variable)
                .map(|required| (variable, required))
        })
        .collect::<Vec<_>>()
        .into_boxed_slice()
}

fn grouped_delta_confirm_is_active(
    requirements: &[(VariableId, VariableSet)],
    variable: VariableId,
    bound: VariableSet,
) -> bool {
    requirements
        .iter()
        .find_map(|&(candidate, required)| (candidate == variable).then_some(required))
        .is_some_and(|required| required.is_subset_of(&bound))
}

/// Conservative proof result for publishing cyclic proposal endpoints before
/// their producer activation reaches fixpoint quiescence.
///
/// This is intentionally narrower than the set of formula contexts that might
/// admit a specialized online reducer. The probe accepts only continuations
/// whose remaining work is an empty-preserving composition of page-local AND
/// confirmations followed by the ordinary outer commit path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FormulaProposalStreamability {
    Linear,
    Barrier(FormulaProposalStreamBarrier),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FormulaProposalStreamBarrier {
    NotSyntheticRoot,
    NotProposalAction,
    OrFrame,
    NonPageLocalConfirm,
    GroupedConfirm,
    OuterContinuation,
}

/// Plan-local identity of one formula-tree occurrence. Compilation allocates a
/// fresh ID for every visit, so repeated references to one `Arc` remain
/// distinct occurrences without relying on object addresses.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct FormulaNodeId(u32);

#[derive(Clone, Debug, Eq, PartialEq)]
enum FiniteFormulaNodeKind {
    Atom,
    And { children: Box<[FormulaNodeId]> },
    Or { children: Box<[FormulaNodeId]> },
}

/// One compiled structural formula occurrence.
///
/// The spans are compiler-derived topological measures, not scheduler tuning
/// constants. `support_span` grades a Boolean consistency traversal. The
/// larger `execution_span` also reserves, for every OR arm, its support guard
/// before the arm's proposal or confirmation traversal. This is the local
/// mixed-radix layout used by the formula continuation grade.
#[derive(Clone, Debug, Eq, PartialEq)]
struct FiniteFormulaNode {
    kind: FiniteFormulaNodeKind,
    /// Candidate verbs may inspect this node's AND children, while Support
    /// remains one action on the owning semantic scope boundary.
    support_atomic: bool,
    /// Exact structural route from the enclosing residual leaf. Composite
    /// nodes retain their route because an execution capability may choose to
    /// treat that whole subtree as one opaque action boundary.
    path: FormulaPath,
    capabilities: FormulaNodeCapabilities,
    support_span: usize,
    execution_span: usize,
}

impl FiniteFormulaNode {
    fn children(&self) -> Option<&[FormulaNodeId]> {
        match &self.kind {
            FiniteFormulaNodeKind::Atom => None,
            FiniteFormulaNodeKind::And { children } | FiniteFormulaNodeKind::Or { children } => {
                Some(children)
            }
        }
    }
}

/// Structural finite-formula program compiled below lowered residual Union
/// leaves or, for the explicit root-formula probe, across one synthetic whole
/// root. Variable selection remains the outer WCO layer in either mode.
#[derive(Clone, Debug, Eq, PartialEq)]
struct FiniteFormulaProgram {
    nodes: Vec<FiniteFormulaNode>,
    /// Formula root per flattened residual leaf occurrence. Opaque leaves have
    /// no root; a lowered Union occurrence always points at an `Or` node.
    roots: Vec<Option<FormulaNodeId>>,
}

impl FiniteFormulaProgram {
    fn compile<'a>(
        root: &dyn Constraint<'a>,
        leaves: &[ResidualLeaf],
        transition_programs: bool,
        synthetic_root: bool,
    ) -> Self {
        struct Builder {
            nodes: Vec<Option<FiniteFormulaNode>>,
            transition_programs: bool,
        }

        impl Builder {
            fn reserve_node(&mut self) -> FormulaNodeId {
                let id = FormulaNodeId(
                    u32::try_from(self.nodes.len()).expect("too many residual formula nodes"),
                );
                self.nodes.push(None);
                id
            }

            fn compiled_node(&self, id: FormulaNodeId) -> &FiniteFormulaNode {
                self.nodes[id.0 as usize]
                    .as_ref()
                    .expect("formula child was not compiled")
            }

            fn spans(&self, kind: &FiniteFormulaNodeKind, support_atomic: bool) -> (usize, usize) {
                let spans = match kind {
                    FiniteFormulaNodeKind::Atom => (2, 2),
                    FiniteFormulaNodeKind::And { children } => children
                        .iter()
                        .try_fold((2usize, 2usize), |(support, execution), &child| {
                            let child = self.compiled_node(child);
                            let support = child
                                .support_span
                                .checked_add(2)
                                .and_then(|weight| support.checked_add(weight))
                                .expect("residual formula support span overflow");
                            let execution = child
                                .execution_span
                                .checked_add(2)
                                .and_then(|weight| execution.checked_add(weight))
                                .expect("residual formula execution span overflow");
                            Some((support, execution))
                        })
                        .expect("residual formula AND span overflow"),
                    FiniteFormulaNodeKind::Or { children } => children
                        .iter()
                        .try_fold((2usize, 2usize), |(support, execution), &child| {
                            let child = self.compiled_node(child);
                            let support = child
                                .support_span
                                .checked_add(2)
                                .and_then(|weight| support.checked_add(weight))
                                .expect("residual formula support span overflow");
                            let execution = child
                                .support_span
                                .checked_add(child.execution_span)
                                .and_then(|weight| weight.checked_add(3))
                                .and_then(|weight| execution.checked_add(weight))
                                .expect("residual formula guarded execution span overflow");
                            Some((support, execution))
                        })
                        .expect("residual formula OR span overflow"),
                };
                if support_atomic {
                    assert!(
                        matches!(kind, FiniteFormulaNodeKind::And { .. }),
                        "only a conjunction can retain an atomic Support boundary"
                    );
                    (2, spans.1)
                } else {
                    spans
                }
            }

            fn compile_node<'a>(
                &mut self,
                constraint: &dyn Constraint<'a>,
                path: &mut Vec<FormulaStep>,
            ) -> FormulaNodeId {
                let id = self.reserve_node();
                let node_path = FormulaPath(path.clone().into_boxed_slice());
                let capabilities = FormulaNodeCapabilities {
                    confirm_page_local: constraint.residual_confirm_is_page_local(),
                    grouped_delta_confirm_requirements: compile_grouped_delta_confirm_requirements(
                        constraint,
                        self.transition_programs,
                    ),
                };
                let (kind, support_atomic) =
                    if let Some(children) = constraint.residual_union_children() {
                        assert!(
                            children.len() > 0,
                            "a residual finite union must expose at least one arm"
                        );
                        let mut compiled = Vec::with_capacity(children.len());
                        self.compile_or_children(constraint, path, &mut compiled);
                        (
                            FiniteFormulaNodeKind::Or {
                                children: compiled.into_boxed_slice(),
                            },
                            false,
                        )
                    } else {
                        match constraint.residual_shape() {
                            ConstraintShape::And(children) => {
                                let mut compiled = Vec::with_capacity(children.len());
                                for child in 0..children.len() {
                                    path.push(FormulaStep::And(child));
                                    compiled.push(self.compile_node(children.child(child), path));
                                    path.pop();
                                }
                                (
                                    FiniteFormulaNodeKind::And {
                                        children: compiled.into_boxed_slice(),
                                    },
                                    false,
                                )
                            }
                            ConstraintShape::ScopedAnd(children) => {
                                let mut compiled = Vec::with_capacity(children.len());
                                for child in 0..children.len() {
                                    path.push(FormulaStep::ScopedAnd(child));
                                    compiled.push(self.compile_node(children.child(child), path));
                                    path.pop();
                                }
                                (
                                    FiniteFormulaNodeKind::And {
                                        children: compiled.into_boxed_slice(),
                                    },
                                    true,
                                )
                            }
                            ConstraintShape::Opaque => (FiniteFormulaNodeKind::Atom, false),
                        }
                    };
                let (support_span, execution_span) = self.spans(&kind, support_atomic);
                self.nodes[id.0 as usize] = Some(FiniteFormulaNode {
                    kind,
                    support_atomic,
                    path: node_path,
                    capabilities,
                    support_span,
                    execution_span,
                });
                id
            }

            /// Compiles one synthetic whole-query root. Only the maximal
            /// exposed AND region at the root is flattened: an AND below an
            /// OR remains inside that arm, preserving the union's private
            /// candidate reducer boundary.
            fn compile_root<'a>(&mut self, root: &dyn Constraint<'a>) -> FormulaNodeId {
                if root.residual_union_children().is_some() {
                    return self.compile_node(root, &mut Vec::new());
                }
                let ConstraintShape::And(_) = root.residual_shape() else {
                    return self.compile_node(root, &mut Vec::new());
                };

                let id = self.reserve_node();
                let capabilities = FormulaNodeCapabilities {
                    confirm_page_local: root.residual_confirm_is_page_local(),
                    grouped_delta_confirm_requirements: compile_grouped_delta_confirm_requirements(
                        root,
                        self.transition_programs,
                    ),
                };
                let mut children = Vec::new();
                self.compile_root_and_children(root, &mut Vec::new(), &mut children);
                let kind = FiniteFormulaNodeKind::And {
                    children: children.into_boxed_slice(),
                };
                let (support_span, execution_span) = self.spans(&kind, false);
                self.nodes[id.0 as usize] = Some(FiniteFormulaNode {
                    kind,
                    support_atomic: false,
                    path: FormulaPath(Box::new([])),
                    capabilities,
                    support_span,
                    execution_span,
                });
                id
            }

            fn compile_root_and_children<'a>(
                &mut self,
                conjunction: &dyn Constraint<'a>,
                path: &mut Vec<FormulaStep>,
                compiled: &mut Vec<FormulaNodeId>,
            ) {
                let ConstraintShape::And(children) = conjunction.residual_shape() else {
                    panic!("synthetic root AND collection entered an opaque constraint")
                };
                for child in 0..children.len() {
                    path.push(FormulaStep::And(child));
                    let constraint = children.child(child);
                    if constraint.residual_union_children().is_none()
                        && matches!(constraint.residual_shape(), ConstraintShape::And(_))
                    {
                        self.compile_root_and_children(constraint, path, compiled);
                    } else {
                        compiled.push(self.compile_node(constraint, path));
                    }
                    path.pop();
                }
            }

            /// Union is associative in the constraint language. Compile one
            /// canonical flat child set across directly nested ORs, while a
            /// connective change (notably AND) remains a terminal node in
            /// this region. Paths retain every original structural step.
            fn compile_or_children<'a>(
                &mut self,
                union: &dyn Constraint<'a>,
                path: &mut Vec<FormulaStep>,
                compiled: &mut Vec<FormulaNodeId>,
            ) {
                let children = union
                    .residual_union_children()
                    .expect("formula OR collection entered an opaque constraint");
                assert!(
                    children.len() > 0,
                    "a residual finite union must expose at least one arm"
                );
                for child in 0..children.len() {
                    path.push(FormulaStep::Or(child));
                    let constraint = children.child(child);
                    if constraint.residual_union_children().is_some() {
                        self.compile_or_children(constraint, path, compiled);
                    } else {
                        compiled.push(self.compile_node(constraint, path));
                    }
                    path.pop();
                }
            }
        }

        fn resolve_leaf<'r, 'a>(
            root: &'r dyn Constraint<'a>,
            leaf: &ResidualLeaf,
        ) -> &'r dyn Constraint<'a> {
            let mut constraint = root;
            for &child in leaf.path.0.iter() {
                constraint = match constraint.residual_shape() {
                    ConstraintShape::And(children) | ConstraintShape::ScopedAnd(children) => {
                        children.child(child)
                    }
                    ConstraintShape::Opaque => {
                        panic!("residual AND shape changed during formula compilation")
                    }
                };
            }
            constraint
        }

        let mut builder = Builder {
            nodes: Vec::new(),
            transition_programs,
        };
        if synthetic_root {
            assert_eq!(
                leaves.len(),
                1,
                "a synthetic residual formula has one outer occurrence"
            );
            let root = builder.compile_root(root);
            return Self {
                nodes: builder
                    .nodes
                    .into_iter()
                    .map(|node| node.expect("reserved residual formula node was not compiled"))
                    .collect(),
                roots: vec![Some(root)],
            };
        }
        let mut roots = vec![None; leaves.len()];
        for (occurrence, leaf) in leaves.iter().enumerate() {
            if leaf.lowering != LeafLowering::FiniteFormula {
                continue;
            }
            let constraint = resolve_leaf(root, leaf);
            assert!(
                constraint.residual_union_children().is_some(),
                "a finite-formula root stopped being a Union"
            );
            roots[occurrence] = Some(builder.compile_node(constraint, &mut Vec::new()));
        }
        Self {
            nodes: builder
                .nodes
                .into_iter()
                .map(|node| node.expect("reserved residual formula node was not compiled"))
                .collect(),
            roots,
        }
    }

    fn node(&self, id: FormulaNodeId) -> &FiniteFormulaNode {
        &self.nodes[id.0 as usize]
    }

    fn root(&self, occurrence: usize) -> Option<FormulaNodeId> {
        self.roots[occurrence]
    }

    fn max_root_span(&self) -> usize {
        self.roots
            .iter()
            .flatten()
            .map(|&root| self.node(root).execution_span)
            .max()
            .unwrap_or(0)
    }

    fn node_span(&self, node: FormulaNodeId, stage: FormulaStage) -> usize {
        let node = self.node(node);
        match stage {
            FormulaStage::Support => node.support_span,
            FormulaStage::Propose | FormulaStage::Confirm => node.execution_span,
        }
    }

    fn child_weight(
        &self,
        parent: FormulaNodeId,
        stage: FormulaStage,
        child: FormulaNodeId,
    ) -> usize {
        let child = self.node(child);
        match (stage, &self.node(parent).kind) {
            (FormulaStage::Support, _) => child
                .support_span
                .checked_add(2)
                .expect("residual formula support child weight overflow"),
            (FormulaStage::Propose | FormulaStage::Confirm, FiniteFormulaNodeKind::And { .. }) => {
                child
                    .execution_span
                    .checked_add(2)
                    .expect("residual formula AND child weight overflow")
            }
            (FormulaStage::Propose | FormulaStage::Confirm, FiniteFormulaNodeKind::Or { .. }) => {
                child
                    .support_span
                    .checked_add(child.execution_span)
                    .and_then(|weight| weight.checked_add(3))
                    .expect("residual formula guarded child weight overflow")
            }
            (_, FiniteFormulaNodeKind::Atom) => {
                panic!("an Atom cannot own a formula child weight")
            }
        }
    }

    fn completed_weight(&self, node: FormulaNodeId, stage: FormulaStage, done: &ChildSet) -> usize {
        let children = self
            .node(node)
            .children()
            .expect("an Atom cannot have finite child progress");
        assert!(
            done.is_valid_for(children.len()),
            "residual formula progress names a non-child occurrence"
        );
        children
            .iter()
            .enumerate()
            .filter(|(child, _)| done.contains(*child))
            .try_fold(0usize, |grade, (_, &child)| {
                grade.checked_add(self.child_weight(node, stage, child))
            })
            .expect("residual formula grade overflow")
    }

    fn entry_focus(&self, node: FormulaNodeId, stage: FormulaStage) -> FormulaFocus {
        let compiled = self.node(node);
        if stage == FormulaStage::Support && compiled.support_atomic {
            return FormulaFocus::Action { node, stage };
        }
        match &compiled.kind {
            FiniteFormulaNodeKind::Atom => FormulaFocus::Action { node, stage },
            FiniteFormulaNodeKind::And { children } | FiniteFormulaNodeKind::Or { children } => {
                FormulaFocus::Plan {
                    node,
                    stage,
                    done: ChildSet::empty(children.len()),
                }
            }
        }
    }

    #[cfg(test)]
    fn start(
        &self,
        variable: VariableId,
        occurrence: usize,
        verb: UnionVerb,
    ) -> FormulaProgramCounter {
        let root = self
            .root(occurrence)
            .expect("an opaque residual leaf has no finite formula program");
        let stage = match &verb {
            UnionVerb::Propose { .. } => FormulaStage::Propose,
            UnionVerb::Confirm { .. } => FormulaStage::Confirm,
        };
        FormulaProgramCounter {
            focus: self.entry_focus(root, stage),
            returns: Box::new([]),
            resume: FormulaOuterResume {
                variable,
                occurrence,
                verb,
            },
        }
    }

    #[cfg(test)]
    fn select_child(&self, counter: &FormulaProgramCounter, child: usize) -> FormulaProgramCounter {
        let FormulaFocus::Plan { stage, .. } = counter.focus else {
            panic!("only a residual formula Plan can select a child")
        };
        self.select_child_with(
            counter,
            child,
            FormulaReturnKind::Child,
            stage,
            |program, node, stage| program.entry_focus(node, stage),
        )
    }

    /// Selects a child as one opaque protocol action even when its compiled
    /// kind is composite. Recursive execution uses this only for Atom nodes;
    /// retaining the explicit operation keeps compiler control tests able to
    /// describe a deliberately opaque structural boundary.
    #[cfg(test)]
    fn select_child_as_action(
        &self,
        counter: &FormulaProgramCounter,
        child: usize,
    ) -> FormulaProgramCounter {
        let FormulaFocus::Plan { stage, .. } = counter.focus else {
            panic!("only a residual formula Plan can select a child")
        };
        self.select_child_with(
            counter,
            child,
            FormulaReturnKind::Child,
            stage,
            |_program, node, stage| FormulaFocus::Action { node, stage },
        )
    }

    /// Starts the Boolean support traversal that guards one unfinished OR arm.
    #[cfg(test)]
    fn guard_child(&self, counter: &FormulaProgramCounter, child: usize) -> FormulaProgramCounter {
        let FormulaFocus::Plan { node, stage, .. } = counter.focus else {
            panic!("only a residual formula Plan can guard a child")
        };
        assert!(matches!(
            self.node(node).kind,
            FiniteFormulaNodeKind::Or { .. }
        ));
        assert_ne!(stage, FormulaStage::Support);
        self.select_child_with(
            counter,
            child,
            FormulaReturnKind::Guard,
            FormulaStage::Support,
            |program, node, stage| program.entry_focus(node, stage),
        )
    }

    /// Enters an OR arm after its support guard returned true.
    #[cfg(test)]
    fn select_supported_child(
        &self,
        counter: &FormulaProgramCounter,
        child: usize,
    ) -> FormulaProgramCounter {
        let FormulaFocus::Plan { node, stage, .. } = counter.focus else {
            panic!("only a residual formula Plan can select a supported child")
        };
        assert!(matches!(
            self.node(node).kind,
            FiniteFormulaNodeKind::Or { .. }
        ));
        assert_ne!(stage, FormulaStage::Support);
        self.select_child_with(
            counter,
            child,
            FormulaReturnKind::Child,
            stage,
            |program, node, stage| program.entry_focus(node, stage),
        )
    }

    #[cfg(test)]
    fn select_child_with(
        &self,
        counter: &FormulaProgramCounter,
        child: usize,
        kind: FormulaReturnKind,
        child_stage: FormulaStage,
        focus: impl FnOnce(&Self, FormulaNodeId, FormulaStage) -> FormulaFocus,
    ) -> FormulaProgramCounter {
        let FormulaFocus::Plan { node, stage, done } = &counter.focus else {
            panic!("only a residual formula Plan can select a child")
        };
        let children = self
            .node(*node)
            .children()
            .expect("a residual formula Plan named an Atom");
        assert!(child < children.len() && !done.contains(child));
        let mut returns = counter.returns.to_vec();
        returns.push(FormulaReturnSite {
            kind,
            parent: *node,
            parent_stage: *stage,
            child,
            done: done.clone(),
        });
        FormulaProgramCounter {
            focus: focus(self, children[child], child_stage),
            returns: returns.into_boxed_slice(),
            resume: counter.resume.clone(),
        }
    }

    /// Marks one structurally dead or irrelevant child complete without
    /// claiming that an AND proposer ran. Stage is canonical control state,
    /// not something inferred from whether the done mask happens to be empty.
    #[cfg(test)]
    fn skip_child(&self, counter: &FormulaProgramCounter, child: usize) -> FormulaProgramCounter {
        let FormulaFocus::Plan { node, stage, done } = &counter.focus else {
            panic!("only a residual formula Plan can skip a child")
        };
        let children = self
            .node(*node)
            .children()
            .expect("a residual formula Plan named an Atom");
        assert!(child < children.len() && !done.contains(child));
        FormulaProgramCounter {
            focus: FormulaFocus::Plan {
                node: *node,
                stage: *stage,
                done: done.with_inserted(child),
            },
            returns: counter.returns.clone(),
            resume: counter.resume.clone(),
        }
    }

    #[cfg(test)]
    fn complete(&self, counter: &FormulaProgramCounter) -> FormulaProgramCounter {
        let (node, stage) = match &counter.focus {
            FormulaFocus::Action { node, stage } => (*node, *stage),
            FormulaFocus::Plan { node, stage, done } => {
                let children = self
                    .node(*node)
                    .children()
                    .expect("a residual formula Plan named an Atom");
                assert_eq!(
                    done.count(),
                    children.len(),
                    "a residual formula completed with live children"
                );
                (*node, *stage)
            }
            FormulaFocus::Complete { .. } => {
                panic!("a completed residual formula was completed twice")
            }
        };
        FormulaProgramCounter {
            focus: FormulaFocus::Complete { node, stage },
            returns: counter.returns.clone(),
            resume: counter.resume.clone(),
        }
    }

    /// Completes a Boolean connective as soon as its annihilator is known.
    /// The decisive truth value is transition-local; the canonical Complete PC
    /// deliberately erases which child established it.
    #[cfg(test)]
    fn complete_support_short_circuit(
        &self,
        counter: &FormulaProgramCounter,
        truth: bool,
    ) -> FormulaProgramCounter {
        let FormulaFocus::Plan {
            node,
            stage: FormulaStage::Support,
            ..
        } = counter.focus
        else {
            panic!("only a support Plan can short-circuit")
        };
        assert!(matches!(
            (&self.node(node).kind, truth),
            (FiniteFormulaNodeKind::And { .. }, false) | (FiniteFormulaNodeKind::Or { .. }, true)
        ));
        FormulaProgramCounter {
            focus: FormulaFocus::Complete {
                node,
                stage: FormulaStage::Support,
            },
            returns: counter.returns.clone(),
            resume: counter.resume.clone(),
        }
    }

    #[cfg(test)]
    fn resume(&self, counter: &FormulaProgramCounter) -> FormulaSuccessor {
        let FormulaFocus::Complete {
            node: completed,
            stage: completed_stage,
        } = counter.focus
        else {
            panic!("only a completed residual formula can return")
        };
        let Some((site, outer)) = counter.returns.split_last() else {
            assert_ne!(
                completed_stage,
                FormulaStage::Support,
                "a support traversal must return to a formula guard"
            );
            assert_eq!(self.root(counter.resume.occurrence), Some(completed));
            let root_stage = match (&self.node(completed).kind, &counter.resume.verb) {
                (FiniteFormulaNodeKind::And { .. }, UnionVerb::Propose { .. }) => {
                    FormulaStage::Confirm
                }
                (_, UnionVerb::Propose { .. }) => FormulaStage::Propose,
                (_, UnionVerb::Confirm { .. }) => FormulaStage::Confirm,
            };
            assert_eq!(completed_stage, root_stage);
            return FormulaSuccessor::Outer(counter.resume.clone());
        };
        let children = self
            .node(site.parent)
            .children()
            .expect("a residual formula return site named an Atom parent");
        assert_eq!(children[site.child], completed);
        if site.kind == FormulaReturnKind::Guard {
            assert_eq!(completed_stage, FormulaStage::Support);
            assert_ne!(site.parent_stage, FormulaStage::Support);
            assert!(matches!(
                self.node(site.parent).kind,
                FiniteFormulaNodeKind::Or { .. }
            ));
            return FormulaSuccessor::Guard {
                parent: FormulaProgramCounter {
                    focus: FormulaFocus::Plan {
                        node: site.parent,
                        stage: site.parent_stage,
                        done: site.done.clone(),
                    },
                    returns: outer.to_vec().into_boxed_slice(),
                    resume: counter.resume.clone(),
                },
                child: site.child,
            };
        }
        let done = site.done.with_inserted(site.child);
        let stage = match (&self.node(site.parent).kind, site.parent_stage) {
            (FiniteFormulaNodeKind::And { .. }, FormulaStage::Propose) => FormulaStage::Confirm,
            _ => site.parent_stage,
        };
        FormulaSuccessor::Formula(FormulaProgramCounter {
            focus: FormulaFocus::Plan {
                node: site.parent,
                stage,
                done,
            },
            returns: outer.to_vec().into_boxed_slice(),
            resume: counter.resume.clone(),
        })
    }

    /// Compiler-derived, history-independent topological grade for one exact
    /// structural continuation. Every control transition above strictly
    /// increases this value, including adaptive child orders.
    #[cfg(test)]
    fn grade(&self, counter: &FormulaProgramCounter) -> usize {
        let root = self
            .root(counter.resume.occurrence)
            .expect("a formula counter resumed an opaque residual leaf");
        let mut expected = root;
        let mut outer_grade = 0usize;
        for site in counter.returns.iter() {
            assert_eq!(site.parent, expected);
            let children = self
                .node(site.parent)
                .children()
                .expect("a formula return site named an Atom parent");
            assert!(site.child < children.len() && !site.done.contains(site.child));
            let child = children[site.child];
            let entry_offset = match site.kind {
                FormulaReturnKind::Guard => {
                    assert_ne!(site.parent_stage, FormulaStage::Support);
                    assert!(matches!(
                        self.node(site.parent).kind,
                        FiniteFormulaNodeKind::Or { .. }
                    ));
                    2
                }
                FormulaReturnKind::Child
                    if site.parent_stage != FormulaStage::Support
                        && matches!(
                            self.node(site.parent).kind,
                            FiniteFormulaNodeKind::Or { .. }
                        ) =>
                {
                    self.node(child)
                        .support_span
                        .checked_add(3)
                        .expect("residual guarded child entry overflow")
                }
                FormulaReturnKind::Child => 2,
            };
            outer_grade = outer_grade
                .checked_add(entry_offset)
                .and_then(|grade| {
                    self.completed_weight(site.parent, site.parent_stage, &site.done)
                        .checked_add(grade)
                })
                .expect("residual formula grade overflow");
            expected = child;
        }

        let (focused, local_grade) = match &counter.focus {
            FormulaFocus::Action { node, .. } => (*node, 1),
            FormulaFocus::Plan {
                node, stage, done, ..
            } => (
                *node,
                self.completed_weight(*node, *stage, done)
                    .checked_add(1)
                    .expect("residual formula grade overflow"),
            ),
            FormulaFocus::Complete { node, stage } => (*node, self.node_span(*node, *stage)),
        };
        assert_eq!(focused, expected);
        outer_grade
            .checked_add(local_grade)
            .expect("residual formula grade overflow")
    }

    /// Whether the active synthetic-root continuation is the exact analogue
    /// of an outer Candidate state whose entire remaining confirmation suffix
    /// is page-local. Only a maximal root AND may expose candidate pages. OR
    /// reducers and nested formula frames retain complete parent groups.
    #[cfg(test)]
    fn root_confirm_suffix_accepts_pages(
        &self,
        counter: &FormulaProgramCounter,
        bound: VariableSet,
    ) -> bool {
        let root = self
            .root(counter.resume.occurrence)
            .expect("a formula counter resumed an opaque residual leaf");
        let FiniteFormulaNodeKind::And { children } = &self.node(root).kind else {
            return false;
        };

        let done = match &counter.focus {
            FormulaFocus::Plan {
                node,
                stage: FormulaStage::Confirm,
                done,
            } if *node == root && counter.returns.is_empty() => done,
            FormulaFocus::Action {
                node,
                stage: FormulaStage::Confirm,
            } if counter.returns.len() == 1 => {
                let site = &counter.returns[0];
                if site.kind != FormulaReturnKind::Child
                    || site.parent != root
                    || site.parent_stage != FormulaStage::Confirm
                    || children[site.child] != *node
                {
                    return false;
                }
                &site.done
            }
            _ => return false,
        };

        children
            .iter()
            .enumerate()
            .filter(|(child, _)| !done.contains(*child))
            .all(|(_, &child)| {
                let node = self.node(child);
                matches!(node.kind, FiniteFormulaNodeKind::Atom)
                    && node.capabilities.confirm_page_local
                    && !node
                        .capabilities
                        .grouped_delta_confirm(counter.resume.variable, bound)
            })
    }

    /// Proves that one focused proposal can be distributed over accepted
    /// endpoint chunks without changing the formula continuation's bag
    /// semantics. Every ancestor must be AND, and every sibling that remains
    /// after the focused child must itself be an AND-only tree of page-local,
    /// non-grouped confirmers.
    #[cfg(test)]
    fn proposal_streamability(
        &self,
        counter: &FormulaProgramCounter,
        bound: VariableSet,
    ) -> FormulaProposalStreamability {
        let focused = match counter.focus {
            FormulaFocus::Action {
                node,
                stage: FormulaStage::Propose,
            } => node,
            _ => {
                return FormulaProposalStreamability::Barrier(
                    FormulaProposalStreamBarrier::NotProposalAction,
                );
            }
        };
        if !matches!(self.node(focused).kind, FiniteFormulaNodeKind::Atom) {
            return FormulaProposalStreamability::Barrier(
                FormulaProposalStreamBarrier::NotProposalAction,
            );
        }

        let mut completed = focused;
        for site in counter.returns.iter().rev() {
            if site.kind != FormulaReturnKind::Child || site.parent_stage != FormulaStage::Propose {
                return FormulaProposalStreamability::Barrier(
                    FormulaProposalStreamBarrier::NotProposalAction,
                );
            }
            let parent = self.node(site.parent);
            let FiniteFormulaNodeKind::And { children } = &parent.kind else {
                return FormulaProposalStreamability::Barrier(
                    FormulaProposalStreamBarrier::OrFrame,
                );
            };
            assert_eq!(children[site.child], completed);
            for (child, &node) in children.iter().enumerate() {
                if child == site.child || site.done.contains(child) {
                    continue;
                }
                let streamability =
                    self.confirm_subtree_streamability(node, counter.resume.variable, bound);
                if streamability != FormulaProposalStreamability::Linear {
                    return streamability;
                }
            }
            completed = site.parent;
        }
        assert_eq!(
            self.root(counter.resume.occurrence),
            Some(completed),
            "formula proposal return stack did not reach its root"
        );
        FormulaProposalStreamability::Linear
    }

    fn confirm_subtree_streamability(
        &self,
        node: FormulaNodeId,
        variable: VariableId,
        bound: VariableSet,
    ) -> FormulaProposalStreamability {
        let node = self.node(node);
        match &node.kind {
            FiniteFormulaNodeKind::Atom => {
                if node.capabilities.grouped_delta_confirm(variable, bound) {
                    FormulaProposalStreamability::Barrier(
                        FormulaProposalStreamBarrier::GroupedConfirm,
                    )
                } else if !node.capabilities.confirm_page_local {
                    FormulaProposalStreamability::Barrier(
                        FormulaProposalStreamBarrier::NonPageLocalConfirm,
                    )
                } else {
                    FormulaProposalStreamability::Linear
                }
            }
            FiniteFormulaNodeKind::And { children } => children
                .iter()
                .find_map(|&child| {
                    match self.confirm_subtree_streamability(child, variable, bound) {
                        FormulaProposalStreamability::Linear => None,
                        barrier => Some(barrier),
                    }
                })
                .unwrap_or(FormulaProposalStreamability::Linear),
            FiniteFormulaNodeKind::Or { .. } => {
                FormulaProposalStreamability::Barrier(FormulaProposalStreamBarrier::OrFrame)
            }
        }
    }

    /// Root paging proof. A root Plan reads no return edge; its directly
    /// selected Action reads exactly one edge to recover the root done mask.
    fn interned_root_confirm_suffix_accepts_pages(
        &self,
        formula_pcs: &FormulaPcInterner,
        counter: FormulaPcId,
        bound: VariableSet,
    ) -> bool {
        let resume = formula_pcs.resume(counter);
        let root = self
            .root(resume.occurrence)
            .expect("a formula counter resumed an opaque residual leaf");
        let FiniteFormulaNodeKind::And { children } = &self.node(root).kind else {
            return false;
        };

        let record = formula_pcs.get(counter);
        let done = match &record.focus {
            FormulaFocus::Plan {
                node,
                stage: FormulaStage::Confirm,
                done,
            } if *node == root && record.return_to.is_none() => done,
            FormulaFocus::Action {
                node,
                stage: FormulaStage::Confirm,
            } => {
                let Some(return_to) = record.return_to else {
                    return false;
                };
                let address = formula_pcs.return_by_id(return_to);
                if address.kind != FormulaReturnKind::Child {
                    return false;
                }
                let parent = formula_pcs.get(address.parent);
                let FormulaFocus::Plan {
                    node: parent_node,
                    stage: FormulaStage::Confirm,
                    done,
                } = &parent.focus
                else {
                    return false;
                };
                if *parent_node != root
                    || parent.return_to.is_some()
                    || children[address.child] != *node
                {
                    return false;
                }
                done
            }
            _ => return false,
        };

        children
            .iter()
            .enumerate()
            .filter(|(child, _)| !done.contains(*child))
            .all(|(_, &child)| {
                let node = self.node(child);
                matches!(node.kind, FiniteFormulaNodeKind::Atom)
                    && node.capabilities.confirm_page_local
                    && !node
                        .capabilities
                        .grouped_delta_confirm(resume.variable, bound)
            })
    }

    /// Delta-proposal paging proof. This is the sole production operation
    /// that intentionally inspects the full persistent ancestry: it follows
    /// exactly one canonical parent edge per formula nesting level when an
    /// action is considered for delta seeding. Ordinary transitions, PC
    /// hashing, state filing, rank lookup, and resume never perform this walk.
    fn interned_proposal_streamability(
        &self,
        formula_pcs: &FormulaPcInterner,
        counter: FormulaPcId,
        bound: VariableSet,
    ) -> FormulaProposalStreamability {
        let focused = match formula_pcs.get(counter).focus {
            FormulaFocus::Action {
                node,
                stage: FormulaStage::Propose,
            } => node,
            _ => {
                return FormulaProposalStreamability::Barrier(
                    FormulaProposalStreamBarrier::NotProposalAction,
                );
            }
        };
        if !matches!(self.node(focused).kind, FiniteFormulaNodeKind::Atom) {
            return FormulaProposalStreamability::Barrier(
                FormulaProposalStreamBarrier::NotProposalAction,
            );
        }

        let resume = formula_pcs.resume(counter);
        let mut completed = focused;
        let mut current = counter;
        while let Some(return_to) = formula_pcs.get(current).return_to {
            let address = formula_pcs.return_by_id(return_to);
            if address.kind != FormulaReturnKind::Child {
                return FormulaProposalStreamability::Barrier(
                    FormulaProposalStreamBarrier::NotProposalAction,
                );
            }
            let parent_record = formula_pcs.get(address.parent);
            let FormulaFocus::Plan {
                node: parent_node,
                stage: FormulaStage::Propose,
                done,
            } = &parent_record.focus
            else {
                return FormulaProposalStreamability::Barrier(
                    FormulaProposalStreamBarrier::NotProposalAction,
                );
            };
            let parent = self.node(*parent_node);
            let FiniteFormulaNodeKind::And { children } = &parent.kind else {
                return FormulaProposalStreamability::Barrier(
                    FormulaProposalStreamBarrier::OrFrame,
                );
            };
            assert_eq!(children[address.child], completed);
            for (child, &node) in children.iter().enumerate() {
                if child == address.child || done.contains(child) {
                    continue;
                }
                let streamability =
                    self.confirm_subtree_streamability(node, resume.variable, bound);
                if streamability != FormulaProposalStreamability::Linear {
                    return streamability;
                }
            }
            completed = *parent_node;
            current = address.parent;
        }
        assert_eq!(
            self.root(resume.occurrence),
            Some(completed),
            "formula proposal return stack did not reach its root"
        );
        FormulaProposalStreamability::Linear
    }
}

/// Exact structural return address for one running formula child.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum FormulaReturnKind {
    /// The child contributes its candidates or Boolean support result to the
    /// enclosing connective.
    Child,
    /// The child is being checked for support before an OR arm may execute.
    Guard,
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct FormulaReturnSite {
    kind: FormulaReturnKind,
    parent: FormulaNodeId,
    parent_stage: FormulaStage,
    child: usize,
    done: ChildSet,
}

/// Effective protocol role at the focused formula node. It is explicit
/// because dead-child progress and action history are different facts: an AND
/// can have a nonempty done mask and still need its one proposer, or an empty
/// mask while already filtering as a parent confirmer.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum FormulaStage {
    /// Boolean consistency of one formula subtree under the committed parent
    /// bindings. Truth is consumed before another state is filed.
    Support,
    Propose,
    Confirm,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum FormulaFocus {
    Action {
        node: FormulaNodeId,
        stage: FormulaStage,
    },
    Plan {
        node: FormulaNodeId,
        stage: FormulaStage,
        done: ChildSet,
    },
    Complete {
        node: FormulaNodeId,
        stage: FormulaStage,
    },
}

/// Existing residual continuation to resume after the formula root completes.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct FormulaOuterResume {
    variable: VariableId,
    occurrence: usize,
    verb: UnionVerb,
}

/// Defunctionalized structural continuation. Candidate values are deliberately
/// absent: equality means identical future computation, while each affine
/// activation will carry originals, working sets, and accumulators in payload.
#[cfg(test)]
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct FormulaProgramCounter {
    focus: FormulaFocus,
    returns: Box<[FormulaReturnSite]>,
    resume: FormulaOuterResume,
}

/// Query-local canonical name for one immutable formula continuation.
///
/// This is an arena-local name, not a portable content identifier. A cloned
/// machine initially preserves the same prefix, then owns an independent
/// namespace; descriptors and payload are never exchanged between machines
/// after their arenas diverge.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct FormulaPcId(u32);

/// Query-local canonical name for the outer WCO continuation shared by every
/// formula state in one activation family.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct FormulaResumeId(u32);

/// Query-local canonical name for one immutable return address. The nonzero
/// representation leaves `None` as the root-stack marker without enlarging a
/// compact program-counter record.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct FormulaReturnId(NonZeroU32);

impl FormulaReturnId {
    fn from_index(index: usize) -> Self {
        let raw = u32::try_from(index)
            .expect("too many residual formula return addresses")
            .checked_add(1)
            .expect("too many residual formula return addresses");
        Self(NonZeroU32::new(raw).expect("formula return address is nonzero"))
    }

    fn index(self) -> usize {
        (self.0.get() - 1) as usize
    }
}

/// A persistent return edge. The exact parent PC already contains its parent
/// focus, done mask, outer return edge, and resume ID, so a child transition
/// never copies or hashes the historical stack.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct FormulaReturnRecord {
    kind: FormulaReturnKind,
    parent: FormulaPcId,
    child: usize,
}

/// Exact O(1)-spine key stored once in the query-local PC arena. `focus` may
/// contain the current connective's dynamic child mask, but ancestry and the
/// outer WCO continuation are compact canonical IDs.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct FormulaPcRecord {
    focus: FormulaFocus,
    return_to: Option<FormulaReturnId>,
    resume: FormulaResumeId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InternedFormulaSuccessor {
    Formula(FormulaPcId),
    /// A completed support traversal must decide whether this OR child runs.
    /// The Boolean result remains transition-local and never enters identity.
    Guard {
        parent: FormulaPcId,
        child: usize,
    },
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
enum FormulaSuccessor {
    Formula(FormulaProgramCounter),
    /// A completed support traversal must decide whether this OR child runs.
    /// The Boolean result remains transition-local and never enters identity.
    Guard {
        parent: FormulaProgramCounter,
        child: usize,
    },
    Outer(FormulaOuterResume),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LeafLowering {
    Opaque,
    FiniteFormula,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ResidualLeaf {
    path: ConstraintPath,
    lowering: LeafLowering,
}

#[cfg(test)]
impl PartialEq<ConstraintPath> for ResidualLeaf {
    fn eq(&self, other: &ConstraintPath) -> bool {
        self.path == *other
    }
}

/// Borrow-free lowering plan safe to store beside its owned root.
///
/// Occurrence identity is the path's preorder position, not the address or
/// concrete type of the resolved constraint. Thus repeating the same `Arc`
/// twice in an AND produces two independent residual occurrences.
#[derive(Clone, Debug, Eq, PartialEq)]
struct ResidualPlan {
    leaves: Vec<ResidualLeaf>,
    /// Structural finite-formula program below lowered Union occurrences.
    /// Runtime migration is intentionally separate from compilation.
    finite_formula: FiniteFormulaProgram,
    /// Whether each opaque leaf's confirmation is homomorphic over ordered
    /// pages of one parent's candidate sequence.
    page_local_confirms: Vec<bool>,
    /// Whether eligible opaque proposal leaves may enter the residual
    /// transition submachine for this exact solve. Finite programs terminate;
    /// repeated programs compute their least fixpoint on the same substrate.
    transition_programs: bool,
    /// Per-variable bound-schema prerequisites under which a lowered cyclic
    /// confirmation needs the immutable complete candidate sequence for each
    /// parent until traversal quiescence.
    grouped_delta_confirm_requirements: Vec<Box<[(VariableId, VariableSet)]>>,
    /// The nontrivial exposed root is one formula occurrence. Whole-root
    /// identity shells around one opaque atom normalize to the flat plan.
    synthetic_root_formula: bool,
}

impl ResidualPlan {
    fn compile<'a>(root: &dyn Constraint<'a>) -> Self {
        Self::compile_mode(root, FormulaScope::OpaqueLeaves, false)
    }

    #[cfg(test)]
    fn compile_finite_unions<'a>(root: &dyn Constraint<'a>) -> Self {
        Self::compile_mode(root, FormulaScope::UnionLeaves, false)
    }

    fn compile_lowering<'a>(root: &dyn Constraint<'a>, lowering: ResidualLowering) -> Self {
        Self::compile_mode(
            root,
            lowering.formula_scope(),
            lowering.transition_programs(),
        )
    }

    fn compile_mode<'a>(
        root: &dyn Constraint<'a>,
        formula_scope: FormulaScope,
        transition_programs: bool,
    ) -> Self {
        /// Whether whole-root formula interpretation would add only Boolean
        /// identity control around one opaque non-union action.
        fn is_formula_identity<'a>(constraint: &dyn Constraint<'a>) -> bool {
            match constraint.residual_shape() {
                ConstraintShape::And(children) if children.len() == 1 => {
                    is_formula_identity(children.child(0))
                }
                ConstraintShape::Opaque => constraint.residual_union_children().is_none(),
                ConstraintShape::And(_) | ConstraintShape::ScopedAnd(_) => false,
            }
        }

        fn visit<'a>(
            constraint: &dyn Constraint<'a>,
            formula_scope: FormulaScope,
            transition_programs: bool,
            path: &mut Vec<usize>,
            leaves: &mut Vec<ResidualLeaf>,
            page_local_confirms: &mut Vec<bool>,
            grouped_delta_confirm_requirements: &mut Vec<Box<[(VariableId, VariableSet)]>>,
        ) {
            match constraint.residual_shape() {
                ConstraintShape::And(children) | ConstraintShape::ScopedAnd(children) => {
                    for child in 0..children.len() {
                        path.push(child);
                        visit(
                            children.child(child),
                            formula_scope,
                            transition_programs,
                            path,
                            leaves,
                            page_local_confirms,
                            grouped_delta_confirm_requirements,
                        );
                        path.pop();
                    }
                }
                ConstraintShape::Opaque => {
                    let lowering = if formula_scope == FormulaScope::UnionLeaves
                        && constraint.residual_union_children().is_some()
                    {
                        LeafLowering::FiniteFormula
                    } else {
                        LeafLowering::Opaque
                    };
                    leaves.push(ResidualLeaf {
                        path: ConstraintPath(path.clone().into_boxed_slice()),
                        lowering,
                    });
                    page_local_confirms.push(
                        matches!(lowering, LeafLowering::Opaque)
                            && constraint.residual_confirm_is_page_local(),
                    );
                    grouped_delta_confirm_requirements.push(
                        if matches!(lowering, LeafLowering::Opaque) {
                            compile_grouped_delta_confirm_requirements(
                                constraint,
                                transition_programs,
                            )
                        } else {
                            Box::new([])
                        },
                    );
                }
            }
        }

        let synthetic_root_formula =
            formula_scope == FormulaScope::WholeRoot && !is_formula_identity(root);
        let mut leaves = Vec::new();
        let mut page_local_confirms = Vec::new();
        let mut grouped_delta_confirm_requirements: Vec<Box<[(VariableId, VariableSet)]>> =
            Vec::new();
        if synthetic_root_formula {
            leaves.push(ResidualLeaf {
                path: ConstraintPath(Box::new([])),
                lowering: LeafLowering::FiniteFormula,
            });
            // Formula control owns the exact inner action boundary. The
            // singleton outer occurrence itself is never an ordinary
            // page-local or grouped confirmer.
            page_local_confirms.push(false);
            grouped_delta_confirm_requirements.push(Box::new([]));
        } else {
            visit(
                root,
                formula_scope,
                transition_programs,
                &mut Vec::new(),
                &mut leaves,
                &mut page_local_confirms,
                &mut grouped_delta_confirm_requirements,
            );
        }
        let finite_formula = FiniteFormulaProgram::compile(
            root,
            &leaves,
            transition_programs,
            synthetic_root_formula,
        );
        Self {
            leaves,
            finite_formula,
            page_local_confirms,
            transition_programs,
            grouped_delta_confirm_requirements,
            synthetic_root_formula,
        }
    }

    fn len(&self) -> usize {
        self.leaves.len()
    }

    fn action_span(&self) -> usize {
        self.finite_formula
            .max_root_span()
            .checked_add(2)
            .expect("residual formula action span overflow")
    }

    fn has_finite_formula(&self, occurrence: usize) -> bool {
        self.finite_formula.root(occurrence).is_some()
    }

    fn formula_action_occurrence(&self, outer: usize, node: FormulaNodeId) -> usize {
        if self.synthetic_root_formula {
            self.len()
                .checked_add(node.0 as usize)
                .expect("formula action occurrence overflow")
        } else {
            outer
        }
    }

    #[cfg(test)]
    fn formula_uses_candidate_pages(
        &self,
        counter: &FormulaProgramCounter,
        bound: VariableSet,
    ) -> bool {
        self.synthetic_root_formula
            && self
                .finite_formula
                .root_confirm_suffix_accepts_pages(counter, bound)
    }

    fn interned_formula_uses_candidate_pages(
        &self,
        formula_pcs: &FormulaPcInterner,
        counter: FormulaPcId,
        bound: VariableSet,
    ) -> bool {
        self.synthetic_root_formula
            && self
                .finite_formula
                .interned_root_confirm_suffix_accepts_pages(formula_pcs, counter, bound)
    }

    #[cfg(test)]
    fn formula_proposal_streamability(
        &self,
        counter: &FormulaProgramCounter,
        bound: VariableSet,
    ) -> FormulaProposalStreamability {
        if !self.synthetic_root_formula {
            return FormulaProposalStreamability::Barrier(
                FormulaProposalStreamBarrier::NotSyntheticRoot,
            );
        }
        let streamability = self.finite_formula.proposal_streamability(counter, bound);
        if streamability != FormulaProposalStreamability::Linear {
            return streamability;
        }

        let UnionVerb::Propose { relevant } = &counter.resume.verb else {
            return FormulaProposalStreamability::Barrier(
                FormulaProposalStreamBarrier::NotProposalAction,
            );
        };
        let checked = ChildSet::empty(self.len()).with_inserted(counter.resume.occurrence);
        if !self.remaining_confirms_accept_pages(relevant, &checked, counter.resume.variable, bound)
        {
            return FormulaProposalStreamability::Barrier(
                FormulaProposalStreamBarrier::OuterContinuation,
            );
        }
        FormulaProposalStreamability::Linear
    }

    fn interned_formula_proposal_streamability(
        &self,
        formula_pcs: &FormulaPcInterner,
        counter: FormulaPcId,
        bound: VariableSet,
    ) -> FormulaProposalStreamability {
        if !self.synthetic_root_formula {
            return FormulaProposalStreamability::Barrier(
                FormulaProposalStreamBarrier::NotSyntheticRoot,
            );
        }
        let streamability =
            self.finite_formula
                .interned_proposal_streamability(formula_pcs, counter, bound);
        if streamability != FormulaProposalStreamability::Linear {
            return streamability;
        }

        let resume = formula_pcs.resume(counter);
        let UnionVerb::Propose { relevant } = &resume.verb else {
            return FormulaProposalStreamability::Barrier(
                FormulaProposalStreamBarrier::NotProposalAction,
            );
        };
        let checked = ChildSet::empty(self.len()).with_inserted(resume.occurrence);
        if !self.remaining_confirms_accept_pages(relevant, &checked, resume.variable, bound) {
            return FormulaProposalStreamability::Barrier(
                FormulaProposalStreamBarrier::OuterContinuation,
            );
        }
        FormulaProposalStreamability::Linear
    }

    fn resolve<'r, 'a>(
        &self,
        root: &'r dyn Constraint<'a>,
        occurrence: usize,
    ) -> &'r dyn Constraint<'a> {
        let mut constraint = root;
        for &child in self.leaves[occurrence].path.0.iter() {
            constraint = match constraint.residual_shape() {
                ConstraintShape::And(children) | ConstraintShape::ScopedAnd(children) => {
                    children.child(child)
                }
                ConstraintShape::Opaque => {
                    panic!("residual AND shape changed during query execution")
                }
            };
        }
        constraint
    }

    fn resolve_formula_node<'r, 'a>(
        &self,
        root: &'r dyn Constraint<'a>,
        occurrence: usize,
        node: FormulaNodeId,
    ) -> &'r dyn Constraint<'a> {
        let mut constraint = self.resolve(root, occurrence);
        for step in self.finite_formula.node(node).path.0.iter() {
            constraint = match *step {
                FormulaStep::Or(child) => constraint
                    .residual_union_children()
                    .expect("residual OR shape changed during query execution")
                    .child(child),
                FormulaStep::And(child) => match constraint.residual_shape() {
                    ConstraintShape::And(children) => children.child(child),
                    ConstraintShape::ScopedAnd(_) | ConstraintShape::Opaque => {
                        panic!("residual AND shape changed during query execution")
                    }
                },
                FormulaStep::ScopedAnd(child) => match constraint.residual_shape() {
                    ConstraintShape::ScopedAnd(children) => children.child(child),
                    ConstraintShape::And(_) | ConstraintShape::Opaque => {
                        panic!("residual scoped-AND shape changed during query execution")
                    }
                },
            };
        }
        constraint
    }

    /// Whether any concrete leaf in this plan owns a true transition source
    /// for the current variable and bound-row schema. Proposal-only paging can
    /// be materialized eagerly behind a quiescent formula barrier when the
    /// whole frontier is finite. A heterogeneous transition sibling keeps the
    /// direct source on the same bounded substrate so the scheduler can
    /// interleave their work.
    fn has_paged_transition_source<'a>(
        &self,
        root: &dyn Constraint<'a>,
        variable: VariableId,
        view: &RowsView<'_>,
    ) -> bool {
        fn formula_has_source<'a>(
            plan: &ResidualPlan,
            root: &dyn Constraint<'a>,
            occurrence: usize,
            node: FormulaNodeId,
            variable: VariableId,
            view: &RowsView<'_>,
        ) -> bool {
            match &plan.finite_formula.node(node).kind {
                FiniteFormulaNodeKind::Atom => {
                    let constraint = plan.resolve_formula_node(root, occurrence, node);
                    constraint.residual_delta_source_is_paged(variable, view)
                        || constraint.residual_proposal_source_has_transition_roots(variable, view)
                }
                FiniteFormulaNodeKind::And { children }
                | FiniteFormulaNodeKind::Or { children } => children.iter().any(|&child| {
                    formula_has_source(plan, root, occurrence, child, variable, view)
                }),
            }
        }

        (0..self.len()).any(|occurrence| {
            self.finite_formula.root(occurrence).map_or_else(
                || {
                    let constraint = self.resolve(root, occurrence);
                    constraint.residual_delta_source_is_paged(variable, view)
                        || constraint.residual_proposal_source_has_transition_roots(variable, view)
                },
                |formula_root| {
                    formula_has_source(self, root, occurrence, formula_root, variable, view)
                },
            )
        })
    }

    /// True exactly when every unchecked relevant confirmer can process
    /// ordered candidate pages independently. Whole-group confirmers may run
    /// first; paging begins only once the remaining continuation is local.
    fn remaining_confirms_are_page_local(&self, relevant: &ChildSet, checked: &ChildSet) -> bool {
        (0..self.len()).all(|leaf| {
            !relevant.contains(leaf) || checked.contains(leaf) || self.page_local_confirms[leaf]
        })
    }

    /// Whether candidate occurrences may be consumed as independent pages.
    /// A grouped delta reducer is deliberately parent-atomic even when its
    /// ordinary protocol confirmation is elementwise.
    fn remaining_confirms_accept_pages(
        &self,
        relevant: &ChildSet,
        checked: &ChildSet,
        variable: VariableId,
        bound: VariableSet,
    ) -> bool {
        self.remaining_confirms_are_page_local(relevant, checked)
            && (0..self.len()).all(|leaf| {
                !relevant.contains(leaf)
                    || checked.contains(leaf)
                    || !grouped_delta_confirm_is_active(
                        &self.grouped_delta_confirm_requirements[leaf],
                        variable,
                        bound,
                    )
            })
    }
}

/// The conservative structural selector that preceded the full-switch probe.
///
/// It remains test-only so coverage widened by the probe can be named exactly:
/// the old policy admitted only exposed AND roots with two flattened,
/// nonempty, overlapping leaf-variable sets. Production ordinary iteration no
/// longer consults it on this branch.
#[cfg(test)]
pub(super) fn useful_default_shape<'a>(root: &dyn Constraint<'a>) -> bool {
    fn overlaps_seen_leaf<'a>(constraint: &dyn Constraint<'a>, seen: &mut VariableSet) -> bool {
        match constraint.residual_shape() {
            ConstraintShape::Opaque | ConstraintShape::ScopedAnd(_) => {
                let variables = constraint.variables();
                if variables.is_empty() {
                    return false;
                }
                let overlaps = !variables.intersect(*seen).is_empty();
                *seen = seen.union(variables);
                overlaps
            }
            ConstraintShape::And(children) => {
                for child in 0..children.len() {
                    if overlaps_seen_leaf(children.child(child), seen) {
                        return true;
                    }
                }
                false
            }
        }
    }

    let children = match root.residual_shape() {
        ConstraintShape::And(children) => children,
        ConstraintShape::ScopedAnd(_) | ConstraintShape::Opaque => return false,
    };
    let mut seen = VariableSet::new_empty();
    for child in 0..children.len() {
        if overlaps_seen_leaf(children.child(child), &mut seen) {
            return true;
        }
    }
    false
}

/// Formula boundary exposed to the canonical residual machine.
///
/// These variants form a chain, not independent feature bits: lowering the
/// whole root necessarily absorbs union-leaf lowering below it.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
#[must_use]
pub enum FormulaScope {
    /// Preserve every composite boundary except exposed associative ANDs.
    #[default]
    OpaqueLeaves,
    /// Lower exposed Union leaves and their recursive AND/OR descendants.
    UnionLeaves,
    /// Lower the maximal exposed root as one synthetic formula occurrence.
    WholeRoot,
}

/// Orthogonal structural lowering selected for one residual solve.
///
/// Formula scope is a three-element chain. Transition programs form the one
/// independent capability axis, giving exactly six canonical lowering forms.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
#[must_use]
pub struct ResidualLowering {
    formula_scope: FormulaScope,
    transition_programs: bool,
}

impl ResidualLowering {
    /// Conservative residual lowering used by explicit probe solvers.
    pub const CONSERVATIVE: Self = Self::new(FormulaScope::OpaqueLeaves, false);
    /// Full formula and transition-program lowering used by ordinary queries.
    pub const FULL: Self = Self::new(FormulaScope::WholeRoot, true);

    /// Constructs one of the six canonical lowering forms.
    pub const fn new(formula_scope: FormulaScope, transition_programs: bool) -> Self {
        Self {
            formula_scope,
            transition_programs,
        }
    }

    /// Returns the formula boundary exposed to the residual machine.
    pub const fn formula_scope(self) -> FormulaScope {
        self.formula_scope
    }

    /// Whether eligible finite and repeated transition programs execute in
    /// the residual submachine.
    pub const fn transition_programs(self) -> bool {
        self.transition_programs
    }
}

/// Measurements from one residual-state solve.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct ResidualStateStats {
    /// Number of distinct exact state descriptors interned.
    pub states_interned: usize,
    /// Number of interning requests that found an existing descriptor.
    pub interner_hits: usize,
    /// Number of filings appended to an already-live canonical bucket.
    pub bucket_merges: usize,
    /// Parent rows appended by those merge filings.
    pub rows_merged: usize,
    /// Number of canonical bucket chunks processed. Every pop is selected by
    /// exactly one physical policy, so this equals `full_pops +
    /// readiness_pops + continuation_pops`.
    pub state_pops: usize,
    /// Ready-state chunks that planned row-local proposal actions without
    /// invoking the constraint protocol.
    pub ready_plan_pops: usize,
    /// Exact row-local preferred-variable groups observed by Ready planning,
    /// summed across pops before topology-scaled agglomeration.
    pub ready_preferred_variable_groups: usize,
    /// Variable groups retained by Ready planning after topology-scaled
    /// agglomeration, summed across pops.
    pub ready_scheduled_variable_groups: usize,
    /// Concrete `(scheduled variable, exact proposer occurrence)` groups filed
    /// by Ready planning, summed across pops.
    pub ready_proposal_groups: usize,
    /// Ready pops where agglomeration reduced the preferred-variable group
    /// count.
    pub agglomerated_ready_pops: usize,
    /// Candidate-state chunks that planned row-local confirmation actions (or
    /// committed a fully checked candidate frontier) without invoking a
    /// constraint verb.
    pub candidate_plan_pops: usize,
    /// Concrete confirmer-occurrence groups filed by Candidate planning,
    /// summed across pops that still had an unchecked relevant occurrence.
    pub candidate_confirmation_groups: usize,
    /// Explicit proposal-action chunks that invoked one flattened leaf.
    pub propose_action_pops: usize,
    /// Explicit Boolean-support chunks that invoked one flattened leaf.
    pub support_action_pops: usize,
    /// Explicit confirmation-action chunks that invoked one flattened leaf.
    pub confirm_action_pops: usize,
    /// Protocol-action pops whose computation produced no successor rows.
    pub dead_action_pops: usize,
    /// Terminal Ready-state chunks emitted for projection.
    pub emit_pops: usize,
    /// Full actionable-width chunks selected from the maximum eligible rank.
    /// The unit is a parent row for Ready/Propose and atomic candidate states,
    /// or a candidate occurrence for an entirely page-local continuation.
    pub full_pops: usize,
    /// Underfilled buckets drained through the minimum-rank readiness gate
    /// because no live state could fill the desired width. The eager solver
    /// counts every one of its readiness-gated pops here.
    pub readiness_pops: usize,
    /// Physical continuation-cohort chunks selected after a full action
    /// partially survived. These pops deliberately bypass global occupancy
    /// harvesting without changing canonical state identity.
    pub continuation_pops: usize,
    /// Continuation-cohort pops whose coalesced receipt occupancy was smaller
    /// than the current desired width.
    pub underfilled_continuation_pops: usize,
    /// Pops that left unprocessed parent rows or candidate occurrences live
    /// under the same state.
    pub partial_pops: usize,
    /// Filings that reopened an interned state after its live bucket had
    /// already been consumed.
    pub state_reentries: usize,
    /// Parent rows carried by [`state_reentries`](Self::state_reentries).
    pub rows_reentered: usize,
    /// Logical flattened-leaf proposal actions. A paged source activation
    /// counts once even though it bypasses the eager `Constraint::propose`
    /// verb.
    pub propose_calls: usize,
    /// Flattened-leaf Boolean-support calls.
    pub support_calls: usize,
    /// Flattened-leaf confirmation calls.
    pub confirm_calls: usize,
    /// Parent rows passed to proposal calls.
    pub propose_rows: usize,
    /// Candidate occurrences produced by proposal actions, including direct
    /// source-page effects that bypass an eager protocol call.
    pub candidates_proposed: usize,
    /// Largest candidate frontier produced by one eager call or direct source
    /// handoff.
    pub max_propose_candidates: usize,
    /// Parent rows passed to confirmation calls.
    pub confirm_rows: usize,
    /// Parent rows passed to Boolean-support calls.
    pub support_rows: usize,
    /// Candidate occurrences presented to confirmation calls, counting an
    /// occurrence once per remaining confirmer it reaches.
    pub candidates_confirmed: usize,
    /// Largest candidate page presented to one confirmation call.
    pub max_confirm_candidates: usize,
    /// Largest flattened-leaf proposal batch.
    pub max_propose_rows: usize,
    /// Largest flattened-leaf Boolean-support batch.
    pub max_support_rows: usize,
    /// Largest flattened-leaf confirmation batch.
    pub max_confirm_rows: usize,
    /// Numeric increases of the lazy scheduler's desired actionable width.
    /// Saturated or growth-one attempts do not increment this counter.
    pub width_increases: usize,
    /// Bounded pages requested from constraint-owned source frontiers.
    pub delta_source_pages: usize,
    /// Physical calls that consumed one compatible cohort of affine source
    /// pages. This is deliberately distinct from canonical delta states.
    pub delta_source_cohorts: usize,
    /// Largest number of compatible affine source activations dispatched by
    /// one physical cohort call.
    pub max_delta_source_cohort: usize,
    /// Ordered source candidates consumed across those pages, including
    /// candidates rejected by an exact secondary source filter.
    pub delta_source_candidates_examined: usize,
    /// Product-state roots admitted from bounded source pages.
    pub delta_source_roots: usize,
    /// Bounded pages requested from live product-state transition nodes.
    pub delta_transition_pages: usize,
    /// Physical calls that consumed one or more affine transition pages under
    /// the same structural transition operator.
    pub delta_transition_cohorts: usize,
    /// Largest number of affine transition pages consumed by one physical
    /// cohort call. Eager fallback rows are not included.
    pub max_delta_transition_cohort: usize,
    /// Ordered outgoing transition candidates consumed across those pages.
    pub delta_transition_candidates_examined: usize,
    /// Transition pages that produced no novel child, accepted endpoint, or
    /// stable continuation and therefore contributed negative-width feedback.
    pub delta_transition_dead_pages: usize,
    /// Direct proposal candidates admitted from bounded source pages without
    /// creating product-state traversal roots.
    pub delta_source_direct_candidates: usize,
    /// Source pages that retired without filing a stable acyclic effect and
    /// without resuming a stable/formula continuation. This counts pages
    /// exactly even when another activation files a stable continuation in
    /// the same batched delta step.
    pub delta_source_dead_pages: usize,
    /// Delta steps that contained at least one dead source page and no stable
    /// continuation, and therefore widened the global cold-harvest demand.
    pub delta_source_negative_steps: usize,
    /// Delta steps that contained at least one dead transition page and no
    /// stable continuation, and therefore widened the global cold-harvest
    /// demand.
    pub delta_transition_negative_steps: usize,
    /// Affine cyclic activations that reached quiescence. This is the feedback
    /// unit for geometric breadth across independent private fixpoints.
    pub delta_activations_completed: usize,
    /// Numeric increases of the independent quiescent-activation cohort width.
    pub delta_activation_width_increases: usize,
    /// One-atom continuation pops used to probe a delta-to-stable handoff
    /// before returning the rest of that cohort to global cold harvesting.
    pub delta_handoff_probe_pops: usize,
}

/// Results and measurements from [`Query::solve_residual_state_profiled`].
#[derive(Clone, Debug)]
#[must_use]
#[non_exhaustive]
pub struct ResidualStateSolve<R> {
    /// Projected query results, preserving bag semantics.
    pub results: Vec<R>,
    /// Scheduler/interner measurements for the solve.
    pub stats: ResidualStateStats,
}

/// Epoch-local identity of one observed residual action.
///
/// The number is meaningful only within the [`ResidualShadowEpoch`] whose
/// snapshot contains it. It is deliberately unrelated to the residual
/// machine's private `StateId`: parallel siblings may intern later states in
/// different orders, so a raw interner index is not a global identity.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ActionEventId(u64);

impl ActionEventId {
    /// Returns this event's ordinal within its owning epoch.
    pub fn get(self) -> u64 {
        self.0
    }
}

/// Concrete constraint verb executed by an observed residual action.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ActionVerb {
    /// Check whether one formula atom can still contribute for each row.
    Support,
    /// Enumerate candidates for one selected variable.
    Propose,
    /// Filter one candidate frontier through one selected leaf occurrence.
    Confirm,
}

/// Exact semantic call site of one observed action.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActionSite {
    /// Executed protocol verb.
    pub verb: ActionVerb,
    /// Variable whose formula traversal selected the action.
    pub variable: VariableId,
    /// Deterministic preorder occurrence in this epoch's compiled plan.
    ///
    /// Like [`ActionEventId`], this is query/epoch-local rather than a global
    /// constraint identity or address.
    pub leaf_occurrence: usize,
    /// Exact committed parent-row schema.
    pub bound: VariableSet,
}

/// Input geometry known at the residual action dispatch boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActionGeometry {
    /// Parent rows presented to the protocol action.
    pub parent_rows: usize,
    /// Candidate occurrences presented to Confirm; zero for Support/Propose.
    pub candidate_occurrences: usize,
    /// Scheduler occupancy consumed by the selected action chunk.
    pub action_atoms: usize,
}

/// Exact nonempty payload filed by a surviving action, or transferred into
/// its native cyclic continuation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActionSurvival {
    /// Parent rows retained in the immediate stable successor cohort, or
    /// transferred into the cyclic scheduler when expansion is deferred.
    pub parent_rows: usize,
    /// Candidate occurrences retained by that stable or cyclic continuation.
    pub candidate_occurrences: usize,
}

/// Semantic outcome of one observed residual protocol action.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActionOutcome {
    /// The action filed a nonempty stable successor or transferred affine work
    /// into its native cyclic continuation.
    Advanced(ActionSurvival),
    /// The action compacted to no successor candidates.
    Dead,
    /// Execution unwound before returning an ordinary outcome.
    Aborted,
}

/// Completion recorded for an observed action.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActionCompletion {
    /// Dispatch-to-successor wall time around the unchanged owned-task
    /// execution. This includes protocol work and residual transition filing;
    /// for cyclic lowering it ends after native seeding, before later shared
    /// expansion cohorts that may combine several action sites.
    pub wall: Duration,
    /// Exact action outcome and immediate survival geometry.
    pub outcome: ActionOutcome,
    /// True when the epoch was closed or invalidated before completion.
    pub stale: bool,
}

/// Backend-neutral executor-local measurement nested inside one action.
///
/// Backends choose honest static labels and units. For example, a synchronous
/// device API that combines upload, dispatch, synchronization, and readback
/// should report one `gpu-round-trip` operation rather than inventing phase
/// boundaries it cannot measure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutorMeasurement {
    /// Executor family, such as `cpu` or `wgpu`.
    pub executor: &'static str,
    /// Measured operation, such as `wavelet-rank` or `gpu-round-trip`.
    pub operation: &'static str,
    /// Unit name, such as `rank-probes`.
    pub work_unit: &'static str,
    /// Exact number of work units presented to this invocation.
    pub work_units: usize,
    /// Start offset from the owning epoch's creation.
    pub started: Duration,
    /// Executor-local wall time.
    pub wall: Duration,
}

/// Executor measurement attached to its exact epoch-local action event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutorSample {
    /// Owning action event within the snapshot's epoch.
    pub event: ActionEventId,
    /// Executor-local measurement.
    pub measurement: ExecutorMeasurement,
    /// True when recorded after the epoch was closed or invalidated.
    pub stale: bool,
}

/// One action and every executor-local sample currently attached to it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActionObservation {
    /// Epoch-local event identity.
    pub event: ActionEventId,
    /// Exact action site.
    pub site: ActionSite,
    /// Exact input geometry.
    pub geometry: ActionGeometry,
    /// Dispatch start offset from epoch creation. A snapshot taken in the
    /// narrow registration-to-dispatch window reports the registration offset
    /// until execution installs the final start offset.
    pub started: Duration,
    /// Completion, or `None` while the action is still executing.
    pub completion: Option<ActionCompletion>,
    /// Executor-local samples correlated through this event's capability,
    /// ordered by start offset and then by their mutex-serialized attachment
    /// order when offsets compare equal.
    pub executor_samples: Vec<ExecutorSample>,
}

/// Terminal state of a one-shot observation epoch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResidualShadowStatus {
    /// New action events may still begin.
    Open,
    /// The affine frontier was exhausted and every begun action completed
    /// normally; new actions are rejected.
    Closed,
    /// Observation lost its affine completion proof through unwind,
    /// abandonment, cancellation, or explicit invalidation; new actions are
    /// rejected.
    Invalidated,
}

impl ResidualShadowStatus {
    const OPEN: u8 = 0;
    const CLOSED: u8 = 1;
    const INVALIDATED: u8 = 2;

    fn from_raw(raw: u8) -> Self {
        match raw {
            Self::OPEN => Self::Open,
            Self::CLOSED => Self::Closed,
            Self::INVALIDATED => Self::Invalidated,
            _ => unreachable!("invalid residual shadow epoch status"),
        }
    }
}

/// Point-in-time copy of one shadow epoch's observations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResidualShadowSnapshot {
    /// Epoch state when the snapshot was taken.
    pub status: ResidualShadowStatus,
    /// Events ordered by epoch-local [`ActionEventId`].
    pub events: Vec<ActionObservation>,
}

struct ShadowEvent {
    event: ActionEventId,
    site: ActionSite,
    geometry: ActionGeometry,
    epoch_started: Instant,
    registered: Duration,
    started: Mutex<Option<Duration>>,
    epoch: Weak<ShadowEpochInner>,
    completion: Mutex<Option<ActionCompletion>>,
    executor_samples: Mutex<Vec<ExecutorSample>>,
}

impl ShadowEvent {
    fn with_epoch_staleness<T>(&self, operation: impl FnOnce(bool) -> T) -> T {
        let Some(epoch) = self.epoch.upgrade() else {
            return operation(true);
        };
        // Terminal transitions hold this same lock. A completion or sample
        // therefore linearizes wholly before close/invalidate (fresh) or
        // wholly after it (stale), rather than reading Open and attaching only
        // after the terminal transition has returned.
        let _events = epoch
            .events
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        operation(epoch.status() != ResidualShadowStatus::Open)
    }

    fn complete(&self, wall: Duration, outcome: ActionOutcome) {
        self.with_epoch_staleness(|stale| {
            let mut completion = self
                .completion
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            if completion.is_none() {
                *completion = Some(ActionCompletion {
                    wall,
                    outcome,
                    stale,
                });
            }
        });
    }

    /// Publishes the dispatch offset through the epoch's snapshot gate.
    ///
    /// An action admitted while the epoch was open may reach dispatch after
    /// explicit invalidation. Publication remains diagnostic-only in that
    /// case: it must not cancel or otherwise perturb the observed query.
    fn publish_started(&self) {
        let Some(epoch) = self.epoch.upgrade() else {
            *self
                .started
                .lock()
                .unwrap_or_else(|poison| poison.into_inner()) =
                Some(Instant::now().duration_since(self.epoch_started));
            return;
        };
        let _events = epoch
            .events
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        *self
            .started
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) =
            Some(Instant::now().duration_since(self.epoch_started));
    }

    fn abort(&self, wall: Duration) {
        #[cfg(test)]
        SHADOW_ABORT_HOOK.with(|hook| {
            if let Some(hook) = hook.borrow_mut().take() {
                hook(self.event);
            }
        });
        let Some(epoch) = self.epoch.upgrade() else {
            let mut completion = self
                .completion
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            if completion.is_none() {
                *completion = Some(ActionCompletion {
                    wall,
                    outcome: ActionOutcome::Aborted,
                    stale: true,
                });
            }
            return;
        };
        let _events = epoch
            .events
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let stale = epoch.status() != ResidualShadowStatus::Open;
        let mut completion = self
            .completion
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if completion.is_none() {
            *completion = Some(ActionCompletion {
                wall,
                outcome: ActionOutcome::Aborted,
                stale,
            });
        }
        epoch.invalidate_locked();
    }

    /// Requires the owning epoch's event lock, which serializes this read
    /// against normal completion and abort.
    fn completed_normally(&self) -> bool {
        self.completion
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .is_some_and(|completion| completion.outcome != ActionOutcome::Aborted)
    }

    fn snapshot(&self) -> ActionObservation {
        let started = self
            .started
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .unwrap_or(self.registered);
        let completion = *self
            .completion
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let mut executor_samples = self
            .executor_samples
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone();
        executor_samples.sort_by_key(|sample| sample.measurement.started);
        ActionObservation {
            event: self.event,
            site: self.site,
            geometry: self.geometry,
            started,
            completion,
            executor_samples,
        }
    }
}

struct ShadowEpochInner {
    started: Instant,
    status: AtomicU8,
    claimed: AtomicBool,
    next_event: AtomicU64,
    /// Also serializes terminal transition against event creation: once close
    /// or invalidate returns, no later event can enter this vector.
    events: Mutex<Vec<Arc<ShadowEvent>>>,
}

impl ShadowEpochInner {
    fn status(&self) -> ResidualShadowStatus {
        ResidualShadowStatus::from_raw(self.status.load(Ordering::Acquire))
    }

    fn invalidate(&self) -> bool {
        let _events = self
            .events
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        self.invalidate_locked()
    }

    /// Requires the event lock. Terminal state is monotonic: an already
    /// Closed epoch is never upgraded to Invalidated.
    fn invalidate_locked(&self) -> bool {
        self.status
            .compare_exchange(
                ResidualShadowStatus::OPEN,
                ResidualShadowStatus::INVALIDATED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// Capability-owned normal terminal transition. Closed is proof that the
    /// affine frontier was exhausted and every begun action completed with an
    /// ordinary outcome. A live or aborted event makes that proof fail closed
    /// as Invalidated.
    fn finish_exhausted(&self) -> ResidualShadowStatus {
        let events = self
            .events
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        match self.status() {
            ResidualShadowStatus::Open => {
                let target = if events.iter().all(|event| event.completed_normally()) {
                    ResidualShadowStatus::Closed
                } else {
                    ResidualShadowStatus::Invalidated
                };
                self.status.store(
                    match target {
                        ResidualShadowStatus::Closed => ResidualShadowStatus::CLOSED,
                        ResidualShadowStatus::Invalidated => ResidualShadowStatus::INVALIDATED,
                        ResidualShadowStatus::Open => unreachable!(),
                    },
                    Ordering::Release,
                );
                target
            }
            terminal => terminal,
        }
    }

    fn claim(&self) {
        let _events = self
            .events
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        assert_eq!(
            self.status(),
            ResidualShadowStatus::Open,
            "cannot attach a closed or invalidated residual shadow epoch"
        );
        assert!(
            self.claimed
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok(),
            "a residual shadow epoch can observe only one residual iterator"
        );
    }
}

/// Arc-backed, one-shot collector for opt-in residual action observations.
///
/// Clones name the same epoch. Normal closure is owned by the claimed serial
/// iterator or top-level parallel drive after proven affine exhaustion;
/// callers may explicitly [`invalidate`](Self::invalidate) a run. Either
/// terminal state rejects new event registration. An action admitted while
/// the epoch was open may still dispatch and complete after invalidation; its
/// late completion is retained as stale rather than changing query execution.
/// Construct a new epoch for a new execution environment or run.
#[derive(Clone)]
pub struct ResidualShadowEpoch {
    inner: Arc<ShadowEpochInner>,
}

impl Default for ResidualShadowEpoch {
    fn default() -> Self {
        Self::new()
    }
}

impl ResidualShadowEpoch {
    /// Creates one open, independent observation epoch.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ShadowEpochInner {
                started: Instant::now(),
                status: AtomicU8::new(ResidualShadowStatus::OPEN),
                claimed: AtomicBool::new(false),
                next_event: AtomicU64::new(0),
                events: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Returns this epoch's current terminal state.
    pub fn status(&self) -> ResidualShadowStatus {
        self.inner.status()
    }

    /// Invalidates this epoch. Returns true only for the winning `Open` to
    /// `Invalidated` transition; a proven [`ResidualShadowStatus::Closed`]
    /// epoch remains closed.
    pub fn invalidate(&self) -> bool {
        self.inner.invalidate()
    }

    /// Copies all observations accumulated so far.
    pub fn snapshot(&self) -> ResidualShadowSnapshot {
        let events = self
            .inner
            .events
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let status = self.status();
        let mut events: Vec<_> = events.iter().map(|event| event.snapshot()).collect();
        events.sort_by_key(|event| event.event);
        ResidualShadowSnapshot { status, events }
    }

    fn begin(&self, site: ActionSite, geometry: ActionGeometry) -> ShadowActionSpan {
        let mut events = self
            .inner
            .events
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        assert_eq!(
            self.status(),
            ResidualShadowStatus::Open,
            "cannot begin a residual shadow action after its epoch is closed or invalidated"
        );
        let raw = self.inner.next_event.fetch_add(1, Ordering::Relaxed);
        assert_ne!(raw, u64::MAX, "residual shadow action event id overflow");
        let registered_at = Instant::now();
        let event = Arc::new(ShadowEvent {
            event: ActionEventId(raw),
            site,
            geometry,
            epoch_started: self.inner.started,
            registered: registered_at.duration_since(self.inner.started),
            started: Mutex::new(None),
            epoch: Arc::downgrade(&self.inner),
            completion: Mutex::new(None),
            executor_samples: Mutex::new(Vec::new()),
        });
        events.push(Arc::clone(&event));
        ShadowActionSpan {
            event,
            execution_started: None,
            finished: false,
        }
    }

    fn finish_exhausted(&self) -> ResidualShadowStatus {
        self.inner.finish_exhausted()
    }
}

/// Capability identifying the exact currently executing shadow action.
///
/// It may be cloned and carried by a synchronous or asynchronous backend. The
/// handle owns the event, so late measurements remain attached to their
/// original epoch-local action even after the dynamic scope has ended.
#[derive(Clone)]
pub struct ActionCorrelation {
    event: Arc<ShadowEvent>,
}

impl ActionCorrelation {
    /// Returns the owning event's epoch-local identity.
    pub fn event(&self) -> ActionEventId {
        self.event.event
    }

    /// Returns a monotonic offset suitable for the `started` field of an
    /// [`ExecutorMeasurement`].
    pub fn elapsed(&self) -> Duration {
        self.event.epoch_started.elapsed()
    }

    /// Attaches one executor-local measurement to this exact action.
    pub fn record_executor_sample(&self, measurement: ExecutorMeasurement) {
        self.event.with_epoch_staleness(|stale| {
            let sample = ExecutorSample {
                event: self.event.event,
                measurement,
                stale,
            };
            self.event
                .executor_samples
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .push(sample);
        });
    }
}

thread_local! {
    static CURRENT_SHADOW_ACTION: RefCell<Vec<ActionCorrelation>> = const { RefCell::new(Vec::new()) };
}

#[cfg(test)]
thread_local! {
    static SHADOW_ABORT_HOOK: RefCell<Option<Box<dyn FnOnce(ActionEventId)>>> = RefCell::new(None);
}

/// Returns the innermost observed residual action on this thread, if any.
///
/// The dynamic scope is stack-disciplined. Nested observed queries temporarily
/// replace the outer action and restore it on return. Backends that transfer
/// work to another thread must explicitly capture and carry the returned
/// capability; ambient thread-local state is intentionally not propagated.
pub fn current_residual_action() -> Option<ActionCorrelation> {
    CURRENT_SHADOW_ACTION.with(|current| current.borrow().last().cloned())
}

struct ShadowActionScope(ActionEventId);

impl ShadowActionScope {
    fn enter(correlation: ActionCorrelation) -> Self {
        let event = correlation.event();
        CURRENT_SHADOW_ACTION.with(|current| current.borrow_mut().push(correlation));
        Self(event)
    }
}

impl Drop for ShadowActionScope {
    fn drop(&mut self) {
        CURRENT_SHADOW_ACTION.with(|current| {
            let correlation = current
                .borrow_mut()
                .pop()
                .expect("residual shadow action scope stack underflow");
            assert_eq!(
                correlation.event(),
                self.0,
                "residual shadow action scopes were dropped out of order"
            );
        });
    }
}

struct ShadowActionSpan {
    event: Arc<ShadowEvent>,
    execution_started: Option<Instant>,
    finished: bool,
}

impl ShadowActionSpan {
    fn correlation(&self) -> ActionCorrelation {
        ActionCorrelation {
            event: Arc::clone(&self.event),
        }
    }

    fn start(&mut self) {
        self.start_with(Instant::now);
    }

    fn start_with(&mut self, execution_clock: impl FnOnce() -> Instant) {
        assert!(
            self.execution_started.is_none(),
            "residual shadow action timer started twice"
        );
        self.event.publish_started();
        // This private clock is deliberately captured only after publication
        // released every observer lock. No snapshot contention or diagnostic
        // metadata write may enter the executor wall measurement.
        self.execution_started = Some(execution_clock());
    }

    fn elapsed(&self) -> Duration {
        self.execution_started
            .expect("residual shadow action completed before its timer started")
            .elapsed()
    }

    fn finish(mut self, wall: Duration, outcome: ActionOutcome) {
        self.event.complete(wall, outcome);
        self.finished = true;
    }
}

impl Drop for ShadowActionSpan {
    fn drop(&mut self) {
        if !self.finished {
            let wall = self
                .execution_started
                .map_or(Duration::ZERO, |started| started.elapsed());
            self.event.abort(wall);
        }
    }
}

/// A dynamic bitset of flattened leaf-occurrence IDs.
///
/// Leaf identity is its deterministic preorder occurrence in the maximal root
/// AND region, not its Rust type, address, or variable set. A dynamic
/// representation avoids aliasing conjunctions with more leaves than the query
/// language's independent 128-variable cap.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct ChildSet(SmallVec<[u64; 1]>);

impl ChildSet {
    fn empty(leaf_count: usize) -> Self {
        Self(SmallVec::from_elem(0, leaf_count.div_ceil(64)))
    }

    fn contains(&self, child: usize) -> bool {
        self.0[child / 64] & (1 << (child % 64)) != 0
    }

    fn insert(&mut self, child: usize) {
        self.0[child / 64] |= 1 << (child % 64);
    }

    fn with_inserted(&self, child: usize) -> Self {
        let mut next = self.clone();
        next.insert(child);
        next
    }

    fn count(&self) -> usize {
        self.0.iter().map(|word| word.count_ones() as usize).sum()
    }

    fn is_subset_of(&self, other: &Self) -> bool {
        self.0
            .iter()
            .zip(&other.0)
            .all(|(left, right)| left & !right == 0)
    }

    fn is_valid_for(&self, leaf_count: usize) -> bool {
        if self.0.len() != leaf_count.div_ceil(64) {
            return false;
        }
        let remainder = leaf_count % 64;
        remainder == 0 || self.0.last().is_none_or(|word| word >> remainder == 0)
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum UnionVerb {
    Propose {
        relevant: ChildSet,
    },
    Confirm {
        relevant: ChildSet,
        checked: ChildSet,
    },
}

impl UnionVerb {
    fn checked_count(&self) -> usize {
        match self {
            UnionVerb::Propose { .. } => 0,
            UnionVerb::Confirm { checked, .. } => checked.count(),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum ResidualPhase {
    /// Plan one joint `(variable, proposing child)` action per row.
    Ready,
    /// Invoke one proposer over a row block whose action is uniform.
    Propose {
        variable: VariableId,
        relevant: ChildSet,
        proposer: usize,
    },
    /// A variable has speculative candidates and some leaf occurrences have
    /// already accepted them. Plan the next confirmer per parent row.
    Candidate {
        variable: VariableId,
        relevant: ChildSet,
        checked: ChildSet,
    },
    /// Invoke one confirmer over a whole-parent candidate block whose action
    /// is uniform.
    Confirm {
        variable: VariableId,
        relevant: ChildSet,
        checked: ChildSet,
        confirmer: usize,
    },
    /// One exact finite-formula continuation. Planning and opaque protocol
    /// actions share this identity; the focused program node distinguishes
    /// them without a compatibility executor or historical arm index.
    Formula { counter: FormulaPcId },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StateDesc {
    /// Committed bindings; their physical columns are ascending variable IDs.
    bound: VariableSet,
    phase: ResidualPhase,
}

impl Hash for StateDesc {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Keep the hash implementation private to this scheduler instead of
        // expanding VariableSet's public trait surface for one interner.
        self.bound.count().hash(state);
        for variable in self.bound {
            variable.hash(state);
        }
        self.phase.hash(state);
    }
}

impl StateDesc {
    fn validate(&self, leaf_count: usize, formula_pcs: &FormulaPcInterner) {
        let validate_variable = |variable: VariableId| {
            assert!(
                !self.bound.is_set(variable),
                "residual action variable is already committed"
            );
        };
        let validate_sets = |relevant: &ChildSet, checked: &ChildSet| {
            assert!(
                relevant.is_valid_for(leaf_count),
                "residual relevant set contains a non-leaf occurrence"
            );
            assert!(
                checked.is_valid_for(leaf_count),
                "residual checked set contains a non-leaf occurrence"
            );
            assert!(relevant.count() > 0, "residual relevant set is empty");
            assert!(checked.count() > 0, "residual checked set is empty");
            assert!(
                checked.is_subset_of(relevant),
                "residual checked set is not a subset of the relevant set"
            );
        };

        match &self.phase {
            ResidualPhase::Ready => {}
            ResidualPhase::Propose {
                variable,
                relevant,
                proposer,
            } => {
                validate_variable(*variable);
                assert!(
                    relevant.is_valid_for(leaf_count),
                    "residual relevant set contains a non-leaf occurrence"
                );
                assert!(relevant.count() > 0, "residual relevant set is empty");
                assert!(
                    *proposer < leaf_count && relevant.contains(*proposer),
                    "residual proposer is not relevant"
                );
            }
            ResidualPhase::Candidate {
                variable,
                relevant,
                checked,
            } => {
                validate_variable(*variable);
                validate_sets(relevant, checked);
            }
            ResidualPhase::Confirm {
                variable,
                relevant,
                checked,
                confirmer,
            } => {
                validate_variable(*variable);
                validate_sets(relevant, checked);
                assert!(
                    *confirmer < leaf_count
                        && relevant.contains(*confirmer)
                        && !checked.contains(*confirmer),
                    "residual confirmer is not an unchecked relevant leaf"
                );
            }
            ResidualPhase::Formula { counter } => {
                let resume = formula_pcs.resume(*counter);
                validate_variable(resume.variable);
                assert!(
                    resume.occurrence < leaf_count,
                    "residual formula is not a leaf occurrence"
                );
                match &resume.verb {
                    UnionVerb::Propose { relevant } => {
                        assert!(relevant.is_valid_for(leaf_count));
                        assert!(relevant.contains(resume.occurrence));
                    }
                    UnionVerb::Confirm { relevant, checked } => {
                        validate_sets(relevant, checked);
                        assert!(
                            relevant.contains(resume.occurrence)
                                && !checked.contains(resume.occurrence),
                            "residual formula is not an unchecked relevant leaf"
                        );
                    }
                }
            }
        }
    }

    /// History-independent grade. Every transition strictly raises it, so
    /// draining the minimum grade is an exact readiness gate: once a state is
    /// popped, no unprocessed predecessor can still file into it.
    #[cfg(test)]
    fn rank(&self, leaf_count: usize) -> usize {
        self.rank_with_span(leaf_count, 2, None, &FormulaPcInterner::default())
    }

    fn rank_with_span(
        &self,
        leaf_count: usize,
        action_span: usize,
        formula: Option<&FiniteFormulaProgram>,
        formula_pcs: &FormulaPcInterner,
    ) -> usize {
        self.validate(leaf_count, formula_pcs);
        assert!(action_span >= 2, "residual action span is too small");
        let stride = leaf_count
            .checked_add(1)
            .and_then(|value| value.checked_mul(action_span))
            .expect("residual-state rank stride overflow");
        let base = self
            .bound
            .count()
            .checked_mul(stride)
            .expect("residual-state rank overflow");
        match &self.phase {
            ResidualPhase::Ready => base,
            ResidualPhase::Propose { .. } => {
                base.checked_add(1).expect("residual-state rank overflow")
            }
            ResidualPhase::Candidate { checked, .. } => checked
                .count()
                .checked_mul(action_span)
                .and_then(|grade| base.checked_add(grade))
                .expect("residual-state rank overflow"),
            ResidualPhase::Confirm { checked, .. } => checked
                .count()
                .checked_mul(action_span)
                .and_then(|grade| grade.checked_add(1))
                .and_then(|grade| base.checked_add(grade))
                .expect("residual-state rank overflow"),
            ResidualPhase::Formula { counter } => formula_pcs
                .resume(*counter)
                .verb
                .checked_count()
                .checked_mul(action_span)
                .and_then(|grade| grade.checked_add(1))
                .and_then(|grade| {
                    formula.expect("formula state rank requires its compiled program");
                    formula_pcs.grade(*counter).checked_add(grade)
                })
                .and_then(|grade| base.checked_add(grade))
                .expect("residual-state rank overflow"),
        }
    }

    /// Candidate occurrences become independent scheduling atoms only after
    /// every confirmer still named by the continuation is page-local.
    fn uses_candidate_pages(&self, plan: &ResidualPlan, formula_pcs: &FormulaPcInterner) -> bool {
        match &self.phase {
            ResidualPhase::Candidate {
                variable,
                relevant,
                checked,
            } => {
                relevant != checked
                    && plan
                        .remaining_confirms_accept_pages(relevant, checked, *variable, self.bound)
            }
            ResidualPhase::Confirm {
                variable,
                relevant,
                checked,
                ..
            } => plan.remaining_confirms_accept_pages(relevant, checked, *variable, self.bound),
            ResidualPhase::Formula { counter } => {
                plan.interned_formula_uses_candidate_pages(formula_pcs, *counter, self.bound)
            }
            ResidualPhase::Ready | ResidualPhase::Propose { .. } => false,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct StateId(u32);

#[derive(Clone, Default)]
struct FormulaPcInterner {
    // Insertion order, rather than a hash, determines stable query-local IDs.
    resumes: IndexSet<FormulaOuterResume, ahash::RandomState>,
    returns: IndexSet<FormulaReturnRecord, ahash::RandomState>,
    counters: IndexSet<FormulaPcRecord, ahash::RandomState>,
    // The compiler-derived grade is immutable with the exact PC. Successor
    // constructors update it algebraically, so neither filing nor transition
    // construction walks a persistent return chain.
    grades: Vec<usize>,
}

impl FormulaPcInterner {
    fn intern_resume(&mut self, resume: FormulaOuterResume) -> FormulaResumeId {
        if self.resumes.len() > u32::MAX as usize {
            if let Some(raw) = self.resumes.get_index_of(&resume) {
                return FormulaResumeId(raw as u32);
            }
            panic!("too many residual formula outer continuations");
        }
        let (raw, _) = self.resumes.insert_full(resume);
        FormulaResumeId(u32::try_from(raw).expect("too many residual formula outer continuations"))
    }

    fn intern_return(&mut self, address: FormulaReturnRecord) -> FormulaReturnId {
        // FormulaReturnId reserves zero for None, so there are u32::MAX
        // representable addresses rather than u32::MAX + 1 PC IDs.
        if self.returns.len() >= u32::MAX as usize {
            if let Some(raw) = self.returns.get_index_of(&address) {
                return FormulaReturnId::from_index(raw);
            }
            panic!("too many residual formula return addresses");
        }
        let (raw, _) = self.returns.insert_full(address);
        FormulaReturnId::from_index(raw)
    }

    fn intern_record(&mut self, counter: FormulaPcRecord, grade: usize) -> FormulaPcId {
        if self.counters.len() > u32::MAX as usize {
            if let Some(raw) = self.counters.get_index_of(&counter) {
                assert_eq!(
                    self.grades[raw], grade,
                    "one canonical formula PC acquired two topological grades"
                );
                return FormulaPcId(raw as u32);
            }
            panic!("too many residual formula program counters");
        }
        let (raw, inserted) = self.counters.insert_full(counter);
        if inserted {
            self.grades.push(grade);
        } else {
            assert_eq!(
                self.grades[raw], grade,
                "one canonical formula PC acquired two topological grades"
            );
        }
        FormulaPcId(u32::try_from(raw).expect("too many residual formula program counters"))
    }

    fn start(
        &mut self,
        program: &FiniteFormulaProgram,
        variable: VariableId,
        occurrence: usize,
        verb: UnionVerb,
    ) -> FormulaPcId {
        let root = program
            .root(occurrence)
            .expect("an opaque residual leaf has no finite formula program");
        let stage = match &verb {
            UnionVerb::Propose { .. } => FormulaStage::Propose,
            UnionVerb::Confirm { .. } => FormulaStage::Confirm,
        };
        let resume = self.intern_resume(FormulaOuterResume {
            variable,
            occurrence,
            verb,
        });
        self.intern_record(
            FormulaPcRecord {
                focus: program.entry_focus(root, stage),
                return_to: None,
                resume,
            },
            1,
        )
    }

    fn select_child(
        &mut self,
        program: &FiniteFormulaProgram,
        counter: FormulaPcId,
        child: usize,
    ) -> FormulaPcId {
        let stage = match self.get(counter).focus {
            FormulaFocus::Plan { stage, .. } => stage,
            _ => panic!("only a residual formula Plan can select a child"),
        };
        self.select_child_with(
            program,
            counter,
            child,
            FormulaReturnKind::Child,
            stage,
            false,
        )
    }

    fn select_child_as_action(
        &mut self,
        program: &FiniteFormulaProgram,
        counter: FormulaPcId,
        child: usize,
    ) -> FormulaPcId {
        let stage = match self.get(counter).focus {
            FormulaFocus::Plan { stage, .. } => stage,
            _ => panic!("only a residual formula Plan can select a child"),
        };
        self.select_child_with(
            program,
            counter,
            child,
            FormulaReturnKind::Child,
            stage,
            true,
        )
    }

    fn guard_child(
        &mut self,
        program: &FiniteFormulaProgram,
        counter: FormulaPcId,
        child: usize,
    ) -> FormulaPcId {
        let (node, stage) = match self.get(counter).focus {
            FormulaFocus::Plan { node, stage, .. } => (node, stage),
            _ => panic!("only a residual formula Plan can guard a child"),
        };
        assert!(matches!(
            program.node(node).kind,
            FiniteFormulaNodeKind::Or { .. }
        ));
        assert_ne!(stage, FormulaStage::Support);
        self.select_child_with(
            program,
            counter,
            child,
            FormulaReturnKind::Guard,
            FormulaStage::Support,
            false,
        )
    }

    fn select_supported_child(
        &mut self,
        program: &FiniteFormulaProgram,
        counter: FormulaPcId,
        child: usize,
    ) -> FormulaPcId {
        let (node, stage) = match self.get(counter).focus {
            FormulaFocus::Plan { node, stage, .. } => (node, stage),
            _ => panic!("only a residual formula Plan can select a supported child"),
        };
        assert!(matches!(
            program.node(node).kind,
            FiniteFormulaNodeKind::Or { .. }
        ));
        assert_ne!(stage, FormulaStage::Support);
        self.select_child_with(
            program,
            counter,
            child,
            FormulaReturnKind::Child,
            stage,
            false,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn select_child_with(
        &mut self,
        program: &FiniteFormulaProgram,
        counter: FormulaPcId,
        child: usize,
        kind: FormulaReturnKind,
        child_stage: FormulaStage,
        force_action: bool,
    ) -> FormulaPcId {
        let (node, parent_stage, done, resume, grade) = {
            let counter_record = self.get(counter);
            let FormulaFocus::Plan { node, stage, done } = &counter_record.focus else {
                panic!("only a residual formula Plan can select a child")
            };
            (
                *node,
                *stage,
                done.clone(),
                counter_record.resume,
                self.grade(counter),
            )
        };
        let children = program
            .node(node)
            .children()
            .expect("a residual formula Plan named an Atom");
        assert!(child < children.len() && !done.contains(child));
        let child_node = children[child];
        let entry_offset = match kind {
            FormulaReturnKind::Guard => 2,
            FormulaReturnKind::Child
                if parent_stage != FormulaStage::Support
                    && matches!(program.node(node).kind, FiniteFormulaNodeKind::Or { .. }) =>
            {
                program
                    .node(child_node)
                    .support_span
                    .checked_add(3)
                    .expect("residual guarded child entry overflow")
            }
            FormulaReturnKind::Child => 2,
        };
        let return_to = self.intern_return(FormulaReturnRecord {
            kind,
            parent: counter,
            child,
        });
        let focus = if force_action {
            FormulaFocus::Action {
                node: child_node,
                stage: child_stage,
            }
        } else {
            program.entry_focus(child_node, child_stage)
        };
        self.intern_record(
            FormulaPcRecord {
                focus,
                return_to: Some(return_to),
                resume,
            },
            grade
                .checked_add(entry_offset)
                .expect("residual formula grade overflow"),
        )
    }

    fn skip_child(
        &mut self,
        program: &FiniteFormulaProgram,
        counter: FormulaPcId,
        child: usize,
    ) -> FormulaPcId {
        let (node, stage, done, return_to, resume, grade) = {
            let counter_record = self.get(counter);
            let FormulaFocus::Plan { node, stage, done } = &counter_record.focus else {
                panic!("only a residual formula Plan can skip a child")
            };
            (
                *node,
                *stage,
                done.clone(),
                counter_record.return_to,
                counter_record.resume,
                self.grade(counter),
            )
        };
        let children = program
            .node(node)
            .children()
            .expect("a residual formula Plan named an Atom");
        assert!(child < children.len() && !done.contains(child));
        let child_weight = program.child_weight(node, stage, children[child]);
        self.intern_record(
            FormulaPcRecord {
                focus: FormulaFocus::Plan {
                    node,
                    stage,
                    done: done.with_inserted(child),
                },
                return_to,
                resume,
            },
            grade
                .checked_add(child_weight)
                .expect("residual formula grade overflow"),
        )
    }

    fn complete(&mut self, program: &FiniteFormulaProgram, counter: FormulaPcId) -> FormulaPcId {
        let (node, stage, return_to, resume, grade) = {
            let counter_record = self.get(counter);
            let (node, stage) = match &counter_record.focus {
                FormulaFocus::Action { node, stage } => (*node, *stage),
                FormulaFocus::Plan { node, stage, done } => {
                    let children = program
                        .node(*node)
                        .children()
                        .expect("a residual formula Plan named an Atom");
                    assert_eq!(
                        done.count(),
                        children.len(),
                        "a residual formula completed with live children"
                    );
                    (*node, *stage)
                }
                FormulaFocus::Complete { .. } => {
                    panic!("a completed residual formula was completed twice")
                }
            };
            (
                node,
                stage,
                counter_record.return_to,
                counter_record.resume,
                self.grade(counter),
            )
        };
        self.intern_record(
            FormulaPcRecord {
                focus: FormulaFocus::Complete { node, stage },
                return_to,
                resume,
            },
            grade
                .checked_add(1)
                .expect("residual formula grade overflow"),
        )
    }

    fn complete_support_short_circuit(
        &mut self,
        program: &FiniteFormulaProgram,
        counter: FormulaPcId,
        truth: bool,
    ) -> FormulaPcId {
        let (node, done, return_to, resume, grade) = {
            let counter_record = self.get(counter);
            let FormulaFocus::Plan {
                node,
                stage: FormulaStage::Support,
                done,
            } = &counter_record.focus
            else {
                panic!("only a support Plan can short-circuit")
            };
            (
                *node,
                done.clone(),
                counter_record.return_to,
                counter_record.resume,
                self.grade(counter),
            )
        };
        assert!(matches!(
            (&program.node(node).kind, truth),
            (FiniteFormulaNodeKind::And { .. }, false) | (FiniteFormulaNodeKind::Or { .. }, true)
        ));
        let local = program
            .completed_weight(node, FormulaStage::Support, &done)
            .checked_add(1)
            .expect("residual formula grade overflow");
        let delta = program
            .node_span(node, FormulaStage::Support)
            .checked_sub(local)
            .expect("support short circuit regressed formula grade");
        self.intern_record(
            FormulaPcRecord {
                focus: FormulaFocus::Complete {
                    node,
                    stage: FormulaStage::Support,
                },
                return_to,
                resume,
            },
            grade
                .checked_add(delta)
                .expect("residual formula grade overflow"),
        )
    }

    fn resume_completed(
        &mut self,
        program: &FiniteFormulaProgram,
        counter: FormulaPcId,
    ) -> Result<InternedFormulaSuccessor, FormulaOuterResume> {
        let (completed, completed_stage, return_to, resume_id, grade) = {
            let counter_record = self.get(counter);
            let FormulaFocus::Complete { node, stage } = counter_record.focus else {
                panic!("only a completed residual formula can return")
            };
            (
                node,
                stage,
                counter_record.return_to,
                counter_record.resume,
                self.grade(counter),
            )
        };
        let Some(return_to) = return_to else {
            let resume = self.resume_by_id(resume_id);
            assert_ne!(
                completed_stage,
                FormulaStage::Support,
                "a support traversal must return to a formula guard"
            );
            assert_eq!(program.root(resume.occurrence), Some(completed));
            let root_stage = match (&program.node(completed).kind, &resume.verb) {
                (FiniteFormulaNodeKind::And { .. }, UnionVerb::Propose { .. }) => {
                    FormulaStage::Confirm
                }
                (_, UnionVerb::Propose { .. }) => FormulaStage::Propose,
                (_, UnionVerb::Confirm { .. }) => FormulaStage::Confirm,
            };
            assert_eq!(completed_stage, root_stage);
            return Err(resume.clone());
        };
        let address = *self.return_by_id(return_to);
        let (parent_node, parent_stage, parent_done, parent_return, parent_resume) = {
            let parent = self.get(address.parent);
            let FormulaFocus::Plan { node, stage, done } = &parent.focus else {
                panic!("a formula return address named a non-Plan parent")
            };
            (*node, *stage, done.clone(), parent.return_to, parent.resume)
        };
        assert_eq!(resume_id, parent_resume);
        let children = program
            .node(parent_node)
            .children()
            .expect("a residual formula return address named an Atom parent");
        assert_eq!(children[address.child], completed);
        if address.kind == FormulaReturnKind::Guard {
            assert_eq!(completed_stage, FormulaStage::Support);
            assert_ne!(parent_stage, FormulaStage::Support);
            assert!(matches!(
                program.node(parent_node).kind,
                FiniteFormulaNodeKind::Or { .. }
            ));
            return Ok(InternedFormulaSuccessor::Guard {
                parent: address.parent,
                child: address.child,
            });
        }
        let stage = match (&program.node(parent_node).kind, parent_stage) {
            (FiniteFormulaNodeKind::And { .. }, FormulaStage::Propose) => FormulaStage::Confirm,
            _ => parent_stage,
        };
        let parent = self.intern_record(
            FormulaPcRecord {
                focus: FormulaFocus::Plan {
                    node: parent_node,
                    stage,
                    done: parent_done.with_inserted(address.child),
                },
                return_to: parent_return,
                resume: parent_resume,
            },
            grade
                .checked_add(1)
                .expect("residual formula grade overflow"),
        );
        Ok(InternedFormulaSuccessor::Formula(parent))
    }

    fn get(&self, id: FormulaPcId) -> &FormulaPcRecord {
        self.counters
            .get_index(id.0 as usize)
            .expect("interned residual formula program counter exists")
    }

    fn resume_by_id(&self, id: FormulaResumeId) -> &FormulaOuterResume {
        self.resumes
            .get_index(id.0 as usize)
            .expect("interned residual formula outer continuation exists")
    }

    fn resume(&self, id: FormulaPcId) -> &FormulaOuterResume {
        self.resume_by_id(self.get(id).resume)
    }

    fn return_by_id(&self, id: FormulaReturnId) -> &FormulaReturnRecord {
        self.returns
            .get_index(id.index())
            .expect("interned residual formula return address exists")
    }

    fn grade(&self, id: FormulaPcId) -> usize {
        self.grades[id.0 as usize]
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.counters.len()
    }

    #[cfg(test)]
    fn resume_len(&self) -> usize {
        self.resumes.len()
    }

    #[cfg(test)]
    fn return_len(&self) -> usize {
        self.returns.len()
    }

    /// Test oracle bridge back to the original flat structural PC. Production
    /// never materializes this representation.
    #[cfg(test)]
    fn materialize(&self, id: FormulaPcId) -> FormulaProgramCounter {
        let record = self.get(id);
        let mut returns = Vec::new();
        let mut current = id;
        while let Some(return_to) = self.get(current).return_to {
            let address = self.return_by_id(return_to);
            let parent = self.get(address.parent);
            let FormulaFocus::Plan { node, stage, done } = &parent.focus else {
                panic!("a formula return address named a non-Plan parent")
            };
            returns.push(FormulaReturnSite {
                kind: address.kind,
                parent: *node,
                parent_stage: *stage,
                child: address.child,
                done: done.clone(),
            });
            current = address.parent;
        }
        returns.reverse();
        FormulaProgramCounter {
            focus: record.focus.clone(),
            returns: returns.into_boxed_slice(),
            resume: self.resume(id).clone(),
        }
    }
}

#[derive(Clone, Default)]
struct StateInterner {
    // The insertion index is the stable StateId; hashing never determines IDs.
    descs: IndexSet<StateDesc, ahash::RandomState>,
    // FormulaPcId is meaningful only inside this exact state-interner. Query
    // clones and Rayon siblings clone the arena together with every descriptor
    // and payload that names it; independently advanced machines never remerge
    // worklists, only projected result values.
    formula_pcs: FormulaPcInterner,
}

impl StateInterner {
    fn start_formula(
        &mut self,
        program: &FiniteFormulaProgram,
        variable: VariableId,
        occurrence: usize,
        verb: UnionVerb,
    ) -> FormulaPcId {
        self.formula_pcs.start(program, variable, occurrence, verb)
    }

    fn formula(&self, id: FormulaPcId) -> &FormulaPcRecord {
        self.formula_pcs.get(id)
    }

    fn formula_resume(&self, id: FormulaPcId) -> &FormulaOuterResume {
        self.formula_pcs.resume(id)
    }

    /// Returns the exact ID and whether the descriptor was already interned.
    fn intern_with_status(
        &mut self,
        desc: StateDesc,
        stats: &mut ResidualStateStats,
    ) -> (StateId, bool) {
        // Preserve successful lookups after the representable StateId space is
        // full, while rejecting a genuinely new state before mutating the set.
        if self.descs.len() > u32::MAX as usize {
            if let Some(raw) = self.descs.get_index_of(&desc) {
                stats.interner_hits += 1;
                return (StateId(raw as u32), true);
            }
            panic!("too many residual states");
        }
        let (raw, inserted) = self.descs.insert_full(desc);
        let id = StateId(u32::try_from(raw).expect("too many residual states"));
        if inserted {
            stats.states_interned += 1;
            (id, false)
        } else {
            stats.interner_hits += 1;
            (id, true)
        }
    }

    fn get(&self, id: StateId) -> &StateDesc {
        self.descs
            .get_index(id.0 as usize)
            .expect("interned residual state exists")
    }
}

#[derive(Clone, Debug)]
struct RowBatch {
    rows: Vec<RawInline>,
    row_count: usize,
}

impl RowBatch {
    fn seed() -> Self {
        Self {
            rows: Vec::new(),
            row_count: 1,
        }
    }

    fn selected(&self, stride: usize, indices: &[usize]) -> Self {
        let mut rows = Vec::with_capacity(stride.saturating_mul(indices.len()));
        for &index in indices {
            let start = index * stride;
            rows.extend_from_slice(&self.rows[start..start + stride]);
        }
        Self {
            rows,
            row_count: indices.len(),
        }
    }

    fn append(&mut self, mut other: Self) {
        self.rows.append(&mut other.rows);
        self.row_count += other.row_count;
    }
}

/// Physical candidate representation, kept outside canonical state identity.
///
/// A live one-parent payload has no row-coordinate information to preserve, so
/// it stores the same plain value vector as the scalar DFS. Multi-parent
/// payloads use the block-native tagged COO representation. The scheduler
/// promotes only when independently affine parent domains reconverge and
/// normalizes back after a split or compaction leaves one parent.
#[derive(Clone, Debug)]
enum CandidatePayload {
    Values(Vec<RawInline>),
    Tagged(Candidates),
}

#[cfg(test)]
enum CandidatePayloadIter<'a> {
    Values(std::slice::Iter<'a, RawInline>),
    Tagged(std::slice::Iter<'a, (u32, RawInline)>),
}

#[cfg(test)]
impl<'a> Iterator for CandidatePayloadIter<'a> {
    type Item = (u32, &'a RawInline);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Values(values) => values.next().map(|value| (0, value)),
            Self::Tagged(pairs) => pairs.next().map(|(parent, value)| (*parent, value)),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Values(values) => values.size_hint(),
            Self::Tagged(pairs) => pairs.size_hint(),
        }
    }
}

impl CandidatePayload {
    fn empty(parent_count: usize) -> Self {
        if parent_count == 1 {
            Self::Values(Vec::new())
        } else {
            Self::Tagged(Vec::new())
        }
    }

    fn from_tagged(pairs: Candidates, parent_count: usize) -> Self {
        if parent_count == 1 {
            let mut values = Vec::with_capacity(pairs.len());
            for (parent, value) in pairs {
                assert_eq!(parent, 0, "one-parent candidate tag must be zero");
                values.push(value);
            }
            Self::Values(values)
        } else {
            if parent_count == 0 {
                assert!(pairs.is_empty(), "an empty parent shell carried candidates");
            }
            Self::Tagged(pairs)
        }
    }

    fn normalize_for(&mut self, parent_count: usize) {
        let payload = std::mem::replace(self, Self::Tagged(Vec::new()));
        *self = match (payload, parent_count) {
            (Self::Values(values), 1) => Self::Values(values),
            (Self::Tagged(pairs), 1) => Self::from_tagged(pairs, 1),
            (Self::Tagged(pairs), 0) => {
                assert!(pairs.is_empty(), "an empty parent shell carried candidates");
                Self::Tagged(pairs)
            }
            (Self::Values(values), 0) => {
                assert!(
                    values.is_empty(),
                    "an empty parent shell carried candidates"
                );
                Self::Tagged(Vec::new())
            }
            (Self::Tagged(pairs), _) => Self::Tagged(pairs),
            (Self::Values(values), _) => {
                let mut pairs = Vec::with_capacity(values.len());
                pairs.extend(values.into_iter().map(|value| (0, value)));
                Self::Tagged(pairs)
            }
        };
    }

    fn len(&self) -> usize {
        match self {
            Self::Values(values) => values.len(),
            Self::Tagged(pairs) => pairs.len(),
        }
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[cfg(test)]
    fn iter(&self) -> CandidatePayloadIter<'_> {
        match self {
            Self::Values(values) => CandidatePayloadIter::Values(values.iter()),
            Self::Tagged(pairs) => CandidatePayloadIter::Tagged(pairs.iter()),
        }
    }

    fn sink(&mut self, parent_count: usize) -> CandidateSink<'_> {
        self.debug_assert_valid_for(parent_count);
        match self {
            Self::Values(values) => {
                assert_eq!(parent_count, 1, "plain candidates require one parent");
                CandidateSink::Values(values)
            }
            Self::Tagged(pairs) => CandidateSink::Tagged(pairs),
        }
    }

    fn as_tagged_mut(&mut self) -> &mut Candidates {
        match self {
            Self::Tagged(pairs) => pairs,
            Self::Values(_) => panic!("tagged candidate payload required"),
        }
    }

    fn one_parent_values(&self) -> &[RawInline] {
        match self {
            Self::Values(values) => values,
            Self::Tagged(_) => panic!("one-parent payload was not normalized to plain values"),
        }
    }

    #[inline]
    fn all_parents_in(&self, parent_count: usize) -> bool {
        match self {
            Self::Values(_) => parent_count == 1,
            Self::Tagged(pairs) => pairs
                .iter()
                .all(|(parent, _)| (*parent as usize) < parent_count),
        }
    }

    #[inline]
    fn mark_live_parents(&self, live: &mut [bool]) {
        match self {
            Self::Values(values) => {
                assert_eq!(live.len(), 1, "plain candidates require one parent");
                live[0] = !values.is_empty();
            }
            Self::Tagged(pairs) => {
                for (parent, _) in pairs {
                    live[*parent as usize] = true;
                }
            }
        }
    }

    fn append_disjoint(&mut self, other: Self, left_parents: usize, right_parents: usize) {
        assert!(left_parents > 0 && right_parents > 0);
        self.debug_assert_valid_for(left_parents);
        other.debug_assert_valid_for(right_parents);
        let offset = u32::try_from(left_parents).expect("too many candidate parents");
        let left = std::mem::replace(self, Self::Tagged(Vec::new()));
        *self = match (left, other) {
            (Self::Values(left), Self::Values(right)) => {
                let mut pairs = Vec::with_capacity(left.len() + right.len());
                pairs.extend(left.into_iter().map(|value| (0, value)));
                pairs.extend(right.into_iter().map(|value| (offset, value)));
                Self::Tagged(pairs)
            }
            (Self::Values(left), Self::Tagged(right)) => {
                let mut pairs = Vec::with_capacity(left.len() + right.len());
                pairs.extend(left.into_iter().map(|value| (0, value)));
                pairs.extend(right.into_iter().map(|(parent, value)| {
                    (
                        parent.checked_add(offset).expect("candidate row overflow"),
                        value,
                    )
                }));
                Self::Tagged(pairs)
            }
            (Self::Tagged(mut left), Self::Values(right)) => {
                left.extend(right.into_iter().map(|value| (offset, value)));
                Self::Tagged(left)
            }
            (Self::Tagged(mut left), Self::Tagged(right)) => {
                left.extend(right.into_iter().map(|(parent, value)| {
                    (
                        parent.checked_add(offset).expect("candidate row overflow"),
                        value,
                    )
                }));
                Self::Tagged(left)
            }
        };
    }

    /// Appends candidates that already share the same affine parent domain.
    /// Formula OR reduction uses this operation; unlike bucket reconvergence,
    /// the right-hand row coordinates must not be shifted.
    fn extend_same_domain(&mut self, other: Self, parent_count: usize) {
        self.debug_assert_valid_for(parent_count);
        other.debug_assert_valid_for(parent_count);
        match (self, other) {
            (Self::Values(left), Self::Values(mut right)) => left.append(&mut right),
            (Self::Tagged(left), Self::Tagged(mut right)) => left.append(&mut right),
            _ => unreachable!("same parent domain selected incompatible candidate shapes"),
        }
    }

    fn sort_unstable(&mut self) {
        match self {
            Self::Values(values) => values.sort_unstable(),
            Self::Tagged(pairs) => pairs.sort_unstable(),
        }
    }

    fn is_sorted(&self) -> bool {
        match self {
            Self::Values(values) => values.is_sorted(),
            Self::Tagged(pairs) => pairs.is_sorted(),
        }
    }

    fn dedup(&mut self) {
        match self {
            Self::Values(values) => values.dedup(),
            Self::Tagged(pairs) => pairs.dedup(),
        }
    }

    fn take_parent_tail(&mut self, first: usize, parent_count: usize) -> Self {
        assert!(first > 0 && first < parent_count);
        self.debug_assert_valid_for(parent_count);
        let Self::Tagged(pairs) = self else {
            unreachable!("a partial parent split requires tagged candidates")
        };
        let cut = pairs.partition_point(|(parent, _)| (*parent as usize) < first);
        let mut tail = pairs.split_off(cut);
        let first_tag = u32::try_from(first).expect("too many candidate parents");
        for (parent, _) in &mut tail {
            *parent = parent
                .checked_sub(first_tag)
                .expect("candidate tail contained an earlier parent");
        }
        self.normalize_for(first);
        Self::from_tagged(tail, parent_count - first)
    }

    /// Splits a disjoint candidate-occurrence tail. Returns the tail payload,
    /// its first parent in the old domain, and the number of parents retained
    /// by the prefix. A one-parent Values payload stays Values on both sides.
    fn take_candidate_tail(&mut self, parent_count: usize, take: usize) -> (Self, usize, usize) {
        assert!(take > 0 && take < self.len());
        self.debug_assert_valid_for(parent_count);
        let cut = self.len() - take;
        match self {
            Self::Values(values) => {
                assert_eq!(parent_count, 1);
                (Self::Values(values.split_off(cut)), 0, 1)
            }
            Self::Tagged(pairs) => {
                let mut tail = pairs.split_off(cut);
                let first_tail_parent = tail[0].0 as usize;
                let prefix_parent_count = pairs.last().unwrap().0 as usize + 1;
                assert!(first_tail_parent < parent_count);
                assert!(prefix_parent_count <= first_tail_parent + 1);
                let first_tag =
                    u32::try_from(first_tail_parent).expect("too many candidate parents");
                for (parent, _) in &mut tail {
                    *parent = parent
                        .checked_sub(first_tag)
                        .expect("candidate tail contained an earlier parent");
                }
                self.normalize_for(prefix_parent_count);
                (
                    Self::from_tagged(tail, parent_count - first_tail_parent),
                    first_tail_parent,
                    prefix_parent_count,
                )
            }
        }
    }

    fn debug_assert_valid_for(&self, parent_count: usize) {
        match self {
            Self::Values(_) => debug_assert_eq!(parent_count, 1),
            Self::Tagged(pairs) => {
                debug_assert_ne!(parent_count, 1);
                debug_assert!(
                    pairs.windows(2).all(|pair| pair[0].0 <= pair[1].0),
                    "candidate tags must remain grouped by ascending parent"
                );
                debug_assert!(pairs
                    .iter()
                    .all(|(parent, _)| (*parent as usize) < parent_count));
            }
        }
    }

    #[cfg(test)]
    fn is_values(&self) -> bool {
        matches!(self, Self::Values(_))
    }

    #[cfg(test)]
    fn tagged_snapshot(&self) -> Candidates {
        self.iter()
            .map(|(parent, value)| (parent, *value))
            .collect()
    }
}

#[cfg(test)]
impl<T: ?Sized> PartialEq<T> for CandidatePayload
where
    T: AsRef<[(u32, RawInline)]>,
{
    fn eq(&self, other: &T) -> bool {
        self.iter().eq(other
            .as_ref()
            .iter()
            .map(|(parent, value)| (*parent, value)))
    }
}

#[derive(Clone, Debug)]
struct CandidateBatch {
    /// Committed parent bindings. The speculative variable is deliberately
    /// absent from this block and travels only in `candidates`.
    parents: RowBatch,
    /// Ragged candidates in the representation implied by `parents.row_count`.
    candidates: CandidatePayload,
}

impl CandidateBatch {
    fn candidate_count(&self) -> usize {
        self.candidates.len()
    }

    fn append(&mut self, other: Self) {
        let left_parents = self.parents.row_count;
        let right_parents = other.parents.row_count;
        self.parents.append(other.parents);
        self.candidates
            .append_disjoint(other.candidates, left_parents, right_parents);
    }

    /// Takes at most `width` complete parent atoms from the tail.
    ///
    /// A candidate-state atom is a parent row *and its entire ragged
    /// candidate group*. Confirmers such as `UnionConstraint` may sort and
    /// deduplicate within that group, so splitting the candidate vector at an
    /// arbitrary element would change semantics. Candidate tags are grouped
    /// by ascending parent throughout the protocol; the tail can therefore be
    /// cut once and remapped densely.
    fn take_tail(&mut self, stride: usize, width: usize) -> Self {
        let take = self.parents.row_count.min(width.max(1));
        debug_assert!(take > 0);
        if take == self.parents.row_count {
            return Self {
                parents: std::mem::replace(
                    &mut self.parents,
                    RowBatch {
                        rows: Vec::new(),
                        row_count: 0,
                    },
                ),
                candidates: std::mem::replace(
                    &mut self.candidates,
                    CandidatePayload::Tagged(Vec::new()),
                ),
            };
        }

        let first = self.parents.row_count - take;
        let tail_rows = self.parents.rows.split_off(first * stride);
        self.parents.row_count = first;

        let CandidatePayload::Tagged(pairs) = &mut self.candidates else {
            unreachable!("a partial parent split requires tagged candidates")
        };
        let candidate_cut = pairs.partition_point(|(row, _)| (*row as usize) < first);
        let mut candidates = pairs.split_off(candidate_cut);
        let first = u32::try_from(first).expect("too many candidate parents");
        for (row, _) in &mut candidates {
            *row = row
                .checked_sub(first)
                .expect("candidate tail contained a prefix row");
        }

        self.candidates.normalize_for(self.parents.row_count);
        let mut tail = Self {
            parents: RowBatch {
                rows: tail_rows,
                row_count: take,
            },
            candidates: CandidatePayload::Tagged(candidates),
        };
        tail.candidates.normalize_for(tail.parents.row_count);
        tail
    }

    /// Takes at most `width` candidate occurrences from the tail, allowing a
    /// parent group to be bisected. Callers must establish that every
    /// remaining confirmer is page-local before using this operation.
    fn take_candidate_tail(&mut self, stride: usize, width: usize) -> Self {
        let take = self.candidate_count().min(width.max(1));
        debug_assert!(take > 0);
        if take == self.candidate_count() {
            return Self {
                parents: std::mem::replace(
                    &mut self.parents,
                    RowBatch {
                        rows: Vec::new(),
                        row_count: 0,
                    },
                ),
                candidates: std::mem::replace(
                    &mut self.candidates,
                    CandidatePayload::Tagged(Vec::new()),
                ),
            };
        }

        let cut = self.candidate_count() - take;
        if let CandidatePayload::Values(values) = &mut self.candidates {
            assert_eq!(self.parents.row_count, 1);
            let tail_candidates = values.split_off(cut);
            return Self {
                parents: self.parents.clone(),
                candidates: CandidatePayload::Values(tail_candidates),
            };
        }
        let CandidatePayload::Tagged(pairs) = &mut self.candidates else {
            unreachable!()
        };
        let mut tail_candidates = pairs.split_off(cut);
        let first_tail_parent = tail_candidates[0].0 as usize;
        let prefix_parent_count = pairs.last().unwrap().0 as usize + 1;
        assert!(
            first_tail_parent < self.parents.row_count,
            "constraint emitted an invalid candidate row tag"
        );
        assert!(
            prefix_parent_count <= first_tail_parent + 1,
            "candidate tags must remain grouped by ascending parent"
        );

        // The prefix stays in place: no O(total-fanout) rescan or retag on a
        // width-one split. The tail copies only its parent suffix, including
        // the one binding duplicated when the cut bisects a parent group.
        let tail_rows = self.parents.rows[first_tail_parent * stride..].to_vec();
        let tail_parent_count = self.parents.row_count - first_tail_parent;
        let first_tail_parent = u32::try_from(first_tail_parent).expect("too many parents");
        for (parent, _) in &mut tail_candidates {
            *parent = parent
                .checked_sub(first_tail_parent)
                .expect("candidate tail contained an earlier parent");
        }
        self.parents.rows.truncate(prefix_parent_count * stride);
        self.parents.row_count = prefix_parent_count;
        self.candidates.normalize_for(prefix_parent_count);

        let mut tail = Self {
            parents: RowBatch {
                rows: tail_rows,
                row_count: tail_parent_count,
            },
            candidates: CandidatePayload::Tagged(tail_candidates),
        };
        tail.candidates.normalize_for(tail_parent_count);
        tail
    }

    /// Stable-partitions parents and their ragged candidate groups in one
    /// pass according to a per-parent leaf-occurrence assignment.
    fn partition<K>(self, stride: usize, assignment: &[K]) -> BTreeMap<K, Self>
    where
        K: Clone + Ord,
    {
        let RowBatch { rows, row_count } = self.parents;
        assert_eq!(assignment.len(), row_count);
        let mut remap = vec![u32::MAX; row_count];
        let mut groups: BTreeMap<K, Self> = BTreeMap::new();

        for (parent, child) in assignment.iter().enumerate() {
            let group = groups.entry(child.clone()).or_insert_with(|| Self {
                parents: RowBatch {
                    rows: Vec::new(),
                    row_count: 0,
                },
                candidates: CandidatePayload::Tagged(Vec::new()),
            });
            remap[parent] =
                u32::try_from(group.parents.row_count).expect("too many candidate parents");
            let start = parent * stride;
            group
                .parents
                .rows
                .extend_from_slice(&rows[start..start + stride]);
            group.parents.row_count += 1;
        }

        match self.candidates {
            CandidatePayload::Values(values) => {
                assert_eq!(row_count, 1, "plain candidates require one parent");
                let group = groups
                    .get_mut(&assignment[0])
                    .expect("the only parent assignment created its group");
                group.candidates = CandidatePayload::Values(values);
            }
            CandidatePayload::Tagged(pairs) => {
                for (parent, value) in pairs {
                    let parent = parent as usize;
                    assert!(
                        parent < row_count,
                        "constraint emitted an invalid candidate row tag"
                    );
                    groups
                        .get_mut(&assignment[parent])
                        .expect("every parent assignment created its group")
                        .candidates
                        .as_tagged_mut()
                        .push((remap[parent], value));
                }
            }
        }
        for group in groups.values_mut() {
            group.candidates.normalize_for(group.parents.row_count);
        }
        groups
    }

    /// Drops parents with no surviving candidates and densely remaps tags.
    fn compact(mut self, stride: usize) -> Option<Self> {
        self.candidates
            .debug_assert_valid_for(self.parents.row_count);
        if self.candidates.is_empty() {
            return None;
        }
        if matches!(self.candidates, CandidatePayload::Values(_)) {
            assert_eq!(self.parents.row_count, 1);
            return Some(self);
        }
        let CandidatePayload::Tagged(pairs) = &mut self.candidates else {
            unreachable!()
        };
        let parent_count = self.parents.row_count;
        let mut next_parent = 0usize;
        let mut no_gap = true;
        for &(row, _) in pairs.iter() {
            let row = row as usize;
            assert!(
                row < parent_count,
                "constraint emitted an invalid candidate row tag"
            );
            if no_gap {
                if row == next_parent {
                    next_parent += 1;
                } else if row > next_parent {
                    no_gap = false;
                }
            }
        }
        if next_parent == parent_count {
            // Candidate tags are grouped by parent. Seeing every tag in order
            // proves the block is already dense without a bitmap allocation.
            return Some(self);
        }

        let mut live = vec![false; parent_count];
        for &(row, _) in pairs.iter() {
            live[row as usize] = true;
        }
        let mut remap = vec![u32::MAX; parent_count];
        let mut indices = Vec::with_capacity(live.iter().filter(|&&x| x).count());
        for (old, is_live) in live.into_iter().enumerate() {
            if is_live {
                remap[old] = u32::try_from(indices.len()).expect("too many candidate parents");
                indices.push(old);
            }
        }
        self.parents = self.parents.selected(stride, &indices);
        for (row, _) in pairs.iter_mut() {
            *row = remap[*row as usize];
        }
        self.candidates.normalize_for(self.parents.row_count);
        Some(self)
    }

    fn into_parent_candidates(self) -> (RowBatch, Vec<Vec<RawInline>>) {
        let parent_count = self.parents.row_count;
        let groups = match self.candidates {
            CandidatePayload::Values(values) => {
                assert_eq!(parent_count, 1);
                vec![values]
            }
            CandidatePayload::Tagged(pairs) => {
                let mut groups = vec![Vec::new(); parent_count];
                let mut previous = 0;
                for (parent, value) in pairs {
                    let parent = parent as usize;
                    assert!(parent < parent_count, "invalid candidate parent tag");
                    assert!(parent >= previous, "candidate tags are not grouped");
                    previous = parent;
                    groups[parent].push(value);
                }
                groups
            }
        };
        assert!(
            groups.iter().all(|group| !group.is_empty()),
            "compacted candidate parent has no candidates"
        );
        (self.parents, groups)
    }
}

/// Stable payload identity for one affine parent entering a lowered formula.
///
/// Tokens are machine-local and never participate in canonical state
/// identity. They survive bucket append, planning partition, and parallel
/// split so each accumulator remains attached to exactly one parent even when
/// duplicate parent bindings are byte-identical.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct ActivationId(u64);

#[inline]
fn debug_assert_candidates_grouped(candidates: &CandidatePayload, parent_count: usize) {
    candidates.debug_assert_valid_for(parent_count);
}

#[derive(Clone, Debug)]
enum FormulaPayloadFrame {
    /// Every OR child reads the same immutable source and appends its result
    /// to one activation-private union accumulator. `source` follows the
    /// protocol's ascending-parent grouping; `output` is fully sorted after
    /// every child accumulation so frame completion can deduplicate in place.
    Or {
        source: CandidatePayload,
        output: CandidatePayload,
    },
    /// AND threads one ascending-parent-grouped candidate stream through its
    /// selected proposer and remaining confirmers. Empty current streams
    /// annihilate this branch without erasing the enclosing OR activation.
    And { current: CandidatePayload },
}

impl FormulaPayloadFrame {
    fn empty_like(&self, parent_count: usize) -> Self {
        match self {
            Self::Or { .. } => Self::Or {
                source: CandidatePayload::empty(parent_count),
                output: CandidatePayload::empty(parent_count),
            },
            Self::And { .. } => Self::And {
                current: CandidatePayload::empty(parent_count),
            },
        }
    }

    fn result(self, parent_count: usize) -> CandidatePayload {
        match self {
            Self::Or { mut output, .. } => {
                // OR is a set-valued reducer. Normalize at this frame's own
                // completion boundary so an enclosing AND never observes
                // duplicate candidates produced by sibling histories. Every
                // accumulation already sorted the complete output.
                output.debug_assert_valid_for(parent_count);
                debug_assert!(output.is_sorted());
                output.dedup();
                output
            }
            Self::And { current } => {
                current.debug_assert_valid_for(parent_count);
                current
            }
        }
    }
}

#[derive(Clone, Debug)]
struct FormulaBatch {
    activations: Vec<ActivationId>,
    parents: RowBatch,
    /// One activation-private reducer frame per live composite on the formula
    /// path. Frame shape follows the structural PC and therefore remains
    /// payload rather than canonical state identity.
    frames: Vec<FormulaPayloadFrame>,
}

impl FormulaBatch {
    fn root_frame(
        kind: &FiniteFormulaNodeKind,
        source: CandidatePayload,
        parent_count: usize,
    ) -> FormulaPayloadFrame {
        match kind {
            FiniteFormulaNodeKind::Or { .. } => FormulaPayloadFrame::Or {
                source,
                output: CandidatePayload::empty(parent_count),
            },
            // A root Atom uses the same single-stream payload as a one-child
            // conjunction. Nested atoms continue to operate directly on
            // their enclosing connective frame.
            FiniteFormulaNodeKind::And { .. } | FiniteFormulaNodeKind::Atom => {
                FormulaPayloadFrame::And { current: source }
            }
        }
    }

    fn from_proposal(
        parents: RowBatch,
        activations: Vec<ActivationId>,
        root_kind: &FiniteFormulaNodeKind,
    ) -> Self {
        assert_eq!(parents.row_count, activations.len());
        let parent_count = activations.len();
        Self {
            activations,
            parents,
            frames: vec![Self::root_frame(
                root_kind,
                CandidatePayload::empty(parent_count),
                parent_count,
            )],
        }
    }

    fn from_confirmation(
        batch: CandidateBatch,
        activations: Vec<ActivationId>,
        root_kind: &FiniteFormulaNodeKind,
    ) -> Self {
        assert_eq!(batch.parents.row_count, activations.len());
        // CandidateBatch inherits the protocol's ascending-parent grouping;
        // formula traversal and splitting require no stronger value order.
        debug_assert_candidates_grouped(&batch.candidates, batch.parents.row_count);
        let parent_count = batch.parents.row_count;
        Self {
            activations,
            parents: batch.parents,
            frames: vec![Self::root_frame(root_kind, batch.candidates, parent_count)],
        }
    }

    fn page_candidate_count(&self) -> usize {
        match self.frames.as_slice() {
            [FormulaPayloadFrame::And { current }] => current.len(),
            _ => 0,
        }
    }

    fn input(&self) -> &CandidatePayload {
        match self
            .frames
            .last()
            .expect("formula payload has no root frame")
        {
            FormulaPayloadFrame::Or { source, .. } => source,
            FormulaPayloadFrame::And { current } => current,
        }
    }

    fn action_candidate_count(&self, stage: FormulaStage) -> usize {
        match stage {
            FormulaStage::Support | FormulaStage::Propose => 0,
            FormulaStage::Confirm => self.input().len(),
        }
    }

    /// Applies one complete Atom action result to the focused reducer frame.
    /// Ordinary protocol execution and quiescent cyclic execution share this
    /// exact boundary so AND replaces its working stream while OR accumulates
    /// one independently evaluated arm.
    fn apply_action_result(&mut self, stage: FormulaStage, result: CandidatePayload) {
        assert_ne!(
            stage,
            FormulaStage::Support,
            "Boolean support never enters a candidate reducer frame"
        );
        // Both protocol verbs preserve ascending parent groups. AND needs
        // only that grouping; OR sorts once after combining sibling arms.
        debug_assert_candidates_grouped(&result, self.parents.row_count);
        match (self.frames.last_mut().unwrap(), stage) {
            (
                FormulaPayloadFrame::Or { output, .. },
                FormulaStage::Propose | FormulaStage::Confirm,
            ) => {
                output.extend_same_domain(result, self.parents.row_count);
                output.sort_unstable();
            }
            (FormulaPayloadFrame::And { current }, FormulaStage::Propose) => {
                assert!(current.is_empty(), "an AND ran two proposers");
                *current = result;
            }
            (FormulaPayloadFrame::And { current }, FormulaStage::Confirm) => {
                *current = result;
            }
            (_, FormulaStage::Support) => unreachable!("support was rejected above"),
        }
    }

    fn validate_tags(&self) {
        assert!(
            self.frames.iter().all(|frame| match frame {
                FormulaPayloadFrame::Or { source, output } => {
                    source.all_parents_in(self.parents.row_count)
                        && output.all_parents_in(self.parents.row_count)
                }
                FormulaPayloadFrame::And { current } => {
                    current.all_parents_in(self.parents.row_count)
                }
            }),
            "formula action emitted an invalid candidate row tag"
        );
    }

    fn enter(&mut self, kind: &FiniteFormulaNodeKind, stage: FormulaStage) {
        let input = match stage {
            FormulaStage::Support => {
                panic!("Boolean support does not allocate candidate reducer frames")
            }
            FormulaStage::Propose => CandidatePayload::empty(self.parents.row_count),
            FormulaStage::Confirm => self.input().clone(),
        };
        self.frames.push(match kind {
            FiniteFormulaNodeKind::And { .. } => FormulaPayloadFrame::And { current: input },
            FiniteFormulaNodeKind::Or { .. } => FormulaPayloadFrame::Or {
                source: input,
                output: CandidatePayload::empty(self.parents.row_count),
            },
            FiniteFormulaNodeKind::Atom => panic!("an Atom cannot own a formula payload frame"),
        });
    }

    fn return_frame(&mut self) {
        assert!(
            self.frames.len() >= 2,
            "the root formula frame cannot return"
        );
        let result = self
            .frames
            .pop()
            .expect("a returning formula node has a payload frame")
            .result(self.parents.row_count);
        match self
            .frames
            .last_mut()
            .expect("a returning formula node has a parent frame")
        {
            FormulaPayloadFrame::Or { output, .. } => {
                output.extend_same_domain(result, self.parents.row_count);
                output.sort_unstable();
            }
            FormulaPayloadFrame::And { current } => *current = result,
        }
    }

    fn current_is_live(&self) -> Vec<bool> {
        let FormulaPayloadFrame::And { current } = self
            .frames
            .last()
            .expect("formula payload has no current frame")
        else {
            panic!("only an AND frame has annihilating current streams")
        };
        let mut live = vec![false; self.parents.row_count];
        current.mark_live_parents(&mut live);
        live
    }

    fn append(&mut self, mut other: Self) {
        let left_parents = self.parents.row_count;
        let right_parents = other.parents.row_count;
        self.parents.append(other.parents);
        self.activations.append(&mut other.activations);
        assert_eq!(self.frames.len(), other.frames.len());

        for (left, right) in self.frames.iter_mut().zip(other.frames) {
            match (left, right) {
                (
                    FormulaPayloadFrame::Or {
                        source: left_source,
                        output: left_output,
                    },
                    FormulaPayloadFrame::Or {
                        source: right_source,
                        output: right_output,
                    },
                ) => {
                    left_source.append_disjoint(right_source, left_parents, right_parents);
                    left_output.append_disjoint(right_output, left_parents, right_parents);
                }
                (
                    FormulaPayloadFrame::And {
                        current: left_current,
                    },
                    FormulaPayloadFrame::And {
                        current: right_current,
                    },
                ) => left_current.append_disjoint(right_current, left_parents, right_parents),
                _ => panic!("one formula state received incompatible payload-frame shapes"),
            }
        }
    }

    fn take_tail(&mut self, stride: usize, width: usize) -> Self {
        let take = self.parents.row_count.min(width.max(1));
        debug_assert!(take > 0);
        if take == self.parents.row_count {
            return Self {
                activations: std::mem::take(&mut self.activations),
                parents: std::mem::replace(
                    &mut self.parents,
                    RowBatch {
                        rows: Vec::new(),
                        row_count: 0,
                    },
                ),
                frames: self
                    .frames
                    .iter_mut()
                    .map(|frame| std::mem::replace(frame, frame.empty_like(0)))
                    .collect(),
            };
        }

        let first = self.parents.row_count - take;
        let rows = self.parents.rows.split_off(first * stride);
        self.parents.row_count = first;
        let activations = self.activations.split_off(first);

        let old_parent_count = first + take;
        let frames = self
            .frames
            .iter_mut()
            .map(|frame| match frame {
                FormulaPayloadFrame::Or { source, output } => FormulaPayloadFrame::Or {
                    source: source.take_parent_tail(first, old_parent_count),
                    output: output.take_parent_tail(first, old_parent_count),
                },
                FormulaPayloadFrame::And { current } => FormulaPayloadFrame::And {
                    current: current.take_parent_tail(first, old_parent_count),
                },
            })
            .collect();
        Self {
            activations,
            parents: RowBatch {
                rows,
                row_count: take,
            },
            frames,
        }
    }

    /// Takes a disjoint tail of candidate occurrences from a synthetic root
    /// AND. The active PC proves that every remaining confirmer is page-local
    /// before the scheduler calls this. A bisected parent is copied into both
    /// pages, while each speculative candidate remains affine to one page.
    fn take_candidate_tail(&mut self, stride: usize, width: usize) -> Self {
        assert_eq!(
            self.frames.len(),
            1,
            "only a synthetic root AND may expose candidate pages"
        );
        let FormulaPayloadFrame::And { current } = &mut self.frames[0] else {
            panic!("only a synthetic root AND may expose candidate pages")
        };
        let take = current.len().min(width.max(1));
        debug_assert!(take > 0);
        if take == current.len() {
            return Self {
                activations: std::mem::take(&mut self.activations),
                parents: std::mem::replace(
                    &mut self.parents,
                    RowBatch {
                        rows: Vec::new(),
                        row_count: 0,
                    },
                ),
                frames: vec![FormulaPayloadFrame::And {
                    current: std::mem::replace(current, CandidatePayload::Tagged(Vec::new())),
                }],
            };
        }

        let (tail_candidates, first_tail_parent, prefix_parent_count) =
            current.take_candidate_tail(self.parents.row_count, take);
        assert!(
            first_tail_parent < self.parents.row_count,
            "constraint emitted an invalid candidate row tag"
        );
        assert!(
            prefix_parent_count <= first_tail_parent + 1,
            "candidate tags must remain grouped by ascending parent"
        );

        let tail_rows = self.parents.rows[first_tail_parent * stride..].to_vec();
        let tail_parent_count = self.parents.row_count - first_tail_parent;
        let tail_activations = self.activations[first_tail_parent..].to_vec();
        self.parents.rows.truncate(prefix_parent_count * stride);
        self.parents.row_count = prefix_parent_count;
        self.activations.truncate(prefix_parent_count);

        Self {
            activations: tail_activations,
            parents: RowBatch {
                rows: tail_rows,
                row_count: tail_parent_count,
            },
            frames: vec![FormulaPayloadFrame::And {
                current: tail_candidates,
            }],
        }
    }

    fn partition<K>(self, stride: usize, assignment: &[K]) -> BTreeMap<K, Self>
    where
        K: Clone + Ord,
    {
        assert_eq!(assignment.len(), self.parents.row_count);
        assert_eq!(self.activations.len(), self.parents.row_count);
        if let Some(first) = assignment.first() {
            if assignment.iter().all(|key| key == first) {
                return BTreeMap::from([(first.clone(), self)]);
            }
        }
        let RowBatch { rows, row_count } = self.parents;
        let mut remap = vec![u32::MAX; row_count];
        let mut groups: BTreeMap<K, Self> = BTreeMap::new();
        let empty_frames: Vec<_> = self
            .frames
            .iter()
            .map(|frame| frame.empty_like(0))
            .collect();

        for (parent, (child, activation)) in assignment
            .iter()
            .zip(self.activations.into_iter())
            .enumerate()
        {
            let group = groups.entry(child.clone()).or_insert_with(|| Self {
                activations: Vec::new(),
                parents: RowBatch {
                    rows: Vec::new(),
                    row_count: 0,
                },
                frames: empty_frames.clone(),
            });
            remap[parent] = u32::try_from(group.parents.row_count)
                .expect("too many partitioned formula parents");
            let start = parent * stride;
            group
                .parents
                .rows
                .extend_from_slice(&rows[start..start + stride]);
            group.parents.row_count += 1;
            group.activations.push(activation);
        }

        fn partition_values<K>(
            values: CandidatePayload,
            assignment: &[K],
            remap: &[u32],
            groups: &mut BTreeMap<K, FormulaBatch>,
            frame: usize,
            field: usize,
        ) where
            K: Clone + Ord,
        {
            let CandidatePayload::Tagged(pairs) = values else {
                unreachable!("a non-uniform formula partition requires multiple parents")
            };
            for (parent, value) in pairs {
                let parent = parent as usize;
                let target = groups
                    .get_mut(&assignment[parent])
                    .expect("every formula assignment created its group");
                match (&mut target.frames[frame], field) {
                    (FormulaPayloadFrame::Or { source, .. }, 0) => {
                        source.as_tagged_mut().push((remap[parent], value))
                    }
                    (FormulaPayloadFrame::Or { output, .. }, 1) => {
                        output.as_tagged_mut().push((remap[parent], value))
                    }
                    (FormulaPayloadFrame::And { current }, 0) => {
                        current.as_tagged_mut().push((remap[parent], value))
                    }
                    _ => panic!("formula frame field disagrees with its structural shape"),
                }
            }
        }
        for (frame, payload) in self.frames.into_iter().enumerate() {
            match payload {
                FormulaPayloadFrame::Or { source, output } => {
                    partition_values(source, assignment, &remap, &mut groups, frame, 0);
                    partition_values(output, assignment, &remap, &mut groups, frame, 1);
                }
                FormulaPayloadFrame::And { current } => {
                    partition_values(current, assignment, &remap, &mut groups, frame, 0);
                }
            }
        }
        for group in groups.values_mut() {
            let parent_count = group.parents.row_count;
            for frame in &mut group.frames {
                match frame {
                    FormulaPayloadFrame::Or { source, output } => {
                        source.normalize_for(parent_count);
                        output.normalize_for(parent_count);
                    }
                    FormulaPayloadFrame::And { current } => {
                        current.normalize_for(parent_count);
                    }
                }
            }
        }
        groups
    }

    /// Moves every affine parent, including its complete reducer-frame stack,
    /// into a one-parent payload suitable for activation-local cyclic work.
    /// Candidate tags are normalized to zero by the ordinary partition path.
    fn into_singletons(self, stride: usize) -> Vec<Self> {
        let parent_count = self.parents.row_count;
        let assignment: Vec<_> = (0..parent_count).collect();
        let groups = self.partition(stride, &assignment);
        assert_eq!(groups.len(), parent_count);
        groups.into_values().collect()
    }

    fn finish(mut self) -> CandidateBatch {
        assert_eq!(self.frames.len(), 1);
        let root = self.frames.pop().unwrap();
        CandidateBatch {
            parents: self.parents,
            candidates: root.result(self.activations.len()),
        }
    }
}

#[derive(Clone, Debug)]
enum StateBucket {
    Rows(RowBatch),
    Candidates(CandidateBatch),
    Formula(FormulaBatch),
}

impl StateBucket {
    fn row_count(&self) -> usize {
        match self {
            StateBucket::Rows(rows) => rows.row_count,
            StateBucket::Candidates(batch) => batch.parents.row_count,
            StateBucket::Formula(batch) => batch.parents.row_count,
        }
    }

    /// Scheduling occupancy. Row-bearing phases are measured in parent rows;
    /// once a candidate continuation is entirely page-local, its actionable
    /// atoms are candidate occurrences instead.
    fn occupancy(&self, candidate_pages: bool) -> usize {
        match self {
            StateBucket::Candidates(batch) if candidate_pages => batch.candidate_count(),
            StateBucket::Formula(batch) if candidate_pages => batch.page_candidate_count(),
            _ => self.row_count(),
        }
    }

    fn append(&mut self, other: Self) {
        match (self, other) {
            (StateBucket::Rows(left), StateBucket::Rows(right)) => left.append(right),
            (StateBucket::Candidates(left), StateBucket::Candidates(right)) => left.append(right),
            (StateBucket::Formula(left), StateBucket::Formula(right)) => left.append(right),
            _ => panic!("one canonical residual state received incompatible payloads"),
        }
    }

    /// Bisects one affine payload into two independently executable shards.
    ///
    /// Row phases split on row boundaries. Candidate phases split either on
    /// complete parent groups or, once the exact residual continuation is
    /// page-local, on candidate-occurrence boundaries. The latter may copy
    /// one parent binding into both shards, but every speculative candidate
    /// remains owned by exactly one side.
    #[cfg(feature = "parallel")]
    fn split_for_parallel(&mut self, stride: usize, candidate_pages: bool) -> Option<Self> {
        match self {
            StateBucket::Rows(batch) if batch.row_count >= 2 => {
                let right_rows = batch.row_count / 2;
                Some(self.take_tail(stride, right_rows, false))
            }
            StateBucket::Candidates(batch) if candidate_pages && batch.candidate_count() >= 2 => {
                let right_candidates = batch.candidate_count() / 2;
                Some(self.take_tail(stride, right_candidates, true))
            }
            StateBucket::Candidates(batch) if !candidate_pages && batch.parents.row_count >= 2 => {
                let right_parents = batch.parents.row_count / 2;
                Some(self.take_tail(stride, right_parents, false))
            }
            StateBucket::Formula(batch) if candidate_pages && batch.page_candidate_count() >= 2 => {
                let right_candidates = batch.page_candidate_count() / 2;
                Some(self.take_tail(stride, right_candidates, true))
            }
            StateBucket::Formula(batch) if batch.parents.row_count >= 2 => {
                let right_parents = batch.parents.row_count / 2;
                Some(self.take_tail(stride, right_parents, false))
            }
            StateBucket::Rows(_) | StateBucket::Candidates(_) | StateBucket::Formula(_) => None,
        }
    }

    /// Removes a tail chunk without bisecting a candidate parent group.
    fn take_tail(&mut self, stride: usize, width: usize, candidate_pages: bool) -> Self {
        match self {
            StateBucket::Rows(batch) => {
                let take = batch.row_count.min(width.max(1));
                debug_assert!(take > 0);
                if take == batch.row_count {
                    return StateBucket::Rows(std::mem::replace(
                        batch,
                        RowBatch {
                            rows: Vec::new(),
                            row_count: 0,
                        },
                    ));
                }
                let first = batch.row_count - take;
                let rows = batch.rows.split_off(first * stride);
                batch.row_count = first;
                StateBucket::Rows(RowBatch {
                    rows,
                    row_count: take,
                })
            }
            StateBucket::Candidates(batch) if candidate_pages => {
                StateBucket::Candidates(batch.take_candidate_tail(stride, width))
            }
            StateBucket::Candidates(batch) => {
                StateBucket::Candidates(batch.take_tail(stride, width))
            }
            StateBucket::Formula(batch) if candidate_pages => {
                StateBucket::Formula(batch.take_candidate_tail(stride, width))
            }
            StateBucket::Formula(batch) => StateBucket::Formula(batch.take_tail(stride, width)),
        }
    }
}

/// Exact protocol verb selected by one concrete residual action state.
///
/// The leaf is a compiled action occurrence, not a constraint address. Outer
/// opaque actions use their residual-plan occurrence; synthetic-root formula
/// atoms use their fresh formula-node occurrence. Together with
/// [`ResidualActionTask::state`], it identifies both the concrete call and the
/// complete canonical continuation that owns it.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum ResidualAction {
    Support { variable: VariableId, leaf: usize },
    Propose { variable: VariableId, leaf: usize },
    Confirm { variable: VariableId, leaf: usize },
}

/// Executor-facing description of one concrete residual protocol action.
///
/// This is deliberately scheduler-owned and hardware-neutral. It records the
/// exact interned state/action identity plus the geometry already known at the
/// dispatch boundary. It does not quote cost, read a clock, or extend the
/// constraint protocol. Planning-only Ready and Candidate states never
/// produce this description.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ResidualActionTask {
    state: StateId,
    action: ResidualAction,
    /// Exact committed row schema. Its cardinality is the physical column
    /// count, while the variable IDs prevent unlike schemas with equal width
    /// from becoming one executor cohort.
    bound: VariableSet,
    /// Number of parent rows presented to the protocol call.
    parent_rows: usize,
    /// Number of candidate occurrences presented to Confirm; zero for
    /// Support/Propose.
    candidate_occurrences: usize,
    /// Scheduler occupancy consumed by this action. This is parent rows until
    /// the remaining confirmation suffix is page-local, then candidate
    /// occurrences.
    action_atoms: usize,
}

impl ResidualActionTask {
    fn observation(self) -> (ActionSite, ActionGeometry) {
        let (verb, variable, leaf_occurrence) = match self.action {
            ResidualAction::Support { variable, leaf } => (ActionVerb::Support, variable, leaf),
            ResidualAction::Propose { variable, leaf } => (ActionVerb::Propose, variable, leaf),
            ResidualAction::Confirm { variable, leaf } => (ActionVerb::Confirm, variable, leaf),
        };
        // `self.state` remains scheduler-private. It is exact only within one
        // interner and is deliberately not copied into the public observation.
        let _local_state = self.state;
        (
            ActionSite {
                verb,
                variable,
                leaf_occurrence,
                bound: self.bound,
            },
            ActionGeometry {
                parent_rows: self.parent_rows,
                candidate_occurrences: self.candidate_occurrences,
                action_atoms: self.action_atoms,
            },
        )
    }
}

/// One affine payload selected from the residual worklist.
///
/// Selection used to return only `(StateDesc, StateBucket)`, discarding the
/// interner identity before dispatch. Keeping all three pieces together gives
/// an executor a stable ownership boundary without changing state identity,
/// worklist order, or protocol semantics.
#[derive(Debug)]
struct SelectedResidualTask {
    state: StateId,
    desc: StateDesc,
    bucket: StateBucket,
}

impl SelectedResidualTask {
    /// Cheap phase classification used by the latency scheduler. It must not
    /// materialize executor geometry on the default path.
    fn is_action_for_plan(&self, plan: &ResidualPlan, interner: &StateInterner) -> bool {
        match &self.desc.phase {
            ResidualPhase::Propose { proposer, .. } => !plan.has_finite_formula(*proposer),
            ResidualPhase::Confirm { confirmer, .. } => !plan.has_finite_formula(*confirmer),
            ResidualPhase::Formula { counter } => {
                matches!(
                    &interner.formula(*counter).focus,
                    FormulaFocus::Action { .. }
                )
            }
            ResidualPhase::Ready | ResidualPhase::Candidate { .. } => false,
        }
    }

    #[cfg(test)]
    fn is_action(&self, interner: &StateInterner) -> bool {
        match &self.desc.phase {
            ResidualPhase::Propose { .. } | ResidualPhase::Confirm { .. } => true,
            ResidualPhase::Formula { counter } => matches!(
                &interner.formula(*counter).focus,
                FormulaFocus::Action { .. }
            ),
            ResidualPhase::Ready | ResidualPhase::Candidate { .. } => false,
        }
    }

    /// Returns executor geometry only for a concrete protocol action.
    #[allow(dead_code)]
    fn action_task(
        &self,
        plan: &ResidualPlan,
        interner: &StateInterner,
    ) -> Option<ResidualActionTask> {
        let (action, candidate_occurrences) = match (&self.desc.phase, &self.bucket) {
            (
                ResidualPhase::Propose {
                    variable, proposer, ..
                },
                StateBucket::Rows(_),
            ) if !plan.has_finite_formula(*proposer) => (
                ResidualAction::Propose {
                    variable: *variable,
                    leaf: *proposer,
                },
                0,
            ),
            (
                ResidualPhase::Confirm {
                    variable,
                    confirmer,
                    ..
                },
                StateBucket::Candidates(batch),
            ) if !plan.has_finite_formula(*confirmer) => (
                ResidualAction::Confirm {
                    variable: *variable,
                    leaf: *confirmer,
                },
                batch.candidate_count(),
            ),
            (ResidualPhase::Formula { counter }, StateBucket::Formula(batch)) => {
                let record = interner.formula(*counter);
                let FormulaFocus::Action { node, stage } = &record.focus else {
                    return None;
                };
                let resume = interner.formula_resume(*counter);
                let occurrence = plan.formula_action_occurrence(resume.occurrence, *node);
                let (action, candidates) = match stage {
                    FormulaStage::Support => (
                        ResidualAction::Support {
                            variable: resume.variable,
                            leaf: occurrence,
                        },
                        0,
                    ),
                    FormulaStage::Propose => (
                        ResidualAction::Propose {
                            variable: resume.variable,
                            leaf: occurrence,
                        },
                        0,
                    ),
                    FormulaStage::Confirm => (
                        ResidualAction::Confirm {
                            variable: resume.variable,
                            leaf: occurrence,
                        },
                        batch.action_candidate_count(*stage),
                    ),
                };
                (action, candidates)
            }
            (ResidualPhase::Ready | ResidualPhase::Candidate { .. }, _) => return None,
            (ResidualPhase::Propose { proposer, .. }, StateBucket::Rows(_))
                if plan.has_finite_formula(*proposer) =>
            {
                return None;
            }
            (ResidualPhase::Confirm { confirmer, .. }, StateBucket::Candidates(_))
                if plan.has_finite_formula(*confirmer) =>
            {
                return None;
            }
            (
                ResidualPhase::Propose { .. }
                | ResidualPhase::Confirm { .. }
                | ResidualPhase::Formula { .. },
                _,
            ) => {
                panic!("canonical residual action received the wrong payload shape")
            }
        };
        let candidate_pages = self.desc.uses_candidate_pages(plan, &interner.formula_pcs);
        Some(ResidualActionTask {
            state: self.state,
            action,
            bound: self.desc.bound,
            parent_rows: self.bucket.row_count(),
            candidate_occurrences,
            action_atoms: self.bucket.occupancy(candidate_pages),
        })
    }
}

type Worklist = BTreeMap<usize, BTreeMap<StateId, StateBucket>>;

/// Physical tail receipt from one transition into a canonical state.
///
/// This token is deliberately absent from [`StateDesc`] and the interner: two
/// histories with identical future computation retain one semantic state even
/// while the lazy scheduler temporarily keeps the newly advanced cohort hot.
/// Single-threaded filing appends at the payload tail. Equal `(rank, state)`
/// receipts in one transition reduction are coalesced by occupancy, so the
/// selected token names their complete appended tail without consuming older
/// work already present under the same canonical key.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ContinuationToken {
    rank: usize,
    state: StateId,
    rows: usize,
    candidates: usize,
}

/// Physical scheduling policy for one exact continuation receipt.
///
/// `Cohort` preserves the ordinary action sprint. `ProbeOne` is reserved for
/// delta-to-stable handoffs: it selects one promising atom, then hands that
/// atom's ordered fanout back to `Cohort`, without resetting the query-wide
/// cold-harvest width or making width part of canonical state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ContinuationMode {
    Cohort,
    ProbeOne,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ActiveContinuation {
    token: ContinuationToken,
    mode: ContinuationMode,
}

impl ActiveContinuation {
    fn cohort(token: ContinuationToken) -> Self {
        Self {
            token,
            mode: ContinuationMode::Cohort,
        }
    }

    fn probe_one(token: ContinuationToken) -> Self {
        Self {
            token,
            mode: ContinuationMode::ProbeOne,
        }
    }
}

impl ContinuationToken {
    fn occupancy(
        self,
        desc: &StateDesc,
        plan: &ResidualPlan,
        formula_pcs: &FormulaPcInterner,
    ) -> usize {
        if desc.uses_candidate_pages(plan, formula_pcs) {
            self.candidates
        } else {
            self.rows
        }
    }

    fn scheduling_key(self) -> (usize, StateId) {
        (self.rank, self.state)
    }
}

fn prefer_continuation(
    selected: &mut Option<ContinuationToken>,
    candidate: Option<ContinuationToken>,
) {
    let Some(candidate) = candidate else {
        return;
    };
    let Some(current) = selected else {
        *selected = Some(candidate);
        return;
    };
    match candidate.scheduling_key().cmp(&current.scheduling_key()) {
        std::cmp::Ordering::Less => {}
        std::cmp::Ordering::Greater => *current = candidate,
        std::cmp::Ordering::Equal => {
            // No scheduler pop can interleave one continuation reduction.
            // Equal keys therefore name successive appends to the same bucket,
            // and checked addition is its exact newly filed tail occupancy.
            let rows = current
                .rows
                .checked_add(candidate.rows)
                .expect("continuation receipt row occupancy overflow");
            let candidates = current
                .candidates
                .checked_add(candidate.candidates)
                .expect("continuation receipt candidate occupancy overflow");
            current.rows = rows;
            current.candidates = candidates;
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SelectionKind {
    Full,
    Readiness,
    Continuation(ContinuationMode),
}

fn rows_view<'v>(vars: &'v [VariableId], rows: &'v [RawInline], row_count: usize) -> RowsView<'v> {
    RowsView::new_with_row_count(vars, rows, row_count)
}

fn file_with_span(
    worklist: &mut Worklist,
    interner: &mut StateInterner,
    leaf_count: usize,
    action_span: usize,
    formula: Option<&FiniteFormulaProgram>,
    desc: StateDesc,
    bucket: StateBucket,
    stats: &mut ResidualStateStats,
) -> Option<ContinuationToken> {
    let rows = bucket.row_count();
    if rows == 0 {
        return None;
    }
    let candidates = match &bucket {
        StateBucket::Rows(_) => 0,
        StateBucket::Candidates(batch) => batch.candidate_count(),
        StateBucket::Formula(batch) => batch.page_candidate_count(),
    };
    let rank = desc.rank_with_span(leaf_count, action_span, formula, &interner.formula_pcs);
    let (id, known) = interner.intern_with_status(desc, stats);
    let level = worklist.entry(rank).or_default();
    if let Some(existing) = level.get_mut(&id) {
        stats.bucket_merges += 1;
        stats.rows_merged += rows;
        existing.append(bucket);
    } else {
        if known {
            stats.state_reentries += 1;
            stats.rows_reentered += rows;
        }
        level.insert(id, bucket);
    }
    Some(ContinuationToken {
        rank,
        state: id,
        rows,
        candidates,
    })
}

fn file_with_plan(
    worklist: &mut Worklist,
    interner: &mut StateInterner,
    plan: &ResidualPlan,
    desc: StateDesc,
    bucket: StateBucket,
    stats: &mut ResidualStateStats,
) -> Option<ContinuationToken> {
    file_with_span(
        worklist,
        interner,
        plan.len(),
        plan.action_span(),
        Some(&plan.finite_formula),
        desc,
        bucket,
        stats,
    )
}

#[cfg(test)]
fn file(
    worklist: &mut Worklist,
    interner: &mut StateInterner,
    leaf_count: usize,
    desc: StateDesc,
    bucket: StateBucket,
    stats: &mut ResidualStateStats,
) -> Option<ContinuationToken> {
    file_with_span(worklist, interner, leaf_count, 2, None, desc, bucket, stats)
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ProposeAction {
    variable_plan: usize,
    leaf: usize,
}

struct VariablePlan {
    variable: VariableId,
    relevant: ChildSet,
    /// Tightest flattened leaf occurrence per row.
    proposers: Vec<usize>,
}

fn estimate_leaf<'a>(
    root: &dyn Constraint<'a>,
    plan: &ResidualPlan,
    leaf: usize,
    variable: VariableId,
    view: &RowsView<'_>,
    out: &mut EstimateSink<'_>,
) -> bool {
    plan.resolve(root, leaf).estimate(variable, view, out)
}

fn propose_leaf<'a>(
    root: &dyn Constraint<'a>,
    plan: &ResidualPlan,
    leaf: usize,
    variable: VariableId,
    view: &RowsView<'_>,
    candidates: &mut CandidateSink<'_>,
) {
    plan.resolve(root, leaf).propose(variable, view, candidates);
}

fn allocate_activations(next: &mut u64, count: usize) -> Vec<ActivationId> {
    let count = u64::try_from(count).expect("too many formula activations");
    let end = next
        .checked_add(count)
        .expect("residual formula activation ID overflow");
    let activations = (*next..end).map(ActivationId).collect();
    *next = end;
    activations
}

fn confirm_leaf<'a>(
    root: &dyn Constraint<'a>,
    plan: &ResidualPlan,
    leaf: usize,
    variable: VariableId,
    view: &RowsView<'_>,
    candidates: &mut CandidateSink<'_>,
) {
    plan.resolve(root, leaf).confirm(variable, view, candidates);
}

fn ready_plan_transition<'a>(
    root: &dyn Constraint<'a>,
    plan: &ResidualPlan,
    desc: &StateDesc,
    rows: RowBatch,
    full: VariableSet,
    influences: &[VariableSet; 128],
    base_estimates: &[usize; 128],
    worklist: &mut Worklist,
    interner: &mut StateInterner,
    stats: &mut ResidualStateStats,
) -> ContinuationToken {
    let leaf_count = plan.len();
    let vars: Vec<VariableId> = desc.bound.into_iter().collect();
    let view = rows_view(&vars, &rows.rows, rows.row_count);
    let unbound: Vec<VariableId> = full.subtract(desc.bound).into_iter().collect();
    let mut plans = Vec::with_capacity(unbound.len());
    let mut estimate_matrix = Vec::with_capacity(unbound.len() * rows.row_count);

    for &variable in &unbound {
        let mut relevant = ChildSet::empty(leaf_count);
        let mut proposers = vec![usize::MAX; rows.row_count];
        let estimate_start = estimate_matrix.len();
        estimate_matrix.resize(estimate_start + rows.row_count, usize::MAX);
        let estimates = &mut estimate_matrix[estimate_start..];
        let mut column = Vec::with_capacity(rows.row_count);
        for leaf in 0..leaf_count {
            column.clear();
            let is_relevant = estimate_leaf(
                root,
                plan,
                leaf,
                variable,
                &view,
                &mut EstimateSink::Column(&mut column),
            );
            if is_relevant {
                assert_eq!(
                    column.len(),
                    rows.row_count,
                    "constraint estimate must append one value per row"
                );
                relevant.insert(leaf);
                for row in 0..rows.row_count {
                    if proposers[row] == usize::MAX || column[row] < estimates[row] {
                        proposers[row] = leaf;
                        estimates[row] = column[row];
                    }
                }
            } else {
                assert_eq!(
                    column.len(),
                    0,
                    "irrelevant constraint estimate must leave its sink untouched"
                );
            }
        }
        assert!(
            proposers.iter().all(|&child| child != usize::MAX),
            "unconstrained variable in residual-state query"
        );
        plans.push(VariablePlan {
            variable,
            relevant,
            proposers,
        });
    }

    let mut preferred = Vec::with_capacity(rows.row_count);
    let mut preferred_counts = vec![0; plans.len()];
    for row in 0..rows.row_count {
        let mut best: Option<(usize, (u64, u64, u64))> = None;
        for (pi, plan) in plans.iter().enumerate() {
            let estimate = estimate_matrix[pi * rows.row_count + row];
            let key = variable_order_key(
                estimate,
                base_estimates[plan.variable],
                influences[plan.variable].count(),
            );
            if best.is_none_or(|(_, best_key)| key > best_key) {
                best = Some((pi, key));
            }
        }
        let variable_plan = best
            .expect("a non-full ready state has an enabled proposal")
            .0;
        preferred.push(variable_plan as u32);
        preferred_counts[variable_plan] += 1;
    }

    let preferred_groups = preferred_counts.iter().filter(|&&count| count > 0).count();
    let mut scheduled = preferred.clone();
    let mut scheduled_groups = preferred_groups;
    if preferred_groups > 1 {
        let mut owners = Vec::new();
        let mut group_sums = Vec::new();
        let mut compatible = Vec::new();
        let mut active = Vec::new();
        let plan = plan_agglomerative_partition(
            &estimate_matrix,
            rows.row_count,
            &unbound,
            influences,
            &preferred,
            &preferred_counts,
            &mut owners,
            &mut scheduled,
            &mut group_sums,
            &mut compatible,
            &mut active,
        );
        debug_assert_eq!(plan.preferred_groups, preferred_groups);
        scheduled_groups = plan.scheduled_groups;
        if scheduled_groups < preferred_groups {
            stats.agglomerated_ready_pops += 1;
        }
    }
    stats.ready_preferred_variable_groups += preferred_groups;
    stats.ready_scheduled_variable_groups += scheduled_groups;

    let mut groups: BTreeMap<ProposeAction, Vec<usize>> = BTreeMap::new();
    for (row, &variable_plan) in scheduled.iter().enumerate() {
        let variable_plan = variable_plan as usize;
        let action = ProposeAction {
            variable_plan,
            leaf: plans[variable_plan].proposers[row],
        };
        groups.entry(action).or_default().push(row);
    }
    stats.ready_proposal_groups += groups.len();

    let mut file_propose_group = |action: ProposeAction, selected: RowBatch| {
        let variable_plan = &plans[action.variable_plan];
        file_with_plan(
            worklist,
            interner,
            plan,
            StateDesc {
                bound: desc.bound,
                phase: ResidualPhase::Propose {
                    variable: variable_plan.variable,
                    relevant: variable_plan.relevant.clone(),
                    proposer: action.leaf,
                },
            },
            StateBucket::Rows(selected),
            stats,
        )
    };

    if groups.len() == 1 {
        let (action, indices) = groups.pop_first().expect("one proposal group was observed");
        debug_assert_eq!(indices.len(), rows.row_count);
        // The common case transfers ownership of the whole parent block:
        // no row copy is necessary when every row chose the same action.
        file_propose_group(action, rows).expect("Ready planning filed an empty action")
    } else {
        let mut continuation = None;
        for (action, indices) in groups {
            let selected = rows.selected(vars.len(), &indices);
            prefer_continuation(&mut continuation, file_propose_group(action, selected));
        }
        continuation.expect("Ready planning filed no action")
    }
}

fn propose_action_transition<'a>(
    root: &dyn Constraint<'a>,
    plan: &ResidualPlan,
    desc: &StateDesc,
    variable: VariableId,
    relevant: &ChildSet,
    proposer: usize,
    rows: RowBatch,
    next_activation: &mut u64,
    worklist: &mut Worklist,
    interner: &mut StateInterner,
    stats: &mut ResidualStateStats,
) -> Option<ContinuationToken> {
    let leaf_count = plan.len();
    if plan.has_finite_formula(proposer) {
        let activations = allocate_activations(next_activation, rows.row_count);
        let formula_root = plan
            .finite_formula
            .root(proposer)
            .expect("a lowered formula has a root");
        let counter = interner.start_formula(
            &plan.finite_formula,
            variable,
            proposer,
            UnionVerb::Propose {
                relevant: relevant.clone(),
            },
        );
        return file_with_plan(
            worklist,
            interner,
            plan,
            StateDesc {
                bound: desc.bound,
                phase: ResidualPhase::Formula { counter },
            },
            StateBucket::Formula(FormulaBatch::from_proposal(
                rows,
                activations,
                &plan.finite_formula.node(formula_root).kind,
            )),
            stats,
        );
    }
    let vars: Vec<VariableId> = desc.bound.into_iter().collect();
    let view = rows_view(&vars, &rows.rows, rows.row_count);
    let mut candidates = CandidatePayload::empty(rows.row_count);
    propose_leaf(
        root,
        plan,
        proposer,
        variable,
        &view,
        &mut candidates.sink(rows.row_count),
    );
    stats.propose_calls += 1;
    stats.propose_rows += rows.row_count;
    stats.max_propose_rows = stats.max_propose_rows.max(rows.row_count);
    stats.candidates_proposed += candidates.len();
    stats.max_propose_candidates = stats.max_propose_candidates.max(candidates.len());

    let mut checked = ChildSet::empty(leaf_count);
    checked.insert(proposer);
    let candidate = CandidateBatch {
        parents: rows,
        candidates,
    };
    if let Some(candidate) = candidate.compact(vars.len()) {
        file_with_plan(
            worklist,
            interner,
            plan,
            StateDesc {
                bound: desc.bound,
                phase: ResidualPhase::Candidate {
                    variable,
                    relevant: relevant.clone(),
                    checked,
                },
            },
            StateBucket::Candidates(candidate),
            stats,
        )
    } else {
        None
    }
}

fn commit_candidates(
    plan: &ResidualPlan,
    desc: &StateDesc,
    variable: VariableId,
    batch: CandidateBatch,
    worklist: &mut Worklist,
    interner: &mut StateInterner,
    stats: &mut ResidualStateStats,
) -> Option<ContinuationToken> {
    let parent_vars: Vec<VariableId> = desc.bound.into_iter().collect();
    let mut next_bound = desc.bound;
    next_bound.set(variable);
    let next_vars: Vec<VariableId> = next_bound.into_iter().collect();
    let mut next_rows = Vec::with_capacity(batch.candidates.len() * next_vars.len());

    let mut commit_one = |parent: usize, candidate: RawInline| {
        let parent = parent as usize;
        let parent_row =
            &batch.parents.rows[parent * parent_vars.len()..(parent + 1) * parent_vars.len()];
        let mut source = 0usize;
        for &column_variable in &next_vars {
            if column_variable == variable {
                next_rows.push(candidate);
            } else {
                next_rows.push(parent_row[source]);
                source += 1;
            }
        }
    };
    match batch.candidates {
        CandidatePayload::Values(values) => {
            assert_eq!(batch.parents.row_count, 1);
            for candidate in values {
                commit_one(0, candidate);
            }
        }
        CandidatePayload::Tagged(pairs) => {
            for (parent, candidate) in pairs {
                commit_one(parent as usize, candidate);
            }
        }
    }

    let row_count = if next_vars.is_empty() {
        0
    } else {
        next_rows.len() / next_vars.len()
    };
    file_with_plan(
        worklist,
        interner,
        plan,
        StateDesc {
            bound: next_bound,
            phase: ResidualPhase::Ready,
        },
        StateBucket::Rows(RowBatch {
            rows: next_rows,
            row_count,
        }),
        stats,
    )
}

fn candidate_plan_transition<'a>(
    root: &dyn Constraint<'a>,
    plan: &ResidualPlan,
    desc: &StateDesc,
    variable: VariableId,
    relevant: &ChildSet,
    checked: &ChildSet,
    batch: CandidateBatch,
    worklist: &mut Worklist,
    interner: &mut StateInterner,
    stats: &mut ResidualStateStats,
) -> ContinuationToken {
    let leaf_count = plan.len();
    if relevant == checked {
        return commit_candidates(plan, desc, variable, batch, worklist, interner, stats)
            .expect("fully checked candidates committed no rows");
    }

    let vars: Vec<VariableId> = desc.bound.into_iter().collect();
    let view = rows_view(&vars, &batch.parents.rows, batch.parents.row_count);
    let mut confirmers = vec![usize::MAX; batch.parents.row_count];
    let mut estimates = vec![usize::MAX; batch.parents.row_count];
    let mut column = Vec::with_capacity(batch.parents.row_count);
    for leaf in 0..leaf_count {
        if !relevant.contains(leaf) || checked.contains(leaf) {
            continue;
        }
        column.clear();
        let is_relevant = estimate_leaf(
            root,
            plan,
            leaf,
            variable,
            &view,
            &mut EstimateSink::Column(&mut column),
        );
        assert!(
            is_relevant,
            "a relevant child became irrelevant before the candidate was committed"
        );
        assert_eq!(
            column.len(),
            batch.parents.row_count,
            "constraint estimate must append one value per row"
        );
        for row in 0..batch.parents.row_count {
            if confirmers[row] == usize::MAX || column[row] < estimates[row] {
                confirmers[row] = leaf;
                estimates[row] = column[row];
            }
        }
    }
    assert!(
        confirmers.iter().all(|&child| child != usize::MAX),
        "candidate state has no enabled transition"
    );
    let mut confirmer_groups = ChildSet::empty(leaf_count);
    for &confirmer in &confirmers {
        confirmer_groups.insert(confirmer);
    }
    stats.candidate_confirmation_groups += confirmer_groups.count();

    let mut file_confirm_group = |confirmer: usize, selected: CandidateBatch| {
        file_with_plan(
            worklist,
            interner,
            plan,
            StateDesc {
                bound: desc.bound,
                phase: ResidualPhase::Confirm {
                    variable,
                    relevant: relevant.clone(),
                    checked: checked.clone(),
                    confirmer,
                },
            },
            StateBucket::Candidates(selected),
            stats,
        )
    };

    let first = confirmers[0];
    if confirmers.iter().all(|&leaf| leaf == first) {
        // The common case keeps ownership of the whole ragged block: no
        // parent copy, candidate rescan, or row-tag remap is necessary.
        file_confirm_group(first, batch).expect("Candidate planning filed an empty action")
    } else {
        let mut continuation = None;
        for (leaf, selected) in batch.partition(vars.len(), &confirmers) {
            prefer_continuation(&mut continuation, file_confirm_group(leaf, selected));
        }
        continuation.expect("Candidate planning filed no action")
    }
}

fn confirm_action_transition<'a>(
    root: &dyn Constraint<'a>,
    plan: &ResidualPlan,
    desc: &StateDesc,
    variable: VariableId,
    relevant: &ChildSet,
    checked: &ChildSet,
    confirmer: usize,
    mut batch: CandidateBatch,
    next_activation: &mut u64,
    worklist: &mut Worklist,
    interner: &mut StateInterner,
    stats: &mut ResidualStateStats,
) -> Option<ContinuationToken> {
    if plan.has_finite_formula(confirmer) {
        let activations = allocate_activations(next_activation, batch.parents.row_count);
        let formula_root = plan
            .finite_formula
            .root(confirmer)
            .expect("a lowered formula has a root");
        let counter = interner.start_formula(
            &plan.finite_formula,
            variable,
            confirmer,
            UnionVerb::Confirm {
                relevant: relevant.clone(),
                checked: checked.clone(),
            },
        );
        return file_with_plan(
            worklist,
            interner,
            plan,
            StateDesc {
                bound: desc.bound,
                phase: ResidualPhase::Formula { counter },
            },
            StateBucket::Formula(FormulaBatch::from_confirmation(
                batch,
                activations,
                &plan.finite_formula.node(formula_root).kind,
            )),
            stats,
        );
    }
    let vars: Vec<VariableId> = desc.bound.into_iter().collect();
    let view = rows_view(&vars, &batch.parents.rows, batch.parents.row_count);
    let candidates_before = batch.candidates.len();
    confirm_leaf(
        root,
        plan,
        confirmer,
        variable,
        &view,
        &mut batch.candidates.sink(batch.parents.row_count),
    );
    stats.confirm_calls += 1;
    stats.confirm_rows += batch.parents.row_count;
    stats.max_confirm_rows = stats.max_confirm_rows.max(batch.parents.row_count);
    stats.candidates_confirmed += candidates_before;
    stats.max_confirm_candidates = stats.max_confirm_candidates.max(candidates_before);

    if let Some(batch) = batch.compact(vars.len()) {
        file_with_plan(
            worklist,
            interner,
            plan,
            StateDesc {
                bound: desc.bound,
                phase: ResidualPhase::Candidate {
                    variable,
                    relevant: relevant.clone(),
                    checked: checked.with_inserted(confirmer),
                },
            },
            StateBucket::Candidates(batch),
            stats,
        )
    } else {
        None
    }
}

fn finish_formula_transition(
    plan: &ResidualPlan,
    desc: &StateDesc,
    resume: &FormulaOuterResume,
    batch: FormulaBatch,
    worklist: &mut Worklist,
    interner: &mut StateInterner,
    stats: &mut ResidualStateStats,
) -> Option<ContinuationToken> {
    let leaf_count = plan.len();
    let (relevant, checked) = match &resume.verb {
        UnionVerb::Propose { relevant } => {
            let mut checked = ChildSet::empty(leaf_count);
            checked.insert(resume.occurrence);
            (relevant.clone(), checked)
        }
        UnionVerb::Confirm { relevant, checked } => {
            (relevant.clone(), checked.with_inserted(resume.occurrence))
        }
    };
    let stride = desc.bound.count();
    let candidate = batch.finish().compact(stride)?;
    file_with_plan(
        worklist,
        interner,
        plan,
        StateDesc {
            bound: desc.bound,
            phase: ResidualPhase::Candidate {
                variable: resume.variable,
                relevant,
                checked,
            },
        },
        StateBucket::Candidates(candidate),
        stats,
    )
}

/// Propagates one transient Boolean support result through its connective
/// continuation. The truth value partitions payload rows but never becomes
/// part of a canonical descriptor: only the future computation is filed.
#[allow(clippy::too_many_arguments)]
fn propagate_formula_support(
    plan: &ResidualPlan,
    desc: &StateDesc,
    completed: FormulaPcId,
    truth: bool,
    batch: FormulaBatch,
    worklist: &mut Worklist,
    interner: &mut StateInterner,
    stats: &mut ResidualStateStats,
) -> Option<ContinuationToken> {
    assert!(matches!(
        interner.formula(completed).focus,
        FormulaFocus::Complete {
            stage: FormulaStage::Support,
            ..
        }
    ));
    let successor = interner
        .formula_pcs
        .resume_completed(&plan.finite_formula, completed);
    match successor {
        Ok(InternedFormulaSuccessor::Formula(parent)) => {
            let (node, done_count) = match &interner.formula(parent).focus {
                FormulaFocus::Plan {
                    node,
                    stage: FormulaStage::Support,
                    done,
                } => (*node, done.count()),
                _ => unreachable!("support child resumed a non-support Plan"),
            };
            let node = plan.finite_formula.node(node);
            let (decisive, identity) = match node.kind {
                FiniteFormulaNodeKind::And { .. } => (!truth, true),
                FiniteFormulaNodeKind::Or { .. } => (truth, false),
                FiniteFormulaNodeKind::Atom => {
                    unreachable!("support child resumed an Atom parent")
                }
            };
            if decisive {
                let completed = interner.formula_pcs.complete_support_short_circuit(
                    &plan.finite_formula,
                    parent,
                    truth,
                );
                return propagate_formula_support(
                    plan, desc, completed, truth, batch, worklist, interner, stats,
                );
            }
            if done_count
                == node
                    .children()
                    .expect("support Plan parent has children")
                    .len()
            {
                let completed = interner.formula_pcs.complete(&plan.finite_formula, parent);
                return propagate_formula_support(
                    plan, desc, completed, identity, batch, worklist, interner, stats,
                );
            }
            file_with_plan(
                worklist,
                interner,
                plan,
                StateDesc {
                    bound: desc.bound,
                    phase: ResidualPhase::Formula { counter: parent },
                },
                StateBucket::Formula(batch),
                stats,
            )
        }
        Ok(InternedFormulaSuccessor::Guard { parent, child }) => {
            let next = if truth {
                interner
                    .formula_pcs
                    .select_supported_child(&plan.finite_formula, parent, child)
            } else {
                interner
                    .formula_pcs
                    .skip_child(&plan.finite_formula, parent, child)
            };
            let mut batch = batch;
            if truth {
                enter_selected_formula_frame(
                    &plan.finite_formula,
                    interner.formula(next),
                    &mut batch,
                );
            }
            continue_formula_transition(plan, desc, next, batch, worklist, interner, stats)
        }
        Err(_) => {
            unreachable!("support traversal escaped without an OR guard")
        }
    }
}

fn continue_formula_transition(
    plan: &ResidualPlan,
    desc: &StateDesc,
    mut counter: FormulaPcId,
    batch: FormulaBatch,
    worklist: &mut Worklist,
    interner: &mut StateInterner,
    stats: &mut ResidualStateStats,
) -> Option<ContinuationToken> {
    let support_complete = match &interner.formula(counter).focus {
        FormulaFocus::Plan {
            node,
            stage: FormulaStage::Support,
            done,
        } => Some((*node, done.count())),
        _ => None,
    };
    if let Some((node, done_count)) = support_complete {
        let formula_node = plan.finite_formula.node(node);
        let children = formula_node
            .children()
            .expect("a support Plan named an Atom");
        if done_count == children.len() {
            let truth = matches!(formula_node.kind, FiniteFormulaNodeKind::And { .. });
            let completed = interner.formula_pcs.complete(&plan.finite_formula, counter);
            return propagate_formula_support(
                plan, desc, completed, truth, batch, worklist, interner, stats,
            );
        }
    }

    let confirm_node = match interner.formula(counter).focus {
        FormulaFocus::Plan {
            node,
            stage: FormulaStage::Confirm,
            ..
        } => Some(node),
        _ => None,
    };
    if let Some(node) = confirm_node {
        if matches!(
            plan.finite_formula.node(node).kind,
            FiniteFormulaNodeKind::And { .. }
        ) {
            let live = batch.current_is_live();
            let first = live[0];
            if live.iter().any(|&is_live| is_live != first) {
                let mut continuation = None;
                for (_, batch) in batch.partition(desc.bound.count(), &live) {
                    prefer_continuation(
                        &mut continuation,
                        continue_formula_transition(
                            plan, desc, counter, batch, worklist, interner, stats,
                        ),
                    );
                }
                return continuation;
            }
            if !first {
                loop {
                    let child = {
                        let FormulaFocus::Plan { node, done, .. } =
                            &interner.formula(counter).focus
                        else {
                            unreachable!("annihilation preserves an AND Plan")
                        };
                        let children = plan
                            .finite_formula
                            .node(*node)
                            .children()
                            .expect("an AND Plan has children");
                        (0..children.len()).find(|&child| !done.contains(child))
                    };
                    let Some(child) = child else {
                        break;
                    };
                    counter = interner
                        .formula_pcs
                        .skip_child(&plan.finite_formula, counter, child);
                }
            }
        }
    }

    let complete = match &interner.formula(counter).focus {
        FormulaFocus::Plan { node, done, .. } => {
            let children = plan
                .finite_formula
                .node(*node)
                .children()
                .expect("a formula Plan named an Atom");
            done.count() == children.len()
        }
        FormulaFocus::Action { .. } => false,
        FormulaFocus::Complete { .. } => {
            panic!("a completed formula was filed as a live continuation")
        }
    };
    if !complete {
        return file_with_plan(
            worklist,
            interner,
            plan,
            StateDesc {
                bound: desc.bound,
                phase: ResidualPhase::Formula { counter },
            },
            StateBucket::Formula(batch),
            stats,
        );
    }

    let completed = interner.formula_pcs.complete(&plan.finite_formula, counter);
    match interner
        .formula_pcs
        .resume_completed(&plan.finite_formula, completed)
    {
        Ok(InternedFormulaSuccessor::Formula(next)) => {
            let mut batch = batch;
            batch.return_frame();
            continue_formula_transition(plan, desc, next, batch, worklist, interner, stats)
        }
        Ok(InternedFormulaSuccessor::Guard { .. }) => {
            unreachable!("ordinary formula completion returned through a support guard")
        }
        Err(resume) => {
            finish_formula_transition(plan, desc, &resume, batch, worklist, interner, stats)
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn formula_plan_transition<'a>(
    root: &dyn Constraint<'a>,
    plan: &ResidualPlan,
    desc: &StateDesc,
    counter: FormulaPcId,
    batch: FormulaBatch,
    worklist: &mut Worklist,
    interner: &mut StateInterner,
    stats: &mut ResidualStateStats,
) -> Option<ContinuationToken> {
    let (node, stage) = match &interner.formula(counter).focus {
        FormulaFocus::Plan { node, stage, .. } => (*node, *stage),
        _ => panic!("formula planning received an action continuation"),
    };
    if stage == FormulaStage::Support {
        return formula_support_plan_transition(
            plan, desc, counter, batch, worklist, interner, stats,
        );
    }
    match &plan.finite_formula.node(node).kind {
        FiniteFormulaNodeKind::Or { children } => formula_or_plan_transition(
            root, plan, desc, counter, children, batch, worklist, interner, stats,
        ),
        FiniteFormulaNodeKind::And { children } => formula_and_plan_transition(
            root, plan, desc, counter, children, batch, worklist, interner, stats,
        ),
        FiniteFormulaNodeKind::Atom => panic!("a formula Plan named an Atom"),
    }
}

#[allow(clippy::too_many_arguments)]
fn formula_support_plan_transition(
    plan: &ResidualPlan,
    desc: &StateDesc,
    counter: FormulaPcId,
    batch: FormulaBatch,
    worklist: &mut Worklist,
    interner: &mut StateInterner,
    stats: &mut ResidualStateStats,
) -> Option<ContinuationToken> {
    let (node, done) = match &interner.formula(counter).focus {
        FormulaFocus::Plan {
            node,
            stage: FormulaStage::Support,
            done,
        } => (*node, done.clone()),
        _ => unreachable!("support planning received a non-support continuation"),
    };
    let children = plan
        .finite_formula
        .node(node)
        .children()
        .expect("a support Plan named an Atom");
    assert!(done.is_valid_for(children.len()));
    let Some(child) = (0..children.len()).find(|&child| !done.contains(child)) else {
        return continue_formula_transition(plan, desc, counter, batch, worklist, interner, stats);
    };
    let next = select_interned_formula_child(
        &plan.finite_formula,
        &mut interner.formula_pcs,
        counter,
        children,
        child,
    );
    continue_formula_transition(plan, desc, next, batch, worklist, interner, stats)
}

fn select_interned_formula_child(
    program: &FiniteFormulaProgram,
    formula_pcs: &mut FormulaPcInterner,
    counter: FormulaPcId,
    children: &[FormulaNodeId],
    child: usize,
) -> FormulaPcId {
    match &program.node(children[child]).kind {
        FiniteFormulaNodeKind::Atom => formula_pcs.select_child_as_action(program, counter, child),
        FiniteFormulaNodeKind::And { .. } | FiniteFormulaNodeKind::Or { .. } => {
            formula_pcs.select_child(program, counter, child)
        }
    }
}

fn enter_selected_formula_frame(
    program: &FiniteFormulaProgram,
    counter: &FormulaPcRecord,
    batch: &mut FormulaBatch,
) {
    if let FormulaFocus::Plan { node, stage, .. } = &counter.focus {
        batch.enter(&program.node(*node).kind, *stage);
    }
}

#[allow(clippy::too_many_arguments)]
fn formula_or_plan_transition<'a>(
    root: &dyn Constraint<'a>,
    plan: &ResidualPlan,
    desc: &StateDesc,
    counter: FormulaPcId,
    children: &[FormulaNodeId],
    batch: FormulaBatch,
    worklist: &mut Worklist,
    interner: &mut StateInterner,
    stats: &mut ResidualStateStats,
) -> Option<ContinuationToken> {
    let resume = interner.formula_resume(counter);
    let (done, occurrence, variable) = match &interner.formula(counter).focus {
        FormulaFocus::Plan { done, .. } => (done.clone(), resume.occurrence, resume.variable),
        _ => unreachable!("OR planning received an action continuation"),
    };
    assert!(matches!(
        batch.frames.last(),
        Some(FormulaPayloadFrame::Or { .. })
    ));
    let child_count = children.len();
    assert!(
        done.is_valid_for(child_count),
        "residual formula progress names a non-child occurrence"
    );

    let vars: Vec<VariableId> = desc.bound.into_iter().collect();
    let view = rows_view(&vars, &batch.parents.rows, batch.parents.row_count);
    let first_undone = (0..child_count)
        .find(|&child| !done.contains(child))
        .expect("unfinished formula has an enabled child");
    let mut assignments = vec![first_undone; batch.parents.row_count];
    let mut estimates = vec![usize::MAX; batch.parents.row_count];
    let mut column = Vec::with_capacity(batch.parents.row_count);
    for child in 0..child_count {
        if done.contains(child) {
            continue;
        }
        column.clear();
        if plan
            .resolve_formula_node(root, occurrence, children[child])
            .estimate(variable, &view, &mut EstimateSink::Column(&mut column))
        {
            assert_eq!(
                column.len(),
                batch.parents.row_count,
                "formula action estimate must append one value per row"
            );
            for parent in 0..batch.parents.row_count {
                if column[parent] < estimates[parent] {
                    estimates[parent] = column[parent];
                    assignments[parent] = child;
                }
            }
        } else {
            assert!(
                column.is_empty(),
                "irrelevant formula action estimate must leave its sink untouched"
            );
        }
    }

    let mut continuation = None;
    for (child, batch) in batch.partition(vars.len(), &assignments) {
        let next = interner
            .formula_pcs
            .guard_child(&plan.finite_formula, counter, child);
        prefer_continuation(
            &mut continuation,
            continue_formula_transition(plan, desc, next, batch, worklist, interner, stats),
        );
    }
    continuation
}

#[allow(clippy::too_many_arguments)]
fn formula_and_plan_transition<'a>(
    root: &dyn Constraint<'a>,
    plan: &ResidualPlan,
    desc: &StateDesc,
    counter: FormulaPcId,
    children: &[FormulaNodeId],
    batch: FormulaBatch,
    worklist: &mut Worklist,
    interner: &mut StateInterner,
    stats: &mut ResidualStateStats,
) -> Option<ContinuationToken> {
    let resume = interner.formula_resume(counter);
    let (done, occurrence, variable) = match &interner.formula(counter).focus {
        FormulaFocus::Plan { done, .. } => (done.clone(), resume.occurrence, resume.variable),
        _ => unreachable!("AND planning received an action continuation"),
    };
    assert!(matches!(
        batch.frames.last(),
        Some(FormulaPayloadFrame::And { .. })
    ));
    assert!(done.is_valid_for(children.len()));

    let vars: Vec<VariableId> = desc.bound.into_iter().collect();
    let view = rows_view(&vars, &batch.parents.rows, batch.parents.row_count);
    let mut next = counter;
    let mut estimates_by_child = Vec::new();
    for child in 0..children.len() {
        if done.contains(child) {
            continue;
        }
        let mut column = Vec::with_capacity(batch.parents.row_count);
        if plan
            .resolve_formula_node(root, occurrence, children[child])
            .estimate(variable, &view, &mut EstimateSink::Column(&mut column))
        {
            assert_eq!(
                column.len(),
                batch.parents.row_count,
                "AND child estimate must append one value per row"
            );
            estimates_by_child.push((child, column));
        } else {
            assert!(
                column.is_empty(),
                "irrelevant AND child estimate must leave its sink untouched"
            );
            next = interner
                .formula_pcs
                .skip_child(&plan.finite_formula, next, child);
        }
    }

    let done_count = match &interner.formula(next).focus {
        FormulaFocus::Plan { done, .. } => done.count(),
        _ => unreachable!("AND relevance planning preserves a Plan"),
    };
    if done_count == children.len() {
        return continue_formula_transition(plan, desc, next, batch, worklist, interner, stats);
    }
    let first = estimates_by_child
        .first()
        .expect("an unfinished AND has a relevant child")
        .0;
    let mut assignments = vec![first; batch.parents.row_count];
    let mut best = vec![usize::MAX; batch.parents.row_count];
    for (child, column) in estimates_by_child {
        for parent in 0..batch.parents.row_count {
            if column[parent] < best[parent] {
                best[parent] = column[parent];
                assignments[parent] = child;
            }
        }
    }

    let mut continuation = None;
    for (child, mut batch) in batch.partition(vars.len(), &assignments) {
        let selected = select_interned_formula_child(
            &plan.finite_formula,
            &mut interner.formula_pcs,
            next,
            children,
            child,
        );
        enter_selected_formula_frame(&plan.finite_formula, interner.formula(selected), &mut batch);
        prefer_continuation(
            &mut continuation,
            continue_formula_transition(plan, desc, selected, batch, worklist, interner, stats),
        );
    }
    continuation
}

#[allow(clippy::too_many_arguments)]
fn finish_formula_action_result(
    plan: &ResidualPlan,
    bound: VariableSet,
    counter: FormulaPcId,
    mut batch: FormulaBatch,
    result: CandidatePayload,
    worklist: &mut Worklist,
    interner: &mut StateInterner,
    stats: &mut ResidualStateStats,
) -> Option<ContinuationToken> {
    let stage = match &interner.formula(counter).focus {
        FormulaFocus::Action { stage, .. } => *stage,
        _ => panic!("formula result received a planning continuation"),
    };
    batch.apply_action_result(stage, result);
    batch.validate_tags();

    let completed = interner.formula_pcs.complete(&plan.finite_formula, counter);
    let desc = StateDesc {
        bound,
        phase: ResidualPhase::Formula { counter },
    };
    match interner
        .formula_pcs
        .resume_completed(&plan.finite_formula, completed)
    {
        Ok(InternedFormulaSuccessor::Formula(next)) => {
            continue_formula_transition(plan, &desc, next, batch, worklist, interner, stats)
        }
        Ok(InternedFormulaSuccessor::Guard { .. }) => {
            unreachable!("candidate action returned through a support guard")
        }
        Err(resume) => {
            finish_formula_transition(plan, &desc, &resume, batch, worklist, interner, stats)
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn formula_action_transition<'a>(
    root: &dyn Constraint<'a>,
    plan: &ResidualPlan,
    desc: &StateDesc,
    counter: FormulaPcId,
    batch: FormulaBatch,
    worklist: &mut Worklist,
    interner: &mut StateInterner,
    stats: &mut ResidualStateStats,
) -> Option<ContinuationToken> {
    let resume = interner.formula_resume(counter);
    let (node, stage, occurrence, variable) = match &interner.formula(counter).focus {
        FormulaFocus::Action { node, stage } => (*node, *stage, resume.occurrence, resume.variable),
        _ => panic!("formula action received a planning continuation"),
    };
    assert_eq!(batch.activations.len(), batch.parents.row_count);

    let vars: Vec<VariableId> = desc.bound.into_iter().collect();
    let view = rows_view(&vars, &batch.parents.rows, batch.parents.row_count);
    let constraint = plan.resolve_formula_node(root, occurrence, node);
    if stage == FormulaStage::Support {
        let support: Vec<bool> = (0..batch.parents.row_count)
            .map(|parent| constraint.satisfied(&view.row_view(parent)))
            .collect();
        stats.support_calls += 1;
        stats.support_rows += batch.parents.row_count;
        stats.max_support_rows = stats.max_support_rows.max(batch.parents.row_count);
        let completed = interner.formula_pcs.complete(&plan.finite_formula, counter);
        let mut continuation = None;
        for (truth, batch) in batch.partition(vars.len(), &support) {
            prefer_continuation(
                &mut continuation,
                propagate_formula_support(
                    plan, desc, completed, truth, batch, worklist, interner, stats,
                ),
            );
        }
        return continuation;
    }

    let mut result = match stage {
        FormulaStage::Support => unreachable!("support returned above"),
        FormulaStage::Propose => CandidatePayload::empty(batch.parents.row_count),
        FormulaStage::Confirm => batch.input().clone(),
    };
    let candidates_before = result.len();
    match stage {
        FormulaStage::Support => unreachable!("support returned above"),
        FormulaStage::Propose => {
            constraint.propose(variable, &view, &mut result.sink(batch.parents.row_count));
            stats.candidates_proposed += result.len();
            stats.max_propose_candidates = stats.max_propose_candidates.max(result.len());
        }
        FormulaStage::Confirm => {
            constraint.confirm(variable, &view, &mut result.sink(batch.parents.row_count));
            stats.candidates_confirmed += candidates_before;
            stats.max_confirm_candidates = stats.max_confirm_candidates.max(candidates_before);
        }
    }
    match stage {
        FormulaStage::Support => unreachable!("support returned above"),
        FormulaStage::Propose => {
            stats.propose_calls += 1;
            stats.propose_rows += batch.parents.row_count;
            stats.max_propose_rows = stats.max_propose_rows.max(batch.parents.row_count);
        }
        FormulaStage::Confirm => {
            stats.confirm_calls += 1;
            stats.confirm_rows += batch.parents.row_count;
            stats.max_confirm_rows = stats.max_confirm_rows.max(batch.parents.row_count);
        }
    }
    finish_formula_action_result(
        plan, desc.bound, counter, batch, result, worklist, interner, stats,
    )
}

/// Semantic result of executing one selected residual-state chunk.
#[derive(Debug)]
enum StepOutcome {
    /// At least one nonempty successor was filed, including a merge into an
    /// already-live canonical bucket.
    Advanced(ContinuationToken),
    /// An action compacted to no successor rows.
    Dead,
    /// Full-bound rows are ready for projection.
    Emit(RowBatch),
}

/// One pull of the mixed stable/delta machine. Delta seeding may immediately
/// file an accepting seed effect while retaining its independent cyclic
/// traversal frontier.
#[derive(Debug)]
enum MachineStep {
    Stable(StepOutcome),
    /// Cyclic work was seeded. Parents with an empty root set may already
    /// have resumed their stable formula continuation.
    DeltaSeeded {
        continuation: Option<ContinuationToken>,
        /// Last newly filed live cyclic activation. The outer scheduler may
        /// arm it only when the selected action was already in a scalar
        /// continuation sprint.
        active: Option<ActiveDeltaContinuation>,
        /// Exact affine parents transferred by the selected action. One delta
        /// activation is created per parent; a last-filed token therefore
        /// names the complete new cohort only when this is one.
        seeded_parents: usize,
    },
}

/// Executes one canonical control state after the scheduler has selected its
/// affine payload chunk. The explicit owned task is the common eager/lazy
/// dispatch boundary; its action-only view is where a future executor can
/// attach a local cost quote without widening [`Constraint`]. The outcome lets
/// callers distinguish semantic progress, branch death, and terminal
/// projection without inferring any of them from worklist size.
fn execute_task<'a>(
    root: &dyn Constraint<'a>,
    plan: &ResidualPlan,
    task: SelectedResidualTask,
    full: VariableSet,
    influences: &[VariableSet; 128],
    base_estimates: &[usize; 128],
    worklist: &mut Worklist,
    interner: &mut StateInterner,
    stats: &mut ResidualStateStats,
    next_activation: &mut u64,
) -> StepOutcome {
    let SelectedResidualTask {
        state: _,
        desc,
        bucket,
    } = task;
    match (&desc.phase, bucket) {
        (ResidualPhase::Ready, StateBucket::Rows(rows)) if desc.bound == full => {
            stats.emit_pops += 1;
            StepOutcome::Emit(rows)
        }
        (ResidualPhase::Ready, StateBucket::Rows(rows)) => {
            stats.ready_plan_pops += 1;
            let continuation = ready_plan_transition(
                root,
                plan,
                &desc,
                rows,
                full,
                influences,
                base_estimates,
                worklist,
                interner,
                stats,
            );
            StepOutcome::Advanced(continuation)
        }
        (
            ResidualPhase::Propose {
                variable,
                relevant,
                proposer,
            },
            StateBucket::Rows(rows),
        ) => {
            if !plan.has_finite_formula(*proposer) {
                stats.propose_action_pops += 1;
            }
            let continuation = propose_action_transition(
                root,
                plan,
                &desc,
                *variable,
                relevant,
                *proposer,
                rows,
                next_activation,
                worklist,
                interner,
                stats,
            );
            if let Some(continuation) = continuation {
                StepOutcome::Advanced(continuation)
            } else {
                stats.dead_action_pops += 1;
                StepOutcome::Dead
            }
        }
        (
            ResidualPhase::Candidate {
                variable,
                relevant,
                checked,
            },
            StateBucket::Candidates(batch),
        ) => {
            stats.candidate_plan_pops += 1;
            let continuation = candidate_plan_transition(
                root, plan, &desc, *variable, relevant, checked, batch, worklist, interner, stats,
            );
            StepOutcome::Advanced(continuation)
        }
        (
            ResidualPhase::Confirm {
                variable,
                relevant,
                checked,
                confirmer,
            },
            StateBucket::Candidates(batch),
        ) => {
            if !plan.has_finite_formula(*confirmer) {
                stats.confirm_action_pops += 1;
            }
            let continuation = confirm_action_transition(
                root,
                plan,
                &desc,
                *variable,
                relevant,
                checked,
                *confirmer,
                batch,
                next_activation,
                worklist,
                interner,
                stats,
            );
            if let Some(continuation) = continuation {
                StepOutcome::Advanced(continuation)
            } else {
                stats.dead_action_pops += 1;
                StepOutcome::Dead
            }
        }
        (ResidualPhase::Formula { counter }, StateBucket::Formula(batch)) => {
            let counter = *counter;
            let action_stage = match &interner.formula(counter).focus {
                FormulaFocus::Plan { .. } => None,
                FormulaFocus::Action { stage, .. } => Some(*stage),
                FormulaFocus::Complete { .. } => {
                    panic!("a completed formula reached executor dispatch")
                }
            };
            let is_action = action_stage.is_some();
            let continuation = match action_stage {
                None => formula_plan_transition(
                    root, plan, &desc, counter, batch, worklist, interner, stats,
                ),
                Some(stage) => {
                    match stage {
                        FormulaStage::Support => stats.support_action_pops += 1,
                        FormulaStage::Propose => stats.propose_action_pops += 1,
                        FormulaStage::Confirm => stats.confirm_action_pops += 1,
                    }
                    formula_action_transition(
                        root, plan, &desc, counter, batch, worklist, interner, stats,
                    )
                }
            };
            if let Some(continuation) = continuation {
                StepOutcome::Advanced(continuation)
            } else {
                if is_action {
                    stats.dead_action_pops += 1;
                }
                StepOutcome::Dead
            }
        }
        _ => panic!("canonical residual state received the wrong payload shape"),
    }
}

/// Compile-time action-dispatch seam shared by the ordinary and shadowed
/// mixed stable/delta control loop. The direct implementation is a zero-sized
/// passthrough and never materializes action geometry. Only the shadow
/// implementation reads clocks, touches TLS, or allocates observer records.
trait ResidualActionDispatch {
    fn observes_actions(&self) -> bool;

    fn run(
        &self,
        task: SelectedResidualTask,
        action: Option<ResidualActionTask>,
        execute: impl FnOnce(SelectedResidualTask) -> MachineStep,
    ) -> MachineStep;
}

#[derive(Clone, Copy)]
struct DirectActionDispatch;

impl ResidualActionDispatch for DirectActionDispatch {
    #[inline]
    fn observes_actions(&self) -> bool {
        false
    }

    #[inline]
    fn run(
        &self,
        task: SelectedResidualTask,
        _action: Option<ResidualActionTask>,
        execute: impl FnOnce(SelectedResidualTask) -> MachineStep,
    ) -> MachineStep {
        execute(task)
    }
}

struct ShadowActionDispatch<'e> {
    epoch: &'e ResidualShadowEpoch,
}

impl ResidualActionDispatch for ShadowActionDispatch<'_> {
    fn observes_actions(&self) -> bool {
        true
    }

    fn run(
        &self,
        task: SelectedResidualTask,
        action: Option<ResidualActionTask>,
        execute: impl FnOnce(SelectedResidualTask) -> MachineStep,
    ) -> MachineStep {
        let Some(action) = action else {
            return execute(task);
        };
        let (site, geometry) = action.observation();
        let pending = self.epoch.begin(site, geometry);
        let scope = ShadowActionScope::enter(pending.correlation());
        // The timed span is deliberately bound after the TLS scope. Reverse
        // drop order captures aborted wall time before scope teardown.
        let mut span = pending;
        span.start();
        let outcome = execute(task);
        let wall = span.elapsed();
        let observed_outcome = match &outcome {
            MachineStep::Stable(StepOutcome::Advanced(continuation))
            | MachineStep::DeltaSeeded {
                continuation: Some(continuation),
                active: None,
                ..
            } => ActionOutcome::Advanced(ActionSurvival {
                parent_rows: continuation.rows,
                candidate_occurrences: continuation.candidates,
            }),
            MachineStep::DeltaSeeded {
                active: Some(_), ..
            } => {
                // Native cyclic lowering transfers the selected affine input
                // into a reopenable delta frontier. Expansion is deliberately
                // not attributed to one event: one canonical expansion cohort
                // may batch activations from several action sites.
                ActionOutcome::Advanced(ActionSurvival {
                    parent_rows: action.parent_rows,
                    candidate_occurrences: action.candidate_occurrences,
                })
            }
            MachineStep::Stable(StepOutcome::Dead)
            | MachineStep::DeltaSeeded {
                continuation: None,
                active: None,
                ..
            } => ActionOutcome::Dead,
            MachineStep::Stable(StepOutcome::Emit(_)) => {
                unreachable!("only Propose and Confirm tasks enter a residual shadow action")
            }
        };
        span.finish(wall, observed_outcome);
        drop(scope);
        outcome
    }
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
fn execute_state<'a>(
    root: &dyn Constraint<'a>,
    plan: &ResidualPlan,
    desc: &StateDesc,
    bucket: StateBucket,
    full: VariableSet,
    influences: &[VariableSet; 128],
    base_estimates: &[usize; 128],
    worklist: &mut Worklist,
    interner: &mut StateInterner,
    stats: &mut ResidualStateStats,
) -> StepOutcome {
    let mut next_activation = 0;
    execute_task(
        root,
        plan,
        SelectedResidualTask {
            // Direct transition tests do not select through the interner. The
            // executor does not consult this synthetic identity; production
            // eager and lazy paths always carry the exact selected StateId.
            state: StateId(u32::MAX),
            desc: desc.clone(),
            bucket,
        },
        full,
        influences,
        base_estimates,
        worklist,
        interner,
        stats,
        &mut next_activation,
    )
}

/// Resumable execution state for [`ResidualStateIter`].
///
/// The exact interner deliberately outlives live buckets. Occupancy scheduling
/// may process a full state before all of its lower-rank feeders, after which
/// a later filing simply reopens the same interned descriptor.
#[derive(Clone)]
struct ResidualStateMachine {
    full: VariableSet,
    leaf_count: usize,
    action_span: usize,
    next_activation: u64,
    interner: StateInterner,
    worklist: Worklist,
    /// Reopenable cyclic work. Its canonical keys are structural, while
    /// activation identity and novelty live behind affine payload credits.
    delta: DeltaScheduler,
    stats: ResidualStateStats,
    binding: Binding,
    emit_vars: Vec<VariableId>,
    emit_rows: Vec<RawInline>,
    emit_next: usize,
    emit_count: usize,
    /// Exact physical cohort activated by a partially surviving full action
    /// or a delta-to-stable handoff. Its physical scheduling mode remains
    /// outside canonical state identity.
    continuation: Option<ActiveContinuation>,
    /// Exact cyclic activation created while probing one stable continuation
    /// atom. This is a physical latency preference only; all logical work
    /// remains owned by [`DeltaScheduler`].
    active_delta: Option<ActiveDeltaContinuation>,
    #[cfg(test)]
    continuation_sprint_enabled: bool,
    last_selection: SelectionKind,
    last_was_action: bool,
    width: usize,
    growth: usize,
    cap: usize,
}

/// Borrow-free residual cursor stored by the ordinary [`Query`].
///
/// The query continues to own the root constraint and postprocessor. This
/// box contains only the lowering plan and exact raw scheduler remainder, so
/// cloning it never needs to clone a projected `R` and no field borrows the
/// surrounding `Query`.
#[derive(Clone)]
pub(super) struct ResidualQueryState {
    plan: ResidualPlan,
    machine: ResidualStateMachine,
}

/// One canonical row used to enter a private residual frame with local
/// variables already bound.
///
/// Values are stored in ascending [`VariableId`] order, exactly like every
/// other residual-state row. The frame owns a separate plan and interner, so
/// these local variable numbers never enter the caller's variable namespace.
pub(super) struct FrameSeedRow {
    bound: VariableSet,
    values: Vec<RawInline>,
}

impl FrameSeedRow {
    /// The ordinary empty binding, represented as one virtual zero-width row.
    pub(super) fn empty() -> Self {
        Self {
            bound: VariableSet::new_empty(),
            values: Vec::new(),
        }
    }

    /// A one-column seed for a captured caller value.
    pub(super) fn one(variable: VariableId, value: RawInline) -> Self {
        Self {
            bound: VariableSet::new_singleton(variable),
            values: vec![value],
        }
    }
}

/// Private, synchronously executed residual submachine.
///
/// The owning value is the frame-plan namespace: its local [`StateDesc`] ranks
/// are never compared with an outer query's ranks. This is intentionally an
/// internal bridge for nested constraint helpers, not cross-plan worklist
/// cohosting.
pub(super) struct SeededResidualFrame<C> {
    root: C,
    plan: ResidualPlan,
    machine: ResidualStateMachine,
    influences: [VariableSet; 128],
    base_estimates: [usize; 128],
}

impl<'a, C> SeededResidualFrame<C>
where
    C: Constraint<'a> + 'a,
{
    pub(super) fn new(root: C, seed: FrameSeedRow, lowering: ResidualLowering) -> Self {
        let full = root.variables();
        assert!(
            seed.bound.is_subset_of(&full),
            "residual frame seed binds a variable outside its local plan"
        );
        assert_eq!(
            seed.values.len(),
            seed.bound.count(),
            "residual frame seed storage disagrees with its bound schema"
        );

        let variables: Vec<_> = seed.bound.into_iter().collect();
        let seed_view = RowsView::new_with_row_count(&variables, &seed.values, 1);
        let mode = if root.satisfied(&seed_view) {
            Search::NextVariable
        } else {
            Search::Done
        };

        let influences = std::array::from_fn(|variable| {
            if full.is_set(variable) {
                root.influence(variable)
            } else {
                VariableSet::new_empty()
            }
        });
        let base_estimates = std::array::from_fn(|variable| {
            if !full.is_set(variable) {
                return usize::MAX;
            }
            let mut estimate = 0usize;
            assert!(
                root.estimate(
                    variable,
                    &RowsView::EMPTY,
                    &mut EstimateSink::Scalar(&mut estimate),
                ),
                "unconstrained variable in residual frame"
            );
            estimate
        });

        let plan = ResidualPlan::compile_lowering(&root, lowering);
        let machine = ResidualStateMachine::new_seeded_for_plan(full, &plan, mode, seed);
        Self {
            root,
            plan,
            machine,
            influences,
            base_estimates,
        }
    }

    pub(super) fn next_binding(&mut self) -> Option<Binding> {
        self.machine.pull(
            &self.root,
            &self.plan,
            &|binding| Some(binding.clone()),
            &self.influences,
            &self.base_estimates,
        )
    }
}

impl ResidualQueryState {
    pub(super) fn new<'a>(
        root: &dyn Constraint<'a>,
        mode: Search,
        lowering: ResidualLowering,
    ) -> Self {
        let plan = ResidualPlan::compile_lowering(root, lowering);
        let machine = ResidualStateMachine::new_for_plan(root.variables(), &plan, mode);
        Self { plan, machine }
    }

    pub(super) fn pull<'a, P, R>(
        &mut self,
        root: &dyn Constraint<'a>,
        postprocessing: &P,
        influences: &[VariableSet; 128],
        base_estimates: &[usize; 128],
    ) -> Option<R>
    where
        P: Fn(&Binding) -> Option<R>,
    {
        self.machine
            .pull(root, &self.plan, postprocessing, influences, base_estimates)
    }
}

impl ResidualStateMachine {
    #[cfg(test)]
    fn new(full: VariableSet, leaf_count: usize, mode: Search) -> Self {
        Self::new_with_span(full, leaf_count, 2, mode)
    }

    fn new_for_plan(full: VariableSet, plan: &ResidualPlan, mode: Search) -> Self {
        Self::new_seeded_for_plan(full, plan, mode, FrameSeedRow::empty())
    }

    fn new_seeded_for_plan(
        full: VariableSet,
        plan: &ResidualPlan,
        mode: Search,
        seed: FrameSeedRow,
    ) -> Self {
        Self::new_with_span_and_seed(full, plan.len(), plan.action_span(), mode, seed)
    }

    #[cfg(test)]
    fn new_with_span(
        full: VariableSet,
        leaf_count: usize,
        action_span: usize,
        mode: Search,
    ) -> Self {
        Self::new_with_span_and_seed(full, leaf_count, action_span, mode, FrameSeedRow::empty())
    }

    fn new_with_span_and_seed(
        full: VariableSet,
        leaf_count: usize,
        action_span: usize,
        mode: Search,
        seed: FrameSeedRow,
    ) -> Self {
        let cap = block_row_cap();
        let mut state = Self {
            full,
            leaf_count,
            action_span,
            next_activation: 0,
            interner: StateInterner::default(),
            worklist: Worklist::new(),
            delta: DeltaScheduler::new(),
            stats: ResidualStateStats::default(),
            binding: Binding::default(),
            emit_vars: Vec::new(),
            emit_rows: Vec::new(),
            emit_next: 0,
            emit_count: 0,
            continuation: None,
            active_delta: None,
            #[cfg(test)]
            continuation_sprint_enabled: true,
            last_selection: SelectionKind::Readiness,
            last_was_action: false,
            width: lazy_start_width().clamp(1, cap),
            growth: lazy_growth(),
            cap,
        };
        if matches!(mode, Search::NextVariable) {
            file_with_span(
                &mut state.worklist,
                &mut state.interner,
                leaf_count,
                action_span,
                None,
                StateDesc {
                    bound: seed.bound,
                    phase: ResidualPhase::Ready,
                },
                StateBucket::Rows(RowBatch {
                    rows: seed.values,
                    row_count: 1,
                }),
                &mut state.stats,
            );
        }
        state
    }

    /// Removes one batch-filling chunk from the next state.
    ///
    /// The deepest bucket that can supply the complete desired actionable
    /// width wins. Rows are the unit until a candidate continuation contains
    /// only page-local confirms, at which point candidate occurrences are the
    /// unit. If no bucket is large enough, the minimum-rank bucket is drained;
    /// strict rank growth makes that underfilled pop readiness-safe. Thus
    /// width one preserves maximum-rank, highest-ID traversal, while a width
    /// above every live bucket is exact minimum-rank scheduling. Partial
    /// remainders are reinserted directly and are not counted as canonical
    /// merges or reentries.
    #[cfg(test)]
    fn take_next(&mut self, width: usize) -> Option<(StateDesc, StateBucket)> {
        self.take_next_inner(None, width)
            .map(|task| (task.desc, task.bucket))
    }

    fn take_next_with_plan(
        &mut self,
        plan: &ResidualPlan,
        width: usize,
    ) -> Option<SelectedResidualTask> {
        self.take_next_inner(Some(plan), width)
    }

    fn take_next_inner(
        &mut self,
        plan: Option<&ResidualPlan>,
        width: usize,
    ) -> Option<SelectedResidualTask> {
        let width = width.max(1);
        let full_state = self.worklist.iter().rev().find_map(|(&rank, level)| {
            level.iter().rev().find_map(|(&id, bucket)| {
                let desc = self.interner.get(id);
                let candidate_pages = plan.is_some_and(|plan| {
                    desc.uses_candidate_pages(plan, &self.interner.formula_pcs)
                });
                (bucket.occupancy(candidate_pages) >= width).then_some((rank, id))
            })
        });
        let (rank, id, is_full) = if let Some((rank, id)) = full_state {
            (rank, id, true)
        } else {
            let (&rank, level) = self.worklist.first_key_value()?;
            let (&id, bucket) = level
                .last_key_value()
                .expect("residual rank has a live state");
            let desc = self.interner.get(id);
            let candidate_pages = plan
                .is_some_and(|plan| desc.uses_candidate_pages(plan, &self.interner.formula_pcs));
            assert!(
                bucket.occupancy(candidate_pages) < width,
                "readiness selected while a full residual bucket existed"
            );
            (rank, id, false)
        };

        let (mut bucket, remove_level) = {
            let level = self
                .worklist
                .get_mut(&rank)
                .expect("selected residual rank exists");
            let bucket = level.remove(&id).expect("selected residual state exists");
            (bucket, level.is_empty())
        };
        if remove_level {
            self.worklist.remove(&rank);
        }

        let desc = self.interner.get(id).clone();
        debug_assert_eq!(
            desc.rank_with_span(
                self.leaf_count,
                self.action_span,
                plan.map(|plan| &plan.finite_formula),
                &self.interner.formula_pcs,
            ),
            rank
        );
        let candidate_pages =
            plan.is_some_and(|plan| desc.uses_candidate_pages(plan, &self.interner.formula_pcs));
        let before = bucket.occupancy(candidate_pages);
        let chunk = bucket.take_tail(desc.bound.count(), width, candidate_pages);
        let remainder = bucket.occupancy(candidate_pages);
        if remainder != 0 {
            assert!(is_full, "only a full pop may leave a remainder");
            self.stats.partial_pops += 1;
            assert!(
                self.worklist
                    .entry(rank)
                    .or_default()
                    .insert(id, bucket)
                    .is_none(),
                "a residual-state remainder collided with another live bucket"
            );
        }
        debug_assert_eq!(chunk.occupancy(candidate_pages), before.min(width));
        if is_full {
            assert!(before >= width, "full residual pop was underfilled");
        } else {
            assert!(before < width, "readiness residual pop was full");
            assert_eq!(remainder, 0, "a readiness pop must drain its bucket");
        }

        self.stats.state_pops += 1;
        if is_full {
            self.stats.full_pops += 1;
            self.last_selection = SelectionKind::Full;
        } else {
            self.stats.readiness_pops += 1;
            self.last_selection = SelectionKind::Readiness;
        }
        Some(SelectedResidualTask {
            state: id,
            desc,
            bucket: chunk,
        })
    }

    /// Removes one coalesced-receipt chunk from the current canonical tail.
    ///
    /// A global strict-deepest flag is insufficient here: another history may
    /// already occupy a deeper state, and an older cohort may already occupy
    /// this exact state. The token limits the tail cut to all equal-key filings
    /// coalesced by the selected transition, preserving DFS latency without
    /// changing readiness legality or canonical state identity. The cut may
    /// deliberately defer the opportunity to merge with older work.
    fn take_continuation(
        &mut self,
        plan: &ResidualPlan,
        active: ActiveContinuation,
        width: usize,
    ) -> SelectedResidualTask {
        let ActiveContinuation { token, mode } = active;
        let desc = self.interner.get(token.state).clone();
        assert_eq!(
            desc.rank_with_span(
                self.leaf_count,
                self.action_span,
                Some(&plan.finite_formula),
                &self.interner.formula_pcs,
            ),
            token.rank,
            "continuation token disagrees with canonical state rank"
        );
        let candidate_pages = desc.uses_candidate_pages(plan, &self.interner.formula_pcs);
        let cohort_occupancy = token.occupancy(&desc, plan, &self.interner.formula_pcs);
        assert!(cohort_occupancy > 0, "continuation cohort is empty");
        let take = match mode {
            ContinuationMode::Cohort => cohort_occupancy.min(width.max(1)),
            ContinuationMode::ProbeOne => 1,
        };

        let (mut bucket, remove_level) = {
            let level = self
                .worklist
                .get_mut(&token.rank)
                .expect("continuation rank remains live");
            let bucket = level
                .remove(&token.state)
                .expect("continuation state remains live");
            (bucket, level.is_empty())
        };
        if remove_level {
            self.worklist.remove(&token.rank);
        }

        let before = bucket.occupancy(candidate_pages);
        assert!(
            before >= cohort_occupancy,
            "canonical bucket lost part of its newly filed continuation cohort"
        );
        let chunk = bucket.take_tail(desc.bound.count(), take, candidate_pages);
        let remainder = bucket.occupancy(candidate_pages);
        if remainder != 0 {
            self.stats.partial_pops += 1;
            assert!(
                self.worklist
                    .entry(token.rank)
                    .or_default()
                    .insert(token.state, bucket)
                    .is_none(),
                "a continuation remainder collided with another live bucket"
            );
        }
        debug_assert_eq!(chunk.occupancy(candidate_pages), take);

        self.stats.state_pops += 1;
        self.stats.continuation_pops += 1;
        self.last_selection = SelectionKind::Continuation(mode);
        if mode == ContinuationMode::ProbeOne {
            self.stats.delta_handoff_probe_pops += 1;
        }
        if cohort_occupancy < width.max(1) {
            self.stats.underfilled_continuation_pops += 1;
        }
        SelectedResidualTask {
            state: token.state,
            desc,
            bucket: chunk,
        }
    }

    /// Whether ordinary acyclic work can fill the current demand width
    /// without invoking the minimum-rank readiness lemma.
    fn has_full_stable(&self, plan: &ResidualPlan, width: usize) -> bool {
        let width = width.max(1);
        self.worklist.values().any(|level| {
            level.iter().any(|(&id, bucket)| {
                let desc = self.interner.get(id);
                bucket.occupancy(desc.uses_candidate_pages(plan, &self.interner.formula_pcs))
                    >= width
            })
        })
    }

    /// Converts one eligible proposer action into activation-owned cyclic
    /// work.
    fn seed_delta_proposal<'a>(
        &mut self,
        root: &dyn Constraint<'a>,
        plan: &ResidualPlan,
        task: SelectedResidualTask,
    ) -> Result<DeltaSeedOutcome, SelectedResidualTask> {
        if !plan.transition_programs {
            return Err(task);
        }
        let (
            ResidualPhase::Propose {
                variable,
                relevant,
                proposer,
            },
            StateBucket::Rows(rows),
        ) = (&task.desc.phase, &task.bucket)
        else {
            return Err(task);
        };

        // Formula actions enter the same cyclic stratum through their own
        // suspension path below; this hook owns ordinary opaque leaf actions.
        if plan.has_finite_formula(*proposer) {
            return Err(task);
        }

        let mut checked = ChildSet::empty(plan.len());
        checked.insert(*proposer);
        if !plan.remaining_confirms_accept_pages(relevant, &checked, *variable, task.desc.bound) {
            return Err(task);
        }
        let variable = *variable;
        let proposer = *proposer;
        let relevant = relevant.clone();

        let vars: Vec<VariableId> = task.desc.bound.into_iter().collect();
        let view = rows_view(&vars, &rows.rows, rows.row_count);
        let constraint = plan.resolve(root, proposer);
        if constraint.residual_delta_source_is_paged(variable, &view)
            || constraint.residual_proposal_source_is_paged(variable, &view)
        {
            let SelectedResidualTask {
                state: _,
                desc,
                bucket,
            } = task;
            let StateBucket::Rows(rows) = bucket else {
                unreachable!("delta proposer was checked above")
            };
            self.stats.propose_action_pops += 1;
            self.stats.propose_calls += 1;
            self.stats.propose_rows += rows.row_count;
            self.stats.max_propose_rows = self.stats.max_propose_rows.max(rows.row_count);
            let successor = StateDesc {
                bound: desc.bound,
                phase: ResidualPhase::Candidate {
                    variable,
                    relevant,
                    checked,
                },
            };
            let active = self.delta.seed_source_proposals(
                DeltaDesc::leaf(variable, proposer),
                successor,
                rows,
            );
            return Ok(DeltaSeedOutcome {
                continuation: None,
                active,
            });
        }
        let mut seeds = Vec::new();
        let supported = constraint.residual_delta_seeds(variable, &view, &mut seeds);
        if !supported {
            assert!(
                seeds.is_empty(),
                "unsupported delta seed hook mutated its output"
            );
            return Err(task);
        }
        let SelectedResidualTask {
            state: _,
            desc,
            bucket,
        } = task;
        let StateBucket::Rows(rows) = bucket else {
            unreachable!("delta proposer was checked above")
        };
        self.stats.propose_action_pops += 1;
        self.stats.propose_calls += 1;
        self.stats.propose_rows += rows.row_count;
        self.stats.max_propose_rows = self.stats.max_propose_rows.max(rows.row_count);
        let successor = StateDesc {
            bound: desc.bound,
            phase: ResidualPhase::Candidate {
                variable,
                relevant,
                checked,
            },
        };
        let outcome = self.delta.seed_proposals(
            DeltaDesc::leaf(variable, proposer),
            successor,
            rows,
            seeds,
            plan,
            &mut self.worklist,
            &mut self.interner,
            &mut self.stats,
        );
        Ok(outcome)
    }

    /// Converts one eligible confirmer into one transition activation per
    /// parent candidate batch. The reducer retains the immutable original
    /// candidate sequence and filters it only after traversal quiesces. Finite
    /// page-local confirmations may receive one disjoint page; repeated paths
    /// remain parent-grouped by their plan capability.
    fn seed_delta_confirm<'a>(
        &mut self,
        root: &dyn Constraint<'a>,
        plan: &ResidualPlan,
        task: SelectedResidualTask,
    ) -> Result<DeltaSeedOutcome, SelectedResidualTask> {
        if !plan.transition_programs {
            return Err(task);
        }
        let (
            ResidualPhase::Confirm {
                variable,
                relevant,
                checked,
                confirmer,
            },
            StateBucket::Candidates(batch),
        ) = (&task.desc.phase, &task.bucket)
        else {
            return Err(task);
        };
        // Lowered finite formulas own their own action reducer. Any ordinary
        // opaque confirmer may offer a transition program; unsupported hooks
        // fall back to the ordinary protocol below.
        if plan.has_finite_formula(*confirmer) {
            return Err(task);
        }
        if grouped_delta_confirm_is_active(
            &plan.grouped_delta_confirm_requirements[*confirmer],
            *variable,
            task.desc.bound,
        ) {
            assert!(
                !task
                    .desc
                    .uses_candidate_pages(plan, &self.interner.formula_pcs),
                "grouped delta confirmation was split into candidate pages"
            );
        }

        let variable = *variable;
        let confirmer = *confirmer;
        let relevant = relevant.clone();
        let checked = checked.clone();
        let vars: Vec<VariableId> = task.desc.bound.into_iter().collect();
        let view = rows_view(&vars, &batch.parents.rows, batch.parents.row_count);
        let constraint = plan.resolve(root, confirmer);
        if constraint.residual_delta_source_is_paged(variable, &view) {
            let SelectedResidualTask {
                state: _,
                desc,
                bucket,
            } = task;
            let StateBucket::Candidates(batch) = bucket else {
                unreachable!("delta confirmer was checked above")
            };
            let candidates_before = batch.candidate_count();
            self.stats.confirm_action_pops += 1;
            self.stats.confirm_calls += 1;
            self.stats.confirm_rows += batch.parents.row_count;
            self.stats.max_confirm_rows = self.stats.max_confirm_rows.max(batch.parents.row_count);
            self.stats.candidates_confirmed += candidates_before;
            self.stats.max_confirm_candidates =
                self.stats.max_confirm_candidates.max(candidates_before);
            let successor = StateDesc {
                bound: desc.bound,
                phase: ResidualPhase::Candidate {
                    variable,
                    relevant,
                    checked: checked.with_inserted(confirmer),
                },
            };
            let active = self.delta.seed_source_confirms(
                DeltaDesc::leaf(variable, confirmer),
                successor,
                batch,
            );
            return Ok(DeltaSeedOutcome {
                continuation: None,
                active,
            });
        }
        let mut seeds = Vec::new();
        let supported = constraint.residual_delta_seeds(variable, &view, &mut seeds);
        if !supported {
            assert!(
                seeds.is_empty(),
                "unsupported delta seed hook mutated its output"
            );
            return Err(task);
        }
        let SelectedResidualTask {
            state: _,
            desc,
            bucket,
        } = task;
        let StateBucket::Candidates(batch) = bucket else {
            unreachable!("delta confirmer was checked above")
        };
        let candidates_before = batch.candidate_count();
        self.stats.confirm_action_pops += 1;
        self.stats.confirm_calls += 1;
        self.stats.confirm_rows += batch.parents.row_count;
        self.stats.max_confirm_rows = self.stats.max_confirm_rows.max(batch.parents.row_count);
        self.stats.candidates_confirmed += candidates_before;
        self.stats.max_confirm_candidates =
            self.stats.max_confirm_candidates.max(candidates_before);
        let successor = StateDesc {
            bound: desc.bound,
            phase: ResidualPhase::Candidate {
                variable,
                relevant,
                checked: checked.with_inserted(confirmer),
            },
        };
        let active = self.delta.seed_confirms(
            DeltaDesc::leaf(variable, confirmer),
            successor,
            batch,
            seeds,
        );
        Ok(DeltaSeedOutcome {
            continuation: None,
            active,
        })
    }

    /// Suspends a currently focused formula Atom behind one transition reducer
    /// activation per affine parent. The exact Action PC ID and every payload
    /// frame remain activation data; [`DeltaDesc`] names only the common
    /// structural expansion kernel. Page-local finite confirmations retain the
    /// formula's geometric candidate split; grouped repeated confirmations keep
    /// their complete parent candidate sequence.
    fn seed_delta_formula<'a>(
        &mut self,
        root: &dyn Constraint<'a>,
        plan: &ResidualPlan,
        task: SelectedResidualTask,
    ) -> Result<DeltaSeedOutcome, SelectedResidualTask> {
        if !plan.transition_programs {
            return Err(task);
        }
        let (ResidualPhase::Formula { counter }, StateBucket::Formula(batch)) =
            (&task.desc.phase, &task.bucket)
        else {
            return Err(task);
        };
        let counter = *counter;
        let resume = self.interner.formula_resume(counter);
        let (node, stage, occurrence, outer_variable) = {
            let counter = self.interner.formula(counter);
            match &counter.focus {
                FormulaFocus::Action { node, stage } => {
                    (*node, *stage, resume.occurrence, resume.variable)
                }
                _ => return Err(task),
            }
        };
        let formula_node = plan.finite_formula.node(node);
        if !matches!(formula_node.kind, FiniteFormulaNodeKind::Atom) {
            return Err(task);
        }
        let stream_proposal = stage == FormulaStage::Propose
            && plan.interned_formula_proposal_streamability(
                &self.interner.formula_pcs,
                counter,
                task.desc.bound,
            ) == FormulaProposalStreamability::Linear;
        if stream_proposal {
            assert!(
                batch
                    .frames
                    .iter()
                    .all(|frame| matches!(frame, FormulaPayloadFrame::And { .. })),
                "a certified linear formula proposal carried a non-AND payload frame"
            );
        }
        let vars: Vec<VariableId> = task.desc.bound.into_iter().collect();
        let view = rows_view(&vars, &batch.parents.rows, batch.parents.row_count);
        let constraint = plan.resolve_formula_node(root, occurrence, node);
        let mut seeds = Vec::new();
        let (variable, paged) = if stage == FormulaStage::Support {
            let Some(route) = constraint.residual_delta_support_seeds(&view, &mut seeds) else {
                assert!(
                    seeds.is_empty(),
                    "unsupported formula support seed hook mutated its output"
                );
                return Err(task);
            };
            (route, false)
        } else {
            let variable = outer_variable;
            let transition_paged = constraint.residual_delta_source_is_paged(variable, &view);
            let proposal_paged = stage == FormulaStage::Propose
                && constraint.residual_proposal_source_is_paged(variable, &view);
            let proposal_has_transition_roots = proposal_paged
                && constraint.residual_proposal_source_has_transition_roots(variable, &view);
            if proposal_paged
                && !transition_paged
                && !proposal_has_transition_roots
                && !stream_proposal
                && !plan.has_paged_transition_source(root, variable, &view)
            {
                // A quiescent formula reducer cannot publish direct proposal
                // pages before the finite frontier settles. Eager proposal
                // materializes the same row-local bag without affine source
                // machinery. When any sibling owns a true transition source,
                // keep the heterogeneous frontier uniformly pageable so its
                // work can still be interleaved.
                return Err(task);
            }
            let paged = transition_paged || proposal_paged;
            if !paged {
                let supported = constraint.residual_delta_seeds(variable, &view, &mut seeds);
                if !supported {
                    assert!(
                        seeds.is_empty(),
                        "unsupported formula delta seed hook mutated its output"
                    );
                    return Err(task);
                }
            }
            (variable, paged)
        };

        let SelectedResidualTask {
            state: _,
            desc,
            bucket,
        } = task;
        let StateBucket::Formula(batch) = bucket else {
            unreachable!("formula delta action was checked above")
        };
        match stage {
            FormulaStage::Support => {
                self.stats.support_action_pops += 1;
                self.stats.support_calls += 1;
                self.stats.support_rows += batch.parents.row_count;
                self.stats.max_support_rows =
                    self.stats.max_support_rows.max(batch.parents.row_count);
            }
            FormulaStage::Propose => {
                self.stats.propose_action_pops += 1;
                self.stats.propose_calls += 1;
                self.stats.propose_rows += batch.parents.row_count;
                self.stats.max_propose_rows =
                    self.stats.max_propose_rows.max(batch.parents.row_count);
            }
            FormulaStage::Confirm => {
                let candidates_before = batch.action_candidate_count(stage);
                self.stats.confirm_action_pops += 1;
                self.stats.confirm_calls += 1;
                self.stats.confirm_rows += batch.parents.row_count;
                self.stats.max_confirm_rows =
                    self.stats.max_confirm_rows.max(batch.parents.row_count);
                self.stats.candidates_confirmed += candidates_before;
                self.stats.max_confirm_candidates =
                    self.stats.max_confirm_candidates.max(candidates_before);
            }
        }
        if paged {
            let active = self.delta.seed_source_formula(
                DeltaDesc::formula(variable, occurrence, node),
                desc.bound,
                counter,
                stage,
                batch,
                stream_proposal,
            );
            return Ok(DeltaSeedOutcome {
                continuation: None,
                active,
            });
        }
        Ok(self.delta.seed_formula(
            DeltaDesc::formula(variable, occurrence, node),
            desc.bound,
            counter,
            stage,
            batch,
            seeds,
            stream_proposal,
            plan,
            &mut self.worklist,
            &mut self.interner,
            &mut self.stats,
        ))
    }

    fn execute_selected_task<'a>(
        &mut self,
        root: &dyn Constraint<'a>,
        plan: &ResidualPlan,
        task: SelectedResidualTask,
        influences: &[VariableSet; 128],
        base_estimates: &[usize; 128],
    ) -> MachineStep {
        let seeded_parents = task.bucket.row_count();
        let task = match self.seed_delta_proposal(root, plan, task) {
            Ok(DeltaSeedOutcome {
                continuation,
                active,
            }) => {
                return MachineStep::DeltaSeeded {
                    continuation,
                    active,
                    seeded_parents,
                };
            }
            Err(task) => task,
        };
        let task = match self.seed_delta_confirm(root, plan, task) {
            Ok(DeltaSeedOutcome {
                continuation,
                active,
            }) => {
                return MachineStep::DeltaSeeded {
                    continuation,
                    active,
                    seeded_parents,
                };
            }
            Err(task) => task,
        };
        let task = match self.seed_delta_formula(root, plan, task) {
            Ok(DeltaSeedOutcome {
                continuation,
                active,
            }) => {
                return MachineStep::DeltaSeeded {
                    continuation,
                    active,
                    seeded_parents,
                };
            }
            Err(task) => task,
        };
        let emit_bound = task.desc.bound;
        let outcome = execute_task(
            root,
            plan,
            task,
            self.full,
            influences,
            base_estimates,
            &mut self.worklist,
            &mut self.interner,
            &mut self.stats,
            &mut self.next_activation,
        );
        if matches!(&outcome, StepOutcome::Emit(_)) {
            self.emit_vars.clear();
            self.emit_vars.extend(emit_bound);
        }
        MachineStep::Stable(outcome)
    }

    fn pop_once_with_dispatch<'a>(
        &mut self,
        dispatch: &impl ResidualActionDispatch,
        root: &dyn Constraint<'a>,
        plan: &ResidualPlan,
        influences: &[VariableSet; 128],
        base_estimates: &[usize; 128],
        width: usize,
    ) -> MachineStep {
        let task = if let Some(token) = self.continuation.take() {
            self.take_continuation(plan, token, width)
        } else {
            self.take_next_with_plan(plan, width)
                .expect("pop_once requires a non-empty residual worklist")
        };
        self.last_was_action = task.is_action_for_plan(plan, &self.interner);
        let action = dispatch
            .observes_actions()
            .then(|| task.action_task(plan, &self.interner))
            .flatten();
        dispatch.run(task, action, |task| {
            self.execute_selected_task(root, plan, task, influences, base_estimates)
        })
    }

    #[cfg(test)]
    fn pop_once<'a>(
        &mut self,
        root: &dyn Constraint<'a>,
        plan: &ResidualPlan,
        influences: &[VariableSet; 128],
        base_estimates: &[usize; 128],
        width: usize,
    ) -> MachineStep {
        self.pop_once_with_dispatch(
            &DirectActionDispatch,
            root,
            plan,
            influences,
            base_estimates,
            width,
        )
    }

    fn increase_width(&mut self) {
        let next = self.width.saturating_mul(self.growth).clamp(1, self.cap);
        if next > self.width {
            self.stats.width_increases += 1;
        }
        self.width = next;
    }

    fn increase_delta_activation_width(&mut self) {
        if self.delta.grow_activation_width(self.growth, self.cap) {
            self.stats.delta_activation_width_increases += 1;
        }
    }

    /// Applies geometric feedback from one delta scheduler step without
    /// confusing exact dead-page telemetry with a globally negative step.
    fn account_delta_feedback(&mut self, outcome: &DeltaStepOutcome) {
        self.stats.delta_activations_completed += outcome.completed_activations;
        if outcome.completed_activations > 0 && outcome.continuation.is_none() {
            self.increase_delta_activation_width();
        }
        if outcome.continuation.is_none() {
            self.stats.delta_source_negative_steps += usize::from(outcome.source_dead_pages > 0);
            self.stats.delta_transition_negative_steps +=
                usize::from(outcome.transition_dead_pages > 0);
        }
        if outcome.dead_pages > 0 && outcome.continuation.is_none() {
            self.increase_width();
        }
    }

    /// Accepts a delta-to-stable handoff into its geometric continuation mode.
    fn accept_delta_step(&mut self, outcome: DeltaStepOutcome) {
        self.account_delta_feedback(&outcome);
        self.active_delta = None;
        self.continuation = outcome.continuation.map(|token| {
            let desc = self.interner.get(token.state);
            let commits_terminal_candidates = match &desc.phase {
                ResidualPhase::Candidate {
                    variable,
                    relevant,
                    checked,
                } if relevant == checked => {
                    let mut committed = desc.bound;
                    committed.set(*variable);
                    committed == self.full
                }
                _ => false,
            };
            if outcome.completed_transition_cohort || commits_terminal_candidates {
                // A geometrically selected activation cohort is already the
                // engine's chosen throughput unit. Keep its exact appended
                // tail hot. The same is safe for fully checked candidates
                // whose commit binds the final variable: their next step can
                // only emit, so probing one cannot reveal downstream
                // selectivity and would strand exact results behind live
                // cyclic work.
                ActiveContinuation::cohort(token)
            } else {
                ActiveContinuation::probe_one(token)
            }
        });
    }

    fn continuation_after_advanced(
        &self,
        plan: &ResidualPlan,
        width: usize,
        continuation: ContinuationToken,
    ) -> Option<ActiveContinuation> {
        #[cfg(test)]
        if !self.continuation_sprint_enabled {
            return None;
        }
        let desc = self.interner.get(continuation.state);
        let successor_is_underfilled =
            continuation.occupancy(desc, plan, &self.interner.formula_pcs) < width.max(1);
        match self.last_selection {
            // ProbeOne governs exactly the delta-filed handoff atom. Once it
            // advances, its ordered fanout is an ordinary semantic cohort;
            // probing that tail again could reverse observable result order.
            SelectionKind::Continuation(ContinuationMode::ProbeOne) => {
                Some(ActiveContinuation::cohort(continuation))
            }
            SelectionKind::Continuation(ContinuationMode::Cohort) => {
                Some(ActiveContinuation::cohort(continuation))
            }
            SelectionKind::Full if self.last_was_action && successor_is_underfilled => {
                Some(ActiveContinuation::cohort(continuation))
            }
            SelectionKind::Full | SelectionKind::Readiness => None,
        }
    }

    /// A cyclic seed inherits depth-first preference only from an existing
    /// continuation lineage when the selected action transferred exactly one
    /// affine parent. Full selections and multi-parent cohorts retain their
    /// deliberate batching unit instead of choosing one arbitrary activation.
    fn active_delta_after_seed(
        &self,
        active: Option<ActiveDeltaContinuation>,
        seeded_parents: usize,
    ) -> Option<ActiveDeltaContinuation> {
        #[cfg(test)]
        if !self.continuation_sprint_enabled {
            return None;
        }
        (seeded_parents == 1
            && matches!(
                self.last_selection,
                SelectionKind::Continuation(ContinuationMode::ProbeOne)
                    | SelectionKind::Continuation(ContinuationMode::Cohort)
            ))
        .then_some(active)
        .flatten()
    }

    fn accept_delta_seed(
        &mut self,
        continuation: Option<ContinuationToken>,
        active: Option<ActiveDeltaContinuation>,
        seeded_parents: usize,
    ) {
        // An immediate stable seed effect is already the earliest legal
        // continuation. It is the yield boundary for this cyclic activation,
        // so no old or new delta focus survives beside it.
        self.active_delta = continuation
            .is_none()
            .then(|| self.active_delta_after_seed(active, seeded_parents))
            .flatten();
        self.continuation = continuation.map(ActiveContinuation::probe_one);
    }

    fn stage_emit(&mut self, rows: RowBatch) {
        debug_assert!(self.emit_next >= self.emit_count);
        self.emit_rows = rows.rows;
        self.emit_next = 0;
        self.emit_count = rows.row_count;
    }

    fn pull_with_dispatch<'a, P, R>(
        &mut self,
        dispatch: &impl ResidualActionDispatch,
        root: &dyn Constraint<'a>,
        plan: &ResidualPlan,
        postprocessing: &P,
        influences: &[VariableSet; 128],
        base_estimates: &[usize; 128],
    ) -> Option<R>
    where
        P: Fn(&Binding) -> Option<R>,
    {
        loop {
            while self.emit_next < self.emit_count {
                let row = self.emit_next;
                // Consume before invoking user code. If it panics and the
                // unwind is caught, a later pull must not repeat its effects.
                self.emit_next += 1;
                let stride = self.emit_vars.len();
                let start = row * stride;
                for (column, &variable) in self.emit_vars.iter().enumerate() {
                    self.binding.set(variable, &self.emit_rows[start + column]);
                }
                if let Some(result) = postprocessing(&self.binding) {
                    return Some(result);
                }
            }
            if self.worklist.is_empty() && self.delta.is_empty() {
                return None;
            }

            let width = self.width;
            // A newly seeded activation on the scalar continuation path is
            // the cyclic analogue of `ActiveContinuation`: follow that exact
            // affine lineage before any cold stable cohort. It owns no work;
            // dropping the token merely returns scheduling to the global
            // source/transition worklists.
            if self.continuation.is_none() {
                if let Some(active) = self.active_delta.take() {
                    let focused = self.delta.step_active(
                        root,
                        plan,
                        active,
                        ACTIVE_DELTA_STEP_WIDTH,
                        &mut self.worklist,
                        &mut self.interner,
                        &mut self.stats,
                    );
                    match focused.status {
                        ActiveDeltaStatus::Yielded => self.accept_delta_step(focused.outcome),
                        ActiveDeltaStatus::Pending => {
                            self.account_delta_feedback(&focused.outcome);
                            self.active_delta = Some(active);
                        }
                        ActiveDeltaStatus::Quiescent => {
                            self.account_delta_feedback(&focused.outcome);
                        }
                    }
                    continue;
                }
            }
            // An underfilled stable bucket is readiness-safe only after every
            // cyclic feeder is quiescent. Full stable work and explicit
            // latency continuations need no harvest lemma and may run first.
            if self.continuation.is_none()
                && !self.delta.is_empty()
                && !self.has_full_stable(plan, width)
            {
                let outcome = self.delta.step(
                    root,
                    plan,
                    width,
                    &mut self.worklist,
                    &mut self.interner,
                    &mut self.stats,
                );
                self.accept_delta_step(outcome);
                continue;
            }
            match self.pop_once_with_dispatch(
                dispatch,
                root,
                plan,
                influences,
                base_estimates,
                width,
            ) {
                MachineStep::Stable(StepOutcome::Advanced(continuation)) => {
                    self.continuation = self.continuation_after_advanced(plan, width, continuation);
                }
                MachineStep::Stable(StepOutcome::Dead) => {
                    self.continuation = None;
                    self.increase_width();
                    self.increase_delta_activation_width();
                }
                MachineStep::Stable(StepOutcome::Emit(rows)) => {
                    self.continuation = None;
                    self.stage_emit(rows);
                    self.increase_width();
                    self.increase_delta_activation_width();
                }
                MachineStep::DeltaSeeded {
                    continuation,
                    active,
                    seeded_parents,
                } => {
                    self.accept_delta_seed(continuation, active, seeded_parents);
                }
            }
        }
    }

    fn pull<'a, P, R>(
        &mut self,
        root: &dyn Constraint<'a>,
        plan: &ResidualPlan,
        postprocessing: &P,
        influences: &[VariableSet; 128],
        base_estimates: &[usize; 128],
    ) -> Option<R>
    where
        P: Fn(&Binding) -> Option<R>,
    {
        self.pull_with_dispatch(
            &DirectActionDispatch,
            root,
            plan,
            postprocessing,
            influences,
            base_estimates,
        )
    }

    /// Observed counterpart of [`Self::pull`]. Both wrappers instantiate the
    /// same mixed stable/delta control loop; static dispatch keeps the ordinary
    /// monomorphization free of observer fields, clocks, TLS, and allocation.
    fn pull_shadow<'a, P, R>(
        &mut self,
        epoch: &ResidualShadowEpoch,
        root: &dyn Constraint<'a>,
        plan: &ResidualPlan,
        postprocessing: &P,
        influences: &[VariableSet; 128],
        base_estimates: &[usize; 128],
    ) -> Option<R>
    where
        P: Fn(&Binding) -> Option<R>,
    {
        self.pull_with_dispatch(
            &ShadowActionDispatch { epoch },
            root,
            plan,
            postprocessing,
            influences,
            base_estimates,
        )
    }
}

#[cfg(feature = "parallel")]
impl ResidualStateMachine {
    /// Construct an empty sibling with the same exact-state vocabulary and
    /// scheduler policy. Affine payload is moved into it by
    /// [`split_for_parallel`](Self::split_for_parallel). Sibling arenas may
    /// allocate different records at the same later numeric ID, which is safe
    /// because Rayon folds each machine independently and combines only `R`;
    /// no descriptor or delta return crosses back between sibling worklists.
    fn parallel_sibling(&self) -> Self {
        Self {
            full: self.full,
            leaf_count: self.leaf_count,
            action_span: self.action_span,
            next_activation: self.next_activation,
            interner: self.interner.clone(),
            worklist: Worklist::new(),
            delta: DeltaScheduler::new(),
            stats: ResidualStateStats::default(),
            binding: Binding::default(),
            emit_vars: Vec::new(),
            emit_rows: Vec::new(),
            emit_next: 0,
            emit_count: 0,
            continuation: None,
            active_delta: None,
            #[cfg(test)]
            continuation_sprint_enabled: self.continuation_sprint_enabled,
            last_selection: SelectionKind::Readiness,
            last_was_action: false,
            width: self.width,
            growth: self.growth,
            cap: self.cap,
        }
    }

    /// Partition the current affine remainder into two independent residual
    /// worklists without restarting from the seed.
    ///
    /// A fresh one-row prefix is advanced through the ordinary state-machine
    /// transitions until it branches. Fully-bound staged rows split directly;
    /// worklist rows split on row boundaries; candidate payloads preserve
    /// whole-parent atomicity unless the plan proves the remaining confirmers
    /// page-local. If two unsplittable buckets already exist, one whole bucket
    /// moves to the sibling. Cross-shard reconvergence is deliberately traded
    /// for parallelism, just as in the affine DAG splitter.
    fn split_for_parallel_with_dispatch<'a>(
        &mut self,
        dispatch: &impl ResidualActionDispatch,
        root: &dyn Constraint<'a>,
        plan: &ResidualPlan,
        influences: &[VariableSet; 128],
        base_estimates: &[usize; 128],
    ) -> Option<Self> {
        // A public producer only splits an unpulled iterator, so no latency
        // continuation can be live here. Clear the physical preference
        // defensively: dropping it never drops affine work, while retaining it
        // across a bucket split could leave the receipt naming the wrong tail.
        self.continuation = None;
        self.active_delta = None;
        loop {
            debug_assert_eq!(
                self.emit_next, 0,
                "parallel residual splits before fold consumption"
            );

            if self.emit_count >= 2 {
                let right_count = self.emit_count / 2;
                let left_count = self.emit_count - right_count;
                let stride = self.emit_vars.len();
                debug_assert!(stride > 0, "a zero-variable query has one result");

                let mut right = self.parallel_sibling();
                right.emit_vars = self.emit_vars.clone();
                right.emit_rows = self.emit_rows.split_off(left_count * stride);
                right.emit_count = right_count;
                self.emit_count = left_count;
                return Some(right);
            }

            // A staged singleton is already an exact affine component. Keep
            // it intact while the other shard owns the remaining worklist.
            if self.emit_count == 1 && (!self.worklist.is_empty() || !self.delta.is_empty()) {
                let mut right = self.parallel_sibling();
                right.emit_vars = std::mem::take(&mut self.emit_vars);
                right.emit_rows = std::mem::take(&mut self.emit_rows);
                right.emit_count = 1;
                self.emit_count = 0;
                return Some(right);
            }

            // Prefer splitting inside one exact state so both workers retain
            // similarly shaped block-native continuations.
            let splittable = self.worklist.iter().rev().find_map(|(&rank, level)| {
                level.iter().rev().find_map(|(&id, bucket)| {
                    let desc = self.interner.get(id);
                    let candidate_pages =
                        desc.uses_candidate_pages(plan, &self.interner.formula_pcs);
                    let can_split = match bucket {
                        StateBucket::Rows(batch) => batch.row_count >= 2,
                        StateBucket::Candidates(batch) if candidate_pages => {
                            batch.candidate_count() >= 2
                        }
                        StateBucket::Candidates(batch) => batch.parents.row_count >= 2,
                        StateBucket::Formula(batch) => batch.parents.row_count >= 2,
                    };
                    can_split.then_some((rank, id, candidate_pages))
                })
            });
            if let Some((rank, id, candidate_pages)) = splittable {
                let desc = self.interner.get(id);
                let stride = desc.bound.count();
                let right_bucket = self
                    .worklist
                    .get_mut(&rank)
                    .and_then(|level| level.get_mut(&id))
                    .and_then(|bucket| bucket.split_for_parallel(stride, candidate_pages))
                    .expect("selected residual payload is splittable");

                let mut right = self.parallel_sibling();
                assert!(
                    right
                        .worklist
                        .entry(rank)
                        .or_default()
                        .insert(id, right_bucket)
                        .is_none(),
                    "fresh residual sibling unexpectedly contained work"
                );
                return Some(right);
            }

            // Distinct state buckets are disjoint affine components even when
            // neither currently contains two scheduling atoms.
            let bucket_count: usize = self.worklist.values().map(BTreeMap::len).sum();
            if bucket_count >= 2 {
                let (&rank, level) = self
                    .worklist
                    .last_key_value()
                    .expect("two buckets imply a nonempty worklist");
                let id = *level
                    .last_key_value()
                    .expect("live residual rank has a bucket")
                    .0;
                let (bucket, remove_level) = {
                    let level = self
                        .worklist
                        .get_mut(&rank)
                        .expect("selected residual rank exists");
                    let bucket = level.remove(&id).expect("selected residual state exists");
                    (bucket, level.is_empty())
                };
                if remove_level {
                    self.worklist.remove(&rank);
                }

                let mut right = self.parallel_sibling();
                right.worklist.entry(rank).or_default().insert(id, bucket);
                return Some(right);
            }

            // One unsplittable affine atom remains. Advance the exact machine
            // rather than manufacturing a second query from the seed.
            let width = self.width.max(1);
            if !self.delta.is_empty() && !self.has_full_stable(plan, width) {
                let outcome = self.delta.step(
                    root,
                    plan,
                    width,
                    &mut self.worklist,
                    &mut self.interner,
                    &mut self.stats,
                );
                // Split negotiation deliberately leaves every stable result
                // in the cold worklist; it only consumes the same geometric
                // feedback as the serial scheduler.
                self.account_delta_feedback(&outcome);
                continue;
            }
            if self.worklist.is_empty() {
                return None;
            }
            match self.pop_once_with_dispatch(
                dispatch,
                root,
                plan,
                influences,
                base_estimates,
                width,
            ) {
                // Split negotiation is a saturated throughput path. It files
                // every successor normally and deliberately does not arm the
                // first-result continuation sprint before the frontier has
                // been partitioned.
                MachineStep::Stable(StepOutcome::Advanced(_)) | MachineStep::DeltaSeeded { .. } => {
                }
                MachineStep::Stable(StepOutcome::Dead) => {
                    self.increase_width();
                    self.increase_delta_activation_width();
                }
                MachineStep::Stable(StepOutcome::Emit(rows)) => {
                    self.stage_emit(rows);
                    self.increase_width();
                    self.increase_delta_activation_width();
                }
            }
        }
    }

    fn split_for_parallel<'a>(
        &mut self,
        root: &dyn Constraint<'a>,
        plan: &ResidualPlan,
        influences: &[VariableSet; 128],
        base_estimates: &[usize; 128],
    ) -> Option<Self> {
        self.split_for_parallel_with_dispatch(
            &DirectActionDispatch,
            root,
            plan,
            influences,
            base_estimates,
        )
    }

    /// Observed counterpart of [`Self::split_for_parallel`]. The affine split
    /// policy is identical, but any concrete action needed to negotiate a
    /// fresh unsplittable seed crosses the same shadow boundary as later shard
    /// folds. This prevents parallel setup from becoming an attribution gap.
    fn split_for_parallel_shadow<'a>(
        &mut self,
        epoch: &ResidualShadowEpoch,
        root: &dyn Constraint<'a>,
        plan: &ResidualPlan,
        influences: &[VariableSet; 128],
        base_estimates: &[usize; 128],
    ) -> Option<Self> {
        self.split_for_parallel_with_dispatch(
            &ShadowActionDispatch { epoch },
            root,
            plan,
            influences,
            base_estimates,
        )
    }
}

/// Demand-driven canonical residual-state execution for any root constraint.
///
/// The iterator begins with a narrow desired actionable width, so full
/// descendant buckets can produce a result before sibling rows or candidate
/// values are evaluated.
/// With a growth factor above one, semantic branch death or raw terminal output
/// immediately prepares a geometrically wider width for later frontier work;
/// filing any nonempty successor leaves the width unchanged. When a full
/// Propose or Confirm action files fewer actionable atoms than that width, the
/// coalesced-receipt physical tail becomes hot and outranks cold sibling
/// harvesting until it emits or dies. Planning splits and readiness pops do not
/// activate a sprint on their own. With no hot lineage they retain ordinary
/// batching; within a hot lineage they may continue its deliberate
/// latency-for-reconvergence tradeoff. The token never changes canonical
/// identity or consumes work older than the coalesced receipt bound. With no
/// hot continuation, the deepest live bucket able to fill the width wins; if
/// none can, the minimum-rank bucket drains through the strict readiness gate.
/// The cap only bounds geometric width growth.
///
/// Dropping the iterator discards its remaining affine frontier. Fully drained,
/// it produces the same result multiset as [`Query::solve_residual_state`].
#[must_use]
pub struct ResidualStateIter<C, P: Fn(&Binding) -> Option<R>, R> {
    root: C,
    plan: ResidualPlan,
    postprocessing: P,
    influences: [VariableSet; 128],
    base_estimates: [usize; 128],
    state: ResidualStateMachine,
    /// Whether the serial iterator has been pulled. A started exact remainder
    /// may still be drained in parallel, but is conservatively kept as one
    /// Rayon leaf rather than split or restarted.
    iteration_started: bool,
}

// Manual implementation avoids the unnecessary `R: Clone` bound that derive
// would add: projected values are never retained in the exact raw remainder.
impl<C, P, R> Clone for ResidualStateIter<C, P, R>
where
    C: Clone,
    P: Fn(&Binding) -> Option<R> + Clone,
{
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
            plan: self.plan.clone(),
            postprocessing: self.postprocessing.clone(),
            influences: self.influences,
            base_estimates: self.base_estimates,
            state: self.state.clone(),
            iteration_started: self.iteration_started,
        }
    }
}

/// Result of fully draining an opt-in [`ResidualShadowIter`].
#[derive(Clone, Debug)]
#[must_use]
#[non_exhaustive]
pub struct ResidualShadowSolve<R> {
    /// Projected query results, preserving bag semantics.
    pub results: Vec<R>,
    /// Ordinary residual scheduler statistics from the observed execution.
    pub stats: ResidualStateStats,
    /// Final point-in-time observation snapshot.
    pub shadow: ResidualShadowSnapshot,
}

/// Serial opt-in wrapper that observes only concrete residual actions.
///
/// The wrapped iterator retains the same owned affine frontier. This wrapper
/// is deliberately separate rather than an observer field on
/// [`ResidualStateIter`], leaving ordinary execution structurally
/// uninstrumented. Every pull is unwind-guarded: a panic in planning, action
/// execution, or result projection immediately invalidates the epoch even if
/// the caller catches the unwind and keeps the iterator.
#[must_use]
pub struct ResidualShadowIter<C, P: Fn(&Binding) -> Option<R>, R> {
    inner: ResidualStateIter<C, P, R>,
    epoch: ResidualShadowEpoch,
    lifecycle: ShadowIteratorLifecycle,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ShadowIteratorLifecycle {
    /// This serial iterator closes on exhaustion and invalidates on drop.
    Owner,
    /// A Rayon producer; the top-level parallel drive owns the epoch terminal
    /// transition, so individual shard exhaustion and drop are inert.
    #[cfg(feature = "parallel")]
    Shard,
    /// Serial exhaustion already closed the epoch.
    Finished,
}

struct ShadowPullGuard {
    epoch: ResidualShadowEpoch,
    armed: bool,
}

impl ShadowPullGuard {
    fn new(epoch: ResidualShadowEpoch) -> Self {
        Self { epoch, armed: true }
    }

    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for ShadowPullGuard {
    fn drop(&mut self) {
        if self.armed {
            self.epoch.invalidate();
        }
    }
}

impl<C, P: Fn(&Binding) -> Option<R>, R> Drop for ResidualShadowIter<C, P, R> {
    fn drop(&mut self) {
        if self.lifecycle == ShadowIteratorLifecycle::Owner {
            self.epoch.invalidate();
        }
    }
}

impl<C, P: Fn(&Binding) -> Option<R>, R> ResidualShadowIter<C, P, R> {
    /// Returns the shared one-shot observation epoch.
    pub fn epoch(&self) -> &ResidualShadowEpoch {
        &self.epoch
    }

    /// Width the next observed engine resumption will use.
    pub fn current_width(&self) -> usize {
        self.inner.current_width()
    }

    /// Ordinary residual measurements accumulated so far.
    pub fn stats(&self) -> &ResidualStateStats {
        self.inner.stats()
    }

    /// Copies this epoch's observations accumulated so far.
    pub fn snapshot(&self) -> ResidualShadowSnapshot {
        self.epoch.snapshot()
    }
}

impl<C, P: Fn(&Binding) -> Option<R>, R> ResidualStateIter<C, P, R> {
    /// Overrides the initial chunk width, clamped to `1..=cap`.
    pub fn start_width(mut self, width: usize) -> Self {
        self.state.width = width.clamp(1, self.state.cap);
        self
    }

    /// Overrides the geometric growth factor (`1` keeps a fixed width).
    pub fn growth(mut self, growth: usize) -> Self {
        self.state.growth = growth.max(1);
        self
    }

    /// Overrides the geometric width-growth cap.
    ///
    /// Like [`DagIter::cap`](super::DagIter::cap), this never raises the
    /// current width. To start above the default cap, set the new cap first:
    /// `.cap(new_cap).start_width(new_cap)`.
    pub fn cap(mut self, cap: usize) -> Self {
        self.state.cap = cap.max(1);
        self.state.width = self.state.width.min(self.state.cap);
        self
    }

    /// Width the next engine resumption will use.
    pub fn current_width(&self) -> usize {
        self.state.width
    }

    /// Measurements accumulated by pulls performed so far.
    pub fn stats(&self) -> &ResidualStateStats {
        &self.state.stats
    }

    /// Wraps this exact affine remainder in a one-shot action observer.
    ///
    /// One epoch may claim one iterator. Parallel shards derived from that
    /// iterator share the already-claimed epoch; a second unrelated iterator
    /// must use a fresh epoch so leaf occurrences remain epoch-local.
    pub fn shadow(self, epoch: ResidualShadowEpoch) -> ResidualShadowIter<C, P, R> {
        epoch.inner.claim();
        ResidualShadowIter {
            inner: self,
            epoch,
            lifecycle: ShadowIteratorLifecycle::Owner,
        }
    }
}

impl<'a, C, P, R> ResidualStateIter<C, P, R>
where
    C: Constraint<'a> + 'a,
    P: Fn(&Binding) -> Option<R>,
{
    /// Fully drains the iterator and returns its results and final profile.
    pub fn collect_profiled(mut self) -> ResidualStateSolve<R> {
        let mut results = Vec::new();
        results.extend(self.by_ref());
        ResidualStateSolve {
            results,
            stats: self.state.stats,
        }
    }
}

impl<'a, C, P, R> Iterator for ResidualStateIter<C, P, R>
where
    C: Constraint<'a> + 'a,
    P: Fn(&Binding) -> Option<R>,
{
    type Item = R;

    fn next(&mut self) -> Option<Self::Item> {
        self.iteration_started = true;
        self.state.pull(
            &self.root,
            &self.plan,
            &self.postprocessing,
            &self.influences,
            &self.base_estimates,
        )
    }
}

impl<'a, C, P, R> ResidualShadowIter<C, P, R>
where
    C: Constraint<'a> + 'a,
    P: Fn(&Binding) -> Option<R>,
{
    /// Fully drains the observed iterator, normally closes its epoch, and
    /// returns results, ordinary scheduler statistics, and the final snapshot.
    pub fn collect_profiled(mut self) -> ResidualShadowSolve<R> {
        let mut results = Vec::new();
        results.extend(self.by_ref());
        ResidualShadowSolve {
            results,
            stats: self.inner.state.stats.clone(),
            shadow: self.epoch.snapshot(),
        }
    }
}

impl<'a, C, P, R> Iterator for ResidualShadowIter<C, P, R>
where
    C: Constraint<'a> + 'a,
    P: Fn(&Binding) -> Option<R>,
{
    type Item = R;

    fn next(&mut self) -> Option<Self::Item> {
        if self.lifecycle == ShadowIteratorLifecycle::Finished {
            return None;
        }
        assert_eq!(
            self.epoch.status(),
            ResidualShadowStatus::Open,
            "cannot resume a residual shadow iterator after its epoch is closed or invalidated"
        );
        let pull = ShadowPullGuard::new(self.epoch.clone());
        self.inner.iteration_started = true;
        let item = self.inner.state.pull_shadow(
            &self.epoch,
            &self.inner.root,
            &self.inner.plan,
            &self.inner.postprocessing,
            &self.inner.influences,
            &self.inner.base_estimates,
        );
        if item.is_none() && self.lifecycle == ShadowIteratorLifecycle::Owner {
            if self.epoch.finish_exhausted() == ResidualShadowStatus::Closed {
                self.lifecycle = ShadowIteratorLifecycle::Finished;
            }
        }
        pull.disarm();
        item
    }
}

fn solve<'a, P, R>(
    root: &dyn Constraint<'a>,
    postprocessing: P,
    influences: [VariableSet; 128],
    base_estimates: [usize; 128],
    mode: Search,
) -> ResidualStateSolve<R>
where
    P: Fn(&Binding) -> Option<R>,
{
    let full = root.variables();
    let plan = ResidualPlan::compile(root);
    let leaf_count = plan.len();
    let mut stats = ResidualStateStats::default();
    let mut interner = StateInterner::default();
    let mut worklist = Worklist::new();
    if matches!(mode, Search::NextVariable) {
        file_with_plan(
            &mut worklist,
            &mut interner,
            &plan,
            StateDesc {
                bound: VariableSet::new_empty(),
                phase: ResidualPhase::Ready,
            },
            StateBucket::Rows(RowBatch::seed()),
            &mut stats,
        );
    }

    let mut results = Vec::new();
    let mut binding = Binding::default();
    let mut next_activation = 0;
    while let Some((&rank, _)) = worklist.first_key_value() {
        let level = worklist
            .remove(&rank)
            .expect("observed worklist level exists");
        for (id, bucket) in level {
            let desc = interner.get(id).clone();
            debug_assert_eq!(
                desc.rank_with_span(
                    leaf_count,
                    plan.action_span(),
                    Some(&plan.finite_formula),
                    &interner.formula_pcs,
                ),
                rank
            );
            let emit_bound = desc.bound;
            stats.state_pops += 1;
            stats.readiness_pops += 1;
            match execute_task(
                root,
                &plan,
                SelectedResidualTask {
                    state: id,
                    desc,
                    bucket,
                },
                full,
                &influences,
                &base_estimates,
                &mut worklist,
                &mut interner,
                &mut stats,
                &mut next_activation,
            ) {
                StepOutcome::Advanced(_) | StepOutcome::Dead => {}
                StepOutcome::Emit(rows) => {
                    let vars: Vec<VariableId> = emit_bound.into_iter().collect();
                    let view = rows_view(&vars, &rows.rows, rows.row_count);
                    for row in 0..rows.row_count {
                        let row_view = view.row_view(row);
                        for (column, &variable) in vars.iter().enumerate() {
                            binding.set(variable, &row_view.row(0)[column]);
                        }
                        if let Some(result) = postprocessing(&binding) {
                            results.push(result);
                        }
                    }
                }
            }
        }
    }

    ResidualStateSolve { results, stats }
}

fn assert_fresh<C, P: Fn(&Binding) -> Option<R>, R>(query: &Query<C, P, R>) {
    assert!(
        !query.iteration_started
            && query.stack.is_empty()
            && query.bound.is_empty()
            && query.touched_variables.is_empty()
            && matches!(query.mode, Search::NextVariable | Search::Done),
        "cannot residual-solve a Query mid-iteration: residual execution restarts from the seed"
    );
}

impl<'a, C, P, R> Query<C, P, R>
where
    C: Constraint<'a> + 'a,
    P: Fn(&Binding) -> Option<R>,
{
    /// Lazily executes any root constraint through canonical residual states.
    ///
    /// The first pull uses a one-parent depth-first batch by default. Filing a
    /// nonempty successor preserves that width. When a full proposal or
    /// confirmation action partially survives, only its coalesced-receipt
    /// physical tail becomes the next continuation; it remains ahead of cold
    /// sibling harvesting until it emits or dies. Planning splits and
    /// readiness-selected work cannot activate a sprint themselves, but may
    /// carry an already-hot lineage forward. Death or raw terminal output grows
    /// the width geometrically for later work. Whenever no continuation is hot
    /// and no live state can fill the desired width, the minimum-rank state
    /// drains readiness-safely. Result order may differ from the ordinary
    /// iterator; a full drain preserves its result multiset.
    ///
    /// # Panics
    ///
    /// Panics if iteration has already started on this query.
    pub fn solve_residual_state_lazy(self) -> ResidualStateIter<C, P, R> {
        self.solve_residual_state_lazy_with(ResidualLowering::CONSERVATIVE)
    }

    /// Lazily executes through residual states with explicit structural
    /// lowering.
    ///
    /// Lowering is independent of scheduler selection. Passing
    /// [`ResidualLowering::CONSERVATIVE`] is identical to
    /// [`solve_residual_state_lazy`](Self::solve_residual_state_lazy).
    ///
    /// # Panics
    ///
    /// Panics if iteration has already started on this query.
    pub fn solve_residual_state_lazy_with(
        self,
        lowering: ResidualLowering,
    ) -> ResidualStateIter<C, P, R> {
        assert_fresh(&self);
        let Query {
            constraint,
            postprocessing,
            influences,
            base_estimates,
            mode,
            ..
        } = self;
        let full = constraint.variables();
        let plan = ResidualPlan::compile_lowering(&constraint, lowering);
        let state = ResidualStateMachine::new_for_plan(full, &plan, mode);
        ResidualStateIter {
            root: constraint,
            plan,
            postprocessing,
            influences,
            base_estimates,
            state,
            iteration_started: false,
        }
    }

    /// Eagerly solves any root constraint through canonical residual states.
    ///
    /// This experimental path recursively flattens the maximal nested AND
    /// region, jointly chooses the next variable and proposing leaf occurrence,
    /// and represents planning plus uniform proposal/confirmation actions as
    /// interned states. Planning states only estimate and partition; explicit
    /// action states invoke one flattened leaf over their assembled row or
    /// whole-parent candidate bucket. Histories with identical future work
    /// append into one bucket before that state runs. Union, ignore, and
    /// regular-path constraints remain opaque semantic boundaries; custom
    /// constraints do too unless they explicitly expose an associative AND
    /// shape. Opaque leaves continue through the ordinary [`Constraint`]
    /// protocol.
    ///
    /// Result order may differ from the ordinary iterator; the result
    /// multiset is the same. Use
    /// [`solve_residual_state_profiled`](Self::solve_residual_state_profiled)
    /// to inspect reconvergence and batch measurements.
    ///
    /// Flattened leaves must obey [`Constraint::estimate`]'s structural,
    /// block-uniform relevance law and remain semantically immutable during
    /// the solve.
    ///
    /// # Panics
    ///
    /// Panics if iteration has already started on this query. Residual
    /// execution always starts from the canonical empty binding.
    pub fn solve_residual_state(self) -> Vec<R> {
        self.solve_residual_state_profiled().results
    }

    /// Residual-state solve returning both results and scheduler measurements.
    ///
    /// # Panics
    ///
    /// Panics if iteration has already started on this query.
    pub fn solve_residual_state_profiled(self) -> ResidualStateSolve<R> {
        assert_fresh(&self);
        let Query {
            constraint,
            postprocessing,
            influences,
            base_estimates,
            mode,
            ..
        } = self;
        solve(
            &constraint,
            postprocessing,
            influences,
            base_estimates,
            mode,
        )
    }
}

// ---------------------------------------------------------------------------
// Explicit parallel residual execution via Rayon.
//
// A fresh residual iterator owns one affine state-machine frontier. Rayon
// requests at most `workers - 1` splits; each split moves disjoint rows,
// complete candidate-parent groups, or plan-proven page-local candidate
// occurrences into a sibling state machine. Constraint and postprocessor
// clones are created only for an actual sibling, and projected `R` values are
// never stored in either machine. A serially started iterator is still
// parallel-consumable, but its exact remainder stays one leaf.
// ---------------------------------------------------------------------------

#[cfg(feature = "parallel")]
pub use parallel::{ResidualShadowParIter, ResidualStateParIter};

#[cfg(feature = "parallel")]
mod parallel {
    use super::*;
    use rayon::iter::plumbing::{bridge_unindexed, Folder, UnindexedConsumer, UnindexedProducer};
    use rayon::iter::{IntoParallelIterator, ParallelIterator};

    /// Parallel iterator over one affine residual-state frontier.
    ///
    /// Construct it explicitly with
    /// [`Query::into_par_residual_state_iter`] for saturated block-native
    /// throughput, or convert a configured [`ResidualStateIter`] through
    /// [`IntoParallelIterator`] to preserve its selected width policy.
    pub struct ResidualStateParIter<C, P: Fn(&Binding) -> Option<R>, R> {
        inner: Box<ResidualStateIter<C, P, R>>,
        split_budget: usize,
    }

    impl<'a, C, P, R> Query<C, P, R>
    where
        C: Constraint<'a> + Clone + Send + 'a,
        P: Fn(&Binding) -> Option<R> + Clone + Send,
        R: Send,
    {
        /// Consume a fresh query as a block-native parallel residual iterator.
        ///
        /// The exact state machine starts at saturated width because this
        /// entry point is an explicit full-enumeration throughput request.
        /// Seed negotiation advances in place until an affine frontier can be
        /// split; it is never restarted. At most one residual shard per Rayon
        /// worker is created, and fully drained output preserves the serial
        /// query's result multiset rather than its order.
        ///
        /// Candidate payloads stay parent-atomic across whole-group
        /// confirmers. Once the compiled continuation proves every remaining
        /// confirmer page-local, candidate occurrences themselves become
        /// independent shard atoms.
        ///
        /// # Panics
        ///
        /// Panics if the query has already been pulled, like the serial
        /// residual entry points.
        pub fn into_par_residual_state_iter(self) -> ResidualStateParIter<C, P, R> {
            let mut residual = self.solve_residual_state_lazy();
            residual.state.width = residual.state.cap;
            residual.into_par_iter()
        }
    }

    impl<'a, C, P, R> IntoParallelIterator for ResidualStateIter<C, P, R>
    where
        C: Constraint<'a> + Clone + Send + 'a,
        P: Fn(&Binding) -> Option<R> + Clone + Send,
        R: Send,
    {
        type Item = R;
        type Iter = ResidualStateParIter<C, P, R>;

        fn into_par_iter(self) -> Self::Iter {
            ResidualStateParIter {
                inner: Box::new(self),
                // Derived inside the pool that consumes this iterator.
                split_budget: 0,
            }
        }
    }

    impl<'a, C, P, R> UnindexedProducer for ResidualStateParIter<C, P, R>
    where
        C: Constraint<'a> + Clone + Send + 'a,
        P: Fn(&Binding) -> Option<R> + Clone + Send,
        R: Send,
    {
        type Item = R;

        fn split(mut self) -> (Self, Option<Self>) {
            if self.inner.iteration_started || self.split_budget == 0 {
                self.split_budget = 0;
                return (self, None);
            }
            self.split_budget -= 1;

            let right_state = {
                let iter = &mut *self.inner;
                iter.state.split_for_parallel(
                    &iter.root,
                    &iter.plan,
                    &iter.influences,
                    &iter.base_estimates,
                )
            };
            let Some(right_state) = right_state else {
                self.split_budget = 0;
                return (self, None);
            };

            // Only an actual shard pays for cloning user-owned execution
            // machinery. The affine state itself is moved, never cloned.
            let right = ResidualStateIter {
                root: self.inner.root.clone(),
                plan: self.inner.plan.clone(),
                postprocessing: self.inner.postprocessing.clone(),
                influences: self.inner.influences,
                base_estimates: self.inner.base_estimates,
                state: right_state,
                iteration_started: false,
            };
            let left_budget = self.split_budget / 2;
            let right_budget = self.split_budget - left_budget;
            self.split_budget = left_budget;
            (
                self,
                Some(ResidualStateParIter {
                    inner: Box::new(right),
                    split_budget: right_budget,
                }),
            )
        }

        fn fold_with<F: Folder<R>>(self, mut folder: F) -> F {
            let ResidualStateParIter {
                inner: mut iter, ..
            } = self;
            while !folder.full() {
                match iter.next() {
                    Some(item) => folder = folder.consume(item),
                    None => break,
                }
            }
            folder
        }
    }

    impl<'a, C, P, R> ParallelIterator for ResidualStateParIter<C, P, R>
    where
        C: Constraint<'a> + Clone + Send + 'a,
        P: Fn(&Binding) -> Option<R> + Clone + Send,
        R: Send,
    {
        type Item = R;

        fn drive_unindexed<Con>(mut self, consumer: Con) -> Con::Result
        where
            Con: UnindexedConsumer<Self::Item>,
        {
            self.split_budget = if self.inner.iteration_started {
                0
            } else {
                rayon::current_num_threads().saturating_sub(1)
            };
            bridge_unindexed(self, consumer)
        }
    }

    /// Parallel iterator over one observed affine residual frontier.
    ///
    /// Shards share only the already-claimed observation epoch. Residual
    /// payload remains moved through the same splitter as
    /// [`ResidualStateParIter`], and every shard allocates globally unique
    /// event ordinals within that epoch. Every live producer owns an armed
    /// abandonment guard; only observing its exact `None` exhaustion disarms
    /// it, so initial-full consumers, split-side cancellation, and unwind
    /// invalidate the top-level drive.
    pub struct ResidualShadowParIter<C, P: Fn(&Binding) -> Option<R>, R> {
        inner: Box<ResidualShadowIter<C, P, R>>,
    }

    impl<'a, C, P, R> IntoParallelIterator for ResidualShadowIter<C, P, R>
    where
        C: Constraint<'a> + Clone + Send + 'a,
        P: Fn(&Binding) -> Option<R> + Clone + Send,
        R: Send,
    {
        type Item = R;
        type Iter = ResidualShadowParIter<C, P, R>;

        fn into_par_iter(self) -> Self::Iter {
            ResidualShadowParIter {
                inner: Box::new(self),
            }
        }
    }

    struct ResidualShadowProducer<C, P: Fn(&Binding) -> Option<R>, R> {
        inner: Box<ResidualShadowIter<C, P, R>>,
        split_budget: usize,
        guard: ShadowProducerGuard,
    }

    struct ShadowProducerGuard {
        abandoned: Arc<AtomicBool>,
        armed: bool,
    }

    impl ShadowProducerGuard {
        fn new(abandoned: Arc<AtomicBool>, armed: bool) -> Self {
            Self { abandoned, armed }
        }

        fn sibling(&self) -> Self {
            Self::new(Arc::clone(&self.abandoned), true)
        }

        fn disarm(&mut self) {
            self.armed = false;
        }
    }

    impl Drop for ShadowProducerGuard {
        fn drop(&mut self) {
            if self.armed {
                self.abandoned.store(true, Ordering::Release);
            }
        }
    }

    impl<'a, C, P, R> UnindexedProducer for ResidualShadowProducer<C, P, R>
    where
        C: Constraint<'a> + Clone + Send + 'a,
        P: Fn(&Binding) -> Option<R> + Clone + Send,
        R: Send,
    {
        type Item = R;

        fn split(mut self) -> (Self, Option<Self>) {
            if self.inner.inner.iteration_started || self.split_budget == 0 {
                self.split_budget = 0;
                return (self, None);
            }
            self.split_budget -= 1;

            let right_state = {
                let iter = &mut self.inner.inner;
                iter.state.split_for_parallel_shadow(
                    &self.inner.epoch,
                    &iter.root,
                    &iter.plan,
                    &iter.influences,
                    &iter.base_estimates,
                )
            };
            let Some(right_state) = right_state else {
                self.split_budget = 0;
                return (self, None);
            };

            let right_inner = ResidualStateIter {
                root: self.inner.inner.root.clone(),
                plan: self.inner.inner.plan.clone(),
                postprocessing: self.inner.inner.postprocessing.clone(),
                influences: self.inner.inner.influences,
                base_estimates: self.inner.inner.base_estimates,
                state: right_state,
                iteration_started: false,
            };
            let right = ResidualShadowIter {
                inner: right_inner,
                epoch: self.inner.epoch.clone(),
                lifecycle: ShadowIteratorLifecycle::Shard,
            };
            let left_budget = self.split_budget / 2;
            let right_budget = self.split_budget - left_budget;
            self.split_budget = left_budget;
            let right_guard = self.guard.sibling();
            (
                self,
                Some(ResidualShadowProducer {
                    inner: Box::new(right),
                    split_budget: right_budget,
                    guard: right_guard,
                }),
            )
        }

        fn fold_with<F: Folder<R>>(self, mut folder: F) -> F {
            let ResidualShadowProducer {
                inner: mut iter,
                mut guard,
                ..
            } = self;
            while !folder.full() {
                match iter.next() {
                    Some(item) => folder = folder.consume(item),
                    None => {
                        guard.disarm();
                        break;
                    }
                }
            }
            folder
        }
    }

    struct ShadowParallelDrive {
        epoch: ResidualShadowEpoch,
        finished: bool,
    }

    impl ShadowParallelDrive {
        fn new(epoch: ResidualShadowEpoch) -> Self {
            Self {
                epoch,
                finished: false,
            }
        }

        fn finish(mut self, complete: bool) {
            if complete {
                self.epoch.finish_exhausted();
            } else {
                self.epoch.invalidate();
            }
            self.finished = true;
        }
    }

    impl Drop for ShadowParallelDrive {
        fn drop(&mut self) {
            if !self.finished {
                self.epoch.invalidate();
            }
        }
    }

    impl<'a, C, P, R> ParallelIterator for ResidualShadowParIter<C, P, R>
    where
        C: Constraint<'a> + Clone + Send + 'a,
        P: Fn(&Binding) -> Option<R> + Clone + Send,
        R: Send,
    {
        type Item = R;

        fn drive_unindexed<Con>(self, consumer: Con) -> Con::Result
        where
            Con: UnindexedConsumer<Self::Item>,
        {
            let mut inner = self.inner;
            let epoch = inner.epoch.clone();
            let finished = inner.lifecycle == ShadowIteratorLifecycle::Finished;
            if !finished {
                assert_eq!(
                    epoch.status(),
                    ResidualShadowStatus::Open,
                    "cannot resume a residual shadow iterator after its epoch is closed or invalidated"
                );
            }
            let split_budget = if finished || inner.inner.iteration_started {
                0
            } else {
                rayon::current_num_threads().saturating_sub(1)
            };
            if !finished {
                inner.lifecycle = ShadowIteratorLifecycle::Shard;
            }
            let drive = ShadowParallelDrive::new(epoch);
            let abandoned = Arc::new(AtomicBool::new(false));
            let result = bridge_unindexed(
                ResidualShadowProducer {
                    inner,
                    split_budget,
                    guard: ShadowProducerGuard::new(Arc::clone(&abandoned), !finished),
                },
                consumer,
            );
            drive.finish(!abandoned.load(Ordering::Acquire));
            result
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inline::encodings::genid::GenId;
    use crate::inline::{Inline, IntoInline};
    use crate::query::intersectionconstraint::IntersectionConstraint;
    use crate::query::unionconstraint::UnionConstraint;
    #[cfg(feature = "parallel")]
    use rayon::prelude::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Mutex;

    #[test]
    fn residual_lowering_has_exactly_six_canonical_forms() {
        let forms: std::collections::HashSet<_> = [
            FormulaScope::OpaqueLeaves,
            FormulaScope::UnionLeaves,
            FormulaScope::WholeRoot,
        ]
        .into_iter()
        .flat_map(|scope| {
            [false, true]
                .into_iter()
                .map(move |transitions| ResidualLowering::new(scope, transitions))
        })
        .collect();

        assert_eq!(forms.len(), 6);
        assert_eq!(ResidualLowering::default(), ResidualLowering::CONSERVATIVE);
        assert_eq!(
            ResidualLowering::FULL,
            ResidualLowering::new(FormulaScope::WholeRoot, true)
        );
    }

    #[derive(Clone, Copy)]
    struct ShapeLeaf(VariableId);

    impl Constraint<'static> for ShapeLeaf {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(self.0)
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            if variable != self.0 {
                return false;
            }
            out.fill(1, view.len());
            true
        }

        fn propose(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }

        fn confirm(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }
    }

    #[derive(Clone, Copy)]
    struct CapabilityLeaf {
        variable: VariableId,
        page_local: bool,
    }

    impl Constraint<'static> for CapabilityLeaf {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(self.variable)
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            if variable != self.variable {
                return false;
            }
            out.fill(1, view.len());
            true
        }

        fn propose(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }

        fn confirm(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }

        fn residual_confirm_is_page_local(&self) -> bool {
            self.page_local
        }
    }

    #[derive(Clone, Copy)]
    struct GroupedCapabilityLeaf(CapabilityLeaf);

    impl Constraint<'static> for GroupedCapabilityLeaf {
        fn variables(&self) -> VariableSet {
            self.0.variables()
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            self.0.estimate(variable, view, out)
        }

        fn propose(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            self.0.propose(variable, view, candidates);
        }

        fn confirm(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            self.0.confirm(variable, view, candidates);
        }

        fn residual_confirm_is_page_local(&self) -> bool {
            self.0.page_local
        }

        fn residual_delta_confirm_grouping_requirements(
            &self,
            variable: VariableId,
        ) -> Option<VariableSet> {
            (variable == self.0.variable).then_some(VariableSet::new_empty())
        }
    }

    #[derive(Clone, Copy)]
    struct ConditionalGroupedCapabilityLeaf {
        variable: VariableId,
        required: VariableId,
    }

    impl Constraint<'static> for ConditionalGroupedCapabilityLeaf {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(self.variable)
                .union(VariableSet::new_singleton(self.required))
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            if variable != self.variable {
                return false;
            }
            out.fill(1, view.len());
            true
        }

        fn propose(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }

        fn confirm(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }

        fn residual_confirm_is_page_local(&self) -> bool {
            true
        }

        fn residual_delta_confirm_grouping_requirements(
            &self,
            variable: VariableId,
        ) -> Option<VariableSet> {
            (variable == self.variable).then_some(VariableSet::new_singleton(self.required))
        }
    }

    #[derive(Clone)]
    struct FanoutLeaf {
        variable: VariableId,
        values: Arc<Vec<RawInline>>,
    }

    impl Constraint<'static> for FanoutLeaf {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(self.variable)
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            if variable != self.variable {
                return false;
            }
            out.fill(self.values.len(), view.len());
            true
        }

        fn propose(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            assert_eq!(variable, self.variable);
            for row in 0..view.len() {
                candidates.extend_row(row as u32, self.values.iter().copied());
            }
        }

        fn confirm(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }
    }

    #[derive(Clone)]
    struct PagedProposalLeaf {
        variable: VariableId,
        values: Arc<Vec<RawInline>>,
        transition_source: bool,
        proposes: Arc<AtomicUsize>,
        pages: Arc<AtomicUsize>,
    }

    impl Constraint<'static> for PagedProposalLeaf {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(self.variable)
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            if variable != self.variable {
                return false;
            }
            out.fill(self.values.len(), view.len());
            true
        }

        fn propose(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            assert_eq!(variable, self.variable);
            self.proposes.fetch_add(1, Ordering::Relaxed);
            for row in 0..view.len() {
                candidates.extend_row(row as u32, self.values.iter().copied());
            }
        }

        fn confirm(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }

        fn residual_delta_source_is_paged(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
        ) -> bool {
            false
        }

        fn residual_proposal_source_is_paged(
            &self,
            variable: VariableId,
            _view: &RowsView<'_>,
        ) -> bool {
            variable == self.variable
        }

        fn residual_proposal_source_has_transition_roots(
            &self,
            variable: VariableId,
            _view: &RowsView<'_>,
        ) -> bool {
            variable == self.variable && self.transition_source
        }

        fn residual_delta_source_page(
            &self,
            variable: VariableId,
            _view: &RowsView<'_>,
            candidates: Option<&[RawInline]>,
            cursor: ResidualDeltaSourceCursor,
            limit: usize,
            roots: &mut Vec<ResidualDeltaOutput>,
            accepted: &mut Vec<RawInline>,
        ) -> Option<ResidualDeltaSourcePage> {
            assert_eq!(variable, self.variable);
            assert!(candidates.is_none());
            assert!(roots.is_empty());
            self.pages.fetch_add(1, Ordering::Relaxed);
            let offset = match cursor {
                ResidualDeltaSourceCursor::Start => 0,
                ResidualDeltaSourceCursor::Offset(offset) => {
                    usize::try_from(offset).expect("test proposal cursor exceeds usize")
                }
                ResidualDeltaSourceCursor::After(_) => {
                    panic!("test proposal source uses ordinal cursors")
                }
            };
            let end = offset.saturating_add(limit).min(self.values.len());
            accepted.extend_from_slice(&self.values[offset..end]);
            Some(ResidualDeltaSourcePage {
                next: (end < self.values.len()).then_some(ResidualDeltaSourceCursor::Offset(
                    u64::try_from(end).expect("test proposal cursor exceeds u64"),
                )),
                examined: end - offset,
            })
        }
    }

    #[derive(Clone, Copy)]
    enum PanicPhase {
        Planning,
        Propose,
    }

    #[derive(Clone)]
    struct PanicLeaf {
        variable: VariableId,
        phase: PanicPhase,
        estimate_calls: Arc<AtomicUsize>,
    }

    impl Constraint<'static> for PanicLeaf {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(self.variable)
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            if variable != self.variable {
                return false;
            }
            let call = self.estimate_calls.fetch_add(1, Ordering::Relaxed);
            if matches!(self.phase, PanicPhase::Planning) && call != 0 {
                panic!("intentional residual planning panic");
            }
            out.fill(1, view.len());
            true
        }

        fn propose(
            &self,
            variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
            assert_eq!(variable, self.variable);
            if matches!(self.phase, PanicPhase::Propose) {
                panic!("intentional residual action panic");
            }
        }

        fn confirm(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }
    }

    fn panic_leaf(phase: PanicPhase) -> PanicLeaf {
        PanicLeaf {
            variable: 0,
            phase,
            estimate_calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    #[derive(Clone)]
    struct PageFilterLeaf {
        variable: VariableId,
        estimate: usize,
        accepted: Option<RawInline>,
        calls: Arc<Mutex<Vec<usize>>>,
    }

    impl Constraint<'static> for PageFilterLeaf {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(self.variable)
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            if variable != self.variable {
                return false;
            }
            out.fill(self.estimate, view.len());
            true
        }

        fn propose(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }

        fn confirm(
            &self,
            variable: VariableId,
            _view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            assert_eq!(variable, self.variable);
            self.calls.lock().unwrap().push(candidates.len());
            if let Some(accepted) = self.accepted {
                candidates.retain(|_, value| *value == accepted);
            }
        }

        fn residual_confirm_is_page_local(&self) -> bool {
            true
        }
    }

    #[derive(Clone)]
    struct ParityFilterLeaf {
        variable: VariableId,
        estimate: usize,
        parity: u8,
        calls: Arc<Mutex<Vec<usize>>>,
    }

    impl Constraint<'static> for ParityFilterLeaf {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(self.variable)
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            if variable != self.variable {
                return false;
            }
            out.fill(self.estimate, view.len());
            true
        }

        fn propose(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }

        fn confirm(
            &self,
            variable: VariableId,
            _view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            assert_eq!(variable, self.variable);
            self.calls.lock().unwrap().push(candidates.len());
            let parity = self.parity;
            candidates.retain(|_, value| value[0] & 1 == parity);
        }

        fn residual_confirm_is_page_local(&self) -> bool {
            true
        }
    }

    #[derive(Clone)]
    struct WholeGroupMinimumLeaf {
        variable: VariableId,
        estimate: usize,
        calls: Arc<Mutex<Vec<usize>>>,
    }

    impl Constraint<'static> for WholeGroupMinimumLeaf {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(self.variable)
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            if variable != self.variable {
                return false;
            }
            out.fill(self.estimate, view.len());
            true
        }

        fn propose(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }

        fn confirm(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            assert_eq!(variable, self.variable);
            self.calls.lock().unwrap().push(candidates.len());
            confirm_per_row(view, candidates, |_, values| {
                let minimum = values.iter().copied().min();
                values.retain(|value| Some(*value) == minimum);
            });
        }
    }

    #[derive(Clone, Copy)]
    struct ZeroVariableTruth(bool);

    impl Constraint<'static> for ZeroVariableTruth {
        fn variables(&self) -> VariableSet {
            VariableSet::new_empty()
        }

        fn estimate(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _out: &mut EstimateSink<'_>,
        ) -> bool {
            false
        }

        fn propose(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }

        fn confirm(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }

        fn satisfied(&self, _view: &RowsView<'_>) -> bool {
            self.0
        }
    }

    #[test]
    fn private_seeded_frame_settles_fully_bound_truth_before_filing() {
        let mut live = SeededResidualFrame::new(
            ZeroVariableTruth(true),
            FrameSeedRow::empty(),
            ResidualLowering::FULL,
        );
        let first = live
            .next_binding()
            .expect("a true zero-variable seed emits once");
        assert!(first.bound.is_empty());
        assert!(live.next_binding().is_none());

        let mut dead = SeededResidualFrame::new(
            ZeroVariableTruth(false),
            FrameSeedRow::empty(),
            ResidualLowering::FULL,
        );
        assert!(dead.next_binding().is_none());
        assert!(dead.machine.worklist.is_empty());
        assert!(dead.machine.delta.is_empty());

        let mut context = VariableContext::new();
        let variable = context.next_variable::<GenId>();
        let accepted = [3; 32];
        let mut accepted_seed = SeededResidualFrame::new(
            variable.is(Inline::new(accepted)),
            FrameSeedRow::one(variable.index, accepted),
            ResidualLowering::FULL,
        );
        assert_eq!(
            accepted_seed
                .next_binding()
                .expect("a satisfied fully bound seed emits once")
                .get(variable.index),
            Some(&accepted),
        );
        assert!(accepted_seed.next_binding().is_none());

        let mut rejected_seed = SeededResidualFrame::new(
            variable.is(Inline::new(accepted)),
            FrameSeedRow::one(variable.index, [4; 32]),
            ResidualLowering::FULL,
        );
        assert!(rejected_seed.next_binding().is_none());
        assert!(rejected_seed.machine.worklist.is_empty());
        assert!(rejected_seed.machine.delta.is_empty());
    }

    #[test]
    fn private_seeded_frame_starts_at_its_local_bound_rank() {
        let value = [7; 32];
        let mut frame = SeededResidualFrame::new(
            FanoutLeaf {
                variable: 0,
                values: Arc::new(vec![[9; 32]]),
            },
            FrameSeedRow::one(0, value),
            ResidualLowering::FULL,
        );

        let (&rank, level) = frame
            .machine
            .worklist
            .first_key_value()
            .expect("the live seed is filed");
        let (&state, _) = level.first_key_value().expect("one seed state");
        let desc = frame.machine.interner.get(state);
        assert_eq!(desc.bound, VariableSet::new_singleton(0));
        assert!(matches!(desc.phase, ResidualPhase::Ready));
        assert_eq!(
            rank,
            desc.rank_with_span(
                frame.plan.len(),
                frame.plan.action_span(),
                Some(&frame.plan.finite_formula),
                &frame.machine.interner.formula_pcs,
            )
        );

        let binding = frame
            .next_binding()
            .expect("a fully bound true seed emits exactly once");
        assert_eq!(binding.get(0), Some(&value));
        assert!(frame.next_binding().is_none());
    }

    #[derive(Clone)]
    struct DeltaSeedTrap {
        variable: VariableId,
        calls: Arc<AtomicUsize>,
    }

    impl Constraint<'static> for DeltaSeedTrap {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(self.variable)
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            if variable != self.variable {
                return false;
            }
            out.fill(1, view.len());
            true
        }

        fn propose(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }

        fn confirm(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }

        fn residual_delta_seeds(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _seeds: &mut Vec<ResidualDeltaSeed>,
        ) -> bool {
            self.calls.fetch_add(1, Ordering::Relaxed);
            true
        }
    }

    /// A concrete root whose manual `Clone` records only the copies paid for
    /// by actual Rayon siblings.
    #[cfg(feature = "parallel")]
    struct CloneCountingFanout {
        variable: VariableId,
        values: Arc<Vec<RawInline>>,
        clones: Arc<AtomicUsize>,
        proposes: Arc<AtomicUsize>,
    }

    #[cfg(feature = "parallel")]
    impl Clone for CloneCountingFanout {
        fn clone(&self) -> Self {
            self.clones.fetch_add(1, Ordering::Relaxed);
            Self {
                variable: self.variable,
                values: Arc::clone(&self.values),
                clones: Arc::clone(&self.clones),
                proposes: Arc::clone(&self.proposes),
            }
        }
    }

    #[cfg(feature = "parallel")]
    impl Constraint<'static> for CloneCountingFanout {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(self.variable)
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            if variable != self.variable {
                return false;
            }
            out.fill(self.values.len(), view.len());
            true
        }

        fn propose(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            assert_eq!(variable, self.variable);
            self.proposes.fetch_add(1, Ordering::Relaxed);
            for row in 0..view.len() {
                candidates.extend_row(row as u32, self.values.iter().copied());
            }
        }

        fn confirm(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }
    }

    #[derive(Clone)]
    struct VerbLeaf {
        variable: VariableId,
        estimate: usize,
        accepts: bool,
        proposes: Arc<AtomicUsize>,
        confirms: Arc<AtomicUsize>,
    }

    #[derive(Clone)]
    struct SinkShapeLeaf {
        variable: VariableId,
        estimate: usize,
        log: Arc<Mutex<Vec<(ActionVerb, bool)>>>,
    }

    impl Constraint<'static> for SinkShapeLeaf {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(self.variable)
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            if variable != self.variable {
                return false;
            }
            out.fill(self.estimate, view.len());
            true
        }

        fn propose(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            assert_eq!(variable, self.variable);
            self.log.lock().unwrap().push((
                ActionVerb::Propose,
                matches!(candidates, CandidateSink::Values(_)),
            ));
            for row in 0..view.len() {
                candidates.push(row as u32, raw(42));
            }
        }

        fn confirm(
            &self,
            variable: VariableId,
            _view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            assert_eq!(variable, self.variable);
            self.log.lock().unwrap().push((
                ActionVerb::Confirm,
                matches!(candidates, CandidateSink::Values(_)),
            ));
        }
    }

    impl Constraint<'static> for VerbLeaf {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(self.variable)
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            if variable != self.variable {
                return false;
            }
            out.fill(self.estimate, view.len());
            true
        }

        fn propose(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            assert_eq!(variable, self.variable);
            self.proposes.fetch_add(1, Ordering::Relaxed);
            for row in 0..view.len() {
                candidates.push(row as u32, raw(1));
            }
        }

        fn confirm(
            &self,
            variable: VariableId,
            _view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            assert_eq!(variable, self.variable);
            self.confirms.fetch_add(1, Ordering::Relaxed);
            if !self.accepts {
                candidates.retain(|_, _| false);
            }
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct LoggedAction {
        verb: ActionVerb,
        leaf_occurrence: usize,
        parent_rows: usize,
        candidate_occurrences: usize,
    }

    #[derive(Clone)]
    struct LoggedLeaf {
        variable: VariableId,
        leaf_occurrence: usize,
        estimate: usize,
        proposed: Arc<Vec<RawInline>>,
        accepted: Option<RawInline>,
        log: Arc<Mutex<Vec<LoggedAction>>>,
    }

    impl LoggedLeaf {
        fn record(&self, verb: ActionVerb, parent_rows: usize, candidate_occurrences: usize) {
            self.log.lock().unwrap().push(LoggedAction {
                verb,
                leaf_occurrence: self.leaf_occurrence,
                parent_rows,
                candidate_occurrences,
            });
            if let Some(action) = current_residual_action() {
                let started = action.elapsed();
                action.record_executor_sample(ExecutorMeasurement {
                    executor: "test-cpu",
                    operation: match verb {
                        ActionVerb::Support => "logged-support",
                        ActionVerb::Propose => "logged-propose",
                        ActionVerb::Confirm => "logged-confirm",
                    },
                    work_unit: "occurrences",
                    work_units: match verb {
                        ActionVerb::Support | ActionVerb::Propose => parent_rows,
                        ActionVerb::Confirm => candidate_occurrences,
                    },
                    started,
                    wall: Duration::ZERO,
                });
            }
        }
    }

    impl Constraint<'static> for LoggedLeaf {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(self.variable)
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            if variable != self.variable {
                return false;
            }
            out.fill(self.estimate, view.len());
            true
        }

        fn propose(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            assert_eq!(variable, self.variable);
            self.record(ActionVerb::Propose, view.len(), 0);
            for row in 0..view.len() {
                candidates.extend_row(row as u32, self.proposed.iter().copied());
            }
        }

        fn confirm(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            assert_eq!(variable, self.variable);
            self.record(ActionVerb::Confirm, view.len(), candidates.len());
            if let Some(accepted) = self.accepted {
                candidates.retain(|_, value| *value == accepted);
            }
        }

        fn residual_confirm_is_page_local(&self) -> bool {
            true
        }
    }

    #[derive(Clone, Copy)]
    struct FirstParentProposer {
        parent: VariableId,
        variable: VariableId,
    }

    impl Constraint<'static> for FirstParentProposer {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(self.parent).union(VariableSet::new_singleton(self.variable))
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            if variable != self.variable {
                return false;
            }
            out.fill(1, view.len());
            true
        }

        fn propose(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            assert_eq!(variable, self.variable);
            if view.len() != 0 {
                candidates.push(0, raw(42));
            }
        }

        fn confirm(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }
    }

    #[derive(Clone)]
    struct StripedConfirmer {
        variable: VariableId,
        parent: VariableId,
        parity: u8,
        calls: Arc<AtomicUsize>,
        rows: Arc<AtomicUsize>,
    }

    impl Constraint<'static> for StripedConfirmer {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(self.variable).union(VariableSet::new_singleton(self.parent))
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            if variable != self.variable {
                return false;
            }
            let parent = view
                .col(self.parent)
                .expect("striped confirmer requires a bound parent");
            out.extend(view.iter().map(|row| {
                if row[parent][0] % 2 == self.parity {
                    1
                } else {
                    8
                }
            }));
            true
        }

        fn propose(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }

        fn confirm(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
            assert_eq!(variable, self.variable);
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.rows.fetch_add(view.len(), Ordering::Relaxed);
        }
    }

    #[derive(Clone, Copy)]
    struct RowEstimateLeaf {
        parent: VariableId,
        variable: VariableId,
        estimates: [usize; 2],
    }

    impl Constraint<'static> for RowEstimateLeaf {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(self.parent).union(VariableSet::new_singleton(self.variable))
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            if variable != self.variable {
                return false;
            }
            let parent = view
                .col(self.parent)
                .expect("row-dependent estimate requires its parent binding");
            out.extend(
                view.iter()
                    .map(|row| self.estimates[(row[parent][0] & 1) as usize]),
            );
            true
        }

        fn propose(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }

        fn confirm(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }
    }

    #[derive(Clone)]
    struct RowAdaptiveLeaf {
        parent: VariableId,
        variable: VariableId,
        estimates: [usize; 4],
        value: RawInline,
        proposed_parents: Arc<Mutex<Vec<Vec<RawInline>>>>,
    }

    impl Constraint<'static> for RowAdaptiveLeaf {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(self.parent).union(VariableSet::new_singleton(self.variable))
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            if variable != self.variable {
                return false;
            }
            if let Some(parent) = view.col(self.parent) {
                out.extend(
                    view.iter()
                        .map(|row| self.estimates[(row[parent][0] & 3) as usize]),
                );
            } else {
                out.fill(100, view.len());
            }
            true
        }

        fn propose(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            assert_eq!(variable, self.variable);
            let parent = view
                .col(self.parent)
                .expect("row-adaptive proposal requires its parent binding");
            self.proposed_parents
                .lock()
                .unwrap()
                .push(view.iter().map(|row| row[parent]).collect());
            for row in 0..view.len() {
                candidates.push(row as u32, self.value);
            }
        }

        fn confirm(
            &self,
            variable: VariableId,
            _view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            assert_eq!(variable, self.variable);
            candidates.retain(|_, value| *value == self.value);
        }
    }

    #[derive(Clone)]
    struct MaskedUnionArm {
        parent: VariableId,
        variable: VariableId,
        live_parity: u8,
        value: RawInline,
        proposal_rows: Arc<AtomicUsize>,
    }

    impl Constraint<'static> for MaskedUnionArm {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(self.parent).union(VariableSet::new_singleton(self.variable))
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            if variable == self.parent {
                out.fill(100, view.len());
                return true;
            }
            if variable != self.variable {
                return false;
            }
            if let Some(parent) = view.col(self.parent) {
                out.extend(view.iter().map(|row| {
                    if row[parent][0] & 1 == self.live_parity {
                        1
                    } else {
                        100
                    }
                }));
            } else {
                out.fill(1, view.len());
            }
            true
        }

        fn propose(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            assert_eq!(variable, self.variable);
            assert!(
                candidates.is_empty(),
                "every union arm needs an empty proposal sink"
            );
            self.proposal_rows.fetch_add(view.len(), Ordering::Relaxed);
            for row in 0..view.len() {
                candidates.push(row as u32, self.value);
            }
        }

        fn confirm(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            if variable == self.variable {
                let value = self.value;
                candidates.retain(|_, candidate| *candidate == value);
            } else {
                assert_eq!(variable, self.parent);
                if let Some(value_column) = view.col(self.variable) {
                    let live_parity = self.live_parity;
                    let accepted_value = self.value;
                    confirm_per_row(view, candidates, |row, values| {
                        values.retain(|parent| {
                            parent[0] & 1 == live_parity && row[value_column] == accepted_value
                        });
                    });
                }
            }
        }

        fn satisfied(&self, view: &RowsView<'_>) -> bool {
            let Some(parent) = view.col(self.parent) else {
                return true;
            };
            let variable = view.col(self.variable);
            view.iter().all(|row| {
                row[parent][0] & 1 == self.live_parity
                    && variable.is_none_or(|variable| row[variable] == self.value)
            })
        }
    }

    type ShapeConstraint = Box<dyn Constraint<'static> + Send + Sync>;

    #[cfg(feature = "parallel")]
    type ParallelShapeConstraint = Arc<dyn Constraint<'static> + Send + Sync>;

    #[cfg(feature = "parallel")]
    fn parallel_shape<C>(constraint: C) -> ParallelShapeConstraint
    where
        C: Constraint<'static> + Send + Sync + 'static,
    {
        Arc::new(constraint)
    }

    #[cfg(feature = "parallel")]
    fn with_parallel_workers<R: Send>(threads: usize, operation: impl FnOnce() -> R + Send) -> R {
        rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build()
            .unwrap()
            .install(operation)
    }

    #[cfg(feature = "parallel")]
    fn parallel_paged_filter_fixture(
        values: Vec<RawInline>,
        accepted: RawInline,
    ) -> Arc<IntersectionConstraint<ParallelShapeConstraint>> {
        let estimate = values.len();
        Arc::new(IntersectionConstraint::new(vec![
            parallel_shape(FanoutLeaf {
                variable: 0,
                values: Arc::new(values),
            }),
            parallel_shape(PageFilterLeaf {
                variable: 0,
                estimate: estimate + 1,
                accepted: None,
                calls: Arc::new(Mutex::new(Vec::new())),
            }),
            parallel_shape(PageFilterLeaf {
                variable: 0,
                estimate: estimate + 2,
                accepted: Some(accepted),
                calls: Arc::new(Mutex::new(Vec::new())),
            }),
        ]))
    }

    fn shape_leaf(variable: VariableId) -> ShapeConstraint {
        Box::new(ShapeLeaf(variable))
    }

    fn shape_and(children: Vec<ShapeConstraint>) -> ShapeConstraint {
        Box::new(IntersectionConstraint::new(children))
    }

    fn raw(byte: u8) -> RawInline {
        let mut value = [0; 32];
        value[0] = byte;
        value
    }

    #[test]
    fn formula_pc_arena_is_exact_compact_and_query_local() {
        #[allow(dead_code)]
        enum LegacyResidualPhaseLayout {
            Ready,
            Propose {
                variable: VariableId,
                relevant: ChildSet,
                proposer: usize,
            },
            Candidate {
                variable: VariableId,
                relevant: ChildSet,
                checked: ChildSet,
            },
            Confirm {
                variable: VariableId,
                relevant: ChildSet,
                checked: ChildSet,
                confirmer: usize,
            },
            Formula {
                counter: FormulaProgramCounter,
            },
        }

        #[allow(dead_code)]
        struct LegacyStateDescLayout {
            bound: VariableSet,
            phase: LegacyResidualPhaseLayout,
        }

        let mut arena = FormulaPcInterner::default();
        let relevant = ChildSet::empty(2).with_inserted(0);
        let resume = arena.intern_resume(FormulaOuterResume {
            variable: 0,
            occurrence: 0,
            verb: UnionVerb::Propose { relevant },
        });
        let parent = arena.intern_record(
            FormulaPcRecord {
                focus: FormulaFocus::Plan {
                    node: FormulaNodeId(5),
                    stage: FormulaStage::Confirm,
                    done: ChildSet::empty(2),
                },
                return_to: None,
                resume,
            },
            7,
        );
        let return_to = arena.intern_return(FormulaReturnRecord {
            kind: FormulaReturnKind::Child,
            parent,
            child: 1,
        });
        let first = FormulaPcRecord {
            focus: FormulaFocus::Plan {
                node: FormulaNodeId(7),
                stage: FormulaStage::Confirm,
                done: ChildSet::empty(2).with_inserted(0),
            },
            return_to: Some(return_to),
            resume,
        };
        let mut second = first.clone();
        let FormulaFocus::Plan { done, .. } = &mut second.focus else {
            unreachable!("the fixture starts at a Plan")
        };
        done.insert(1);

        let first_id = arena.intern_record(first.clone(), 11);
        assert_eq!(arena.intern_record(first.clone(), 11), first_id);
        let second_id = arena.intern_record(second.clone(), 13);
        assert_ne!(first_id, second_id);
        assert_eq!(arena.len(), 3);
        assert_eq!(arena.resume_len(), 1);
        assert_eq!(arena.return_len(), 1);
        assert_eq!(arena.get(first_id), &first);
        assert_eq!(arena.get(second_id), &second);
        assert_eq!(arena.clone().get(first_id), &first);
        assert_eq!(
            arena.return_by_id(return_to),
            &FormulaReturnRecord {
                kind: FormulaReturnKind::Child,
                parent,
                child: 1,
            }
        );

        // Numeric IDs are deliberately arena-local. Divergent query/Rayon
        // clones may allocate the same next number to different records; the
        // machine splitter therefore clones descriptors and payload with the
        // arena and never exchanges them after divergence.
        let mut left = arena.clone();
        let mut right = arena.clone();
        let mut left_record = first.clone();
        left_record.focus = FormulaFocus::Action {
            node: FormulaNodeId(11),
            stage: FormulaStage::Confirm,
        };
        let mut right_record = first.clone();
        right_record.focus = FormulaFocus::Action {
            node: FormulaNodeId(12),
            stage: FormulaStage::Confirm,
        };
        let left_id = left.intern_record(left_record, 17);
        let right_id = right.intern_record(right_record, 17);
        assert_eq!(left_id, right_id);
        assert_ne!(left.get(left_id), right.get(right_id));

        let legacy_counter = FormulaProgramCounter {
            focus: first.focus.clone(),
            returns: vec![FormulaReturnSite {
                kind: FormulaReturnKind::Child,
                parent: FormulaNodeId(5),
                parent_stage: FormulaStage::Confirm,
                child: 1,
                done: ChildSet::empty(2),
            }]
            .into_boxed_slice(),
            resume: FormulaOuterResume {
                variable: 0,
                occurrence: 0,
                verb: UnionVerb::Propose {
                    relevant: ChildSet::empty(2).with_inserted(0),
                },
            },
        };

        assert_eq!(std::mem::size_of::<FormulaPcId>(), 4);
        assert_eq!(std::mem::size_of::<Option<FormulaReturnId>>(), 4);
        assert!(
            std::mem::size_of::<FormulaPcRecord>() < std::mem::size_of_val(&legacy_counter),
            "a persistent PC record should be smaller than the boxed structural PC"
        );
        assert!(
            std::mem::size_of::<StateDesc>() < std::mem::size_of::<LegacyStateDescLayout>(),
            "a compact PC ID should reduce the owning descriptor layout"
        );
        eprintln!(
            "formula_pc={} formula_record={} formula_pc_id={} state_desc={} legacy_state_desc={} resumes={} returns={} records={}",
            std::mem::size_of::<FormulaProgramCounter>(),
            std::mem::size_of::<FormulaPcRecord>(),
            std::mem::size_of::<FormulaPcId>(),
            std::mem::size_of::<StateDesc>(),
            std::mem::size_of::<LegacyStateDescLayout>(),
            arena.resume_len(),
            arena.return_len(),
            arena.len(),
        );
    }

    fn candidate_payload(parent_count: usize, candidates: Candidates) -> CandidatePayload {
        CandidatePayload::from_tagged(candidates, parent_count)
    }

    fn ready_desc(bound_count: usize) -> StateDesc {
        let mut bound = VariableSet::new_empty();
        for variable in 0..bound_count {
            bound.set(variable);
        }
        StateDesc {
            bound,
            phase: ResidualPhase::Ready,
        }
    }

    fn ready_bucket(bound_count: usize, row_count: usize, marker: u8) -> StateBucket {
        StateBucket::Rows(RowBatch {
            rows: vec![raw(marker); bound_count * row_count],
            row_count,
        })
    }

    fn scheduler_fixture(entries: &[(usize, usize, u8)]) -> ResidualStateMachine {
        let mut machine = ResidualStateMachine::new(VariableSet::new_empty(), 1, Search::Done);
        for &(bound_count, row_count, marker) in entries {
            file(
                &mut machine.worklist,
                &mut machine.interner,
                machine.leaf_count,
                ready_desc(bound_count),
                ready_bucket(bound_count, row_count, marker),
                &mut machine.stats,
            );
        }
        machine
    }

    fn ready_action_fixture(
        leaves: Vec<RowEstimateLeaf>,
    ) -> (Vec<(VariableId, usize, usize)>, ResidualStateStats) {
        const PARENT: VariableId = 0;
        let root = IntersectionConstraint::new(leaves);
        let plan = ResidualPlan::compile(&root);
        let desc = StateDesc {
            bound: VariableSet::new_singleton(PARENT),
            phase: ResidualPhase::Ready,
        };
        let rows = RowBatch {
            rows: vec![raw(0), raw(1)],
            row_count: 2,
        };
        let influences = [VariableSet::new_empty(); 128];
        let base_estimates = [1; 128];
        let mut worklist = Worklist::new();
        let mut interner = StateInterner::default();
        let mut stats = ResidualStateStats::default();

        let _continuation = ready_plan_transition(
            &root,
            &plan,
            &desc,
            rows,
            root.variables(),
            &influences,
            &base_estimates,
            &mut worklist,
            &mut interner,
            &mut stats,
        );

        let mut actions = Vec::new();
        for level in worklist.values() {
            for (&id, bucket) in level {
                let ResidualPhase::Propose {
                    variable, proposer, ..
                } = interner.get(id).phase
                else {
                    panic!("Ready planning filed a non-proposal state")
                };
                actions.push((variable, proposer, bucket.row_count()));
            }
        }
        actions.sort_unstable();
        (actions, stats)
    }

    #[test]
    fn ready_agglomeration_coalesces_near_variable_choices() {
        const PARENT: VariableId = 0;
        const LEFT: VariableId = 1;
        const RIGHT: VariableId = 2;
        let (actions, stats) = ready_action_fixture(vec![
            RowEstimateLeaf {
                parent: PARENT,
                variable: LEFT,
                estimates: [1, 2],
            },
            RowEstimateLeaf {
                parent: PARENT,
                variable: RIGHT,
                estimates: [2, 1],
            },
        ]);

        assert_eq!(actions, [(LEFT, 0, 2)]);
        assert_eq!(stats.ready_preferred_variable_groups, 2);
        assert_eq!(stats.ready_scheduled_variable_groups, 1);
        assert_eq!(stats.ready_proposal_groups, 1);
        assert_eq!(stats.agglomerated_ready_pops, 1);
    }

    #[test]
    fn ready_agglomeration_selects_each_scheduled_rows_exact_proposer() {
        const PARENT: VariableId = 0;
        const LEFT: VariableId = 1;
        const RIGHT: VariableId = 2;
        let (actions, stats) = ready_action_fixture(vec![
            RowEstimateLeaf {
                parent: PARENT,
                variable: LEFT,
                estimates: [1, 4],
            },
            RowEstimateLeaf {
                parent: PARENT,
                variable: LEFT,
                estimates: [4, 2],
            },
            RowEstimateLeaf {
                parent: PARENT,
                variable: RIGHT,
                estimates: [2, 1],
            },
        ]);

        assert_eq!(actions, [(LEFT, 0, 1), (LEFT, 1, 1)]);
        assert_eq!(stats.ready_preferred_variable_groups, 2);
        assert_eq!(stats.ready_scheduled_variable_groups, 1);
        assert_eq!(stats.ready_proposal_groups, 2);
        assert_eq!(stats.agglomerated_ready_pops, 1);
    }

    #[test]
    fn ready_agglomeration_keeps_incompatible_exact_choices() {
        const PARENT: VariableId = 0;
        const LEFT: VariableId = 1;
        const RIGHT: VariableId = 2;
        let (actions, stats) = ready_action_fixture(vec![
            RowEstimateLeaf {
                parent: PARENT,
                variable: LEFT,
                estimates: [1, 64],
            },
            RowEstimateLeaf {
                parent: PARENT,
                variable: RIGHT,
                estimates: [64, 1],
            },
        ]);

        assert_eq!(actions, [(LEFT, 0, 1), (RIGHT, 1, 1)]);
        assert_eq!(stats.ready_preferred_variable_groups, 2);
        assert_eq!(stats.ready_scheduled_variable_groups, 2);
        assert_eq!(stats.ready_proposal_groups, 2);
        assert_eq!(stats.agglomerated_ready_pops, 0);
    }

    #[test]
    fn synthetic_root_formula_keeps_outer_per_row_variable_choice() {
        const PARENT: VariableId = 0;
        const LEFT: VariableId = 1;
        const RIGHT: VariableId = 2;
        let root = IntersectionConstraint::new(vec![
            RowEstimateLeaf {
                parent: PARENT,
                variable: LEFT,
                estimates: [1, 64],
            },
            RowEstimateLeaf {
                parent: PARENT,
                variable: RIGHT,
                estimates: [64, 1],
            },
        ]);
        let plan = ResidualPlan::compile_lowering(
            &root,
            ResidualLowering::new(FormulaScope::WholeRoot, false),
        );
        let desc = StateDesc {
            bound: VariableSet::new_singleton(PARENT),
            phase: ResidualPhase::Ready,
        };
        let rows = RowBatch {
            rows: vec![raw(0), raw(1)],
            row_count: 2,
        };
        let influences = [VariableSet::new_empty(); 128];
        let base_estimates = [1; 128];
        let mut worklist = Worklist::new();
        let mut interner = StateInterner::default();
        let mut stats = ResidualStateStats::default();
        let _ = ready_plan_transition(
            &root,
            &plan,
            &desc,
            rows,
            root.variables(),
            &influences,
            &base_estimates,
            &mut worklist,
            &mut interner,
            &mut stats,
        );

        let mut actions = Vec::new();
        for level in worklist.values() {
            for (&id, bucket) in level {
                let ResidualPhase::Propose {
                    variable, proposer, ..
                } = interner.get(id).phase
                else {
                    panic!("Ready planning filed a non-proposal state")
                };
                actions.push((variable, proposer, bucket.row_count()));
            }
        }
        actions.sort_unstable();
        assert_eq!(actions, [(LEFT, 0, 1), (RIGHT, 0, 1)]);
        assert_eq!(stats.ready_preferred_variable_groups, 2);
        assert_eq!(stats.ready_scheduled_variable_groups, 2);
    }

    #[test]
    fn box_and_arc_forward_object_safe_residual_shapes() {
        let boxed: Box<dyn Constraint<'static> + Send + Sync> =
            Box::new(IntersectionConstraint::new(vec![ShapeLeaf(0)]));
        let boxed_children = match boxed.residual_shape() {
            ConstraintShape::And(children) => children,
            ConstraintShape::ScopedAnd(_) | ConstraintShape::Opaque => {
                panic!("boxed intersection changed shape")
            }
        };
        assert_eq!(boxed_children.len(), 1);
        assert_eq!(
            boxed_children.child(0).variables(),
            VariableSet::new_singleton(0)
        );

        let arc: Arc<dyn Constraint<'static> + Send + Sync> =
            Arc::new(IntersectionConstraint::new(vec![ShapeLeaf(1)]));
        let arc_children = match arc.residual_shape() {
            ConstraintShape::And(children) => children,
            ConstraintShape::ScopedAnd(_) | ConstraintShape::Opaque => {
                panic!("Arc intersection changed shape")
            }
        };
        assert_eq!(arc_children.len(), 1);
        assert_eq!(
            arc_children.child(0).variables(),
            VariableSet::new_singleton(1)
        );
    }

    #[test]
    fn nested_and_plan_is_deterministic_preorder_and_resolves_paths() {
        let root = IntersectionConstraint::new(vec![
            shape_leaf(0),
            shape_and(vec![
                shape_leaf(1),
                shape_and(vec![shape_leaf(2), shape_leaf(3)]),
            ]),
            shape_leaf(4),
        ]);
        let plan = ResidualPlan::compile(&root);
        let paths: Vec<Vec<usize>> = plan
            .leaves
            .iter()
            .map(|leaf| leaf.path.0.to_vec())
            .collect();
        assert_eq!(
            paths,
            [vec![0], vec![1, 0], vec![1, 1, 0], vec![1, 1, 1], vec![2]]
        );
        for variable in 0..5 {
            assert_eq!(
                plan.resolve(&root, variable).variables(),
                VariableSet::new_singleton(variable)
            );
        }

        let right = IntersectionConstraint::new(vec![shape_and(vec![
            shape_leaf(0),
            shape_and(vec![
                shape_leaf(1),
                shape_and(vec![shape_leaf(2), shape_leaf(3)]),
            ]),
        ])]);
        let right_paths: Vec<Vec<usize>> = ResidualPlan::compile(&right)
            .leaves
            .iter()
            .map(|leaf| leaf.path.0.to_vec())
            .collect();
        assert_eq!(
            right_paths,
            [
                vec![0, 0],
                vec![0, 1, 0],
                vec![0, 1, 1, 0],
                vec![0, 1, 1, 1]
            ]
        );
    }

    #[test]
    fn opaque_root_is_one_empty_path_occurrence() {
        let root = ShapeLeaf(9);
        let plan = ResidualPlan::compile(&root);
        assert_eq!(
            plan.leaves,
            vec![ConstraintPath(Vec::new().into_boxed_slice())]
        );
        assert_eq!(
            plan.resolve(&root, 0).variables(),
            VariableSet::new_singleton(9)
        );
    }

    #[test]
    fn whole_root_scope_normalizes_formula_identity_shells() {
        let opaque = ShapeLeaf(9);
        let opaque_plan = ResidualPlan::compile_lowering(
            &opaque,
            ResidualLowering::new(FormulaScope::WholeRoot, true),
        );
        assert!(!opaque_plan.synthetic_root_formula);
        assert_eq!(opaque_plan.len(), 1);
        assert!(opaque_plan.finite_formula.root(0).is_none());
        assert!(opaque_plan.leaves[0].path.0.is_empty());

        let nested = shape_and(vec![shape_and(vec![shape_leaf(9)])]);
        let nested_plan = ResidualPlan::compile_lowering(
            nested.as_ref(),
            ResidualLowering::new(FormulaScope::WholeRoot, true),
        );
        assert!(!nested_plan.synthetic_root_formula);
        assert_eq!(nested_plan.len(), 1);
        assert!(nested_plan.finite_formula.root(0).is_none());
        assert_eq!(nested_plan.leaves[0].path.0.as_ref(), [0, 0]);
    }

    #[test]
    fn synthetic_formula_flattens_only_the_maximal_root_and() {
        let arm = || shape_and(vec![shape_leaf(0), shape_leaf(0)]);
        let union = UnionConstraint::new(vec![arm(), arm()]);
        let root = IntersectionConstraint::new(vec![
            shape_and(vec![shape_leaf(0), shape_leaf(0)]),
            Box::new(union) as ShapeConstraint,
        ]);
        let plan = ResidualPlan::compile_lowering(
            &root,
            ResidualLowering::new(FormulaScope::WholeRoot, false),
        );
        assert_eq!(plan.len(), 1);
        assert!(plan.synthetic_root_formula);
        let program = &plan.finite_formula;
        let root = program.root(0).expect("synthetic formula has a root");
        let FiniteFormulaNodeKind::And { children } = &program.node(root).kind else {
            panic!("exposed root AND did not compile as AND")
        };
        assert_eq!(children.len(), 3, "direct nested AND was not flattened");
        assert_eq!(program.node(children[0]).kind, FiniteFormulaNodeKind::Atom);
        assert_eq!(program.node(children[1]).kind, FiniteFormulaNodeKind::Atom);

        let FiniteFormulaNodeKind::Or {
            children: union_arms,
        } = &program.node(children[2]).kind
        else {
            panic!("the OR boundary disappeared")
        };
        assert_eq!(union_arms.len(), 2);
        assert!(union_arms
            .iter()
            .all(|&arm| matches!(program.node(arm).kind, FiniteFormulaNodeKind::And { .. })));
    }

    #[test]
    fn whole_root_scope_absorbs_union_leaf_lowering() {
        let union = UnionConstraint::new(vec![shape_leaf(0), shape_leaf(0)]);
        let root =
            IntersectionConstraint::new(vec![shape_leaf(0), Box::new(union) as ShapeConstraint]);

        let union_leaves = ResidualPlan::compile_lowering(
            &root,
            ResidualLowering::new(FormulaScope::UnionLeaves, false),
        );
        assert!(!union_leaves.synthetic_root_formula);
        assert_eq!(union_leaves.len(), 2);
        assert!(union_leaves.finite_formula.root(0).is_none());
        assert!(union_leaves.finite_formula.root(1).is_some());

        let whole_root = ResidualPlan::compile_lowering(
            &root,
            ResidualLowering::new(FormulaScope::WholeRoot, false),
        );
        assert!(whole_root.synthetic_root_formula);
        assert_eq!(whole_root.len(), 1);
        assert!(whole_root.finite_formula.root(0).is_some());
        assert!(whole_root
            .finite_formula
            .nodes
            .iter()
            .any(|node| matches!(&node.kind, FiniteFormulaNodeKind::Or { .. })));
    }

    #[test]
    fn synthetic_formula_repeated_occurrences_have_distinct_action_sites() {
        let shared = Arc::new(CapabilityLeaf {
            variable: 0,
            page_local: true,
        });
        let root = IntersectionConstraint::new(vec![shared.clone(), shared]);
        let plan = ResidualPlan::compile_lowering(
            &root,
            ResidualLowering::new(FormulaScope::WholeRoot, false),
        );
        let program = &plan.finite_formula;
        let root = program.root(0).unwrap();
        let FiniteFormulaNodeKind::And { children } = &program.node(root).kind else {
            panic!("synthetic root is not AND")
        };
        assert_ne!(children[0], children[1]);
        assert_ne!(
            plan.formula_action_occurrence(0, children[0]),
            plan.formula_action_occurrence(0, children[1])
        );
        assert_ne!(
            program.node(children[0]).path,
            program.node(children[1]).path
        );
    }

    #[test]
    fn formula_proposal_streamability_accepts_only_linear_synthetic_roots() {
        fn start(plan: &ResidualPlan) -> FormulaProgramCounter {
            plan.finite_formula.start(
                0,
                0,
                UnionVerb::Propose {
                    relevant: ChildSet::empty(plan.len()).with_inserted(0),
                },
            )
        }

        let lowering = ResidualLowering::FULL;

        let linear_root = IntersectionConstraint::new(vec![
            Box::new(CapabilityLeaf {
                variable: 0,
                page_local: false,
            }) as ShapeConstraint,
            shape_and(vec![
                Box::new(CapabilityLeaf {
                    variable: 0,
                    page_local: true,
                }),
                Box::new(CapabilityLeaf {
                    variable: 0,
                    page_local: true,
                }),
            ]),
        ]);
        let linear_plan = ResidualPlan::compile_lowering(&linear_root, lowering);
        let linear_start = start(&linear_plan);
        let FiniteFormulaNodeKind::And { children } = &linear_plan
            .finite_formula
            .node(linear_plan.finite_formula.root(0).unwrap())
            .kind
        else {
            panic!("synthetic conjunction did not compile as AND")
        };
        assert_eq!(children.len(), 3, "nested root AND should be flattened");
        let linear_action = linear_plan
            .finite_formula
            .select_child_as_action(&linear_start, 0);
        assert_eq!(
            linear_plan.formula_proposal_streamability(&linear_action, VariableSet::new_empty(),),
            FormulaProposalStreamability::Linear,
            "the focused proposer itself need not be a page-local confirmer"
        );

        let non_local_root = IntersectionConstraint::new(vec![
            Box::new(CapabilityLeaf {
                variable: 0,
                page_local: false,
            }) as ShapeConstraint,
            Box::new(CapabilityLeaf {
                variable: 0,
                page_local: false,
            }),
        ]);
        let non_local_plan = ResidualPlan::compile_lowering(&non_local_root, lowering);
        let non_local_action = non_local_plan
            .finite_formula
            .select_child_as_action(&start(&non_local_plan), 0);
        assert_eq!(
            non_local_plan
                .formula_proposal_streamability(&non_local_action, VariableSet::new_empty(),),
            FormulaProposalStreamability::Barrier(
                FormulaProposalStreamBarrier::NonPageLocalConfirm
            )
        );

        let grouped_root = IntersectionConstraint::new(vec![
            Box::new(CapabilityLeaf {
                variable: 0,
                page_local: false,
            }) as ShapeConstraint,
            Box::new(GroupedCapabilityLeaf(CapabilityLeaf {
                variable: 0,
                page_local: true,
            })),
        ]);
        let grouped_plan = ResidualPlan::compile_lowering(&grouped_root, lowering);
        let grouped_action = grouped_plan
            .finite_formula
            .select_child_as_action(&start(&grouped_plan), 0);
        assert_eq!(
            grouped_plan.formula_proposal_streamability(&grouped_action, VariableSet::new_empty(),),
            FormulaProposalStreamability::Barrier(FormulaProposalStreamBarrier::GroupedConfirm)
        );

        let union = UnionConstraint::new(vec![
            Box::new(CapabilityLeaf {
                variable: 0,
                page_local: true,
            }) as ShapeConstraint,
            Box::new(CapabilityLeaf {
                variable: 0,
                page_local: true,
            }),
        ]);
        let union_plan = ResidualPlan::compile_lowering(&union, lowering);
        let union_action = union_plan
            .finite_formula
            .select_child_as_action(&start(&union_plan), 0);
        assert_eq!(
            union_plan.formula_proposal_streamability(&union_action, VariableSet::new_empty(),),
            FormulaProposalStreamability::Barrier(FormulaProposalStreamBarrier::OrFrame)
        );

        let old_formula_plan = ResidualPlan::compile_lowering(
            &union,
            ResidualLowering::new(FormulaScope::UnionLeaves, true),
        );
        let old_formula_action = old_formula_plan
            .finite_formula
            .select_child_as_action(&start(&old_formula_plan), 0);
        assert_eq!(
            old_formula_plan
                .formula_proposal_streamability(&old_formula_action, VariableSet::new_empty(),),
            FormulaProposalStreamability::Barrier(FormulaProposalStreamBarrier::NotSyntheticRoot)
        );
    }

    #[test]
    fn formula_grouped_confirm_capability_depends_on_bound_schema() {
        let root = IntersectionConstraint::new(vec![
            Box::new(CapabilityLeaf {
                variable: 0,
                page_local: false,
            }) as ShapeConstraint,
            Box::new(ConditionalGroupedCapabilityLeaf {
                variable: 0,
                required: 1,
            }),
        ]);
        let plan = ResidualPlan::compile_lowering(&root, ResidualLowering::FULL);
        let start = plan.finite_formula.start(
            0,
            0,
            UnionVerb::Propose {
                relevant: ChildSet::empty(plan.len()).with_inserted(0),
            },
        );
        let action = plan.finite_formula.select_child_as_action(&start, 0);

        assert_eq!(
            plan.formula_proposal_streamability(&action, VariableSet::new_empty()),
            FormulaProposalStreamability::Linear,
            "an unmet grouping prerequisite leaves the continuation pageable"
        );
        assert_eq!(
            plan.formula_proposal_streamability(&action, VariableSet::new_singleton(1)),
            FormulaProposalStreamability::Barrier(FormulaProposalStreamBarrier::GroupedConfirm),
            "binding the prerequisite restores the parent-atomic barrier"
        );
    }

    #[test]
    fn finite_formula_compiles_a_direct_or_and_canonical_arm_progress() {
        let root = UnionConstraint::new(vec![shape_leaf(0), shape_leaf(0)]);
        let plan = ResidualPlan::compile_finite_unions(&root);
        let program = &plan.finite_formula;
        let root = program.root(0).expect("lowered Union has a formula root");
        let FiniteFormulaNodeKind::Or { children } = &program.node(root).kind else {
            panic!("a lowered Union did not compile to Or")
        };
        assert_eq!(children.len(), 2);
        assert_eq!(program.node(children[0]).support_span, 2);
        assert_eq!(program.node(children[0]).execution_span, 2);
        assert_eq!(program.node(children[1]).support_span, 2);
        assert_eq!(program.node(children[1]).execution_span, 2);
        assert_eq!(program.node(root).support_span, 10);
        assert_eq!(program.node(root).execution_span, 16);
        assert_eq!(program.node(children[0]).kind, FiniteFormulaNodeKind::Atom);
        assert_eq!(
            program.node(children[0]).path,
            FormulaPath(vec![FormulaStep::Or(0)].into_boxed_slice())
        );
        assert_eq!(
            program.node(children[0]).capabilities,
            FormulaNodeCapabilities {
                confirm_page_local: false,
                grouped_delta_confirm_requirements: Box::new([]),
            }
        );

        let mut relevant = ChildSet::empty(plan.len());
        relevant.insert(0);
        let start = program.start(0, 0, UnionVerb::Propose { relevant });
        let run_arm = |counter: FormulaProgramCounter, arm| {
            let action = program.select_child(&counter, arm);
            let complete = program.complete(&action);
            match program.resume(&complete) {
                FormulaSuccessor::Formula(counter) => counter,
                FormulaSuccessor::Guard { .. } => panic!("ordinary arm returned as a guard"),
                FormulaSuccessor::Outer(_) => panic!("one Or arm completed the root"),
            }
        };
        let left_then_right = run_arm(run_arm(start.clone(), 0), 1);
        let right_then_left = run_arm(run_arm(start, 1), 0);
        assert_eq!(
            left_then_right, right_then_left,
            "exact done masks must erase historical arm order"
        );
    }

    #[test]
    fn persistent_formula_pcs_match_structural_oracle_and_reconverge() {
        fn assert_equivalent(
            program: &FiniteFormulaProgram,
            arena: &FormulaPcInterner,
            compact: FormulaPcId,
            structural: &FormulaProgramCounter,
        ) {
            assert_eq!(arena.materialize(compact), *structural);
            assert_eq!(arena.grade(compact), program.grade(structural));
        }

        let and_root =
            IntersectionConstraint::new(vec![shape_leaf(0), shape_leaf(0), shape_leaf(0)]);
        let and_plan = ResidualPlan::compile_lowering(
            &and_root,
            ResidualLowering::new(FormulaScope::WholeRoot, false),
        );
        let and_program = &and_plan.finite_formula;
        let verb = UnionVerb::Propose {
            relevant: ChildSet::empty(and_plan.len()).with_inserted(0),
        };
        let mut and_arena = FormulaPcInterner::default();
        let mut run_prefix = |order: [usize; 2]| {
            let mut structural = and_program.start(0, 0, verb.clone());
            let mut compact = and_arena.start(and_program, 0, 0, verb.clone());
            assert_equivalent(and_program, &and_arena, compact, &structural);
            for child in order {
                structural = and_program.select_child_as_action(&structural, child);
                compact = and_arena.select_child_as_action(and_program, compact, child);
                assert_equivalent(and_program, &and_arena, compact, &structural);

                let structural_complete = and_program.complete(&structural);
                let compact_complete = and_arena.complete(and_program, compact);
                assert_equivalent(
                    and_program,
                    &and_arena,
                    compact_complete,
                    &structural_complete,
                );
                let FormulaSuccessor::Formula(next_structural) =
                    and_program.resume(&structural_complete)
                else {
                    panic!("a two-child prefix completed a three-child root")
                };
                let Ok(InternedFormulaSuccessor::Formula(next_compact)) =
                    and_arena.resume_completed(and_program, compact_complete)
                else {
                    panic!("a compact two-child prefix completed a three-child root")
                };
                structural = next_structural;
                compact = next_compact;
                assert_equivalent(and_program, &and_arena, compact, &structural);
            }
            (structural, compact)
        };
        let (left_first, left_first_id) = run_prefix([0, 1]);
        let (right_first, right_first_id) = run_prefix([1, 0]);
        assert_eq!(left_first, right_first);
        assert_eq!(left_first_id, right_first_id);

        let or_root = UnionConstraint::new(vec![shape_leaf(0), shape_leaf(0)]);
        let or_plan = ResidualPlan::compile_finite_unions(&or_root);
        let or_program = &or_plan.finite_formula;
        let or_verb = UnionVerb::Propose {
            relevant: ChildSet::empty(or_plan.len()).with_inserted(0),
        };
        let mut or_arena = FormulaPcInterner::default();
        let structural_start = or_program.start(0, 0, or_verb.clone());
        let compact_start = or_arena.start(or_program, 0, 0, or_verb);

        let structural_guard = or_program.guard_child(&structural_start, 0);
        let compact_guard = or_arena.guard_child(or_program, compact_start, 0);
        assert_equivalent(or_program, &or_arena, compact_guard, &structural_guard);
        let structural_guard_complete = or_program.complete(&structural_guard);
        let compact_guard_complete = or_arena.complete(or_program, compact_guard);
        let FormulaSuccessor::Guard {
            parent: structural_parent,
            child: structural_child,
        } = or_program.resume(&structural_guard_complete)
        else {
            panic!("structural support did not return to its OR guard")
        };
        let Ok(InternedFormulaSuccessor::Guard {
            parent: compact_parent,
            child: compact_child,
        }) = or_arena.resume_completed(or_program, compact_guard_complete)
        else {
            panic!("compact support did not return to its OR guard")
        };
        assert_eq!(structural_child, compact_child);
        assert_equivalent(or_program, &or_arena, compact_parent, &structural_parent);

        let structural_false = or_program.skip_child(&structural_parent, structural_child);
        let compact_false = or_arena.skip_child(or_program, compact_parent, compact_child);
        let structural_true_action =
            or_program.select_supported_child(&structural_parent, structural_child);
        let compact_true_action =
            or_arena.select_supported_child(or_program, compact_parent, compact_child);
        assert_equivalent(
            or_program,
            &or_arena,
            compact_true_action,
            &structural_true_action,
        );
        let structural_true_complete = or_program.complete(&structural_true_action);
        let compact_true_complete = or_arena.complete(or_program, compact_true_action);
        let FormulaSuccessor::Formula(structural_true) =
            or_program.resume(&structural_true_complete)
        else {
            panic!("structural OR arm did not return to its parent")
        };
        let Ok(InternedFormulaSuccessor::Formula(compact_true)) =
            or_arena.resume_completed(or_program, compact_true_complete)
        else {
            panic!("compact OR arm did not return to its parent")
        };
        assert_eq!(structural_true, structural_false);
        assert_eq!(compact_true, compact_false);
        assert_equivalent(or_program, &or_arena, compact_true, &structural_true);
    }

    #[test]
    fn finite_formula_or_guard_is_strict_and_true_false_paths_reconverge() {
        let root = UnionConstraint::new(vec![shape_leaf(0), shape_leaf(0)]);
        let plan = ResidualPlan::compile_finite_unions(&root);
        let program = &plan.finite_formula;
        let mut relevant = ChildSet::empty(plan.len());
        relevant.insert(0);
        let start = program.start(0, 0, UnionVerb::Propose { relevant });

        let guard = program.guard_child(&start, 0);
        assert!(matches!(
            guard.focus,
            FormulaFocus::Action {
                stage: FormulaStage::Support,
                ..
            }
        ));
        assert!(program.grade(&guard) > program.grade(&start));
        assert!(!plan.formula_uses_candidate_pages(&guard, VariableSet::new_empty(),));

        let guard_complete = program.complete(&guard);
        assert!(program.grade(&guard_complete) > program.grade(&guard));
        let FormulaSuccessor::Guard { parent, child } = program.resume(&guard_complete) else {
            panic!("support did not return to its OR guard")
        };
        assert_eq!(parent, start);
        assert_eq!(child, 0);

        let false_path = program.skip_child(&parent, child);
        let true_action = program.select_supported_child(&parent, child);
        assert!(matches!(
            true_action.focus,
            FormulaFocus::Action {
                stage: FormulaStage::Propose,
                ..
            }
        ));
        assert!(program.grade(&true_action) > program.grade(&guard_complete));
        let true_complete = program.complete(&true_action);
        assert!(program.grade(&true_complete) > program.grade(&true_action));
        let FormulaSuccessor::Formula(true_path) = program.resume(&true_complete) else {
            panic!("executed OR arm did not return to its parent")
        };
        assert_eq!(true_path, false_path);
        assert_eq!(program.grade(&true_path), program.grade(&false_path));
        assert!(program.grade(&false_path) > program.grade(&guard_complete));
    }

    #[test]
    fn finite_formula_support_short_circuits_erase_boolean_witness_history() {
        let nested = UnionConstraint::new(vec![shape_leaf(0), shape_leaf(0)]);
        let guarded =
            IntersectionConstraint::new(vec![Box::new(nested) as ShapeConstraint, shape_leaf(0)]);
        let root = UnionConstraint::new(vec![Box::new(guarded) as ShapeConstraint, shape_leaf(0)]);
        let plan = ResidualPlan::compile_finite_unions(&root);
        let program = &plan.finite_formula;
        let mut relevant = ChildSet::empty(plan.len());
        relevant.insert(0);
        let outer = program.start(0, 0, UnionVerb::Propose { relevant });
        let and_support = program.guard_child(&outer, 0);
        let FormulaFocus::Plan {
            node: and_node,
            stage: FormulaStage::Support,
            ..
        } = and_support.focus
        else {
            panic!("guarded AND did not enter support planning")
        };
        let and_children = program.node(and_node).children().unwrap();

        let nested_support = program.select_child(&and_support, 0);
        let FormulaFocus::Plan {
            node: nested_node,
            stage: FormulaStage::Support,
            ..
        } = nested_support.focus
        else {
            panic!("nested OR did not enter support planning")
        };
        let nested_children = program.node(nested_node).children().unwrap();
        let nested_true = |child| {
            let atom = program.select_child(&nested_support, child);
            let atom_complete = program.complete(&atom);
            let FormulaSuccessor::Formula(or_plan) = program.resume(&atom_complete) else {
                panic!("support atom did not return to nested OR")
            };
            program.complete_support_short_circuit(&or_plan, true)
        };
        let nested_left = nested_true(0);
        let nested_right = nested_true(1);
        assert_eq!(nested_left, nested_right);
        assert!(nested_children.len() == 2);

        let FormulaSuccessor::Formula(and_after_nested) = program.resume(&nested_left) else {
            panic!("supported nested OR did not return to AND")
        };
        let direct_false_atom = program.select_child(&and_support, 1);
        let direct_false_complete = program.complete(&direct_false_atom);
        let FormulaSuccessor::Formula(direct_false_plan) = program.resume(&direct_false_complete)
        else {
            panic!("support atom did not return to guarded AND")
        };
        let direct_short = program.complete_support_short_circuit(&direct_false_plan, false);

        let late_false_atom = program.select_child(&and_after_nested, 1);
        let late_false_complete = program.complete(&late_false_atom);
        let FormulaSuccessor::Formula(late_false_plan) = program.resume(&late_false_complete)
        else {
            panic!("late support atom did not return to guarded AND")
        };
        let late_short = program.complete_support_short_circuit(&late_false_plan, false);
        assert_eq!(direct_short, late_short);
        assert_eq!(and_children.len(), 2);

        let FormulaSuccessor::Guard {
            parent: resumed_outer,
            child,
        } = program.resume(&direct_short)
        else {
            panic!("short-circuited AND did not return to its OR guard")
        };
        assert_eq!(resumed_outer, outer);
        assert_eq!(child, 0);
    }

    #[test]
    fn finite_formula_support_falls_back_when_support_hook_is_unsupported() {
        let delta_calls = Arc::new(AtomicUsize::new(0));
        let root = UnionConstraint::new(vec![DeltaSeedTrap {
            variable: 0,
            calls: Arc::clone(&delta_calls),
        }]);
        let plan = ResidualPlan::compile_lowering(
            &root,
            ResidualLowering::new(FormulaScope::UnionLeaves, true),
        );
        let mut relevant = ChildSet::empty(plan.len());
        relevant.insert(0);
        let mut machine = ResidualStateMachine::new_for_plan(root.variables(), &plan, Search::Done);
        let parent = machine.interner.start_formula(
            &plan.finite_formula,
            0,
            0,
            UnionVerb::Propose { relevant },
        );
        let support = machine
            .interner
            .formula_pcs
            .guard_child(&plan.finite_formula, parent, 0);
        let root_node = plan.finite_formula.root(0).unwrap();
        let task = SelectedResidualTask {
            state: StateId(0),
            desc: StateDesc {
                bound: VariableSet::new_empty(),
                phase: ResidualPhase::Formula { counter: support },
            },
            bucket: StateBucket::Formula(FormulaBatch::from_proposal(
                RowBatch::seed(),
                vec![ActivationId(0)],
                &plan.finite_formula.node(root_node).kind,
            )),
        };
        let returned = machine
            .seed_delta_formula(&root, &plan, task)
            .expect_err("an unsupported support hook must retain synchronous execution");
        assert_eq!(
            returned.desc.phase,
            ResidualPhase::Formula { counter: support }
        );
        assert_eq!(returned.bucket.row_count(), 1);
        assert_eq!(delta_calls.load(Ordering::Relaxed), 0);
        assert_eq!(machine.stats, ResidualStateStats::default());
    }

    #[test]
    fn finite_formula_compiles_and_to_or_as_structural_return_sites() {
        let nested = UnionConstraint::new(vec![shape_leaf(0), shape_leaf(0)]);
        let guarded = IntersectionConstraint::new(vec![
            shape_leaf(0),
            Box::new(nested) as ShapeConstraint,
            shape_leaf(0),
        ]);
        let root = UnionConstraint::new(vec![Box::new(guarded) as ShapeConstraint, shape_leaf(0)]);
        let plan = ResidualPlan::compile_finite_unions(&root);
        let program = &plan.finite_formula;
        let outer = program.root(0).unwrap();
        let FiniteFormulaNodeKind::Or {
            children: outer_children,
        } = &program.node(outer).kind
        else {
            panic!("formula root is not Or")
        };
        let guarded = outer_children[0];
        let FiniteFormulaNodeKind::And {
            children: and_children,
        } = &program.node(guarded).kind
        else {
            panic!("outer arm is not And")
        };
        let nested = and_children[1];
        let FiniteFormulaNodeKind::Or {
            children: nested_children,
        } = &program.node(nested).kind
        else {
            panic!("And child is not nested Or")
        };
        assert_eq!(nested_children.len(), 2);
        assert_eq!(program.node(nested).support_span, 10);
        assert_eq!(program.node(nested).execution_span, 16);
        assert_eq!(program.node(guarded).support_span, 22);
        assert_eq!(program.node(guarded).execution_span, 28);
        assert_eq!(program.node(outer).support_span, 30);
        assert_eq!(program.node(outer).execution_span, 62);

        let mut relevant = ChildSet::empty(plan.len());
        relevant.insert(0);
        let root_plan = program.start(0, 0, UnionVerb::Propose { relevant });
        let and_plan = program.select_child(&root_plan, 0);
        let nested_plan = program.select_child(&and_plan, 1);
        assert_eq!(nested_plan.returns.len(), 2);
        assert_eq!(
            nested_plan.returns[0],
            FormulaReturnSite {
                kind: FormulaReturnKind::Child,
                parent: outer,
                parent_stage: FormulaStage::Propose,
                child: 0,
                done: ChildSet::empty(outer_children.len()),
            }
        );
        assert_eq!(
            nested_plan.returns[1],
            FormulaReturnSite {
                kind: FormulaReturnKind::Child,
                parent: guarded,
                parent_stage: FormulaStage::Propose,
                child: 1,
                done: ChildSet::empty(and_children.len()),
            }
        );
        assert_eq!(
            nested_plan.focus,
            FormulaFocus::Plan {
                node: nested,
                stage: FormulaStage::Propose,
                done: ChildSet::empty(nested_children.len()),
            }
        );
    }

    #[test]
    fn formula_payload_normalizes_a_local_or_before_returning_to_and() {
        let mut batch = FormulaBatch {
            activations: vec![ActivationId(0)],
            parents: RowBatch {
                rows: Vec::new(),
                row_count: 1,
            },
            frames: vec![
                FormulaPayloadFrame::And {
                    current: CandidatePayload::Values(Vec::new()),
                },
                FormulaPayloadFrame::Or {
                    source: CandidatePayload::Values(vec![raw(9)]),
                    output: CandidatePayload::Values(Vec::new()),
                },
            ],
        };

        batch.apply_action_result(
            FormulaStage::Propose,
            CandidatePayload::Values(vec![raw(2), raw(1), raw(2)]),
        );
        batch.return_frame();
        assert_eq!(batch.frames.len(), 1);
        let FormulaPayloadFrame::And { current } = &batch.frames[0] else {
            panic!("local OR returned into the wrong parent-frame shape")
        };
        assert!(current.is_values());
        assert_eq!(current.one_parent_values(), [raw(1), raw(2)]);
    }

    #[test]
    fn formula_or_deduplicates_within_an_affine_parent_but_not_across_parents() {
        let candidate = raw(7);
        let result = FormulaPayloadFrame::Or {
            source: candidate_payload(2, Vec::new()),
            output: candidate_payload(
                2,
                vec![
                    (0, candidate),
                    (0, candidate),
                    (1, candidate),
                    (1, candidate),
                ],
            ),
        }
        .result(2);

        assert_eq!(result, [(0, candidate), (1, candidate)]);
        assert!(!result.is_values());
    }

    #[test]
    fn one_parent_ordinary_and_formula_actions_receive_plain_value_sinks() {
        for lowering in [
            ResidualLowering::CONSERVATIVE,
            ResidualLowering::new(FormulaScope::WholeRoot, false),
        ] {
            let log = Arc::new(Mutex::new(Vec::new()));
            let root = IntersectionConstraint::new(vec![
                Box::new(SinkShapeLeaf {
                    variable: 0,
                    estimate: 1,
                    log: Arc::clone(&log),
                }) as ShapeConstraint,
                Box::new(SinkShapeLeaf {
                    variable: 0,
                    estimate: 2,
                    log: Arc::clone(&log),
                }) as ShapeConstraint,
            ]);

            let results: Vec<_> = Query::new(root, |binding: &Binding| binding.get(0).copied())
                .solve_residual_state_lazy_with(lowering)
                .collect();

            assert_eq!(results, [raw(42)]);
            assert_eq!(
                *log.lock().unwrap(),
                [(ActionVerb::Propose, true), (ActionVerb::Confirm, true)],
                "one-parent actions must stay tagless under {lowering:?}"
            );
        }
    }

    #[test]
    fn formula_and_payload_requires_only_parent_grouping() {
        let initial = vec![(0, raw(9)), (0, raw(1)), (1, raw(8)), (1, raw(2))];
        let mut batch = FormulaBatch::from_confirmation(
            CandidateBatch {
                parents: RowBatch {
                    rows: vec![raw(20), raw(21)],
                    row_count: 2,
                },
                candidates: candidate_payload(2, initial.clone()),
            },
            vec![ActivationId(0), ActivationId(1)],
            &FiniteFormulaNodeKind::Atom,
        );

        assert_eq!(batch.input(), initial.as_slice());

        let confirmed = vec![(0, raw(7)), (0, raw(3)), (1, raw(6)), (1, raw(4))];
        batch.apply_action_result(
            FormulaStage::Confirm,
            candidate_payload(2, confirmed.clone()),
        );
        assert_eq!(batch.input(), confirmed.as_slice());
    }

    #[test]
    fn formula_uniform_partition_reuses_the_complete_payload() {
        let batch = FormulaBatch {
            activations: vec![ActivationId(10), ActivationId(11), ActivationId(12)],
            parents: RowBatch {
                rows: [1, 2, 3, 4, 5, 6].map(raw).into_iter().collect(),
                row_count: 3,
            },
            frames: vec![
                FormulaPayloadFrame::Or {
                    source: candidate_payload(3, vec![(0, raw(20)), (2, raw(21))]),
                    output: candidate_payload(3, vec![(1, raw(22))]),
                },
                FormulaPayloadFrame::And {
                    current: candidate_payload(3, vec![(0, raw(30)), (1, raw(31)), (2, raw(32))]),
                },
            ],
        };
        let mut groups = batch.partition(2, &[7u8, 7, 7]);
        assert_eq!(groups.len(), 1);
        let group = groups.remove(&7).expect("uniform assignment has one key");
        assert_eq!(
            group.activations,
            [ActivationId(10), ActivationId(11), ActivationId(12)]
        );
        assert_eq!(group.parents.rows, [1, 2, 3, 4, 5, 6].map(raw));
        assert_eq!(group.parents.row_count, 3);
        let [FormulaPayloadFrame::Or { source, output }, FormulaPayloadFrame::And { current }] =
            group.frames.as_slice()
        else {
            panic!("uniform partition changed formula frame shapes")
        };
        assert_eq!(source, &vec![(0, raw(20)), (2, raw(21))]);
        assert_eq!(output, &vec![(1, raw(22))]);
        assert_eq!(current, &vec![(0, raw(30)), (1, raw(31)), (2, raw(32))]);

        let empty = FormulaBatch {
            activations: Vec::new(),
            parents: RowBatch {
                rows: Vec::new(),
                row_count: 0,
            },
            frames: vec![FormulaPayloadFrame::And {
                current: CandidatePayload::Tagged(Vec::new()),
            }],
        };
        assert!(empty.partition::<u8>(0, &[]).is_empty());
    }

    #[test]
    fn finite_formula_and_stage_is_not_inferred_from_its_done_mask() {
        let guarded =
            IntersectionConstraint::new(vec![shape_leaf(0), shape_leaf(0), shape_leaf(0)]);
        let union = UnionConstraint::new(vec![Box::new(guarded) as ShapeConstraint, shape_leaf(0)]);
        let root =
            IntersectionConstraint::new(vec![shape_leaf(0), Box::new(union) as ShapeConstraint]);
        let plan = ResidualPlan::compile_finite_unions(&root);
        let program = &plan.finite_formula;

        let mut propose_relevant = ChildSet::empty(plan.len());
        propose_relevant.insert(1);
        let outer_propose = program.start(
            0,
            1,
            UnionVerb::Propose {
                relevant: propose_relevant,
            },
        );
        let and_needs_proposer = program.select_child(&outer_propose, 0);
        let skipped_dead_child = program.skip_child(&and_needs_proposer, 0);
        let FormulaFocus::Plan { stage, done, .. } = &skipped_dead_child.focus else {
            panic!("guarded arm did not enter an AND plan")
        };
        assert_eq!(*stage, FormulaStage::Propose);
        assert_eq!(done.count(), 1);
        assert!(program.grade(&skipped_dead_child) > program.grade(&and_needs_proposer));

        // Only returning from an actually selected proposer changes the AND
        // into its confirmation suffix. Dead/irrelevant progress does not.
        let proposer = program.select_child(&skipped_dead_child, 1);
        assert!(matches!(
            proposer.focus,
            FormulaFocus::Action {
                stage: FormulaStage::Propose,
                ..
            }
        ));
        let proposer_done = program.complete(&proposer);
        let FormulaSuccessor::Formula(filtering_suffix) = program.resume(&proposer_done) else {
            panic!("AND proposer returned past its parent")
        };
        let FormulaFocus::Plan { stage, done, .. } = filtering_suffix.focus else {
            panic!("AND proposer did not resume its parent plan")
        };
        assert_eq!(stage, FormulaStage::Confirm);
        assert_eq!(done.count(), 2);

        // The identical AND occurrence entered as a parent confirmer starts
        // in Confirm even though no child has yet been checked.
        let mut confirm_relevant = ChildSet::empty(plan.len());
        confirm_relevant.insert(0);
        confirm_relevant.insert(1);
        let mut checked = ChildSet::empty(plan.len());
        checked.insert(0);
        let outer_confirm = program.start(
            0,
            1,
            UnionVerb::Confirm {
                relevant: confirm_relevant,
                checked,
            },
        );
        let and_is_confirmer = program.select_child(&outer_confirm, 0);
        let FormulaFocus::Plan { stage, done, .. } = &and_is_confirmer.focus else {
            panic!("confirming guarded arm did not enter an AND plan")
        };
        assert_eq!(*stage, FormulaStage::Confirm);
        assert_eq!(done.count(), 0);
        assert_ne!(and_needs_proposer, and_is_confirmer);
    }

    #[test]
    fn finite_formula_and_child_orders_return_to_one_exact_parent_counter() {
        let guarded = IntersectionConstraint::new(vec![shape_leaf(0), shape_leaf(0)]);
        let root = UnionConstraint::new(vec![Box::new(guarded) as ShapeConstraint]);
        let plan = ResidualPlan::compile_finite_unions(&root);
        let program = &plan.finite_formula;
        let mut relevant = ChildSet::empty(plan.len());
        relevant.insert(0);
        let outer = program.start(0, 0, UnionVerb::Propose { relevant });
        let and_plan = program.select_child(&outer, 0);

        let run = |first, second| {
            let first_action = program.select_child_as_action(&and_plan, first);
            let first_complete = program.complete(&first_action);
            let FormulaSuccessor::Formula(and_confirm) = program.resume(&first_complete) else {
                panic!("AND proposer returned past its own frame")
            };
            let second_action = program.select_child_as_action(&and_confirm, second);
            let second_complete = program.complete(&second_action);
            let FormulaSuccessor::Formula(and_done) = program.resume(&second_complete) else {
                panic!("AND confirmer returned past its own frame")
            };
            let and_complete = program.complete(&and_done);
            let FormulaSuccessor::Formula(outer_done) = program.resume(&and_complete) else {
                panic!("AND frame returned past its enclosing OR")
            };
            outer_done
        };

        assert_eq!(
            run(0, 1),
            run(1, 0),
            "the done set and parent return site must erase AND child history"
        );
    }

    #[test]
    fn recursive_formula_orders_return_to_one_exact_pc_at_every_zipper_depth() {
        let inner = UnionConstraint::new(vec![shape_leaf(0), shape_leaf(0)]);
        let guarded =
            IntersectionConstraint::new(vec![Box::new(inner) as ShapeConstraint, shape_leaf(0)]);
        let root = UnionConstraint::new(vec![Box::new(guarded) as ShapeConstraint]);
        let plan = ResidualPlan::compile_finite_unions(&root);
        let program = &plan.finite_formula;
        let mut relevant = ChildSet::empty(plan.len());
        relevant.insert(0);
        let outer = program.start(0, 0, UnionVerb::Propose { relevant });

        let assert_advance = |before: &FormulaProgramCounter, after: &FormulaProgramCounter| {
            assert!(
                program.grade(after) > program.grade(before),
                "recursive formula grade did not advance: {before:?} -> {after:?}"
            );
        };
        let finish_atom = |parent: &FormulaProgramCounter, child| {
            let action = program.select_child_as_action(parent, child);
            assert_advance(parent, &action);
            let complete = program.complete(&action);
            assert_advance(&action, &complete);
            let FormulaSuccessor::Formula(next) = program.resume(&complete) else {
                panic!("nested Atom returned past the formula root")
            };
            assert_advance(&complete, &next);
            next
        };
        let finish_inner = |and: &FormulaProgramCounter, order: [usize; 2]| {
            let mut inner = program.select_child(and, 0);
            assert_advance(and, &inner);
            inner = finish_atom(&inner, order[0]);
            inner = finish_atom(&inner, order[1]);
            let complete = program.complete(&inner);
            assert_advance(&inner, &complete);
            let FormulaSuccessor::Formula(and) = program.resume(&complete) else {
                panic!("inner OR returned past its enclosing AND")
            };
            assert_advance(&complete, &and);
            and
        };
        let run = |nested_first: bool, inner_order: [usize; 2]| {
            let mut and = program.select_child(&outer, 0);
            assert_advance(&outer, &and);
            if nested_first {
                and = finish_inner(&and, inner_order);
                and = finish_atom(&and, 1);
            } else {
                and = finish_atom(&and, 1);
                and = finish_inner(&and, inner_order);
            }
            let complete = program.complete(&and);
            assert_advance(&and, &complete);
            let FormulaSuccessor::Formula(outer_done) = program.resume(&complete) else {
                panic!("nested AND returned past its enclosing OR")
            };
            assert_advance(&complete, &outer_done);
            outer_done
        };

        let canonical = run(true, [0, 1]);
        for equivalent in [run(true, [1, 0]), run(false, [0, 1]), run(false, [1, 0])] {
            assert_eq!(equivalent, canonical);
            assert_eq!(program.grade(&equivalent), program.grade(&canonical));
        }
    }

    #[test]
    fn finite_formula_repeated_arcs_keep_distinct_node_and_resume_identity() {
        let union = Arc::new(UnionConstraint::new(vec![ShapeLeaf(0), ShapeLeaf(0)]));
        let root = IntersectionConstraint::new(vec![
            Box::new(Arc::clone(&union)) as ShapeConstraint,
            Box::new(union) as ShapeConstraint,
        ]);
        let plan = ResidualPlan::compile_finite_unions(&root);
        let program = &plan.finite_formula;
        let left = program.root(0).unwrap();
        let right = program.root(1).unwrap();
        assert_ne!(left, right);
        assert_eq!(program.nodes.len(), 6);
        let FiniteFormulaNodeKind::Or {
            children: left_children,
        } = &program.node(left).kind
        else {
            panic!("left repeated occurrence is not Or")
        };
        let FiniteFormulaNodeKind::Or {
            children: right_children,
        } = &program.node(right).kind
        else {
            panic!("right repeated occurrence is not Or")
        };
        assert_eq!(left_children.len(), right_children.len());
        assert!(left_children
            .iter()
            .all(|child| !right_children.contains(child)));
        assert_eq!(
            program.node(left).support_span,
            program.node(right).support_span
        );
        assert_eq!(
            program.node(left).execution_span,
            program.node(right).execution_span
        );

        let mut left_relevant = ChildSet::empty(plan.len());
        left_relevant.insert(0);
        let mut right_relevant = ChildSet::empty(plan.len());
        right_relevant.insert(1);
        let left_counter = program.start(
            0,
            0,
            UnionVerb::Propose {
                relevant: left_relevant,
            },
        );
        let right_counter = program.start(
            0,
            1,
            UnionVerb::Propose {
                relevant: right_relevant,
            },
        );
        assert_ne!(left_counter, right_counter);
    }

    #[test]
    fn finite_formula_compiler_grades_every_adaptive_transition_strictly() {
        let nested = UnionConstraint::new(vec![shape_leaf(0), shape_leaf(0)]);
        let guarded = IntersectionConstraint::new(vec![
            shape_leaf(0),
            Box::new(nested) as ShapeConstraint,
            shape_leaf(0),
        ]);
        let root = UnionConstraint::new(vec![Box::new(guarded) as ShapeConstraint, shape_leaf(0)]);
        let plan = ResidualPlan::compile_finite_unions(&root);
        let program = &plan.finite_formula;
        let mut relevant = ChildSet::empty(plan.len());
        relevant.insert(0);
        let verb = UnionVerb::Propose { relevant };
        let mut counter = program.start(0, 0, verb.clone());
        let mut formula_pcs = FormulaPcInterner::default();
        let mut compact = formula_pcs.start(program, 0, 0, verb);
        let mut transitions = 0usize;

        loop {
            let grade = program.grade(&counter);
            assert_eq!(formula_pcs.materialize(compact), counter);
            assert_eq!(formula_pcs.grade(compact), grade);
            let (successor, compact_successor) = match &counter.focus {
                FormulaFocus::Action { .. } => (
                    FormulaSuccessor::Formula(program.complete(&counter)),
                    Ok(InternedFormulaSuccessor::Formula(
                        formula_pcs.complete(program, compact),
                    )),
                ),
                FormulaFocus::Plan { node, done, .. } => {
                    let children = program.node(*node).children().unwrap();
                    if done.count() == children.len() {
                        (
                            FormulaSuccessor::Formula(program.complete(&counter)),
                            Ok(InternedFormulaSuccessor::Formula(
                                formula_pcs.complete(program, compact),
                            )),
                        )
                    } else {
                        let child = (0..children.len())
                            .rev()
                            .find(|&child| !done.contains(child))
                            .unwrap();
                        (
                            FormulaSuccessor::Formula(program.select_child(&counter, child)),
                            Ok(InternedFormulaSuccessor::Formula(
                                formula_pcs.select_child(program, compact, child),
                            )),
                        )
                    }
                }
                FormulaFocus::Complete { .. } => (
                    program.resume(&counter),
                    formula_pcs.resume_completed(program, compact),
                ),
            };
            transitions += 1;
            assert!(transitions < 64, "finite formula control did not terminate");
            match (successor, compact_successor) {
                (
                    FormulaSuccessor::Formula(next),
                    Ok(InternedFormulaSuccessor::Formula(next_compact)),
                ) => {
                    let next_grade = program.grade(&next);
                    assert!(
                        next_grade > grade,
                        "formula grade regressed from {grade} to {next_grade}: {counter:?} -> {next:?}"
                    );
                    counter = next;
                    compact = next_compact;
                }
                (FormulaSuccessor::Guard { .. }, _)
                | (_, Ok(InternedFormulaSuccessor::Guard { .. })) => {
                    panic!("ordinary compiler walk unexpectedly entered a guard")
                }
                (FormulaSuccessor::Outer(resume), Err(compact_resume)) => {
                    assert_eq!(resume, counter.resume);
                    assert_eq!(compact_resume, resume);
                    assert!(matches!(counter.focus, FormulaFocus::Complete { .. }));
                    break;
                }
                pair => panic!("structural and compact formula successors diverged: {pair:?}"),
            }
        }
        assert!(
            transitions > 10,
            "alternating formula trace was too shallow"
        );
    }

    #[test]
    fn finite_formula_ranks_fit_strictly_between_outer_protocol_grades() {
        let union = UnionConstraint::new(vec![shape_leaf(0), shape_leaf(0)]);
        let root =
            IntersectionConstraint::new(vec![shape_leaf(0), Box::new(union) as ShapeConstraint]);
        let plan = ResidualPlan::compile_finite_unions(&root);
        let program = &plan.finite_formula;
        let action_span = plan.action_span();
        assert_eq!(action_span, 18);

        let mut relevant = ChildSet::empty(plan.len());
        relevant.insert(0);
        relevant.insert(1);
        let rank = |formula_pcs: &FormulaPcInterner, phase| {
            StateDesc {
                bound: VariableSet::new_empty(),
                phase,
            }
            .rank_with_span(plan.len(), action_span, Some(program), formula_pcs)
        };

        let mut formula_pcs = FormulaPcInterner::default();

        let outer_propose = rank(
            &formula_pcs,
            ResidualPhase::Propose {
                variable: 0,
                relevant: relevant.clone(),
                proposer: 1,
            },
        );
        let start = formula_pcs.start(
            program,
            0,
            1,
            UnionVerb::Propose {
                relevant: relevant.clone(),
            },
        );
        let action = formula_pcs.select_child_as_action(program, start, 0);
        let child_complete = formula_pcs.complete(program, action);
        let Ok(InternedFormulaSuccessor::Formula(next_plan)) =
            formula_pcs.resume_completed(program, child_complete)
        else {
            panic!("first OR child returned past its root")
        };
        let second_action = formula_pcs.select_child_as_action(program, next_plan, 1);
        let second_complete = formula_pcs.complete(program, second_action);
        let Ok(InternedFormulaSuccessor::Formula(full_plan)) =
            formula_pcs.resume_completed(program, second_complete)
        else {
            panic!("second OR child returned past its root Plan")
        };
        let root_complete = formula_pcs.complete(program, full_plan);

        let formula_ranks = [
            start,
            action,
            child_complete,
            next_plan,
            second_action,
            second_complete,
            full_plan,
            root_complete,
        ]
        .map(|counter| rank(&formula_pcs, ResidualPhase::Formula { counter }));
        assert!(formula_ranks[0] > outer_propose);
        assert!(formula_ranks.windows(2).all(|pair| pair[0] < pair[1]));

        let proposal_candidate = rank(
            &formula_pcs,
            ResidualPhase::Candidate {
                variable: 0,
                relevant: relevant.clone(),
                checked: ChildSet::empty(plan.len()).with_inserted(1),
            },
        );
        assert_eq!(proposal_candidate, action_span);
        assert!(formula_ranks.last().unwrap() < &proposal_candidate);

        let checked = ChildSet::empty(plan.len()).with_inserted(0);
        let outer_confirm = rank(
            &formula_pcs,
            ResidualPhase::Confirm {
                variable: 0,
                relevant: relevant.clone(),
                checked: checked.clone(),
                confirmer: 1,
            },
        );
        let confirm_start =
            formula_pcs.start(program, 0, 1, UnionVerb::Confirm { relevant, checked });
        assert_eq!(outer_confirm, action_span + 1);
        assert_eq!(
            rank(
                &formula_pcs,
                ResidualPhase::Formula {
                    counter: confirm_start,
                }
            ),
            action_span + 2
        );
    }

    #[test]
    fn legacy_selector_requires_overlapping_actionable_exposed_leaves() {
        assert!(!useful_default_shape(&ShapeLeaf(0)));
        assert!(!useful_default_shape(&IntersectionConstraint::new(Vec::<
            ShapeConstraint,
        >::new(
        ))));
        assert!(!useful_default_shape(&IntersectionConstraint::new(vec![
            shape_leaf(0)
        ])));

        for truth in [true, false] {
            let constant = Box::new(ZeroVariableTruth(truth)) as ShapeConstraint;
            let one_actionable = IntersectionConstraint::new(vec![constant, shape_leaf(0)]);
            assert!(
                !useful_default_shape(&one_actionable),
                "a {truth} constant leaf must not make one actionable leaf residual-worthy"
            );
        }

        assert!(
            !useful_default_shape(&IntersectionConstraint::new(vec![
                shape_leaf(0),
                shape_leaf(1),
            ])),
            "disjoint leaves have no shared-variable residual action"
        );
        assert!(useful_default_shape(&IntersectionConstraint::new(vec![
            shape_leaf(0),
            shape_leaf(0),
        ])));
        assert!(useful_default_shape(&IntersectionConstraint::new(vec![
            Box::new(ZeroVariableTruth(true)) as ShapeConstraint,
            shape_leaf(0),
            shape_and(vec![shape_leaf(1), shape_and(vec![shape_leaf(0)])]),
        ])));
        assert!(
            !useful_default_shape(&IntersectionConstraint::new(vec![
                shape_leaf(0),
                shape_and(vec![shape_leaf(1), shape_and(vec![shape_leaf(2)])]),
            ])),
            "nested ANDs flatten, but disjoint variable sets remain a DAG case"
        );
        let boxed_and: Box<dyn Constraint<'static> + Send + Sync> =
            Box::new(IntersectionConstraint::new(vec![
                shape_leaf(3),
                shape_leaf(3),
            ]));
        assert!(useful_default_shape(boxed_and.as_ref()));
        let arc_and: Arc<dyn Constraint<'static> + Send + Sync> =
            Arc::new(IntersectionConstraint::new(vec![
                ShapeLeaf(4),
                ShapeLeaf(4),
            ]));
        assert!(useful_default_shape(arc_and.as_ref()));

        // Union stays one opaque leaf: equal variables inside its variants do
        // not look like two residual occurrences. A separate sibling that
        // shares the variable does create an overlap at the opaque boundary.
        let opaque_union = UnionConstraint::new(vec![shape_leaf(0), shape_leaf(0)]);
        assert!(!useful_default_shape(&opaque_union));
        assert!(!useful_default_shape(&IntersectionConstraint::new(vec![
            Box::new(opaque_union) as ShapeConstraint,
            shape_leaf(1),
        ])));
        let opaque_union = UnionConstraint::new(vec![shape_leaf(0), shape_leaf(0)]);
        assert!(useful_default_shape(&IntersectionConstraint::new(vec![
            Box::new(opaque_union) as ShapeConstraint,
            shape_leaf(0),
        ])));

        // An RPQ is likewise one opaque two-variable leaf. Its internal state
        // machine is never flattened; only overlap with another AND sibling
        // is visible to the selector.
        use crate::inline::encodings::genid::GenId;
        use crate::query::regularpathconstraint::{PathOp, RegularPathConstraint};
        use crate::trible::TribleSet;
        let mut context = VariableContext::new();
        let start = context.next_variable::<GenId>();
        let end = context.next_variable::<GenId>();
        let rpq = RegularPathConstraint::new(
            TribleSet::new(),
            start,
            end,
            &[PathOp::Attr([0; crate::id::ID_LEN])],
        );
        assert!(!useful_default_shape(&rpq));
        assert!(!useful_default_shape(&IntersectionConstraint::new(vec![
            Box::new(rpq) as ShapeConstraint,
            shape_leaf(2),
        ])));
        let rpq = RegularPathConstraint::new(
            TribleSet::new(),
            start,
            end,
            &[PathOp::Attr([0; crate::id::ID_LEN])],
        );
        assert!(useful_default_shape(&IntersectionConstraint::new(vec![
            Box::new(rpq) as ShapeConstraint,
            shape_leaf(end.index),
        ])));
    }

    #[test]
    fn full_switch_routes_every_live_legacy_fallback_shape_to_residual() {
        fn assert_residual<C>(root: C)
        where
            C: Constraint<'static> + 'static,
        {
            let mut query = Query::new(root, |_| Some(()));
            assert_eq!(query.scheduler, QueryScheduler::ResidualState);
            let _ = query.next();
            assert!(query.residual.is_some());
            assert!(query.dag.is_none());
        }

        assert_residual(ShapeLeaf(0));
        assert_residual(IntersectionConstraint::new(vec![shape_leaf(0)]));
        assert_residual(IntersectionConstraint::new(vec![
            shape_leaf(0),
            shape_leaf(1),
        ]));
        assert_residual(UnionConstraint::new(vec![ShapeLeaf(0), ShapeLeaf(0)]));

        use crate::inline::encodings::genid::GenId;
        use crate::query::regularpathconstraint::{PathOp, RegularPathConstraint};
        use crate::trible::TribleSet;
        let mut context = VariableContext::new();
        let start = context.next_variable::<GenId>();
        let end = context.next_variable::<GenId>();
        assert_residual(RegularPathConstraint::new(
            TribleSet::new(),
            start,
            end,
            &[PathOp::Attr(crate::id::rngid().raw()), PathOp::Plus],
        ));

        let mut true_constant = Query::new(ZeroVariableTruth(true), |_| Some(()));
        assert_eq!(true_constant.scheduler, QueryScheduler::ResidualState);
        assert_eq!(true_constant.next(), Some(()));
        assert!(true_constant.residual.is_some());

        let mut false_constant = Query::new(ZeroVariableTruth(false), |_| Some(()));
        assert_eq!(false_constant.scheduler, QueryScheduler::LazyDag);
        assert_eq!(false_constant.next(), None);
        assert!(false_constant.residual.is_none());
        assert!(false_constant.dag.is_none());
    }

    #[test]
    fn full_switch_keeps_constant_edges_exact() {
        let false_root =
            IntersectionConstraint::new(
                vec![Box::new(ZeroVariableTruth(false)) as ShapeConstraint],
            );
        let mut false_query = Query::new(false_root, |_| Some(()));
        assert_eq!(false_query.scheduler, QueryScheduler::LazyDag);
        assert_eq!(false_query.next(), None);
        assert!(false_query.residual.is_none());
        assert!(false_query.dag.is_none());

        let values = Arc::new(vec![raw(3), raw(7), raw(11)]);
        let make_true_and_one_real = || {
            IntersectionConstraint::new(vec![
                Box::new(ZeroVariableTruth(true)) as ShapeConstraint,
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::clone(&values),
                }) as ShapeConstraint,
            ])
        };
        let project = |binding: &Binding| binding.get(0).copied();
        let mut ordinary = Query::new(make_true_and_one_real(), project);
        assert_eq!(ordinary.scheduler, QueryScheduler::ResidualState);
        let mut ordinary_bag: Vec<_> = ordinary.by_ref().collect();
        assert!(ordinary.residual.is_some());
        let mut explicit_dag = Query::new(make_true_and_one_real(), project).lazy_dag_scheduler();
        assert_eq!(explicit_dag.scheduler, QueryScheduler::LazyDag);
        let mut explicit_dag_bag: Vec<_> = explicit_dag.by_ref().collect();
        assert!(explicit_dag.dag.is_some());
        let mut expected_bag = values.as_ref().clone();
        ordinary_bag.sort_unstable();
        explicit_dag_bag.sort_unstable();
        expected_bag.sort_unstable();
        assert_eq!(ordinary_bag, expected_bag);
        assert_eq!(ordinary_bag, explicit_dag_bag);

        // A false constant must suppress residual admission even when the
        // remaining exposed shape has an overlapping-variable pair.
        let false_overlapping = IntersectionConstraint::new(vec![
            Box::new(ZeroVariableTruth(false)) as ShapeConstraint,
            Box::new(FanoutLeaf {
                variable: 0,
                values: Arc::clone(&values),
            }) as ShapeConstraint,
            shape_leaf(0),
        ]);
        assert!(useful_default_shape(&false_overlapping));
        let mut false_overlapping = Query::new(false_overlapping, |_| Some(()));
        assert_eq!(false_overlapping.scheduler, QueryScheduler::LazyDag);
        assert_eq!(false_overlapping.next(), None);
        assert!(false_overlapping.residual.is_none());
        assert!(false_overlapping.dag.is_none());
        let debug = format!("{false_overlapping:?}");
        assert!(debug.contains("scheduler: LazyDag"), "{debug}");
        assert!(debug.contains("residual_started: false"), "{debug}");
    }

    #[test]
    fn repeated_objects_keep_distinct_occurrence_paths() {
        let shared: Arc<dyn Constraint<'static> + Send + Sync> = Arc::new(ShapeLeaf(7));
        let root = IntersectionConstraint::new(vec![Arc::clone(&shared), Arc::clone(&shared)]);
        let plan = ResidualPlan::compile(&root);
        assert_eq!(
            plan.leaves,
            vec![
                ConstraintPath(vec![0].into_boxed_slice()),
                ConstraintPath(vec![1].into_boxed_slice())
            ]
        );
        assert_eq!(
            plan.resolve(&root, 0).variables(),
            VariableSet::new_singleton(7)
        );
        assert_eq!(
            plan.resolve(&root, 1).variables(),
            VariableSet::new_singleton(7)
        );
    }

    #[test]
    fn ignored_candidate_and_flattens_but_other_scope_boundaries_stay_opaque() {
        use crate::inline::encodings::genid::GenId;
        use crate::trible::TribleSet;

        let ignored = IgnoreConstraint::new(
            VariableSet::new_singleton(0),
            shape_and(vec![shape_leaf(0), shape_leaf(1)]),
        );
        let path = RegularPathConstraint::new(
            TribleSet::new(),
            Variable::<GenId>::new(2),
            Variable::<GenId>::new(3),
            &[PathOp::Attr([0; 16])],
        );
        let root = IntersectionConstraint::new(vec![
            Box::new(ignored) as ShapeConstraint,
            Box::new(path) as ShapeConstraint,
        ]);
        let plan = ResidualPlan::compile(&root);
        assert_eq!(
            plan.leaves,
            vec![
                ConstraintPath(vec![0, 0].into_boxed_slice()),
                ConstraintPath(vec![0, 1].into_boxed_slice()),
                ConstraintPath(vec![1].into_boxed_slice())
            ],
            "Ignore exposes candidate AND children, while the path stays atomic"
        );

        let union = UnionConstraint::new(vec![
            IntersectionConstraint::new(vec![shape_leaf(4), shape_leaf(5)]),
            IntersectionConstraint::new(vec![shape_leaf(4), shape_leaf(5)]),
        ]);
        let root =
            IntersectionConstraint::new(vec![shape_and(vec![Box::new(union) as ShapeConstraint])]);
        assert_eq!(
            ResidualPlan::compile(&root).leaves,
            vec![ConstraintPath(vec![0, 0].into_boxed_slice())],
            "an AND may contain a union, but lowering must not enter its AND arms"
        );
    }

    #[test]
    fn candidate_tail_chunks_keep_parent_groups_whole_and_remap_tags() {
        let mut original_candidates = vec![(0, raw(10)), (0, raw(10)), (1, raw(11))];
        original_candidates.extend((12..44).map(|byte| (2, raw(byte))));
        let mut prefix = CandidateBatch {
            parents: RowBatch {
                rows: vec![raw(0), raw(1), raw(2)],
                row_count: 3,
            },
            candidates: candidate_payload(3, original_candidates.clone()),
        };

        let tail = prefix.take_tail(1, 2);
        assert_eq!(prefix.parents.rows, [raw(0)]);
        assert_eq!(prefix.parents.row_count, 1);
        assert!(prefix.candidates.is_values());
        assert_eq!(prefix.candidates, [(0, raw(10)), (0, raw(10))]);
        assert_eq!(tail.parents.rows, [raw(1), raw(2)]);
        assert_eq!(tail.parents.row_count, 2);
        let mut expected_tail = vec![(0, raw(11))];
        expected_tail.extend((12..44).map(|byte| (1, raw(byte))));
        assert_eq!(tail.candidates, expected_tail);

        prefix.append(tail);
        assert_eq!(prefix.parents.rows, [raw(0), raw(1), raw(2)]);
        assert_eq!(prefix.parents.row_count, 3);
        assert_eq!(prefix.candidates, original_candidates);
    }

    #[test]
    fn disjoint_candidate_append_promotes_once_and_preserves_occurrence_order() {
        let mut values_values = CandidatePayload::Values(vec![raw(1), raw(1)]);
        values_values.append_disjoint(CandidatePayload::Values(vec![raw(2)]), 1, 1);
        assert_eq!(values_values, [(0, raw(1)), (0, raw(1)), (1, raw(2))]);

        let mut values_tagged = CandidatePayload::Values(vec![raw(3)]);
        values_tagged.append_disjoint(
            candidate_payload(2, vec![(0, raw(4)), (1, raw(5)), (1, raw(5))]),
            1,
            2,
        );
        assert_eq!(
            values_tagged,
            [(0, raw(3)), (1, raw(4)), (2, raw(5)), (2, raw(5))]
        );

        let mut tagged_values = candidate_payload(2, vec![(0, raw(6)), (1, raw(7)), (1, raw(7))]);
        tagged_values.append_disjoint(CandidatePayload::Values(vec![raw(8)]), 2, 1);
        assert_eq!(
            tagged_values,
            [(0, raw(6)), (1, raw(7)), (1, raw(7)), (2, raw(8))]
        );

        let mut tagged_tagged = candidate_payload(2, vec![(0, raw(9)), (1, raw(10))]);
        tagged_tagged.append_disjoint(candidate_payload(2, vec![(0, raw(11)), (1, raw(12))]), 2, 2);
        assert_eq!(
            tagged_tagged,
            [(0, raw(9)), (1, raw(10)), (2, raw(11)), (3, raw(12))]
        );
    }

    #[test]
    fn candidate_partition_and_compaction_demote_single_parent_groups() {
        let mut empty_shell = CandidatePayload::Values(Vec::new());
        empty_shell.normalize_for(0);
        assert!(!empty_shell.is_values());

        let batch = CandidateBatch {
            parents: RowBatch {
                rows: vec![raw(20), raw(21)],
                row_count: 2,
            },
            candidates: candidate_payload(2, vec![(0, raw(1)), (1, raw(2)), (1, raw(2))]),
        };
        let groups = batch.partition(1, &[0u8, 1u8]);
        assert_eq!(groups.len(), 2);
        for group in groups.values() {
            assert_eq!(group.parents.row_count, 1);
            assert!(group.candidates.is_values());
        }

        let compacted = CandidateBatch {
            parents: RowBatch {
                rows: vec![raw(30), raw(31), raw(32)],
                row_count: 3,
            },
            candidates: candidate_payload(3, vec![(1, raw(4)), (1, raw(4))]),
        }
        .compact(1)
        .expect("one parent survives");
        assert_eq!(compacted.parents.rows, [raw(31)]);
        assert!(compacted.candidates.is_values());
        assert_eq!(compacted.candidates.one_parent_values(), [raw(4), raw(4)]);
    }

    #[test]
    fn candidate_pages_may_bisect_one_parent_without_losing_occurrences() {
        fn expanded(batch: &CandidateBatch) -> Vec<(RawInline, RawInline)> {
            batch
                .candidates
                .iter()
                .map(|(parent, candidate)| (batch.parents.rows[parent as usize], *candidate))
                .collect()
        }

        let original = CandidateBatch {
            parents: RowBatch {
                rows: vec![raw(20), raw(21)],
                row_count: 2,
            },
            candidates: candidate_payload(
                2,
                vec![
                    (0, raw(1)),
                    (0, raw(1)),
                    (0, raw(2)),
                    (0, raw(3)),
                    (1, raw(4)),
                    (1, raw(5)),
                ],
            ),
        };
        let expected = expanded(&original);
        let mut prefix = original;
        let page = prefix.take_candidate_tail(1, 3);

        assert_eq!(prefix.parents.rows, [raw(20)]);
        assert!(prefix.candidates.is_values());
        assert_eq!(prefix.candidates, [(0, raw(1)), (0, raw(1)), (0, raw(2))]);
        assert_eq!(page.parents.rows, [raw(20), raw(21)]);
        assert_eq!(page.candidates, [(0, raw(3)), (1, raw(4)), (1, raw(5))]);

        let mut actual = expanded(&prefix);
        actual.extend(expanded(&page));
        assert_eq!(
            actual, expected,
            "every duplicate occurrence belongs to one page"
        );
    }

    #[test]
    fn candidate_page_split_and_remerge_preserves_randomized_affine_multiplicity() {
        fn next(seed: &mut u64) -> usize {
            *seed = seed
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (*seed >> 32) as usize
        }

        fn expanded(batch: &CandidateBatch, stride: usize) -> Vec<(Vec<RawInline>, RawInline)> {
            batch
                .candidates
                .iter()
                .map(|(parent, candidate)| {
                    let parent = parent as usize;
                    let start = parent * stride;
                    (
                        batch.parents.rows[start..start + stride].to_vec(),
                        *candidate,
                    )
                })
                .collect()
        }

        fn assert_dense(batch: &CandidateBatch) {
            assert!(!batch.candidates.is_empty());
            assert!(batch
                .candidates
                .iter()
                .all(|(row, _)| (row as usize) < batch.parents.row_count));
            let snapshot = batch.candidates.tagged_snapshot();
            assert!(snapshot.windows(2).all(|pair| pair[0].0 <= pair[1].0));
            let mut seen = vec![false; batch.parents.row_count];
            for (row, _) in snapshot {
                seen[row as usize] = true;
            }
            assert!(seen.into_iter().all(|live| live));
        }

        let mut seed = 0xC0FF_EE12_3456_789Au64;
        for stride in [0, 1, 3] {
            for case in 0..128usize {
                let parent_count = 1 + next(&mut seed) % 7;
                let mut parent_rows = Vec::with_capacity(parent_count * stride);
                let mut candidates = Vec::new();
                for parent in 0..parent_count {
                    for column in 0..stride {
                        let mut value = raw(parent as u8);
                        value[1] = column as u8;
                        value[2] = case as u8;
                        parent_rows.push(value);
                    }
                    let candidate_count = 1 + next(&mut seed) % 7;
                    for occurrence in 0..candidate_count {
                        let mut value = raw(parent as u8);
                        value[1] = occurrence as u8;
                        value[2] = case as u8;
                        candidates.push((parent as u32, value));
                    }
                }

                let original = CandidateBatch {
                    parents: RowBatch {
                        rows: parent_rows,
                        row_count: parent_count,
                    },
                    candidates: candidate_payload(parent_count, candidates),
                };
                let mut expected = expanded(&original, stride);
                let mut remainder = original;
                let mut pages = Vec::new();
                while !remainder.candidates.is_empty() {
                    let width = 1 + next(&mut seed) % 9;
                    let page = remainder.take_candidate_tail(stride, width);
                    assert_dense(&page);
                    pages.push(page);
                }
                assert_eq!(remainder.parents.row_count, 0);
                assert!(remainder.parents.rows.is_empty());

                for i in (1..pages.len()).rev() {
                    let j = next(&mut seed) % (i + 1);
                    pages.swap(i, j);
                }
                let expected_parent_occurrences: usize =
                    pages.iter().map(|page| page.parents.row_count).sum();
                let mut merged = pages.pop().expect("the original batch was nonempty");
                for page in pages {
                    merged.append(page);
                }
                assert_dense(&merged);
                assert_eq!(merged.parents.row_count, expected_parent_occurrences);

                let vars: Vec<VariableId> = (0..stride).collect();
                let view = rows_view(&vars, &merged.parents.rows, merged.parents.row_count);
                assert_eq!(view.len(), expected_parent_occurrences);

                let mut actual = expanded(&merged, stride);
                expected.sort_unstable();
                actual.sort_unstable();
                assert_eq!(actual, expected, "stride={stride}, case={case}");
            }
        }
    }

    #[test]
    fn paging_begins_only_after_atomic_remaining_confirms_are_checked() {
        let root = IntersectionConstraint::new(vec![
            CapabilityLeaf {
                variable: 0,
                page_local: false,
            },
            CapabilityLeaf {
                variable: 0,
                page_local: false,
            },
            CapabilityLeaf {
                variable: 0,
                page_local: true,
            },
        ]);
        let plan = ResidualPlan::compile(&root);
        let formula_pcs = FormulaPcInterner::default();
        let mut relevant = ChildSet::empty(plan.len());
        relevant.insert(0);
        relevant.insert(1);
        relevant.insert(2);
        let mut proposer_checked = ChildSet::empty(plan.len());
        proposer_checked.insert(0);
        let before_atomic = StateDesc {
            bound: VariableSet::new_empty(),
            phase: ResidualPhase::Candidate {
                variable: 0,
                relevant: relevant.clone(),
                checked: proposer_checked.clone(),
            },
        };
        assert!(!before_atomic.uses_candidate_pages(&plan, &formula_pcs));

        let after_atomic = StateDesc {
            bound: VariableSet::new_empty(),
            phase: ResidualPhase::Candidate {
                variable: 0,
                relevant,
                checked: proposer_checked.with_inserted(1),
            },
        };
        assert!(after_atomic.uses_candidate_pages(&plan, &formula_pcs));
    }

    #[test]
    fn page_local_candidate_state_uses_candidate_occupancy_and_keeps_remainder_live() {
        let root = IntersectionConstraint::new(vec![
            CapabilityLeaf {
                variable: 0,
                page_local: false,
            },
            CapabilityLeaf {
                variable: 0,
                page_local: true,
            },
        ]);
        let plan = ResidualPlan::compile(&root);
        let formula_pcs = FormulaPcInterner::default();
        let mut relevant = ChildSet::empty(plan.len());
        relevant.insert(0);
        relevant.insert(1);
        let mut checked = ChildSet::empty(plan.len());
        checked.insert(0);
        let desc = StateDesc {
            bound: VariableSet::new_empty(),
            phase: ResidualPhase::Candidate {
                variable: 0,
                relevant,
                checked,
            },
        };
        assert!(desc.uses_candidate_pages(&plan, &formula_pcs));

        let mut machine = ResidualStateMachine::new(root.variables(), plan.len(), Search::Done);
        let token = file(
            &mut machine.worklist,
            &mut machine.interner,
            plan.len(),
            desc.clone(),
            StateBucket::Candidates(CandidateBatch {
                parents: RowBatch::seed(),
                candidates: CandidatePayload::Values((0..8).map(raw).collect()),
            }),
            &mut machine.stats,
        )
        .expect("fixture files one candidate state");

        let task = machine
            .take_next_with_plan(&plan, 2)
            .expect("page-local candidates are live");
        assert_eq!(task.state, token.state);
        assert_eq!(task.desc, desc);
        let StateBucket::Candidates(page) = task.bucket else {
            panic!("candidate state returned row payload")
        };
        assert_eq!(page.parents.row_count, 1);
        assert!(page.candidates.is_values());
        assert_eq!(page.candidates, [(0, raw(6)), (0, raw(7))]);

        let (_, level) = machine
            .worklist
            .first_key_value()
            .expect("candidate remainder stays under the same rank");
        let (&id, remainder) = level.first_key_value().unwrap();
        assert_eq!(machine.interner.get(id), &desc);
        assert_eq!(remainder.occupancy(true), 6);
        assert_eq!(machine.stats.partial_pops, 1);
    }

    #[test]
    fn selected_task_exposes_exact_action_identity_and_batch_geometry_only_for_verbs() {
        const PARENT: VariableId = 0;
        const VARIABLE: VariableId = 1;
        let root = IntersectionConstraint::new(vec![
            CapabilityLeaf {
                variable: VARIABLE,
                page_local: false,
            },
            CapabilityLeaf {
                variable: VARIABLE,
                page_local: true,
            },
        ]);
        let plan = ResidualPlan::compile(&root);
        let interner = StateInterner::default();
        let bound = VariableSet::new_singleton(PARENT);
        let mut relevant = ChildSet::empty(plan.len());
        relevant.insert(0);
        relevant.insert(1);
        let mut checked = ChildSet::empty(plan.len());
        checked.insert(0);

        let propose = SelectedResidualTask {
            state: StateId(41),
            desc: StateDesc {
                bound,
                phase: ResidualPhase::Propose {
                    variable: VARIABLE,
                    relevant: relevant.clone(),
                    proposer: 0,
                },
            },
            bucket: StateBucket::Rows(RowBatch {
                rows: vec![raw(10), raw(11), raw(12)],
                row_count: 3,
            }),
        };
        assert_eq!(
            propose.action_task(&plan, &interner),
            Some(ResidualActionTask {
                state: StateId(41),
                action: ResidualAction::Propose {
                    variable: VARIABLE,
                    leaf: 0,
                },
                bound,
                parent_rows: 3,
                candidate_occurrences: 0,
                action_atoms: 3,
            })
        );

        let candidate_batch = || CandidateBatch {
            parents: RowBatch {
                rows: vec![raw(20), raw(21)],
                row_count: 2,
            },
            candidates: candidate_payload(
                2,
                vec![
                    (0, raw(1)),
                    (0, raw(2)),
                    (0, raw(3)),
                    (1, raw(4)),
                    (1, raw(5)),
                ],
            ),
        };
        let candidate = SelectedResidualTask {
            state: StateId(42),
            desc: StateDesc {
                bound,
                phase: ResidualPhase::Candidate {
                    variable: VARIABLE,
                    relevant: relevant.clone(),
                    checked: checked.clone(),
                },
            },
            bucket: StateBucket::Candidates(candidate_batch()),
        };
        assert!(!candidate.is_action(&interner));
        assert_eq!(candidate.action_task(&plan, &interner), None);

        let confirm = SelectedResidualTask {
            state: StateId(43),
            desc: StateDesc {
                bound,
                phase: ResidualPhase::Confirm {
                    variable: VARIABLE,
                    relevant,
                    checked,
                    confirmer: 1,
                },
            },
            bucket: StateBucket::Candidates(candidate_batch()),
        };
        assert!(confirm.is_action(&interner));
        assert_eq!(
            confirm.action_task(&plan, &interner),
            Some(ResidualActionTask {
                state: StateId(43),
                action: ResidualAction::Confirm {
                    variable: VARIABLE,
                    leaf: 1,
                },
                bound,
                parent_rows: 2,
                candidate_occurrences: 5,
                action_atoms: 5,
            })
        );

        let atomic_plan = ResidualPlan::compile(&IntersectionConstraint::new(vec![
            CapabilityLeaf {
                variable: VARIABLE,
                page_local: false,
            },
            CapabilityLeaf {
                variable: VARIABLE,
                page_local: false,
            },
        ]));
        assert_eq!(
            confirm
                .action_task(&atomic_plan, &interner)
                .expect("the same concrete confirmation remains actionable")
                .action_atoms,
            2,
            "whole-parent confirmations quote parent rows, not occurrences"
        );

        let ready = SelectedResidualTask {
            state: StateId(44),
            desc: StateDesc {
                bound,
                phase: ResidualPhase::Ready,
            },
            bucket: StateBucket::Rows(RowBatch {
                rows: vec![raw(30)],
                row_count: 1,
            }),
        };
        assert!(!ready.is_action(&interner));
        assert_eq!(ready.action_task(&plan, &interner), None);
    }

    fn paged_filter_fixture(
        values: Vec<RawInline>,
        accepted: RawInline,
        first_calls: Arc<Mutex<Vec<usize>>>,
        second_calls: Arc<Mutex<Vec<usize>>>,
    ) -> IntersectionConstraint<ShapeConstraint> {
        let estimate = values.len();
        IntersectionConstraint::new(vec![
            Box::new(FanoutLeaf {
                variable: 0,
                values: Arc::new(values),
            }) as ShapeConstraint,
            Box::new(PageFilterLeaf {
                variable: 0,
                estimate: estimate + 1,
                accepted: None,
                calls: first_calls,
            }) as ShapeConstraint,
            Box::new(PageFilterLeaf {
                variable: 0,
                estimate: estimate + 2,
                accepted: Some(accepted),
                calls: second_calls,
            }) as ShapeConstraint,
        ])
    }

    fn logged_filter_fixture(
        values: Vec<RawInline>,
        accepted: RawInline,
        log: Arc<Mutex<Vec<LoggedAction>>>,
    ) -> IntersectionConstraint<ShapeConstraint> {
        let estimate = values.len();
        IntersectionConstraint::new(vec![
            Box::new(LoggedLeaf {
                variable: 0,
                leaf_occurrence: 0,
                estimate,
                proposed: Arc::new(values),
                accepted: None,
                log: Arc::clone(&log),
            }) as ShapeConstraint,
            Box::new(LoggedLeaf {
                variable: 0,
                leaf_occurrence: 1,
                estimate: estimate + 1,
                proposed: Arc::new(Vec::new()),
                accepted: Some(accepted),
                log,
            }) as ShapeConstraint,
        ])
    }

    #[cfg(feature = "parallel")]
    fn parallel_logged_filter_fixture(
        values: Vec<RawInline>,
        accepted: RawInline,
        log: Arc<Mutex<Vec<LoggedAction>>>,
    ) -> Arc<IntersectionConstraint<ParallelShapeConstraint>> {
        let estimate = values.len();
        Arc::new(IntersectionConstraint::new(vec![
            parallel_shape(LoggedLeaf {
                variable: 0,
                leaf_occurrence: 0,
                estimate,
                proposed: Arc::new(values),
                accepted: None,
                log: Arc::clone(&log),
            }),
            parallel_shape(LoggedLeaf {
                variable: 0,
                leaf_occurrence: 1,
                estimate: estimate + 1,
                proposed: Arc::new(Vec::new()),
                accepted: Some(accepted),
                log,
            }),
        ]))
    }

    fn observation_site(verb: ActionVerb, leaf_occurrence: usize) -> ActionSite {
        ActionSite {
            verb,
            variable: 0,
            leaf_occurrence,
            bound: VariableSet::new_empty(),
        }
    }

    fn observation_geometry(parent_rows: usize, candidate_occurrences: usize) -> ActionGeometry {
        ActionGeometry {
            parent_rows,
            candidate_occurrences,
            action_atoms: parent_rows.max(candidate_occurrences),
        }
    }

    fn executor_measurement(operation: &'static str, started: Duration) -> ExecutorMeasurement {
        ExecutorMeasurement {
            executor: "test-executor",
            operation,
            work_unit: "test-items",
            work_units: 1,
            started,
            wall: Duration::ZERO,
        }
    }

    #[test]
    fn residual_shadow_preserves_bag_stats_and_action_sequence_at_every_width() {
        let values: Vec<_> = (0..16).map(raw).collect();
        let accepted = raw(5);
        let mut saw_dead_confirm = false;
        let mut saw_surviving_confirm = false;

        for width in [1, 3, 16] {
            let direct_log = Arc::new(Mutex::new(Vec::new()));
            let direct = Query::new(
                logged_filter_fixture(values.clone(), accepted, Arc::clone(&direct_log)),
                |binding: &Binding| binding.get(0).copied(),
            )
            .solve_residual_state_lazy()
            .cap(16)
            .start_width(width)
            .collect_profiled();

            let shadow_log = Arc::new(Mutex::new(Vec::new()));
            let epoch = ResidualShadowEpoch::new();
            let shadow = Query::new(
                logged_filter_fixture(values.clone(), accepted, Arc::clone(&shadow_log)),
                |binding: &Binding| binding.get(0).copied(),
            )
            .solve_residual_state_lazy()
            .cap(16)
            .start_width(width)
            .shadow(epoch.clone())
            .collect_profiled();

            let mut direct_results = direct.results;
            let mut shadow_results = shadow.results;
            direct_results.sort_unstable();
            shadow_results.sort_unstable();
            assert_eq!(shadow_results, direct_results);
            assert_eq!(shadow_results, [accepted]);
            assert_eq!(shadow.stats, direct.stats);
            assert_eq!(shadow.shadow.status, ResidualShadowStatus::Closed);
            assert_eq!(epoch.status(), ResidualShadowStatus::Closed);

            let direct_calls = direct_log.lock().unwrap().clone();
            let shadow_calls = shadow_log.lock().unwrap().clone();
            assert_eq!(shadow_calls, direct_calls);
            let observed_calls: Vec<_> = shadow
                .shadow
                .events
                .iter()
                .map(|event| LoggedAction {
                    verb: event.site.verb,
                    leaf_occurrence: event.site.leaf_occurrence,
                    parent_rows: event.geometry.parent_rows,
                    candidate_occurrences: event.geometry.candidate_occurrences,
                })
                .collect();
            assert_eq!(observed_calls, direct_calls);
            assert_eq!(
                shadow.shadow.events.len(),
                shadow.stats.support_action_pops
                    + shadow.stats.propose_action_pops
                    + shadow.stats.confirm_action_pops
            );

            for event in &shadow.shadow.events {
                assert_eq!(event.site.variable, 0);
                assert_eq!(event.site.bound, VariableSet::new_empty());
                assert_eq!(event.executor_samples.len(), 1);
                let sample = event.executor_samples[0];
                assert_eq!(sample.event, event.event);
                assert!(!sample.stale);
                assert!(sample.measurement.started >= event.started);
                assert_eq!(
                    sample.measurement.work_units,
                    match event.site.verb {
                        ActionVerb::Support | ActionVerb::Propose => event.geometry.parent_rows,
                        ActionVerb::Confirm => event.geometry.candidate_occurrences,
                    }
                );
                assert_eq!(
                    event.geometry.action_atoms,
                    match event.site.verb {
                        ActionVerb::Support | ActionVerb::Propose => event.geometry.parent_rows,
                        ActionVerb::Confirm => event.geometry.candidate_occurrences,
                    }
                );
                let completion = event.completion.expect("drained action completed");
                assert!(!completion.stale);
                if event.site.verb == ActionVerb::Confirm {
                    match completion.outcome {
                        ActionOutcome::Dead => saw_dead_confirm = true,
                        ActionOutcome::Advanced(survival) => {
                            saw_surviving_confirm = true;
                            assert_eq!(survival.parent_rows, 1);
                            assert_eq!(survival.candidate_occurrences, 1);
                        }
                        ActionOutcome::Aborted => panic!("drained confirmation aborted"),
                    }
                }
            }
        }

        assert!(saw_dead_confirm);
        assert!(saw_surviving_confirm);
    }

    #[test]
    fn residual_shadow_nested_scopes_restore_and_own_executor_samples() {
        assert!(current_residual_action().is_none());
        let epoch = ResidualShadowEpoch::new();
        let mut outer = epoch.begin(
            observation_site(ActionVerb::Propose, 0),
            observation_geometry(1, 0),
        );
        let outer_correlation = outer.correlation();
        let outer_scope = ShadowActionScope::enter(outer_correlation.clone());
        outer.start();
        assert_eq!(
            current_residual_action().map(|action| action.event()),
            Some(outer_correlation.event())
        );

        let mut inner = epoch.begin(
            observation_site(ActionVerb::Confirm, 1),
            observation_geometry(1, 2),
        );
        let inner_correlation = inner.correlation();
        let inner_scope = ShadowActionScope::enter(inner_correlation.clone());
        inner.start();
        assert_eq!(
            current_residual_action().map(|action| action.event()),
            Some(inner_correlation.event())
        );
        inner_correlation.record_executor_sample(executor_measurement("first", Duration::ZERO));
        inner_correlation.record_executor_sample(executor_measurement("second", Duration::ZERO));
        drop(inner_scope);
        assert_eq!(
            current_residual_action().map(|action| action.event()),
            Some(outer_correlation.event())
        );
        outer_correlation.record_executor_sample(executor_measurement("outer", Duration::ZERO));
        drop(outer_scope);
        assert!(current_residual_action().is_none());

        inner.finish(
            Duration::ZERO,
            ActionOutcome::Advanced(ActionSurvival {
                parent_rows: 1,
                candidate_occurrences: 1,
            }),
        );
        outer.finish(Duration::ZERO, ActionOutcome::Dead);
        assert_eq!(epoch.finish_exhausted(), ResidualShadowStatus::Closed);
        let snapshot = epoch.snapshot();
        assert_eq!(snapshot.events.len(), 2);
        assert_eq!(snapshot.events[0].event, outer_correlation.event());
        assert_eq!(
            snapshot.events[0].executor_samples[0].measurement.operation,
            "outer"
        );
        assert_eq!(snapshot.events[1].event, inner_correlation.event());
        assert_eq!(
            snapshot.events[1]
                .executor_samples
                .iter()
                .map(|sample| sample.measurement.operation)
                .collect::<Vec<_>>(),
            ["first", "second"]
        );
    }

    #[test]
    fn residual_shadow_late_samples_stay_with_their_terminal_epoch() {
        let old_epoch = ResidualShadowEpoch::new();
        let mut old_span = old_epoch.begin(
            observation_site(ActionVerb::Propose, 0),
            observation_geometry(1, 0),
        );
        let old_correlation = old_span.correlation();
        old_span.start();
        assert!(old_epoch.invalidate());

        let new_epoch = ResidualShadowEpoch::new();
        let mut new_span = new_epoch.begin(
            observation_site(ActionVerb::Confirm, 1),
            observation_geometry(1, 1),
        );
        let new_correlation = new_span.correlation();
        new_span.start();
        old_correlation.record_executor_sample(executor_measurement("late-old", Duration::ZERO));
        new_correlation.record_executor_sample(executor_measurement("current-new", Duration::ZERO));
        old_span.finish(Duration::ZERO, ActionOutcome::Dead);
        new_span.finish(Duration::ZERO, ActionOutcome::Dead);
        assert_eq!(new_epoch.finish_exhausted(), ResidualShadowStatus::Closed);

        let old = old_epoch.snapshot();
        let new = new_epoch.snapshot();
        assert_eq!(old.events[0].event.get(), 0);
        assert_eq!(new.events[0].event.get(), 0);
        assert_eq!(
            old.events[0].executor_samples[0].measurement.operation,
            "late-old"
        );
        assert!(old.events[0].executor_samples[0].stale);
        assert!(old.events[0].completion.unwrap().stale);
        assert_eq!(
            new.events[0].executor_samples[0].measurement.operation,
            "current-new"
        );
        assert!(!new.events[0].executor_samples[0].stale);
        assert!(!new.events[0].completion.unwrap().stale);
    }

    #[test]
    fn residual_shadow_serial_lifecycle_closes_or_invalidates_automatically() {
        let dropped_epoch = ResidualShadowEpoch::new();
        let dropped = Query::new(
            FanoutLeaf {
                variable: 0,
                values: Arc::new(vec![raw(1)]),
            },
            |binding: &Binding| binding.get(0).copied(),
        )
        .solve_residual_state_lazy()
        .shadow(dropped_epoch.clone());
        drop(dropped);
        assert_eq!(dropped_epoch.status(), ResidualShadowStatus::Invalidated);

        let drained_epoch = ResidualShadowEpoch::new();
        let drained: Vec<_> = Query::new(
            FanoutLeaf {
                variable: 0,
                values: Arc::new(vec![raw(2)]),
            },
            |binding: &Binding| binding.get(0).copied(),
        )
        .solve_residual_state_lazy()
        .shadow(drained_epoch.clone())
        .collect();
        assert_eq!(drained, [raw(2)]);
        assert_eq!(drained_epoch.status(), ResidualShadowStatus::Closed);
        assert!(!drained_epoch.invalidate());
        assert_eq!(drained_epoch.status(), ResidualShadowStatus::Closed);
    }

    #[test]
    fn residual_shadow_planning_unwind_invalidates_without_an_action_event() {
        let epoch = ResidualShadowEpoch::new();
        let mut observed = Query::new(panic_leaf(PanicPhase::Planning), |binding: &Binding| {
            binding.get(0).copied()
        })
        .solve_residual_state_lazy()
        .shadow(epoch.clone());

        let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| observed.next()));
        assert!(unwind.is_err());
        assert_eq!(epoch.status(), ResidualShadowStatus::Invalidated);
        assert!(epoch.snapshot().events.is_empty());
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| observed.next())).is_err()
        );
    }

    #[test]
    fn residual_shadow_action_unwind_records_aborted_and_never_closes() {
        let epoch = ResidualShadowEpoch::new();
        let aborted_before_scope_drop = Arc::new(AtomicBool::new(false));
        SHADOW_ABORT_HOOK.with(|hook| {
            let observed = Arc::clone(&aborted_before_scope_drop);
            *hook.borrow_mut() = Some(Box::new(move |event| {
                observed.store(
                    current_residual_action().map(|action| action.event()) == Some(event),
                    Ordering::Release,
                );
            }));
        });
        let mut observed = Query::new(panic_leaf(PanicPhase::Propose), |binding: &Binding| {
            binding.get(0).copied()
        })
        .solve_residual_state_lazy()
        .shadow(epoch.clone());

        let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| observed.next()));
        assert!(unwind.is_err());
        assert!(aborted_before_scope_drop.load(Ordering::Acquire));
        assert!(current_residual_action().is_none());
        let snapshot = epoch.snapshot();
        assert_eq!(snapshot.status, ResidualShadowStatus::Invalidated);
        assert_eq!(snapshot.events.len(), 1);
        assert_eq!(
            snapshot.events[0].completion.unwrap().outcome,
            ActionOutcome::Aborted
        );
    }

    #[test]
    fn residual_shadow_projection_unwind_invalidates_after_normal_action_completion() {
        let epoch = ResidualShadowEpoch::new();
        let mut observed = Query::new(
            FanoutLeaf {
                variable: 0,
                values: Arc::new(vec![raw(1)]),
            },
            |_binding: &Binding| -> Option<RawInline> {
                panic!("intentional residual projection panic")
            },
        )
        .solve_residual_state_lazy()
        .shadow(epoch.clone());

        let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| observed.next()));
        assert!(unwind.is_err());
        let snapshot = epoch.snapshot();
        assert_eq!(snapshot.status, ResidualShadowStatus::Invalidated);
        assert_eq!(snapshot.events.len(), 1);
        assert_ne!(
            snapshot.events[0].completion.unwrap().outcome,
            ActionOutcome::Aborted
        );
    }

    #[test]
    fn residual_shadow_live_action_cannot_be_normally_closed_in_either_lock_order() {
        let close_first = ResidualShadowEpoch::new();
        let mut close_first_span = close_first.begin(
            observation_site(ActionVerb::Propose, 0),
            observation_geometry(1, 0),
        );
        close_first_span.start();
        assert_eq!(
            close_first.finish_exhausted(),
            ResidualShadowStatus::Invalidated
        );
        drop(close_first_span);
        let close_first_snapshot = close_first.snapshot();
        assert_eq!(
            close_first_snapshot.events[0].completion.unwrap().outcome,
            ActionOutcome::Aborted
        );
        assert_eq!(
            close_first_snapshot.status,
            ResidualShadowStatus::Invalidated
        );

        let abort_first = ResidualShadowEpoch::new();
        let mut abort_first_span = abort_first.begin(
            observation_site(ActionVerb::Confirm, 1),
            observation_geometry(1, 1),
        );
        abort_first_span.start();
        drop(abort_first_span);
        assert_eq!(
            abort_first.finish_exhausted(),
            ResidualShadowStatus::Invalidated
        );
        assert_eq!(abort_first.status(), ResidualShadowStatus::Invalidated);
    }

    #[test]
    fn residual_shadow_completion_stores_the_exact_captured_wall_duration() {
        let epoch = ResidualShadowEpoch::new();
        let mut span = epoch.begin(
            observation_site(ActionVerb::Propose, 0),
            observation_geometry(1, 0),
        );
        let scope = ShadowActionScope::enter(span.correlation());
        let epoch_inner = Arc::clone(&epoch.inner);
        let event = Arc::clone(&span.event);
        span.start_with(|| {
            let events = epoch_inner
                .events
                .try_lock()
                .expect("execution clock captured while the snapshot gate was held");
            let started = event
                .started
                .try_lock()
                .expect("execution clock captured while start publication was held");
            assert!(started.is_some(), "dispatch offset was not published first");
            drop(started);
            drop(events);
            Instant::now()
        });
        let captured = Duration::from_nanos(123_456);
        span.finish(captured, ActionOutcome::Dead);
        assert!(current_residual_action().is_some());
        drop(scope);
        assert_eq!(epoch.finish_exhausted(), ResidualShadowStatus::Closed);
        assert_eq!(
            epoch.snapshot().events[0].completion.unwrap().wall,
            captured
        );
    }

    #[test]
    fn residual_shadow_admitted_action_may_start_after_explicit_invalidation() {
        let epoch = ResidualShadowEpoch::new();
        let mut span = epoch.begin(
            observation_site(ActionVerb::Confirm, 0),
            observation_geometry(1, 1),
        );
        let registered = epoch.snapshot().events[0].started;
        assert!(epoch.invalidate());

        // Observation is diagnostic-only: invalidation rejects new events but
        // never cancels an action that the open epoch already admitted.
        span.start();
        let published = epoch.snapshot();
        assert_eq!(published.status, ResidualShadowStatus::Invalidated);
        assert!(published.events[0].started >= registered);
        assert!(published.events[0].completion.is_none());

        span.finish(Duration::ZERO, ActionOutcome::Dead);
        let completed = epoch.snapshot();
        assert_eq!(completed.status, ResidualShadowStatus::Invalidated);
        assert!(completed.events[0].completion.unwrap().stale);
    }

    #[test]
    fn residual_shadow_reports_whole_group_confirm_geometry_with_bound_schema() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let root = IntersectionConstraint::new(vec![
            Box::new(FanoutLeaf {
                variable: 0,
                values: Arc::new(vec![raw(1)]),
            }) as ShapeConstraint,
            Box::new(FanoutLeaf {
                variable: 1,
                values: Arc::new(vec![raw(8), raw(9)]),
            }) as ShapeConstraint,
            Box::new(WholeGroupMinimumLeaf {
                variable: 1,
                estimate: 65,
                calls: Arc::clone(&calls),
            }) as ShapeConstraint,
        ]);
        let epoch = ResidualShadowEpoch::new();
        let solved = Query::new(root, |binding: &Binding| {
            Some((binding.get(0).copied()?, binding.get(1).copied()?))
        })
        .solve_residual_state_lazy()
        .cap(64)
        .start_width(64)
        .shadow(epoch)
        .collect_profiled();

        assert_eq!(solved.results, [(raw(1), raw(8))]);
        assert_eq!(*calls.lock().unwrap(), [2]);
        let confirmation = solved
            .shadow
            .events
            .iter()
            .find(|event| event.site.verb == ActionVerb::Confirm && event.site.variable == 1)
            .expect("whole-group confirmation was observed");
        assert_eq!(confirmation.site.bound, VariableSet::new_singleton(0));
        assert_eq!(confirmation.geometry.parent_rows, 1);
        assert_eq!(confirmation.geometry.candidate_occurrences, 2);
        assert_eq!(confirmation.geometry.action_atoms, 1);
    }

    #[test]
    fn residual_shadow_terminal_epoch_rejects_claim_under_the_transition_lock() {
        let epoch = ResidualShadowEpoch::new();
        let drained: Vec<_> = Query::new(
            FanoutLeaf {
                variable: 0,
                values: Arc::new(vec![raw(1)]),
            },
            |binding: &Binding| binding.get(0).copied(),
        )
        .solve_residual_state_lazy()
        .shadow(epoch.clone())
        .collect();
        assert_eq!(drained, [raw(1)]);
        let claim = std::panic::catch_unwind({
            let epoch = epoch.clone();
            move || epoch.inner.claim()
        });
        assert!(claim.is_err());
        assert!(epoch.inner.claimed.load(Ordering::Acquire));
        assert_eq!(epoch.status(), ResidualShadowStatus::Closed);
    }

    #[test]
    fn residual_shadow_event_ids_do_not_alias_colliding_private_state_ids() {
        let state = StateId(7);
        let first = ResidualActionTask {
            state,
            action: ResidualAction::Propose {
                variable: 0,
                leaf: 0,
            },
            bound: VariableSet::new_empty(),
            parent_rows: 1,
            candidate_occurrences: 0,
            action_atoms: 1,
        };
        let second = ResidualActionTask {
            state,
            action: ResidualAction::Confirm {
                variable: 0,
                leaf: 1,
            },
            bound: VariableSet::new_empty(),
            parent_rows: 1,
            candidate_occurrences: 1,
            action_atoms: 1,
        };
        let epoch = ResidualShadowEpoch::new();
        let (first_site, first_geometry) = first.observation();
        let (second_site, second_geometry) = second.observation();
        let mut first_span = epoch.begin(first_site, first_geometry);
        let mut second_span = epoch.begin(second_site, second_geometry);
        let first_event = first_span.correlation().event();
        let second_event = second_span.correlation().event();
        assert_ne!(first_event, second_event);
        first_span.start();
        second_span.start();
        first_span.finish(Duration::ZERO, ActionOutcome::Dead);
        second_span.finish(Duration::ZERO, ActionOutcome::Dead);
        assert_eq!(epoch.finish_exhausted(), ResidualShadowStatus::Closed);
        let snapshot = epoch.snapshot();
        assert_eq!(snapshot.events[0].site.verb, ActionVerb::Propose);
        assert_eq!(snapshot.events[1].site.verb, ActionVerb::Confirm);
    }

    #[test]
    fn residual_shadow_handles_are_send_sync_and_selected_payload_stays_affine() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ResidualShadowEpoch>();
        assert_send_sync::<ActionCorrelation>();
        assert_send_sync::<ResidualShadowSnapshot>();

        trait AmbiguousIfClone<Marker> {
            fn marker() {}
        }
        impl<T: ?Sized> AmbiguousIfClone<()> for T {}
        struct CloneMarker;
        impl<T: ?Sized + Clone> AmbiguousIfClone<CloneMarker> for T {}
        let _ = <SelectedResidualTask as AmbiguousIfClone<_>>::marker;
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn residual_shadow_parallel_drive_shares_one_epoch_without_attribution_gaps() {
        use std::collections::HashSet;

        let values: Vec<_> = (0..128).map(raw).collect();
        let accepted = raw(37);
        let expected: Vec<_> = Query::new(
            parallel_logged_filter_fixture(
                values.clone(),
                accepted,
                Arc::new(Mutex::new(Vec::new())),
            ),
            |binding: &Binding| binding.get(0).copied(),
        )
        .solve_residual_state_lazy()
        .cap(128)
        .start_width(128)
        .collect();

        let log = Arc::new(Mutex::new(Vec::new()));
        let epoch = ResidualShadowEpoch::new();
        let run_epoch = epoch.clone();
        let root = parallel_logged_filter_fixture(values, accepted, Arc::clone(&log));
        let mut observed: Vec<_> = with_parallel_workers(4, move || {
            Query::new(root, |binding: &Binding| binding.get(0).copied())
                .solve_residual_state_lazy()
                .cap(128)
                .start_width(128)
                .shadow(run_epoch)
                .into_par_iter()
                .collect()
        });
        let mut expected = expected;
        observed.sort_unstable();
        expected.sort_unstable();
        assert_eq!(observed, expected);
        assert_eq!(observed, [accepted]);
        assert_eq!(epoch.status(), ResidualShadowStatus::Closed);

        let snapshot = epoch.snapshot();
        assert_eq!(snapshot.status, ResidualShadowStatus::Closed);
        assert_eq!(snapshot.events.len(), log.lock().unwrap().len());
        assert!(snapshot.events.len() > 2);
        let ids: HashSet<_> = snapshot.events.iter().map(|event| event.event).collect();
        assert_eq!(ids.len(), snapshot.events.len());
        for event in &snapshot.events {
            assert_eq!(event.executor_samples.len(), 1);
            assert_eq!(event.executor_samples[0].event, event.event);
            assert!(!event.executor_samples[0].stale);
            assert!(!event.completion.unwrap().stale);
        }
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn residual_shadow_parallel_short_circuit_invalidates_the_epoch() {
        let epoch = ResidualShadowEpoch::new();
        let run_epoch = epoch.clone();
        let root = Arc::new(FanoutLeaf {
            variable: 0,
            values: Arc::new((0..128).map(raw).collect()),
        });
        let found = with_parallel_workers(4, move || {
            Query::new(root, |binding: &Binding| binding.get(0).copied())
                .solve_residual_state_lazy()
                .cap(128)
                .start_width(128)
                .shadow(run_epoch)
                .into_par_iter()
                .find_any(|_| true)
        });
        assert!(found.is_some());
        assert_eq!(epoch.status(), ResidualShadowStatus::Invalidated);
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn residual_shadow_parallel_action_unwind_records_aborted_and_invalidates() {
        let epoch = ResidualShadowEpoch::new();
        let run_epoch = epoch.clone();
        let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            with_parallel_workers(4, move || {
                Query::new(panic_leaf(PanicPhase::Propose), |binding: &Binding| {
                    binding.get(0).copied()
                })
                .solve_residual_state_lazy()
                .cap(128)
                .start_width(128)
                .shadow(run_epoch)
                .into_par_iter()
                .collect::<Vec<_>>()
            })
        }));
        assert!(unwind.is_err());
        let snapshot = epoch.snapshot();
        assert_eq!(snapshot.status, ResidualShadowStatus::Invalidated);
        assert_eq!(snapshot.events.len(), 1);
        assert_eq!(
            snapshot.events[0].completion.unwrap().outcome,
            ActionOutcome::Aborted
        );
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn residual_shadow_parallel_producer_abandonment_is_detected_before_and_after_split() {
        for take in [0, 1] {
            let clones = Arc::new(AtomicUsize::new(0));
            let epoch = ResidualShadowEpoch::new();
            let run_epoch = epoch.clone();
            let root = CloneCountingFanout {
                variable: 0,
                values: Arc::new((0..128).map(raw).collect()),
                clones: Arc::clone(&clones),
                proposes: Arc::new(AtomicUsize::new(0)),
            };
            let results = with_parallel_workers(4, move || {
                Query::new(root, |binding: &Binding| binding.get(0).copied())
                    .solve_residual_state_lazy()
                    .cap(128)
                    .start_width(128)
                    .shadow(run_epoch)
                    .into_par_iter()
                    .take_any(take)
                    .collect::<Vec<_>>()
            });
            assert_eq!(results.len(), take);
            assert_eq!(epoch.status(), ResidualShadowStatus::Invalidated);
            if take == 0 {
                assert_eq!(clones.load(Ordering::Relaxed), 0);
                assert!(epoch.snapshot().events.is_empty());
            } else {
                assert!(
                    clones.load(Ordering::Relaxed) > 0,
                    "the frontier did not split"
                );
            }
        }
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn residual_shadow_finished_serial_iterator_stays_closed_in_rayon() {
        let epoch = ResidualShadowEpoch::new();
        let mut observed = Query::new(
            FanoutLeaf {
                variable: 0,
                values: Arc::new(vec![raw(1), raw(2)]),
            },
            |binding: &Binding| binding.get(0).copied(),
        )
        .solve_residual_state_lazy()
        .shadow(epoch.clone());
        let serial: Vec<_> = observed.by_ref().collect();
        assert_eq!(serial.len(), 2);
        assert_eq!(epoch.status(), ResidualShadowStatus::Closed);

        let parallel =
            with_parallel_workers(4, move || observed.into_par_iter().collect::<Vec<_>>());
        assert!(parallel.is_empty());
        assert_eq!(epoch.status(), ResidualShadowStatus::Closed);
    }

    fn paged_filter_first_trace(
        accepted: RawInline,
        sprint: bool,
    ) -> (
        Option<RawInline>,
        Vec<usize>,
        Vec<usize>,
        ResidualStateStats,
        usize,
    ) {
        let first_calls = Arc::new(Mutex::new(Vec::new()));
        let second_calls = Arc::new(Mutex::new(Vec::new()));
        let root = paged_filter_fixture(
            (0..64).map(raw).collect(),
            accepted,
            Arc::clone(&first_calls),
            Arc::clone(&second_calls),
        );
        let mut lazy = Query::new(root, |binding: &Binding| binding.get(0).copied())
            .solve_residual_state_lazy()
            .cap(64);
        lazy.state.continuation_sprint_enabled = sprint;
        let result = lazy.next();
        let first = first_calls.lock().unwrap().clone();
        let second = second_calls.lock().unwrap().clone();
        (
            result,
            first,
            second,
            lazy.stats().clone(),
            lazy.current_width(),
        )
    }

    fn root_formula_paged_filter_first_trace(
        accepted: RawInline,
    ) -> (
        Option<RawInline>,
        Vec<usize>,
        Vec<usize>,
        ResidualStateStats,
        usize,
    ) {
        let first_calls = Arc::new(Mutex::new(Vec::new()));
        let second_calls = Arc::new(Mutex::new(Vec::new()));
        let root = paged_filter_fixture(
            (0..64).map(raw).collect(),
            accepted,
            Arc::clone(&first_calls),
            Arc::clone(&second_calls),
        );
        let mut lazy = Query::new(root, |binding: &Binding| binding.get(0).copied())
            .solve_residual_state_lazy_with(ResidualLowering::new(FormulaScope::WholeRoot, false))
            .cap(64);
        let result = lazy.next();
        let first = first_calls.lock().unwrap().clone();
        let second = second_calls.lock().unwrap().clone();
        (
            result,
            first,
            second,
            lazy.stats().clone(),
            lazy.current_width(),
        )
    }

    #[test]
    fn synthetic_root_formula_width_one_preserves_candidate_descent() {
        let (result, first_calls, second_calls, stats, width) =
            root_formula_paged_filter_first_trace(raw(63));
        assert_eq!(result, Some(raw(63)));
        assert_eq!(first_calls, [1]);
        assert_eq!(second_calls, [1]);
        assert_eq!(stats.candidates_proposed, 64);
        assert_eq!(stats.candidates_confirmed, 2);
        assert_eq!(stats.max_confirm_candidates, 1);
        assert_eq!(stats.partial_pops, 1);
        assert_eq!(width, 2);
    }

    #[test]
    fn synthetic_root_formula_grows_page_local_misses_geometrically() {
        for (accepted, expected) in [(raw(0), Some(raw(0))), (raw(255), None)] {
            let (result, first_calls, second_calls, stats, width) =
                root_formula_paged_filter_first_trace(accepted);
            assert_eq!(result, expected);
            assert_eq!(first_calls, [1, 2, 4, 8, 16, 32, 1]);
            assert_eq!(second_calls, [1, 2, 4, 8, 16, 32, 1]);
            assert_eq!(stats.candidates_proposed, 64);
            assert_eq!(stats.candidates_confirmed, 128);
            assert_eq!(stats.max_confirm_candidates, 32);
            assert_eq!(stats.width_increases, 6);
            assert_eq!(width, 64);
        }
    }

    #[test]
    fn synthetic_root_formula_matches_atom_or_and_alternation_oracles() {
        let project = |binding: &Binding| binding.get(0).copied();

        let atom = || FanoutLeaf {
            variable: 0,
            values: Arc::new(vec![raw(1), raw(2), raw(2)]),
        };
        let mut atom_expected: Vec<_> = Query::new(atom(), project).sequential().collect();
        let mut atom_actual: Vec<_> = Query::new(atom(), project)
            .solve_residual_state_lazy_with(ResidualLowering::new(FormulaScope::WholeRoot, false))
            .collect();
        atom_expected.sort_unstable();
        atom_actual.sort_unstable();
        assert_eq!(atom_actual, atom_expected);

        let union = || {
            UnionConstraint::new(vec![
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(vec![raw(1), raw(2)]),
                }) as ShapeConstraint,
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(vec![raw(2), raw(3)]),
                }) as ShapeConstraint,
            ])
        };
        let mut union_expected: Vec<_> = Query::new(union(), project).sequential().collect();
        let mut union_actual: Vec<_> = Query::new(union(), project)
            .solve_residual_state_lazy_with(ResidualLowering::new(FormulaScope::WholeRoot, false))
            .collect();
        union_expected.sort_unstable();
        union_actual.sort_unstable();
        assert_eq!(union_actual, union_expected);
        assert_eq!(union_actual, [raw(1), raw(2), raw(3)]);

        let alternating = || {
            let arm = |values: Vec<RawInline>, accepted: RawInline| {
                Box::new(IntersectionConstraint::new(vec![
                    Box::new(FanoutLeaf {
                        variable: 0,
                        values: Arc::new(values),
                    }) as ShapeConstraint,
                    Box::new(PageFilterLeaf {
                        variable: 0,
                        estimate: 10,
                        accepted: Some(accepted),
                        calls: Arc::new(Mutex::new(Vec::new())),
                    }) as ShapeConstraint,
                ])) as ShapeConstraint
            };
            IntersectionConstraint::new(vec![
                Box::new(UnionConstraint::new(vec![
                    arm(vec![raw(1), raw(2)], raw(1)),
                    arm(vec![raw(2), raw(3)], raw(3)),
                ])) as ShapeConstraint,
                Box::new(PageFilterLeaf {
                    variable: 0,
                    estimate: 20,
                    accepted: Some(raw(3)),
                    calls: Arc::new(Mutex::new(Vec::new())),
                }) as ShapeConstraint,
            ])
        };
        let mut alternating_expected: Vec<_> =
            Query::new(alternating(), project).sequential().collect();
        let mut alternating_actual: Vec<_> = Query::new(alternating(), project)
            .solve_residual_state_lazy_with(ResidualLowering::new(FormulaScope::WholeRoot, false))
            .collect();
        alternating_expected.sort_unstable();
        alternating_actual.sort_unstable();
        assert_eq!(alternating_actual, alternating_expected);
        assert_eq!(alternating_actual, [raw(3)]);
    }

    #[test]
    fn synthetic_root_formula_preserves_ignore_support_and_zero_variable_boundaries() {
        let make_ignored = || {
            let inner = IntersectionConstraint::new(vec![
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(vec![raw(4), raw(5)]),
                }) as ShapeConstraint,
                Box::new(FanoutLeaf {
                    variable: 1,
                    values: Arc::new(vec![raw(9)]),
                }) as ShapeConstraint,
            ]);
            IgnoreConstraint::new(VariableSet::new_singleton(1), Box::new(inner))
        };
        let plan = ResidualPlan::compile_lowering(
            &make_ignored(),
            ResidualLowering::new(FormulaScope::WholeRoot, false),
        );
        let root = plan.finite_formula.root(0).unwrap();
        let node = plan.finite_formula.node(root);
        let FiniteFormulaNodeKind::And { children } = &node.kind else {
            panic!("Ignore must expose only candidate-stage AND structure")
        };
        assert!(node.support_atomic);
        assert_eq!(children.len(), 2);
        assert_eq!(node.support_span, 2, "Ignore Support remains one action");
        assert!(node.execution_span > node.support_span);
        assert!(matches!(
            plan.finite_formula.entry_focus(root, FormulaStage::Support),
            FormulaFocus::Action {
                node: focused,
                stage: FormulaStage::Support,
            } if focused == root
        ));
        assert!(matches!(
            plan.finite_formula.entry_focus(root, FormulaStage::Propose),
            FormulaFocus::Plan {
                node: focused,
                stage: FormulaStage::Propose,
                ..
            } if focused == root
        ));

        let mut relevant = ChildSet::empty(plan.len());
        relevant.insert(0);
        let candidate_start = plan
            .finite_formula
            .start(0, 0, UnionVerb::Propose { relevant });
        let candidate_child = plan.finite_formula.select_child(&candidate_start, 0);
        assert!(
            plan.finite_formula.grade(&candidate_child)
                > plan.finite_formula.grade(&candidate_start),
            "entering scoped candidate structure must strictly raise rank"
        );
        let support_start = FormulaProgramCounter {
            focus: plan.finite_formula.entry_focus(root, FormulaStage::Support),
            returns: Box::new([]),
            resume: candidate_start.resume.clone(),
        };
        let support_complete = plan.finite_formula.complete(&support_start);
        assert!(
            plan.finite_formula.grade(&support_complete)
                > plan.finite_formula.grade(&support_start),
            "atomic scoped Support must strictly raise rank"
        );

        let outer = IntersectionConstraint::new(vec![
            Box::new(FanoutLeaf {
                variable: 0,
                values: Arc::new(vec![raw(4)]),
            }) as ShapeConstraint,
            Box::new(make_ignored()) as ShapeConstraint,
        ]);
        let outer_plan = ResidualPlan::compile_lowering(
            &outer,
            ResidualLowering::new(FormulaScope::WholeRoot, false),
        );
        let outer_root = outer_plan.finite_formula.root(0).unwrap();
        let FiniteFormulaNodeKind::And { children } =
            &outer_plan.finite_formula.node(outer_root).kind
        else {
            panic!("synthetic root must remain a conjunction")
        };
        assert_eq!(
            children.len(),
            2,
            "root flattening must retain the nested scoped-AND node"
        );
        let scoped = outer_plan.finite_formula.node(children[1]);
        assert!(scoped.support_atomic);
        assert_eq!(
            scoped.path,
            FormulaPath(vec![FormulaStep::And(1)].into_boxed_slice())
        );

        let project = |binding: &Binding| binding.get(0).copied();
        let mut expected: Vec<_> = Query::new(make_ignored(), project).sequential().collect();
        let mut actual: Vec<_> = Query::new(make_ignored(), project)
            .solve_residual_state_lazy_with(ResidualLowering::new(FormulaScope::WholeRoot, false))
            .collect();
        expected.sort_unstable();
        actual.sort_unstable();
        assert_eq!(actual, expected);

        for truth in [false, true] {
            let expected = if truth { vec![()] } else { Vec::new() };
            let actual = Query::new(ZeroVariableTruth(truth), |_| Some(()))
                .solve_residual_state_lazy_with(ResidualLowering::new(
                    FormulaScope::WholeRoot,
                    false,
                ))
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn width_one_confirms_one_candidate_then_descends() {
        let first_calls = Arc::new(Mutex::new(Vec::new()));
        let second_calls = Arc::new(Mutex::new(Vec::new()));
        let values: Vec<_> = (0..64).map(raw).collect();
        let root = paged_filter_fixture(
            values,
            raw(63),
            Arc::clone(&first_calls),
            Arc::clone(&second_calls),
        );
        let mut lazy = Query::new(root, |binding: &Binding| binding.get(0).copied())
            .solve_residual_state_lazy()
            .cap(64);

        assert_eq!(lazy.next(), Some(raw(63)));
        assert_eq!(*first_calls.lock().unwrap(), [1]);
        assert_eq!(*second_calls.lock().unwrap(), [1]);
        assert_eq!(lazy.stats().candidates_proposed, 64);
        assert_eq!(lazy.stats().max_propose_candidates, 64);
        assert_eq!(lazy.stats().confirm_calls, 2);
        assert_eq!(lazy.stats().candidates_confirmed, 2);
        assert_eq!(lazy.stats().max_confirm_candidates, 1);
        assert_eq!(lazy.stats().partial_pops, 1);
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn conservative_query_clone_snapshots_parked_candidate_remainder() {
        let values: Vec<_> = (0..64).map(raw).collect();
        let root = Arc::new(IntersectionConstraint::new(vec![
            Box::new(FanoutLeaf {
                variable: 0,
                values: Arc::new(values.clone()),
            }) as ShapeConstraint,
            Box::new(PageFilterLeaf {
                variable: 0,
                estimate: values.len() + 1,
                accepted: None,
                calls: Arc::new(Mutex::new(Vec::new())),
            }) as ShapeConstraint,
        ]));
        let mut query = Query::new(root, |binding: &Binding| binding.get(0).copied())
            .residual_lowering(ResidualLowering::CONSERVATIVE)
            .residual_state_scheduler();

        assert_eq!(query.next(), Some(raw(63)));
        let runtime = query.residual.as_deref().expect("residual cursor started");
        assert!(runtime.machine.worklist.values().any(|level| {
            level
                .values()
                .any(|bucket| matches!(bucket, StateBucket::Candidates(_)))
        }));
        assert!(runtime.machine.stats.partial_pops > 0);

        let cloned = query.clone();
        assert_eq!(query.collect::<Vec<_>>(), cloned.collect::<Vec<_>>());
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn ordinary_query_clone_snapshots_parked_formula_remainder() {
        let values: Vec<_> = (0..64).map(raw).collect();
        let root = Arc::new(IntersectionConstraint::new(vec![
            Box::new(FanoutLeaf {
                variable: 0,
                values: Arc::new(values.clone()),
            }) as ShapeConstraint,
            Box::new(PageFilterLeaf {
                variable: 0,
                estimate: values.len() + 1,
                accepted: None,
                calls: Arc::new(Mutex::new(Vec::new())),
            }) as ShapeConstraint,
        ]));
        let mut query = Query::new(root, |binding: &Binding| binding.get(0).copied());

        let first = query.next().expect("the formula frontier is nonempty");
        let runtime = query.residual.as_deref().expect("residual cursor started");
        assert!(runtime.machine.worklist.values().any(|level| {
            level
                .values()
                .any(|bucket| matches!(bucket, StateBucket::Formula(_)))
        }));
        assert!(runtime.machine.stats.partial_pops > 0);

        let cloned = query.clone();
        let mut left = query.collect::<Vec<_>>();
        let mut right = cloned.collect::<Vec<_>>();
        let mut expected = values;
        expected.remove(
            expected
                .iter()
                .position(|value| *value == first)
                .expect("the emitted value belongs to the proposal domain"),
        );
        left.sort_unstable();
        right.sort_unstable();
        expected.sort_unstable();
        assert_eq!(left, expected);
        assert_eq!(right, expected);
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn ordinary_query_clone_snapshots_unconsumed_staged_output() {
        let values: Vec<_> = (0..8).map(raw).collect();
        let root = Arc::new(FanoutLeaf {
            variable: 0,
            values: Arc::new(values),
        });
        let mut query = Query::new(root, |binding: &Binding| binding.get(0).copied())
            .residual_state_scheduler();

        assert!(query.next().is_some());
        assert!(query.next().is_some());
        let runtime = query.residual.as_deref().expect("residual cursor started");
        assert!(
            runtime.machine.emit_next < runtime.machine.emit_count,
            "the second geometric pull must leave one raw row staged"
        );

        let cloned = query.clone();
        assert_eq!(query.collect::<Vec<_>>(), cloned.collect::<Vec<_>>());
    }

    #[test]
    fn surviving_second_page_sprints_to_emit_before_cold_candidates() {
        let (result, first_calls, second_calls, stats, width) =
            paged_filter_first_trace(raw(62), true);
        assert_eq!(result, Some(raw(62)));
        // The first width-one page dies; the second width-two page survives.
        // Cold candidate harvesting must not run another page before that
        // underfilled survivor commits and emits.
        assert_eq!(first_calls, [1, 2]);
        assert_eq!(second_calls, [1, 2]);
        assert_eq!(stats.candidates_confirmed, 6);
        assert_eq!(stats.max_confirm_candidates, 2);
        assert_eq!(stats.underfilled_continuation_pops, 2);
        assert_eq!(stats.delta_handoff_probe_pops, 0);
        assert_eq!(
            stats.state_pops,
            stats.full_pops + stats.readiness_pops + stats.continuation_pops,
            "every state pop has exactly one physical selection policy"
        );
        assert_eq!(stats.width_increases, 2);
        assert_eq!(width, 4);

        let (old_result, old_first, old_second, old_stats, _) =
            paged_filter_first_trace(raw(62), false);
        assert_eq!(old_result, result);
        let old_pages = [1, 2, 2, 4, 8, 16, 31];
        assert_eq!(old_first, old_pages);
        assert_eq!(old_second, old_pages);
        assert_eq!(old_stats.continuation_pops, 0);
    }

    #[test]
    fn surviving_midpoint_page_sprints_without_scanning_its_cold_prefix() {
        let (result, first_calls, second_calls, stats, width) =
            paged_filter_first_trace(raw(32), true);
        assert_eq!(result, Some(raw(32)));
        let expected_pages = [1, 2, 4, 8, 16, 32];
        assert_eq!(first_calls, expected_pages);
        assert_eq!(second_calls, expected_pages);
        assert_eq!(stats.candidates_confirmed, 126);
        assert_eq!(stats.max_confirm_candidates, 32);
        assert_eq!(stats.underfilled_continuation_pops, 2);
        assert_eq!(stats.delta_handoff_probe_pops, 0);
        assert_eq!(stats.width_increases, 6);
        assert_eq!(width, 64);

        let (old_result, old_first, old_second, old_stats, _) =
            paged_filter_first_trace(raw(32), false);
        assert_eq!(old_result, result);
        assert_eq!(old_first, [1, 2, 4, 8, 16, 32, 1]);
        assert_eq!(old_second, [1, 2, 4, 8, 16, 32, 1]);
        assert_eq!(old_stats.continuation_pops, 0);
    }

    #[test]
    fn late_and_absent_hits_grow_candidate_pages_geometrically() {
        for (accepted, expected) in [(raw(0), Some(raw(0))), (raw(255), None)] {
            let first_calls = Arc::new(Mutex::new(Vec::new()));
            let second_calls = Arc::new(Mutex::new(Vec::new()));
            let root = paged_filter_fixture(
                (0..64).map(raw).collect(),
                accepted,
                Arc::clone(&first_calls),
                Arc::clone(&second_calls),
            );
            let mut lazy = Query::new(root, |binding: &Binding| binding.get(0).copied())
                .solve_residual_state_lazy()
                .cap(64);

            assert_eq!(lazy.next(), expected);
            assert_eq!(*first_calls.lock().unwrap(), [1, 2, 4, 8, 16, 32, 1]);
            assert_eq!(*second_calls.lock().unwrap(), [1, 2, 4, 8, 16, 32, 1]);
            assert_eq!(lazy.stats().candidates_proposed, 64);
            assert_eq!(lazy.stats().candidates_confirmed, 128);
            assert_eq!(lazy.stats().max_confirm_candidates, 32);
            assert_eq!(lazy.stats().width_increases, 6);
        }
    }

    #[test]
    fn duplicate_candidate_multiplicity_survives_page_splitting() {
        let values = vec![raw(0), raw(0), raw(1), raw(1), raw(1), raw(2)];
        let make = || {
            IntersectionConstraint::new(vec![
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(values.clone()),
                }) as ShapeConstraint,
                Box::new(PageFilterLeaf {
                    variable: 0,
                    estimate: 100,
                    accepted: None,
                    calls: Arc::new(Mutex::new(Vec::new())),
                }) as ShapeConstraint,
            ])
        };
        let project = |binding: &Binding| binding.get(0).copied();
        let mut sequential: Vec<_> = Query::new(make(), project).sequential().collect();
        let mut cap_one: Vec<_> = Query::new(make(), project)
            .solve_residual_state_lazy()
            .cap(1)
            .collect();
        let mut geometric: Vec<_> = Query::new(make(), project)
            .solve_residual_state_lazy()
            .cap(64)
            .collect();
        sequential.sort_unstable();
        cap_one.sort_unstable();
        geometric.sort_unstable();
        assert_eq!(sequential, values);
        assert_eq!(cap_one, sequential);
        assert_eq!(geometric, sequential);
    }

    #[test]
    fn zero_width_parent_multiplicity_survives_forced_reconvergence_and_default_sprint() {
        let make = |calls: Arc<Mutex<Vec<usize>>>| {
            IntersectionConstraint::new(vec![
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::new((0..8).map(raw).collect()),
                }) as ShapeConstraint,
                Box::new(ParityFilterLeaf {
                    variable: 0,
                    estimate: 9,
                    parity: 0,
                    calls,
                }) as ShapeConstraint,
            ])
        };

        // Mechanism coverage: width one rejects candidate 7. Width two then
        // leaves 6 from page [5, 6] and 4 from page [3, 4]. Those pages
        // reconverge in the same checked Candidate state as two parent
        // occurrences with no committed
        // columns: rows=[], row_count=2. Draining through a projection that
        // rejects every terminal row forces that merged bucket to execute.
        let calls = Arc::new(Mutex::new(Vec::new()));
        let projected = Arc::new(Mutex::new(0usize));
        let projected_rows = Arc::clone(&projected);
        let mut profiled = Query::new(make(Arc::clone(&calls)), move |_| {
            *projected_rows.lock().unwrap() += 1;
            None::<()>
        })
        .solve_residual_state_lazy()
        .cap(8)
        .start_width(1)
        .growth(2);
        // This is specifically a reconvergence regression: the default
        // continuation sprint now follows each surviving page before its cold
        // sibling can merge. Pin the old physical schedule so the fixture
        // continues to exercise several zero-width parent occurrences under
        // one canonical state. The default sprint remains enabled in the
        // exact-bag comparison below.
        profiled.state.continuation_sprint_enabled = false;
        let profiled = profiled.collect_profiled();
        assert!(profiled.results.is_empty());
        assert_eq!(profiled.stats.bucket_merges, 1);
        assert_eq!(profiled.stats.rows_merged, 1);
        assert_eq!(*projected.lock().unwrap(), 4);
        assert_eq!(&*calls.lock().unwrap(), &[1, 2, 2, 3]);

        // Production-schedule coverage: with sprinting enabled, the same
        // pages need not reconverge, but every affine occurrence must remain
        // in the exact output bag.
        let project = |binding: &Binding| binding.get(0).copied();
        let mut residual: Vec<_> = Query::new(make(Arc::new(Mutex::new(Vec::new()))), project)
            .solve_residual_state_lazy()
            .cap(8)
            .start_width(1)
            .growth(2)
            .collect();
        let mut sequential: Vec<_> = Query::new(make(Arc::new(Mutex::new(Vec::new()))), project)
            .sequential()
            .collect();
        residual.sort_unstable();
        sequential.sort_unstable();
        assert_eq!(residual, (0..8).step_by(2).map(raw).collect::<Vec<_>>());
        assert_eq!(residual, sequential);
    }

    #[test]
    fn whole_group_confirmer_runs_atomically_before_page_local_suffix() {
        let whole_calls = Arc::new(Mutex::new(Vec::new()));
        let page_calls = Arc::new(Mutex::new(Vec::new()));
        let make = || {
            IntersectionConstraint::new(vec![
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(vec![raw(3), raw(1), raw(1), raw(2)]),
                }) as ShapeConstraint,
                Box::new(WholeGroupMinimumLeaf {
                    variable: 0,
                    estimate: 5,
                    calls: Arc::clone(&whole_calls),
                }) as ShapeConstraint,
                Box::new(PageFilterLeaf {
                    variable: 0,
                    estimate: 6,
                    accepted: None,
                    calls: Arc::clone(&page_calls),
                }) as ShapeConstraint,
            ])
        };
        let project = |binding: &Binding| binding.get(0).copied();
        let mut residual: Vec<_> = Query::new(make(), project)
            .solve_residual_state_lazy()
            .cap(1)
            .collect();
        let mut sequential: Vec<_> = Query::new(make(), project).sequential().collect();
        residual.sort_unstable();
        sequential.sort_unstable();
        assert_eq!(residual, [raw(1), raw(1)]);
        assert_eq!(residual, sequential);
        assert_eq!(*whole_calls.lock().unwrap(), [4, 4]);
        assert_eq!(*page_calls.lock().unwrap(), [1, 1, 2]);

        let synthetic_whole_calls = Arc::new(Mutex::new(Vec::new()));
        let synthetic_page_calls = Arc::new(Mutex::new(Vec::new()));
        let synthetic_root = IntersectionConstraint::new(vec![
            Box::new(FanoutLeaf {
                variable: 0,
                values: Arc::new(vec![raw(3), raw(1), raw(1), raw(2)]),
            }) as ShapeConstraint,
            Box::new(WholeGroupMinimumLeaf {
                variable: 0,
                estimate: 5,
                calls: Arc::clone(&synthetic_whole_calls),
            }) as ShapeConstraint,
            Box::new(PageFilterLeaf {
                variable: 0,
                estimate: 6,
                accepted: None,
                calls: Arc::clone(&synthetic_page_calls),
            }) as ShapeConstraint,
        ]);
        let mut synthetic: Vec<_> = Query::new(synthetic_root, project)
            .solve_residual_state_lazy_with(ResidualLowering::new(FormulaScope::WholeRoot, false))
            .cap(1)
            .collect();
        synthetic.sort_unstable();
        assert_eq!(synthetic, residual);
        assert_eq!(*synthetic_whole_calls.lock().unwrap(), [4]);
        assert_eq!(*synthetic_page_calls.lock().unwrap(), [1, 1]);
    }

    #[test]
    fn opaque_union_deduplicates_whole_group_before_page_local_suffix() {
        let left_calls = Arc::new(Mutex::new(Vec::new()));
        let right_calls = Arc::new(Mutex::new(Vec::new()));
        let suffix_calls = Arc::new(Mutex::new(Vec::new()));
        let make = || {
            let union = UnionConstraint::new(vec![
                PageFilterLeaf {
                    variable: 0,
                    estimate: 10,
                    accepted: Some(raw(0)),
                    calls: Arc::clone(&left_calls),
                },
                PageFilterLeaf {
                    variable: 0,
                    estimate: 10,
                    accepted: Some(raw(1)),
                    calls: Arc::clone(&right_calls),
                },
            ]);
            IntersectionConstraint::new(vec![
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(vec![raw(0), raw(0), raw(1), raw(1), raw(2)]),
                }) as ShapeConstraint,
                Box::new(union) as ShapeConstraint,
                Box::new(PageFilterLeaf {
                    variable: 0,
                    estimate: 30,
                    accepted: None,
                    calls: Arc::clone(&suffix_calls),
                }) as ShapeConstraint,
            ])
        };
        let project = |binding: &Binding| binding.get(0).copied();
        let mut residual: Vec<_> = Query::new(make(), project)
            .solve_residual_state_lazy()
            .cap(1)
            .collect();
        let mut sequential: Vec<_> = Query::new(make(), project).sequential().collect();
        residual.sort_unstable();
        sequential.sort_unstable();
        assert_eq!(residual, [raw(0), raw(1)]);
        assert_eq!(residual, sequential);
        assert_eq!(*left_calls.lock().unwrap(), [5, 5]);
        assert_eq!(*right_calls.lock().unwrap(), [5, 5]);
        assert_eq!(*suffix_calls.lock().unwrap(), [1, 1, 2]);
    }

    #[test]
    fn finite_union_proposal_matches_sequential_dag_and_opaque_residual() {
        let make = || {
            UnionConstraint::new(vec![
                FanoutLeaf {
                    variable: 0,
                    values: Arc::new(vec![raw(3), raw(1), raw(1)]),
                },
                FanoutLeaf {
                    variable: 0,
                    values: Arc::new(vec![raw(2), raw(3)]),
                },
            ])
        };
        let project = |binding: &Binding| binding.get(0).copied();
        let mut sequential: Vec<_> = Query::new(make(), project).sequential().collect();
        let mut dag: Vec<_> = Query::new(make(), project).lazy_dag_scheduler().collect();
        let mut opaque: Vec<_> = Query::new(make(), project)
            .solve_residual_state_lazy()
            .collect();
        let epoch = ResidualShadowEpoch::new();
        let mut lowered = Query::new(make(), project)
            .solve_residual_state_lazy_with(ResidualLowering::new(FormulaScope::UnionLeaves, false))
            .shadow(epoch)
            .collect_profiled();
        sequential.sort_unstable();
        dag.sort_unstable();
        opaque.sort_unstable();
        lowered.results.sort_unstable();
        assert_eq!(lowered.results, [raw(1), raw(2), raw(3)]);
        assert_eq!(lowered.results, sequential);
        assert_eq!(lowered.results, dag);
        assert_eq!(lowered.results, opaque);

        // Entering the lowered formula is planning. Every direct OR child has
        // one exact support guard followed by its proposal action. Observation
        // preserves the enclosing residual occurrence as the public call site;
        // the canonical formula counter and event ID distinguish each action.
        assert_eq!(lowered.stats.support_action_pops, 2);
        assert_eq!(lowered.stats.support_calls, 2);
        assert_eq!(lowered.stats.propose_action_pops, 2);
        assert_eq!(lowered.stats.propose_calls, 2);
        assert_eq!(lowered.stats.confirm_action_pops, 0);
        assert_eq!(lowered.shadow.events.len(), 4);
        assert_eq!(
            lowered
                .shadow
                .events
                .iter()
                .map(|event| event.event.get())
                .collect::<Vec<_>>(),
            [0, 1, 2, 3]
        );
        for (event, verb) in lowered.shadow.events.iter().zip([
            ActionVerb::Support,
            ActionVerb::Propose,
            ActionVerb::Support,
            ActionVerb::Propose,
        ]) {
            assert_eq!(event.site, observation_site(verb, 0));
            assert_eq!(event.geometry, observation_geometry(1, 0));
        }
    }

    #[test]
    fn finite_union_confirmation_fans_out_the_immutable_original_group() {
        let make = |left_calls: Arc<Mutex<Vec<usize>>>, right_calls: Arc<Mutex<Vec<usize>>>| {
            let union = UnionConstraint::new(vec![
                PageFilterLeaf {
                    variable: 0,
                    estimate: 10,
                    accepted: Some(raw(0)),
                    calls: left_calls,
                },
                PageFilterLeaf {
                    variable: 0,
                    estimate: 11,
                    accepted: Some(raw(1)),
                    calls: right_calls,
                },
            ]);
            IntersectionConstraint::new(vec![
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(vec![raw(0), raw(0), raw(1), raw(1), raw(2)]),
                }) as ShapeConstraint,
                Box::new(union) as ShapeConstraint,
            ])
        };
        let project = |binding: &Binding| binding.get(0).copied();
        let fresh = || Arc::new(Mutex::new(Vec::new()));
        let mut sequential: Vec<_> = Query::new(make(fresh(), fresh()), project)
            .sequential()
            .collect();
        let mut dag: Vec<_> = Query::new(make(fresh(), fresh()), project)
            .lazy_dag_scheduler()
            .collect();
        let mut opaque: Vec<_> = Query::new(make(fresh(), fresh()), project)
            .solve_residual_state_lazy()
            .collect();
        let left_calls = fresh();
        let right_calls = fresh();
        let mut lowered: Vec<_> = Query::new(
            make(Arc::clone(&left_calls), Arc::clone(&right_calls)),
            project,
        )
        .solve_residual_state_lazy_with(ResidualLowering::new(FormulaScope::UnionLeaves, false))
        .collect();
        sequential.sort_unstable();
        dag.sort_unstable();
        opaque.sort_unstable();
        lowered.sort_unstable();
        assert_eq!(lowered, [raw(0), raw(1)]);
        assert_eq!(lowered, sequential);
        assert_eq!(lowered, dag);
        assert_eq!(lowered, opaque);
        assert_eq!(*left_calls.lock().unwrap(), [5]);
        assert_eq!(*right_calls.lock().unwrap(), [5]);
    }

    #[test]
    fn finite_union_dead_arm_masks_split_then_remerge_by_done_set() {
        let left_rows = Arc::new(AtomicUsize::new(0));
        let right_rows = Arc::new(AtomicUsize::new(0));
        let make = |left_rows: Arc<AtomicUsize>, right_rows: Arc<AtomicUsize>| {
            let union = UnionConstraint::new(vec![
                MaskedUnionArm {
                    parent: 0,
                    variable: 1,
                    live_parity: 0,
                    value: raw(10),
                    proposal_rows: left_rows,
                },
                MaskedUnionArm {
                    parent: 0,
                    variable: 1,
                    live_parity: 1,
                    value: raw(20),
                    proposal_rows: right_rows,
                },
            ]);
            IntersectionConstraint::new(vec![
                Box::new(LoggedLeaf {
                    variable: 0,
                    leaf_occurrence: 99,
                    estimate: 1,
                    proposed: Arc::new(vec![raw(0), raw(1)]),
                    accepted: None,
                    log: Arc::new(Mutex::new(Vec::new())),
                }) as ShapeConstraint,
                Box::new(union) as ShapeConstraint,
            ])
        };
        let project =
            |binding: &Binding| Some((binding.get(0).copied()?, binding.get(1).copied()?));
        let mut sequential: Vec<_> = Query::new(
            make(Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0))),
            project,
        )
        .sequential()
        .collect();
        let mut lowered = Query::new(
            make(Arc::clone(&left_rows), Arc::clone(&right_rows)),
            project,
        )
        .solve_residual_state_lazy_with(ResidualLowering::new(FormulaScope::UnionLeaves, false))
        .cap(2)
        .start_width(2)
        .growth(1)
        .collect_profiled();
        sequential.sort_unstable();
        lowered.results.sort_unstable();
        assert_eq!(lowered.results, [(raw(0), raw(10)), (raw(1), raw(20))]);
        assert_eq!(lowered.results, sequential);
        assert_eq!(left_rows.load(Ordering::Relaxed), 1);
        assert_eq!(right_rows.load(Ordering::Relaxed), 1);
        assert!(
            lowered.stats.bucket_merges > 0,
            "opposite done-arm histories never reconverged"
        );
    }

    #[test]
    fn finite_union_keeps_duplicate_outer_parents_affine() {
        let make = || {
            let leaf = |estimate| VerbLeaf {
                variable: 1,
                estimate,
                accepts: true,
                proposes: Arc::new(AtomicUsize::new(0)),
                confirms: Arc::new(AtomicUsize::new(0)),
            };
            let arm = |estimate| {
                IntersectionConstraint::new(vec![
                    Box::new(leaf(estimate)) as ShapeConstraint,
                    Box::new(leaf(estimate + 100)) as ShapeConstraint,
                ])
            };
            IntersectionConstraint::new(vec![
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(vec![raw(7), raw(7)]),
                }) as ShapeConstraint,
                Box::new(UnionConstraint::new(vec![arm(100), arm(101)])) as ShapeConstraint,
            ])
        };
        let project =
            |binding: &Binding| Some((binding.get(0).copied()?, binding.get(1).copied()?));
        let mut sequential: Vec<_> = Query::new(make(), project).sequential().collect();
        let mut lowered: Vec<_> = Query::new(make(), project)
            .solve_residual_state_lazy_with(ResidualLowering::new(FormulaScope::UnionLeaves, false))
            .collect();
        sequential.sort_unstable();
        lowered.sort_unstable();
        assert_eq!(lowered, [(raw(7), raw(1)), (raw(7), raw(1))]);
        assert_eq!(lowered, sequential);
    }

    #[test]
    fn finite_union_lazy_first_result_only_runs_one_parent_cohort() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let make_arm = |leaf_occurrence, proposed| LoggedLeaf {
            variable: 1,
            leaf_occurrence,
            estimate: 100,
            proposed: Arc::new(vec![proposed]),
            accepted: None,
            log: Arc::clone(&log),
        };
        let root = IntersectionConstraint::new(vec![
            Box::new(FanoutLeaf {
                variable: 0,
                values: Arc::new((0..32).map(raw).collect()),
            }) as ShapeConstraint,
            Box::new(UnionConstraint::new(vec![
                make_arm(0, raw(10)),
                make_arm(1, raw(20)),
            ])) as ShapeConstraint,
        ]);
        let mut lowered = Query::new(root, |binding: &Binding| {
            Some((binding.get(0).copied()?, binding.get(1).copied()?))
        })
        .solve_residual_state_lazy_with(ResidualLowering::new(FormulaScope::UnionLeaves, false))
        .cap(32)
        .start_width(1)
        .growth(2);
        assert!(lowered.next().is_some());
        let calls = log.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert!(calls.iter().all(|call| call.parent_rows == 1));
    }

    #[test]
    fn singleton_rpq_seed_stays_hot_but_wide_cold_seed_remains_batched() {
        use crate::id::{id_into_value, ExclusiveId, Id};
        use crate::query::regularpathconstraint::{PathOp, RegularPathConstraint};
        use crate::trible::{Trible, TribleSet};

        let kind = Id::new([200; crate::id::ID_LEN]).unwrap();
        let p = Id::new([201; crate::id::ID_LEN]).unwrap();
        let q = Id::new([202; crate::id::ID_LEN]).unwrap();
        let seed = Id::new([210; crate::id::ID_LEN]).unwrap();
        let alternate = Id::new([211; crate::id::ID_LEN]).unwrap();
        let red = Id::new([212; crate::id::ID_LEN]).unwrap();
        let blue = Id::new([213; crate::id::ID_LEN]).unwrap();
        let nodes: Vec<Vec<Id>> = (0..4)
            .map(|component| {
                (0..16)
                    .map(|position| {
                        let ordinal = component * 16 + position + 1;
                        Id::new([u8::try_from(ordinal).unwrap(); crate::id::ID_LEN]).unwrap()
                    })
                    .collect()
            })
            .collect();
        let mut graph = TribleSet::new();
        let insert = |graph: &mut TribleSet, from: &Id, attribute: &Id, to: &Id| {
            graph.insert(&Trible::new::<GenId>(
                ExclusiveId::force_ref(from),
                attribute,
                &to.to_inline(),
            ));
        };
        for component in &nodes {
            for (position, node) in component.iter().enumerate() {
                let source_class = match position % 4 {
                    0 => &seed,
                    1 => &alternate,
                    _ => &red,
                };
                insert(&mut graph, node, &kind, source_class);
                insert(
                    &mut graph,
                    node,
                    &kind,
                    if position % 2 == 0 { &red } else { &blue },
                );
                for offset in 1..=2 {
                    insert(
                        &mut graph,
                        node,
                        &p,
                        &component[(position + offset) % component.len()],
                    );
                    insert(
                        &mut graph,
                        node,
                        &q,
                        &component[(position + 2 + offset) % component.len()],
                    );
                }
            }
        }
        let graph = Arc::new(graph);
        let make = || {
            let source_var = Variable::<GenId>::new(0);
            let target_var = Variable::<GenId>::new(1);
            let source = UnionConstraint::new(vec![
                graph.pattern(
                    source_var,
                    Inline::<GenId>::new(id_into_value(&kind)),
                    Inline::<GenId>::new(id_into_value(&seed)),
                ),
                graph.pattern(
                    source_var,
                    Inline::<GenId>::new(id_into_value(&kind)),
                    Inline::<GenId>::new(id_into_value(&alternate)),
                ),
            ]);
            let path = RegularPathConstraint::new(
                graph.as_ref().clone(),
                source_var,
                target_var,
                &[
                    PathOp::Attr(p.raw()),
                    PathOp::Attr(q.raw()),
                    PathOp::Union,
                    PathOp::Plus,
                ],
            );
            let target = UnionConstraint::new(vec![
                graph.pattern(
                    target_var,
                    Inline::<GenId>::new(id_into_value(&kind)),
                    Inline::<GenId>::new(id_into_value(&red)),
                ),
                graph.pattern(
                    target_var,
                    Inline::<GenId>::new(id_into_value(&kind)),
                    Inline::<GenId>::new(id_into_value(&blue)),
                ),
            ]);
            IntersectionConstraint::new(vec![
                Box::new(source) as ShapeConstraint,
                Box::new(path) as ShapeConstraint,
                Box::new(target) as ShapeConstraint,
            ])
        };
        let project =
            |binding: &Binding| Some((binding.get(0).copied()?, binding.get(1).copied()?));

        let mut focused = Query::new(make(), project)
            .solve_residual_state_lazy_with(ResidualLowering::FULL)
            .cap(64)
            .start_width(1)
            .growth(2);
        let focused_first = focused.next().expect("the ring has a path result");
        let focused_first_stats = focused.stats().clone();
        assert_eq!(focused_first_stats.propose_rows, 3);
        assert_eq!(focused_first_stats.max_propose_rows, 1);
        assert_eq!(
            focused_first_stats.support_action_pops
                + focused_first_stats.propose_action_pops
                + focused_first_stats.confirm_action_pops,
            10,
            "the singleton path seed must reach target confirmation before the cold source remainder"
        );

        let mut cold = Query::new(make(), project)
            .solve_residual_state_lazy_with(ResidualLowering::FULL)
            .cap(64)
            .start_width(1)
            .growth(2);
        cold.state.continuation_sprint_enabled = false;
        let cold_first = cold.next().expect("the control ring has a path result");
        let cold_first_stats = cold.stats().clone();
        assert!(cold_first_stats.propose_rows > focused_first_stats.propose_rows);
        assert!(cold_first_stats.max_propose_rows > 1);
        assert!(
            cold_first_stats.support_action_pops
                + cold_first_stats.propose_action_pops
                + cold_first_stats.confirm_action_pops
                > focused_first_stats.support_action_pops
                    + focused_first_stats.propose_action_pops
                    + focused_first_stats.confirm_action_pops,
            "without physical focus wider cold work runs before target confirmation"
        );

        let mut focused_bag: Vec<_> = std::iter::once(focused_first).chain(focused).collect();
        let mut cold_bag: Vec<_> = std::iter::once(cold_first).chain(cold).collect();
        focused_bag.sort_unstable();
        cold_bag.sort_unstable();
        assert_eq!(focused_bag, cold_bag);
        assert_eq!(focused_bag.len(), 4 * 8 * 16);
    }

    #[test]
    fn finite_one_arm_union_is_a_valid_submachine() {
        let make = || {
            UnionConstraint::new(vec![FanoutLeaf {
                variable: 0,
                values: Arc::new(vec![raw(4), raw(4), raw(5)]),
            }])
        };
        let project = |binding: &Binding| binding.get(0).copied();
        let mut sequential: Vec<_> = Query::new(make(), project).sequential().collect();
        let mut lowered: Vec<_> = Query::new(make(), project)
            .solve_residual_state_lazy_with(ResidualLowering::new(FormulaScope::UnionLeaves, false))
            .collect();
        sequential.sort_unstable();
        lowered.sort_unstable();
        assert_eq!(lowered, [raw(4), raw(5)]);
        assert_eq!(lowered, sequential);
    }

    #[test]
    fn quiescent_formula_eagerly_materializes_only_proposal_paging() {
        let run = |transition_source| {
            let proposes = Arc::new(AtomicUsize::new(0));
            let pages = Arc::new(AtomicUsize::new(0));
            let leaf = |values| PagedProposalLeaf {
                variable: 0,
                values: Arc::new(values),
                transition_source,
                proposes: Arc::clone(&proposes),
                pages: Arc::clone(&pages),
            };
            let root = UnionConstraint::new(vec![
                leaf(vec![raw(1), raw(2), raw(2)]),
                leaf(vec![raw(2), raw(3)]),
            ]);
            let mut solve = Query::new(root, |binding: &Binding| binding.get(0).copied())
                .solve_residual_state_lazy_with(ResidualLowering::FULL)
                .collect_profiled();
            solve.results.sort_unstable();
            (solve, proposes, pages)
        };

        let (proposal_only, proposes, pages) = run(false);
        assert_eq!(proposal_only.results, [raw(1), raw(2), raw(3)]);
        assert_eq!(proposes.load(Ordering::Relaxed), 2);
        assert_eq!(pages.load(Ordering::Relaxed), 0);
        assert_eq!(proposal_only.stats.delta_source_pages, 0);

        let (transition, proposes, pages) = run(true);
        assert_eq!(transition.results, proposal_only.results);
        assert_eq!(proposes.load(Ordering::Relaxed), 0);
        assert!(pages.load(Ordering::Relaxed) > 0);
        assert!(transition.stats.delta_source_pages > 0);

        let finite_proposes = Arc::new(AtomicUsize::new(0));
        let finite_pages = Arc::new(AtomicUsize::new(0));
        let transition_proposes = Arc::new(AtomicUsize::new(0));
        let transition_pages = Arc::new(AtomicUsize::new(0));
        let root = UnionConstraint::new(vec![
            PagedProposalLeaf {
                variable: 0,
                values: Arc::new(vec![raw(1), raw(2)]),
                transition_source: false,
                proposes: Arc::clone(&finite_proposes),
                pages: Arc::clone(&finite_pages),
            },
            PagedProposalLeaf {
                variable: 0,
                values: Arc::new(vec![raw(2), raw(3)]),
                transition_source: true,
                proposes: Arc::clone(&transition_proposes),
                pages: Arc::clone(&transition_pages),
            },
        ]);
        let mut heterogeneous: Vec<_> =
            Query::new(root, |binding: &Binding| binding.get(0).copied())
                .solve_residual_state_lazy_with(ResidualLowering::FULL)
                .collect();
        heterogeneous.sort_unstable();
        assert_eq!(heterogeneous, [raw(1), raw(2), raw(3)]);
        assert_eq!(finite_proposes.load(Ordering::Relaxed), 0);
        assert!(finite_pages.load(Ordering::Relaxed) > 0);
        assert_eq!(transition_proposes.load(Ordering::Relaxed), 0);
        assert!(transition_pages.load(Ordering::Relaxed) > 0);
    }

    #[test]
    fn finite_union_executes_direct_and_arm_children() {
        let make_arm = |values: Vec<RawInline>, accepted: RawInline| {
            IntersectionConstraint::new(vec![
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(values),
                }) as ShapeConstraint,
                Box::new(PageFilterLeaf {
                    variable: 0,
                    estimate: 20,
                    accepted: Some(accepted),
                    calls: Arc::new(Mutex::new(Vec::new())),
                }) as ShapeConstraint,
            ])
        };
        let make = || {
            UnionConstraint::new(vec![
                make_arm(vec![raw(1), raw(2)], raw(1)),
                make_arm(vec![raw(2), raw(3)], raw(3)),
            ])
        };
        let project = |binding: &Binding| binding.get(0).copied();
        let mut sequential: Vec<_> = Query::new(make(), project).sequential().collect();
        let mut lowered = Query::new(make(), project)
            .solve_residual_state_lazy_with(ResidualLowering::new(FormulaScope::UnionLeaves, false))
            .collect_profiled();
        sequential.sort_unstable();
        lowered.results.sort_unstable();
        assert_eq!(lowered.results, [raw(1), raw(3)]);
        assert_eq!(lowered.results, sequential);
        assert_eq!(lowered.stats.propose_calls, 2);
        assert_eq!(lowered.stats.confirm_calls, 2);
    }

    #[test]
    fn finite_union_and_confirmation_threads_current_but_preserves_sibling_input() {
        let first_calls = Arc::new(Mutex::new(Vec::new()));
        let second_calls = Arc::new(Mutex::new(Vec::new()));
        let sibling_calls = Arc::new(Mutex::new(Vec::new()));
        let make = |first_calls: Arc<Mutex<Vec<usize>>>,
                    second_calls: Arc<Mutex<Vec<usize>>>,
                    sibling_calls: Arc<Mutex<Vec<usize>>>| {
            let and_arm = IntersectionConstraint::new(vec![
                Box::new(PageFilterLeaf {
                    variable: 0,
                    estimate: 10,
                    accepted: Some(raw(0)),
                    calls: first_calls,
                }) as ShapeConstraint,
                Box::new(PageFilterLeaf {
                    variable: 0,
                    estimate: 11,
                    accepted: Some(raw(0)),
                    calls: second_calls,
                }) as ShapeConstraint,
            ]);
            let union = UnionConstraint::new(vec![
                Box::new(and_arm) as ShapeConstraint,
                Box::new(PageFilterLeaf {
                    variable: 0,
                    estimate: 12,
                    accepted: Some(raw(2)),
                    calls: sibling_calls,
                }) as ShapeConstraint,
            ]);
            IntersectionConstraint::new(vec![
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(vec![raw(0), raw(1), raw(2), raw(3)]),
                }) as ShapeConstraint,
                Box::new(union) as ShapeConstraint,
            ])
        };
        let project = |binding: &Binding| binding.get(0).copied();
        let mut sequential: Vec<_> = Query::new(
            make(
                Arc::new(Mutex::new(Vec::new())),
                Arc::new(Mutex::new(Vec::new())),
                Arc::new(Mutex::new(Vec::new())),
            ),
            project,
        )
        .sequential()
        .collect();
        let mut lowered: Vec<_> = Query::new(
            make(
                Arc::clone(&first_calls),
                Arc::clone(&second_calls),
                Arc::clone(&sibling_calls),
            ),
            project,
        )
        .solve_residual_state_lazy_with(ResidualLowering::new(FormulaScope::UnionLeaves, false))
        .collect();
        sequential.sort_unstable();
        lowered.sort_unstable();
        assert_eq!(lowered, [raw(0), raw(2)]);
        assert_eq!(lowered, sequential);
        assert_eq!(*first_calls.lock().unwrap(), [4]);
        assert_eq!(*second_calls.lock().unwrap(), [1]);
        assert_eq!(*sibling_calls.lock().unwrap(), [4]);
    }

    #[test]
    fn finite_union_empty_and_child_annihilates_only_its_private_branch() {
        let rejecting_calls = Arc::new(Mutex::new(Vec::new()));
        let skipped_calls = Arc::new(Mutex::new(Vec::new()));
        let sibling_calls = Arc::new(Mutex::new(Vec::new()));
        let make = |rejecting_calls: Arc<Mutex<Vec<usize>>>,
                    skipped_calls: Arc<Mutex<Vec<usize>>>,
                    sibling_calls: Arc<Mutex<Vec<usize>>>| {
            let and_arm = IntersectionConstraint::new(vec![
                Box::new(PageFilterLeaf {
                    variable: 0,
                    estimate: 10,
                    accepted: Some(raw(99)),
                    calls: rejecting_calls,
                }) as ShapeConstraint,
                Box::new(PageFilterLeaf {
                    variable: 0,
                    estimate: 11,
                    accepted: None,
                    calls: skipped_calls,
                }) as ShapeConstraint,
            ]);
            let union = UnionConstraint::new(vec![
                Box::new(and_arm) as ShapeConstraint,
                Box::new(PageFilterLeaf {
                    variable: 0,
                    estimate: 12,
                    accepted: Some(raw(2)),
                    calls: sibling_calls,
                }) as ShapeConstraint,
            ]);
            IntersectionConstraint::new(vec![
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(vec![raw(0), raw(1), raw(2), raw(3)]),
                }) as ShapeConstraint,
                Box::new(union) as ShapeConstraint,
            ])
        };
        let project = |binding: &Binding| binding.get(0).copied();
        let mut sequential: Vec<_> = Query::new(
            make(
                Arc::new(Mutex::new(Vec::new())),
                Arc::new(Mutex::new(Vec::new())),
                Arc::new(Mutex::new(Vec::new())),
            ),
            project,
        )
        .sequential()
        .collect();
        let mut lowered: Vec<_> = Query::new(
            make(
                Arc::clone(&rejecting_calls),
                Arc::clone(&skipped_calls),
                Arc::clone(&sibling_calls),
            ),
            project,
        )
        .solve_residual_state_lazy_with(ResidualLowering::new(FormulaScope::UnionLeaves, false))
        .collect();
        sequential.sort_unstable();
        lowered.sort_unstable();
        assert_eq!(lowered, [raw(2)]);
        assert_eq!(lowered, sequential);
        assert_eq!(*rejecting_calls.lock().unwrap(), [4]);
        assert!(skipped_calls.lock().unwrap().is_empty());
        assert_eq!(*sibling_calls.lock().unwrap(), [4]);
    }

    #[test]
    fn finite_union_and_selects_proposers_per_row_then_remerges_canonically() {
        let left_proposals = Arc::new(Mutex::new(Vec::new()));
        let right_proposals = Arc::new(Mutex::new(Vec::new()));
        let make = |left_proposals: Arc<Mutex<Vec<Vec<RawInline>>>>,
                    right_proposals: Arc<Mutex<Vec<Vec<RawInline>>>>| {
            let and_arm = IntersectionConstraint::new(vec![
                Box::new(RowAdaptiveLeaf {
                    parent: 0,
                    variable: 1,
                    estimates: [1, 10, 1, 10],
                    value: raw(7),
                    proposed_parents: left_proposals,
                }) as ShapeConstraint,
                Box::new(RowAdaptiveLeaf {
                    parent: 0,
                    variable: 1,
                    estimates: [10, 1, 10, 1],
                    value: raw(7),
                    proposed_parents: right_proposals,
                }) as ShapeConstraint,
            ]);
            IntersectionConstraint::new(vec![
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(vec![raw(0), raw(1)]),
                }) as ShapeConstraint,
                Box::new(UnionConstraint::new(vec![and_arm])) as ShapeConstraint,
            ])
        };
        let project =
            |binding: &Binding| Some((binding.get(0).copied()?, binding.get(1).copied()?));
        let mut sequential: Vec<_> = Query::new(
            make(
                Arc::new(Mutex::new(Vec::new())),
                Arc::new(Mutex::new(Vec::new())),
            ),
            project,
        )
        .sequential()
        .collect();
        let mut lowered = Query::new(
            make(Arc::clone(&left_proposals), Arc::clone(&right_proposals)),
            project,
        )
        .solve_residual_state_lazy_with(ResidualLowering::new(FormulaScope::UnionLeaves, false))
        .cap(2)
        .start_width(2)
        .growth(1)
        .collect_profiled();
        sequential.sort_unstable();
        lowered.results.sort_unstable();
        assert_eq!(lowered.results, [(raw(0), raw(7)), (raw(1), raw(7))]);
        assert_eq!(lowered.results, sequential);
        assert_eq!(*left_proposals.lock().unwrap(), [vec![raw(0)]]);
        assert_eq!(*right_proposals.lock().unwrap(), [vec![raw(1)]]);
        assert!(
            lowered.stats.bucket_merges > 0,
            "opposite AND child histories did not reconverge at one canonical PC"
        );

        let root_left_proposals = Arc::new(Mutex::new(Vec::new()));
        let root_right_proposals = Arc::new(Mutex::new(Vec::new()));
        let mut synthetic = Query::new(
            make(
                Arc::clone(&root_left_proposals),
                Arc::clone(&root_right_proposals),
            ),
            project,
        )
        .solve_residual_state_lazy_with(ResidualLowering::new(FormulaScope::WholeRoot, false))
        .cap(2)
        .start_width(2)
        .growth(1)
        .collect_profiled();
        synthetic.results.sort_unstable();
        assert_eq!(synthetic.results, sequential);
        assert_eq!(*root_left_proposals.lock().unwrap(), [vec![raw(0)]]);
        assert_eq!(*root_right_proposals.lock().unwrap(), [vec![raw(1)]]);
        assert!(
            synthetic.stats.bucket_merges > 0,
            "synthetic root histories did not remerge at one canonical PC"
        );
    }

    #[test]
    fn recursive_formula_remerges_opposite_row_orders_at_inner_and_outer_depths() {
        let left_proposals = Arc::new(Mutex::new(Vec::new()));
        let right_proposals = Arc::new(Mutex::new(Vec::new()));
        let outer_proposals = Arc::new(Mutex::new(Vec::new()));
        let make = |left_proposals: Arc<Mutex<Vec<Vec<RawInline>>>>,
                    right_proposals: Arc<Mutex<Vec<Vec<RawInline>>>>,
                    outer_proposals: Arc<Mutex<Vec<Vec<RawInline>>>>| {
            let inner_or = UnionConstraint::new(vec![
                Box::new(RowAdaptiveLeaf {
                    parent: 0,
                    variable: 1,
                    estimates: [1, 10, 1, 10],
                    value: raw(7),
                    proposed_parents: left_proposals,
                }) as ShapeConstraint,
                Box::new(RowAdaptiveLeaf {
                    parent: 0,
                    variable: 1,
                    estimates: [10, 1, 10, 1],
                    value: raw(7),
                    proposed_parents: right_proposals,
                }) as ShapeConstraint,
            ]);
            let outer_and = IntersectionConstraint::new(vec![
                Box::new(inner_or) as ShapeConstraint,
                Box::new(RowAdaptiveLeaf {
                    parent: 0,
                    variable: 1,
                    estimates: [20, 20, 1, 1],
                    value: raw(7),
                    proposed_parents: outer_proposals,
                }) as ShapeConstraint,
            ]);
            IntersectionConstraint::new(vec![
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(vec![raw(0), raw(1), raw(2), raw(3)]),
                }) as ShapeConstraint,
                Box::new(UnionConstraint::new(vec![outer_and])) as ShapeConstraint,
            ])
        };
        let project =
            |binding: &Binding| Some((binding.get(0).copied()?, binding.get(1).copied()?));
        let blank = || Arc::new(Mutex::new(Vec::new()));
        let mut sequential: Vec<_> = Query::new(make(blank(), blank(), blank()), project)
            .sequential()
            .collect();
        let mut opaque: Vec<_> = Query::new(make(blank(), blank(), blank()), project)
            .solve_residual_state_lazy()
            .collect();
        let mut lowered = Query::new(
            make(
                Arc::clone(&left_proposals),
                Arc::clone(&right_proposals),
                Arc::clone(&outer_proposals),
            ),
            project,
        )
        .solve_residual_state_lazy_with(ResidualLowering::new(FormulaScope::UnionLeaves, false))
        .cap(4)
        .start_width(4)
        .growth(1)
        .collect_profiled();
        sequential.sort_unstable();
        opaque.sort_unstable();
        lowered.results.sort_unstable();
        assert_eq!(
            lowered.results,
            [
                (raw(0), raw(7)),
                (raw(1), raw(7)),
                (raw(2), raw(7)),
                (raw(3), raw(7)),
            ]
        );
        assert_eq!(lowered.results, sequential);
        assert_eq!(lowered.results, opaque);

        let flatten = |log: &Arc<Mutex<Vec<Vec<RawInline>>>>| {
            let mut parents: Vec<_> = log.lock().unwrap().iter().flatten().copied().collect();
            parents.sort_unstable();
            parents
        };
        assert_eq!(flatten(&left_proposals), [raw(0), raw(1)]);
        assert_eq!(flatten(&right_proposals), [raw(0), raw(1)]);
        assert_eq!(flatten(&outer_proposals), [raw(2), raw(3)]);
        assert!(
            lowered.stats.bucket_merges >= 3,
            "recursive opposite-order histories did not remerge at multiple zipper depths: {:?}",
            lowered.stats
        );
    }

    #[test]
    fn recursive_union_compiler_flattens_or_but_preserves_and_nodes() {
        let terminal = |values: Vec<RawInline>, accepted: RawInline| {
            Box::new(IntersectionConstraint::new(vec![
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(values),
                }) as ShapeConstraint,
                Box::new(PageFilterLeaf {
                    variable: 0,
                    estimate: 20,
                    accepted: Some(accepted),
                    calls: Arc::new(Mutex::new(Vec::new())),
                }) as ShapeConstraint,
            ])) as ShapeConstraint
        };
        let make = || {
            let inner = UnionConstraint::new(vec![
                terminal(vec![raw(1), raw(2)], raw(1)),
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(vec![raw(3)]),
                }) as ShapeConstraint,
            ]);
            UnionConstraint::new(vec![
                Box::new(inner) as ShapeConstraint,
                terminal(vec![raw(2), raw(4)], raw(4)),
            ])
        };
        let plan = ResidualPlan::compile_finite_unions(&make());
        let formula_root = plan.finite_formula.root(0).unwrap();
        let FiniteFormulaNodeKind::Or { children } = &plan.finite_formula.node(formula_root).kind
        else {
            panic!("lowered recursive union is not an OR")
        };
        assert_eq!(children.len(), 3);
        assert_eq!(
            children
                .iter()
                .map(|&child| plan.finite_formula.node(child).path.clone())
                .collect::<Vec<_>>(),
            vec![
                FormulaPath(vec![FormulaStep::Or(0), FormulaStep::Or(0)].into_boxed_slice()),
                FormulaPath(vec![FormulaStep::Or(0), FormulaStep::Or(1)].into_boxed_slice()),
                FormulaPath(vec![FormulaStep::Or(1)].into_boxed_slice()),
            ]
        );

        let project = |binding: &Binding| binding.get(0).copied();
        let mut sequential: Vec<_> = Query::new(make(), project).sequential().collect();
        let mut dag: Vec<_> = Query::new(make(), project).lazy_dag_scheduler().collect();
        let mut opaque: Vec<_> = Query::new(make(), project)
            .solve_residual_state_lazy()
            .collect();
        let mut lowered: Vec<_> = Query::new(make(), project)
            .solve_residual_state_lazy_with(ResidualLowering::new(FormulaScope::UnionLeaves, false))
            .collect();
        sequential.sort_unstable();
        dag.sort_unstable();
        opaque.sort_unstable();
        lowered.sort_unstable();
        assert_eq!(lowered, [raw(1), raw(3), raw(4)]);
        assert_eq!(lowered, sequential);
        assert_eq!(lowered, dag);
        assert_eq!(lowered, opaque);
    }

    #[test]
    fn recursive_union_confirm_preserves_each_nested_original_fanout() {
        let zero_calls = Arc::new(Mutex::new(Vec::new()));
        let one_calls = Arc::new(Mutex::new(Vec::new()));
        let two_calls = Arc::new(Mutex::new(Vec::new()));
        let make = |zero_calls: Arc<Mutex<Vec<usize>>>,
                    one_calls: Arc<Mutex<Vec<usize>>>,
                    two_calls: Arc<Mutex<Vec<usize>>>| {
            let filter = |accepted, calls| {
                Box::new(PageFilterLeaf {
                    variable: 0,
                    estimate: 10,
                    accepted: Some(raw(accepted)),
                    calls,
                }) as ShapeConstraint
            };
            let nested = UnionConstraint::new(vec![filter(0, zero_calls), filter(1, one_calls)]);
            let union = UnionConstraint::new(vec![
                Box::new(nested) as ShapeConstraint,
                filter(2, two_calls),
            ]);
            IntersectionConstraint::new(vec![
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(vec![raw(0), raw(0), raw(1), raw(2), raw(3)]),
                }) as ShapeConstraint,
                Box::new(union) as ShapeConstraint,
            ])
        };
        let project = |binding: &Binding| binding.get(0).copied();
        let mut sequential: Vec<_> = Query::new(
            make(
                Arc::new(Mutex::new(Vec::new())),
                Arc::new(Mutex::new(Vec::new())),
                Arc::new(Mutex::new(Vec::new())),
            ),
            project,
        )
        .sequential()
        .collect();
        let mut lowered: Vec<_> = Query::new(
            make(
                Arc::clone(&zero_calls),
                Arc::clone(&one_calls),
                Arc::clone(&two_calls),
            ),
            project,
        )
        .solve_residual_state_lazy_with(ResidualLowering::new(FormulaScope::UnionLeaves, false))
        .collect();
        sequential.sort_unstable();
        lowered.sort_unstable();
        assert_eq!(lowered, [raw(0), raw(1), raw(2)]);
        assert_eq!(lowered, sequential);
        assert_eq!(*zero_calls.lock().unwrap(), [5]);
        assert_eq!(*one_calls.lock().unwrap(), [5]);
        assert_eq!(*two_calls.lock().unwrap(), [5]);
    }

    #[test]
    fn recursive_union_crosses_and_without_flattening_away_its_sibling() {
        let make = || {
            let nested = UnionConstraint::new(vec![
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(vec![raw(1)]),
                }) as ShapeConstraint,
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(vec![raw(2)]),
                }) as ShapeConstraint,
            ]);
            let guarded = IntersectionConstraint::new(vec![
                Box::new(nested) as ShapeConstraint,
                Box::new(PageFilterLeaf {
                    variable: 0,
                    estimate: 20,
                    accepted: Some(raw(2)),
                    calls: Arc::new(Mutex::new(Vec::new())),
                }) as ShapeConstraint,
            ]);
            UnionConstraint::new(vec![
                Box::new(guarded) as ShapeConstraint,
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(vec![raw(4)]),
                }) as ShapeConstraint,
            ])
        };

        // Descending through the AND and treating its nested Union children as
        // outer arms would drop the sibling filter and incorrectly admit 1.
        // OR flattening therefore stops at the AND occurrence. Execution
        // crosses the connective boundary with an activation-private current
        // frame, then enters the nested OR as another explicit frame.
        let plan = ResidualPlan::compile_finite_unions(&make());
        let formula_root = plan.finite_formula.root(0).unwrap();
        let FiniteFormulaNodeKind::Or { children } = &plan.finite_formula.node(formula_root).kind
        else {
            panic!("lowered guarded union is not an OR")
        };
        assert_eq!(children.len(), 2);
        let FiniteFormulaNodeKind::And {
            children: and_children,
        } = &plan.finite_formula.node(children[0]).kind
        else {
            panic!("guarded outer arm is not an AND")
        };
        assert!(matches!(
            plan.finite_formula.node(and_children[0]).kind,
            FiniteFormulaNodeKind::Or { .. }
        ));
        assert_eq!(
            children
                .iter()
                .map(|&child| plan.finite_formula.node(child).path.clone())
                .collect::<Vec<_>>(),
            vec![
                FormulaPath(vec![FormulaStep::Or(0)].into_boxed_slice()),
                FormulaPath(vec![FormulaStep::Or(1)].into_boxed_slice()),
            ]
        );

        let project = |binding: &Binding| binding.get(0).copied();
        let mut sequential: Vec<_> = Query::new(make(), project).sequential().collect();
        let mut opaque = Query::new(make(), project)
            .solve_residual_state_lazy()
            .collect_profiled();
        let mut lowered = Query::new(make(), project)
            .solve_residual_state_lazy_with(ResidualLowering::new(FormulaScope::UnionLeaves, false))
            .collect_profiled();
        sequential.sort_unstable();
        opaque.results.sort_unstable();
        lowered.results.sort_unstable();
        assert_eq!(lowered.results, [raw(2), raw(4)]);
        assert_eq!(lowered.results, sequential);
        assert_eq!(lowered.results, opaque.results);
        assert!(!lowered.results.contains(&raw(1)));
        assert_eq!(lowered.stats.propose_calls, 3);
        assert_eq!(lowered.stats.confirm_calls, 1);
        assert_eq!(opaque.stats.propose_calls, 1);
        assert_eq!(opaque.stats.confirm_calls, 0);
    }

    #[test]
    fn recursive_union_confirm_threads_inner_or_result_through_enclosing_and() {
        let zero_calls = Arc::new(Mutex::new(Vec::new()));
        let one_calls = Arc::new(Mutex::new(Vec::new()));
        let and_calls = Arc::new(Mutex::new(Vec::new()));
        let outer_calls = Arc::new(Mutex::new(Vec::new()));
        let make = |zero_calls: Arc<Mutex<Vec<usize>>>,
                    one_calls: Arc<Mutex<Vec<usize>>>,
                    and_calls: Arc<Mutex<Vec<usize>>>,
                    outer_calls: Arc<Mutex<Vec<usize>>>| {
            let inner = UnionConstraint::new(vec![
                Box::new(PageFilterLeaf {
                    variable: 0,
                    estimate: 1,
                    accepted: Some(raw(0)),
                    calls: zero_calls,
                }) as ShapeConstraint,
                Box::new(PageFilterLeaf {
                    variable: 0,
                    estimate: 2,
                    accepted: Some(raw(1)),
                    calls: one_calls,
                }) as ShapeConstraint,
            ]);
            let guarded = IntersectionConstraint::new(vec![
                Box::new(inner) as ShapeConstraint,
                Box::new(PageFilterLeaf {
                    variable: 0,
                    estimate: 10,
                    accepted: Some(raw(1)),
                    calls: and_calls,
                }) as ShapeConstraint,
            ]);
            let root_union = UnionConstraint::new(vec![
                Box::new(guarded) as ShapeConstraint,
                Box::new(PageFilterLeaf {
                    variable: 0,
                    estimate: 20,
                    accepted: Some(raw(3)),
                    calls: outer_calls,
                }) as ShapeConstraint,
            ]);
            IntersectionConstraint::new(vec![
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(vec![raw(0), raw(1), raw(2), raw(3)]),
                }) as ShapeConstraint,
                Box::new(root_union) as ShapeConstraint,
            ])
        };
        let project = |binding: &Binding| binding.get(0).copied();
        let blank = || Arc::new(Mutex::new(Vec::new()));
        let mut sequential: Vec<_> = Query::new(make(blank(), blank(), blank(), blank()), project)
            .sequential()
            .collect();
        let mut opaque: Vec<_> = Query::new(make(blank(), blank(), blank(), blank()), project)
            .solve_residual_state_lazy()
            .collect();
        let mut lowered: Vec<_> = Query::new(
            make(
                Arc::clone(&zero_calls),
                Arc::clone(&one_calls),
                Arc::clone(&and_calls),
                Arc::clone(&outer_calls),
            ),
            project,
        )
        .solve_residual_state_lazy_with(ResidualLowering::new(FormulaScope::UnionLeaves, false))
        .collect();
        sequential.sort_unstable();
        opaque.sort_unstable();
        lowered.sort_unstable();
        assert_eq!(lowered, [raw(1), raw(3)]);
        assert_eq!(lowered, sequential);
        assert_eq!(lowered, opaque);
        assert_eq!(*zero_calls.lock().unwrap(), [4]);
        assert_eq!(*one_calls.lock().unwrap(), [4]);
        assert_eq!(*and_calls.lock().unwrap(), [2]);
        assert_eq!(*outer_calls.lock().unwrap(), [4]);
    }

    #[test]
    fn recursive_inner_or_deduplicates_before_its_and_sibling() {
        let sibling_calls = Arc::new(Mutex::new(Vec::new()));
        let make = |sibling_calls: Arc<Mutex<Vec<usize>>>| {
            let inner = UnionConstraint::new(vec![
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(vec![raw(5), raw(5), raw(5)]),
                }) as ShapeConstraint,
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(vec![raw(5), raw(5)]),
                }) as ShapeConstraint,
            ]);
            let guarded = IntersectionConstraint::new(vec![
                Box::new(inner) as ShapeConstraint,
                Box::new(PageFilterLeaf {
                    variable: 0,
                    estimate: 20,
                    accepted: Some(raw(5)),
                    calls: sibling_calls,
                }) as ShapeConstraint,
            ]);
            UnionConstraint::new(vec![guarded])
        };
        let project = |binding: &Binding| binding.get(0).copied();
        let mut sequential: Vec<_> = Query::new(make(Arc::new(Mutex::new(Vec::new()))), project)
            .sequential()
            .collect();
        let mut opaque: Vec<_> = Query::new(make(Arc::new(Mutex::new(Vec::new()))), project)
            .solve_residual_state_lazy()
            .collect();
        let mut lowered: Vec<_> = Query::new(make(Arc::clone(&sibling_calls)), project)
            .solve_residual_state_lazy_with(ResidualLowering::new(FormulaScope::UnionLeaves, false))
            .collect();
        sequential.sort_unstable();
        opaque.sort_unstable();
        lowered.sort_unstable();
        assert_eq!(lowered, [raw(5)]);
        assert_eq!(lowered, sequential);
        assert_eq!(lowered, opaque);
        assert_eq!(
            *sibling_calls.lock().unwrap(),
            [1],
            "the enclosing AND observed an unnormalized inner OR output"
        );
    }

    #[test]
    fn recursive_formula_executes_two_connective_alternations_and_nested_and() {
        let make = || {
            let deepest_and = IntersectionConstraint::new(vec![
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(vec![raw(1), raw(2)]),
                }) as ShapeConstraint,
                Box::new(PageFilterLeaf {
                    variable: 0,
                    estimate: 20,
                    accepted: Some(raw(2)),
                    calls: Arc::new(Mutex::new(Vec::new())),
                }) as ShapeConstraint,
            ]);
            let middle_or = UnionConstraint::new(vec![
                Box::new(deepest_and) as ShapeConstraint,
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(vec![raw(3)]),
                }) as ShapeConstraint,
            ]);
            let outer_and = IntersectionConstraint::new(vec![
                Box::new(middle_or) as ShapeConstraint,
                Box::new(PageFilterLeaf {
                    variable: 0,
                    estimate: 30,
                    accepted: Some(raw(2)),
                    calls: Arc::new(Mutex::new(Vec::new())),
                }) as ShapeConstraint,
            ]);
            UnionConstraint::new(vec![
                Box::new(outer_and) as ShapeConstraint,
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(vec![raw(4)]),
                }) as ShapeConstraint,
            ])
        };
        let project = |binding: &Binding| binding.get(0).copied();
        let mut sequential: Vec<_> = Query::new(make(), project).sequential().collect();
        let mut opaque: Vec<_> = Query::new(make(), project)
            .solve_residual_state_lazy()
            .collect();
        let mut lowered: Vec<_> = Query::new(make(), project)
            .solve_residual_state_lazy_with(ResidualLowering::new(FormulaScope::UnionLeaves, false))
            .collect();
        sequential.sort_unstable();
        opaque.sort_unstable();
        lowered.sort_unstable();
        assert_eq!(lowered, [raw(2), raw(4)]);
        assert_eq!(lowered, sequential);
        assert_eq!(lowered, opaque);
    }

    #[test]
    fn recursive_empty_inner_or_annihilates_only_its_enclosing_and_branch() {
        let skipped_calls = Arc::new(Mutex::new(Vec::new()));
        let make = |skipped_calls: Arc<Mutex<Vec<usize>>>| {
            let empty_inner = UnionConstraint::new(vec![
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(Vec::new()),
                }) as ShapeConstraint,
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(Vec::new()),
                }) as ShapeConstraint,
            ]);
            let empty_and = IntersectionConstraint::new(vec![
                Box::new(empty_inner) as ShapeConstraint,
                Box::new(PageFilterLeaf {
                    variable: 0,
                    estimate: 20,
                    accepted: None,
                    calls: skipped_calls,
                }) as ShapeConstraint,
            ]);
            UnionConstraint::new(vec![
                Box::new(empty_and) as ShapeConstraint,
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(vec![raw(9)]),
                }) as ShapeConstraint,
            ])
        };
        let project = |binding: &Binding| binding.get(0).copied();
        let mut sequential: Vec<_> = Query::new(make(Arc::new(Mutex::new(Vec::new()))), project)
            .sequential()
            .collect();
        let mut opaque: Vec<_> = Query::new(make(Arc::new(Mutex::new(Vec::new()))), project)
            .solve_residual_state_lazy()
            .collect();
        let mut lowered: Vec<_> = Query::new(make(Arc::clone(&skipped_calls)), project)
            .solve_residual_state_lazy_with(ResidualLowering::new(FormulaScope::UnionLeaves, false))
            .collect();
        sequential.sort_unstable();
        opaque.sort_unstable();
        lowered.sort_unstable();
        assert_eq!(lowered, [raw(9)]);
        assert_eq!(lowered, sequential);
        assert_eq!(lowered, opaque);
        assert!(
            skipped_calls.lock().unwrap().is_empty(),
            "an annihilated recursive AND continued into a sibling action"
        );
    }

    #[test]
    fn recursive_inner_or_skips_row_dead_arms_and_remerges_the_live_results() {
        let even_rows = Arc::new(AtomicUsize::new(0));
        let odd_rows = Arc::new(AtomicUsize::new(0));
        let make = |even_rows: Arc<AtomicUsize>, odd_rows: Arc<AtomicUsize>| {
            let inner = UnionConstraint::new(vec![
                Box::new(MaskedUnionArm {
                    parent: 0,
                    variable: 1,
                    live_parity: 0,
                    value: raw(10),
                    proposal_rows: even_rows,
                }) as ShapeConstraint,
                Box::new(MaskedUnionArm {
                    parent: 0,
                    variable: 1,
                    live_parity: 1,
                    value: raw(20),
                    proposal_rows: odd_rows,
                }) as ShapeConstraint,
            ]);
            let guarded = IntersectionConstraint::new(vec![
                Box::new(inner) as ShapeConstraint,
                Box::new(PageFilterLeaf {
                    variable: 1,
                    estimate: 200,
                    accepted: None,
                    calls: Arc::new(Mutex::new(Vec::new())),
                }) as ShapeConstraint,
            ]);
            IntersectionConstraint::new(vec![
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(vec![raw(0), raw(1)]),
                }) as ShapeConstraint,
                Box::new(UnionConstraint::new(vec![guarded])) as ShapeConstraint,
            ])
        };
        let project =
            |binding: &Binding| Some((binding.get(0).copied()?, binding.get(1).copied()?));
        let mut sequential: Vec<_> = Query::new(
            make(Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0))),
            project,
        )
        .sequential()
        .collect();
        let mut opaque: Vec<_> = Query::new(
            make(Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0))),
            project,
        )
        .solve_residual_state_lazy()
        .collect();
        let mut lowered: Vec<_> =
            Query::new(make(Arc::clone(&even_rows), Arc::clone(&odd_rows)), project)
                .solve_residual_state_lazy_with(ResidualLowering::new(
                    FormulaScope::UnionLeaves,
                    false,
                ))
                .collect();
        sequential.sort_unstable();
        opaque.sort_unstable();
        lowered.sort_unstable();
        assert_eq!(lowered, [(raw(0), raw(10)), (raw(1), raw(20))]);
        assert_eq!(lowered, sequential);
        assert_eq!(lowered, opaque);
        assert_eq!(even_rows.load(Ordering::Relaxed), 1);
        assert_eq!(odd_rows.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn recursive_formula_preserves_duplicate_affine_parent_occurrences() {
        let make = || {
            let inner = UnionConstraint::new(vec![
                Box::new(VerbLeaf {
                    variable: 1,
                    estimate: 10,
                    accepts: true,
                    proposes: Arc::new(AtomicUsize::new(0)),
                    confirms: Arc::new(AtomicUsize::new(0)),
                }) as ShapeConstraint,
                Box::new(VerbLeaf {
                    variable: 1,
                    estimate: 11,
                    accepts: true,
                    proposes: Arc::new(AtomicUsize::new(0)),
                    confirms: Arc::new(AtomicUsize::new(0)),
                }) as ShapeConstraint,
            ]);
            let guarded = IntersectionConstraint::new(vec![
                Box::new(inner) as ShapeConstraint,
                Box::new(PageFilterLeaf {
                    variable: 1,
                    estimate: 30,
                    accepted: Some(raw(1)),
                    calls: Arc::new(Mutex::new(Vec::new())),
                }) as ShapeConstraint,
            ]);
            IntersectionConstraint::new(vec![
                Box::new(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(vec![raw(7), raw(7)]),
                }) as ShapeConstraint,
                Box::new(UnionConstraint::new(vec![guarded])) as ShapeConstraint,
            ])
        };
        let project =
            |binding: &Binding| Some((binding.get(0).copied()?, binding.get(1).copied()?));
        let mut sequential: Vec<_> = Query::new(make(), project).sequential().collect();
        let mut opaque: Vec<_> = Query::new(make(), project)
            .solve_residual_state_lazy()
            .collect();
        let mut lowered: Vec<_> = Query::new(make(), project)
            .solve_residual_state_lazy_with(ResidualLowering::new(FormulaScope::UnionLeaves, false))
            .collect();
        sequential.sort_unstable();
        opaque.sort_unstable();
        lowered.sort_unstable();
        assert_eq!(lowered, [(raw(7), raw(1)), (raw(7), raw(1))]);
        assert_eq!(lowered, sequential);
        assert_eq!(lowered, opaque);
    }

    #[test]
    fn repeated_finite_union_object_has_distinct_outer_occurrences() {
        let make = || {
            let union = Arc::new(UnionConstraint::new(vec![
                FanoutLeaf {
                    variable: 0,
                    values: Arc::new(vec![raw(1), raw(2)]),
                },
                FanoutLeaf {
                    variable: 0,
                    values: Arc::new(vec![raw(2), raw(3)]),
                },
            ]));
            IntersectionConstraint::new(vec![
                Box::new(Arc::clone(&union)) as ShapeConstraint,
                Box::new(union) as ShapeConstraint,
            ])
        };
        let plan = ResidualPlan::compile_finite_unions(&make());
        for occurrence in 0..2 {
            let formula_root = plan.finite_formula.root(occurrence).unwrap();
            let FiniteFormulaNodeKind::Or { children } =
                &plan.finite_formula.node(formula_root).kind
            else {
                panic!("repeated lowered union is not an OR")
            };
            assert_eq!(children.len(), 2);
        }
        assert_ne!(plan.leaves[0].path, plan.leaves[1].path);

        let project = |binding: &Binding| binding.get(0).copied();
        let mut sequential: Vec<_> = Query::new(make(), project).sequential().collect();
        let mut lowered: Vec<_> = Query::new(make(), project)
            .solve_residual_state_lazy_with(ResidualLowering::new(FormulaScope::UnionLeaves, false))
            .collect();
        sequential.sort_unstable();
        lowered.sort_unstable();
        assert_eq!(lowered, [raw(1), raw(2), raw(3)]);
        assert_eq!(lowered, sequential);
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn recursive_formula_parallel_split_preserves_deep_affine_frame_stack() {
        let make = || {
            let leaf = |estimate| VerbLeaf {
                variable: 1,
                estimate,
                accepts: true,
                proposes: Arc::new(AtomicUsize::new(0)),
                confirms: Arc::new(AtomicUsize::new(0)),
            };
            let arm = |estimate| {
                let deepest_and = parallel_shape(IntersectionConstraint::new(vec![
                    parallel_shape(leaf(estimate)),
                    parallel_shape(leaf(estimate + 100)),
                ]));
                let inner_or = parallel_shape(UnionConstraint::new(vec![
                    deepest_and,
                    parallel_shape(leaf(estimate + 200)),
                ]));
                parallel_shape(IntersectionConstraint::new(vec![
                    inner_or,
                    parallel_shape(leaf(estimate + 1_000)),
                ]))
            };
            Arc::new(IntersectionConstraint::new(vec![
                parallel_shape(FanoutLeaf {
                    variable: 0,
                    values: Arc::new((0..128).map(raw).collect()),
                }),
                parallel_shape(UnionConstraint::new(vec![arm(200), arm(201)])),
            ]))
        };
        let project =
            |binding: &Binding| Some((binding.get(0).copied()?, binding.get(1).copied()?));
        let mut expected: Vec<_> = Query::new(make(), project)
            .solve_residual_state_lazy_with(ResidualLowering::new(FormulaScope::UnionLeaves, false))
            .cap(128)
            .start_width(128)
            .collect();
        let mut parallel: Vec<_> = with_parallel_workers(4, || {
            Query::new(make(), project)
                .solve_residual_state_lazy_with(ResidualLowering::new(
                    FormulaScope::UnionLeaves,
                    false,
                ))
                .cap(128)
                .start_width(128)
                .into_par_iter()
                .collect()
        });
        expected.sort_unstable();
        parallel.sort_unstable();
        assert_eq!(parallel, expected);
        assert_eq!(parallel.len(), 128);
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn parallel_page_local_sharding_bisects_one_parent_duplicate_run() {
        let values = vec![
            raw(0),
            raw(0),
            raw(0),
            raw(1),
            raw(1),
            raw(2),
            raw(2),
            raw(3),
            raw(3),
            raw(4),
            raw(4),
            raw(5),
        ];
        let calls = Arc::new(Mutex::new(Vec::new()));
        let make = || {
            Arc::new(IntersectionConstraint::new(vec![
                parallel_shape(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(values.clone()),
                }),
                parallel_shape(PageFilterLeaf {
                    variable: 0,
                    estimate: values.len() + 1,
                    accepted: None,
                    calls: Arc::clone(&calls),
                }),
            ]))
        };
        let project = |binding: &Binding| binding.get(0).copied();

        let mut one_worker = with_parallel_workers(1, || {
            Query::new(make(), project)
                .into_par_residual_state_iter()
                .collect::<Vec<_>>()
        });
        one_worker.sort_unstable();
        assert_eq!(one_worker, values);
        assert_eq!(*calls.lock().unwrap(), [values.len()]);

        calls.lock().unwrap().clear();
        let mut four_workers = with_parallel_workers(4, || {
            Query::new(make(), project)
                .into_par_residual_state_iter()
                .collect::<Vec<_>>()
        });
        four_workers.sort_unstable();
        assert_eq!(four_workers, values);

        let page_sizes = calls.lock().unwrap();
        assert_eq!(page_sizes.iter().sum::<usize>(), values.len());
        assert!(page_sizes.len() > 1, "one parent must span several shards");
        assert!(
            page_sizes.iter().all(|&size| size < values.len()),
            "no worker may receive the original complete parent run"
        );
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn parallel_staged_emit_split_moves_each_raw_row_once() {
        let root = FanoutLeaf {
            variable: 0,
            values: Arc::new(Vec::new()),
        };
        let plan = ResidualPlan::compile(&root);
        let mut machine = ResidualStateMachine::new(root.variables(), plan.len(), Search::Done);
        machine.emit_vars = vec![0];
        machine.emit_rows = (0..7).map(raw).collect();
        machine.emit_count = 7;

        let right = machine
            .split_for_parallel(
                &root,
                &plan,
                &[VariableSet::new_empty(); 128],
                &[usize::MAX; 128],
            )
            .expect("seven staged rows are splittable");

        assert_eq!(machine.emit_count, 4);
        assert_eq!(machine.emit_rows, (0..4).map(raw).collect::<Vec<_>>());
        assert_eq!(right.emit_count, 3);
        assert_eq!(right.emit_rows, (4..7).map(raw).collect::<Vec<_>>());
        assert!(machine.worklist.is_empty());
        assert!(right.worklist.is_empty());
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn parallel_split_drops_only_active_delta_preference() {
        let root = ShapeLeaf(0);
        let plan = ResidualPlan::compile(&root);
        let influences = [VariableSet::new_empty(); 128];
        let base_estimates = [usize::MAX; 128];
        let mut machine = ResidualStateMachine::new(root.variables(), plan.len(), Search::Done);
        file(
            &mut machine.worklist,
            &mut machine.interner,
            plan.len(),
            ready_desc(1),
            StateBucket::Rows(RowBatch {
                rows: vec![raw(1), raw(2)],
                row_count: 2,
            }),
            &mut machine.stats,
        );
        let active = machine
            .delta
            .seed_source_proposals(
                DeltaDesc::leaf(0, 0),
                StateDesc {
                    bound: VariableSet::new_empty(),
                    phase: ResidualPhase::Ready,
                },
                RowBatch::seed(),
            )
            .expect("one physical delta preference was filed");
        machine.active_delta = Some(active);

        let right = machine
            .split_for_parallel(&root, &plan, &influences, &base_estimates)
            .expect("the two stable rows are splittable");
        assert!(machine.active_delta.is_none());
        assert!(right.active_delta.is_none());
        assert!(
            !machine.delta.is_empty(),
            "clearing the preference must not drop its affine scheduler work"
        );
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn parallel_split_clears_live_continuation_without_losing_affine_rows() {
        let root = ShapeLeaf(0);
        let plan = ResidualPlan::compile(&root);
        let influences = [VariableSet::new_empty(); 128];
        let base_estimates = [usize::MAX; 128];
        let expected: Vec<_> = (0..6).map(raw).collect();
        let mut machine = ResidualStateMachine::new(root.variables(), plan.len(), Search::Done);
        let desc = ready_desc(1);
        let first = file(
            &mut machine.worklist,
            &mut machine.interner,
            plan.len(),
            desc.clone(),
            StateBucket::Rows(RowBatch {
                rows: expected[..2].to_vec(),
                row_count: 2,
            }),
            &mut machine.stats,
        )
        .expect("fixture files the first continuation receipt");
        let second = file(
            &mut machine.worklist,
            &mut machine.interner,
            plan.len(),
            desc,
            StateBucket::Rows(RowBatch {
                rows: expected[2..].to_vec(),
                row_count: 4,
            }),
            &mut machine.stats,
        )
        .expect("fixture files the equal-key continuation receipt");
        let mut continuation = None;
        prefer_continuation(&mut continuation, Some(first));
        prefer_continuation(&mut continuation, Some(second));
        let continuation = continuation.expect("equal receipts coalesce");
        assert_eq!(continuation.rows, expected.len());
        machine.continuation = Some(ActiveContinuation::probe_one(continuation));

        let mut right = machine
            .split_for_parallel(&root, &plan, &influences, &base_estimates)
            .expect("six continuation rows are splittable");
        assert!(machine.continuation.is_none());
        assert!(right.continuation.is_none());

        let project = |binding: &Binding| binding.get(0).copied();
        let drain = |machine: &mut ResidualStateMachine| {
            std::iter::from_fn(|| {
                machine.pull(&root, &plan, &project, &influences, &base_estimates)
            })
            .collect::<Vec<_>>()
        };
        let left_rows = drain(&mut machine);
        let right_rows = drain(&mut right);
        assert!(!left_rows.is_empty());
        assert!(!right_rows.is_empty());
        let mut actual = left_rows;
        actual.extend(right_rows);
        actual.sort_unstable();
        assert_eq!(actual, expected);

        for stats in [&machine.stats, &right.stats] {
            assert!(stats.state_pops > 0);
            assert_eq!(
                stats.state_pops,
                stats.full_pops + stats.readiness_pops + stats.continuation_pops,
                "every shard pop has exactly one physical selection policy"
            );
        }
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn parallel_atomic_custom_and_union_keep_parent_run_whole() {
        let whole_calls = Arc::new(Mutex::new(Vec::new()));
        let suffix_calls = Arc::new(Mutex::new(Vec::new()));
        let custom_root = Arc::new(IntersectionConstraint::new(vec![
            parallel_shape(FanoutLeaf {
                variable: 0,
                values: Arc::new(vec![raw(3), raw(1), raw(1), raw(2)]),
            }),
            parallel_shape(WholeGroupMinimumLeaf {
                variable: 0,
                estimate: 5,
                calls: Arc::clone(&whole_calls),
            }),
            parallel_shape(PageFilterLeaf {
                variable: 0,
                estimate: 6,
                accepted: None,
                calls: Arc::clone(&suffix_calls),
            }),
        ]));
        let project = |binding: &Binding| binding.get(0).copied();
        let mut custom = with_parallel_workers(4, || {
            Query::new(custom_root, project)
                .into_par_residual_state_iter()
                .collect::<Vec<_>>()
        });
        custom.sort_unstable();
        assert_eq!(custom, [raw(1), raw(1)]);
        assert_eq!(*whole_calls.lock().unwrap(), [4]);
        let mut custom_suffix = suffix_calls.lock().unwrap().clone();
        custom_suffix.sort_unstable();
        assert_eq!(custom_suffix, [1, 1]);

        let left_calls = Arc::new(Mutex::new(Vec::new()));
        let right_calls = Arc::new(Mutex::new(Vec::new()));
        let union_suffix_calls = Arc::new(Mutex::new(Vec::new()));
        let union = UnionConstraint::new(vec![
            PageFilterLeaf {
                variable: 0,
                estimate: 10,
                accepted: Some(raw(0)),
                calls: Arc::clone(&left_calls),
            },
            PageFilterLeaf {
                variable: 0,
                estimate: 10,
                accepted: Some(raw(1)),
                calls: Arc::clone(&right_calls),
            },
        ]);
        let union_root = Arc::new(IntersectionConstraint::new(vec![
            parallel_shape(FanoutLeaf {
                variable: 0,
                values: Arc::new(vec![raw(0), raw(0), raw(1), raw(1), raw(2)]),
            }),
            parallel_shape(union),
            parallel_shape(PageFilterLeaf {
                variable: 0,
                estimate: 30,
                accepted: None,
                calls: Arc::clone(&union_suffix_calls),
            }),
        ]));
        let mut union_results = with_parallel_workers(4, || {
            Query::new(union_root, project)
                .into_par_residual_state_iter()
                .collect::<Vec<_>>()
        });
        union_results.sort_unstable();
        assert_eq!(union_results, [raw(0), raw(1)]);
        assert_eq!(*left_calls.lock().unwrap(), [5]);
        assert_eq!(*right_calls.lock().unwrap(), [5]);
        let mut union_suffix = union_suffix_calls.lock().unwrap().clone();
        union_suffix.sort_unstable();
        assert_eq!(union_suffix, [1, 1]);
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn started_residual_parallel_conversion_drains_exact_remainder_once() {
        let values: Vec<_> = (0..9).map(raw).collect();
        let make = || {
            Arc::new(IntersectionConstraint::new(vec![
                parallel_shape(FanoutLeaf {
                    variable: 0,
                    values: Arc::new(values.clone()),
                }),
                parallel_shape(PageFilterLeaf {
                    variable: 0,
                    estimate: values.len() + 1,
                    accepted: None,
                    calls: Arc::new(Mutex::new(Vec::new())),
                }),
            ]))
        };
        let project = |binding: &Binding| binding.get(0).copied();

        let mut serial = Query::new(make(), project)
            .solve_residual_state_lazy()
            .cap(64);
        let first = serial.next();
        let serial_remainder: Vec<_> = serial.collect();

        let mut started = Query::new(make(), project)
            .solve_residual_state_lazy()
            .cap(64);
        assert_eq!(started.next(), first);
        let parallel_remainder =
            with_parallel_workers(4, move || started.into_par_iter().collect::<Vec<_>>());
        assert_eq!(parallel_remainder, serial_remainder);
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn parallel_residual_matches_early_late_absent_and_zero_column_oracles() {
        let project = |binding: &Binding| binding.get(0).copied();
        let values: Vec<_> = (0..64).map(raw).collect();
        for accepted in [raw(0), raw(63), raw(255)] {
            let make = || parallel_paged_filter_fixture(values.clone(), accepted);
            let mut expected: Vec<_> = values
                .iter()
                .copied()
                .filter(|value| *value == accepted)
                .collect();
            let mut sequential: Vec<_> = Query::new(make(), project).sequential().collect();
            let mut dag: Vec<_> = Query::new(make(), project).solve_dag_lazy().collect();
            let mut residual: Vec<_> = Query::new(make(), project)
                .solve_residual_state_lazy()
                .collect();
            expected.sort_unstable();
            sequential.sort_unstable();
            dag.sort_unstable();
            residual.sort_unstable();
            assert_eq!(sequential, expected);
            assert_eq!(dag, expected);
            assert_eq!(residual, expected);
            for workers in [1, 4] {
                let mut parallel = with_parallel_workers(workers, || {
                    Query::new(make(), project)
                        .into_par_residual_state_iter()
                        .collect::<Vec<_>>()
                });
                parallel.sort_unstable();
                assert_eq!(parallel, expected, "workers={workers}");
            }
        }

        for truth in [false, true] {
            let expected = if truth { vec![()] } else { Vec::new() };
            assert_eq!(
                Query::new(ZeroVariableTruth(truth), |_| Some(()))
                    .sequential()
                    .collect::<Vec<_>>(),
                expected
            );
            assert_eq!(
                Query::new(ZeroVariableTruth(truth), |_| Some(()))
                    .solve_dag_lazy()
                    .collect::<Vec<_>>(),
                expected
            );
            assert_eq!(
                Query::new(ZeroVariableTruth(truth), |_| Some(()))
                    .solve_residual_state_lazy()
                    .collect::<Vec<_>>(),
                expected
            );
            for workers in [1, 4] {
                let parallel = with_parallel_workers(workers, || {
                    Query::new(ZeroVariableTruth(truth), |_| Some(()))
                        .into_par_residual_state_iter()
                        .collect::<Vec<_>>()
                });
                assert_eq!(parallel, expected, "truth={truth}, workers={workers}");
            }
        }
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn parallel_residual_clones_only_for_siblings_and_not_projected_rows() {
        struct NonCloneResult(RawInline);

        let values: Vec<_> = (0..16).map(raw).collect();
        for workers in [1, 4] {
            let clones = Arc::new(AtomicUsize::new(0));
            let proposes = Arc::new(AtomicUsize::new(0));
            let root = CloneCountingFanout {
                variable: 0,
                values: Arc::new(values.clone()),
                clones: Arc::clone(&clones),
                proposes: Arc::clone(&proposes),
            };
            let results = with_parallel_workers(workers, || {
                Query::new(root, |binding: &Binding| {
                    Some(NonCloneResult(*binding.get(0).unwrap()))
                })
                .into_par_residual_state_iter()
                .collect::<Vec<_>>()
            });
            let mut raw_results: Vec<_> = results.into_iter().map(|result| result.0).collect();
            raw_results.sort_unstable();
            assert_eq!(raw_results, values);
            assert_eq!(
                proposes.load(Ordering::Relaxed),
                1,
                "parallel negotiation must advance one seed, not restart shards"
            );

            let clone_count = clones.load(Ordering::Relaxed);
            if workers == 1 {
                assert_eq!(clone_count, 0);
            } else {
                assert!((1..=workers - 1).contains(&clone_count));
            }
        }
    }

    #[test]
    fn width_one_selects_the_deepest_live_state() {
        let mut machine = scheduler_fixture(&[(1, 4, 1), (2, 3, 2), (3, 1, 3)]);

        let (desc, chunk) = machine.take_next(1).expect("fixture has live work");

        assert_eq!(desc, ready_desc(3));
        assert_eq!(chunk.row_count(), 1);
        assert_eq!(machine.stats.full_pops, 1);
        assert_eq!(machine.stats.readiness_pops, 0);
    }

    #[test]
    fn continuation_preference_coalesces_equal_keys_across_interleaved_receipts() {
        let first = ContinuationToken {
            rank: 7,
            state: StateId(3),
            rows: 2,
            candidates: 5,
        };
        let lower = ContinuationToken {
            rank: 6,
            state: StateId(99),
            rows: 100,
            candidates: 101,
        };
        let equal = ContinuationToken {
            rank: 7,
            state: StateId(3),
            rows: 3,
            candidates: 7,
        };
        let higher = ContinuationToken {
            rank: 8,
            state: StateId(1),
            rows: 11,
            candidates: 13,
        };
        let higher_equal = ContinuationToken {
            rank: 8,
            state: StateId(1),
            rows: 17,
            candidates: 19,
        };

        let mut selected = None;
        for receipt in [first, lower, equal] {
            prefer_continuation(&mut selected, Some(receipt));
        }
        assert_eq!(
            selected,
            Some(ContinuationToken {
                rows: 5,
                candidates: 12,
                ..first
            })
        );

        prefer_continuation(&mut selected, Some(higher));
        prefer_continuation(&mut selected, Some(lower));
        prefer_continuation(&mut selected, Some(higher_equal));
        assert_eq!(
            selected,
            Some(ContinuationToken {
                rows: 28,
                candidates: 32,
                ..higher
            })
        );
    }

    #[test]
    fn continuation_receipt_coalescing_checks_both_occupancy_dimensions() {
        for (rows, candidates) in [(usize::MAX, 0), (0, usize::MAX)] {
            let result = std::panic::catch_unwind(|| {
                let mut selected = Some(ContinuationToken {
                    rank: 1,
                    state: StateId(0),
                    rows,
                    candidates,
                });
                prefer_continuation(
                    &mut selected,
                    Some(ContinuationToken {
                        rank: 1,
                        state: StateId(0),
                        rows: usize::from(rows != 0),
                        candidates: usize::from(candidates != 0),
                    }),
                );
            });
            assert!(result.is_err());
        }
    }

    #[test]
    fn coalesced_zero_width_rows_preserve_affine_multiplicity() {
        let root = ShapeLeaf(0);
        let plan = ResidualPlan::compile(&root);
        let desc = ready_desc(0);
        let mut machine = ResidualStateMachine::new(root.variables(), plan.len(), Search::Done);

        let zero_rows = |row_count| {
            StateBucket::Rows(RowBatch {
                rows: Vec::new(),
                row_count,
            })
        };
        let first = file(
            &mut machine.worklist,
            &mut machine.interner,
            plan.len(),
            desc.clone(),
            zero_rows(3),
            &mut machine.stats,
        );
        let second = file(
            &mut machine.worklist,
            &mut machine.interner,
            plan.len(),
            desc.clone(),
            zero_rows(4),
            &mut machine.stats,
        );
        let mut receipt = None;
        prefer_continuation(&mut receipt, first);
        prefer_continuation(&mut receipt, second);
        let receipt = receipt.expect("virtual rows produce a physical receipt");
        assert_eq!(receipt.rows, 7);

        let task = machine.take_continuation(&plan, ActiveContinuation::cohort(receipt), 8);
        let StateBucket::Rows(rows) = task.bucket else {
            panic!("zero-width continuation changed payload shape")
        };
        assert!(rows.rows.is_empty());
        assert_eq!(rows.row_count, 7);
        assert_eq!(machine.stats.underfilled_continuation_pops, 1);
        assert!(machine.worklist.is_empty());
    }

    #[test]
    fn coalesced_candidate_page_receipts_track_candidate_and_formula_tails() {
        let coalesce = |first, second| {
            let mut receipt = None;
            prefer_continuation(&mut receipt, first);
            prefer_continuation(&mut receipt, second);
            receipt.expect("equal candidate-page receipts coalesce")
        };

        let candidate_root = IntersectionConstraint::new(vec![
            CapabilityLeaf {
                variable: 0,
                page_local: false,
            },
            CapabilityLeaf {
                variable: 0,
                page_local: true,
            },
        ]);
        let candidate_plan = ResidualPlan::compile(&candidate_root);
        let candidate_formula_pcs = FormulaPcInterner::default();
        let mut relevant = ChildSet::empty(candidate_plan.len());
        relevant.insert(0);
        relevant.insert(1);
        let checked = ChildSet::empty(candidate_plan.len()).with_inserted(0);
        let candidate_desc = StateDesc {
            bound: VariableSet::new_empty(),
            phase: ResidualPhase::Candidate {
                variable: 0,
                relevant,
                checked,
            },
        };
        assert!(candidate_desc.uses_candidate_pages(&candidate_plan, &candidate_formula_pcs));
        let candidate_bucket = |row_count, candidates| {
            StateBucket::Candidates(CandidateBatch {
                parents: RowBatch {
                    rows: Vec::new(),
                    row_count,
                },
                candidates: candidate_payload(row_count, candidates),
            })
        };
        let mut candidate_machine = ResidualStateMachine::new(
            candidate_root.variables(),
            candidate_plan.len(),
            Search::Done,
        );
        let first = file_with_plan(
            &mut candidate_machine.worklist,
            &mut candidate_machine.interner,
            &candidate_plan,
            candidate_desc.clone(),
            candidate_bucket(1, vec![(0, raw(10)), (0, raw(11))]),
            &mut candidate_machine.stats,
        );
        let second = file_with_plan(
            &mut candidate_machine.worklist,
            &mut candidate_machine.interner,
            &candidate_plan,
            candidate_desc.clone(),
            candidate_bucket(2, vec![(0, raw(12)), (1, raw(13)), (1, raw(14))]),
            &mut candidate_machine.stats,
        );
        let candidate_receipt = coalesce(first, second);
        assert_eq!(candidate_receipt.rows, 3);
        assert_eq!(candidate_receipt.candidates, 5);

        let task = candidate_machine.take_continuation(
            &candidate_plan,
            ActiveContinuation::cohort(candidate_receipt),
            4,
        );
        let StateBucket::Candidates(page) = task.bucket else {
            panic!("candidate-page receipt changed payload shape")
        };
        assert_eq!(page.parents.row_count, 3);
        assert_eq!(
            page.candidates,
            [(0, raw(11)), (1, raw(12)), (2, raw(13)), (2, raw(14)),]
        );
        assert_eq!(candidate_machine.stats.underfilled_continuation_pops, 0);
        let StateBucket::Candidates(remainder) = candidate_machine
            .worklist
            .get(&candidate_receipt.rank)
            .and_then(|level| level.get(&candidate_receipt.state))
            .expect("the bisected hot parent remains")
        else {
            panic!("candidate remainder changed payload shape")
        };
        assert_eq!(remainder.parents.row_count, 1);
        assert_eq!(remainder.candidates, [(0, raw(10))]);

        let formula_root = IntersectionConstraint::new(vec![
            CapabilityLeaf {
                variable: 0,
                page_local: false,
            },
            CapabilityLeaf {
                variable: 0,
                page_local: true,
            },
        ]);
        let formula_plan = ResidualPlan::compile_lowering(
            &formula_root,
            ResidualLowering::new(FormulaScope::WholeRoot, false),
        );
        let relevant = ChildSet::empty(formula_plan.len()).with_inserted(0);
        let mut formula_machine =
            ResidualStateMachine::new(formula_root.variables(), formula_plan.len(), Search::Done);
        let start = formula_machine.interner.start_formula(
            &formula_plan.finite_formula,
            0,
            0,
            UnionVerb::Propose { relevant },
        );
        let action = formula_machine.interner.formula_pcs.select_child_as_action(
            &formula_plan.finite_formula,
            start,
            0,
        );
        let completed = formula_machine
            .interner
            .formula_pcs
            .complete(&formula_plan.finite_formula, action);
        let Ok(InternedFormulaSuccessor::Formula(counter)) = formula_machine
            .interner
            .formula_pcs
            .resume_completed(&formula_plan.finite_formula, completed)
        else {
            panic!("root AND proposer did not return to its confirmation suffix")
        };
        let formula_desc = StateDesc {
            bound: VariableSet::new_empty(),
            phase: ResidualPhase::Formula { counter },
        };
        assert!(
            formula_desc.uses_candidate_pages(&formula_plan, &formula_machine.interner.formula_pcs)
        );
        let formula_bucket = |activations, row_count, current| {
            StateBucket::Formula(FormulaBatch {
                activations,
                parents: RowBatch {
                    rows: Vec::new(),
                    row_count,
                },
                frames: vec![FormulaPayloadFrame::And {
                    current: candidate_payload(row_count, current),
                }],
            })
        };
        let first = file_with_plan(
            &mut formula_machine.worklist,
            &mut formula_machine.interner,
            &formula_plan,
            formula_desc.clone(),
            formula_bucket(vec![ActivationId(10)], 1, vec![(0, raw(10)), (0, raw(11))]),
            &mut formula_machine.stats,
        );
        let second = file_with_plan(
            &mut formula_machine.worklist,
            &mut formula_machine.interner,
            &formula_plan,
            formula_desc,
            formula_bucket(
                vec![ActivationId(11), ActivationId(12)],
                2,
                vec![(0, raw(12)), (1, raw(13)), (1, raw(14))],
            ),
            &mut formula_machine.stats,
        );
        let formula_receipt = coalesce(first, second);
        assert_eq!(formula_receipt.rows, 3);
        assert_eq!(formula_receipt.candidates, 5);

        let task = formula_machine.take_continuation(
            &formula_plan,
            ActiveContinuation::cohort(formula_receipt),
            8,
        );
        let StateBucket::Formula(page) = task.bucket else {
            panic!("formula-page receipt changed payload shape")
        };
        assert_eq!(
            page.activations,
            [ActivationId(10), ActivationId(11), ActivationId(12)]
        );
        assert_eq!(page.parents.row_count, 3);
        let [FormulaPayloadFrame::And { current }] = page.frames.as_slice() else {
            panic!("formula continuation lost its root AND frame")
        };
        assert_eq!(
            current,
            &vec![
                (0, raw(10)),
                (0, raw(11)),
                (1, raw(12)),
                (2, raw(13)),
                (2, raw(14)),
            ]
        );
        assert_eq!(formula_machine.stats.underfilled_continuation_pops, 1);
        assert!(formula_machine.worklist.is_empty());
    }

    #[test]
    fn continuation_token_cuts_only_the_new_tail_of_a_merged_state() {
        const PARENT: VariableId = 0;
        const VARIABLE: VariableId = 1;
        let root = IntersectionConstraint::new(vec![
            CapabilityLeaf {
                variable: VARIABLE,
                page_local: false,
            },
            CapabilityLeaf {
                variable: VARIABLE,
                page_local: true,
            },
        ]);
        let plan = ResidualPlan::compile(&root);
        let formula_pcs = FormulaPcInterner::default();
        let mut relevant = ChildSet::empty(plan.len());
        relevant.insert(0);
        relevant.insert(1);
        let mut checked = ChildSet::empty(plan.len());
        checked.insert(0);
        let desc = StateDesc {
            // A nonzero row stride makes the old/new cohort boundary directly
            // observable instead of relying on the virtual seed row.
            bound: VariableSet::new_singleton(PARENT),
            phase: ResidualPhase::Candidate {
                variable: VARIABLE,
                relevant,
                checked,
            },
        };
        assert!(desc.uses_candidate_pages(&plan, &formula_pcs));

        let mut machine = ResidualStateMachine::new(root.variables(), plan.len(), Search::Done);
        let old = StateBucket::Candidates(CandidateBatch {
            parents: RowBatch {
                rows: vec![raw(10)],
                row_count: 1,
            },
            candidates: CandidatePayload::Values(vec![raw(1), raw(2), raw(3)]),
        });
        let _old_token = file(
            &mut machine.worklist,
            &mut machine.interner,
            plan.len(),
            desc.clone(),
            old,
            &mut machine.stats,
        )
        .unwrap();
        let hot = file(
            &mut machine.worklist,
            &mut machine.interner,
            plan.len(),
            desc.clone(),
            StateBucket::Candidates(CandidateBatch {
                parents: RowBatch {
                    rows: vec![raw(99)],
                    row_count: 1,
                },
                candidates: CandidatePayload::Values(vec![raw(42)]),
            }),
            &mut machine.stats,
        )
        .unwrap();

        // A deeper unrelated state is also live. A global "strict deepest"
        // flag would be free to steal it; the physical token is exact.
        file(
            &mut machine.worklist,
            &mut machine.interner,
            plan.len(),
            ready_desc(3),
            ready_bucket(3, 1, 77),
            &mut machine.stats,
        );

        let task = machine.take_continuation(&plan, ActiveContinuation::cohort(hot), 8);
        assert_eq!(task.state, hot.state);
        assert_eq!(task.desc, desc);
        let StateBucket::Candidates(chunk) = task.bucket else {
            panic!("continuation returned a row payload")
        };
        assert_eq!(chunk.parents.rows, [raw(99)]);
        assert_eq!(chunk.candidates, [(0, raw(42))]);
        assert_eq!(machine.stats.continuation_pops, 1);
        assert_eq!(machine.stats.underfilled_continuation_pops, 1);

        let rank = desc.rank(plan.len());
        let level = machine
            .worklist
            .get(&rank)
            .expect("old cohort remains live");
        let old = level
            .values()
            .next()
            .expect("merged state retained its old payload");
        let StateBucket::Candidates(old) = old else {
            panic!("old cohort changed payload shape")
        };
        assert_eq!(old.parents.rows, [raw(10)]);
        assert_eq!(old.candidates, [(0, raw(1)), (0, raw(2)), (0, raw(3))]);
        assert!(machine
            .worklist
            .values()
            .flat_map(|level| level.keys())
            .any(|&id| machine.interner.get(id) == &ready_desc(3)));
    }

    #[test]
    fn probe_one_preserves_old_cold_tail_across_hit_miss_and_clone() {
        let root = ShapeLeaf(0);
        let plan = ResidualPlan::compile(&root);
        let desc = ready_desc(1);
        let mut machine = ResidualStateMachine::new(root.variables(), plan.len(), Search::Done);
        machine.width = 8;
        machine.cap = 64;

        file(
            &mut machine.worklist,
            &mut machine.interner,
            plan.len(),
            desc.clone(),
            StateBucket::Rows(RowBatch {
                rows: [10, 11, 12].map(raw).into(),
                row_count: 3,
            }),
            &mut machine.stats,
        );
        let first_hot = file(
            &mut machine.worklist,
            &mut machine.interner,
            plan.len(),
            desc.clone(),
            StateBucket::Rows(RowBatch {
                rows: [40, 41, 42, 43].map(raw).into(),
                row_count: 4,
            }),
            &mut machine.stats,
        )
        .expect("first hot receipt is nonempty");
        let second_hot = file(
            &mut machine.worklist,
            &mut machine.interner,
            plan.len(),
            desc.clone(),
            StateBucket::Rows(RowBatch {
                rows: [44, 45, 46, 47].map(raw).into(),
                row_count: 4,
            }),
            &mut machine.stats,
        )
        .expect("equal-key hot receipt is nonempty");
        let mut hot = None;
        prefer_continuation(&mut hot, Some(first_hot));
        prefer_continuation(&mut hot, Some(second_hot));
        let hot = hot.expect("equal-key hot receipts coalesce");
        assert_eq!(hot.rows, 8);
        machine.continuation = Some(ActiveContinuation::probe_one(hot));

        let mut missed = machine.clone();
        assert_eq!(missed.continuation, machine.continuation);
        let hit = machine.continuation.take().unwrap();
        let hit_width = machine.width;
        let hit_task = machine.take_continuation(&plan, hit, hit_width);
        let miss = missed.continuation.take().unwrap();
        let miss_width = missed.width;
        let miss_task = missed.take_continuation(&plan, miss, miss_width);
        for task in [hit_task, miss_task] {
            let StateBucket::Rows(rows) = task.bucket else {
                panic!("ready probe changed payload shape")
            };
            assert_eq!(rows.rows, [raw(47)]);
            assert_eq!(rows.row_count, 1);
        }

        // A hit returns the selected atom's ordered fanout to ordinary cohort
        // continuation. Only the original delta handoff is probed one-at-a-time.
        let successor = file(
            &mut machine.worklist,
            &mut machine.interner,
            plan.len(),
            desc,
            StateBucket::Rows(RowBatch {
                rows: [50, 51, 52].map(raw).into(),
                row_count: 3,
            }),
            &mut machine.stats,
        )
        .unwrap();
        let resumed = machine
            .continuation_after_advanced(&plan, machine.width, successor)
            .expect("the probe hit has an ordered successor");
        assert_eq!(resumed, ActiveContinuation::cohort(successor));
        let resumed = machine.take_continuation(&plan, resumed, machine.width);
        let StateBucket::Rows(resumed) = resumed.bucket else {
            panic!("probe successor changed payload shape")
        };
        assert_eq!(resumed.rows, [50, 51, 52].map(raw));
        assert_eq!(resumed.row_count, 3);
        assert_eq!(
            machine.stats.delta_handoff_probe_pops, 1,
            "the ordered successor cohort must not be probed again"
        );

        // A miss leaves the unprobed hot prefix merged with older work. The
        // next selection is ordinary global cold harvesting at width eight.
        let cold = missed.take_next_with_plan(&plan, missed.width).unwrap();
        let StateBucket::Rows(cold) = cold.bucket else {
            panic!("cold ready cohort changed payload shape")
        };
        assert_eq!(
            cold.rows,
            [12, 40, 41, 42, 43, 44, 45, 46]
                .map(raw)
                .into_iter()
                .collect::<Vec<_>>()
        );
        assert_eq!(cold.row_count, 8);
        assert_eq!(machine.width, 8);
        assert_eq!(missed.width, 8);
        assert_eq!(machine.stats.delta_handoff_probe_pops, 1);
        assert_eq!(missed.stats.delta_handoff_probe_pops, 1);
        assert_eq!(machine.stats.underfilled_continuation_pops, 1);
        assert_eq!(missed.stats.underfilled_continuation_pops, 0);
    }

    #[test]
    fn mixed_delta_feedback_arms_probe_without_widening() {
        let mut machine = ResidualStateMachine::new(VariableSet::new_singleton(0), 1, Search::Done);
        machine.width = 4;
        machine.cap = 64;
        let mut relevant = ChildSet::empty(1);
        relevant.insert(0);
        let (state, _) = machine.interner.intern_with_status(
            StateDesc {
                bound: VariableSet::new_empty(),
                phase: ResidualPhase::Candidate {
                    variable: 0,
                    relevant: relevant.clone(),
                    checked: ChildSet::empty(1),
                },
            },
            &mut machine.stats,
        );
        let token = ContinuationToken {
            rank: 7,
            state,
            rows: 2,
            candidates: 0,
        };

        machine.accept_delta_step(DeltaStepOutcome {
            continuation: Some(token),
            dead_pages: 2,
            source_dead_pages: 2,
            transition_dead_pages: 0,
            completed_activations: 0,
            completed_transition_cohort: false,
        });
        assert_eq!(machine.width, 4);
        assert_eq!(machine.stats.delta_source_negative_steps, 0);
        assert_eq!(
            machine.continuation,
            Some(ActiveContinuation::probe_one(token))
        );

        machine.accept_delta_step(DeltaStepOutcome {
            continuation: Some(token),
            dead_pages: 0,
            source_dead_pages: 0,
            transition_dead_pages: 0,
            completed_activations: 2,
            completed_transition_cohort: true,
        });
        assert_eq!(
            machine.continuation,
            Some(ActiveContinuation::cohort(token))
        );

        let (terminal_state, _) = machine.interner.intern_with_status(
            StateDesc {
                bound: VariableSet::new_empty(),
                phase: ResidualPhase::Candidate {
                    variable: 0,
                    relevant: relevant.clone(),
                    checked: relevant,
                },
            },
            &mut machine.stats,
        );
        let terminal = ContinuationToken {
            state: terminal_state,
            ..token
        };
        machine.accept_delta_step(DeltaStepOutcome {
            continuation: Some(terminal),
            dead_pages: 0,
            source_dead_pages: 0,
            transition_dead_pages: 0,
            completed_activations: 0,
            completed_transition_cohort: false,
        });
        assert_eq!(
            machine.continuation,
            Some(ActiveContinuation::cohort(terminal)),
            "fully checked candidates that bind the last variable emit directly"
        );

        machine.accept_delta_step(DeltaStepOutcome {
            continuation: None,
            dead_pages: 2,
            source_dead_pages: 2,
            transition_dead_pages: 0,
            completed_activations: 0,
            completed_transition_cohort: false,
        });
        assert_eq!(machine.width, 8);
        assert_eq!(machine.stats.delta_source_negative_steps, 1);
        assert!(machine.continuation.is_none());

        machine.accept_delta_step(DeltaStepOutcome {
            continuation: None,
            dead_pages: 0,
            source_dead_pages: 0,
            transition_dead_pages: 0,
            completed_activations: 1,
            completed_transition_cohort: false,
        });
        assert_eq!(machine.width, 8);
        assert_eq!(machine.delta.activation_width(), 2);
        assert_eq!(machine.stats.delta_activations_completed, 3);
        assert_eq!(machine.stats.delta_activation_width_increases, 1);
    }

    #[test]
    fn active_delta_seed_requires_one_parent_continuation_lineage() {
        let root = CapabilityLeaf {
            variable: 0,
            page_local: true,
        };
        let plan = ResidualPlan::compile(&root);
        let mut machine = ResidualStateMachine::new(root.variables(), plan.len(), Search::Done);
        let active = machine
            .delta
            .seed_source_proposals(
                DeltaDesc::leaf(0, 0),
                StateDesc {
                    bound: VariableSet::new_empty(),
                    phase: ResidualPhase::Ready,
                },
                RowBatch::seed(),
            )
            .expect("one cyclic source activation was filed");

        machine.last_selection = SelectionKind::Continuation(ContinuationMode::ProbeOne);
        machine.accept_delta_seed(None, Some(active), 1);
        assert_eq!(machine.active_delta, Some(active));

        machine.last_selection = SelectionKind::Continuation(ContinuationMode::Cohort);
        machine.accept_delta_seed(None, Some(active), 1);
        assert_eq!(
            machine.active_delta,
            Some(active),
            "the action after a one-atom planning probe is a singleton cohort"
        );
        machine.accept_delta_seed(None, Some(active), 512);
        assert!(
            machine.active_delta.is_none(),
            "a wide cohort must not pick an arbitrary last activation"
        );

        machine.last_selection = SelectionKind::Full;
        machine.accept_delta_seed(None, Some(active), 1);
        assert!(machine.active_delta.is_none());

        machine.last_selection = SelectionKind::Continuation(ContinuationMode::ProbeOne);
        let stable = file(
            &mut machine.worklist,
            &mut machine.interner,
            plan.len(),
            ready_desc(0),
            StateBucket::Rows(RowBatch::seed()),
            &mut machine.stats,
        )
        .expect("one stable seed effect was filed");
        machine.accept_delta_seed(Some(stable), Some(active), 1);
        assert!(
            machine.active_delta.is_none(),
            "an immediate stable effect is the activation's yield boundary"
        );
        assert_eq!(
            machine.continuation,
            Some(ActiveContinuation::probe_one(stable))
        );

        machine.continuation_sprint_enabled = false;
        machine.accept_delta_seed(None, Some(active), 1);
        assert!(
            machine.active_delta.is_none(),
            "the stable continuation ablation must also disable cyclic focus"
        );
    }

    #[test]
    fn full_action_successor_that_fills_width_returns_to_global_batching() {
        let root = CapabilityLeaf {
            variable: 127,
            page_local: true,
        };
        let plan = ResidualPlan::compile(&root);
        let mut machine = ResidualStateMachine::new(root.variables(), plan.len(), Search::Done);
        let successor = file(
            &mut machine.worklist,
            &mut machine.interner,
            plan.len(),
            ready_desc(1),
            ready_bucket(1, 2, 11),
            &mut machine.stats,
        )
        .unwrap();
        file(
            &mut machine.worklist,
            &mut machine.interner,
            plan.len(),
            ready_desc(2),
            ready_bucket(2, 2, 22),
            &mut machine.stats,
        );

        machine.last_selection = SelectionKind::Full;
        machine.last_was_action = true;
        assert_eq!(
            machine.continuation_after_advanced(&plan, 2, successor),
            None,
            "a width-filling successor must remain globally schedulable"
        );

        let task = machine
            .take_next_with_plan(&plan, 2)
            .expect("global work remains live");
        assert_eq!(task.desc, ready_desc(2));
        assert_eq!(task.bucket.row_count(), 2);
        assert_eq!(machine.stats.continuation_pops, 0);
    }

    #[test]
    fn no_full_bucket_drains_the_minimum_rank_even_if_a_deeper_bucket_is_larger() {
        let mut machine = scheduler_fixture(&[(1, 2, 1), (2, 7, 2), (3, 5, 3)]);

        let (desc, chunk) = machine.take_next(8).expect("fixture has live work");

        assert_eq!(desc, ready_desc(1));
        assert_eq!(chunk.row_count(), 2);
        assert_eq!(machine.stats.full_pops, 0);
        assert_eq!(machine.stats.readiness_pops, 1);
        assert_eq!(machine.stats.partial_pops, 0);
    }

    #[test]
    fn deepest_full_bucket_wins_over_deeper_underfill_and_shallower_surplus() {
        let mut machine = scheduler_fixture(&[(1, 16, 1), (2, 9, 2), (3, 8, 3), (4, 7, 4)]);

        let (desc, chunk) = machine.take_next(8).expect("fixture has live work");

        assert_eq!(desc, ready_desc(3));
        assert_eq!(chunk.row_count(), 8);
        assert_eq!(machine.stats.full_pops, 1);
        assert_eq!(machine.stats.readiness_pops, 0);
        assert_eq!(machine.stats.partial_pops, 0);
    }

    #[test]
    fn full_planner_remainder_runs_before_a_deeper_underfilled_action() {
        let mut machine = scheduler_fixture(&[(1, 4, 1)]);
        let mut relevant = ChildSet::empty(machine.leaf_count);
        relevant.insert(0);
        let propose = StateDesc {
            bound: ready_desc(1).bound,
            phase: ResidualPhase::Propose {
                variable: 127,
                relevant,
                proposer: 0,
            },
        };
        file(
            &mut machine.worklist,
            &mut machine.interner,
            machine.leaf_count,
            propose.clone(),
            ready_bucket(1, 1, 2),
            &mut machine.stats,
        );

        for _ in 0..2 {
            let (desc, chunk) = machine.take_next(2).expect("fixture has live work");
            assert_eq!(desc, ready_desc(1));
            assert_eq!(chunk.row_count(), 2);
        }
        let (desc, chunk) = machine.take_next(2).expect("action remains live");
        assert_eq!(desc, propose);
        assert_eq!(chunk.row_count(), 1);
        assert_eq!(machine.stats.full_pops, 2);
        assert_eq!(machine.stats.readiness_pops, 1);
        assert_eq!(machine.stats.partial_pops, 1);
    }

    #[test]
    fn readiness_ties_use_the_same_highest_state_id_rule_as_full_ties() {
        let mut machine = ResidualStateMachine::new(VariableSet::new_empty(), 1, Search::Done);
        let mut first_bound = VariableSet::new_empty();
        first_bound.set(0);
        let first = StateDesc {
            bound: first_bound,
            phase: ResidualPhase::Ready,
        };
        let mut second_bound = VariableSet::new_empty();
        second_bound.set(1);
        let second = StateDesc {
            bound: second_bound,
            phase: ResidualPhase::Ready,
        };
        for (desc, marker) in [(first, 1), (second.clone(), 2)] {
            file(
                &mut machine.worklist,
                &mut machine.interner,
                machine.leaf_count,
                desc,
                ready_bucket(1, 1, marker),
                &mut machine.stats,
            );
        }

        let (desc, chunk) = machine.take_next(2).expect("fixture has live work");

        assert_eq!(desc, second);
        assert_eq!(chunk.row_count(), 1);
        assert_eq!(machine.stats.readiness_pops, 1);
    }

    #[test]
    fn confirm_occupancy_counts_whole_parents_not_ragged_candidates() {
        fn confirm_desc() -> StateDesc {
            let mut relevant = ChildSet::empty(2);
            relevant.insert(0);
            relevant.insert(1);
            let mut checked = ChildSet::empty(2);
            checked.insert(0);
            StateDesc {
                bound: ready_desc(1).bound,
                phase: ResidualPhase::Confirm {
                    variable: 127,
                    relevant,
                    checked,
                    confirmer: 1,
                },
            }
        }

        fn candidate_bucket(parent_count: usize) -> StateBucket {
            let mut candidates = vec![(0, raw(9)); 64];
            if parent_count == 2 {
                candidates.push((1, raw(10)));
            }
            StateBucket::Candidates(CandidateBatch {
                parents: RowBatch {
                    rows: vec![raw(3); parent_count],
                    row_count: parent_count,
                },
                candidates: candidate_payload(parent_count, candidates),
            })
        }

        let mut underfilled = ResidualStateMachine::new(VariableSet::new_empty(), 2, Search::Done);
        for (desc, bucket) in [
            (ready_desc(1), ready_bucket(1, 2, 1)),
            (confirm_desc(), candidate_bucket(1)),
        ] {
            file(
                &mut underfilled.worklist,
                &mut underfilled.interner,
                underfilled.leaf_count,
                desc,
                bucket,
                &mut underfilled.stats,
            );
        }

        let (desc, chunk) = underfilled.take_next(2).expect("ready bucket is full");
        assert_eq!(desc, ready_desc(1));
        assert_eq!(chunk.row_count(), 2);
        let (desc, chunk) = underfilled
            .take_next(2)
            .expect("underfilled confirmation remains live");
        assert_eq!(desc, confirm_desc());
        match chunk {
            StateBucket::Candidates(batch) => {
                assert_eq!(batch.parents.row_count, 1);
                assert_eq!(batch.candidates.len(), 64);
                assert!(batch.candidates.is_values());
                assert!(batch.candidates.iter().all(|(parent, _)| parent == 0));
            }
            StateBucket::Rows(_) | StateBucket::Formula(_) => {
                panic!("confirmation returned a non-candidate payload")
            }
        }

        let mut full = ResidualStateMachine::new(VariableSet::new_empty(), 2, Search::Done);
        for (desc, bucket) in [
            (ready_desc(1), ready_bucket(1, 2, 1)),
            (confirm_desc(), candidate_bucket(2)),
        ] {
            file(
                &mut full.worklist,
                &mut full.interner,
                full.leaf_count,
                desc,
                bucket,
                &mut full.stats,
            );
        }

        let (desc, chunk) = full.take_next(2).expect("confirmation bucket is full");
        assert_eq!(desc, confirm_desc());
        match chunk {
            StateBucket::Candidates(batch) => {
                assert_eq!(batch.parents.row_count, 2);
                assert_eq!(batch.candidates.len(), 65);
                assert_eq!(
                    batch
                        .candidates
                        .tagged_snapshot()
                        .last()
                        .map(|(parent, _)| *parent),
                    Some(1)
                );
            }
            StateBucket::Rows(_) | StateBucket::Formula(_) => {
                panic!("confirmation returned a non-candidate payload")
            }
        }
    }

    #[test]
    fn ready_planning_pop_delays_the_proposal_verb_until_action_pop() {
        let proposes = Arc::new(AtomicUsize::new(0));
        let confirms = Arc::new(AtomicUsize::new(0));
        let root = IntersectionConstraint::new(vec![VerbLeaf {
            variable: 0,
            estimate: 1,
            accepts: true,
            proposes: Arc::clone(&proposes),
            confirms,
        }]);
        let plan = ResidualPlan::compile(&root);
        let mut machine =
            ResidualStateMachine::new(root.variables(), plan.len(), Search::NextVariable);
        machine.cap = 1;
        let influences = [VariableSet::new_empty(); 128];
        let base_estimates = [1; 128];

        assert!(matches!(
            machine.pop_once(&root, &plan, &influences, &base_estimates, 1),
            MachineStep::Stable(StepOutcome::Advanced(_))
        ));
        assert_eq!(machine.stats.ready_plan_pops, 1);
        assert_eq!(machine.stats.ready_preferred_variable_groups, 1);
        assert_eq!(machine.stats.ready_scheduled_variable_groups, 1);
        assert_eq!(machine.stats.ready_proposal_groups, 1);
        assert_eq!(machine.stats.agglomerated_ready_pops, 0);
        assert_eq!(machine.stats.propose_action_pops, 0);
        assert_eq!(machine.stats.propose_calls, 0);
        assert_eq!(proposes.load(Ordering::Relaxed), 0);

        assert!(matches!(
            machine.pop_once(&root, &plan, &influences, &base_estimates, 1),
            MachineStep::Stable(StepOutcome::Advanced(_))
        ));
        assert_eq!(machine.stats.propose_action_pops, 1);
        assert_eq!(machine.stats.propose_calls, 1);
        assert_eq!(proposes.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn successor_merge_into_an_already_live_state_is_advanced() {
        let proposes = Arc::new(AtomicUsize::new(0));
        let root = IntersectionConstraint::new(vec![VerbLeaf {
            variable: 0,
            estimate: 1,
            accepts: true,
            proposes: Arc::clone(&proposes),
            confirms: Arc::new(AtomicUsize::new(0)),
        }]);
        let plan = ResidualPlan::compile(&root);
        let mut relevant = ChildSet::empty(plan.len());
        relevant.insert(0);
        let mut checked = ChildSet::empty(plan.len());
        checked.insert(0);
        let candidate_desc = StateDesc {
            bound: VariableSet::new_empty(),
            phase: ResidualPhase::Candidate {
                variable: 0,
                relevant: relevant.clone(),
                checked,
            },
        };
        let mut worklist = Worklist::new();
        let mut interner = StateInterner::default();
        let mut stats = ResidualStateStats::default();
        assert!(file(
            &mut worklist,
            &mut interner,
            plan.len(),
            candidate_desc,
            StateBucket::Candidates(CandidateBatch {
                parents: RowBatch::seed(),
                candidates: CandidatePayload::Values(vec![raw(7)]),
            }),
            &mut stats,
        )
        .is_some());
        let propose_desc = StateDesc {
            bound: VariableSet::new_empty(),
            phase: ResidualPhase::Propose {
                variable: 0,
                relevant,
                proposer: 0,
            },
        };

        let outcome = execute_state(
            &root,
            &plan,
            &propose_desc,
            StateBucket::Rows(RowBatch::seed()),
            root.variables(),
            &[VariableSet::new_empty(); 128],
            &[1; 128],
            &mut worklist,
            &mut interner,
            &mut stats,
        );

        assert!(matches!(outcome, StepOutcome::Advanced(_)));
        assert_eq!(stats.bucket_merges, 1);
        assert_eq!(stats.rows_merged, 1);
        assert_eq!(stats.dead_action_pops, 0);
        assert_eq!(proposes.load(Ordering::Relaxed), 1);
        let (_, level) = worklist.first_key_value().expect("candidate remains live");
        assert_eq!(level.len(), 1);
        assert_eq!(level.first_key_value().unwrap().1.row_count(), 2);
    }

    #[test]
    fn action_with_partial_parent_survival_is_advanced() {
        const PARENT: VariableId = 0;
        const VARIABLE: VariableId = 1;
        let root = IntersectionConstraint::new(vec![FirstParentProposer {
            parent: PARENT,
            variable: VARIABLE,
        }]);
        let plan = ResidualPlan::compile(&root);
        let mut relevant = ChildSet::empty(plan.len());
        relevant.insert(0);
        let desc = StateDesc {
            bound: VariableSet::new_singleton(PARENT),
            phase: ResidualPhase::Propose {
                variable: VARIABLE,
                relevant,
                proposer: 0,
            },
        };
        let mut worklist = Worklist::new();
        let mut interner = StateInterner::default();
        let mut stats = ResidualStateStats::default();

        let outcome = execute_state(
            &root,
            &plan,
            &desc,
            StateBucket::Rows(RowBatch {
                rows: vec![raw(10), raw(11)],
                row_count: 2,
            }),
            root.variables(),
            &[VariableSet::new_empty(); 128],
            &[1; 128],
            &mut worklist,
            &mut interner,
            &mut stats,
        );

        assert!(matches!(outcome, StepOutcome::Advanced(_)));
        assert_eq!(stats.dead_action_pops, 0);
        assert_eq!((stats.propose_rows, stats.max_propose_rows), (2, 2));
        let (_, level) = worklist.first_key_value().expect("one parent survived");
        let bucket = level.first_key_value().unwrap().1;
        assert_eq!(bucket.row_count(), 1);
        let StateBucket::Candidates(batch) = bucket else {
            panic!("partial proposal did not file candidates")
        };
        assert_eq!(batch.parents.rows, [raw(10)]);
        assert_eq!(batch.candidates, [(0, raw(42))]);
    }

    #[test]
    fn width_increases_count_only_numeric_growth_before_saturation() {
        let mut machine = ResidualStateMachine::new(VariableSet::new_empty(), 0, Search::Done);
        machine.width = 1;
        machine.growth = 1;
        machine.cap = 4;
        machine.increase_width();
        assert_eq!((machine.width, machine.stats.width_increases), (1, 0));

        machine.growth = 2;
        machine.increase_width();
        assert_eq!((machine.width, machine.stats.width_increases), (2, 1));
        machine.increase_width();
        assert_eq!((machine.width, machine.stats.width_increases), (4, 2));
        machine.increase_width();
        assert_eq!((machine.width, machine.stats.width_increases), (4, 2));
    }

    #[test]
    fn candidate_planning_pop_delays_confirmation_until_action_pop() {
        let proposes = Arc::new(AtomicUsize::new(0));
        let confirms = Arc::new(AtomicUsize::new(0));
        let root = IntersectionConstraint::new(vec![
            VerbLeaf {
                variable: 0,
                estimate: 1,
                accepts: true,
                proposes: Arc::clone(&proposes),
                confirms: Arc::clone(&confirms),
            },
            VerbLeaf {
                variable: 0,
                estimate: 2,
                accepts: true,
                proposes,
                confirms: Arc::clone(&confirms),
            },
        ]);
        let plan = ResidualPlan::compile(&root);
        let mut relevant = ChildSet::empty(plan.len());
        relevant.insert(0);
        relevant.insert(1);
        let mut checked = ChildSet::empty(plan.len());
        checked.insert(0);
        let candidate_desc = StateDesc {
            bound: VariableSet::new_empty(),
            phase: ResidualPhase::Candidate {
                variable: 0,
                relevant,
                checked,
            },
        };
        let candidate_bucket = StateBucket::Candidates(CandidateBatch {
            parents: RowBatch::seed(),
            candidates: CandidatePayload::Values(vec![raw(1)]),
        });
        let mut worklist = Worklist::new();
        let mut interner = StateInterner::default();
        let mut stats = ResidualStateStats::default();
        let influences = [VariableSet::new_empty(); 128];
        let base_estimates = [1; 128];

        assert!(matches!(
            execute_state(
                &root,
                &plan,
                &candidate_desc,
                candidate_bucket,
                root.variables(),
                &influences,
                &base_estimates,
                &mut worklist,
                &mut interner,
                &mut stats,
            ),
            StepOutcome::Advanced(_)
        ));
        assert_eq!(stats.candidate_plan_pops, 1);
        assert_eq!(stats.confirm_action_pops, 0);
        assert_eq!(stats.confirm_calls, 0);
        assert_eq!(confirms.load(Ordering::Relaxed), 0);

        let (&rank, _) = worklist
            .first_key_value()
            .expect("confirm action was filed");
        let mut level = worklist.remove(&rank).unwrap();
        let (id, bucket) = level.pop_first().unwrap();
        assert!(level.is_empty());
        let action_desc = interner.get(id).clone();
        assert!(matches!(
            execute_state(
                &root,
                &plan,
                &action_desc,
                bucket,
                root.variables(),
                &influences,
                &base_estimates,
                &mut worklist,
                &mut interner,
                &mut stats,
            ),
            StepOutcome::Advanced(_)
        ));
        assert_eq!(stats.confirm_action_pops, 1);
        assert_eq!(stats.confirm_calls, 1);
        assert_eq!(confirms.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn fully_checked_single_leaf_candidate_commits_to_ready_without_a_verb() {
        let proposes = Arc::new(AtomicUsize::new(0));
        let confirms = Arc::new(AtomicUsize::new(0));
        let root = IntersectionConstraint::new(vec![VerbLeaf {
            variable: 0,
            estimate: 1,
            accepts: true,
            proposes: Arc::clone(&proposes),
            confirms: Arc::clone(&confirms),
        }]);
        let plan = ResidualPlan::compile(&root);
        let mut relevant = ChildSet::empty(plan.len());
        relevant.insert(0);
        let desc = StateDesc {
            bound: VariableSet::new_empty(),
            phase: ResidualPhase::Candidate {
                variable: 0,
                relevant: relevant.clone(),
                checked: relevant,
            },
        };
        let bucket = StateBucket::Candidates(CandidateBatch {
            parents: RowBatch::seed(),
            candidates: CandidatePayload::Values(vec![raw(7)]),
        });
        let mut worklist = Worklist::new();
        let mut interner = StateInterner::default();
        let mut stats = ResidualStateStats::default();
        let influences = [VariableSet::new_empty(); 128];
        let base_estimates = [1; 128];

        assert!(matches!(
            execute_state(
                &root,
                &plan,
                &desc,
                bucket,
                root.variables(),
                &influences,
                &base_estimates,
                &mut worklist,
                &mut interner,
                &mut stats,
            ),
            StepOutcome::Advanced(_)
        ));
        assert_eq!(stats.candidate_plan_pops, 1);
        assert_eq!(stats.propose_calls, 0);
        assert_eq!(stats.confirm_calls, 0);
        assert_eq!(proposes.load(Ordering::Relaxed), 0);
        assert_eq!(confirms.load(Ordering::Relaxed), 0);

        let (_, level) = worklist.first_key_value().expect("Ready state was filed");
        let (&id, payload) = level.first_key_value().unwrap();
        assert_eq!(
            interner.get(id),
            &StateDesc {
                bound: VariableSet::new_singleton(0),
                phase: ResidualPhase::Ready,
            }
        );
        let StateBucket::Rows(rows) = payload else {
            panic!("committed candidate did not become a row payload")
        };
        assert_eq!((rows.row_count, rows.rows.as_slice()), (1, &[raw(7)][..]));
    }

    #[test]
    fn confirmation_action_that_rejects_every_candidate_files_no_successor() {
        let proposes = Arc::new(AtomicUsize::new(0));
        let confirms = Arc::new(AtomicUsize::new(0));
        let root = IntersectionConstraint::new(vec![
            VerbLeaf {
                variable: 0,
                estimate: 1,
                accepts: true,
                proposes: Arc::clone(&proposes),
                confirms: Arc::clone(&confirms),
            },
            VerbLeaf {
                variable: 0,
                estimate: 2,
                accepts: false,
                proposes,
                confirms: Arc::clone(&confirms),
            },
        ]);
        let plan = ResidualPlan::compile(&root);
        let mut relevant = ChildSet::empty(plan.len());
        relevant.insert(0);
        relevant.insert(1);
        let mut checked = ChildSet::empty(plan.len());
        checked.insert(0);
        let candidate_desc = StateDesc {
            bound: VariableSet::new_empty(),
            phase: ResidualPhase::Candidate {
                variable: 0,
                relevant,
                checked,
            },
        };
        let candidate_bucket = StateBucket::Candidates(CandidateBatch {
            parents: RowBatch::seed(),
            candidates: CandidatePayload::Values(vec![raw(1)]),
        });
        let mut worklist = Worklist::new();
        let mut interner = StateInterner::default();
        let mut stats = ResidualStateStats::default();
        let influences = [VariableSet::new_empty(); 128];
        let base_estimates = [1; 128];

        assert!(matches!(
            execute_state(
                &root,
                &plan,
                &candidate_desc,
                candidate_bucket,
                root.variables(),
                &influences,
                &base_estimates,
                &mut worklist,
                &mut interner,
                &mut stats,
            ),
            StepOutcome::Advanced(_)
        ));
        let (&rank, _) = worklist
            .first_key_value()
            .expect("confirm action was filed");
        let mut level = worklist.remove(&rank).unwrap();
        let (id, bucket) = level.pop_first().unwrap();
        let action_desc = interner.get(id).clone();
        assert!(matches!(
            execute_state(
                &root,
                &plan,
                &action_desc,
                bucket,
                root.variables(),
                &influences,
                &base_estimates,
                &mut worklist,
                &mut interner,
                &mut stats,
            ),
            StepOutcome::Dead
        ));

        assert!(worklist.is_empty());
        assert_eq!(stats.candidate_plan_pops, 1);
        assert_eq!(stats.confirm_action_pops, 1);
        assert_eq!(stats.dead_action_pops, 1);
        assert_eq!(stats.confirm_calls, 1);
        assert_eq!(confirms.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn separately_planned_candidate_chunks_merge_uniform_confirm_actions() {
        const PARENT: VariableId = 0;
        const VARIABLE: VariableId = 1;
        let proposer_calls = Arc::new(AtomicUsize::new(0));
        let proposer_confirms = Arc::new(AtomicUsize::new(0));
        let even_calls = Arc::new(AtomicUsize::new(0));
        let even_rows = Arc::new(AtomicUsize::new(0));
        let odd_calls = Arc::new(AtomicUsize::new(0));
        let odd_rows = Arc::new(AtomicUsize::new(0));
        let root = IntersectionConstraint::new(vec![
            Box::new(VerbLeaf {
                variable: VARIABLE,
                estimate: 0,
                accepts: true,
                proposes: proposer_calls,
                confirms: proposer_confirms,
            }) as ShapeConstraint,
            Box::new(StripedConfirmer {
                variable: VARIABLE,
                parent: PARENT,
                parity: 0,
                calls: Arc::clone(&even_calls),
                rows: Arc::clone(&even_rows),
            }) as ShapeConstraint,
            Box::new(StripedConfirmer {
                variable: VARIABLE,
                parent: PARENT,
                parity: 1,
                calls: Arc::clone(&odd_calls),
                rows: Arc::clone(&odd_rows),
            }) as ShapeConstraint,
        ]);
        let plan = ResidualPlan::compile(&root);
        let mut relevant = ChildSet::empty(plan.len());
        relevant.insert(0);
        relevant.insert(1);
        relevant.insert(2);
        let mut checked = ChildSet::empty(plan.len());
        checked.insert(0);
        let desc = StateDesc {
            bound: VariableSet::new_singleton(PARENT),
            phase: ResidualPhase::Candidate {
                variable: VARIABLE,
                relevant,
                checked,
            },
        };
        let chunk = |first_parent: u8| {
            StateBucket::Candidates(CandidateBatch {
                parents: RowBatch {
                    rows: vec![raw(first_parent), raw(first_parent + 1)],
                    row_count: 2,
                },
                candidates: candidate_payload(2, vec![(0, raw(10)), (1, raw(11))]),
            })
        };
        let mut worklist = Worklist::new();
        let mut interner = StateInterner::default();
        let mut stats = ResidualStateStats::default();
        let influences = [VariableSet::new_empty(); 128];
        let base_estimates = [1; 128];

        for first_parent in [0, 2] {
            assert!(matches!(
                execute_state(
                    &root,
                    &plan,
                    &desc,
                    chunk(first_parent),
                    root.variables(),
                    &influences,
                    &base_estimates,
                    &mut worklist,
                    &mut interner,
                    &mut stats,
                ),
                StepOutcome::Advanced(_)
            ));
        }
        assert_eq!(stats.candidate_plan_pops, 2);
        assert_eq!(stats.candidate_confirmation_groups, 4);
        assert_eq!(stats.confirm_calls, 0);
        assert_eq!(even_calls.load(Ordering::Relaxed), 0);
        assert_eq!(odd_calls.load(Ordering::Relaxed), 0);

        let (&rank, level) = worklist
            .first_key_value()
            .expect("striped Confirm actions were filed");
        assert_eq!(level.len(), 2);
        assert!(level.values().all(|bucket| bucket.row_count() == 2));
        assert_eq!((stats.bucket_merges, stats.rows_merged), (2, 2));

        let level = worklist.remove(&rank).unwrap();
        for (id, bucket) in level {
            let action_desc = interner.get(id).clone();
            assert!(matches!(
                execute_state(
                    &root,
                    &plan,
                    &action_desc,
                    bucket,
                    root.variables(),
                    &influences,
                    &base_estimates,
                    &mut worklist,
                    &mut interner,
                    &mut stats,
                ),
                StepOutcome::Advanced(_)
            ));
        }
        assert_eq!(stats.confirm_action_pops, 2);
        assert_eq!(stats.confirm_calls, 2);
        assert_eq!(
            (
                even_calls.load(Ordering::Relaxed),
                even_rows.load(Ordering::Relaxed),
                odd_calls.load(Ordering::Relaxed),
                odd_rows.load(Ordering::Relaxed),
            ),
            (1, 2, 1, 2)
        );
    }

    #[test]
    fn child_sets_do_not_alias_across_the_u128_boundary() {
        let mut set = ChildSet::empty(129);
        set.insert(0);
        set.insert(128);
        assert!(set.contains(0));
        assert!(set.contains(128));
        assert_eq!(set.count(), 2);
    }

    #[test]
    fn interner_collapses_order_independent_checked_sets() {
        let mut left_checked = ChildSet::empty(3);
        left_checked.insert(0);
        left_checked.insert(1);
        let mut right_checked = ChildSet::empty(3);
        right_checked.insert(1);
        right_checked.insert(0);
        let relevant = {
            let mut set = ChildSet::empty(3);
            set.insert(0);
            set.insert(1);
            set.insert(2);
            set
        };
        let desc = |checked| StateDesc {
            bound: VariableSet::new_singleton(7),
            phase: ResidualPhase::Candidate {
                variable: 9,
                relevant: relevant.clone(),
                checked,
            },
        };
        let mut stats = ResidualStateStats::default();
        let mut interner = StateInterner::default();
        let left = interner
            .intern_with_status(desc(left_checked), &mut stats)
            .0;
        let right = interner
            .intern_with_status(desc(right_checked), &mut stats)
            .0;
        assert_eq!(left, right);
        assert_eq!(stats.states_interned, 1);
        assert_eq!(stats.interner_hits, 1);
    }

    #[test]
    fn interner_ids_follow_insertion_order_across_hashers_and_clones() {
        let first = ready_desc(0);
        let second = ready_desc(1);
        let third = ready_desc(2);
        let mut stats = ResidualStateStats::default();
        let mut interner = StateInterner::default();

        assert_eq!(
            interner.intern_with_status(first.clone(), &mut stats),
            (StateId(0), false)
        );
        assert_eq!(
            interner.intern_with_status(second.clone(), &mut stats),
            (StateId(1), false)
        );
        assert_eq!(
            interner.intern_with_status(first.clone(), &mut stats),
            (StateId(0), true)
        );
        assert_eq!(interner.get(StateId(0)), &first);
        assert_eq!(interner.get(StateId(1)), &second);

        let mut cloned = interner.clone();
        let mut cloned_stats = ResidualStateStats::default();
        assert_eq!(cloned.get(StateId(0)), &first);
        assert_eq!(cloned.get(StateId(1)), &second);
        assert_eq!(
            interner.intern_with_status(third.clone(), &mut stats),
            (StateId(2), false)
        );
        assert_eq!(
            cloned.intern_with_status(third.clone(), &mut cloned_stats),
            (StateId(2), false)
        );

        // A fresh randomized hasher must produce the same insertion-order IDs.
        let mut fresh = StateInterner::default();
        let mut fresh_stats = ResidualStateStats::default();
        assert_eq!(
            fresh.intern_with_status(first, &mut fresh_stats),
            (StateId(0), false)
        );
        assert_eq!(
            fresh.intern_with_status(second, &mut fresh_stats),
            (StateId(1), false)
        );
        assert_eq!(
            fresh.intern_with_status(third, &mut fresh_stats),
            (StateId(2), false)
        );
        assert_eq!(stats.states_interned, 3);
        assert_eq!(stats.interner_hits, 1);
    }

    #[test]
    fn state_identity_includes_every_future_computation_dimension() {
        let mut relevant = ChildSet::empty(3);
        relevant.insert(0);
        relevant.insert(1);
        let mut checked = ChildSet::empty(3);
        checked.insert(0);
        let mut relevant_all = relevant.clone();
        relevant_all.insert(2);
        let candidate = StateDesc {
            bound: VariableSet::new_singleton(2),
            phase: ResidualPhase::Candidate {
                variable: 4,
                relevant: relevant.clone(),
                checked: checked.clone(),
            },
        };
        let variants = vec![
            StateDesc {
                bound: VariableSet::new_singleton(3),
                ..candidate.clone()
            },
            StateDesc {
                phase: ResidualPhase::Candidate {
                    variable: 5,
                    relevant: relevant.clone(),
                    checked: checked.clone(),
                },
                ..candidate.clone()
            },
            StateDesc {
                phase: ResidualPhase::Candidate {
                    variable: 4,
                    relevant: relevant_all.clone(),
                    checked: checked.clone(),
                },
                ..candidate.clone()
            },
            StateDesc {
                phase: ResidualPhase::Candidate {
                    variable: 4,
                    relevant: relevant.clone(),
                    checked: {
                        let mut other = ChildSet::empty(3);
                        other.insert(1);
                        other
                    },
                },
                ..candidate.clone()
            },
            StateDesc {
                phase: ResidualPhase::Ready,
                ..candidate.clone()
            },
            StateDesc {
                phase: ResidualPhase::Propose {
                    variable: 4,
                    relevant: relevant.clone(),
                    proposer: 0,
                },
                ..candidate.clone()
            },
            StateDesc {
                phase: ResidualPhase::Propose {
                    variable: 4,
                    relevant: relevant.clone(),
                    proposer: 1,
                },
                ..candidate.clone()
            },
            StateDesc {
                phase: ResidualPhase::Propose {
                    variable: 4,
                    relevant: relevant_all.clone(),
                    proposer: 0,
                },
                ..candidate.clone()
            },
            StateDesc {
                phase: ResidualPhase::Confirm {
                    variable: 4,
                    relevant: relevant.clone(),
                    checked: checked.clone(),
                    confirmer: 1,
                },
                ..candidate.clone()
            },
            StateDesc {
                phase: ResidualPhase::Confirm {
                    variable: 4,
                    relevant: relevant_all.clone(),
                    checked: checked.clone(),
                    confirmer: 1,
                },
                ..candidate.clone()
            },
            StateDesc {
                phase: ResidualPhase::Confirm {
                    variable: 4,
                    relevant: relevant_all,
                    checked,
                    confirmer: 2,
                },
                ..candidate.clone()
            },
        ];

        let mut stats = ResidualStateStats::default();
        let mut interner = StateInterner::default();
        let original = interner.intern_with_status(candidate, &mut stats).0;
        for variant in variants {
            assert_ne!(original, interner.intern_with_status(variant, &mut stats).0);
        }
        assert_eq!(stats.states_interned, 12);
        assert_eq!(stats.interner_hits, 0);
    }

    #[test]
    fn action_ranks_are_history_independent_and_strictly_increase() {
        let leaf_count = 4;
        let bound = VariableSet::new_singleton(1);
        let mut relevant = ChildSet::empty(leaf_count);
        relevant.insert(0);
        relevant.insert(1);
        relevant.insert(2);
        let mut checked_a = ChildSet::empty(leaf_count);
        checked_a.insert(0);
        let mut checked_b = ChildSet::empty(leaf_count);
        checked_b.insert(1);
        let checked_ab = checked_a.with_inserted(1);

        let ready = StateDesc {
            bound,
            phase: ResidualPhase::Ready,
        };
        let propose = StateDesc {
            bound,
            phase: ResidualPhase::Propose {
                variable: 3,
                relevant: relevant.clone(),
                proposer: 0,
            },
        };
        let candidate = |checked| StateDesc {
            bound,
            phase: ResidualPhase::Candidate {
                variable: 3,
                relevant: relevant.clone(),
                checked,
            },
        };
        let confirm = |checked, confirmer| StateDesc {
            bound,
            phase: ResidualPhase::Confirm {
                variable: 3,
                relevant: relevant.clone(),
                checked,
                confirmer,
            },
        };

        // S = 2(L + 1) = 10. The action grades interleave planning
        // states, so every concrete transition raises rank by exactly one
        // until a complete candidate jumps to the next binding schema.
        assert_eq!(ready.rank(leaf_count), 10);
        assert_eq!(propose.rank(leaf_count), 11);
        assert_eq!(candidate(checked_a.clone()).rank(leaf_count), 12);
        assert_eq!(candidate(checked_b).rank(leaf_count), 12);
        assert_eq!(confirm(checked_a, 1).rank(leaf_count), 13);
        assert_eq!(candidate(checked_ab.clone()).rank(leaf_count), 14);
        assert_eq!(confirm(checked_ab.clone(), 2).rank(leaf_count), 15);

        let full_candidate = candidate(checked_ab.with_inserted(2));
        assert_eq!(full_candidate.rank(leaf_count), 16);
        let next_ready = StateDesc {
            bound: bound.union(VariableSet::new_singleton(3)),
            phase: ResidualPhase::Ready,
        };
        assert_eq!(next_ready.rank(leaf_count), 20);
        assert!(full_candidate.rank(leaf_count) < next_ready.rank(leaf_count));
    }

    #[test]
    fn action_descriptors_reject_noncanonical_child_sets() {
        let leaf_count = 3;
        let bound = VariableSet::new_singleton(0);
        let mut relevant = ChildSet::empty(leaf_count);
        relevant.insert(0);
        relevant.insert(1);
        let mut checked = ChildSet::empty(leaf_count);
        checked.insert(0);

        let irrelevant_proposer = StateDesc {
            bound,
            phase: ResidualPhase::Propose {
                variable: 1,
                relevant: relevant.clone(),
                proposer: 2,
            },
        };
        assert!(std::panic::catch_unwind(|| irrelevant_proposer.rank(leaf_count)).is_err());

        let mut outside = checked.clone();
        outside.insert(2);
        let checked_outside_relevant = StateDesc {
            bound,
            phase: ResidualPhase::Candidate {
                variable: 1,
                relevant: relevant.clone(),
                checked: outside,
            },
        };
        assert!(std::panic::catch_unwind(|| checked_outside_relevant.rank(leaf_count)).is_err());

        let already_checked_confirmer = StateDesc {
            bound,
            phase: ResidualPhase::Confirm {
                variable: 1,
                relevant,
                checked,
                confirmer: 0,
            },
        };
        assert!(std::panic::catch_unwind(|| already_checked_confirmer.rank(leaf_count)).is_err());

        let mut non_leaf_relevant = ChildSet::empty(leaf_count);
        non_leaf_relevant.0[0] |= 1 << 63;
        let non_leaf_proposer_set = StateDesc {
            bound,
            phase: ResidualPhase::Propose {
                variable: 1,
                relevant: non_leaf_relevant,
                proposer: 0,
            },
        };
        assert!(std::panic::catch_unwind(|| non_leaf_proposer_set.rank(leaf_count)).is_err());
    }
}
