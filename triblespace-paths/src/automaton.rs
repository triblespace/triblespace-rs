use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;

use triblespace_core::id::RawId;

/// Dense state number in a fixed automaton.
pub type StateId = u32;

/// One epsilon-free property-path step.
///
/// `ForwardExcept([])` and `ReverseExcept([])` are wildcards. Exclusion lists
/// are sorted and deduplicated by [`Automaton::new`], making automaton equality
/// a canonical merge-compatibility check.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum Step {
    /// Follow an edge with exactly this attribute.
    Forward(RawId),
    /// Follow an edge with exactly this attribute in reverse.
    Reverse(RawId),
    /// Follow any forward edge except the listed attributes.
    ForwardExcept(Vec<RawId>),
    /// Follow any reverse edge except the listed attributes.
    ReverseExcept(Vec<RawId>),
}

impl Step {
    /// Matches every forward edge.
    pub fn forward_any() -> Self {
        Self::ForwardExcept(Vec::new())
    }

    /// Matches every reverse edge.
    pub fn reverse_any() -> Self {
        Self::ReverseExcept(Vec::new())
    }

    pub(crate) fn canonicalize(&mut self) {
        let excluded = match self {
            Self::ForwardExcept(excluded) | Self::ReverseExcept(excluded) => excluded,
            Self::Forward(_) | Self::Reverse(_) => return,
        };
        excluded.sort_unstable();
        excluded.dedup();
    }

    pub(crate) fn matches(&self, attribute: &RawId) -> bool {
        match self {
            Self::Forward(expected) | Self::Reverse(expected) => expected == attribute,
            Self::ForwardExcept(excluded) | Self::ReverseExcept(excluded) => {
                excluded.binary_search(attribute).is_err()
            }
        }
    }

    pub(crate) fn is_reverse(&self) -> bool {
        matches!(self, Self::Reverse(_) | Self::ReverseExcept(_))
    }
}

/// A labeled transition between two automaton states.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct Transition {
    /// Source state.
    pub from: StateId,
    /// Destination state.
    pub to: StateId,
    /// Graph step consumed by the transition.
    pub step: Step,
}

impl Transition {
    /// Creates one transition.
    pub fn new(from: StateId, to: StateId, step: Step) -> Self {
        Self { from, to, step }
    }
}

/// A canonical fixed, epsilon-free nondeterministic automaton.
///
/// Epsilon behavior belongs in the compiler: nullable expressions are encoded
/// by accepting an initial state, and epsilon transitions are eliminated before
/// constructing this value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Automaton {
    state_count: StateId,
    initial: BTreeSet<StateId>,
    accepting: BTreeSet<StateId>,
    transitions: Vec<Transition>,
}

/// Invalid fixed-automaton description.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AutomatonError {
    /// An automaton needs at least one state.
    NoStates,
    /// An automaton needs at least one initial state.
    NoInitialState,
    /// A referenced state lies outside `0..state_count`.
    StateOutOfRange {
        /// Invalid state.
        state: StateId,
        /// Exclusive state bound.
        state_count: StateId,
    },
}

impl fmt::Display for AutomatonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoStates => write!(f, "an automaton needs at least one state"),
            Self::NoInitialState => write!(f, "an automaton needs an initial state"),
            Self::StateOutOfRange { state, state_count } => {
                write!(f, "automaton state {state} is outside 0..{state_count}")
            }
        }
    }
}

impl Error for AutomatonError {}

impl Automaton {
    /// Validates and canonicalizes a fixed automaton.
    pub fn new(
        state_count: StateId,
        initial: impl IntoIterator<Item = StateId>,
        accepting: impl IntoIterator<Item = StateId>,
        transitions: impl IntoIterator<Item = Transition>,
    ) -> Result<Self, AutomatonError> {
        if state_count == 0 {
            return Err(AutomatonError::NoStates);
        }

        let initial = initial.into_iter().collect::<BTreeSet<_>>();
        if initial.is_empty() {
            return Err(AutomatonError::NoInitialState);
        }
        let accepting = accepting.into_iter().collect::<BTreeSet<_>>();
        let mut transitions = transitions.into_iter().collect::<Vec<_>>();

        for state in initial.iter().chain(accepting.iter()).copied().chain(
            transitions
                .iter()
                .flat_map(|transition| [transition.from, transition.to]),
        ) {
            if state >= state_count {
                return Err(AutomatonError::StateOutOfRange { state, state_count });
            }
        }

        for transition in &mut transitions {
            transition.step.canonicalize();
        }
        transitions.sort_unstable();
        transitions.dedup();

        Ok(Self {
            state_count,
            initial,
            accepting,
            transitions,
        })
    }

    /// Number of states, which are exactly `0..state_count`.
    pub fn state_count(&self) -> StateId {
        self.state_count
    }

    /// Canonically ordered initial states.
    pub fn initial_states(&self) -> impl Iterator<Item = StateId> + '_ {
        self.initial.iter().copied()
    }

    /// Whether `state` accepts a completed path.
    pub fn is_accepting(&self, state: StateId) -> bool {
        self.accepting.contains(&state)
    }

    /// Canonically ordered, duplicate-free transitions.
    pub fn transitions(&self) -> &[Transition] {
        &self.transitions
    }
}
