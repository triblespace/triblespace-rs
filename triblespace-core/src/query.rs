//! Query facilities for matching tribles by declaring patterns of constraints.
//! Build queries with the [`find!`](crate::prelude::find) macro which binds variables and
//! combines constraint expressions:
//!
//! ```
//! # use triblespace_core::prelude::*;
//! # use triblespace_core::prelude::valueschemas::ShortString;
//! let results = find!((x: Value<ShortString>), x.is("foo".to_value())).collect::<Vec<_>>();
//! ```
//!
//! For a tour of the language see the "Query Language" chapter in the book.
//! Conceptual background on schemas and join strategy appears in the
//! "Query Engine" and "Atreides Join" chapters.
pub mod constantconstraint;
pub mod hashmapconstraint;
pub mod hashsetconstraint;
pub mod ignore;
pub mod intersectionconstraint;
pub mod patchconstraint;
pub mod regularpathconstraint;
pub mod unionconstraint;
mod variableset;

use std::cmp::Reverse;
use std::fmt;
use std::iter::FromIterator;
use std::marker::PhantomData;

use arrayvec::ArrayVec;
use constantconstraint::*;
pub use ignore::IgnoreConstraint;

use crate::value::schemas::genid::GenId;
use crate::value::RawValue;
use crate::value::Value;
use crate::value::ValueSchema;

pub use regularpathconstraint::PathEngine;
pub use regularpathconstraint::PathOp;
pub use regularpathconstraint::RegularPathConstraint;
pub use regularpathconstraint::ThompsonEngine;
pub use variableset::VariableSet;

/// Types storing tribles can implement this trait to expose them to queries.
/// The trait provides a method to create a constraint for a given trible pattern.
pub trait TriblePattern {
    /// The type of the constraint created by the pattern method.
    type PatternConstraint<'a>: Constraint<'a>
    where
        Self: 'a;

    /// Create a constraint for a given trible pattern.
    /// The method takes three variables, one for each part of the trible.
    /// The schemas of the entities and attributes are always [GenId], while the value
    /// schema can be any type implementing [ValueSchema] and is specified as a type parameter.
    ///
    /// This method is usually not called directly, but rather through typed query language
    /// macros like [pattern!][crate::namespace].
    fn pattern<'a, V: ValueSchema>(
        &'a self,
        e: Variable<GenId>,
        a: Variable<GenId>,
        v: Variable<V>,
    ) -> Self::PatternConstraint<'a>;
}

/// Low-level identifier for a variable in a query.
pub type VariableId = usize;

/// Context for creating variables in a query.
/// The context keeps track of the next index to assign to a variable.
/// This allows for the creation of new anonymous variables in higher-level query languages.
#[derive(Debug)]
pub struct VariableContext {
    pub next_index: VariableId,
}

impl Default for VariableContext {
    fn default() -> Self {
        Self::new()
    }
}

impl VariableContext {
    /// Create a new variable context.
    /// The context starts with an index of 0.
    pub fn new() -> Self {
        VariableContext { next_index: 0 }
    }

    /// Create a new variable.
    /// The variable is assigned the next available index.
    ///
    /// Panics if the number of variables exceeds 128.
    ///
    /// This method is usually not called directly, but rather through typed query language
    /// macros like [find!][crate::query].
    pub fn next_variable<T: ValueSchema>(&mut self) -> Variable<T> {
        assert!(
            self.next_index < 128,
            "currently queries support at most 128 variables"
        );
        let v = Variable::new(self.next_index);
        self.next_index += 1;
        v
    }
}

/// A placeholder for unknowns in a query.
/// Within the query engine each variable is identified by an integer,
/// which can be accessed via the `index` property.
/// Variables also have an associated type which is used to parse the [Value]s
/// found by the query engine.
#[derive(Debug)]
pub struct Variable<T: ValueSchema> {
    pub index: VariableId,
    typed: PhantomData<T>,
}

impl<T: ValueSchema> Copy for Variable<T> {}

impl<T: ValueSchema> Clone for Variable<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: ValueSchema> Variable<T> {
    pub fn new(index: VariableId) -> Self {
        Variable {
            index,
            typed: PhantomData,
        }
    }

    pub fn extract(self, binding: &Binding) -> &Value<T> {
        let raw = binding.get(self.index).unwrap_or_else(|| {
            panic!(
                "query variable (idx {}) was never bound; ensure it appears in a constraint or remove it from the projection",
                self.index
            )
        });
        Value::as_transmute_raw(raw)
    }
}

/// Collections can implement this trait so that they can be used in queries.
/// The returned constraint will filter the values assigned to the variable
/// to only those that are contained in the collection.
pub trait ContainsConstraint<'a, T: ValueSchema> {
    type Constraint: Constraint<'a>;

    /// Create a constraint that filters the values assigned to the variable
    /// to only those that are contained in the collection.
    ///
    /// The returned constraint will usually perform a conversion between the
    /// concrete rust type stored in the collection a [Value] of the appropriate schema
    /// type for the variable.
    fn has(self, v: Variable<T>) -> Self::Constraint;
}

impl<T: ValueSchema> Variable<T> {
    /// Create a constraint so that only a specific value can be assigned to the variable.
    pub fn is(self, constant: Value<T>) -> ConstantConstraint {
        ConstantConstraint::new(self, constant)
    }
}

/// The binding keeps track of the values assigned to variables in a query.
/// It maps variables to values - by their index - via a simple array,
/// and keeps track of which variables are bound.
/// It is used to store intermediate results and to pass information
/// between different constraints.
/// The binding is mutable, as it is modified by the query engine.
/// It is not thread-safe and should not be shared between threads.
/// The binding is a simple data structure that is cheap to clone.
/// It is not intended to be used as a long-term storage for query results.
#[derive(Clone, Debug)]
pub struct Binding {
    pub bound: VariableSet,
    values: [RawValue; 128],
}

impl Binding {
    /// Create a new empty binding.
    pub fn set(&mut self, variable: VariableId, value: &RawValue) {
        self.values[variable] = *value;
        self.bound.set(variable);
    }

    /// Unset a variable in the binding.
    /// This is used to backtrack in the query engine.
    pub fn unset(&mut self, variable: VariableId) {
        self.bound.unset(variable);
    }

    /// Check if a variable is bound in the binding.
    pub fn get(&self, variable: VariableId) -> Option<&RawValue> {
        if self.bound.is_set(variable) {
            Some(&self.values[variable])
        } else {
            None
        }
    }
}

impl Default for Binding {
    fn default() -> Self {
        Self {
            bound: VariableSet::new_empty(),
            values: [[0; 32]; 128],
        }
    }
}

/// A constraint is a predicate used to filter the results of a query.
/// It restricts the values that can be assigned to a variable.
/// Constraints can be combined using logical operators like `and` and `or`.
/// This trait provides methods to estimate, propose, and confirm values for a variable:
/// - `estimate` method estimates the number of values that match the constraint.
/// - `propose` method suggests values for a variable that match the constraint.
/// - `confirm` method verifies a value for a variable that matches the constraint.
/// - `variables` method returns the set of variables used by the constraint.
///   The trait is generic over the lifetime of an underlying borrowed data structure that the
///   constraint might use, such as a [std::collections::HashMap] or a [crate::trible::TribleSet].
///
/// Note that the constraint does not store any state, but rather operates on the binding
/// passed to it by the query engine. This allows the query engine to efficiently
/// backtrack and try different values for the variables, potentially in parallel.
///
/// The trait is designed to be simple and flexible, allowing for a wide range of
/// constraints to be implemented, while still allowing for efficient exploration of the
/// search space by the query engine.
///
/// In contrast to many other query languages, the constraints are not evaluated in a
/// fixed order, but rather the query engine uses the estimates provided by the constraints
/// to guide the search. This allows for a more flexible and efficient exploration of the
/// search space, as the query engine can focus on the most promising parts.
/// This also also obviates the need for complex query optimization techniques, as the
/// constraints themselves provide the necessary information to guide the search,
/// and the query engine can adapt dynamically to the data and the query, providing
/// skew-resistance and predictable performance. This also removes the need for ordered indexes,
/// allowing for queries to be executed on unsorted data structures like hashmaps, which
/// enables easy integration with native Rust data structures and libraries.
/// This also allows for the query engine to be easily extended with new constraints,
/// so long as they provide reasonable estimates of the number of values that match the constraint.
/// See the module documentation for notes on the accuracy of these estimates.
///
/// The trait is designed to be used in combination with the [Query] struct, which provides
/// a simple and efficient way to iterate over the results of a query.
pub trait Constraint<'a> {
    /// Return the set of variables used by the constraint.
    /// This is only called once at the beginning of the query.
    /// The query engine uses this information to keep track of the variables
    /// that are used by each constraint.
    fn variables(&self) -> VariableSet;

    /// Estimate the number of values that match the constraint.
    /// This is used by the query engine to guide the search.
    /// The estimate should be as accurate as possible, while being cheap to compute,
    /// and is not required to be exact or a permissible heuristic.
    /// The binding passed to the method contains the values assigned to the variables so far.
    ///
    /// If the variable is not used by the constraint, the method should return `None`.
    fn estimate(&self, variable: VariableId, binding: &Binding) -> Option<usize>;

    /// Propose values for a variable that match the constraint.
    /// This is used by the query engine to explore the search space.
    /// The method should add values to the `proposals` vector that match the constraint.
    /// The binding passed to the method contains the values assigned to the variables so far.
    ///
    /// If the variable is not used by the constraint, the method should do nothing.
    fn propose(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawValue>);

    /// Confirm a value for a variable that matches the constraint.
    /// This is used by the query engine to prune the search space, and confirm that a value satisfies the constraint.
    /// The method should remove values from the `proposals` vector that do not match the constraint.
    /// The binding passed to the method contains the values assigned to the variables so far.
    ///
    /// If the variable is not used by the constraint, the method should do nothing.
    fn confirm(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawValue>);

    /// Return the set of variables potentially influenced when the passed
    /// variable is bound or unbound.
    ///
    /// By default this includes all variables used by the constraint except the
    /// queried one when the constraint contains the variable, otherwise the set
    /// is empty.
    fn influence(&self, variable: VariableId) -> VariableSet {
        let mut vars = self.variables();
        if vars.is_set(variable) {
            vars.unset(variable);
            vars
        } else {
            VariableSet::new_empty()
        }
    }
}

impl<'a, T: Constraint<'a> + ?Sized> Constraint<'a> for Box<T> {
    fn variables(&self) -> VariableSet {
        let inner: &T = self;
        inner.variables()
    }

    fn estimate(&self, variable: VariableId, binding: &Binding) -> Option<usize> {
        let inner: &T = self;
        inner.estimate(variable, binding)
    }

    fn propose(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawValue>) {
        let inner: &T = self;
        inner.propose(variable, binding, proposals)
    }

    fn confirm(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawValue>) {
        let inner: &T = self;
        inner.confirm(variable, binding, proposals)
    }

    fn influence(&self, variable: VariableId) -> VariableSet {
        let inner: &T = self;
        inner.influence(variable)
    }
}

impl<'a, T: Constraint<'a> + ?Sized> Constraint<'static> for std::sync::Arc<T> {
    fn variables(&self) -> VariableSet {
        let inner: &T = self;
        inner.variables()
    }

    fn estimate(&self, variable: VariableId, binding: &Binding) -> Option<usize> {
        let inner: &T = self;
        inner.estimate(variable, binding)
    }

    fn propose(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawValue>) {
        let inner: &T = self;
        inner.propose(variable, binding, proposals)
    }

    fn confirm(&self, variable: VariableId, binding: &Binding, proposal: &mut Vec<RawValue>) {
        let inner: &T = self;
        inner.confirm(variable, binding, proposal)
    }

    fn influence(&self, variable: VariableId) -> VariableSet {
        let inner: &T = self;
        inner.influence(variable)
    }
}

/// A query is an iterator over the results of a query.
/// It takes a constraint and a post-processing function as input,
/// and returns the results of the query as a stream of values.
/// The query engine uses a depth-first search to find solutions to the query,
/// proposing values for the variables and backtracking when it reaches a dead end.
/// The query engine is designed to be simple and efficient, providing low, consistent,
/// and predictable latency, skew resistance, and no required (or possible) tuning.
/// The query engine is designed to be used in combination with the [Constraint] trait,
/// which provides a simple and flexible way to implement constraints that can be used
/// to filter the results of a query.
///
/// This struct is usually not created directly, but rather through the `find!` macro,
/// which provides a convenient way to declare variables and concrete types for them.
/// And which sets up the nessecairy context for higher-level query languages
/// like the one provided by the [crate::namespace] module.
pub struct Query<C, P: Fn(&Binding) -> R, R> {
    constraint: C,
    postprocessing: P,
    mode: Search,
    binding: Binding,
    influences: [VariableSet; 128],
    estimates: [usize; 128],
    touched_variables: VariableSet,
    stack: ArrayVec<VariableId, 128>,
    unbound: ArrayVec<VariableId, 128>,
    values: ArrayVec<Option<Vec<RawValue>>, 128>,
}

impl<'a, C: Constraint<'a>, P: Fn(&Binding) -> R, R> Query<C, P, R> {
    /// Create a new query.
    /// The query takes a constraint and a post-processing function as input,
    /// and returns the results of the query as a stream of values.
    ///
    /// This method is usually not called directly, but rather through the [find!] macro,
    pub fn new(constraint: C, postprocessing: P) -> Self {
        let variables = constraint.variables();
        let influences = std::array::from_fn(|v| {
            if variables.is_set(v) {
                constraint.influence(v)
            } else {
                VariableSet::new_empty()
            }
        });
        let binding = Binding::default();
        let estimates = std::array::from_fn(|v| {
            if variables.is_set(v) {
                constraint
                    .estimate(v, &binding)
                    .expect("unconstrained variable in query")
            } else {
                usize::MAX
            }
        });
        let mut unbound = ArrayVec::from_iter(variables);
        unbound.sort_unstable_by_key(|v| {
            (
                Reverse(
                    estimates[*v]
                        .checked_ilog2()
                        .map(|magnitude| magnitude + 1)
                        .unwrap_or(0),
                ),
                influences[*v].count(),
            )
        });

        Query {
            constraint,
            postprocessing,
            mode: Search::NextVariable,
            binding,
            influences,
            estimates,
            touched_variables: VariableSet::new_empty(),
            stack: ArrayVec::new(),
            unbound,
            values: ArrayVec::from([const { None }; 128]),
        }
    }
}

/// The search mode of the query engine.
/// The query engine uses a depth-first search to find solutions to the query,
/// proposing values for the variables and backtracking when it reaches a dead end.
/// The search mode is used to keep track of the current state of the search.
/// The search mode can be one of the following:
/// - `NextVariable` - The query engine is looking for the next variable to assign a value to.
/// - `NextValue` - The query engine is looking for the next value to assign to a variable.
/// - `Backtrack` - The query engine is backtracking to try a different value for a variable.
/// - `Done` - The query engine has finished the search and there are no more results.
#[derive(Copy, Clone, Debug)]
enum Search {
    NextVariable,
    NextValue,
    Backtrack,
    Done,
}

impl<'a, C: Constraint<'a>, P: Fn(&Binding) -> R, R> Iterator for Query<C, P, R> {
    type Item = R;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match &self.mode {
                Search::NextVariable => {
                    self.mode = Search::NextValue;
                    if self.unbound.is_empty() {
                        return Some((self.postprocessing)(&self.binding));
                    }

                    let mut stale_estimates = VariableSet::new_empty();

                    while let Some(variable) = self.touched_variables.drain_next_ascending() {
                        stale_estimates = stale_estimates.union(self.influences[variable]);
                    }

                    // We remove the bound variables from the stale estimates,
                    // as already bound variables cannot be influenced by the unbound ones.
                    stale_estimates = stale_estimates.subtract(self.binding.bound);

                    if !stale_estimates.is_empty() {
                        while let Some(v) = stale_estimates.drain_next_ascending() {
                            self.estimates[v] = self
                                .constraint
                                .estimate(v, &self.binding)
                                .expect("unconstrained variable in query");
                        }

                        self.unbound.sort_unstable_by_key(|v| {
                            (
                                Reverse(
                                    self.estimates[*v]
                                        .checked_ilog2()
                                        .map(|magnitude| magnitude + 1)
                                        .unwrap_or(0),
                                ),
                                self.influences[*v].count(),
                            )
                        });
                    }

                    let variable = self.unbound.pop().expect("non-empty unbound");
                    let estimate = self.estimates[variable];

                    self.stack.push(variable);
                    let values = self.values[variable].get_or_insert(Vec::new());
                    values.clear();
                    values.reserve_exact(estimate.saturating_sub(values.capacity()));
                    self.constraint.propose(variable, &self.binding, values);
                }
                Search::NextValue => {
                    if let Some(&variable) = self.stack.last() {
                        if let Some(assignment) = self.values[variable]
                            .as_mut()
                            .expect("values should be initialized")
                            .pop()
                        {
                            self.binding.set(variable, &assignment);
                            self.touched_variables.set(variable);
                            self.mode = Search::NextVariable;
                        } else {
                            self.mode = Search::Backtrack;
                        }
                    } else {
                        self.mode = Search::Done;
                        return None;
                    }
                }
                Search::Backtrack => {
                    if let Some(variable) = self.stack.pop() {
                        self.binding.unset(variable);
                        // Note that we did not update estiamtes for the unbound variables
                        // as we are backtracking, so the estimates are still valid.
                        // Since we choose this variable before, we know that it would
                        // still go last in the unbound list.
                        self.unbound.push(variable);

                        // However, we need to update the touched variables,
                        // as we are backtracking and the variable is no longer bound.
                        // We're essentially restoring the estimate of the touched variables
                        // to the state before we bound this variable.
                        self.touched_variables.set(variable);
                        self.mode = Search::NextValue;
                    } else {
                        self.mode = Search::Done;
                        return None;
                    }
                }
                Search::Done => {
                    return None;
                }
            }
        }
    }
}

impl<'a, C: Constraint<'a>, P: Fn(&Binding) -> R, R> fmt::Debug for Query<C, P, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Query")
            .field("constraint", &std::any::type_name::<C>())
            .field("mode", &self.mode)
            .field("binding", &self.binding)
            .field("stack", &self.stack)
            .field("unbound", &self.unbound)
            .finish()
    }
}

/// The `find!` macro is a convenient way to declare variables and concrete types for them.
/// It also sets up the nessecairy context for higher-level query languages like the one
/// provided by the [crate::namespace] module, by injecting a `_local_find_context!` macro
/// that provides a reference to the current variable context. [^note]
///
/// [^note]: This is a bit of a hack to simulate dynamic scoping, which is not possible in Rust.
/// But it allows for a more ergonomic query language syntax that does not require the user
/// to manually pass around the variable context.
///
/// The `find!` macro takes two arguments:
/// - A tuple of variables and their concrete result types, e.g., `(a: Value<ShortString>, b: Ratio)`.
/// - A constraint that describes the pattern you are looking for, e.g., `and!(a.is("Hello World!"), b.is(42))`.
///
/// Note that concrete type declarations for variables, e.g., `a: Value<ShortString>`, `a: String`, or `a: _`,
/// are optional, and can be omitted if the type can be inferred from context.
/// Variable schema types are automatically inferred from the constraint, if possible.
/// The query will automatically perform the necessary conversions between the schema types
/// and the concrete types of the variables. If the conversion fails, the query will panic.
/// For more control over the conversion, you can use a `Value<_>` type for the variable, and use
/// the `TryFromValue` trait to convert the values manually and handle the errors explicitly.
///
/// The macro expands to a call to the [Query::new] constructor, which takes the variables and the constraint
/// as arguments, and returns a [Query] object that can be used to iterate over the results of the query.
///
/// The macro also injects a `_local_find_context!` macro that provides a reference to the current variable context.
/// This allows for macros in query languages, like [pattern!](crate::namespace),
/// to declare new variables in the same scope as the `find!` macro.
/// But you should not use the `_local_find_context!` macro directly,
/// unless you are implementing a custom query language.
#[macro_export]
macro_rules! find {
    // Zero variables: return unit `()` from the closure.
    ((), $Constraint:expr) => {
        {
            let mut ctx = $crate::query::VariableContext::new();

            macro_rules! __local_find_context {
                () => { &mut ctx }
            }

            $crate::query::Query::new($Constraint,
                move |_binding| {
                    
            })
        }
    };

    // Single variable case: return a 1-tuple `(v,)` so destructuring `for (v,) in ...` works.
    (($Var:ident $( : $Ty:ty)? $(,)?), $Constraint:expr) => {
        {
            let mut ctx = $crate::query::VariableContext::new();

            macro_rules! __local_find_context {
                () => { &mut ctx }
            }

            let $Var = ctx.next_variable();
            $crate::query::Query::new($Constraint,
                move |binding| {
                    let $Var$(:$Ty)? = $crate::value::FromValue::from_value($Var.extract(binding));
                    ($Var,)
            })
        }
    };

    // Two-or-more variables: return a tuple of all variables.
    (($first:ident $(:$T1:ty)?, $($rest:ident $(:$Trest:ty)?),+ $(,)?), $Constraint:expr) => {
        {
            let mut ctx = $crate::query::VariableContext::new();

            macro_rules! __local_find_context {
                () => { &mut ctx }
            }

            let $first = ctx.next_variable();
            $(let $rest = ctx.next_variable();)+
            $crate::query::Query::new($Constraint,
                move |binding| {
                    let $first$(:$T1)? = $crate::value::FromValue::from_value($first.extract(binding));
                    $(let $rest$(:$Trest)? = $crate::value::FromValue::from_value($rest.extract(binding));)+
                    ($first, $($rest),+)
            })
        }
    };
}
pub use find;

#[macro_export]
macro_rules! matches {
    (($($Var:ident$(:$Ty:ty)?),* $(,)?), $Constraint:expr) => {
        $crate::query::find!(($($Var$(:$Ty)?),*), $Constraint).next().is_some()
    };
}
pub use matches;

#[macro_export]
macro_rules! temp {
    (($Var:ident), $body:expr) => {{
        let $Var = __local_find_context!().next_variable();
        $body
    }};
    (($Var:ident,), $body:expr) => {
        $crate::temp!(($Var), $body)
    };
    (($Var:ident, $($rest:ident),+ $(,)?), $body:expr) => {{
        $crate::temp!(
            ($Var),
            $crate::temp!(($($rest),+), $body)
        )
    }};
}
pub use temp;

// Helper to construct tuples of variables with correct arity. Defined at
// top-level to avoid nested repetition issues inside other macro_rules!
macro_rules! __tribles_mk_tuple {
    () => { () };
    ($single:ident) => { ($single,) };
    ($a:ident, $b:ident $(, $rest:ident)*) => { ($a, $b $(, $rest)*) };
}

#[cfg(test)]
mod tests {
    use valueschemas::ShortString;

    use crate::ignore;
    use crate::prelude::valueschemas::*;
    use crate::prelude::*;

    use crate::examples::literature;

    use fake::faker::lorem::en::Sentence;
    use fake::faker::lorem::en::Words;
    use fake::faker::name::raw::*;
    use fake::locales::*;
    use fake::Fake;

    use std::collections::HashSet;

    use super::*;

    pub mod knights {
        use crate::prelude::*;

        attributes! {
            "8143F46E812E88C4544E7094080EC523" as loves: valueschemas::GenId;
            "D6E0F2A6E5214E1330565B4D4138E55C" as name: valueschemas::ShortString;
        }
    }

    mod social {
        use crate::prelude::*;

        attributes! {
            "A19EC1D9DD534BA9896223A457A6B9C9" as name: valueschemas::ShortString;
            "C21DE0AA5BA3446AB886C9640BA60244" as friend: valueschemas::GenId;
        }
    }

    #[test]
    fn and_set() {
        let mut books = HashSet::<String>::new();
        let mut movies = HashSet::<Value<ShortString>>::new();

        books.insert("LOTR".to_string());
        books.insert("Dragonrider".to_string());
        books.insert("Highlander".to_string());

        movies.insert("LOTR".to_value());
        movies.insert("Highlander".to_value());

        let inter: Vec<_> =
            find!((a: Value<ShortString>), and!(books.has(a), movies.has(a))).collect();

        assert_eq!(inter.len(), 2);

        let cross: Vec<_> =
            find!((a: Value<ShortString>, b: Value<ShortString>), and!(books.has(a), movies.has(b))).collect();

        assert_eq!(cross.len(), 6);

        let one: Vec<_> = find!((a: Value<ShortString>),
            and!(books.has(a), a.is(ShortString::value_from("LOTR")))
        )
        .collect();

        assert_eq!(one.len(), 1);
    }

    #[test]
    fn pattern() {
        let mut kb = TribleSet::new();
        (0..1000).for_each(|_| {
            let author = fucid();
            let book = fucid();
            kb += entity! { &author @
               literature::firstname: FirstName(EN).fake::<String>(),
               literature::lastname: LastName(EN).fake::<String>(),
            };
            kb += entity! { &book @
               literature::author: &author,
               literature::title: Words(1..3).fake::<Vec<String>>().join(" "),
               literature::quote: Sentence(5..25).fake::<String>().to_blob().get_handle()
            };
        });

        let author = fucid();
        let book = fucid();
        kb += entity! { &author @
           literature::firstname: "Frank",
           literature::lastname: "Herbert",
        };
        kb += entity! { &book @
           literature::author: &author,
           literature::title: "Dune",
           literature::quote: "I must not fear. Fear is the \
                   mind-killer. Fear is the little-death that brings total \
                   obliteration. I will face my fear. I will permit it to \
                   pass over me and through me. And when it has gone past I \
                   will turn the inner eye to see its path. Where the fear \
                   has gone there will be nothing. Only I will remain.".to_blob().get_handle()
        };

        (0..100).for_each(|_| {
            let author = fucid();
            let book = fucid();
            kb += entity! { &author @
               literature::firstname: "Fake",
               literature::lastname: "Herbert",
            };
            kb += entity! { &book @
               literature::author: &author,
               literature::title: Words(1..3).fake::<Vec<String>>().join(" "),
               literature::quote: Sentence(5..25).fake::<String>().to_blob().get_handle()
            };
        });

        let r: Vec<_> = find!(
        (author: Value<_>, book: Value<_>, title: Value<_>, quote: Value<_>),
        pattern!(&kb, [
        {?author @
            literature::firstname: "Frank",
            literature::lastname: "Herbert"},
        {?book @
          literature::author: ?author,
          literature::title: ?title,
          literature::quote: ?quote
        }]))
        .collect();

        assert_eq!(1, r.len())
    }

    #[test]
    fn constant() {
        let q: Query<IntersectionConstraint<_>, _, _> = find! {
            (string: Value<_>, number: Value<_>),
            and!(
                string.is(ShortString::value_from("Hello World!")),
                number.is(I256BE::value_from(42))
            )
        };
        let r: Vec<_> = q.collect();

        assert_eq!(1, r.len())
    }

    #[test]
    fn matches_true() {
        assert!(matches!((a: Value<_>), a.is(I256BE::value_from(42))));
    }

    #[test]
    fn matches_false() {
        assert!(!matches!(
            (a: Value<_>),
            and!(a.is(I256BE::value_from(1)), a.is(I256BE::value_from(2)))
        ));
    }

    #[test]
    fn temp_variables_span_patterns() {
        use social::*;

        let mut kb = TribleSet::new();
        let alice = fucid();
        let bob = fucid();

        kb += entity! { &alice @ name: "Alice", friend: &bob };
        kb += entity! { &bob @ name: "Bob" };

        let matches: Vec<_> = find!(
            (person_name: Value<_>),
            temp!((mutual_friend),
                and!(
                    pattern!(&kb, [{ _?person @ name: ?person_name, friend: ?mutual_friend }]),
                    pattern!(&kb, [{ ?mutual_friend @ name: "Bob" }])
                )
            )
        )
        .collect();

        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].0.from_value::<&str>(), "Alice");
    }

    #[test]
    fn ignore_skips_variables() {
        let results: Vec<_> = find!(
            (x: Value<_>),
            ignore!((y), and!(x.is(I256BE::value_from(1)), y.is(I256BE::value_from(2))))
        )
        .collect();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, I256BE::value_from(1));
    }

    #[test]
    fn estimate_override_debug_order() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let mut ctx = VariableContext::new();
        let a = ctx.next_variable::<ShortString>();
        let b = ctx.next_variable::<ShortString>();

        let base = and!(
            a.is(ShortString::value_from("A")),
            b.is(ShortString::value_from("B"))
        );

        let mut wrapper = crate::debug::query::EstimateOverrideConstraint::new(base);
        wrapper.set_estimate(a.index, 10);
        wrapper.set_estimate(b.index, 1);

        let record = Rc::new(RefCell::new(Vec::new()));
        let debug = crate::debug::query::DebugConstraint::new(wrapper, Rc::clone(&record));

        let q: Query<_, _, _> = Query::new(debug, |_| ());
        let r: Vec<_> = q.collect();
        assert_eq!(1, r.len());
        assert_eq!(&*record.borrow(), &[b.index, a.index]);
    }
}
