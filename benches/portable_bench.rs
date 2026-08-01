//! Portable engine benchmark v2 — the instrument the engine-history sweep
//! depends on. Successor of the synthetic-only harness; adds a real-dataset
//! spine (pile checkout at cumulative-trible rungs, RAM archive build, and a
//! source-agnostic query matrix over TribleSet / SuccinctArchive / WGPU).
//!
//! CONSTRAINTS, each paid for by a real failure on 2026-07-25:
//!
//! * *Stable core API only.* `prelude::*`, `find!`, `pattern!`, `path!`,
//!   `or!`, `and!`, `temp!`, `value_range`, `Pile`, `Repository`,
//!   `Workspace::checkout`, `SuccinctArchive` — every item verified present
//!   at both 739fd05c (2026-07-03) and 6a6a94f1 (2026-07-24). Everything
//!   that drifted this month lives outside this surface. The LSM index-home
//!   path is deliberately NOT here — it lives in `portable_bench_lsm.rs`
//!   with an independent compilation fate.
//! * *Raw samples, never a bare mean.* Minima reproduce to 0.2–1.7%; means
//!   drift 6–15%; maxima swing 3x. We print min/p50/p95/max so the floor and
//!   the tail are both first-class.
//! * *It must be able to say NO SIGNAL.* If the floor is unstable across
//!   halves of the run (>10% drift, or fewer than 8 samples), the measure
//!   self-rejects instead of emitting a plausible number.
//! * *Workload identity.* Result cardinalities are gated per measure across
//!   iterations AND across execution arms (set vs archive vs GPU must return
//!   identical counts for identical queries). Violation = exit 3, never a
//!   silent speed win.
//! * *Panics are an outcome, not a crash.* At pre-2026-07-11 commits the
//!   wired q3b's `or!` trips the `unionconstraint.rs` variable-set assert and
//!   used to kill the whole bench (2026-07-27 sample-2 finding). Every query
//!   measure and every panic-prone phase (checkout, build_ram, GPU init) runs
//!   under `catch_unwind` with the default panic hook silenced; a panicking
//!   measure reports `  <key>  PANIC (<msg>)`, joins a `PANIC    :` summary
//!   line, contributes a `usize::MAX` sentinel to the identity tuple (so runs
//!   stay comparable on the surviving measures), is EXCLUDED from the
//!   SIGNAL/NO-SIGNAL verdict counts, and never takes the other measures with
//!   it. Exit code stays 0 while at least one measure has SIGNAL.
//!
//! PHASES (each with its own measure keys, each SKIP-able):
//!   checkout   — `Workspace::checkout` of the first k commits of a dataset
//!                pile's data branch, where k is derived from a cumulative-
//!                trible rung target by walking the commit chain once
//!                (per-commit tribles = SimpleArchive blob length / 64).
//!                Sub-first-chunk rungs checkout commit 1 and carve a sorted
//!                prefix in-process. With `--chunk-aligned` the rung SNAPS to
//!                the nearest cumulative-commit boundary instead (no carving)
//!                — the alignment the LSM bench always applies — and the
//!                snapped rung is printed so sweeps record actual-vs-nominal.
//!                SKIP when no pile is given.
//!   build_ram  — `SuccinctArchive<OrderedUniverse>::from(&set)`, gated on
//!                `--max-ram` (default 20M tribles).
//!   q<N>_<arm> — the query matrix (see QUERIES below) over arms `set`,
//!                `arch`, and (feature = "gpu") `gpu`. q2 is `path!`-based
//!                and TribleSet-only (RegularPathConstraint has no archive
//!                back-end), so it reports as `q2_set` only.
//!   F1..F5     — the Harkonnen adversarial fixtures (vendored builders from
//!                examples/backoff_matrix_fixtures.rs), TTFR + total each.
//!
//! DATASET MODES:
//!   --pile <path>  an assertion-native dataset pile (e.g. sparqloscope's
//!                  dblp-ladder): one exact named branch identity whose
//!                  linear commit chain carries SimpleArchive content blobs.
//!                  !!! `Repository::new`
//!                  appends ONE small commit-metadata blob record to the
//!                  pile file on open — always point --pile at a clonefile
//!                  copy (`cp -c` on APFS, free) of the dataset pile, never
//!                  at the original.
//!   (no --pile)    DBLP-shaped synthetic data minted in-process from a
//!                  seeded splitmix64 — the harness runs at any commit with
//!                  zero setup, and the SAME query matrix applies because
//!                  the generator emits facts under the SAME content-derived
//!                  attribute ids the N-Triples importer would assign.
//!
//! USAGE:
//!   cargo bench --bench portable_bench -- [--pile P] [--branch B]
//!     [--rung 1M] [--chunk-aligned] [--iters 12] [--warmup 3]
//!     [--build-iters 8] [--build-warmup 2] [--max-ram 20M] [--range-min 2]
//!     [--chain 20000] [--ring 5000] [--oasis 4000] [--fan 32]
//!     [--khop 256] [--diamond 256]
//!   Sizes accept k/M/G suffixes. Unknown tokens are skipped with a note
//!   (cargo may inject its own flags).
//!
//! EXIT CODES: 0 usable (>= 1 measure with SIGNAL; PANIC'd measures are
//! their own outcome, excluded from the verdict), 3 workload-identity
//! violation, 4 no measure had signal. BUILD-FAIL is expressed by
//! compilation, not at runtime.
//!
//! JUNE-PROTOCOL ADAPTATION (2026-07-27, engine/june-on-tip): this branch's
//! engine has no `path!` / RegularPathConstraint (the June-protocol
//! transplant carries the strict-union, bag-semantics query core without the
//! regular-path layer). Every `path!`-based measure — q2 (wired + q2b/q2c
//! candidates) and the F1/F2/F4 transitive-closure fixtures — therefore
//! cannot be EXPRESSED here; each SKIPs with an explicit line in the output
//! instead of vanishing silently. F3/F5 and the full q1/q3/q4/q5 matrix run
//! unchanged. Timing methodology, warmups, floor-rejection, and gate logic
//! are untouched. The pristine pre-adaptation harness is commit 0fff24d0.

use std::time::Instant;

use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;

use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
use triblespace_core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
use triblespace_core::import::ntriples::uri_to_id_pure;
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::metadata;
use triblespace_core::prelude::inlineencodings::{GenId, I256BE};
use triblespace_core::prelude::*;
use triblespace_core::repo::branch_assertion::{BranchAssertionStore, BranchIdentity};
use triblespace_core::repo::branch_frontier::{BranchResolution, ResolvedHead};
use triblespace_core::repo::pile::Pile;
use triblespace_core::repo::{self, Repository};

// GPU comparison arm. The whole arm is behind `feature = "gpu"` so it
// VANISHES when the feature is off (early commits predate the crate, and the
// CPU-only sweep must not link a GPU device). `WgpuSuccinctArchive::new` owns
// device init + shader compilation internally; it implements TriblePattern,
// so the SAME generic `measure_queries` drives it.
#[cfg(feature = "gpu")]
use triblespace_gpu::WgpuSuccinctArchive;

// ---------------------------------------------------------------------------
// Crash isolation: panics are a fourth outcome (SIGNAL / NO-SIGNAL / SKIP /
// PANIC), never a dead process. Keep these helpers IDENTICAL in
// portable_bench_lsm.rs.
// ---------------------------------------------------------------------------

/// Outcome of one measure across a run: samples to report, or the first line
/// of the panic message that killed it (any samples it collected before
/// panicking are discarded — a broken measure must not look healthy).
enum Outcome {
    Samples(Vec<f64>),
    Panic(String),
}

/// Sentinel a panicked measure contributes to identity tuples. Deterministic
/// panics map to the same sentinel on both sides of a comparison, so runs
/// stay comparable on their surviving measures.
const PANIC_COUNT: usize = usize::MAX;

/// `catch_unwind` with the default panic hook silenced around the call (441
/// sweep commits of EXPECTED q3 panics must not spam stderr — hook saved and
/// restored) and the payload reduced to the first line of its message.
fn quiet_catch<R>(f: impl FnOnce() -> R) -> Result<R, String> {
    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
    std::panic::set_hook(hook);
    out.map_err(|payload| {
        let msg = if let Some(s) = payload.downcast_ref::<&str>() {
            s
        } else if let Some(s) = payload.downcast_ref::<String>() {
            s.as_str()
        } else {
            "non-string panic payload"
        };
        msg.lines().next().unwrap_or("").to_owned()
    })
}

/// One guarded timed call. Skips slots that already panicked (the panic is
/// deterministic — re-panicking every iteration buys nothing), records the
/// sample when warmed up, and downgrades a panic to a marked slot.
fn timed_guarded<R>(
    panicked: &mut Option<String>,
    samples: &mut Vec<f64>,
    recording: bool,
    f: impl FnOnce() -> R,
) -> Option<R> {
    if panicked.is_some() {
        return None;
    }
    let t = Instant::now();
    match quiet_catch(f) {
        Ok(r) => {
            if recording {
                samples.push(t.elapsed().as_secs_f64() * 1000.0);
            }
            Some(r)
        }
        Err(msg) => {
            *panicked = Some(msg);
            None
        }
    }
}

/// Cross-arm identity comparison that ignores PANIC sentinels: a query that
/// panicked on either arm has no count to disagree with.
fn counts_match(a: [usize; 4], b: [usize; 4]) -> bool {
    a.iter()
        .zip(b.iter())
        .all(|(&x, &y)| x == PANIC_COUNT || y == PANIC_COUNT || x == y)
}

/// Snap a rung target to the nearest cumulative-commit (chunk) boundary.
/// Returns k, the 1-based count of commits whose cumulative trible total is
/// closest to the target; ties resolve to the smaller k. IDENTICAL in both
/// benches so the two instruments land on the same prefix at the same rung.
fn snap_to_chunk(cum: &[usize], rung: usize) -> usize {
    let mut best = (usize::MAX, 1);
    for (i, &c) in cum.iter().enumerate() {
        let d = c.abs_diff(rung);
        if d < best.0 {
            best = (d, i + 1);
        }
    }
    best.1
}

// ---------------------------------------------------------------------------
// DBLP vocabulary + content-derived attributes.
//
// The N-Triples importer derives each attribute id from the pair
// (predicate IRI, value schema) via the entity-core derivation
// (`metadata::iri` + `metadata::value_encoding` -> content-addressed root).
// Reproducing that derivation here (vendored from sparqloscope-bench
// `attr()`) makes query constants line up with imported pile data across
// processes and machines — no minted constants, no guessed hex.
// ---------------------------------------------------------------------------

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const DBLP_HAS_SIGNATURE: &str = "https://dblp.org/rdf/schema#hasSignature";
const DBLP_PUBLISHED_IN_STREAM: &str = "https://dblp.org/rdf/schema#publishedInStream";
const DBLP_PUBLISHED_AS_PART_OF: &str = "https://dblp.org/rdf/schema#publishedAsPartOf";
const DBLP_RELATED_STREAM: &str = "https://dblp.org/rdf/schema#relatedStream";
const DBLP_SUB_STREAM: &str = "https://dblp.org/rdf/schema#subStream";
const DBLP_CREATED_BY: &str = "https://dblp.org/rdf/schema#createdBy";
const DBLP_NUMBER_OF_CREATORS: &str = "https://dblp.org/rdf/schema#numberOfCreators";
/// Fixed subject of the wired q2 (pile mode): the conf/damp stream.
const STREAM_CONF_DAMP: &str = "https://dblp.org/streams/conf/damp";

/// The importer's (IRI, value schema) -> attribute derivation.
fn attr<S: InlineEncoding + MetaDescribe>(iri: &str) -> Attribute<S> {
    Attribute::<S>::from(entity! {
        metadata::iri:            iri.to_owned().to_blob().get_handle(),
        metadata::value_encoding: <S as MetaDescribe>::id(),
    })
}

/// Every attribute the wired queries and candidates touch, derived once.
struct DblpAttrs {
    rdf_type: Attribute<GenId>,
    has_signature: Attribute<GenId>,
    published_in_stream: Attribute<GenId>,
    published_as_part_of: Attribute<GenId>,
    related_stream: Attribute<GenId>,
    // Only the removed path!-based q2c consumed sub_stream; kept (like the
    // candidates and builders) so re-wiring on a path!-bearing engine is a
    // small edit.
    #[allow(dead_code)]
    sub_stream: Attribute<GenId>,
    created_by: Attribute<GenId>,
    number_of_creators: Attribute<I256BE>,
}

impl DblpAttrs {
    fn derive() -> Self {
        Self {
            rdf_type: attr(RDF_TYPE),
            has_signature: attr(DBLP_HAS_SIGNATURE),
            published_in_stream: attr(DBLP_PUBLISHED_IN_STREAM),
            published_as_part_of: attr(DBLP_PUBLISHED_AS_PART_OF),
            related_stream: attr(DBLP_RELATED_STREAM),
            sub_stream: attr(DBLP_SUB_STREAM),
            created_by: attr(DBLP_CREATED_BY),
            number_of_creators: attr(DBLP_NUMBER_OF_CREATORS),
        }
    }
}

// ---------------------------------------------------------------------------
// QUERIES — the taste-gate table. One WIRED query per category runs in the
// matrix; the CANDIDATE alternates are compiled (#[allow(dead_code)]) so
// swapping the supervisor's pick in is a one-line edit, never an API risk.
// All candidates use only range-stable core API and are shaped for DBLP.
//
// cat        key  status     query
// BGP join   q1   WIRED      pubs -publishedInStream-> stream -rdf:type-> t
//                            (2-pattern chain join across entities; large x
//                            medium with a shared temp entity)
//            --   CANDIDATE  q1b star join { ?s @ rdf:type ?o1,
//                            publishedInStream ?o2 } (co-subject star;
//                            intersection on the SAME entity, no hop)
//            --   CANDIDATE  q1c large-large join with near-empty result
//                            { ?s @ publishedAsPartOf ?o1, createdBy ?o2 }
//                            (type-disjoint entity sets; exercises fast
//                            refutation of a doomed join)
// path       q2   WIRED      fixed-subject transitive closure:
//                            conf/damp relatedStream+ ?o (bounded frontier
//                            from a constant; TribleSet-only)
//            --   CANDIDATE  q2b all-pairs relatedStream+ (full ALP closure;
//                            the expensive frontier — cost scales with the
//                            whole stream graph)
//            --   CANDIDATE  q2c join+plus: ?s subStream ?m . ?m
//                            relatedStream+ ?o (pattern! join feeding a
//                            path! closure — the composition seam)
// union      q3   WIRED=q3b      { ?s rdf:type ?o } UNION { ?s hasSignature ?o }
//                            (or! of two large disjoint-object predicates,
//                            no constraint — pure union enumeration)
//            --   CANDIDATE  q3b union constrained by a GenId join:
//                            (publishedAsPartOf | publishedInStream) and
//                            createdBy — or! under and!, branch pruning
//            --   CANDIDATE  q3c see q3b with the small journal-volume
//                            constraint (pile-only: Handle<LongString>
//                            object space)
// range      q4   WIRED      numberOfCreators >= --range-min via
//                            value_range (selectivity dial: 2 = 50%,
//                            3 = 30%, 7 = 5% on DBLP)
//            --   CANDIDATE  q4b double-ended window
//                            numberOfCreators in [2, 4]
//            --   CANDIDATE  q4c yearOfPublication window (VERIFY the
//                            imported value schema with import --stats
//                            before wiring — gYear may not be I256BE)
// exists     q5   WIRED=q5b      self-referencing filter { ?s rdf:type ?s }
//                            (FILTER ?s = ?o desugared to an equality
//                            constraint — negation-free, near-zero rows)
//            --   CANDIDATE  q5b witness-set semijoin (EXISTS translation:
//                            engine-enumerated HashSet + .has())
//            --   CANDIDATE  q5c exists! short-circuit boolean over
//                            hasSignature (first-match latency probe)
// ---------------------------------------------------------------------------

/// CANDIDATE q1b: co-subject star join (see table).
#[allow(dead_code)]
fn q1b_star<S: TriblePattern>(src: &S, qa: &DblpAttrs) -> usize {
    let rdf_type = qa.rdf_type.clone();
    let published_in_stream = qa.published_in_stream.clone();
    find!(
        (s: Id, o1: Id, o2: Id),
        pattern!(src, [{ ?s @ rdf_type: ?o1, published_in_stream: ?o2 }])
    )
    .count()
}

/// CANDIDATE q1c: large-large join with a near-empty result (see table).
#[allow(dead_code)]
fn q1c_doomed<S: TriblePattern>(src: &S, qa: &DblpAttrs) -> usize {
    let published_as_part_of = qa.published_as_part_of.clone();
    let created_by = qa.created_by.clone();
    find!(
        (s: Id, o1: Id, o2: Id),
        pattern!(src, [{ ?s @ published_as_part_of: ?o1, created_by: ?o2 }])
    )
    .count()
}

// CANDIDATE q2b (all-pairs closure) and q2c (join feeding a closure) are
// REMOVED on this branch: both are `path!`-based and the June-protocol
// engine has no `path!`/RegularPathConstraint to compile them against. See
// the adaptation note in the module docs; the originals live at 0fff24d0.

/// CANDIDATE q3b: union constrained by a GenId join (see table).
#[allow(dead_code)]
fn q3b_union_join<S: TriblePattern>(src: &S, qa: &DblpAttrs) -> usize {
    let published_as_part_of = qa.published_as_part_of.clone();
    let published_in_stream = qa.published_in_stream.clone();
    let created_by = qa.created_by.clone();
    find!(
        (s: Id, o1: Id, o2: Id),
        and!(
            or!(
                pattern!(src, [{ ?s @ published_as_part_of: ?o1 }]),
                pattern!(src, [{ ?s @ published_in_stream: ?o1 }])
            ),
            pattern!(src, [{ ?s @ created_by: ?o2 }])
        )
    )
    .count()
}

/// CANDIDATE q4b: double-ended value window (see table).
#[allow(dead_code)]
fn q4b_window<S: TriblePattern>(src: &S, qa: &DblpAttrs) -> usize {
    let number_of_creators = qa.number_of_creators.clone();
    let lo: Inline<I256BE> = 2i128.to_inline();
    let hi: Inline<I256BE> = 4i128.to_inline();
    find!(
        (s: Id, o: Inline<I256BE>),
        and!(
            pattern!(src, [{ ?s @ number_of_creators: ?o }]),
            value_range(o, lo, hi)
        )
    )
    .count()
}

/// CANDIDATE q5b: witness-set semijoin — the sparqloscope EXISTS
/// translation. The witness set dedups row multiplicity (existence ignores
/// it); `.has()` is a monotone membership constraint.
#[allow(dead_code)]
fn q5b_semijoin<S: TriblePattern>(src: &S, qa: &DblpAttrs) -> usize {
    let rdf_type = qa.rdf_type.clone();
    let has_signature = qa.has_signature.clone();
    let witnesses: std::collections::HashSet<Id> = find!(
        (s: Id, o: Id),
        pattern!(src, [{ ?s @ has_signature: ?o }])
    )
    .map(|(s, _)| s)
    .collect();
    find!(
        (s: Id, o: Id),
        and!(
            pattern!(src, [{ ?s @ rdf_type: ?o }]),
            witnesses.has(s)
        )
    )
    .count()
}

/// CANDIDATE q5c: exists! short-circuit boolean (see table).
#[allow(dead_code)]
fn q5c_exists<S: TriblePattern>(src: &S, qa: &DblpAttrs) -> bool {
    let has_signature = qa.has_signature.clone();
    exists!(temp!((s, o), pattern!(src, [{ ?s @ has_signature: ?o }])))
}

/// The wired matrix: q1/q3/q4/q5 over any TriblePattern source. Returns the
/// per-query outcomes (keyed `q<N>_<arm>`) and the result cardinalities for
/// the cross-arm identity gate (PANIC_COUNT sentinel for panicked queries).
/// Each query runs under its own catch_unwind guard — one panicking query
/// (q3's `or!` at pre-2026-07-11 commits) must not take the matrix with it.
/// q2 is TribleSet-only — see `measure_path_query`.
fn measure_queries<S: TriblePattern>(
    src: &S,
    arm: &str,
    qa: &DblpAttrs,
    range_min: i128,
    iters: usize,
    warmup: usize,
) -> (Vec<(String, Outcome)>, [usize; 4]) {
    let rdf_type = qa.rdf_type.clone();
    let published_in_stream = qa.published_in_stream.clone();
    let number_of_creators = qa.number_of_creators.clone();
    let lo: Inline<I256BE> = range_min.to_inline();
    let hi: Inline<I256BE> = i128::MAX.to_inline();

    let mut samples: Vec<Vec<f64>> = vec![Vec::new(); 4];
    let mut panicked: [Option<String>; 4] = [None, None, None, None];
    let mut ident: [Option<usize>; 4] = [None; 4];
    for i in 0..(warmup + iters) {
        let recording = i >= warmup;
        let mut counts = [PANIC_COUNT; 4];

        // q1 — BGP chain join: pub -publishedInStream-> stream -type-> t.
        if let Some(n) = timed_guarded(&mut panicked[0], &mut samples[0], recording, || {
            find!(
                (p: Id, st: Id, ty: Id),
                pattern!(src, [
                    { ?p @ published_in_stream: ?st },
                    { ?st @ rdf_type: ?ty }
                ])
            )
            .count()
        }) {
            counts[0] = n;
        }

        // q3 — UNION under a join constraint (q3b, JP-blessed 2026-07-26):
        // (publishedAsPartOf | publishedInStream) and!-constrained by
        // createdBy — exercises branch pruning under a join rather than
        // pure union enumeration. KNOWN: `or!` trips the unionconstraint
        // variable-set assert at pre-2026-07-11 commits — the guard turns
        // that into a PANIC outcome instead of a dead bench. Term-native
        // constants (2026-07-27 resurrection of 78c1a1b7) restore the
        // literal-fold, so both arms declare exactly {s, o1} again and q3
        // produces SIGNAL on this lineage; the guard stays for the sweep
        // commits where the desugar-era panic is still expected.
        if let Some(n) = timed_guarded(&mut panicked[1], &mut samples[1], recording, || {
            q3b_union_join(src, qa)
        }) {
            counts[1] = n;
        }

        // q4 — value range: numberOfCreators >= range_min. I256BE is
        // big-endian two's complement, so [raw(min>=0), raw(i128::MAX)]
        // matches the numeric range exactly.
        if let Some(n) = timed_guarded(&mut panicked[2], &mut samples[2], recording, || {
            find!(
                (s: Id, o: Inline<I256BE>),
                and!(
                    pattern!(src, [{ ?s @ number_of_creators: ?o }]),
                    value_range(o, lo, hi)
                )
            )
            .count()
        }) {
            counts[2] = n;
        }

        // q5 — EXISTS as witness-set semijoin (q5b, JP-blessed 2026-07-26):
        // the sparqloscope EXISTS translation (HashSet witnesses + .has()).
        if let Some(n) = timed_guarded(&mut panicked[3], &mut samples[3], recording, || {
            q5b_semijoin(src, qa)
        }) {
            counts[3] = n;
        }

        for (k, &n) in counts.iter().enumerate() {
            if n == PANIC_COUNT {
                continue;
            }
            match ident[k] {
                None => ident[k] = Some(n),
                Some(expected) if expected != n => {
                    println!(
                        "WORKLOAD IDENTITY VIOLATION ({arm} {}): iter {i} saw {n}, expected {expected}",
                        ["q1", "q3", "q4", "q5"][k]
                    );
                    std::process::exit(3);
                }
                _ => {}
            }
        }
    }
    let mut final_counts = [PANIC_COUNT; 4];
    let keyed = samples
        .into_iter()
        .zip(panicked)
        .zip(["q1", "q3", "q4", "q5"])
        .enumerate()
        .map(|(k, ((s, p), q))| {
            let outcome = match p {
                Some(msg) => Outcome::Panic(msg),
                None => {
                    final_counts[k] = ident[k].unwrap_or(0);
                    Outcome::Samples(s)
                }
            };
            (format!("{q}_{arm}"), outcome)
        })
        .collect();
    (keyed, final_counts)
}

// The wired q2 (`measure_path_query`, fixed-subject transitive closure via
// path!) is REMOVED on this branch — no `path!`/RegularPathConstraint on the
// June-protocol engine. Its call site SKIPs explicitly and q2 keeps the
// "never ran" sentinel in the identity line. Original at 0fff24d0.

// ---------------------------------------------------------------------------
// VENDORED: Harkonnen R1 adversarial pacing fixtures (data-generation half of
// examples/backoff_matrix_fixtures.rs, e847d3e1 2026-07-19). The scheduler-
// pinning scaffolding is deliberately dropped: this harness must measure
// whichever engine the commit under test ships. Attribute ids were minted via
// `trible genid` on 2026-07-19 — never invented.
// ---------------------------------------------------------------------------
mod r1_schema {
    // mp/msrc/khop keep their minted ids although the path!-based fixtures
    // that consume them (F1/F2/F4) are SKIPped on this branch.
    #![allow(dead_code)]
    use triblespace_core::prelude::*;

    attributes! {
        // metronome / ring edge
        "277A42231FD9D42DD50D789D8F9E8661" as mp: inlineencodings::GenId;
        // multi-source marker (K>1 eager-cohort control)
        "0F64BC179033DB2703C65E7DBBAA9AD3" as msrc: inlineencodings::GenId;
        // oasis: type marker, p edge, q edge
        "A0C25A0F02E2D5232269F274761B2AB1" as otype: inlineencodings::GenId;
        "831EA731FB6C91252CDDC4FC399DC975" as op: inlineencodings::GenId;
        "2B3A5EF282FED1F652A2C182E116C28C" as oq: inlineencodings::GenId;
        // thin k-hop functional chain edge
        "EE09E63B176F818960267C5041CA6C92" as khop: inlineencodings::GenId;
        // diamond (reconvergence-capture) route attributes
        "E73DC5D12C49394D3C6D883A152E57C9" as da: inlineencodings::GenId;
        "C41A8C9EC883E09D34C86F87C15EA965" as db: inlineencodings::GenId;
    }
}

/// Deterministic UFOID-shaped ids (shared locality prefix, splitmix suffix)
/// so succinct-backend value order — and therefore exploration order — is
/// reproducible across runs and machines.
struct Ids {
    next: u64,
}

impl Ids {
    fn new() -> Self {
        Self { next: 1 }
    }

    fn splitmix64(mut v: u64) -> u64 {
        v = v.wrapping_add(0x9E37_79B9_7F4A_7C15);
        v = (v ^ (v >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        v = (v ^ (v >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        v ^ (v >> 31)
    }

    fn mint(&mut self) -> ExclusiveId {
        let c = self.next;
        self.next += 1;
        let mut raw = [0u8; 16];
        raw[..4].copy_from_slice(&0xD46B_0001u32.to_be_bytes());
        raw[4..12].copy_from_slice(&Self::splitmix64(c).to_be_bytes());
        raw[12..].copy_from_slice(&Self::splitmix64(c ^ 0xD1B5_4A32).to_be_bytes()[..4]);
        ExclusiveId::force(Id::new(raw).expect("nonzero prefix"))
    }

    /// Mint with a chosen leading suffix byte so a fixture can pin where a
    /// value lands in sorted-universe order (succinct enumerates ascending;
    /// tail-first exploration visits DESCENDING).
    fn mint_ordered(&mut self, order: u8) -> ExclusiveId {
        let c = self.next;
        self.next += 1;
        let mut raw = [0u8; 16];
        raw[..4].copy_from_slice(&0xD46B_0001u32.to_be_bytes());
        raw[4] = order;
        raw[5..12].copy_from_slice(&Self::splitmix64(c).to_be_bytes()[..7]);
        raw[12..].copy_from_slice(&Self::splitmix64(c ^ 0x5EED_5EED).to_be_bytes()[..4]);
        ExclusiveId::force(Id::new(raw).expect("nonzero prefix"))
    }
}

/// F1/F2 — metronome chain and ring: v0 -mp-> v1 -mp-> ...; ring closes the
/// loop; `sources` start nodes carry `msrc` for the K>1 eager-cohort control.
/// Unused on this branch (F1/F2 are path!-based, SKIPped) — kept, like the
/// query candidates, so re-wiring on a path!-bearing engine is a small edit.
#[allow(dead_code)]
fn build_chain(n: usize, ring: bool, sources: usize) -> (TribleSet, Id) {
    let mut ids = Ids::new();
    let mut set = TribleSet::new();
    let nodes: Vec<ExclusiveId> = (0..n).map(|_| ids.mint()).collect();
    for w in nodes.windows(2) {
        set += entity! { &w[0] @ r1_schema::mp: &w[1] };
    }
    if ring {
        set += entity! { &nodes[n - 1] @ r1_schema::mp: &nodes[0] };
    }
    for s in nodes.iter().take(sources.max(1)) {
        set += entity! { s @ r1_schema::msrc: s };
    }
    let start: Id = *nodes[0];
    (set, start)
}

/// F3 — oasis-last: `k` typed entities; the single oasis (order byte 0x00,
/// explored LAST) owns the only complete op->oq path; the first `deaths`
/// entities in exploration order have no `op` edge (cheap deaths); every
/// other entity fans `fan` junk op-edges (expensive depth-2 refutations).
fn build_oasis(k: usize, fan: usize, deaths: usize) -> (TribleSet, Id) {
    let mut ids = Ids::new();
    let mut set = TribleSet::new();
    let oasis = ids.mint_ordered(0x00);
    let y_star = ids.mint_ordered(0x01);
    let z = ids.mint_ordered(0x02);
    set += entity! { &oasis @ r1_schema::otype: &oasis };
    set += entity! { &oasis @ r1_schema::op: &y_star };
    set += entity! { &y_star @ r1_schema::oq: &z };
    for i in 0..k {
        let order = 0xFF - ((i % 0x80) as u8);
        let e = ids.mint_ordered(order.max(0x03));
        set += entity! { &e @ r1_schema::otype: &e };
        if i >= deaths {
            for _ in 0..fan {
                let junk = ids.mint_ordered(0x7F);
                set += entity! { &e @ r1_schema::op: &junk };
            }
        }
    }
    let start: Id = *oasis;
    (set, start)
}

/// F4 — thin functional k-hop chain from a constant: c0 -khop-> x1 ... -> xk.
/// The fusion-or-nothing fixture: nothing accumulates on a functional chain,
/// so per-row pipeline overhead is measured undiluted.
/// Unused on this branch (F4 is path!-based, SKIPped) — kept for re-wiring.
#[allow(dead_code)]
fn build_khop(k: usize) -> (TribleSet, Id) {
    let mut ids = Ids::new();
    let mut set = TribleSet::new();
    let nodes: Vec<ExclusiveId> = (0..=k).map(|_| ids.mint()).collect();
    for w in nodes.windows(2) {
        set += entity! { &w[0] @ r1_schema::khop: &w[1] };
    }
    let start: Id = *nodes[0];
    (set, start)
}

/// F5 — two-route diamond for reconvergence capture: two populations prefer
/// opposite orders of (da, db) then share identical continuations; the eager
/// solver merges them maximally, a width-1 sprint historically reenters.
fn build_diamond(n_per_route: usize) -> TribleSet {
    let mut ids = Ids::new();
    let mut set = TribleSet::new();
    for route in 0..2usize {
        for _ in 0..n_per_route {
            let e = ids.mint();
            let x = ids.mint();
            let y = ids.mint();
            let (fat, thin) = if route == 0 { (3usize, 1usize) } else { (1, 3) };
            for _ in 0..thin {
                set += entity! { &e @ r1_schema::da: &x };
            }
            for _ in 0..fat {
                let alt = ids.mint();
                set += entity! { &e @ r1_schema::db: &alt };
            }
            set += entity! { &e @ r1_schema::db: &y };
        }
    }
    set
}

// ---------------------------------------------------------------------------
// Synthetic DBLP-shaped dataset (NO-PILE fallback). Emits facts under the
// SAME content-derived attributes the importer assigns, so the wired query
// matrix applies unchanged. Deterministic: Ids/splitmix only.
// ---------------------------------------------------------------------------

/// Build ~`target` tribles of DBLP-shaped data. Returns the set and the
/// root stream of the first relatedStream chain (the q2 fixed subject).
fn build_dblp_shaped(target: usize, qa: &DblpAttrs) -> (TribleSet, Id) {
    let rdf_type = qa.rdf_type.clone();
    let has_signature = qa.has_signature.clone();
    let published_in_stream = qa.published_in_stream.clone();
    let number_of_creators = qa.number_of_creators.clone();
    let related_stream = qa.related_stream.clone();

    let mut ids = Ids::new();
    let mut set = TribleSet::new();

    // A handful of classes; streams chained 32-long for the q2 closure.
    let classes: Vec<ExclusiveId> = (0..4).map(|_| ids.mint()).collect();
    let stream_class = ids.mint();

    let streams = (target / 50).clamp(16, 65_536);
    let stream_ids: Vec<ExclusiveId> = (0..streams).map(|_| ids.mint()).collect();
    for s in &stream_ids {
        set += entity! { s @ rdf_type: &stream_class };
    }
    for chunk in stream_ids.chunks(32) {
        for w in chunk.windows(2) {
            set += entity! { &w[0] @ related_stream: &w[1] };
        }
    }
    // ~8 self-typed entities so q5 has controlled, nonzero rows.
    for _ in 0..8 {
        let e = ids.mint();
        set += entity! { &e @ rdf_type: &e };
    }

    // 4 tribles per publication: type, stream, signature, creator count.
    let overhead = set.len();
    let pubs = target.saturating_sub(overhead).max(4) / 4;
    for j in 0..pubs {
        let p = ids.mint();
        let sig = ids.mint();
        set += entity! { &p @ rdf_type: &classes[j % classes.len()] };
        set += entity! { &p @ published_in_stream: &stream_ids[j % streams] };
        set += entity! { &p @ has_signature: &sig };
        set += entity! { &p @ number_of_creators: ((j % 10) + 1) as i128 };
    }
    let root: Id = *stream_ids[0];
    (set, root)
}

// ---------------------------------------------------------------------------
// Pile checkout (rung -> k mapping + Workspace::checkout).
// ---------------------------------------------------------------------------

type CommitHandle = Inline<Handle<SimpleArchive>>;

/// Walk a linear branch parents-first (oldest-first). Vendored from
/// sparqloscope-bench; uses only `repo::parent` facts.
fn commit_chain(
    reader: &triblespace_core::repo::pile::PileReader,
    head: CommitHandle,
) -> Vec<(CommitHandle, TribleSet)> {
    let mut chain = Vec::new();
    let mut cursor = Some(head);
    while let Some(handle) = cursor {
        let meta: TribleSet = reader.get(handle).expect("read commit metadata");
        let parents: Vec<CommitHandle> = find!(
            (p: Inline<Handle<SimpleArchive>>),
            pattern!(&meta, [{ repo::parent: ?p }])
        )
        .map(|(p,)| p)
        .collect();
        chain.push((handle, meta));
        cursor = match parents[..] {
            [] => None,
            [p] => Some(p),
            _ => panic!("merge commit in data branch (expected a linear chain)"),
        };
    }
    chain.reverse();
    chain
}

/// Select one exact asserted branch identity by its resolved name and return
/// its single complete tip. A divergent or incomplete frontier would change
/// the linear checkout workload, so the benchmark refuses to guess a head.
fn asserted_branch(pile: &mut Pile, branch: Option<&str>) -> (String, CommitHandle) {
    let snapshot = pile
        .assertion_snapshot()
        .expect("snapshot branch assertions");
    let mut identities: Vec<BranchIdentity> = snapshot
        .iter()
        .map(|assertion| *assertion.identity())
        .collect();
    identities.sort_unstable();
    identities.dedup();

    let mut reader = pile.reader().expect("pile reader");
    let mut named = Vec::new();
    for identity in identities {
        let Ok(name): Result<anybytes::View<str>, _> = reader.get(identity.name()) else {
            continue;
        };
        named.push((identity, name.as_ref().to_owned()));
    }

    let candidates: Vec<_> = match branch {
        Some(want) => named.into_iter().filter(|(_, name)| name == want).collect(),
        None => named
            .into_iter()
            .filter(|(_, name)| name != "manifest")
            .collect(),
    };
    let [(identity, branch_name)] = candidates.as_slice() else {
        let names: Vec<_> = candidates.iter().map(|(_, name)| name).collect();
        match branch {
            Some(want) => panic!(
                "expected exactly one asserted branch named {want:?}, found {} ({names:?})",
                candidates.len()
            ),
            None => panic!(
                "cannot auto-pick a data branch ({} non-manifest exact identities: {names:?}) -- pass --branch",
                candidates.len()
            ),
        }
    };

    let resolution = repo::branch_frontier::resolve_branch(&snapshot, identity, &mut reader)
        .expect("resolve asserted data branch");
    let head = match resolution {
        BranchResolution::Complete(frontier) => match frontier.resolved_head() {
            ResolvedHead::Existing(head) => head,
            ResolvedHead::Synthetic(_) => {
                panic!("branch {branch_name:?} has a divergent frontier; expected one linear tip")
            }
        },
        other => panic!("branch {branch_name:?} is not completely replicated: {other:?}"),
    };
    (branch_name.clone(), head)
}

/// Open the pile, resolve the data branch, map the rung to k commits, and
/// measure `Workspace::checkout` of those commits. Returns the checked-out
/// set (prefix-carved for sub-first-chunk rungs; chunk-boundary-snapped when
/// `chunk_aligned`), the checkout samples, and the identity count.
fn pile_checkout(
    path: &std::path::Path,
    branch: Option<&str>,
    rung: usize,
    chunk_aligned: bool,
    iters: usize,
    warmup: usize,
) -> (TribleSet, Vec<f64>, usize) {
    let mut pile = Pile::open(path).expect("open pile");
    pile.refresh().expect("load pile records");
    let (branch_name, head) = asserted_branch(&mut pile, branch);
    let reader = pile.reader().expect("pile reader");
    let chain = commit_chain(&reader, head);

    // Rung -> k: one walk, per-commit tribles = SimpleArchive blob len / 64.
    let mut handles: Vec<CommitHandle> = Vec::new();
    let mut cum: Vec<usize> = Vec::new();
    let mut total = 0usize;
    for (handle, meta) in &chain {
        let contents: Vec<CommitHandle> = find!(
            (c: Inline<Handle<SimpleArchive>>),
            pattern!(meta, [{ repo::content: ?c }])
        )
        .map(|(c,)| c)
        .collect();
        let [content] = contents[..] else { continue }; // skip empty commits
        let blob: Blob<SimpleArchive> = reader.get(content).expect("read content blob");
        total += blob.bytes.len() / 64;
        handles.push(*handle);
        cum.push(total);
    }
    assert!(
        !handles.is_empty(),
        "branch {branch_name:?} has no content commits"
    );
    let (k, carve) = if chunk_aligned {
        (snap_to_chunk(&cum, rung), None)
    } else if rung < cum[0] {
        (1, Some(rung))
    } else {
        match cum.iter().position(|&c| c >= rung) {
            Some(idx) => (idx + 1, None),
            None => {
                println!(
                    "note     : rung {rung} exceeds pile total ~{total} tribles; using the full chain"
                );
                (handles.len(), None)
            }
        }
    };
    if chunk_aligned {
        println!(
            "rung     : target {rung} -> snapped {} (chunk-aligned, k={k}/{} commits) on branch {branch_name:?}",
            cum[k - 1],
            handles.len()
        );
    } else {
        println!(
            "rung     : target {rung} -> k={k}/{} commits (~{} cumulative tribles{}) on branch {branch_name:?}",
            handles.len(),
            cum[k - 1],
            carve.map(|n| format!(", carving a {n}-trible sorted prefix")).unwrap_or_default()
        );
    }

    // Workspace::checkout, min-statistic over warmed iterations. NOTE:
    // Repository::new appends one commit-metadata blob record to the pile —
    // run against a clonefile copy (see module docs).
    let mut repo = Repository::new(pile, SigningKey::generate(&mut OsRng), TribleSet::new())
        .expect("create repository view");
    let mut ws = repo
        .create_workspace("benchmark-checkout")
        .expect("create detached checkout workspace");
    let mut samples = Vec::new();
    let mut out: Option<TribleSet> = None;
    let mut ident: Option<usize> = None;
    for i in 0..(warmup + iters) {
        let recording = i >= warmup;
        let t = Instant::now();
        let co = ws.checkout(&handles[..k]).expect("checkout");
        let mut set = co.into_facts();
        if let Some(n) = carve {
            let mut prefix = TribleSet::new();
            for t in set.iter().take(n) {
                prefix.insert(t);
            }
            set = prefix;
        }
        if recording {
            samples.push(t.elapsed().as_secs_f64() * 1000.0);
        }
        match ident {
            None => ident = Some(set.len()),
            Some(expected) if expected != set.len() => {
                println!(
                    "WORKLOAD IDENTITY VIOLATION (checkout): iter {i} saw {} tribles, expected {expected}",
                    set.len()
                );
                std::process::exit(3);
            }
            _ => {}
        }
        out = Some(set);
    }
    drop(ws);
    repo.close().expect("close pile");
    (
        out.expect("at least one iteration"),
        samples,
        ident.unwrap_or(0),
    )
}

// ---------------------------------------------------------------------------
// Measurement report (format-stable: sweep parsers key on these lines).
// ---------------------------------------------------------------------------

fn pct(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let i = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[i]
}

/// Report one measure. Returns `false` when the floor is unstable between
/// the first and second half of the run — the "no signal" verdict.
fn report(name: &str, mut s: Vec<f64>) -> bool {
    if s.len() < 8 {
        println!(
            "  {name:<14} NO SIGNAL (only {} samples, need >= 8)",
            s.len()
        );
        return false;
    }
    let half = s.len() / 2;
    let (a, b) = s.split_at(half);
    let fa = a.iter().cloned().fold(f64::INFINITY, f64::min);
    let fb = b.iter().cloned().fold(f64::INFINITY, f64::min);
    let drift = (fa - fb).abs() / fa.min(fb);
    s.sort_by(|x, y| x.partial_cmp(y).unwrap());
    println!(
        "  {name:<14} min {:9.3}  p50 {:9.3}  p95 {:9.3}  max {:9.3}  n={:<5} floor-drift {:5.1}%",
        s[0],
        pct(&s, 0.50),
        pct(&s, 0.95),
        s[s.len() - 1],
        s.len(),
        drift * 100.0
    );
    if drift > 0.10 {
        println!(
            "  {name:<14} NO SIGNAL (floor moved {:.1}% between halves)",
            drift * 100.0
        );
        return false;
    }
    true
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

struct Cfg {
    pile: Option<std::path::PathBuf>,
    branch: Option<String>,
    rung: usize,
    chunk_aligned: bool,
    iters: usize,
    warmup: usize,
    build_iters: usize,
    build_warmup: usize,
    max_ram: usize,
    range_min: i128,
    chain_n: usize,
    ring_n: usize,
    oasis_k: usize,
    oasis_fan: usize,
    khop_k: usize,
    diamond_n: usize,
}

fn parse_size(s: &str) -> Option<usize> {
    let (num, mul) = match s.chars().last()? {
        'k' | 'K' => (&s[..s.len() - 1], 1_000),
        'M' => (&s[..s.len() - 1], 1_000_000),
        'G' => (&s[..s.len() - 1], 1_000_000_000),
        _ => (s, 1),
    };
    num.parse::<usize>().ok().map(|n| n * mul)
}

fn parse_cfg() -> Cfg {
    let mut cfg = Cfg {
        pile: None,
        branch: None,
        rung: 1_000_000,
        chunk_aligned: false,
        iters: 12,
        warmup: 3,
        build_iters: 8,
        build_warmup: 2,
        max_ram: 20_000_000,
        range_min: 2,
        chain_n: 20_000,
        ring_n: 5_000,
        oasis_k: 4_000,
        oasis_fan: 32,
        khop_k: 256,
        diamond_n: 256,
    };
    fn take_size(args: &[String], i: &mut usize) -> usize {
        *i += 1;
        args.get(*i)
            .and_then(|v| parse_size(v))
            .unwrap_or_else(|| panic!("{} needs a size argument", args[*i - 1]))
    }
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--pile" => {
                i += 1;
                cfg.pile = Some(args.get(i).expect("--pile needs a path").into());
            }
            "--branch" => {
                i += 1;
                cfg.branch = Some(args.get(i).expect("--branch needs a name").clone());
            }
            "--rung" => cfg.rung = take_size(&args, &mut i),
            "--chunk-aligned" => cfg.chunk_aligned = true,
            "--iters" => cfg.iters = take_size(&args, &mut i),
            "--warmup" => cfg.warmup = take_size(&args, &mut i),
            "--build-iters" => cfg.build_iters = take_size(&args, &mut i),
            "--build-warmup" => cfg.build_warmup = take_size(&args, &mut i),
            "--max-ram" => cfg.max_ram = take_size(&args, &mut i),
            "--range-min" => {
                i += 1;
                cfg.range_min = args
                    .get(i)
                    .and_then(|v| v.parse().ok())
                    .expect("--range-min needs an integer");
            }
            "--chain" => cfg.chain_n = take_size(&args, &mut i),
            "--ring" => cfg.ring_n = take_size(&args, &mut i),
            "--oasis" => cfg.oasis_k = take_size(&args, &mut i),
            "--fan" => cfg.oasis_fan = take_size(&args, &mut i),
            "--khop" => cfg.khop_k = take_size(&args, &mut i),
            "--diamond" => cfg.diamond_n = take_size(&args, &mut i),
            other => println!("note     : skipping unrecognized arg {other:?}"),
        }
        i += 1;
    }
    cfg
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

fn main() {
    let cfg = parse_cfg();
    println!("engine   : current (query)");
    println!(
        "config   : rung={} iters={} warmup={} build_iters={} build_warmup={} max_ram={} range_min={}",
        cfg.rung, cfg.iters, cfg.warmup, cfg.build_iters, cfg.build_warmup, cfg.max_ram, cfg.range_min
    );
    println!(
        "fixtures : chain={} ring={} oasis_k={} fan={} khop={} diamond={}",
        cfg.chain_n, cfg.ring_n, cfg.oasis_k, cfg.oasis_fan, cfg.khop_k, cfg.diamond_n
    );

    let qa = DblpAttrs::derive();
    let mut all: Vec<(String, Outcome)> = Vec::new();

    // -- CHECKOUT / data acquisition ---------------------------------------
    // Guarded: at commits where the pile/checkout surface drifted this phase
    // may panic; the Harkonnen fixtures further down carry no data
    // dependency and must still be measured.
    let dataset: Option<(TribleSet, Id)> = match &cfg.pile {
        Some(path) => match quiet_catch(|| {
            pile_checkout(
                path,
                cfg.branch.as_deref(),
                cfg.rung,
                cfg.chunk_aligned,
                cfg.build_iters,
                cfg.build_warmup,
            )
        }) {
            Ok((set, samples, tribles)) => {
                println!("dataset  : pile checkout, {tribles} tribles");
                all.push(("checkout".to_owned(), Outcome::Samples(samples)));
                Some((set, uri_to_id_pure(STREAM_CONF_DAMP)))
            }
            Err(msg) => {
                all.push(("checkout".to_owned(), Outcome::Panic(msg)));
                println!(
                    "note     : checkout panicked — skipping the query matrix, fixtures still run"
                );
                None
            }
        },
        None => {
            println!(
                "  {:<14} SKIP (no pile — synthetic in-process data)",
                "checkout"
            );
            match quiet_catch(|| build_dblp_shaped(cfg.rung, &qa)) {
                Ok((set, root)) => {
                    println!("dataset  : synthetic DBLP-shaped, {} tribles", set.len());
                    Some((set, root))
                }
                Err(msg) => {
                    all.push(("synthetic".to_owned(), Outcome::Panic(msg)));
                    println!(
                        "note     : synthetic build panicked — skipping the query matrix, fixtures still run"
                    );
                    None
                }
            }
        }
    };

    // Identity-tuple state (PANIC sentinels when a phase never produced it).
    let mut tribles = PANIC_COUNT;
    let mut set_counts = [PANIC_COUNT; 4];
    // Never assigned on this branch: q2 cannot run (no path!), so the
    // "never ran" sentinel is q2's permanent identity value here.
    let q2_count = PANIC_COUNT;
    let mut arch_state = "skip";

    if let Some((set, _q2_root)) = dataset {
        tribles = set.len();

        // -- BUILD-RAM -----------------------------------------------------
        let arch: Option<SuccinctArchive<OrderedUniverse>> = if set.len() <= cfg.max_ram {
            match quiet_catch(|| {
                let mut samples = Vec::new();
                let mut arch = None;
                for i in 0..(cfg.build_warmup + cfg.build_iters) {
                    let recording = i >= cfg.build_warmup;
                    let t = Instant::now();
                    let a: SuccinctArchive<OrderedUniverse> = (&set).into();
                    if recording {
                        samples.push(t.elapsed().as_secs_f64() * 1000.0);
                    }
                    arch = Some(a);
                }
                (samples, arch)
            }) {
                Ok((samples, arch)) => {
                    all.push(("build_ram".to_owned(), Outcome::Samples(samples)));
                    arch
                }
                Err(msg) => {
                    all.push(("build_ram".to_owned(), Outcome::Panic(msg)));
                    arch_state = "panic";
                    None
                }
            }
        } else {
            println!(
                "  {:<14} SKIP ({} tribles > --max-ram {})",
                "build_ram",
                set.len(),
                cfg.max_ram
            );
            None
        };

        // -- QUERY matrix --------------------------------------------------
        let (set_outcomes, counts) =
            measure_queries(&set, "set", &qa, cfg.range_min, cfg.iters, cfg.warmup);
        set_counts = counts;
        all.extend(set_outcomes);
        // q2 is path!-based and cannot be expressed on the June-protocol
        // engine — explicit SKIP; q2 keeps the "never ran" sentinel in the
        // identity line. See the adaptation note in the module docs.
        println!(
            "  {:<14} SKIP (no path!/RegularPathConstraint on the June-protocol engine)",
            "q2_set"
        );

        if let Some(arch) = &arch {
            let (outcomes, counts) =
                measure_queries(arch, "arch", &qa, cfg.range_min, cfg.iters, cfg.warmup);
            all.extend(outcomes);
            if !counts_match(counts, set_counts) {
                println!(
                    "WORKLOAD IDENTITY VIOLATION (arch vs set): archive returned {counts:?}, set returned {set_counts:?}"
                );
                std::process::exit(3);
            }
            arch_state = "ok";
        }

        #[cfg(feature = "gpu")]
        {
            if arch.is_some() {
                // The resident wrapper consumes its archive; build a second
                // one from the same set so the CPU arm keeps its own. Init +
                // warm are guarded as one phase (`gpu_init`): a panicking
                // device must not take the CPU measures with it.
                match quiet_catch(|| {
                    let gpu_arch: SuccinctArchive<OrderedUniverse> = (&set).into();
                    let wgpu_arch =
                        WgpuSuccinctArchive::new(gpu_arch).expect("resident archive enqueue");
                    // Warm the device: the first find! pays device init +
                    // shader compilation; not the steady state.
                    let _ = measure_queries(&wgpu_arch, "gpu-warm", &qa, cfg.range_min, 1, 1);
                    wgpu_arch
                }) {
                    Ok(wgpu_arch) => {
                        let (outcomes, counts) = measure_queries(
                            &wgpu_arch,
                            "gpu",
                            &qa,
                            cfg.range_min,
                            cfg.iters,
                            cfg.warmup,
                        );
                        all.extend(outcomes);
                        if !counts_match(counts, set_counts) {
                            println!(
                                "WORKLOAD IDENTITY VIOLATION (gpu vs set): GPU returned {counts:?}, set returned {set_counts:?}"
                            );
                            std::process::exit(3);
                        }
                    }
                    Err(msg) => {
                        all.push(("gpu_init".to_owned(), Outcome::Panic(msg)));
                    }
                }
            } else {
                println!(
                    "  {:<14} SKIP (build_ram gate — no archive to wrap)",
                    "q*_gpu"
                );
            }
        }
    }

    // -- Harkonnen F1..F5 --------------------------------------------------
    // F1 (chain), F2 (ring), and F4 (k-hop) are path!-based transitive-
    // closure fixtures; without path! on the June-protocol engine they
    // cannot be expressed, so their slots SKIP explicitly below and their
    // sets are never built. F3/F5 are pattern!-join fixtures — unchanged.
    let (oasis, _o0) = build_oasis(cfg.oasis_k, cfg.oasis_fan, 20);
    let diamond = build_diamond(cfg.diamond_n);

    let mut f_samples: Vec<Vec<f64>> = vec![Vec::new(); 10];
    let mut f_panicked: [Option<String>; 10] = Default::default();
    let mut f_ident: [Option<usize>; 5] = [None; 5];
    for i in 0..(cfg.warmup + cfg.iters) {
        let recording = i >= cfg.warmup;
        let mut counts = [PANIC_COUNT; 5];

        // F1 metronome chain / F2 ring: path!-based — SKIPped on this
        // branch (slots 0..=3 stay empty; see the emission loop below).

        // F3 oasis-last: 3-way join, adversarial exploration order.
        timed_guarded(&mut f_panicked[4], &mut f_samples[4], recording, || {
            find!(
                (e: Inline<GenId>, y: Inline<GenId>, z: Inline<GenId>),
                and!(
                    pattern!(&oasis, [{ ?e @ r1_schema::otype: ?e }]),
                    pattern!(&oasis, [{ ?e @ r1_schema::op: ?y }]),
                    pattern!(&oasis, [{ ?y @ r1_schema::oq: ?z }]),
                )
            )
            .next()
        });
        if let Some(n) = timed_guarded(&mut f_panicked[5], &mut f_samples[5], recording, || {
            find!(
                (e: Inline<GenId>, y: Inline<GenId>, z: Inline<GenId>),
                and!(
                    pattern!(&oasis, [{ ?e @ r1_schema::otype: ?e }]),
                    pattern!(&oasis, [{ ?e @ r1_schema::op: ?y }]),
                    pattern!(&oasis, [{ ?y @ r1_schema::oq: ?z }]),
                )
            )
            .count()
        }) {
            counts[2] = n;
        }

        // F4 thin k-hop functional chain: path!-based — SKIPped on this
        // branch (slots 6..=7 stay empty; see the emission loop below).

        // F5 two-route diamond (reconvergence capture).
        timed_guarded(&mut f_panicked[8], &mut f_samples[8], recording, || {
            find!(
                (e: Inline<GenId>, x: Inline<GenId>, y: Inline<GenId>),
                and!(
                    pattern!(&diamond, [{ ?e @ r1_schema::da: ?x }]),
                    pattern!(&diamond, [{ ?e @ r1_schema::db: ?y }]),
                )
            )
            .next()
        });
        if let Some(n) = timed_guarded(&mut f_panicked[9], &mut f_samples[9], recording, || {
            find!(
                (e: Inline<GenId>, x: Inline<GenId>, y: Inline<GenId>),
                and!(
                    pattern!(&diamond, [{ ?e @ r1_schema::da: ?x }]),
                    pattern!(&diamond, [{ ?e @ r1_schema::db: ?y }]),
                )
            )
            .count()
        }) {
            counts[4] = n;
        }

        for (k, &n) in counts.iter().enumerate() {
            if n == PANIC_COUNT {
                continue;
            }
            match f_ident[k] {
                None => f_ident[k] = Some(n),
                Some(expected) if expected != n => {
                    println!(
                        "WORKLOAD IDENTITY VIOLATION (fixture F{}): iter {i} saw {n}, expected {expected}",
                        k + 1
                    );
                    std::process::exit(3);
                }
                _ => {}
            }
        }
    }
    let f_keys = [
        "F1-ttfr", "F1-total", "F2-ttfr", "F2-total", "F3-ttfr", "F3-total", "F4-ttfr", "F4-total",
        "F5-ttfr", "F5-total",
    ];
    // F1/F2/F4 (slots 0..=3 and 6..=7) never run on this branch — their
    // measures are path!-based. They SKIP explicitly and keep the "never
    // ran" sentinel in the identity tuple instead of masquerading as 0-row
    // measures (their `f_ident` entries stay `None`, so the sentinel falls
    // out of the same `Some`-gated assignment that serves ran-measures).
    let f_skipped = [
        true, true, true, true, false, false, true, true, false, false,
    ];
    let mut f_final = [PANIC_COUNT; 5];
    for (k, v) in f_ident.iter().enumerate() {
        if let (Some(n), None) = (v, &f_panicked[2 * k + 1]) {
            f_final[k] = *n;
        }
    }
    for (k, ((key, samples), p)) in f_keys.iter().zip(f_samples).zip(f_panicked).enumerate() {
        if f_skipped[k] {
            println!(
                "  {key:<14} SKIP (no path!/RegularPathConstraint on the June-protocol engine)"
            );
            continue;
        }
        let outcome = match p {
            Some(msg) => Outcome::Panic(msg),
            None => Outcome::Samples(samples),
        };
        all.push(((*key).to_owned(), outcome));
    }

    // -- identity + verdicts ----------------------------------------------
    // PANIC_COUNT (usize::MAX) marks measures that panicked (or whose phase
    // never ran): a deterministic panic yields the same sentinel on both
    // sides of a comparison, so runs stay comparable on surviving measures.
    println!(
        "identity : tribles={} q[1,3,4,5]={:?} q2={} arch={} F={:?}  (runs comparable ONLY if ALL match; {} = PANIC sentinel)",
        tribles, set_counts, q2_count, arch_state, f_final, PANIC_COUNT
    );

    let mut signal: Vec<String> = Vec::new();
    let mut no_signal: Vec<String> = Vec::new();
    let mut panicked: Vec<String> = Vec::new();
    for (name, outcome) in all {
        match outcome {
            Outcome::Samples(samples) => {
                if report(&name, samples) {
                    signal.push(name);
                } else {
                    no_signal.push(name);
                }
            }
            Outcome::Panic(msg) => {
                println!("  {name:<14} PANIC ({msg})");
                panicked.push(name);
            }
        }
    }
    println!(
        "SIGNAL   : {}",
        if signal.is_empty() {
            "(none)".to_owned()
        } else {
            signal.join(" ")
        }
    );
    if !no_signal.is_empty() {
        println!("NO-SIGNAL: {}", no_signal.join(" "));
    }
    if !panicked.is_empty() {
        println!("PANIC    : {}", panicked.join(" "));
    }
    if signal.is_empty() {
        eprintln!("VERDICT: NO SIGNAL on any measure — do not compare this run.");
        std::process::exit(4);
    }
    println!(
        "VERDICT  : usable ({}/{} measures with signal, {} panicked)",
        signal.len(),
        signal.len() + no_signal.len(),
        panicked.len()
    );
}
