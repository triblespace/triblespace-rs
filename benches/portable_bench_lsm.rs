//! Portable engine benchmark v2 — LSM (index-home) arm.
//!
//! A SEPARATE bench target on purpose: the index-home API
//! (`append_range` / `SuccinctRollup` / `Manifest` / `UnionArchive`) is
//! tip-era surface that does not exist at older sweep commits. Keeping it in
//! its own `[[bench]]` gives it an INDEPENDENT compilation fate — at commits
//! where the API differs this target BUILD-FAILs honestly while
//! `portable_bench` keeps running. Never fold this file into the main bench.
//!
//! WHAT IT MEASURES:
//!   build_lsm  — streaming commit ranges into physical succinct leaves via
//!                `repo::index_home::append_range` (leaf build + blob puts +
//!                ordered size-tiered carry merges, FANOUT=4), against a
//!                scratch in-memory blob store seeded with the commit-DAG
//!                metadata (merges validate convex range unions against the
//!                DAG). The SOURCE pile is never written — reads only.
//!   q<N>_union — the same wired query matrix as the main bench (q3 = q3b
//!                union-under-join, q5 = q5b witness semijoin — keep
//!                `measure_queries` + the q3b/q5b fns byte-identical with
//!                portable_bench.rs or `q<N>_union` stops being comparable
//!                with `q<N>_set`/`q<N>_arch`), executed over the
//!                `UnionArchive` of the built leaves (per-shard confirm +
//!                cross-shard dedup). q2 is `path!`-based and
//!                TribleSet-only, so it has no union arm.
//!
//! Workload identity: the union-arm counts are gated against the same
//! queries run over the plain unioned TribleSet (untimed) — union-of-leaves
//! must be indistinguishable from the set at the interface.
//!
//! DATASET MODES (mirrors the main bench):
//!   --pile <path>  first k commits of the data branch for the rung target.
//!                  ALWAYS chunk-aligned: the rung SNAPS to the nearest
//!                  cumulative-commit boundary (leaves are per-commit
//!                  physical artifacts, a carved prefix has no commit
//!                  identity to hang a `CommitRange` on) via the SAME
//!                  `snap_to_chunk` the main bench applies under
//!                  `--chunk-aligned`, and the snapped rung + actual trible
//!                  count are printed so sweeps record actual-vs-nominal.
//!                  Opens the pile READ-ONLY (no Repository, no puts).
//!   (no --pile)    the synthetic DBLP-shaped set, split into --chunks
//!                  synthetic commits with real parent-linked metadata in
//!                  the scratch store.
//!
//! USAGE:
//!   cargo bench --bench portable_bench_lsm -- [--pile P] [--branch B]
//!     [--rung 1M] [--iters 12] [--warmup 3] [--build-iters 8]
//!     [--build-warmup 2] [--chunks 16] [--range-min 2]
//!
//! Crash isolation mirrors the main bench: every query measure and every
//! panic-prone phase (build_lsm, manifest attach) runs under `catch_unwind`
//! with the panic hook silenced; a panicking measure reports
//! `  <key>  PANIC (<msg>)`, joins a `PANIC    :` summary line, contributes
//! a `usize::MAX` sentinel to the identity tuple, and is EXCLUDED from the
//! SIGNAL/NO-SIGNAL verdict counts.
//!
//! EXIT CODES: 0 usable (>= 1 measure with SIGNAL; PANIC'd measures are
//! their own outcome, excluded from the verdict), 3 workload-identity
//! violation, 4 no measure had signal. BUILD-FAIL is expressed by
//! compilation (the design point of this file), never emulated at runtime.

use std::time::Instant;

use triblespace_core::blob::encodings::longstring::LongString;
use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
use triblespace_core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::metadata;
use triblespace_core::prelude::inlineencodings::{GenId, I256BE};
use triblespace_core::prelude::*;
use triblespace_core::repo::index_home::{append_range, IndexKind, Manifest, SuccinctRollup};
use triblespace_core::repo::index_range::CommitRange;
use triblespace_core::repo::pile::Pile;
use triblespace_core::repo::{self, PinStore};

// ---------------------------------------------------------------------------
// DBLP vocabulary + content-derived attributes (identical derivation to the
// main bench; see portable_bench.rs for provenance).
// ---------------------------------------------------------------------------

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const DBLP_HAS_SIGNATURE: &str = "https://dblp.org/rdf/schema#hasSignature";
const DBLP_PUBLISHED_IN_STREAM: &str = "https://dblp.org/rdf/schema#publishedInStream";
const DBLP_PUBLISHED_AS_PART_OF: &str = "https://dblp.org/rdf/schema#publishedAsPartOf";
const DBLP_RELATED_STREAM: &str = "https://dblp.org/rdf/schema#relatedStream";
const DBLP_CREATED_BY: &str = "https://dblp.org/rdf/schema#createdBy";
const DBLP_NUMBER_OF_CREATORS: &str = "https://dblp.org/rdf/schema#numberOfCreators";

fn attr<S: InlineEncoding + MetaDescribe>(iri: &str) -> Attribute<S> {
    Attribute::<S>::from(entity! {
        metadata::iri:            iri.to_owned().to_blob().get_handle(),
        metadata::value_encoding: <S as MetaDescribe>::id(),
    })
}

struct DblpAttrs {
    rdf_type: Attribute<GenId>,
    has_signature: Attribute<GenId>,
    published_in_stream: Attribute<GenId>,
    published_as_part_of: Attribute<GenId>,
    related_stream: Attribute<GenId>,
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
            created_by: attr(DBLP_CREATED_BY),
            number_of_creators: attr(DBLP_NUMBER_OF_CREATORS),
        }
    }
}

// ---------------------------------------------------------------------------
// Crash isolation: panics are a fourth outcome (SIGNAL / NO-SIGNAL / SKIP /
// PANIC), never a dead process. Keep these helpers IDENTICAL in
// portable_bench.rs.
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

/// CANDIDATE q3b: union constrained by a GenId join — byte-identical with
/// portable_bench.rs (the wired q3).
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

/// CANDIDATE q5b: witness-set semijoin (EXISTS translation) — byte-identical
/// with portable_bench.rs (the wired q5).
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

/// The wired matrix: q1/q3/q4/q5 over any TriblePattern source — kept
/// byte-identical with portable_bench.rs so `q<N>_union` lines are directly
/// comparable with `q<N>_set`/`q<N>_arch` from `portable_bench` at the same
/// commit. Returns the per-query outcomes (keyed `q<N>_<arm>`) and the
/// result cardinalities for the identity gate (PANIC_COUNT sentinel for
/// panicked queries). Each query runs under its own catch_unwind guard.
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
        // that into a PANIC outcome instead of a dead bench.
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

// ---------------------------------------------------------------------------
// Synthetic DBLP-shaped dataset (same generator as the main bench).
// ---------------------------------------------------------------------------

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
}

fn build_dblp_shaped(target: usize, qa: &DblpAttrs) -> TribleSet {
    let rdf_type = qa.rdf_type.clone();
    let has_signature = qa.has_signature.clone();
    let published_in_stream = qa.published_in_stream.clone();
    let number_of_creators = qa.number_of_creators.clone();
    let related_stream = qa.related_stream.clone();

    let mut ids = Ids::new();
    let mut set = TribleSet::new();

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
    for _ in 0..8 {
        let e = ids.mint();
        set += entity! { &e @ rdf_type: &e };
    }

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
    set
}

// ---------------------------------------------------------------------------
// Commit-chunk acquisition.
// ---------------------------------------------------------------------------

type CommitHandle = Inline<Handle<SimpleArchive>>;

/// One logical range's worth of input: the commit handle, its metadata facts
/// (for the scratch store's DAG), and its content set (the leaf source).
struct Chunk {
    handle: CommitHandle,
    meta: TribleSet,
    content: TribleSet,
}

/// READ-ONLY pile walk: first k commits of the data branch for the rung.
fn pile_chunks(path: &std::path::Path, branch: Option<&str>, rung: usize) -> Vec<Chunk> {
    let mut pile = Pile::open(path).expect("open pile");
    pile.refresh().expect("load pile records");
    let reader = pile.reader().expect("pile reader");

    let branch_ids: Vec<Id> = pile
        .pins()
        .expect("list branches")
        .collect::<Result<Vec<_>, _>>()
        .expect("list branches");
    let mut named: Vec<(Id, String, TribleSet)> = Vec::new();
    for id in branch_ids {
        let Ok(Some(meta_handle)) = pile.head(id) else {
            continue;
        };
        let Ok(meta): Result<TribleSet, _> = reader.get(meta_handle) else {
            continue;
        };
        let handles: Vec<Inline<Handle<LongString>>> = find!(
            (n: Inline<Handle<LongString>>),
            pattern!(&meta, [{ metadata::name: ?n }])
        )
        .map(|(n,)| n)
        .collect();
        let [h] = handles[..] else { continue };
        let Ok(name): Result<anybytes::View<str>, _> = reader.get(h) else {
            continue;
        };
        named.push((id, name.as_ref().to_owned(), meta));
    }
    let (_branch_id, branch_name, branch_meta) = match branch {
        Some(want) => named
            .into_iter()
            .find(|(_, n, _)| n == want)
            .unwrap_or_else(|| panic!("no branch named {want:?} in pile")),
        None => {
            let mut data: Vec<_> = named
                .into_iter()
                .filter(|(_, n, _)| n != "manifest")
                .collect();
            match data.len() {
                1 => data.remove(0),
                n => panic!(
                    "cannot auto-pick a data branch ({n} non-manifest branches: {:?}) — pass --branch",
                    data.iter().map(|(_, n, _)| n.clone()).collect::<Vec<_>>()
                ),
            }
        }
    };

    let heads: Vec<CommitHandle> = find!(
        (c: Inline<Handle<SimpleArchive>>),
        pattern!(&branch_meta, [{ repo::head: ?c }])
    )
    .map(|(c,)| c)
    .collect();
    let [head] = heads[..] else {
        panic!("branch {branch_name:?} has no unique head commit")
    };

    // Oldest-first linear chain walk.
    let mut chain: Vec<(CommitHandle, TribleSet)> = Vec::new();
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

    // Rung -> k: cumulative per-commit trible counts from the content blob
    // LENGTH (SimpleArchive is 64 bytes/trible — the same accounting the
    // main bench uses, no deserialization on the walk), then SNAP to the
    // nearest chunk boundary. Always chunk-aligned: leaves are per-commit
    // physical artifacts, so sub-chunk rungs cannot be carved here.
    let mut entries: Vec<(CommitHandle, TribleSet, CommitHandle)> = Vec::new();
    let mut cum: Vec<usize> = Vec::new();
    let mut total = 0usize;
    for (handle, meta) in chain {
        let contents: Vec<CommitHandle> = find!(
            (c: Inline<Handle<SimpleArchive>>),
            pattern!(&meta, [{ repo::content: ?c }])
        )
        .map(|(c,)| c)
        .collect();
        let [content] = contents[..] else { continue };
        let blob: Blob<SimpleArchive> = reader.get(content).expect("read content blob");
        total += blob.bytes.len() / 64;
        cum.push(total);
        entries.push((handle, meta, content));
    }
    assert!(
        !entries.is_empty(),
        "branch {branch_name:?} has no content commits"
    );
    let k = snap_to_chunk(&cum, rung);
    println!(
        "rung     : target {rung} -> snapped {} (chunk-aligned, k={k}/{} commits) on branch {branch_name:?}",
        cum[k - 1],
        entries.len()
    );
    let chunks: Vec<Chunk> = entries
        .into_iter()
        .take(k)
        .map(|(handle, meta, content)| {
            let set: TribleSet = reader.get(content).expect("read commit content");
            Chunk {
                handle,
                meta,
                content: set,
            }
        })
        .collect();
    // The reader is a snapshot; closing the pile here is safe and keeps the
    // "dropped without close()" warning out of sweep logs.
    pile.close().expect("close pile");
    chunks
}

/// Synthetic commit chain: split the DBLP-shaped set into `n` chunks and
/// mint parent-linked commit metadata for each (content-addressed handles,
/// no store required to NAME them — the scratch store is seeded per
/// iteration).
fn synthetic_chunks(target: usize, n: usize, qa: &DblpAttrs) -> Vec<Chunk> {
    let set = build_dblp_shaped(target, qa);
    let per = set.len().div_ceil(n.max(1));
    let mut ids = Ids::new();
    let mut chunks: Vec<Chunk> = Vec::new();
    let mut current = TribleSet::new();
    let mut parent: Option<CommitHandle> = None;
    let mut flush = |content: TribleSet, parent: &mut Option<CommitHandle>| {
        let content_handle = content.clone().to_blob().get_handle();
        let cid = ids.mint();
        let mut meta: TribleSet = entity! { &cid @ repo::content: content_handle }.into();
        if let Some(p) = *parent {
            meta += entity! { &cid @ repo::parent: p };
        }
        let handle = meta.clone().to_blob().get_handle();
        *parent = Some(handle);
        Chunk {
            handle,
            meta,
            content,
        }
    };
    for t in set.iter() {
        current.insert(t);
        if current.len() >= per {
            chunks.push(flush(
                std::mem::replace(&mut current, TribleSet::new()),
                &mut parent,
            ));
        }
    }
    if current.len() > 0 {
        chunks.push(flush(current, &mut parent));
    }
    println!(
        "dataset  : synthetic DBLP-shaped, {} tribles in {} synthetic commits",
        set.len(),
        chunks.len()
    );
    chunks
}

// ---------------------------------------------------------------------------
// Measurement report (format identical to the main bench).
// ---------------------------------------------------------------------------

fn pct(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let i = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[i]
}

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
// CLI + main
// ---------------------------------------------------------------------------

fn parse_size(s: &str) -> Option<usize> {
    let (num, mul) = match s.chars().last()? {
        'k' | 'K' => (&s[..s.len() - 1], 1_000),
        'M' => (&s[..s.len() - 1], 1_000_000),
        'G' => (&s[..s.len() - 1], 1_000_000_000),
        _ => (s, 1),
    };
    num.parse::<usize>().ok().map(|n| n * mul)
}

fn main() {
    let mut pile: Option<std::path::PathBuf> = None;
    let mut branch: Option<String> = None;
    let mut rung = 1_000_000usize;
    let mut iters = 12usize;
    let mut warmup = 3usize;
    let mut build_iters = 8usize;
    let mut build_warmup = 2usize;
    let mut n_chunks = 16usize;
    let mut range_min = 2i128;

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
                pile = Some(args.get(i).expect("--pile needs a path").into());
            }
            "--branch" => {
                i += 1;
                branch = Some(args.get(i).expect("--branch needs a name").clone());
            }
            "--rung" => rung = take_size(&args, &mut i),
            "--iters" => iters = take_size(&args, &mut i),
            "--warmup" => warmup = take_size(&args, &mut i),
            "--build-iters" => build_iters = take_size(&args, &mut i),
            "--build-warmup" => build_warmup = take_size(&args, &mut i),
            "--chunks" => n_chunks = take_size(&args, &mut i),
            "--range-min" => {
                i += 1;
                range_min = args
                    .get(i)
                    .and_then(|v| v.parse().ok())
                    .expect("--range-min needs an integer");
            }
            other => println!("note     : skipping unrecognized arg {other:?}"),
        }
        i += 1;
    }

    println!("engine   : current (query), LSM index-home arm");
    println!(
        "config   : rung={rung} iters={iters} warmup={warmup} build_iters={build_iters} build_warmup={build_warmup} chunks={n_chunks} range_min={range_min}"
    );

    let qa = DblpAttrs::derive();
    let chunks = match &pile {
        Some(path) => pile_chunks(path, branch.as_deref(), rung),
        None => synthetic_chunks(rung, n_chunks, &qa),
    };

    // -- BUILD-LSM ---------------------------------------------------------
    // Per iteration: a fresh scratch store seeded with the commit metadata
    // (untimed), then the timed streaming loop — leaf build, artifact puts,
    // and carry merges all inside `append_range`. Guarded: at commits where
    // the index-home surface panics (rather than failing to compile) this
    // records a PANIC outcome instead of a dead bench.
    let kind = SuccinctRollup::new();
    let mut all: Vec<(String, Outcome)> = Vec::new();
    let built = quiet_catch(|| {
        let mut samples = Vec::new();
        let mut kept: Option<(MemoryBlobStore, TribleSet)> = None;
        let mut ident: Option<usize> = None;
        for i in 0..(build_warmup + build_iters) {
            let recording = i >= build_warmup;
            let mut store = MemoryBlobStore::default();
            for chunk in &chunks {
                let stored: CommitHandle =
                    store.put(chunk.meta.clone()).expect("seed commit metadata");
                assert_eq!(
                    stored, chunk.handle,
                    "commit metadata handle drifted between pile and scratch store"
                );
            }
            let t = Instant::now();
            let mut head_set = TribleSet::new();
            for chunk in &chunks {
                append_range(
                    &mut store,
                    &kind,
                    &chunk.content,
                    CommitRange::leaf(chunk.handle),
                    &mut head_set,
                )
                .expect("append range");
            }
            if recording {
                samples.push(t.elapsed().as_secs_f64() * 1000.0);
            }
            match ident {
                None => ident = Some(head_set.len()),
                Some(expected) if expected != head_set.len() => {
                    println!(
                        "WORKLOAD IDENTITY VIOLATION (build_lsm): iter {i} manifest has {} tribles, expected {expected}",
                        head_set.len()
                    );
                    std::process::exit(3);
                }
                _ => {}
            }
            kept = Some((store, head_set));
        }
        let (store, head_set) = kept.expect("at least one build iteration");
        (samples, store, head_set, ident.unwrap_or(0))
    });

    // Identity-tuple state (PANIC sentinels when a phase never produced it).
    let mut union_counts = [PANIC_COUNT; 4];
    let mut full_len = PANIC_COUNT;
    let mut manifest_ident = PANIC_COUNT;

    match built {
        Ok((samples, mut store, head_set, ident)) => {
            all.push(("build_lsm".to_owned(), Outcome::Samples(samples)));
            manifest_ident = ident;

            // -- attach + q<N>_union ---------------------------------------
            match quiet_catch(|| {
                let reader = store.reader().expect("scratch store reader");
                let manifest: Manifest<SuccinctRollup> =
                    Manifest::from_tribles(&head_set, &reader, &kind).expect("parse manifest");
                let mut segments: Vec<SuccinctArchive<OrderedUniverse>> = Vec::new();
                for entry in manifest.ranges() {
                    for artifact in entry.artifacts() {
                        segments.push(kind.attach(&reader, artifact).expect("attach segment"));
                    }
                }
                (manifest.ranges().len(), segments)
            }) {
                Ok((n_ranges, segments)) => {
                    println!(
                        "lsm      : {} logical ranges, {} physical segments",
                        n_ranges,
                        segments.len()
                    );
                    let union = SuccinctRollup::union(&segments);
                    let (union_outcomes, counts) =
                        measure_queries(&union, "union", &qa, range_min, iters, warmup);
                    union_counts = counts;
                    all.extend(union_outcomes);

                    // Identity gate: union-of-leaves must equal the plain set
                    // at the interface (untimed reference run; PANIC
                    // sentinels are skipped — a query that panicked on
                    // either side has no count to disagree with).
                    let mut full = TribleSet::new();
                    for chunk in &chunks {
                        full.union(chunk.content.clone());
                    }
                    full_len = full.len();
                    let (_, set_counts) = measure_queries(&full, "set-ref", &qa, range_min, 1, 0);
                    if !counts_match(union_counts, set_counts) {
                        println!(
                            "WORKLOAD IDENTITY VIOLATION (union vs set): union returned {union_counts:?}, set returned {set_counts:?}"
                        );
                        std::process::exit(3);
                    }
                }
                Err(msg) => {
                    all.push(("attach".to_owned(), Outcome::Panic(msg)));
                    println!("note     : manifest attach panicked — no union arm");
                }
            }
        }
        Err(msg) => {
            all.push(("build_lsm".to_owned(), Outcome::Panic(msg)));
            println!("note     : build_lsm panicked — no leaves to query");
        }
    }

    println!(
        "identity : tribles={} q[1,3,4,5]={:?} manifest={}  (runs comparable ONLY if ALL match; {} = PANIC sentinel)",
        full_len, union_counts, manifest_ident, PANIC_COUNT
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
