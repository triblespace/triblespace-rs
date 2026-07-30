//! Results ledger: canonical telemetry sessions/spans plus bench
//! outcome entities, written DIRECTLY (no tracing) through the stable
//! LEDGER dependency (`triblespace` 0.47) — measurement I/O never
//! depends on the era of the subject.
//!
//! DISCIPLINE: this module uses ONLY the ledger's umbrella surface
//! (`triblespace::prelude`, `triblespace::core::…`). The umbrella
//! macros expand to absolute `::triblespace::core` paths, which in
//! this crate resolve to the ledger; the core-macro flavor (what the
//! subject-side modules use) expands to `::triblespace_core`, which
//! resolves to the SUBJECT's core and must never appear here.
//!
//! The telemetry schema ids are declared byte-for-byte identical to
//! `june-on-tip/src/telemetry.rs` — the minted ids are the contract;
//! GORBIE's telemetry-viewer renders the axis.

use std::path::Path;
use std::sync::LazyLock;

use anyhow::{anyhow, bail, Context, Result};
use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;

use triblespace::core::blob::encodings::simplearchive::SimpleArchive;
use triblespace::core::metadata;
use triblespace::core::repo::{self, branch, PushResult, Workspace};
use triblespace::core::repo::pile::Pile;
use triblespace::prelude::blobencodings::LongString;
use triblespace::prelude::inlineencodings::{GenId, Handle, ShortString};
use triblespace::prelude::*;

/// Canonical telemetry attributes — byte-for-byte the ids of
/// `triblespace::telemetry::schema` (read from
/// `june-on-tip/src/telemetry.rs`).
pub mod tele {
    use triblespace::prelude::blobencodings::LongString;
    use triblespace::prelude::inlineencodings::{GenId, Handle, ShortString, U256BE};
    use triblespace::prelude::*;

    attributes! {
        "3E062AA7E3554C8F2DB94883CE639BFE" as pub session: GenId;
        "146E5AA2F7CB3D8B654BC7742A13CAB3" as pub parent: GenId;
        "CCB0147D20C4C6FCAC0E3D87FAFF71D1" as pub name: Handle<LongString>;
        "8A4BE2C4D0E90D2B9EE0E1A07ECA2CFA" as pub category: ShortString;
        "E11A84A30CC112650DC860B66B8BD8A9" as pub begin_ns: U256BE;
        "2786FA563372FB6EF469EC7710719A49" as pub end_ns: U256BE;
        "7593602383D0B0D21BBE382A67E5BD9F" as pub duration_ns: U256BE;
        "7E96DD9A0B5002796B645ED25F5E99AC" as pub source: Handle<LongString>;
    }
}

/// Bench decorations — minted for this suite (`trible genid`, never
/// guessed): session provenance (commit/engine/config) and per-measure
/// outcome entities (of_run/workload/outcome/rows).
pub mod bench {
    use triblespace::prelude::blobencodings::LongString;
    use triblespace::prelude::inlineencodings::{GenId, Handle, ShortString, U256BE};
    use triblespace::prelude::*;

    attributes! {
        /// Subject git rev (short=12) the session measured.
        "2C96F6429B3E772B15A0AB630C2B394F" as pub commit: ShortString;
        /// Engine label (--label) naming the subject on the axis.
        "2C899A2497B9565328A42A44996BD6A1" as pub engine: ShortString;
        /// Full run configuration: CLI + dataset + suite crate version.
        "8A3D02A290208D39DC18C69FAF38F1E1" as pub config: Handle<LongString>;
        /// Outcome entity -> its session.
        "75342A5BCA3BAD27285C5B76DB22CFCF" as pub of_run: GenId;
        /// Outcome entity -> measure key (e.g. "harkonnen/F5/total").
        "81ADFDA915ABA850EE23FEE3B88FC02F" as pub workload: Handle<LongString>;
        /// signal | skip:<reason> | panic:<reason> | gate_fail:<reason>.
        "5ACAF4FD8D71F0205694F646520707B5" as pub outcome: ShortString;
        /// Result cardinality of the measure, where meaningful.
        "B5A378BDC1A7F1C4576B2DC6902B5995" as pub rows: U256BE;
    }
}

/// Tag id of a telemetry session entity.
pub static KIND_SESSION: LazyLock<Id> =
    LazyLock::new(|| Id::from_hex("2701F7019B865D461F0169B1303026D6").expect("kind_session id"));
/// Tag id of a telemetry span entity.
pub static KIND_SPAN: LazyLock<Id> =
    LazyLock::new(|| Id::from_hex("0AF9FEB9A2BFEB1BE8A8229829181085").expect("kind_span id"));
/// The results branch every suite run commits to.
pub static RESULTS_BRANCH: LazyLock<Id> =
    LazyLock::new(|| Id::from_hex("F6D99F76BC15E78C0BBD44F9D28A0C0A").expect("results branch id"));

/// Clip a string to a ShortString-safe payload: first line, NULs
/// stripped, at most 32 bytes on a char boundary.
fn clip32(s: &str) -> String {
    let line = s.lines().next().unwrap_or("").replace('\0', "");
    let mut end = line.len().min(32);
    while !line.is_char_boundary(end) {
        end -= 1;
    }
    line[..end].to_owned()
}

/// One open results pile + workspace on the results branch. All facts
/// accumulate in a pending set; `finish` commits, pushes, and closes.
pub struct ResultsLedger {
    repo: Repository<Pile>,
    ws: Workspace<Pile>,
    session: Id,
    pending: TribleSet,
}

impl ResultsLedger {
    /// Open (creating file and results branch as needed) and start a
    /// session entity decorated with the bench provenance attributes.
    pub fn open(path: &Path, commit: &str, label: &str, config: &str) -> Result<Self> {
        if !path.exists() {
            std::fs::OpenOptions::new()
                .create_new(true)
                .append(true)
                .open(path)
                .with_context(|| format!("create results pile {}", path.display()))?;
        }
        let mut pile =
            Pile::open(path).map_err(|e| anyhow!("open results pile {}: {e:?}", path.display()))?;
        pile.refresh().map_err(|e| anyhow!("load results pile: {e:?}"))?;
        let branch_exists = pile
            .head(*RESULTS_BRANCH)
            .map_err(|e| anyhow!("read results branch head: {e:?}"))?
            .is_some();

        // Commit metadata: the schema self-description — provenance as
        // exhaust, not product.
        let mut described = tele::describe();
        described += bench::describe();
        let meta: TribleSet = described.into();

        let mut repo = Repository::new(pile, SigningKey::generate(&mut OsRng), meta)
            .map_err(|e| anyhow!("open repository on results pile: {e:?}"))?;

        if !branch_exists {
            // `Repository::create_branch` mints a fresh id; the suite
            // needs the FIXED minted branch id, so replicate its steps
            // (unsigned branch metadata, as speced).
            let name_blob: Blob<LongString> = "tribleset-bench".to_owned().to_blob();
            let name_handle = name_blob.get_handle();
            repo.storage_mut()
                .put::<LongString, _>(name_blob)
                .map_err(|e| anyhow!("store branch name blob: {e:?}"))?;
            let branch_set = branch::branch_unsigned(*RESULTS_BRANCH, name_handle, None);
            let branch_handle = repo
                .storage_mut()
                .put(branch_set.to_blob())
                .map_err(|e| anyhow!("store branch metadata blob: {e:?}"))?;
            match repo
                .storage_mut()
                .update(*RESULTS_BRANCH, None, Some(branch_handle))
                .map_err(|e| anyhow!("create results branch: {e:?}"))?
            {
                PushResult::Success() => {}
                PushResult::Conflict(_) => bail!("results branch creation raced another writer"),
            }
        }

        let mut ws = repo
            .pull(*RESULTS_BRANCH)
            .map_err(|e| anyhow!("pull results branch: {e:?}"))?;

        let session_owner = ufoid();
        let session = *session_owner;
        let name_handle = ws.put("tribleset-bench".to_string());
        let config_handle = ws.put(config.to_string());
        let commit_short = clip32(commit);
        let label_short = clip32(label);
        let pending = entity! { &session_owner @
            metadata::tag: *KIND_SESSION,
            tele::category: "session",
            tele::name: name_handle,
            tele::begin_ns: 0u64,
            bench::commit: commit_short.as_str(),
            bench::engine: label_short.as_str(),
            bench::config: config_handle,
        }
        .into();

        Ok(Self {
            repo,
            ws,
            session,
            pending,
        })
    }

    /// The session entity id (for logging).
    pub fn session(&self) -> Id {
        self.session
    }

    /// Record one measured iteration as a telemetry span.
    pub fn span(&mut self, name: &str, begin_ns: u64, duration_ns: u64) {
        let span_owner = ufoid();
        let name_handle = self.ws.put(name.to_string());
        self.pending += TribleSet::from(entity! { &span_owner @
            metadata::tag: *KIND_SPAN,
            tele::session: self.session,
            tele::category: "bench",
            tele::name: name_handle,
            tele::begin_ns: begin_ns,
            tele::end_ns: begin_ns + duration_ns,
            tele::duration_ns: duration_ns,
        });
    }

    /// Record a per-measure outcome entity.
    pub fn outcome(&mut self, workload: &str, outcome: &str, rows: Option<u64>) {
        let outcome_owner = ufoid();
        let workload_handle = self.ws.put(workload.to_string());
        let outcome_short = clip32(outcome);
        self.pending += TribleSet::from(entity! { &outcome_owner @
            bench::of_run: self.session,
            bench::workload: workload_handle,
            bench::outcome: outcome_short.as_str(),
        });
        if let Some(r) = rows {
            self.pending += TribleSet::from(entity! { &outcome_owner @ bench::rows: r });
        }
    }

    /// Commit and push whatever has accumulated, keeping the session open.
    ///
    /// # Why an interrupted run used to lose everything
    ///
    /// Measures accumulated in `pending` and were committed exactly once, in
    /// [`finish`]. But each measure PRINTS its console line the moment it
    /// completes, so a run that is killed after two hours of printing results
    /// leaves nothing on disk — the log text and the pile disagree completely.
    ///
    /// That is not hypothetical. On 2026-07-30 a query phase was stopped to
    /// hand the host to another agent after ~21 minutes of arms that had all
    /// printed; the results pile contained only the *previous* phase. I then
    /// reported those printed measures as durable, because watching them
    /// scroll past is indistinguishable from watching them persist.
    ///
    /// Called from a measure's `emit`, this makes the invariant a reader
    /// naturally assumes actually true: *if it printed, it is on disk.*
    ///
    /// # What an interrupted run then looks like
    ///
    /// Checkpoints do not write the session's `end_ns` — only [`finish`] does.
    /// So an interrupted run is not merely present-but-short, it is
    /// *identifiably incomplete*: a session with spans and no end. That is
    /// strictly better than the two alternatives, losing it silently or
    /// having it look finished.
    ///
    /// Cost is one commit and push per measure, against measures that take
    /// seconds to minutes each. Empty checkpoints are skipped so a gated or
    /// skipped measure does not append an empty commit.
    pub fn checkpoint(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        self.ws
            .commit(std::mem::take(&mut self.pending), "tribleset-bench checkpoint");
        self.repo
            .push(&mut self.ws)
            .map_err(|e| anyhow!("checkpoint results: {e:?}"))?;
        Ok(())
    }

    /// Close the session (end/duration), commit, push, close the pile.
    pub fn finish(mut self, end_ns: u64) -> Result<()> {
        let session_ref = ExclusiveId::force_ref(&self.session);
        self.pending += TribleSet::from(entity! { session_ref @
            tele::end_ns: end_ns,
            tele::duration_ns: end_ns,
        });
        self.ws.commit(std::mem::take(&mut self.pending), "tribleset-bench run");
        self.repo
            .push(&mut self.ws)
            .map_err(|e| anyhow!("push results: {e:?}"))?;
        self.repo
            .close()
            .map_err(|e| anyhow!("close results pile: {e:?}"))?;
        Ok(())
    }
}

/// Per-measure durations, the quantity every results table is made of.
///
/// # Why this exists
///
/// [`verify`] proves a results pile is well-formed; it counts spans but
/// never projects [`tele::duration_ns`], so the timings were readable only
/// by the process that wrote them. On 2026-07-29 an audit of two published
/// benchmark tables found their figures appeared in no file on disk: one
/// was rescued because the run log happens to print a suite total, the
/// other could not be re-derived at all and had to be marked unconfirmed.
/// The data was in the pile the whole time.
///
/// A benchmark that records provenance-grade telemetry and then cannot read
/// it back is not a ledger — it is a write-only log with extra steps. This
/// makes every published number one command from re-derivation.
///
/// Groups are the first path segment of a measure key
/// (`sparqloscope_gpu/join-2-small-large/total` → `sparqloscope_gpu`), which
/// is exactly the backing/arm axis the comparison tables are built on, so
/// the group totals printed here ARE the table rows.
pub fn report(path: &Path, only: Option<&str>) -> Result<()> {
    use std::collections::BTreeMap;

    let mut pile =
        Pile::open(path).map_err(|e| anyhow!("open results pile {}: {e:?}", path.display()))?;
    pile.refresh().map_err(|e| anyhow!("load results pile: {e:?}"))?;
    let reader = pile.reader().map_err(|e| anyhow!("pile reader: {e:?}"))?;
    let Some(meta_handle) = pile
        .head(*RESULTS_BRANCH)
        .map_err(|e| anyhow!("read results branch head: {e:?}"))?
    else {
        bail!("no results branch {:X} in {}", &*RESULTS_BRANCH, path.display());
    };
    let branch_meta: TribleSet = reader
        .get(meta_handle)
        .map_err(|e| anyhow!("read branch metadata: {e:?}"))?;

    // Same linear walk as `verify`. Deliberately duplicated rather than
    // extracted: `reader` borrows `pile`, so a shared helper returning both
    // is self-referential, and the alternatives (generic over the blob
    // store, or a closure) are refactors of the ONLY working reader of
    // these piles. Extract once both have a compiler behind them.
    let heads: Vec<Inline<Handle<SimpleArchive>>> = find!(
        (c: Inline<Handle<SimpleArchive>>),
        pattern!(&branch_meta, [{ repo::head: ?c }])
    )
    .map(|(c,)| c)
    .collect();
    let [head] = heads[..] else { bail!("results branch has no unique head commit") };
    let mut facts = TribleSet::new();
    let mut cursor = Some(head);
    while let Some(handle) = cursor {
        let meta: TribleSet = reader
            .get(handle)
            .map_err(|e| anyhow!("read commit metadata: {e:?}"))?;
        for (content,) in find!(
            (c: Inline<Handle<SimpleArchive>>),
            pattern!(&meta, [{ repo::content: ?c }])
        ) {
            let set: TribleSet = reader
                .get(content)
                .map_err(|e| anyhow!("read commit content: {e:?}"))?;
            facts += set;
        }
        let parents: Vec<Inline<Handle<SimpleArchive>>> = find!(
            (p: Inline<Handle<SimpleArchive>>),
            pattern!(&meta, [{ repo::parent: ?p }])
        )
        .map(|(p,)| p)
        .collect();
        cursor = match parents[..] {
            [] => None,
            [p] => Some(p),
            _ => bail!("merge commit on the results branch (expected a linear chain)"),
        };
    }

    // Keyed by the INLINE form, not by `Id`. `?s` here is an entity
    // position so it projects as `Id`, but `tele::session` below is a
    // GenId-valued attribute, whose idiomatic projection is
    // `Inline<GenId>` (see triblespace-core/tests/or_pattern.rs). Rather
    // than convert inline→Id — a direction with no example in the tests —
    // convert the session Id the confirmed way, `Id::to_inline()`, and
    // compare in the inline domain. A linear scan is right: a results pile
    // holds one session per run, and every pile checked held exactly one.
    let mut engines: Vec<(Inline<GenId>, String)> = Vec::new();
    for (s, eng) in find!(
        (s: Id, eng: Inline<ShortString>),
        pattern!(&facts, [{ ?s @ bench::engine: ?eng }])
    ) {
        engines.push((
            s.to_inline(),
            eng.try_from_inline()
                .map_err(|e| anyhow!("engine decode: {e:?}"))?,
        ));
    }

    // The span entity is projected so `find!`'s SET semantics cannot
    // collapse repeated iterations of one measure into a single row — the
    // same trap `verify` documents.
    let kind_span: Id = *KIND_SPAN;
    let mut rows: Vec<(String, String, u64)> = Vec::new();
    for (_span, sess, n, d) in find!(
        (span: Id, sess: Inline<GenId>, n: Inline<Handle<LongString>>, d: u64),
        pattern!(&facts, [{ ?span @ metadata::tag: kind_span, tele::session: ?sess,
                            tele::name: ?n, tele::duration_ns: ?d }])
    ) {
        let name: anybytes::View<str> =
            reader.get(n).map_err(|e| anyhow!("span name blob: {e:?}"))?;
        let name = name.as_ref().to_owned();
        if let Some(pat) = only {
            if !name.contains(pat) {
                continue;
            }
        }
        let engine = engines
            .iter()
            .find(|(id, _)| *id == sess)
            .map(|(_, e)| e.clone())
            .unwrap_or_else(|| "<unlabelled session>".to_owned());
        rows.push((engine, name, d));
    }

    if rows.is_empty() {
        bail!(
            "no spans with durations in {}{}",
            path.display(),
            only.map(|p| format!(" matching {p:?}")).unwrap_or_default()
        );
    }

    rows.sort();
    let mut by_group: BTreeMap<(String, String), (u64, usize)> = BTreeMap::new();
    let mut by_engine: BTreeMap<String, (u64, usize)> = BTreeMap::new();
    println!("report   : {} — {} measured span(s)", path.display(), rows.len());
    let mut current = String::new();
    for (engine, name, ns) in &rows {
        if *engine != current {
            println!("\n=== {engine} ===");
            current = engine.clone();
        }
        println!("  {:<52} {:>12.3} ms", name, *ns as f64 / 1e6);
        let group = name.split('/').next().unwrap_or(name).to_owned();
        let g = by_group.entry((engine.clone(), group)).or_insert((0, 0));
        g.0 += ns;
        g.1 += 1;
        let e = by_engine.entry(engine.clone()).or_insert((0, 0));
        e.0 += ns;
        e.1 += 1;
    }

    println!("\n=== totals by group ===");
    for ((engine, group), (ns, n)) in &by_group {
        println!(
            "  {:<28} {:<22} {:>14.3} ms  ({n} spans)",
            engine,
            group,
            *ns as f64 / 1e6
        );
    }
    println!("\n=== totals by engine ===");
    for (engine, (ns, n)) in &by_engine {
        println!("  {:<28} {:>14.3} ms  ({n} spans)", engine, *ns as f64 / 1e6);
    }

    pile.close().map_err(|e| anyhow!("close results pile: {e:?}"))?;
    Ok(())
}

/// The acceptance instrument: reopen a results pile READ-ONLY (no
/// Repository, no appends), walk the results branch, and print session
/// + span + outcome counts.
pub fn verify(path: &Path) -> Result<()> {
    let mut pile =
        Pile::open(path).map_err(|e| anyhow!("open results pile {}: {e:?}", path.display()))?;
    pile.refresh().map_err(|e| anyhow!("load results pile: {e:?}"))?;
    let reader = pile.reader().map_err(|e| anyhow!("pile reader: {e:?}"))?;
    let Some(meta_handle) = pile
        .head(*RESULTS_BRANCH)
        .map_err(|e| anyhow!("read results branch head: {e:?}"))?
    else {
        bail!("no results branch {:X} in {}", &*RESULTS_BRANCH, path.display());
    };
    let branch_meta: TribleSet = reader
        .get(meta_handle)
        .map_err(|e| anyhow!("read branch metadata: {e:?}"))?;

    // Walk the linear commit chain, uniting the content sets.
    let heads: Vec<Inline<Handle<SimpleArchive>>> = find!(
        (c: Inline<Handle<SimpleArchive>>),
        pattern!(&branch_meta, [{ repo::head: ?c }])
    )
    .map(|(c,)| c)
    .collect();
    let [head] = heads[..] else { bail!("results branch has no unique head commit") };
    let mut facts = TribleSet::new();
    let mut commits = 0usize;
    let mut cursor = Some(head);
    while let Some(handle) = cursor {
        let meta: TribleSet = reader
            .get(handle)
            .map_err(|e| anyhow!("read commit metadata: {e:?}"))?;
        for (content,) in find!(
            (c: Inline<Handle<SimpleArchive>>),
            pattern!(&meta, [{ repo::content: ?c }])
        ) {
            let set: TribleSet = reader
                .get(content)
                .map_err(|e| anyhow!("read commit content: {e:?}"))?;
            facts += set;
        }
        commits += 1;
        let parents: Vec<Inline<Handle<SimpleArchive>>> = find!(
            (p: Inline<Handle<SimpleArchive>>),
            pattern!(&meta, [{ repo::parent: ?p }])
        )
        .map(|(p,)| p)
        .collect();
        cursor = match parents[..] {
            [] => None,
            [p] => Some(p),
            _ => bail!("merge commit on the results branch (expected a linear chain)"),
        };
    }

    let kind_session: Id = *KIND_SESSION;
    let kind_span: Id = *KIND_SPAN;

    let sessions: Vec<Id> = find!(
        (s: Id),
        pattern!(&facts, [{ ?s @ metadata::tag: kind_session }])
    )
    .map(|(s,)| s)
    .collect();

    let span_count = find!(
        (s: Id),
        pattern!(&facts, [{ ?s @ metadata::tag: kind_span }])
    )
    .count();

    println!(
        "verify   : {} — {} commits, {} tribles on the results branch",
        path.display(),
        commits,
        facts.len()
    );
    println!("sessions : {}", sessions.len());
    for (s, c, eng, cfg) in find!(
        (
            s: Id,
            c: Inline<ShortString>,
            eng: Inline<ShortString>,
            cfg: Inline<Handle<LongString>>
        ),
        pattern!(&facts, [{ ?s @ bench::commit: ?c, bench::engine: ?eng, bench::config: ?cfg }])
    ) {
        let commit: String = c.try_from_inline().map_err(|e| anyhow!("commit decode: {e:?}"))?;
        let engine: String = eng
            .try_from_inline()
            .map_err(|e| anyhow!("engine decode: {e:?}"))?;
        let config: anybytes::View<str> = reader
            .get(cfg)
            .map_err(|e| anyhow!("config blob: {e:?}"))?;
        println!("  {s:X}  commit={commit} engine={engine}");
        println!("    config: {}", config.as_ref());
    }

    println!("spans    : {span_count}");
    // Project the span id too: find! heads have SET semantics, so a
    // name-only head would collapse the per-iteration spans to one row
    // per distinct name.
    //
    // The runner deliberately stores raw per-iteration observations and
    // never aggregates; reading a duration out of them is the viewer's
    // job, and this is the suite's own minimal viewer. `min` leads the
    // summary on purpose: on a contended machine the fastest observed
    // iteration is the least contaminated estimate of the work itself,
    // while the spread against `max` shows how much interference the
    // run actually absorbed.
    //
    // Keyed by (session, name), never by name alone: a results pile
    // accumulates every run ever made against it, so collapsing on the
    // name would silently average one rung's arm into another's — the
    // exact confusion a comparative arm exists to avoid.
    let mut span_times: std::collections::BTreeMap<(Id, String), Vec<u64>> = Default::default();
    for (_s, run, n, d) in find!(
        (s: Id, run: Id, n: Inline<Handle<LongString>>, d: u64),
        pattern!(&facts, [{ ?s @
            metadata::tag: kind_span,
            tele::session: ?run,
            tele::name: ?n,
            tele::duration_ns: ?d
        }])
    ) {
        let name: anybytes::View<str> =
            reader.get(n).map_err(|e| anyhow!("span name blob: {e:?}"))?;
        span_times
            .entry((run, name.as_ref().to_owned()))
            .or_default()
            .push(d);
    }
    let mut current: Option<Id> = None;
    for ((run, name), times) in &mut span_times {
        if current != Some(*run) {
            current = Some(*run);
            println!("  session {run:X}");
            println!(
                "  {:<45}{:>4}{:>12}{:>12}{:>12}",
                "span", "n", "min ms", "median ms", "max ms"
            );
        }
        times.sort_unstable();
        let ms = |ns: u64| ns as f64 / 1e6;
        println!(
            "  {name:<45}{:>4}{:>12.3}{:>12.3}{:>12.3}",
            times.len(),
            ms(times[0]),
            ms(times[times.len() / 2]),
            ms(times[times.len() - 1]),
        );
    }

    // Optional rows per outcome entity (the engine is monotone; the
    // optional join happens here in Rust).
    let mut rows_of: std::collections::HashMap<Id, u64> = Default::default();
    for (o, r) in find!(
        (o: Id, r: u64),
        pattern!(&facts, [{ ?o @ bench::rows: ?r }])
    ) {
        rows_of.insert(o, r);
    }

    let mut outcome_rows: Vec<(String, String, Option<u64>)> = Vec::new();
    for (o, w, v) in find!(
        (o: Id, w: Inline<Handle<LongString>>, v: Inline<ShortString>),
        pattern!(&facts, [{ ?o @ bench::workload: ?w, bench::outcome: ?v }])
    ) {
        let workload: anybytes::View<str> =
            reader.get(w).map_err(|e| anyhow!("workload blob: {e:?}"))?;
        let outcome: String = v
            .try_from_inline()
            .map_err(|e| anyhow!("outcome decode: {e:?}"))?;
        outcome_rows.push((workload.as_ref().to_owned(), outcome, rows_of.get(&o).copied()));
    }
    println!("outcomes : {}", outcome_rows.len());
    let mut histogram: std::collections::BTreeMap<(String, String), usize> = Default::default();
    for (workload, outcome, _) in &outcome_rows {
        let group = workload.split('/').next().unwrap_or(workload).to_owned();
        *histogram.entry((group, outcome.clone())).or_default() += 1;
    }
    for ((group, outcome), count) in &histogram {
        println!("  {group:<14} {outcome:<28} x{count}");
    }
    outcome_rows.sort();
    let mut any_rows = false;
    for (workload, outcome, rows) in &outcome_rows {
        if let Some(n) = rows {
            if !any_rows {
                println!("rows     :");
                any_rows = true;
            }
            println!("  {workload:<45} {outcome:<10} rows={n}");
        }
    }

    pile.close().map_err(|e| anyhow!("close results pile: {e:?}"))?;
    Ok(())
}
