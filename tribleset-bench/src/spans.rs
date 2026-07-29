//! Collect engine `tracing` spans and write them into the results ledger.
//!
//! The ledger's span schema — `session / parent / name / category /
//! begin_ns / end_ns / duration_ns` — has always described a tree, and until
//! now only ever held a flat list: every span was a `Measure` taken from
//! OUTSIDE the engine, so nothing had a parent. Engine spans arrive nested,
//! and the nesting is the whole value: a `blob.hash` inside a
//! `rollup.attach` is what separates "attach cost 81 seconds" from "attach
//! spent 74 of its 81 seconds verifying a hash".
//!
//! # Buffer, then flush
//!
//! Spans fire on query threads while the ledger wants `&mut` on the main
//! one. Rather than lock the ledger per span — which would put the
//! instrument inside the thing being measured — records are appended to a
//! mutex-guarded `Vec` and written once at the end. The cost during
//! measurement is one push per closed span.
//!
//! # Categories
//!
//! Engine spans land under `engine`, distinct from the `bench` category the
//! outer measurements use. A reader can then separate "what the harness
//! timed" from "what the engine reported about itself", which matters when
//! they disagree — and they will, since the harness's span includes work the
//! engine never sees.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

use triblespace::core::id::Id;
use tracing::span::{Attributes, Id as SpanId};
use tracing::Subscriber;
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

/// One closed span, ready to become tribles.
pub struct Record {
    pub name: &'static str,
    pub begin_ns: u64,
    pub duration_ns: u64,
    /// Index into the collector's records, not a ledger entity: the parent
    /// may not have closed yet when a child does, so entities cannot be
    /// assigned until the flush.
    pub parent: Option<usize>,
    /// `bytes`, `live`, `level`, `segments` — whichever the span carried.
    pub fields: Vec<(&'static str, u64)>,
}

#[derive(Default)]
struct Open {
    started: Option<Instant>,
    parent: Option<SpanId>,
    fields: Vec<(&'static str, u64)>,
}

struct Inner {
    base: Instant,
    open: Mutex<HashMap<u64, Open>>,
    /// Closed spans in close order, plus the span id each was, so parents
    /// can be resolved to indices after the fact.
    done: Mutex<Vec<(u64, Record)>>,
}

/// Collects closed spans; the bench drains it once, at the end.
///
/// A cloneable handle rather than an `Arc<Collector>`, because
/// `tracing-subscriber` implements `Layer` for the type it is given and not
/// for an `Arc` of it — so the sharing has to live inside.
#[derive(Clone)]
pub struct Collector {
    inner: std::sync::Arc<Inner>,
}

impl Collector {
    pub fn new(base: Instant) -> Self {
        Self {
            inner: std::sync::Arc::new(Inner {
                base,
                open: Mutex::new(HashMap::new()),
                done: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Closed spans, oldest first, with `parent` resolved to indices.
    ///
    /// A parent that never closed — an unfinished span at shutdown — leaves
    /// its children unparented rather than dropping them: a truncated tree
    /// is still evidence, and silently discarding the deepest spans would
    /// lose exactly the ones a profile is for.
    pub fn drain(&self) -> Vec<Record> {
        let done = std::mem::take(&mut *self.inner.done.lock().unwrap());
        let index: HashMap<u64, usize> = done
            .iter()
            .enumerate()
            .map(|(i, (id, _))| (*id, i))
            .collect();
        done.into_iter()
            .map(|(_, mut r)| {
                r.parent = r.parent.and_then(|raw| index.get(&(raw as u64)).copied());
                r
            })
            .collect()
    }
}

struct FieldGrab<'a>(&'a mut Vec<(&'static str, u64)>);

impl tracing::field::Visit for FieldGrab<'_> {
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.0.push((field.name(), value));
    }
    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.0.push((field.name(), value.max(0) as u64));
    }
    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.0.push((field.name(), value as u64));
    }
    fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
}

impl<S> Layer<S> for Collector
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &SpanId, ctx: Context<'_, S>) {
        let mut fields = Vec::new();
        attrs.record(&mut FieldGrab(&mut fields));
        let parent = ctx.current_span().id().cloned();
        self.inner.open.lock().unwrap().insert(
            id.into_u64(),
            Open {
                started: Some(Instant::now()),
                parent,
                fields,
            },
        );
    }

    fn on_close(&self, id: SpanId, ctx: Context<'_, S>) {
        let raw = id.into_u64();
        let Some(open) = self.inner.open.lock().unwrap().remove(&raw) else {
            return;
        };
        let Some(started) = open.started else { return };
        let name = ctx
            .metadata(&id)
            .map(|m| m.name())
            .unwrap_or("span");
        let rec = Record {
            name,
            begin_ns: started.duration_since(self.inner.base).as_nanos() as u64,
            duration_ns: started.elapsed().as_nanos() as u64,
            parent: open.parent.map(|p| p.into_u64() as usize),
            fields: open.fields,
        };
        self.inner.done.lock().unwrap().push((raw, rec));
    }
}

/// Write drained spans into the ledger, parents before children.
pub fn flush(led: &mut crate::ledger::ResultsLedger, records: Vec<Record>) -> usize {
    let mut entities: Vec<Option<Id>> = vec![None; records.len()];
    // Records close innermost-first, so a parent lands AFTER its children.
    // Writing in close order would ask for a parent entity that does not
    // exist yet, so resolve by walking each chain to its root first.
    let mut order: Vec<usize> = (0..records.len()).collect();
    order.sort_by_key(|&i| depth(&records, i));
    for i in order {
        let parent = records[i].parent.and_then(|p| entities[p]);
        let id = led.span_in(
            records[i].name,
            "engine",
            records[i].begin_ns,
            records[i].duration_ns,
            parent,
        );
        entities[i] = Some(id);
    }
    records.len()
}

fn depth(records: &[Record], mut i: usize) -> usize {
    let mut d = 0;
    let mut guard = 0;
    while let Some(p) = records[i].parent {
        i = p;
        d += 1;
        guard += 1;
        // A cycle cannot arise from a span tree, but a corrupted index
        // should not hang a benchmark's final write.
        if guard > records.len() {
            break;
        }
    }
    d
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing_subscriber::prelude::*;

    /// Spans must be CAPTURED and NESTED. An instrumented build that records
    /// nothing produces an empty profile, which reads as "nothing was slow"
    /// — the most expensive possible way to be wrong about performance.
    #[test]
    fn nested_spans_are_captured_with_their_parents() {
        let collector = Collector::new(Instant::now());
        let subscriber = tracing_subscriber::registry().with(collector.clone());

        tracing::subscriber::with_default(subscriber, || {
            let outer = tracing::info_span!("rollup.attach");
            let _o = outer.enter();
            {
                let inner = tracing::info_span!("blob.hash", bytes = 4096u64);
                let _i = inner.enter();
            }
        });

        let records = collector.drain();
        assert_eq!(records.len(), 2, "expected both spans, got {}", records.len());

        // The inner span closes FIRST, so it is index 0 and its parent is
        // the record that closes after it. That ordering is why `flush`
        // sorts by depth rather than writing in close order.
        let inner = records.iter().find(|r| r.name == "blob.hash").expect("inner");
        let outer_idx = records.iter().position(|r| r.name == "rollup.attach").expect("outer");
        assert_eq!(
            inner.parent,
            Some(outer_idx),
            "blob.hash must be parented to rollup.attach"
        );
        assert_eq!(
            inner.fields,
            vec![("bytes", 4096u64)],
            "span fields must survive: a duration without its size is not a rate"
        );
        assert!(inner.duration_ns > 0);
    }

    /// A parent that never closes leaves children unparented rather than
    /// dropping them — a truncated tree is still evidence.
    #[test]
    fn an_unclosed_parent_does_not_lose_its_children() {
        let collector = Collector::new(Instant::now());
        let subscriber = tracing_subscriber::registry().with(collector.clone());
        let guard = tracing::subscriber::set_default(subscriber);
        let outer = tracing::info_span!("outer.never.closes");
        let entered = outer.enter();
        {
            let inner = tracing::info_span!("inner.closes");
            let _i = inner.enter();
        }
        let records = collector.drain();
        assert_eq!(records.len(), 1, "the closed child must survive");
        assert_eq!(records[0].name, "inner.closes");
        assert_eq!(records[0].parent, None, "its parent had not closed");
        drop(entered);
        drop(guard);
    }
}
