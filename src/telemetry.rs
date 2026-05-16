use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::core::metadata;
use crate::core::repo::pile::Pile;
use crate::core::repo::{Repository, Workspace};
use crate::prelude::blobschemas::LongString;
use crate::prelude::inlineschemas::{Blake3, GenId, Handle, ShortString, U256BE};
use crate::prelude::*;
use ed25519_dalek::SigningKey;
use rand_core06::OsRng;
use thread_local::ThreadLocal;
use tracing::Subscriber;
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::EnvFilter;

const ENV_TELEMETRY_PILE: &str = "TELEMETRY_PILE";
const ENV_PILE: &str = "PILE";
const ENV_TELEMETRY_BRANCH: &str = "TELEMETRY_BRANCH";
const ENV_TELEMETRY_FLUSH_MS: &str = "TELEMETRY_FLUSH_MS";

pub mod schema {
    use super::*;

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

    #[allow(non_upper_case_globals)]
    pub const kind_session: Id = crate::macros::id_hex!("2701F7019B865D461F0169B1303026D6");
    #[allow(non_upper_case_globals)]
    pub const kind_span: Id = crate::macros::id_hex!("0AF9FEB9A2BFEB1BE8A8229829181085");

    #[allow(non_upper_case_globals)]
    pub const telemetry_metadata: Id = crate::macros::id_hex!("BCFDE38F7E452924C72803239392EA05");

    pub fn build_telemetry_metadata<B>(blobs: &mut B) -> std::result::Result<Fragment, B::PutError>
    where
        B: BlobStore<Blake3>,
    {
        let attrs = describe(blobs)?;

        let mut protocol = entity! { ExclusiveId::force_ref(&telemetry_metadata) @
            metadata::name: blobs.put("triblespace_telemetry")?,
            metadata::description: blobs.put(
                "Span-based profiling events emitted by TribleSpace telemetry.",
            )?,
            metadata::tag: metadata::KIND_PROTOCOL,
            metadata::attribute*: attrs,
        };

        protocol += entity! { ExclusiveId::force_ref(&kind_session) @
            metadata::name: blobs.put("telemetry_session")?,
            metadata::description: blobs.put(
                "A profiling session. Groups spans emitted during one telemetry run.",
            )?,
            metadata::tag: metadata::KIND_TAG,
        };
        protocol += entity! { ExclusiveId::force_ref(&kind_span) @
            metadata::name: blobs.put("telemetry_span")?,
            metadata::description: blobs.put(
                "A begin/end span with optional parent links.",
            )?,
            metadata::tag: metadata::KIND_TAG,
        };

        Ok(protocol)
    }
}

fn is_valid_short(value: &str) -> bool {
    value.as_bytes().len() <= 32 && !value.as_bytes().iter().any(|b| *b == 0)
}

struct ThreadTelemetry {
    workspace: Workspace<Pile>,
    last_flush: Instant,
}

struct TelemetryInner {
    repo: Mutex<Option<Repository<Pile>>>,
    workspaces: ThreadLocal<Arc<Mutex<ThreadTelemetry>>>,
    registry: Mutex<Vec<Arc<Mutex<ThreadTelemetry>>>>,
    session: Id,
    base: Instant,
    branch_id: Id,
    flush_interval: Duration,
    shutdown: AtomicBool,
}

impl TelemetryInner {
    fn now_ns(&self) -> u64 {
        self.base.elapsed().as_nanos() as u64
    }

    fn get_or_init_thread(&self) -> &Arc<Mutex<ThreadTelemetry>> {
        self.workspaces.get_or(|| {
            let mut repo_guard = self.repo.lock().expect("telemetry repo lock");
            let repo = repo_guard.as_mut().expect("telemetry repo not closed");
            let ws = repo
                .pull(self.branch_id)
                .expect("telemetry pull workspace");
            let arc = Arc::new(Mutex::new(ThreadTelemetry {
                workspace: ws,
                last_flush: Instant::now(),
            }));
            self.registry.lock().expect("telemetry registry lock").push(arc.clone());
            arc
        })
    }

    fn maybe_flush(&self, state: &mut ThreadTelemetry) {
        if state.last_flush.elapsed() < self.flush_interval {
            return;
        }
        let mut repo_guard = self.repo.lock().expect("telemetry repo lock");
        if let Some(repo) = repo_guard.as_mut() {
            if let Err(e) = repo.push(&mut state.workspace) {
                log::warn!("telemetry flush failed: {e:?}");
            }
        }
        state.last_flush = Instant::now();
    }
}

#[derive(Debug, Clone, Copy)]
struct TelemetrySpanData {
    span: Id,
    start_ns: u64,
}

#[derive(Default)]
struct FieldCapture {
    source: Option<String>,
}

impl tracing::field::Visit for FieldCapture {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        match field.name() {
            "source" if !value.is_empty() => self.source = Some(value.to_string()),
            _ => {}
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        match field.name() {
            "source" => {
                let mut raw = format!("{value:?}");
                if raw.starts_with('"') && raw.ends_with('"') && raw.len() >= 2 {
                    raw = raw[1..raw.len() - 1].to_string();
                }
                if !raw.is_empty() {
                    self.source = Some(raw);
                }
            }
            _ => {}
        }
    }
}

/// Tracing layer that turns spans into TribleSpace telemetry.
///
/// Construct via [`Telemetry::layer_from_env`] and attach to your application's subscriber.
pub struct TelemetryLayer {
    inner: Arc<TelemetryInner>,
}

impl TelemetryLayer {
    fn parent_id<S>(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        ctx: &Context<'_, S>,
    ) -> Option<Id>
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        if let Some(parent) = attrs.parent() {
            if let Some(span) = ctx.span(parent) {
                if let Some(data) = span.extensions().get::<TelemetrySpanData>() {
                    return Some(data.span);
                }
            }
        }

        if let Some(id) = ctx.current_span().id() {
            if let Some(span) = ctx.span(id) {
                if let Some(data) = span.extensions().get::<TelemetrySpanData>() {
                    return Some(data.span);
                }
            }
        }

        None
    }
}

impl<S> Layer<S> for TelemetryLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        ctx: Context<'_, S>,
    ) {
        if self.inner.shutdown.load(Ordering::Relaxed) {
            return;
        }

        let Some(span) = ctx.span(id) else {
            return;
        };

        let meta = attrs.metadata();
        let mut fields = FieldCapture::default();
        attrs.record(&mut fields);

        let start_ns = self.inner.now_ns();
        let span_id = *ufoid();
        let parent = self.parent_id(attrs, &ctx);

        span.extensions_mut().insert(TelemetrySpanData {
            span: span_id,
            start_ns,
        });

        let thread_state = self.inner.get_or_init_thread();
        let mut state = thread_state.lock().expect("telemetry thread state lock");

        let target = meta.target();
        let category = target.split("::").next().unwrap_or(target);
        let category = if !category.is_empty() && is_valid_short(category) {
            category
        } else {
            "span"
        };

        span_begin(
            &mut state.workspace,
            self.inner.session,
            span_id,
            parent,
            start_ns,
            category,
            meta.name(),
            fields.source,
        );
    }

    fn on_close(&self, id: tracing::span::Id, ctx: Context<'_, S>) {
        if self.inner.shutdown.load(Ordering::Relaxed) {
            return;
        }

        let Some(span) = ctx.span(&id) else {
            return;
        };
        let Some(data) = span.extensions().get::<TelemetrySpanData>().copied() else {
            return;
        };

        let end_ns = self.inner.now_ns();

        let thread_state = self.inner.get_or_init_thread();
        let mut state = thread_state.lock().expect("telemetry thread state lock");

        span_end(
            &mut state.workspace,
            data.span,
            end_ns,
            end_ns.saturating_sub(data.start_ns),
        );

        self.inner.maybe_flush(&mut state);
    }
}

pub struct Telemetry {
    inner: Arc<TelemetryInner>,
}

impl Telemetry {
    /// Start a telemetry sink and return a layer that writes spans into it.
    ///
    /// This does **not** install a tracing subscriber. Embed the returned layer into your
    /// application's subscriber, and keep the returned [`Telemetry`] guard alive to
    /// flush and close the sink on shutdown.
    pub fn layer_from_env(session_name: &str) -> Option<(TelemetryLayer, Self)> {
        let pile_path = std::env::var(ENV_TELEMETRY_PILE)
            .ok()
            .or_else(|| std::env::var(ENV_PILE).ok())?;
        let pile_path = pile_path.trim();
        if pile_path.is_empty() {
            return None;
        }
        let pile_path = PathBuf::from(pile_path);

        let branch_hex = std::env::var(ENV_TELEMETRY_BRANCH).ok()?;
        let branch_hex = branch_hex.trim();
        if branch_hex.len() != 32 {
            log::warn!(
                "TELEMETRY_BRANCH must be a 32-char hex ID, got {} chars",
                branch_hex.len()
            );
            return None;
        }
        let branch_id = Id::from_hex(branch_hex)?;

        let flush_ms = std::env::var(ENV_TELEMETRY_FLUSH_MS)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(250);
        let flush_interval = Duration::from_millis(flush_ms.max(10));

        let base = Instant::now();
        let session_id = *ufoid();

        // Open and restore pile.
        if let Some(parent) = pile_path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent).ok()?;
        }
        let mut pile = Pile::open(&pile_path).ok()?;
        if pile.restore().is_err() {
            let _ = pile.close();
            return None;
        }

        let signing_key = SigningKey::generate(&mut OsRng);
        let metadata_set: TribleSet = schema::build_telemetry_metadata(&mut pile)
            .ok()?
            .into();
        let mut repo = Repository::new(pile, signing_key, metadata_set).ok()?;

        // Commit session start entity.
        let mut ws = repo.pull(branch_id).ok()?;
        let session_entity = ExclusiveId::force_ref(&session_id);
        let mut init = TribleSet::new();
        init += entity! { session_entity @
            metadata::tag: schema::kind_session,
            schema::category: "session",
            schema::name: ws.put(session_name.to_string()),
            schema::begin_ns: 0u64,
        };
        ws.commit(init, "telemetry session");
        if repo.push(&mut ws).is_err() {
            let _ = repo.close();
            return None;
        }

        let inner = Arc::new(TelemetryInner {
            repo: Mutex::new(Some(repo)),
            workspaces: ThreadLocal::new(),
            registry: Mutex::new(Vec::new()),
            session: session_id,
            base,
            branch_id,
            flush_interval,
            shutdown: AtomicBool::new(false),
        });

        let layer = TelemetryLayer {
            inner: inner.clone(),
        };

        Some((layer, Self { inner }))
    }

    /// Convenience for standalone processes: start telemetry and install a global subscriber
    /// (only if none exists).
    pub fn install_global_from_env(session_name: &str) -> Option<Self> {
        let (layer, guard) = Self::layer_from_env(session_name)?;

        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
        let subscriber = tracing_subscriber::registry().with(filter).with(layer);

        if tracing::subscriber::set_global_default(subscriber).is_err() {
            log::warn!("triblespace telemetry disabled: tracing subscriber already set");
            drop(guard);
            return None;
        }

        Some(guard)
    }
}

impl Drop for Telemetry {
    fn drop(&mut self) {
        self.inner.shutdown.store(true, Ordering::Relaxed);

        // Flush all thread-local workspaces.
        let registry = self.inner.registry.lock().expect("telemetry registry lock");
        {
            let mut repo_guard = self.inner.repo.lock().expect("telemetry repo lock");
            if let Some(repo) = repo_guard.as_mut() {
                for state_arc in registry.iter() {
                    let mut state = state_arc.lock().expect("telemetry thread state lock");
                    if let Err(e) = repo.push(&mut state.workspace) {
                        log::warn!("telemetry shutdown flush failed: {e:?}");
                    }
                }

                // Commit session end entity.
                let end_ns = self.inner.now_ns();
                if let Ok(mut ws) = repo.pull(self.inner.branch_id) {
                    let session_entity = ExclusiveId::force_ref(&self.inner.session);
                    let mut end = TribleSet::new();
                    end += entity! { session_entity @
                        schema::end_ns: end_ns,
                        schema::duration_ns: end_ns,
                    };
                    ws.commit(end, "telemetry session end");
                    if let Err(e) = repo.push(&mut ws) {
                        log::warn!("telemetry session end push failed: {e:?}");
                    }
                }
            }
        }
        drop(registry);

        // Close the pile.
        let mut repo_guard = self.inner.repo.lock().expect("telemetry repo lock");
        if let Some(repo) = repo_guard.take() {
            if let Err(e) = repo.close() {
                log::warn!("telemetry pile close failed: {e:?}");
            }
        }
    }
}

fn span_begin(
    ws: &mut Workspace<Pile>,
    session: Id,
    span_id: Id,
    parent: Option<Id>,
    at_ns: u64,
    category: &str,
    name: &str,
    source: Option<String>,
) {
    let span_entity = ExclusiveId::force_ref(&span_id);
    let mut tribles = TribleSet::new();
    tribles += entity! { span_entity @
        metadata::tag: schema::kind_span,
        schema::session: session,
        schema::category: category,
        schema::name: ws.put(name.to_string()),
        schema::begin_ns: at_ns,
    };
    if let Some(parent) = parent {
        tribles += entity! { span_entity @ schema::parent: parent };
    }
    if let Some(source) = source {
        tribles += entity! { span_entity @ schema::source: ws.put(source) };
    }
    ws.commit(tribles, "telemetry span");
}

fn span_end(ws: &mut Workspace<Pile>, span_id: Id, at_ns: u64, duration_ns: u64) {
    let span_entity = ExclusiveId::force_ref(&span_id);
    let mut tribles = TribleSet::new();
    tribles += entity! { span_entity @
        schema::end_ns: at_ns,
        schema::duration_ns: duration_ns,
    };
    ws.commit(tribles, "telemetry span end");
}
