//! Data import and conversion helpers bridging external formats into Trible Space.
//!
//! This module hosts adapters that translate common interchange formats into
//! [`TribleSet`](crate::trible::TribleSet) changes ready to merge into a
//! repository or workspace.

mod import_attribute;
pub mod json;
pub mod json_tree;
pub mod ntriples;

pub(crate) use import_attribute::ImportAttribute;

use triblespace_core_macros::attributes;

use crate::blob::schemas::longstring::LongString;
use crate::value::schemas::hash::{Blake3, Handle};
use crate::value::schemas::shortstring::ShortString;

attributes! {
    /// The canonical RDF URI for an entity. Use this when importing data
    /// from an external vocabulary where the entity's identity is a URI —
    /// the same URI always deterministically maps to the same triblespace
    /// Id by round-tripping through an `rdf_uri` fragment.
    "AA68DE115445A63D62A63FF3284D030C" as pub rdf_uri: Handle<Blake3, LongString>;

    /// BCP-47 language tag for a reified language-tagged literal entity.
    ///
    /// RDF's `"text"@lang` form is reified as a small entity with two
    /// attributes — `rdf_lang` (the BCP-47 tag, fits in a `ShortString`)
    /// and `rdf_text` (the lexical form, hashed as a `LongString` blob).
    /// Two literals with the same `(lang, text)` pair derive the same
    /// intrinsic id, giving content-addressed deduplication for free.
    "904DCA1F8C0BF087B02C0581F69EDF4D" as pub rdf_lang: ShortString;

    /// Lexical form of a reified language-tagged literal entity. See
    /// [`rdf_lang`] for the full encoding rationale.
    "02923632852C6AF8CD0D2596ACC343D2" as pub rdf_text: Handle<Blake3, LongString>;
}
