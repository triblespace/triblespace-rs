# Triblespace checkout metadata

Commits store a `content` handle plus an optional `metadata` handle, each
pointing to a SimpleArchive TribleSet. `Workspace::checkout` unions the
`content` TribleSet for the selected commits. `Workspace::checkout_metadata`
unions the referenced metadata TribleSets. `Workspace::checkout_with_metadata`
returns both in one pass as `(content, metadata)`.

Metadata is not something you attach at write time. A commit's metadata is the
*metafacts* of the content committed: `entity!{}` collects the description of
every attribute it asserts — rust identifier, declaring module, doc comment,
and the encoding its values are in — into the fragment's `metafacts`, and
`Workspace::commit` archives those as that commit's metadata. Describing a
pile and writing to it are therefore the same act, with no separate step left
to skip.

Content committed as a bare `TribleSet` carries no descriptions, so its commit
gets no metadata handle and contributes an empty metadata TribleSet on
checkout. Merge commits carry neither content nor metadata.

Metadata archives are content-addressed, so the many commits a tool makes over
the same handful of attributes converge on a handful of distinct metadata
blobs rather than one per commit.
