//! Standalone learning implementation of HATCH: a variable-width
//! fingerprint trie over 64-byte EAV trible keys.
//!
//! This intentionally does not integrate with triblespace's PATCH internals.
//! It is a correctness-first sandbox for incremental insertion, lossy
//! fingerprint child selection, segment checkpoints, and maintained aggregate
//! metadata.

use siphasher::sip128::SipHasher24;

pub type Key = [u8; KEY_LEN];

pub const KEY_LEN: usize = 64;
pub const ENTITY_END: u8 = 16;
pub const ATTRIBUTE_END: u8 = 32;
pub const VALUE_END: u8 = 64;
pub const MAX_FANOUT: usize = 256;

const SIP_KEY: [u8; 16] = [
    0x48, 0x41, 0x54, 0x43, 0x48, 0x2d, 0x6c, 0x65, 0x61, 0x72, 0x6e, 0x69, 0x6e, 0x67, 0x21, 0x01,
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Span {
    pub start: u8,
    pub end: u8,
}

impl Span {
    fn new(start: usize, end: usize) -> Self {
        assert!(start < end, "empty HATCH span [{start},{end})");
        assert!(end <= KEY_LEN, "span end past key length: {end}");
        assert!(
            end <= next_boundary(start),
            "HATCH span [{start},{end}) crosses a segment checkpoint"
        );
        Self {
            start: start as u8,
            end: end as u8,
        }
    }

    fn len(self) -> usize {
        (self.end - self.start) as usize
    }
}

#[derive(Clone, Debug, Default)]
pub struct Hatch {
    root: Option<Entry>,
}

#[derive(Clone, Debug, Default)]
pub struct HatchWide {
    root: Option<Entry>,
}

#[derive(Clone, Debug)]
enum Entry {
    Leaf(Key),
    Node(Box<Node>),
}

#[derive(Clone, Debug)]
pub struct Node {
    pub span: Span,
    childleaf: Key,
    children: ChildTable,
    pub hash: u128,
    pub leaf_count: u64,
    pub segment_count: u64,
}

#[derive(Clone, Debug, Default)]
struct ChildTable {
    children: Vec<Child>,
}

#[derive(Clone, Debug)]
struct Child {
    fp: u16,
    subkey: Vec<u8>,
    entry: Entry,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HatchStats {
    pub node_count: u64,
    pub leaf_count: u64,
    pub total_fanout: u64,
    pub max_fanout: usize,
    pub fp_collision_entries: u64,
    pub fanout_buckets: [u64; 8],
}

impl HatchStats {
    pub fn avg_fanout(self) -> f64 {
        if self.node_count == 0 {
            0.0
        } else {
            self.total_fanout as f64 / self.node_count as f64
        }
    }
}

impl Hatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, key: Key) -> bool {
        match &mut self.root {
            Some(root) => insert_entry(root, key, 0),
            None => {
                self.root = Some(Entry::Leaf(key));
                true
            }
        }
    }

    pub fn lookup(&self, key: &Key) -> bool {
        self.root
            .as_ref()
            .is_some_and(|root| lookup_entry(root, key, 0))
    }

    pub fn has_prefix(&self, prefix_len: u8, key: &Key) -> bool {
        assert!(
            prefix_len == ENTITY_END || prefix_len == ATTRIBUTE_END,
            "HATCH only checkpoints prefixes at 16 and 32 bytes"
        );
        self.root
            .as_ref()
            .is_some_and(|root| has_prefix_entry(root, key, prefix_len as usize, 0))
    }

    pub fn root_hash(&self) -> u128 {
        self.root.as_ref().map_or(0, Entry::hash)
    }

    pub fn leaf_count(&self) -> u64 {
        self.root.as_ref().map_or(0, Entry::leaf_count)
    }

    pub fn segment_count(&self) -> u64 {
        match &self.root {
            Some(Entry::Leaf(_)) => 1,
            Some(Entry::Node(node)) => node.segment_count,
            None => 0,
        }
    }

    pub fn stats(&self) -> HatchStats {
        let mut stats = HatchStats::default();
        if let Some(root) = &self.root {
            root.accumulate_stats(&mut stats);
        }
        stats
    }

    pub fn assert_segment_spans(&self) {
        if let Some(root) = &self.root {
            root.assert_segment_spans();
        }
    }
}

impl HatchWide {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, key: Key) -> bool {
        match &mut self.root {
            Some(root) => insert_dense_entry(root, key, 0),
            None => {
                self.root = Some(Entry::Leaf(key));
                true
            }
        }
    }

    pub fn lookup(&self, key: &Key) -> bool {
        self.root
            .as_ref()
            .is_some_and(|root| lookup_entry(root, key, 0))
    }

    pub fn has_prefix(&self, prefix_len: u8, key: &Key) -> bool {
        assert!(
            prefix_len == ENTITY_END || prefix_len == ATTRIBUTE_END,
            "HATCH only checkpoints prefixes at 16 and 32 bytes"
        );
        self.root
            .as_ref()
            .is_some_and(|root| has_prefix_entry(root, key, prefix_len as usize, 0))
    }

    pub fn infixes<const INFIX_LEN: usize>(
        &self,
        prefix: &[u8],
        prefix_len: usize,
        infix_len: usize,
        out: &mut Vec<[u8; INFIX_LEN]>,
    ) -> u64 {
        assert!(
            prefix_len == ENTITY_END as usize || prefix_len == ATTRIBUTE_END as usize,
            "HATCH only checkpoints prefixes at 16 and 32 bytes"
        );
        assert_eq!(
            infix_len, INFIX_LEN,
            "runtime infix_len disagrees with INFIX_LEN"
        );
        assert!(
            (prefix_len, INFIX_LEN)
                == (
                    ENTITY_END as usize,
                    ATTRIBUTE_END as usize - ENTITY_END as usize
                )
                || (prefix_len, INFIX_LEN)
                    == (
                        ATTRIBUTE_END as usize,
                        VALUE_END as usize - ATTRIBUTE_END as usize
                    ),
            "HATCH propose only supports whole next segments after e or e,a"
        );
        assert!(
            prefix.len() >= prefix_len,
            "prefix slice shorter than prefix_len"
        );

        let Some(root) = &self.root else {
            return 0;
        };
        let mut nodes_visited = 0;
        infixes_entry(
            root,
            &prefix[..prefix_len],
            prefix_len,
            prefix_len + INFIX_LEN,
            0,
            out,
            &mut nodes_visited,
        );
        nodes_visited
    }

    pub fn root_hash(&self) -> u128 {
        self.root.as_ref().map_or(0, Entry::hash)
    }

    pub fn leaf_count(&self) -> u64 {
        self.root.as_ref().map_or(0, Entry::leaf_count)
    }

    pub fn segment_count(&self) -> u64 {
        match &self.root {
            Some(Entry::Leaf(_)) => 1,
            Some(Entry::Node(node)) => node.segment_count,
            None => 0,
        }
    }

    pub fn stats(&self) -> HatchStats {
        let mut stats = HatchStats::default();
        if let Some(root) = &self.root {
            root.accumulate_stats(&mut stats);
        }
        stats
    }

    pub fn assert_segment_spans(&self) {
        if let Some(root) = &self.root {
            root.assert_segment_spans();
        }
    }

    pub fn assert_widest_valid_spans(&self) {
        if let Some(root) = &self.root {
            root.assert_widest_valid_spans();
        }
    }
}

impl Node {
    fn new(span: Span, children: Vec<Child>) -> Self {
        assert!(!children.is_empty(), "HATCH nodes must have children");
        let mut node = Self {
            span,
            childleaf: children[0].entry.representative(),
            children: ChildTable { children },
            hash: 0,
            leaf_count: 0,
            segment_count: 0,
        };
        node.recompute();
        node
    }

    fn recompute(&mut self) {
        self.childleaf = self.children.children[0].entry.representative();
        self.hash = 0;
        self.leaf_count = 0;
        self.segment_count = 0;

        for child in &self.children.children {
            self.hash ^= child.entry.hash();
            self.leaf_count += child.entry.leaf_count();
            self.segment_count += child.entry.count_segment(self.span.end as usize);
        }
    }
}

impl Entry {
    fn representative(&self) -> Key {
        match self {
            Entry::Leaf(key) => *key,
            Entry::Node(node) => node.childleaf,
        }
    }

    fn min_branch_start(&self) -> usize {
        match self {
            Entry::Leaf(_) => KEY_LEN,
            Entry::Node(node) => node.span.start as usize,
        }
    }

    fn hash(&self) -> u128 {
        match self {
            Entry::Leaf(key) => leaf_hash(key),
            Entry::Node(node) => node.hash,
        }
    }

    fn leaf_count(&self) -> u64 {
        match self {
            Entry::Leaf(_) => 1,
            Entry::Node(node) => node.leaf_count,
        }
    }

    fn count_segment(&self, at_depth: usize) -> u64 {
        match self {
            Entry::Leaf(_) => 1,
            Entry::Node(node) => {
                if segment_of_depth(at_depth) == segment_of_depth(node.span.start as usize) {
                    node.segment_count
                } else {
                    1
                }
            }
        }
    }

    fn accumulate_stats(&self, stats: &mut HatchStats) {
        match self {
            Entry::Leaf(_) => stats.leaf_count += 1,
            Entry::Node(node) => {
                let fanout = node.children.children.len();
                stats.node_count += 1;
                stats.total_fanout += fanout as u64;
                stats.max_fanout = stats.max_fanout.max(fanout);
                stats.fanout_buckets[fanout_bucket(fanout)] += 1;
                stats.fp_collision_entries += node.children.fp_collision_entries();
                for child in &node.children.children {
                    child.entry.accumulate_stats(stats);
                }
            }
        }
    }

    fn assert_segment_spans(&self) {
        match self {
            Entry::Leaf(_) => {}
            Entry::Node(node) => {
                assert!(
                    node.span.end as usize <= next_boundary(node.span.start as usize),
                    "span {:?} crossed segment checkpoint",
                    node.span
                );
                for child in &node.children.children {
                    child.entry.assert_segment_spans();
                }
            }
        }
    }

    fn assert_widest_valid_spans(&self) {
        match self {
            Entry::Leaf(_) => {}
            Entry::Node(node) => {
                let start = node.span.start as usize;
                let end = node.span.end as usize;
                let boundary = next_boundary(start);
                let mut keys = Vec::with_capacity(node.leaf_count as usize);
                self.collect_leaves(&mut keys);
                assert!(
                    distinct_key_prefixes(&keys, start, end) <= MAX_FANOUT,
                    "dense span {:?} exceeds max fanout",
                    node.span
                );
                if end < boundary {
                    assert!(
                        distinct_key_prefixes(&keys, start, end + 1) > MAX_FANOUT,
                        "dense span {:?} is not widest valid",
                        node.span
                    );
                }
                for child in &node.children.children {
                    child.entry.assert_widest_valid_spans();
                }
            }
        }
    }
}

impl ChildTable {
    fn get_exact_mut(&mut self, key: &Key, span: Span) -> Option<&mut Child> {
        let subkey = &key[span.start as usize..span.end as usize];
        let fp = fingerprint16(subkey);
        self.children
            .iter_mut()
            .find(|child| child.fp == fp && child.subkey == subkey)
    }

    fn get_exact(&self, key: &Key, span: Span) -> Option<&Child> {
        let subkey = &key[span.start as usize..span.end as usize];
        let fp = fingerprint16(subkey);
        self.children
            .iter()
            .find(|child| child.fp == fp && child.subkey == subkey)
    }

    fn get_exact_for_prefix(&self, prefix: &[u8], span: Span) -> Option<&Child> {
        let subkey = &prefix[span.start as usize..span.end as usize];
        let fp = fingerprint16(subkey);
        self.children
            .iter()
            .find(|child| child.fp == fp && child.subkey == subkey)
    }

    fn best_partial_lcp(&self, key: &Key, span: Span) -> usize {
        let subkey = &key[span.start as usize..span.end as usize];
        self.children
            .iter()
            .map(|child| common_prefix_len(&child.subkey, subkey))
            .filter(|&len| len > 0 && len < span.len())
            .max()
            .unwrap_or(0)
    }

    fn push(&mut self, subkey: &[u8], entry: Entry) {
        debug_assert!(
            !self.children.iter().any(|child| child.subkey == subkey),
            "duplicate child subkey"
        );
        self.children.push(Child::new(subkey, entry));
    }

    fn fp_collision_entries(&self) -> u64 {
        let mut collisions = 0u64;
        for (i, child) in self.children.iter().enumerate() {
            if self.children[..i].iter().any(|prev| prev.fp == child.fp) {
                collisions += 1;
            }
        }
        collisions
    }
}

impl Child {
    fn new(subkey: &[u8], entry: Entry) -> Self {
        Self {
            fp: fingerprint16(subkey),
            subkey: subkey.to_vec(),
            entry,
        }
    }
}

fn insert_entry(entry: &mut Entry, key: Key, depth: usize) -> bool {
    match entry {
        Entry::Leaf(existing) => {
            if *existing == key {
                return false;
            }
            let start = first_diff(existing, &key, depth).expect("distinct keys must diverge");
            let old = Entry::Leaf(*existing);
            *entry = split_existing_entry(old, key, start);
            true
        }
        Entry::Node(node) => {
            if let Some(start) = first_diff(&node.childleaf, &key, depth)
                .filter(|&idx| idx < node.span.start as usize)
            {
                let old = std::mem::replace(entry, Entry::Leaf(key));
                *entry = split_existing_entry(old, key, start);
                return true;
            }

            if let Some(child) = node.children.get_exact_mut(&key, node.span) {
                let inserted = insert_entry(&mut child.entry, key, node.span.end as usize);
                if inserted {
                    node.recompute();
                }
                return inserted;
            }

            let split_rel = node.children.best_partial_lcp(&key, node.span);
            if split_rel > 0 {
                split_node_for_new_child(node, key, split_rel);
                return true;
            }

            let subkey = &key[node.span.start as usize..node.span.end as usize];
            node.children.push(subkey, Entry::Leaf(key));
            node.recompute();
            true
        }
    }
}

fn insert_dense_entry(entry: &mut Entry, key: Key, depth: usize) -> bool {
    match entry {
        Entry::Leaf(existing) => {
            if *existing == key {
                return false;
            }
            let keys = vec![*existing, key];
            *entry = build_dense_entry(keys, depth);
            true
        }
        Entry::Node(node) => {
            if first_diff(&node.childleaf, &key, depth)
                .is_some_and(|idx| idx < node.span.start as usize)
            {
                let mut keys = Vec::with_capacity(node.leaf_count as usize + 1);
                entry.collect_leaves(&mut keys);
                if keys.contains(&key) {
                    return false;
                }
                keys.push(key);
                *entry = build_dense_entry(keys, depth);
                return true;
            }

            if let Some(child) = node.children.get_exact_mut(&key, node.span) {
                let inserted = insert_dense_entry(&mut child.entry, key, node.span.end as usize);
                if inserted {
                    node.recompute();
                }
                return inserted;
            }

            let subkey = &key[node.span.start as usize..node.span.end as usize];
            node.children.push(subkey, Entry::Leaf(key));
            if node.children.children.len() > MAX_FANOUT {
                narrow_dense_node(node);
            } else {
                node.recompute();
            }
            true
        }
    }
}

fn lookup_entry(entry: &Entry, key: &Key, depth: usize) -> bool {
    match entry {
        Entry::Leaf(stored) => stored == key,
        Entry::Node(node) => {
            let span_start = node.span.start as usize;
            let span_end = node.span.end as usize;
            if key[depth..span_start] != node.childleaf[depth..span_start] {
                return false;
            }
            node.children
                .get_exact(key, node.span)
                .is_some_and(|child| lookup_entry(&child.entry, key, span_end))
        }
    }
}

fn has_prefix_entry(entry: &Entry, key: &Key, prefix_len: usize, depth: usize) -> bool {
    match entry {
        Entry::Leaf(stored) => stored[..prefix_len] == key[..prefix_len],
        Entry::Node(node) => {
            let span_start = node.span.start as usize;
            let span_end = node.span.end as usize;
            let skipped_end = prefix_len.min(span_start);
            if key[depth..skipped_end] != node.childleaf[depth..skipped_end] {
                return false;
            }

            if prefix_len <= span_start {
                return true;
            }
            if prefix_len <= span_end {
                let rel = prefix_len - span_start;
                return node
                    .children
                    .children
                    .iter()
                    .any(|child| child.subkey[..rel] == key[span_start..prefix_len]);
            }

            node.children
                .get_exact(key, node.span)
                .is_some_and(|child| has_prefix_entry(&child.entry, key, prefix_len, span_end))
        }
    }
}

fn infixes_entry<const INFIX_LEN: usize>(
    entry: &Entry,
    prefix: &[u8],
    prefix_len: usize,
    infix_end: usize,
    depth: usize,
    out: &mut Vec<[u8; INFIX_LEN]>,
    nodes_visited: &mut u64,
) {
    match entry {
        Entry::Leaf(stored) => {
            if stored[..prefix_len] == *prefix {
                out.push(infix_array::<INFIX_LEN>(stored, prefix_len));
            }
        }
        Entry::Node(node) => {
            *nodes_visited += 1;
            let span_start = node.span.start as usize;
            let span_end = node.span.end as usize;
            let skipped_end = prefix_len.min(span_start);
            if prefix[depth..skipped_end] != node.childleaf[depth..skipped_end] {
                return;
            }

            if prefix_len <= span_start {
                collect_infixes_entry(entry, prefix_len, infix_end, out, nodes_visited, false);
                return;
            }
            if prefix_len <= span_end {
                let rel = prefix_len - span_start;
                for child in &node.children.children {
                    if child.subkey[..rel] == prefix[span_start..prefix_len] {
                        collect_infixes_entry(
                            &child.entry,
                            prefix_len,
                            infix_end,
                            out,
                            nodes_visited,
                            true,
                        );
                    }
                }
                return;
            }

            if let Some(child) = node.children.get_exact_for_prefix(prefix, node.span) {
                infixes_entry(
                    &child.entry,
                    prefix,
                    prefix_len,
                    infix_end,
                    span_end,
                    out,
                    nodes_visited,
                );
            }
        }
    }
}

fn collect_infixes_entry<const INFIX_LEN: usize>(
    entry: &Entry,
    infix_start: usize,
    infix_end: usize,
    out: &mut Vec<[u8; INFIX_LEN]>,
    nodes_visited: &mut u64,
    count_current: bool,
) {
    match entry {
        Entry::Leaf(key) => out.push(infix_array::<INFIX_LEN>(key, infix_start)),
        Entry::Node(node) => {
            if count_current {
                *nodes_visited += 1;
            }
            let span_start = node.span.start as usize;
            let span_end = node.span.end as usize;

            if infix_end <= span_start {
                out.push(infix_array::<INFIX_LEN>(&node.childleaf, infix_start));
            } else if infix_end <= span_end {
                for child in &node.children.children {
                    out.push(infix_array::<INFIX_LEN>(
                        &child.entry.representative(),
                        infix_start,
                    ));
                }
            } else {
                for child in &node.children.children {
                    collect_infixes_entry(
                        &child.entry,
                        infix_start,
                        infix_end,
                        out,
                        nodes_visited,
                        true,
                    );
                }
            }
        }
    }
}

fn infix_array<const INFIX_LEN: usize>(key: &Key, start: usize) -> [u8; INFIX_LEN] {
    key[start..start + INFIX_LEN].try_into().unwrap()
}

fn split_existing_entry(old: Entry, key: Key, start: usize) -> Entry {
    let old_rep = old.representative();
    let child_start = old.min_branch_start();
    let end = next_boundary(start).min(child_start).min(KEY_LEN);
    assert!(
        start < end,
        "cannot split existing entry at [{start},{end})"
    );

    let span = Span::new(start, end);
    let children = vec![
        Child::new(&old_rep[start..end], old),
        Child::new(&key[start..end], Entry::Leaf(key)),
    ];
    Entry::Node(Box::new(Node::new(span, children)))
}

fn split_node_for_new_child(node: &mut Node, key: Key, split_rel: usize) {
    let old_span = node.span;
    let old_start = old_span.start as usize;
    let old_end = old_span.end as usize;
    let split = old_start + split_rel;
    assert!(split > old_start && split < old_end);

    struct Group {
        prefix: Vec<u8>,
        items: Vec<(Vec<u8>, Entry)>,
    }

    fn push_group(groups: &mut Vec<Group>, prefix: &[u8], suffix: &[u8], entry: Entry) {
        if let Some(group) = groups.iter_mut().find(|group| group.prefix == prefix) {
            group.items.push((suffix.to_vec(), entry));
        } else {
            groups.push(Group {
                prefix: prefix.to_vec(),
                items: vec![(suffix.to_vec(), entry)],
            });
        }
    }

    let mut groups = Vec::new();
    for child in std::mem::take(&mut node.children.children) {
        let prefix = &child.subkey[..split_rel];
        let suffix = &child.subkey[split_rel..];
        push_group(&mut groups, prefix, suffix, child.entry);
    }
    push_group(
        &mut groups,
        &key[old_start..split],
        &key[split..old_end],
        Entry::Leaf(key),
    );

    let suffix_span = Span::new(split, old_end);
    let mut parent_children = Vec::with_capacity(groups.len());
    for group in groups {
        let entry = if group.items.len() == 1 {
            group.items.into_iter().next().unwrap().1
        } else {
            let suffix_children = group
                .items
                .into_iter()
                .map(|(suffix, entry)| Child::new(&suffix, entry))
                .collect();
            Entry::Node(Box::new(Node::new(suffix_span, suffix_children)))
        };
        parent_children.push(Child::new(&group.prefix, entry));
    }

    node.span = Span::new(old_start, split);
    node.children.children = parent_children;
    node.recompute();
}

impl Entry {
    fn collect_leaves(&self, out: &mut Vec<Key>) {
        match self {
            Entry::Leaf(key) => out.push(*key),
            Entry::Node(node) => {
                for child in &node.children.children {
                    child.entry.collect_leaves(out);
                }
            }
        }
    }
}

fn build_dense_entry(keys: Vec<Key>, depth: usize) -> Entry {
    debug_assert!(!keys.is_empty());
    if keys.len() == 1 {
        return Entry::Leaf(keys[0]);
    }

    let start = first_varying_depth(&keys, depth).expect("duplicate keys reached dense rebuild");
    let end = widest_valid_span(&keys, start);
    let children = dense_children(keys, start, end);
    Entry::Node(Box::new(Node::new(Span::new(start, end), children)))
}

fn narrow_dense_node(node: &mut Node) {
    let start = node.span.start as usize;
    let current_end = node.span.end as usize;
    let old_children = std::mem::take(&mut node.children.children);
    let mut keys = Vec::with_capacity(node.leaf_count as usize);
    for child in old_children {
        child.entry.collect_leaves(&mut keys);
    }
    let end = widest_valid_span_with_limit(&keys, start, current_end);
    node.span = Span::new(start, end);
    node.children.children = dense_children(keys, start, end);
    node.recompute();
}

fn dense_children(mut keys: Vec<Key>, start: usize, end: usize) -> Vec<Child> {
    keys.sort_unstable_by(|left, right| left[start..end].cmp(&right[start..end]));

    let mut children = Vec::new();
    let mut i = 0;
    while i < keys.len() {
        let mut j = i + 1;
        while j < keys.len() && keys[j][start..end] == keys[i][start..end] {
            j += 1;
        }
        children.push(Child::new(
            &keys[i][start..end],
            build_dense_entry(keys[i..j].to_vec(), end),
        ));
        i = j;
    }
    children
}

fn first_varying_depth(keys: &[Key], depth: usize) -> Option<usize> {
    let first = keys[0];
    (depth..KEY_LEN).find(|&idx| keys.iter().any(|key| key[idx] != first[idx]))
}

fn widest_valid_span(keys: &[Key], start: usize) -> usize {
    widest_valid_span_with_limit(keys, start, next_boundary(start))
}

fn widest_valid_span_with_limit(keys: &[Key], start: usize, limit: usize) -> usize {
    for end in (start + 1..=limit).rev() {
        if distinct_key_prefixes(keys, start, end) <= MAX_FANOUT {
            return end;
        }
    }
    unreachable!("one byte can only have 256 distinct prefixes")
}

fn distinct_key_prefixes(keys: &[Key], start: usize, end: usize) -> usize {
    let mut refs: Vec<&Key> = keys.iter().collect();
    refs.sort_unstable_by(|left, right| left[start..end].cmp(&right[start..end]));
    let mut distinct = 0usize;
    let mut prev: Option<&[u8]> = None;
    for key in refs {
        let prefix = &key[start..end];
        if prev != Some(prefix) {
            distinct += 1;
            if distinct > MAX_FANOUT {
                break;
            }
            prev = Some(prefix);
        }
    }
    distinct
}

fn leaf_hash(key: &Key) -> u128 {
    SipHasher24::new_with_key(&SIP_KEY).hash(key).into()
}

fn fingerprint16(bytes: &[u8]) -> u16 {
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for byte in bytes {
        h ^= *byte as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (h ^ (h >> 32) ^ (h >> 16)) as u16
}

fn first_diff(a: &Key, b: &Key, start: usize) -> Option<usize> {
    (start..KEY_LEN).find(|&idx| a[idx] != b[idx])
}

fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter()
        .zip(b)
        .take_while(|(left, right)| left == right)
        .count()
}

fn next_boundary(start: usize) -> usize {
    match start {
        0..=15 => ENTITY_END as usize,
        16..=31 => ATTRIBUTE_END as usize,
        32..=63 => VALUE_END as usize,
        KEY_LEN => VALUE_END as usize,
        _ => panic!("invalid HATCH depth {start}"),
    }
}

fn segment_of_depth(depth: usize) -> u8 {
    match depth {
        0..=15 => 0,
        16..=31 => 1,
        32..=64 => 2,
        _ => panic!("invalid HATCH depth {depth}"),
    }
}

fn fanout_bucket(fanout: usize) -> usize {
    match fanout {
        0..=1 => 0,
        2..=3 => 1,
        4..=7 => 2,
        8..=15 => 3,
        16..=31 => 4,
        32..=63 => 5,
        64..=127 => 6,
        128..=256 => 7,
        _ => panic!("unexpected HATCH fanout {fanout}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(e: u8, a: u8, v: u8) -> Key {
        let mut key = [0u8; KEY_LEN];
        key[0] = e;
        key[16] = a;
        key[32] = v;
        key
    }

    #[test]
    fn duplicate_insert_is_idempotent() {
        let mut hatch = Hatch::new();
        let k = key(1, 2, 3);
        assert!(hatch.insert(k));
        let hash = hatch.root_hash();
        assert!(!hatch.insert(k));
        assert_eq!(hatch.leaf_count(), 1);
        assert_eq!(hatch.root_hash(), hash);
    }

    #[test]
    fn wide_infixes_enumerates_distinct_next_segments() {
        let keys = [key(1, 10, 1), key(1, 10, 2), key(1, 11, 1), key(2, 10, 1)];
        let mut hatch = HatchWide::new();
        for key in keys {
            assert!(hatch.insert(key));
        }

        let mut e_prefix = [0u8; 16];
        e_prefix[0] = 1;
        let mut attrs = Vec::new();
        let visited = hatch.infixes::<16>(&e_prefix, 16, 16, &mut attrs);
        attrs.sort_unstable();
        assert!(visited > 0);
        assert_eq!(attrs.len(), 2);
        assert_eq!(attrs[0][0], 10);
        assert_eq!(attrs[1][0], 11);

        let mut ea_prefix = [0u8; 32];
        ea_prefix[0] = 1;
        ea_prefix[16] = 10;
        let mut values = Vec::new();
        hatch.infixes::<32>(&ea_prefix, 32, 32, &mut values);
        values.sort_unstable();
        assert_eq!(values.len(), 2);
        assert_eq!(values[0][0], 1);
        assert_eq!(values[1][0], 2);
    }
}
