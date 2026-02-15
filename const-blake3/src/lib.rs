//! This is the reference implementation of BLAKE3. It is used for testing and
//! as a readable example of the algorithms involved. Section 5.1 of [the BLAKE3
//! spec](https://github.com/BLAKE3-team/BLAKE3-specs/blob/master/blake3.pdf)
//! discusses this implementation. You can render docs for this implementation
//! by running `cargo doc --open` in this directory.
//!
//! # Example
//!
//! ```
//! let mut hasher = const_blake3::Hasher::new();
//! hasher.update(b"abc");
//! hasher.update(b"def");
//! let mut hash = [0; 32];
//! hasher.finalize(&mut hash);
//! let mut extended_hash = [0; 500];
//! hasher.finalize(&mut extended_hash);
//! assert_eq!(hash, extended_hash[..32]);
//! ```

const KEY_LEN: usize = 32;
const BLOCK_LEN: usize = 64;
const WORD_BYTES: usize = core::mem::size_of::<u32>();
const CHUNK_LEN: usize = 1024;

// WORKAROUND: `core::cmp::min` is not yet `const fn` on stable because it
// depends on the still-unstable `const_cmp` work that makes the comparison
// traits callable in const contexts (rust-lang/rust#143800). Keep a tiny
// specialized version until that lands.
const fn min_usize(a: usize, b: usize) -> usize {
    if a < b {
        a
    } else {
        b
    }
}

const CHUNK_START: u32 = 1 << 0;
const CHUNK_END: u32 = 1 << 1;
const PARENT: u32 = 1 << 2;
const ROOT: u32 = 1 << 3;
const KEYED_HASH: u32 = 1 << 4;
const DERIVE_KEY_CONTEXT: u32 = 1 << 5;
const DERIVE_KEY_MATERIAL: u32 = 1 << 6;

const IV: [u32; 8] = [
    0x6A09E667, 0xBB67AE85, 0x3C6EF372, 0xA54FF53A, 0x510E527F, 0x9B05688C, 0x1F83D9AB, 0x5BE0CD19,
];

const MSG_PERMUTATION: [usize; 16] = [2, 6, 3, 10, 7, 0, 4, 13, 1, 11, 12, 5, 9, 14, 15, 8];

// The mixing function, G, which mixes either a column or a diagonal.
const fn g(state: &mut [u32; 16], a: usize, b: usize, c: usize, d: usize, mx: u32, my: u32) {
    state[a] = state[a].wrapping_add(state[b]).wrapping_add(mx);
    state[d] = (state[d] ^ state[a]).rotate_right(16);
    state[c] = state[c].wrapping_add(state[d]);
    state[b] = (state[b] ^ state[c]).rotate_right(12);
    state[a] = state[a].wrapping_add(state[b]).wrapping_add(my);
    state[d] = (state[d] ^ state[a]).rotate_right(8);
    state[c] = state[c].wrapping_add(state[d]);
    state[b] = (state[b] ^ state[c]).rotate_right(7);
}

const fn round(state: &mut [u32; 16], m: &[u32; 16]) {
    // Mix the columns.
    g(state, 0, 4, 8, 12, m[0], m[1]);
    g(state, 1, 5, 9, 13, m[2], m[3]);
    g(state, 2, 6, 10, 14, m[4], m[5]);
    g(state, 3, 7, 11, 15, m[6], m[7]);
    // Mix the diagonals.
    g(state, 0, 5, 10, 15, m[8], m[9]);
    g(state, 1, 6, 11, 12, m[10], m[11]);
    g(state, 2, 7, 8, 13, m[12], m[13]);
    g(state, 3, 4, 9, 14, m[14], m[15]);
}

const fn permute(m: &mut [u32; 16]) {
    let mut permuted = [0; 16];
    // WORKAROUND: `for` loops lower to iterator machinery that isn't const yet
    // (rust-lang/rust#87575), so we expand the loop manually.
    let mut i = 0;
    while i < 16 {
        permuted[i] = m[MSG_PERMUTATION[i]];
        i += 1;
    }
    *m = permuted;
}

const fn compress(
    chaining_value: &[u32; 8],
    block_words: &[u32; 16],
    counter: u64,
    block_len: u32,
    flags: u32,
) -> [u32; 16] {
    let counter_low = counter as u32;
    let counter_high = (counter >> 32) as u32;
    #[rustfmt::skip]
    let mut state = [
        chaining_value[0], chaining_value[1], chaining_value[2], chaining_value[3],
        chaining_value[4], chaining_value[5], chaining_value[6], chaining_value[7],
        IV[0],             IV[1],             IV[2],             IV[3],
        counter_low,       counter_high,      block_len,         flags,
    ];
    let mut block = *block_words;

    round(&mut state, &block); // round 1
    permute(&mut block);
    round(&mut state, &block); // round 2
    permute(&mut block);
    round(&mut state, &block); // round 3
    permute(&mut block);
    round(&mut state, &block); // round 4
    permute(&mut block);
    round(&mut state, &block); // round 5
    permute(&mut block);
    round(&mut state, &block); // round 6
    permute(&mut block);
    round(&mut state, &block); // round 7

    // WORKAROUND: same as above, we use a manual loop until `for` is const
    // (rust-lang/rust#87575).
    let mut i = 0;
    while i < 8 {
        state[i] ^= state[i + 8];
        state[i + 8] ^= chaining_value[i];
        i += 1;
    }
    state
}

const fn first_8_words(compression_output: [u32; 16]) -> [u32; 8] {
    let mut result = [0; 8];
    // WORKAROUND: array conversions like `try_into()` rely on traits that are
    // not const-callable yet (rust-lang/rust#143773), so copy the words by
    // hand for now.
    let mut i = 0;
    while i < 8 {
        result[i] = compression_output[i];
        i += 1;
    }
    result
}

const fn words_from_little_endian_bytes(bytes: &[u8], words: &mut [u32]) {
    let mut word_index = 0;
    // WORKAROUND: slice iterators such as `chunks_exact` are not available in
    // const contexts (rust-lang/rust#137737), so we index manually.
    while word_index < words.len() {
        let byte_index = word_index * 4;
        words[word_index] = u32::from_le_bytes([
            bytes[byte_index],
            bytes[byte_index + 1],
            bytes[byte_index + 2],
            bytes[byte_index + 3],
        ]);
        word_index += 1;
    }
}

// Each chunk or parent node can produce either an 8-word chaining value or, by
// setting the ROOT flag, any number of final output bytes. The Output struct
// captures the state just prior to choosing between those two possibilities.
struct Output {
    input_chaining_value: [u32; 8],
    block_words: [u32; 16],
    counter: u64,
    block_len: u32,
    flags: u32,
}

impl Output {
    const fn chaining_value(&self) -> [u32; 8] {
        first_8_words(compress(
            &self.input_chaining_value,
            &self.block_words,
            self.counter,
            self.block_len,
            self.flags,
        ))
    }

    const fn root_output_bytes(&self, out_slice: &mut [u8]) {
        let mut offset = 0;
        let total = out_slice.len();
        let block_bytes = BLOCK_LEN;
        let mut words = [0; 16];
        let mut word_bytes: [u8; WORD_BYTES] = [0; WORD_BYTES];
        // WORKAROUND: We can't rely on `chunks_mut` or iterator adapters to
        // walk `out_slice` yet, since they depend on non-const traits
        // (rust-lang/rust#87575). Instead we manage a running offset and
        // re-compute positions with division/modulo inside a single loop.
        while offset < total {
            let block_index = offset / block_bytes;
            let block_byte_index = offset % block_bytes;

            if block_byte_index == 0 {
                words = compress(
                    &self.input_chaining_value,
                    &self.block_words,
                    block_index as u64,
                    self.block_len,
                    self.flags | ROOT,
                );
            }

            let word_index = block_byte_index / WORD_BYTES;
            let byte_index = block_byte_index % WORD_BYTES;
            if byte_index == 0 {
                word_bytes = words[word_index].to_le_bytes();
            }

            out_slice[offset] = word_bytes[byte_index];
            offset += 1;
        }
    }
}

struct ChunkState {
    chaining_value: [u32; 8],
    chunk_counter: u64,
    block: [u8; BLOCK_LEN],
    block_len: u8,
    blocks_compressed: u8,
    flags: u32,
}

impl ChunkState {
    const fn new(key_words: [u32; 8], chunk_counter: u64, flags: u32) -> Self {
        Self {
            chaining_value: key_words,
            chunk_counter,
            block: [0; BLOCK_LEN],
            block_len: 0,
            blocks_compressed: 0,
            flags,
        }
    }

    const fn len(&self) -> usize {
        BLOCK_LEN * self.blocks_compressed as usize + self.block_len as usize
    }

    const fn start_flag(&self) -> u32 {
        if self.blocks_compressed == 0 {
            CHUNK_START
        } else {
            0
        }
    }

    const fn update(&mut self, input: &[u8], start: usize, len: usize) {
        let mut input_index = 0;
        while input_index < len {
            // If the block buffer is full, compress it and clear it. More
            // input is coming, so this compression is not CHUNK_END.
            if self.block_len as usize == BLOCK_LEN {
                let mut block_words = [0; 16];
                words_from_little_endian_bytes(&self.block, &mut block_words);
                self.chaining_value = first_8_words(compress(
                    &self.chaining_value,
                    &block_words,
                    self.chunk_counter,
                    BLOCK_LEN as u32,
                    self.flags | self.start_flag(),
                ));
                self.blocks_compressed += 1;
                self.block = [0; BLOCK_LEN];
                self.block_len = 0;
            }

            // Copy input bytes into the block buffer.
            let want = BLOCK_LEN - self.block_len as usize;
            let remaining = len - input_index;
            let take = min_usize(want, remaining);
            // WORKAROUND: slicing with ranges would call `Index`/`IndexMut`,
            // which aren't const-stable yet (rust-lang/rust#143772). Copy byte
            // by byte instead of using `copy_from_slice` on a subslice.
            let mut i = 0;
            while i < take {
                self.block[self.block_len as usize + i] = input[start + input_index + i];
                i += 1;
            }
            self.block_len += take as u8;
            input_index += take;
        }
    }

    const fn output(&self) -> Output {
        let mut block_words = [0; 16];
        words_from_little_endian_bytes(&self.block, &mut block_words);
        Output {
            input_chaining_value: self.chaining_value,
            block_words,
            counter: self.chunk_counter,
            block_len: self.block_len as u32,
            flags: self.flags | self.start_flag() | CHUNK_END,
        }
    }
}

const fn parent_output(
    left_child_cv: [u32; 8],
    right_child_cv: [u32; 8],
    key_words: [u32; 8],
    flags: u32,
) -> Output {
    let mut block_words = [0; 16];
    let mut i = 0;
    // WORKAROUND: building subslices for `copy_from_slice` would require
    // `Index` in const contexts (rust-lang/rust#143772), so copy by hand.
    while i < 8 {
        block_words[i] = left_child_cv[i];
        i += 1;
    }
    let mut j = 0;
    while j < 8 {
        block_words[8 + j] = right_child_cv[j];
        j += 1;
    }
    Output {
        input_chaining_value: key_words,
        block_words,
        counter: 0,                  // Always 0 for parent nodes.
        block_len: BLOCK_LEN as u32, // Always BLOCK_LEN (64) for parent nodes.
        flags: PARENT | flags,
    }
}

const fn parent_cv(
    left_child_cv: [u32; 8],
    right_child_cv: [u32; 8],
    key_words: [u32; 8],
    flags: u32,
) -> [u32; 8] {
    parent_output(left_child_cv, right_child_cv, key_words, flags).chaining_value()
}

/// An incremental hasher that can accept any number of writes.
pub struct Hasher {
    chunk_state: ChunkState,
    key_words: [u32; 8],
    cv_stack: [[u32; 8]; 54], // Space for 54 subtree chaining values:
    cv_stack_len: u8,         // 2^54 * CHUNK_LEN = 2^64
    flags: u32,
}

impl Hasher {
    const fn new_internal(key_words: [u32; 8], flags: u32) -> Self {
        Self {
            chunk_state: ChunkState::new(key_words, 0, flags),
            key_words,
            cv_stack: [[0; 8]; 54],
            cv_stack_len: 0,
            flags,
        }
    }

    /// Construct a new `Hasher` for the regular hash function.
    pub const fn new() -> Self {
        Self::new_internal(IV, 0)
    }

    /// Construct a new `Hasher` for the keyed hash function.
    pub const fn new_keyed(key: &[u8; KEY_LEN]) -> Self {
        let mut key_words = [0; 8];
        words_from_little_endian_bytes(key, &mut key_words);
        Self::new_internal(key_words, KEYED_HASH)
    }

    /// Construct a new `Hasher` for the key derivation function. The context
    /// string should be hardcoded, globally unique, and application-specific.
    pub const fn new_derive_key(context: &str) -> Self {
        let mut context_hasher = Self::new_internal(IV, DERIVE_KEY_CONTEXT);
        context_hasher.update(context.as_bytes());
        let mut context_key = [0; KEY_LEN];
        context_hasher.finalize(&mut context_key);
        let mut context_key_words = [0; 8];
        words_from_little_endian_bytes(&context_key, &mut context_key_words);
        Self::new_internal(context_key_words, DERIVE_KEY_MATERIAL)
    }

    const fn push_stack(&mut self, cv: [u32; 8]) {
        self.cv_stack[self.cv_stack_len as usize] = cv;
        self.cv_stack_len += 1;
    }

    const fn pop_stack(&mut self) -> [u32; 8] {
        self.cv_stack_len -= 1;
        self.cv_stack[self.cv_stack_len as usize]
    }

    // Section 5.1.2 of the BLAKE3 spec explains this algorithm in more detail.
    const fn add_chunk_chaining_value(&mut self, mut new_cv: [u32; 8], mut total_chunks: u64) {
        // This chunk might complete some subtrees. For each completed subtree,
        // its left child will be the current top entry in the CV stack, and
        // its right child will be the current value of `new_cv`. Pop each left
        // child off the stack, merge it with `new_cv`, and overwrite `new_cv`
        // with the result. After all these merges, push the final value of
        // `new_cv` onto the stack. The number of completed subtrees is given
        // by the number of trailing 0-bits in the new total number of chunks.
        while total_chunks & 1 == 0 {
            new_cv = parent_cv(self.pop_stack(), new_cv, self.key_words, self.flags);
            total_chunks >>= 1;
        }
        self.push_stack(new_cv);
    }

    /// Add input to the hash state. This can be called any number of times.
    pub const fn update(&mut self, input: &[u8]) {
        let mut input_index = 0;
        let input_len = input.len();
        while input_index < input_len {
            // If the current chunk is complete, finalize it and reset the
            // chunk state. More input is coming, so this chunk is not ROOT.
            if self.chunk_state.len() == CHUNK_LEN {
                let chunk_cv = self.chunk_state.output().chaining_value();
                let total_chunks = self.chunk_state.chunk_counter + 1;
                self.add_chunk_chaining_value(chunk_cv, total_chunks);
                self.chunk_state = ChunkState::new(self.key_words, total_chunks, self.flags);
            }

            // Compress input bytes into the current chunk state.
            let want = CHUNK_LEN - self.chunk_state.len();
            let remaining = input_len - input_index;
            let take = min_usize(want, remaining);
            // WORKAROUND: slicing `input[take..]` would need `Index` in const
            // contexts (rust-lang/rust#143772), so we track the offset instead
            // of reborrowing subslices.
            self.chunk_state.update(input, input_index, take);
            input_index += take;
        }
    }

    /// Finalize the hash and write any number of output bytes.
    pub const fn finalize(&self, out_slice: &mut [u8]) {
        // Starting with the Output from the current chunk, compute all the
        // parent chaining values along the right edge of the tree, until we
        // have the root Output.
        let mut output = self.chunk_state.output();
        let mut parent_nodes_remaining = self.cv_stack_len as usize;
        while parent_nodes_remaining > 0 {
            parent_nodes_remaining -= 1;
            output = parent_output(
                self.cv_stack[parent_nodes_remaining],
                output.chaining_value(),
                self.key_words,
                self.flags,
            );
        }
        output.root_output_bytes(out_slice);
    }
}

#[cfg(kani)]
mod proofs;
