use cubecl::client::ComputeClient;
use cubecl::prelude::*;
use cubecl::server::Handle;
use triblespace_core::blob::encodings::succinctarchive::{
    SuccinctRotation, WaveletMatrixFreezeBackend,
};

use crate::{FreezeGeometry, GpuFreezeError};

const BLOCK_SIZE: u32 = super::BLOCK_SIZE;
const THREADS: u32 = super::THREADS;

fn read_packed_u32(bytes: &[u8], index: usize) -> Result<u32, GpuFreezeError> {
    let start = index
        .checked_mul(std::mem::size_of::<u32>())
        .ok_or(GpuFreezeError::GeometryOverflow("host packed byte offset"))?;
    let end = start
        .checked_add(std::mem::size_of::<u32>())
        .ok_or(GpuFreezeError::GeometryOverflow("host packed byte end"))?;
    let word = bytes.get(start..end).ok_or(GpuFreezeError::ReadbackSize {
        expected: end,
        actual: bytes.len(),
    })?;
    let mut raw = [0; std::mem::size_of::<u32>()];
    raw.copy_from_slice(word);
    Ok(u32::from_ne_bytes(raw))
}

#[cube(launch_unchecked)]
fn block_count_pack(
    codes: &Array<u32>,
    layers: &mut Array<u32>,
    block_zeros: &mut Array<u32>,
    params: &Array<u32>,
    #[comptime] block_size: u32,
) {
    let block = ABSOLUTE_POS as u32;
    let num_blocks = params[3];
    if block < num_blocks {
        let len = params[0];
        let shift = params[1];
        let layer_word_offset = params[2];
        let block_size = u32::cast_from(block_size);
        let start = block * block_size;
        let remaining = len - start;
        let span = if remaining < block_size {
            remaining
        } else {
            block_size
        };
        let end = start + span;
        let word_base = start / 32u32;
        let mut zeros = u32::cast_from(0u32);
        let mut word = u32::cast_from(0u32);
        let mut word_bit = u32::cast_from(0u32);
        let mut word_index = u32::cast_from(0u32);
        let mut position = start;
        while position < end {
            let bit = (codes[position as usize] >> shift) & 1u32;
            zeros += 1u32 - bit;
            word |= bit << word_bit;
            word_bit += 1u32;
            if word_bit == 32u32 {
                layers[(layer_word_offset + word_base + word_index) as usize] = word;
                word = u32::cast_from(0u32);
                word_bit = u32::cast_from(0u32);
                word_index += 1u32;
            }
            position += 1u32;
        }
        if word_bit > 0u32 {
            layers[(layer_word_offset + word_base + word_index) as usize] = word;
        }
        block_zeros[block as usize] = zeros;
    }
}

/// This vector has `ceil(len / 256)` entries. Keeping its sequential scan on
/// the device avoids one readback and upload per wavelet level. A hierarchical
/// scan can replace it later without changing the core backend contract.
#[cube(launch_unchecked)]
fn scan_block_zeros(
    block_zeros: &Array<u32>,
    block_zero_offsets: &mut Array<u32>,
    total_zeros: &mut Array<u32>,
) {
    if ABSOLUTE_POS == 0 {
        let mut total = u32::cast_from(0u32);
        let mut block = u32::cast_from(0u32);
        let block_count = u32::cast_from(block_zeros.len());
        while block < block_count {
            block_zero_offsets[block as usize] = total;
            total += block_zeros[block as usize];
            block += 1u32;
        }
        total_zeros[0] = total;
    }
}

#[cube(launch_unchecked)]
fn stable_scatter(
    codes: &Array<u32>,
    output: &mut Array<u32>,
    block_zero_offsets: &Array<u32>,
    total_zeros: &Array<u32>,
    params: &Array<u32>,
    #[comptime] block_size: u32,
) {
    let block = ABSOLUTE_POS as u32;
    let num_blocks = params[3];
    if block < num_blocks {
        let len = params[0];
        let shift = params[1];
        let total_zeros = total_zeros[0];
        let block_size = u32::cast_from(block_size);
        let start = block * block_size;
        let remaining = len - start;
        let span = if remaining < block_size {
            remaining
        } else {
            block_size
        };
        let end = start + span;
        let zero_base = block_zero_offsets[block as usize];
        let one_base = total_zeros + (start - zero_base);
        let mut local_zero = u32::cast_from(0u32);
        let mut local_one = u32::cast_from(0u32);
        let mut position = start;
        while position < end {
            let code = codes[position as usize];
            let bit = (code >> shift) & 1u32;
            if bit == 0u32 {
                output[(zero_base + local_zero) as usize] = code;
                local_zero += 1u32;
            } else {
                output[(one_base + local_one) as usize] = code;
                local_one += 1u32;
            }
            position += 1u32;
        }
    }
}

/// Whole number of host pages required by Metal's `newBufferWithBytesNoCopy:`.
/// Apple silicon uses 16 KiB pages; 16 KiB-aligned, 16 KiB-granular regions are
/// also valid on 4 KiB-page hosts because 16 KiB is a multiple of 4 KiB.
#[cfg(all(feature = "wgpu", target_os = "macos"))]
const HOST_PAGE_BYTES: usize = 16 * 1024;

/// Page-aligned, page-granular, exclusively owned host allocation backing a
/// registered sequence buffer. The whole region belongs to this allocation, so
/// no foreign heap data ever shares a page with a buffer the GPU can see.
#[cfg(all(feature = "wgpu", target_os = "macos"))]
struct AlignedArena {
    ptr: std::ptr::NonNull<u8>,
    bytes: usize,
}

#[cfg(all(feature = "wgpu", target_os = "macos"))]
impl AlignedArena {
    fn layout(bytes: usize) -> std::alloc::Layout {
        std::alloc::Layout::from_size_align(bytes, HOST_PAGE_BYTES)
            .expect("page-rounded arena layout")
    }

    /// Allocate `bytes` (a nonzero whole number of pages) of zeroed memory.
    fn zeroed(bytes: usize) -> Self {
        debug_assert!(bytes > 0 && bytes % HOST_PAGE_BYTES == 0);
        let layout = Self::layout(bytes);
        // Zero-initialized so the registered tail beyond any pass's logical
        // length is defined bytes, even though no kernel indexes it.
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        let Some(ptr) = std::ptr::NonNull::new(ptr) else {
            std::alloc::handle_alloc_error(layout);
        };
        Self { ptr, bytes }
    }

    fn ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }
}

// SAFETY: the arena is a plain owned allocation. Host writes go through raw
// pointers (never references into the buffer) and are serialized by the
// take-the-entry-out-of-the-slot discipline in `registered_sequence`, so no
// two threads ever write it concurrently and no thread writes it while GPU
// submissions referencing it are in flight.
#[cfg(all(feature = "wgpu", target_os = "macos"))]
unsafe impl Send for AlignedArena {}
#[cfg(all(feature = "wgpu", target_os = "macos"))]
unsafe impl Sync for AlignedArena {}

#[cfg(all(feature = "wgpu", target_os = "macos"))]
impl Drop for AlignedArena {
    fn drop(&mut self) {
        // Runs only after every `Arc` clone is gone — including the keepalive
        // clone the registration hands to the runtime's storage, which the
        // device holds until it is itself dropped. The host pages therefore
        // outlive every command submission that can reference them.
        unsafe { std::alloc::dealloc(self.ptr.as_ptr(), Self::layout(self.bytes)) };
    }
}

/// One live external-buffer registration of an [`AlignedArena`].
///
/// An entry rests in the backend's shared slot only between passes, and only
/// after the pass that used it drained the device queue with a successful
/// `client.sync()`. A pass takes the entry out of the slot for exclusive use,
/// so rewriting the arena before launching is free of both host-side data
/// races and host-write-vs-GPU-read hazards.
#[cfg(all(feature = "wgpu", target_os = "macos"))]
struct RegisteredSequenceArena {
    arena: std::sync::Arc<AlignedArena>,
    handle: Handle,
}

/// Where a freeze pass's rotation codes live on the device.
enum SequenceInput {
    /// Transient device buffer filled by an ordinary host-to-device upload.
    Uploaded(Handle),
    /// Zero-upload registration of backend-owned host pages that the GPU
    /// reads in place through unified memory.
    #[cfg(all(feature = "wgpu", target_os = "macos"))]
    Registered(RegisteredSequenceArena),
}

impl SequenceInput {
    fn handle(&self) -> Handle {
        match self {
            Self::Uploaded(handle) => handle.clone(),
            #[cfg(all(feature = "wgpu", target_os = "macos"))]
            Self::Registered(entry) => entry.handle.clone(),
        }
    }

    fn is_registered(&self) -> bool {
        match self {
            Self::Uploaded(_) => false,
            #[cfg(all(feature = "wgpu", target_os = "macos"))]
            Self::Registered(_) => true,
        }
    }
}

/// Reusable CubeCL wavelet-freeze backend.
///
/// Construct it once and reuse it across LSM compactions so CubeCL's runtime,
/// shader cache, and allocator survive between merges. Each rotation stages
/// its codes once — on Apple silicon by copying them into a reusable
/// page-aligned host arena the GPU reads in place, elsewhere by a
/// host-to-device upload — keeps all stable partitions device-resident,
/// explicitly synchronizes queued work, and then performs one final readback.
///
/// Returned device and geometry errors are suitable for
/// `AcceleratedSuccinctRollup`'s CPU fallback. Allocation failure, runtime
/// panic, abort, and OOM are not converted into `GpuFreezeError`.
pub struct CubeClWaveletFreeze<R: Runtime> {
    client: ComputeClient<R>,
    #[cfg(all(feature = "wgpu", target_os = "macos"))]
    sequence_arena: std::sync::Mutex<Option<RegisteredSequenceArena>>,
    #[cfg(all(feature = "wgpu", target_os = "macos"))]
    upload_sequences: bool,
}

impl<R: Runtime> CubeClWaveletFreeze<R> {
    /// Use `device` for all subsequent freeze passes.
    pub fn new(device: &R::Device) -> Self {
        Self {
            client: R::client(device),
            #[cfg(all(feature = "wgpu", target_os = "macos"))]
            sequence_arena: std::sync::Mutex::new(None),
            // The ordinary upload path is the default: the registered-arena
            // seam was found unsound (arena reuse violates the fork's
            // handle-lifetime immutability because registrations are never
            // released) and is reverted on `main` (`4900615f`). This branch
            // predates the revert; integration rebases onto it. Nothing on
            // this branch selects the registered path.
            #[cfg(all(feature = "wgpu", target_os = "macos"))]
            upload_sequences: true,
        }
    }

    /// Force the transient host-to-device upload input path even where the
    /// registration seam is available. Parity harnesses use this to compare
    /// the registered and uploaded inputs byte for byte; production callers
    /// should keep the default.
    #[cfg(all(feature = "wgpu", target_os = "macos"))]
    pub fn with_uploaded_sequences(mut self, upload: bool) -> Self {
        self.upload_sequences = upload;
        self
    }

    /// Whether this runtime's server implements `register_external_aliased`.
    /// Only the WGPU server does (Metal, macOS); every other server keeps the
    /// panicking default, so the registered path must never be selected there.
    #[cfg(all(feature = "wgpu", target_os = "macos"))]
    fn runtime_registers_host_buffers() -> bool {
        core::any::TypeId::of::<R>() == core::any::TypeId::of::<cubecl::wgpu::WgpuRuntime>()
    }

    /// Allocate a fresh page-rounded arena of `capacity` bytes and register it
    /// with the device as an immutable external buffer.
    #[cfg(all(feature = "wgpu", target_os = "macos"))]
    fn register_arena(&self, capacity: usize) -> RegisteredSequenceArena {
        let arena = std::sync::Arc::new(AlignedArena::zeroed(capacity));
        // SAFETY — the four properties the registration contract demands:
        // * Alignment: `AlignedArena` allocates with 16 KiB alignment and a
        //   16 KiB-multiple length, satisfying `newBufferWithBytesNoCopy:`'s
        //   page-aligned base + whole-pages length requirement on both Apple
        //   silicon (16 KiB pages) and 4 KiB-page hosts. `offset` 0 trivially
        //   meets Metal's buffer-binding offset alignment.
        // * Immutability: the arena is written only in `registered_sequence`,
        //   which owns the entry exclusively (taken out of the shared slot)
        //   and writes strictly before any launch of the pass. Entries return
        //   to the slot only after a successful `client.sync()` drained every
        //   submission referencing them, so no host write ever overlaps a
        //   kernel that reads the buffer. The whole region is this allocation
        //   alone — no foreign heap data shares its pages — and the handle is
        //   used exclusively as a kernel *input*; the runtime additionally
        //   pins it `can_mut() == false` so it can never become an in-place
        //   destination.
        // * Lifetime: the `keepalive` Arc clone passed here is held by the
        //   runtime's storage until the device itself is dropped, so the host
        //   pages outlive every command submission that can reference the
        //   handle — even if this backend (and its own Arc) is dropped while
        //   work is still queued.
        // * Coherency: the buffer is created with `StorageModeShared` on
        //   unified memory; Metal guarantees CPU writes issued before a
        //   command buffer is committed are visible to the GPU, and our
        //   write-then-launch program order (single pass, one thread) plus
        //   the sync-before-reuse discipline above provides exactly that
        //   ordering. The GPU never writes the buffer, so no GPU-to-CPU
        //   coherency obligation exists.
        let handle = unsafe {
            self.client.register_external_aliased(
                arena.ptr() as *mut core::ffi::c_void,
                capacity as u64,
                0,
                capacity as u64,
                std::sync::Arc::clone(&arena) as std::sync::Arc<dyn std::any::Any + Send + Sync>,
            )
        };
        RegisteredSequenceArena { arena, handle }
    }

    /// Stage `sequence` in the reusable registered host arena, growing (and
    /// re-registering) it when a pass needs more capacity.
    #[cfg(all(feature = "wgpu", target_os = "macos"))]
    fn registered_sequence(
        &self,
        sequence: &[u32],
        sequence_bytes: usize,
    ) -> Result<RegisteredSequenceArena, GpuFreezeError> {
        let resident = self.sequence_arena.lock().unwrap().take();
        let entry = match resident {
            Some(entry) if entry.arena.bytes >= sequence_bytes => entry,
            stale => {
                // Grow geometrically so steadily growing compactions
                // re-register O(log) times. Dropping `stale` releases only our
                // Arc and handle clones: its registration (and keepalive) stay
                // with the device, so pages of superseded arenas remain valid
                // until the device drops. That retention is bounded by the
                // geometric growth to less than the final arena's size.
                let previous = stale.map_or(0, |entry| entry.arena.bytes);
                let capacity = sequence_bytes
                    .max(previous.saturating_mul(2))
                    .checked_add(HOST_PAGE_BYTES - 1)
                    .ok_or(GpuFreezeError::GeometryOverflow("registered arena bytes"))?
                    & !(HOST_PAGE_BYTES - 1);
                self.register_arena(capacity)
            }
        };
        // SAFETY: the entry is exclusively ours (fresh, or taken from the slot
        // where only fully synchronized passes return it), so no kernel is
        // reading and no other thread is writing these pages. Raw-pointer
        // writes; no `&mut` to the buffer is ever formed. Stale bytes beyond
        // `sequence_bytes` are never indexed by the kernels, which bound every
        // access by the pass's length parameter.
        unsafe {
            std::ptr::copy_nonoverlapping(
                sequence.as_ptr().cast::<u8>(),
                entry.arena.ptr(),
                sequence_bytes,
            );
        }
        Ok(entry)
    }

    /// Stage the rotation codes for the device: registered host pages on the
    /// Apple-silicon WGPU runtime, an ordinary upload everywhere else.
    fn sequence_input(
        &self,
        sequence: &[u32],
        sequence_bytes: usize,
    ) -> Result<SequenceInput, GpuFreezeError> {
        #[cfg(all(feature = "wgpu", target_os = "macos"))]
        if !self.upload_sequences && Self::runtime_registers_host_buffers() {
            return Ok(SequenceInput::Registered(
                self.registered_sequence(sequence, sequence_bytes)?,
            ));
        }
        let _ = sequence_bytes;
        Ok(SequenceInput::Uploaded(
            self.client.create_from_slice(u32::as_bytes(sequence)),
        ))
    }

    fn freeze_with_alphabet(
        &self,
        alphabet_size: usize,
        sequence: &[u32],
        planes: &mut [&mut [u64]],
        plane_words: &[usize],
    ) -> Result<(), GpuFreezeError> {
        let geometry = FreezeGeometry::checked(alphabet_size, sequence.len(), plane_words)?;
        if let Some((position, &code)) = sequence
            .iter()
            .enumerate()
            .find(|(_, code)| **code as usize >= alphabet_size)
        {
            return Err(GpuFreezeError::CodeOutOfAlphabet {
                position,
                code,
                alphabet_size,
            });
        }
        if sequence.is_empty() {
            for plane in planes {
                plane.fill(0);
            }
            return Ok(());
        }

        let sequence_bytes = sequence
            .len()
            .checked_mul(std::mem::size_of::<u32>())
            .ok_or(GpuFreezeError::GeometryOverflow("sequence byte count"))?;
        let input = self.sequence_input(sequence, sequence_bytes)?;
        let mut current = input.handle();
        let mut other = self.client.empty(sequence_bytes);
        let layers = self.client.empty(geometry.packed_bytes);
        let block_bytes = geometry.num_blocks as usize * std::mem::size_of::<u32>();
        let cube_dim = CubeDim::new_1d(THREADS);
        let dispatch = cubecl::calculate_cube_count_elemwise(
            &self.client,
            geometry.num_blocks as usize,
            cube_dim,
        );

        for layer in 0..geometry.width {
            let shift = geometry.width - 1 - layer;
            let layer_word_offset = layer
                .checked_mul(geometry.words_per_layer)
                .ok_or(GpuFreezeError::GeometryOverflow("layer word offset"))?;
            let block_zeros = self.client.empty(block_bytes);
            let count_params = [geometry.len, shift, layer_word_offset, geometry.num_blocks];
            let count_params_handle = self.client.create_from_slice(u32::as_bytes(&count_params));
            unsafe {
                block_count_pack::launch_unchecked::<R>(
                    &self.client,
                    dispatch.clone(),
                    cube_dim,
                    ArrayArg::from_raw_parts(current.clone(), geometry.len as usize),
                    ArrayArg::from_raw_parts(layers.clone(), geometry.packed_words as usize),
                    ArrayArg::from_raw_parts(block_zeros.clone(), geometry.num_blocks as usize),
                    ArrayArg::from_raw_parts(count_params_handle, count_params.len()),
                    BLOCK_SIZE,
                )
            };

            // The final plane is already packed above. Stable partitioning is
            // only needed to order the sequence consumed by the next plane.
            if layer + 1 == geometry.width {
                continue;
            }

            let zero_offsets = self.client.empty(block_bytes);
            let total_zeros = self.client.empty(std::mem::size_of::<u32>());
            unsafe {
                scan_block_zeros::launch_unchecked::<R>(
                    &self.client,
                    CubeCount::Static(1, 1, 1),
                    CubeDim::new_1d(1),
                    ArrayArg::from_raw_parts(block_zeros, geometry.num_blocks as usize),
                    ArrayArg::from_raw_parts(zero_offsets.clone(), geometry.num_blocks as usize),
                    ArrayArg::from_raw_parts(total_zeros.clone(), 1),
                )
            };
            let scatter_params = [geometry.len, shift, 0u32, geometry.num_blocks];
            let scatter_params_handle = self
                .client
                .create_from_slice(u32::as_bytes(&scatter_params));
            unsafe {
                stable_scatter::launch_unchecked::<R>(
                    &self.client,
                    dispatch.clone(),
                    cube_dim,
                    ArrayArg::from_raw_parts(current.clone(), geometry.len as usize),
                    ArrayArg::from_raw_parts(other.clone(), geometry.len as usize),
                    ArrayArg::from_raw_parts(zero_offsets, geometry.num_blocks as usize),
                    ArrayArg::from_raw_parts(total_zeros, 1),
                    ArrayArg::from_raw_parts(scatter_params_handle, scatter_params.len()),
                    BLOCK_SIZE,
                )
            };
            if layer == 0 && input.is_registered() {
                // A registered buffer must never become a kernel destination:
                // it aliases host pages the registration contract keeps
                // immutable, and its handle is pinned `can_mut() == false`.
                // Swapping it into the ping-pong pair would make it the
                // layer-1 scatter output, so replace it with a fresh device
                // buffer first. The buffer count matches the upload path: two
                // transient device buffers per pass.
                current = self.client.empty(sequence_bytes);
            }
            std::mem::swap(&mut current, &mut other);
        }

        cubecl::future::block_on(self.client.sync())
            .map_err(|error| GpuFreezeError::Device(format!("{error:?}")))?;
        // The successful sync above drained every submission that read the
        // registered arena, so it may rest in the slot for the next pass to
        // rewrite. Error paths drop the entry instead: its registration and
        // keepalive stay with the device, so possibly still-queued kernels
        // keep reading valid pages, and the next pass registers a fresh
        // arena. (Failures also open `AcceleratedSuccinctRollup`'s circuit
        // breaker, so this path does not recur.)
        #[cfg(all(feature = "wgpu", target_os = "macos"))]
        if let SequenceInput::Registered(entry) = input {
            let mut slot = self.sequence_arena.lock().unwrap();
            if slot
                .as_ref()
                .is_none_or(|resident| resident.arena.bytes < entry.arena.bytes)
            {
                *slot = Some(entry);
            }
        }
        // The packed-planes readback cannot use a registered buffer: the
        // fork's only registration surface (`register_external_aliased`)
        // requires the aliased host region to stay immutable while the handle
        // lives, and permanently pins external handles `can_mut() == false`
        // precisely so they are never kernel destinations — `layers` is
        // written by every `block_count_pack` dispatch. No mutable or
        // import-for-write registration variant exists at the pinned rev, so
        // the explicit sync-then-read discipline below remains the output
        // path.
        let packed_bytes = self
            .client
            .read_one(layers)
            .map_err(|error| GpuFreezeError::Device(format!("{error:?}")))?;
        if packed_bytes.len() != geometry.packed_bytes {
            return Err(GpuFreezeError::ReadbackSize {
                expected: geometry.packed_bytes,
                actual: packed_bytes.len(),
            });
        }
        for (depth, output) in planes.iter_mut().enumerate() {
            output.fill(0);
            let base = depth
                .checked_mul(geometry.words_per_layer as usize)
                .ok_or(GpuFreezeError::GeometryOverflow("host layer word offset"))?;
            for (word, slot) in output.iter_mut().enumerate() {
                let low_index = base
                    .checked_add(word * 2)
                    .ok_or(GpuFreezeError::GeometryOverflow("host packed word index"))?;
                let high_in_layer = word * 2 + 1;
                let low = read_packed_u32(&packed_bytes, low_index)? as u64;
                let high = if high_in_layer < geometry.words_per_layer as usize {
                    read_packed_u32(&packed_bytes, base + high_in_layer)? as u64
                } else {
                    0
                };
                *slot = low | (high << 32);
            }
        }
        Ok(())
    }
}

impl<R: Runtime> WaveletMatrixFreezeBackend for CubeClWaveletFreeze<R> {
    type Error = GpuFreezeError;

    fn freeze_rotation(
        &self,
        _rotation: SuccinctRotation,
        alphabet_size: usize,
        sequence: &[u32],
        planes: &mut [&mut [u64]],
    ) -> Result<(), Self::Error> {
        let plane_words: Vec<_> = planes.iter().map(|plane| plane.len()).collect();
        self.freeze_with_alphabet(alphabet_size, sequence, planes, &plane_words)
    }
}

/// WGPU/Metal/Vulkan backend using CubeCL's selected device.
#[cfg(feature = "wgpu")]
pub type WgpuWaveletFreeze = CubeClWaveletFreeze<cubecl::wgpu::WgpuRuntime>;

/// CUDA backend using CubeCL's selected device.
#[cfg(feature = "cuda")]
pub type CudaWaveletFreeze = CubeClWaveletFreeze<cubecl::cuda::CudaRuntime>;
