#![doc = include_str!("../README.md")]

use std::fmt;

#[cfg(any(feature = "wgpu", feature = "cuda"))]
mod cubecl_backend;
#[cfg(all(test, feature = "wgpu"))]
mod resident_ordered_oracle;
#[cfg(feature = "wgpu")]
mod resident_program;
#[cfg(feature = "wgpu")]
mod resident_proposals;
#[cfg(feature = "wgpu")]
mod resident_round;
#[cfg(feature = "wgpu")]
mod resident_support;
#[cfg(feature = "wgpu")]
mod succinct_query;

#[cfg(any(feature = "wgpu", feature = "cuda"))]
pub use cubecl_backend::CubeClWaveletFreeze;
#[cfg(feature = "cuda")]
pub use cubecl_backend::CudaWaveletFreeze;
#[cfg(feature = "wgpu")]
pub use cubecl_backend::WgpuWaveletFreeze;
#[cfg(feature = "wgpu")]
pub use resident_program::{ResidentTransitionError, WgpuQueryProgram};
#[cfg(feature = "wgpu")]
pub use resident_round::{
    ResidentRoundArm, ResidentRoundError, ResidentRoundInputs, ResidentRoundMetadata,
    ResidentRowChoice, ResidentRowChoices, ResidentRowPlanner, WgpuResidentRowPlanner,
    DEAD_ROW_SENTINEL,
};
#[cfg(feature = "wgpu")]
pub use resident_support::{ResidentSupportError, WgpuResidentFrontier, WgpuResidentRound};
#[cfg(feature = "wgpu")]
pub use succinct_query::{
    WgpuBitVector, WgpuContext, WgpuQueryStats, WgpuSuccinctArchive, WgpuWaveletMatrix,
    DEFAULT_MIN_RANK_BATCH,
};

#[cfg(any(test, feature = "wgpu", feature = "cuda"))]
const BLOCK_SIZE: u32 = 256;
#[cfg(any(feature = "wgpu", feature = "cuda"))]
const THREADS: u32 = 64;

/// Failure to freeze a wavelet rotation on an accelerator.
#[derive(Debug, Eq, PartialEq)]
pub enum GpuFreezeError {
    /// The backend's `u32` codes cannot represent this alphabet cardinality.
    AlphabetTooWide(usize),
    /// The current kernels use `u32` positions.
    SequenceTooLong(usize),
    /// Core and backend disagreed about the number of packed output layers.
    OutputLayers {
        /// Number of layers implied by the alphabet.
        expected: usize,
        /// Number of layers supplied by core.
        actual: usize,
    },
    /// Core and backend disagreed about one packed output plane's length.
    OutputPlaneWords {
        /// Zero-based wavelet depth.
        depth: usize,
        /// Required number of `u64` words.
        expected: usize,
        /// Supplied number of `u64` words.
        actual: usize,
    },
    /// A sequence code lies outside the declared alphabet.
    CodeOutOfAlphabet {
        /// Position of the invalid code.
        position: usize,
        /// Invalid code.
        code: u32,
        /// Declared alphabet cardinality.
        alphabet_size: usize,
    },
    /// Checked host-side size or offset arithmetic overflowed a device lane.
    GeometryOverflow(&'static str),
    /// A device command or explicit synchronization failed.
    Device(String),
    /// The device returned a buffer with an unexpected byte count.
    ReadbackSize {
        /// Number of bytes allocated for the readback.
        expected: usize,
        /// Number of bytes returned by the runtime.
        actual: usize,
    },
}

impl fmt::Display for GpuFreezeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlphabetTooWide(size) => {
                write!(f, "alphabet of size {size} exceeds u32 backend codes")
            }
            Self::SequenceTooLong(len) => write!(f, "sequence length {len} exceeds u32 positions"),
            Self::OutputLayers { expected, actual } => write!(
                f,
                "core reserved {actual} wavelet layers, backend expected {expected}"
            ),
            Self::OutputPlaneWords {
                depth,
                expected,
                actual,
            } => write!(
                f,
                "core reserved {actual} words for wavelet layer {depth}, backend expected {expected}"
            ),
            Self::CodeOutOfAlphabet {
                position,
                code,
                alphabet_size,
            } => write!(
                f,
                "sequence code {code} at position {position} lies outside alphabet of size {alphabet_size}"
            ),
            Self::GeometryOverflow(quantity) => {
                write!(f, "GPU wavelet geometry overflows {quantity}")
            }
            Self::Device(error) => write!(f, "accelerator operation failed: {error}"),
            Self::ReadbackSize { expected, actual } => write!(
                f,
                "accelerator returned {actual} bytes, expected {expected}"
            ),
        }
    }
}

impl std::error::Error for GpuFreezeError {}

#[cfg(any(test, feature = "wgpu", feature = "cuda"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FreezeGeometry {
    len: u32,
    width: u32,
    words_per_layer: u32,
    packed_words: u32,
    packed_bytes: usize,
    num_blocks: u32,
}

#[cfg(any(test, feature = "wgpu", feature = "cuda"))]
impl FreezeGeometry {
    fn checked(
        alphabet_size: usize,
        sequence_len: usize,
        plane_words: &[usize],
    ) -> Result<Self, GpuFreezeError> {
        if alphabet_size > u32::MAX as usize {
            return Err(GpuFreezeError::AlphabetTooWide(alphabet_size));
        }
        let len = u32::try_from(sequence_len)
            .map_err(|_| GpuFreezeError::SequenceTooLong(sequence_len))?;
        let width_usize = needed_bits(alphabet_size);
        if plane_words.len() != width_usize {
            return Err(GpuFreezeError::OutputLayers {
                expected: width_usize,
                actual: plane_words.len(),
            });
        }
        let expected_plane_words = sequence_len.div_ceil(64);
        for (depth, &actual) in plane_words.iter().enumerate() {
            if actual != expected_plane_words {
                return Err(GpuFreezeError::OutputPlaneWords {
                    depth,
                    expected: expected_plane_words,
                    actual,
                });
            }
        }

        let width = u32::try_from(width_usize)
            .map_err(|_| GpuFreezeError::GeometryOverflow("wavelet width"))?;
        let words_per_layer = len.div_ceil(32);
        let packed_words = width
            .checked_mul(words_per_layer)
            .ok_or(GpuFreezeError::GeometryOverflow("packed u32 word count"))?;
        let packed_bytes = usize::try_from(packed_words)
            .ok()
            .and_then(|words| words.checked_mul(std::mem::size_of::<u32>()))
            .ok_or(GpuFreezeError::GeometryOverflow("packed byte count"))?;
        sequence_len
            .checked_mul(std::mem::size_of::<u32>())
            .ok_or(GpuFreezeError::GeometryOverflow("sequence byte count"))?;

        let num_blocks = len.div_ceil(BLOCK_SIZE);
        usize::try_from(num_blocks)
            .ok()
            .and_then(|blocks| blocks.checked_mul(std::mem::size_of::<u32>()))
            .ok_or(GpuFreezeError::GeometryOverflow("block-prefix byte count"))?;
        Ok(Self {
            len,
            width,
            words_per_layer,
            packed_words,
            packed_bytes,
            num_blocks,
        })
    }
}

#[cfg(any(test, feature = "wgpu", feature = "cuda"))]
fn needed_bits(value: usize) -> usize {
    (usize::BITS - value.leading_zeros()).max(1) as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "wgpu")]
    #[test]
    fn wgpu_rollup_is_send_and_sync_for_repository_hooks() {
        use triblespace_core::repo::index_home::AcceleratedSuccinctRollup;

        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<AcceleratedSuccinctRollup<WgpuWaveletFreeze>>();
    }

    fn geometry(alphabet_size: usize, len: usize) -> Result<FreezeGeometry, GpuFreezeError> {
        let planes = vec![len.div_ceil(64); needed_bits(alphabet_size)];
        FreezeGeometry::checked(alphabet_size, len, &planes)
    }

    #[test]
    fn packing_boundaries_are_exact() {
        for len in [31usize, 32, 33, 63, 64, 65, 255, 256, 257] {
            let geometry = geometry(257, len).unwrap();
            assert_eq!(geometry.width, 9);
            assert_eq!(geometry.words_per_layer as usize, len.div_ceil(32));
            assert_eq!(geometry.packed_bytes, 9 * len.div_ceil(32) * 4);
            assert_eq!(geometry.num_blocks as usize, len.div_ceil(256));
        }
    }

    #[test]
    fn alphabet_width_boundaries_are_exact() {
        for (alphabet_size, expected) in [(255, 8), (256, 9), (257, 9)] {
            assert_eq!(geometry(alphabet_size, 65).unwrap().width, expected);
        }
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn rejects_alphabet_and_sequence_outside_u32_contract() {
        assert_eq!(
            geometry(u32::MAX as usize + 1, 1),
            Err(GpuFreezeError::AlphabetTooWide(u32::MAX as usize + 1))
        );
        let too_long = u32::MAX as usize + 1;
        let planes = vec![too_long.div_ceil(64)];
        assert_eq!(
            FreezeGeometry::checked(1, too_long, &planes),
            Err(GpuFreezeError::SequenceTooLong(too_long))
        );
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn rejects_packed_offsets_that_do_not_fit_u32() {
        let len = u32::MAX as usize;
        let planes = vec![len.div_ceil(64); needed_bits(u32::MAX as usize)];
        assert_eq!(
            FreezeGeometry::checked(u32::MAX as usize, len, &planes),
            Err(GpuFreezeError::GeometryOverflow("packed u32 word count"))
        );
    }

    #[test]
    fn rejects_malformed_output_shape() {
        assert_eq!(
            FreezeGeometry::checked(256, 65, &[2; 8]),
            Err(GpuFreezeError::OutputLayers {
                expected: 9,
                actual: 8,
            })
        );
        let mut planes = [2; 9];
        planes[4] = 1;
        assert_eq!(
            FreezeGeometry::checked(256, 65, &planes),
            Err(GpuFreezeError::OutputPlaneWords {
                depth: 4,
                expected: 2,
                actual: 1,
            })
        );
    }
}
