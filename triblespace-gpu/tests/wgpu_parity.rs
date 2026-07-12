use triblespace_core::blob::encodings::succinctarchive::{
    merge_ordered_archives, merge_ordered_archives_with_backend, OrderedUniverse, SuccinctArchive,
};
use triblespace_core::trible::{Trible, TribleSet};
use triblespace_gpu::WgpuWaveletFreeze;

fn trible(seed: u64, ordinal: u64) -> Trible {
    let mut state = seed.wrapping_add(ordinal.wrapping_mul(0x9e37_79b9_7f4a_7c15));
    let mut data = [0u8; 64];
    for chunk in data.chunks_exact_mut(8) {
        state ^= state >> 30;
        state = state.wrapping_mul(0xbf58_476d_1ce4_e5b9);
        state ^= state >> 27;
        state = state.wrapping_mul(0x94d0_49bb_1331_11eb);
        state ^= state >> 31;
        chunk.copy_from_slice(&state.to_le_bytes());
    }
    data[0] |= 0x80;
    data[16] |= 0x80;
    // Keep the surrounding bytes irregular while making distinct ordinals
    // unconditionally distinct rather than relying on the mixer above.
    data[56..].copy_from_slice(&ordinal.to_be_bytes());
    Trible { data }
}

#[test]
#[ignore = "requires a native WGPU adapter"]
fn wgpu_merge_is_byte_identical_to_canonical_cpu_merge() {
    for output_rows in [0u64, 1, 31, 32, 33, 63, 64, 65, 255, 256, 257] {
        let mut left = TribleSet::new();
        let mut right = TribleSet::new();

        for ordinal in 0..output_rows {
            let trible = trible(0xA11C_E000, ordinal);
            if ordinal % 2 == 0 {
                left.insert(&trible);
            } else {
                right.insert(&trible);
            }
            if ordinal % 17 == 0 {
                left.insert(&trible);
                right.insert(&trible);
            }
        }

        let mut expected = left.clone();
        expected.union(right.clone());
        assert_eq!(expected.len(), output_rows as usize);

        let archives: Vec<SuccinctArchive<OrderedUniverse>> =
            [&left, &right].into_iter().map(Into::into).collect();
        let cpu = merge_ordered_archives(&archives);
        let backend = WgpuWaveletFreeze::new(&Default::default());
        let gpu = merge_ordered_archives_with_backend(&archives, &backend).unwrap();

        assert_eq!(
            gpu.bytes.as_ref(),
            cpu.bytes.as_ref(),
            "{output_rows} output rows"
        );
        assert_eq!(TribleSet::from(&gpu), TribleSet::from(&cpu));
    }
}
