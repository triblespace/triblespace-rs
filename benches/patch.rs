use fake::faker::lorem::en::Sentence;
use fake::Fake;
use rand::seq::SliceRandom;
use rand::thread_rng;
use triblespace::core::patch::bytetable::init as table_init;
use triblespace::core::patch::bytetable::ByteEntry;
use triblespace::core::patch::bytetable::ByteTable;
use triblespace::core::patch::Entry;
use triblespace::core::patch::IdentitySchema;
use triblespace::core::patch::PATCH;

fn patch_fill_benchmark() {
    let mut patch = PATCH::<64, IdentitySchema>::new();

    for _ in 0..2_000_000 {
        let text: String = Sentence(3..8).fake();
        let mut key = [0u8; 64];
        let bytes = text.as_bytes();
        let len = bytes.len().min(64);
        key[..len].copy_from_slice(&bytes[..len]);
        let entry = Entry::new(&key);
        patch.insert(&entry);
    }

    let avg = patch.debug_branch_fill();
    println!("Average fill: {:?}", avg);
}

fn byte_table_resize_benchmark() {
    table_init();

    #[derive(Clone, Debug)]
    struct Dummy(u8);

    unsafe impl ByteEntry for Dummy {
        fn key(&self) -> u8 {
            self.0
        }
    }

    fn average_fill(random: bool, runs: usize) -> (f32, Vec<(usize, f32)>) {
        let mut order: Vec<u8> = (0..=255).collect();
        // Accumulate the element count before growth for each table size.
        // We know the table tops out at 256 slots (2^8), so pre-allocate
        // a slot for each possible power-of-two size to avoid resizing.
        let mut inserted_totals: Vec<usize> = vec![0; 8];

        for _ in 0..runs {
            if random {
                order.shuffle(&mut thread_rng());
            }

            let mut table: Box<[Option<Dummy>]> = vec![None; 2].into_boxed_slice();
            let mut size = 2usize;
            let mut inserted = 0usize;

            for key in order.iter().copied() {
                let mut entry = Dummy(key);
                loop {
                    match table.table_insert(entry) {
                        None => {
                            inserted += 1;
                            break;
                        }
                        Some(displaced) => {
                            let index = usize::ilog2(size) as usize - 1;
                            inserted_totals[index] += inserted;

                            size *= 2;
                            let mut grown: Box<[Option<Dummy>]> =
                                vec![None; size].into_boxed_slice();
                            table.table_grow(&mut grown);
                            table = grown;
                            entry = displaced;
                        }
                    }
                }
            }

            // Record the fill after the final insertions without a subsequent resize.
            let index = usize::ilog2(size) as usize - 1;
            inserted_totals[index] += inserted;
        }

        let mut by_size = Vec::new();
        let mut total = 0.0f32;
        for (index, inserted_total) in inserted_totals.into_iter().enumerate() {
            let size = 1usize << (index + 1);
            let avg_inserted = inserted_total as f32 / runs as f32;
            let ratio = avg_inserted / size as f32;
            by_size.push((size, ratio));
            total += ratio;
        }

        let avg = if !by_size.is_empty() {
            total / by_size.len() as f32
        } else {
            0.0
        };

        (avg, by_size)
    }

    const RUNS: usize = 100;
    let (avg_random, by_size_random) = average_fill(true, RUNS);
    let (avg_seq, by_size_seq) = average_fill(false, RUNS);

    println!(
        "ByteTable resize fill - random: {:.3}, sequential: {:.3}",
        avg_random, avg_seq
    );

    println!("Per-size fill (random)");
    for (size, ratio) in by_size_random {
        println!("  size {:>3}: {:.3}", size, ratio);
    }

    println!("Per-size fill (sequential)");
    for (size, ratio) in by_size_seq {
        println!("  size {:>3}: {:.3}", size, ratio);
    }
}

fn main() {
    patch_fill_benchmark();
    byte_table_resize_benchmark();
}
