mod distributions;

use std::time::Duration;

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use distributions::{DISTRIBUTIONS, NAMES};

const ARRAY_LEN: usize = 2;
pub const ALGOS: [&dyn Fn(&mut [u32]); ARRAY_LEN] = [&ips4o_rs::sort, &ips4o_rs::sort_par];
pub const ALGO_NAMES: [&'static str; ARRAY_LEN] = ["ips4o_rs_seq", "ips4o_rs_par"];

fn my_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("bench");
    for (algo, algo_name) in ALGOS.iter().zip(ALGO_NAMES) {
        for (d, d_name) in DISTRIBUTIONS.iter().zip(NAMES) {
            for exp in 5..=24 {
                let len = 1usize << exp;
                group.bench_function(
                    BenchmarkId::new(algo_name, format!("{}/2^{}/{}", d_name, exp, len)),
                    |b| {
                        b.iter_batched_ref(
                            || -> Vec<u32> { d(len) },
                            |v| algo(v),
                            BatchSize::SmallInput,
                        )
                    },
                );
            }
        }
    }
}

criterion_group!(
    name = benches;
    config = Criterion::default().warm_up_time(Duration::from_secs(1)).measurement_time(Duration::from_nanos(1)).sample_size(10);
    targets = my_bench,
);
criterion_main!(benches);
