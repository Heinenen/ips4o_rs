mod distributions;

use std::{mem::MaybeUninit, time::Duration};

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use distributions::{sorted, DISTRIBUTIONS, NAMES};
use ips4o_rs::crum_analyze::glidesort_merge::{double_merge, par_double_merge};
use rayon::current_num_threads;

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
    let len = 1_0000_000 / 2;
    // total_len = 10^9;
    // 1 Threads: 3.5s
    // 2 Threads: 2.1s
    // 3 Threads: 1.9s
    // 4 Threads: 1.9s
    // 5 Threads:
    // 6 Threads: 1.9s
    // 8 Threads:
    // 10Threads:
    // 12Threads: 2.06s

    //

    group.bench_function("par_merge", |b| {
        b.iter_batched_ref(
            || -> (Vec<u32>, [Box<[MaybeUninit<u32>; 131072]>; 16]) {
                let scratches: [Box<[MaybeUninit<u32>; 131072]>; 16] = core::array::from_fn(|_| {
                    Box::new(unsafe { MaybeUninit::uninit().assume_init() })
                });
                let mut v = sorted(len);
                v.append(&mut sorted(len));
                (v, scratches)
            },
            |(v, scratches)| {
                let mut scratches = scratches
                    .iter_mut()
                    .map(|it| it.as_mut_slice())
                    .collect::<Vec<_>>();
                let num_threads = current_num_threads();
                par_double_merge(v, len, &mut scratches[..num_threads], &u32::lt);
            },
            BatchSize::LargeInput,
        )
    });
    group.bench_function("seq_merge", |b| {
        b.iter_batched_ref(
            || -> (Vec<u32>, [Box<[MaybeUninit<u32>; 131072]>; 16]) {
                let scratches: [Box<[MaybeUninit<u32>; 131072]>; 16] = core::array::from_fn(|_| {
                    Box::new(unsafe { MaybeUninit::uninit().assume_init() })
                });
                let mut v = sorted(len);
                v.append(&mut sorted(len));
                (v, scratches)
            },
            |(v, scratches)| {
                let mut scratches = scratches
                    .iter_mut()
                    .map(|it| it.as_mut_slice())
                    .collect::<Vec<_>>();
                double_merge(v, len, &mut scratches[0], &u32::lt);
            },
            BatchSize::LargeInput,
        )
    });
}

criterion_group!(
    name = benches;
    config = Criterion::default().warm_up_time(Duration::from_secs(1)).measurement_time(Duration::from_nanos(1)).sample_size(10);
    // config = Criterion::default();
    targets = my_bench,
);
criterion_main!(benches);
