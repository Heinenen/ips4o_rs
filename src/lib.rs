#![feature(is_sorted, let_chains, new_uninit, maybe_uninit_write_slice)]
use base_case::sort_simple_cases;
use constants::{BASE_CASE_MULTIPLIER, BASE_CASE_SIZE, BLOCK_SIZE, MIN_PARALLEL_BLOCKS_PER_THREAD};
use parallel::parallel_ips4o;
use rayon::current_num_threads;
use sequential::sequential_ips4o;
use std::{cmp::Ordering, fmt::Debug, mem::size_of};

mod base_case;
mod bucket_pointers;
mod classifier;
mod constants;
mod parallel;
mod permute_blocks;
mod sequential;
mod storage;
mod util;

pub(crate) trait Sortable: Clone + Debug + Default {}
impl<T: Clone + Debug + Default> Sortable for T {}

pub(crate) trait Less<T>: Fn(&T, &T) -> bool {}
impl<T, F: Fn(&T, &T) -> bool> Less<T> for F {}

pub(crate) trait PSortable: Sortable + Send + Sync + Copy {}
impl<T: Sortable + Send + Sync + Copy> PSortable for T {}

pub(crate) trait PLess<T>: Less<T> + Sync {}
impl<T, F: Less<T> + Sync> PLess<T> for F {}

#[inline]
pub fn sort<T>(v: &mut [T])
where
    T: Ord + Debug + Default + Clone,
{
    ips4o(v, T::lt);
    debug_assert!(v.is_sorted());
}

#[inline]
pub fn sort_by<T, F>(v: &mut [T], compare: F)
where
    T: Debug + Default + Clone,
    F: Fn(&T, &T) -> Ordering,
{
    ips4o(v, |a, b| compare(a, b) == Ordering::Less);
    debug_assert!(v.is_sorted_by(|a, b| Some(compare(a, b))));
}

#[inline]
pub fn sort_by_key<T, K, F>(v: &mut [T], f: F)
where
    T: Debug + Default + Clone,
    F: Fn(&T) -> K,
    K: Ord,
{
    ips4o(v, |a, b| f(a).lt(&f(b)));
    let is_less = |a, b| f(a).lt(&f(b));
    debug_assert!(v.is_sorted_by(is_less_to_compare!(is_less)));
}

#[inline]
pub fn sort_par<T>(v: &mut [T])
where
    T: Ord + Debug + Default + Clone + Copy + Send + Sync,
{
    ips4o_par(v, T::lt);
    debug_assert!(v.is_sorted());
}

fn ips4o<T, F>(v: &mut [T], is_less: F)
where
    T: Sortable,
    F: Less<T>,
{
    // Sorting has no meaningful behavior on zero-sized types. Do nothing.
    if size_of::<T>() == 0 {
        return;
    }
    if sort_simple_cases(v, &is_less) {
        return;
    }
    if v.len() <= BASE_CASE_MULTIPLIER * BASE_CASE_SIZE {
        base_case::base_case_sort(v, &is_less);
        return;
    }
    sequential_ips4o(v, &is_less);
}

#[inline]
#[allow(unused)]
fn ips4o_par<T, F>(v: &mut [T], mut is_less: F)
where
    T: PSortable,
    F: Fn(&T, &T) -> bool + Sync,
{
    // Sorting has no meaningful behavior on zero-sized types. Do nothing.
    if size_of::<T>() == 0 {
        return;
    }
    if sort_simple_cases(v, &is_less) {
        return;
    }
    if v.len() <= BASE_CASE_MULTIPLIER * BASE_CASE_SIZE {
        base_case::base_case_sort(v, &is_less);
        return;
    }
    // Sorting in parallel makes no sense with only one thread
    if current_num_threads() == 1 {
        ips4o(v, is_less);
        return;
    }
    if v.len() <= current_num_threads() * MIN_PARALLEL_BLOCKS_PER_THREAD * BLOCK_SIZE {
        sequential_ips4o(v, &is_less);
        return;
    }
    parallel_ips4o(v, &is_less);
}

#[cfg(test)]
mod tests {
    use std::{
        cmp::{max, min},
        fs, panic,
    };

    use rand::{distributions::Uniform, rngs::StdRng, seq::SliceRandom, Rng, SeedableRng};

    use crate::{debug, sort, sort_par, PSortable};

    const TEST_PARALLEL: bool = false;

    const FAILING_INPUT: &str = "./target/failing_input.json";

    fn sort_test<T>(input: &mut [T])
    where
        T: Ord + PSortable,
    {
        if TEST_PARALLEL {
            sort_par(input)
        } else {
            sort(input)
        }
    }

    fn sort_and_save_to_file_if_failed(mut input: Vec<u64>) {
        let clone = input.clone();
        let result = panic::catch_unwind(move || {
            sort_test(&mut input);
            input
        });
        match result {
            Ok(sorted_input) => {
                let mut sorted = clone.clone();
                sorted.sort();
                if sorted != sorted_input {
                    let data =
                        serde_json::to_string(&clone).expect("unable to serialize failing slice");
                    fs::write(FAILING_INPUT, data).expect("unable to write failing slice to file");
                    panic!("result is not a sorted permutation of its input")
                }
            }
            Err(_e) => {
                let data =
                    serde_json::to_string(&clone).expect("unable to serialize failing slice");
                fs::write(FAILING_INPUT, data).expect("unable to write failing slice to file");
                panic!()
            }
        }
    }

    #[test]
    fn simple_test1() {
        let mut input = some_vec();
        input.append(&mut some_vec());
        input.append(&mut some_vec());
        input.append(&mut some_vec());
        input.append(&mut some_vec());
        debug!(input);
        sort_test(&mut input);
        debug!(input);
    }

    #[test]
    fn simple_test2() {
        let mut input = [
            1, 9, 26, 29, 1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
            22, 23, 24, 25, 27, 28,
        ];
        debug!(input);
        sort_test(&mut input);
        debug!(input);
        assert!(input.is_sorted());
    }

    #[test]
    fn simple_test3() {
        let mut input = [4, 4, 4, 4, 4, 4, 1, 2];
        debug!(input);
        sort_test(&mut input);
        debug!(input);
        assert!(input.is_sorted());
    }

    #[test]
    fn simple_test4() {
        let mut input = some_vec();
        input.append(&mut some_vec());
        debug!(input);
        sort_test(&mut input);
        debug!(input);
        assert!(input.is_sorted());
    }

    #[test]
    fn fuzz() {
        // let mut rng = rand::thread_rng();
        let mut rng = StdRng::seed_from_u64(0);
        for _ in 0..5000 {
            let len: usize = rng.gen_range(0..10000);
            let (a, b) = (
                rng.gen_range(u64::MIN..u64::MAX),
                rng.gen_range(u64::MIN..u64::MAX),
            );
            let (lower, upper) = (min(a, b), max(a, b));
            let input: Vec<_> = (0..len).map(|_| rng.gen_range(lower..upper)).collect();
            sort_and_save_to_file_if_failed(input);
        }
    }

    #[ignore = "only used to reproduce failing test"]
    #[test]
    fn test_json_input() {
        let input = fs::read_to_string(FAILING_INPUT).expect("no file found at given path");
        let mut input: Vec<u64> = serde_json::from_str(&input).unwrap();
        let mut sorted = input.clone();
        sorted.sort();
        sort_test(&mut input);
        assert!(input.is_sorted());
        assert!(input == sorted);
    }

    #[test]
    fn test_block_aligned_buckets() {
        // blocks of size 8
        let mut v = vec![];
        for i in 0..200 {
            v.append(&mut vec![i, i, i, i, i, i, i, i]);
        }

        let mut rng = StdRng::seed_from_u64(0);
        for _ in 0..1 {
            v.shuffle(&mut rng);
            sort_test(&mut v);
            assert!(v.is_sorted());
        }
    }

    #[test]
    fn big_test() {
        let len = 1usize << 20;
        let mut rng = StdRng::seed_from_u64(0);
        let range = Uniform::from(0..10_000);
        let mut v: Vec<u32> = (0..len).map(|_| rng.sample(&range)).collect();
        let mut sorted = v.clone();
        sorted.sort();
        sort_test(&mut v);
        assert!(v.is_sorted());
        assert!(v == sorted);
    }

    fn some_vec() -> Vec<i32> {
        vec![5, 5, 35, 7, 4, 4, 4, 7, 67, 7, 7, 6] //           3*4 +  2*5 + 1*6 +  4*7 + 1*35 + 1*67
                                                   // times 2:  6*4 +  4*5 + 2*6 +  8*7 + 2*35 + 2*67
                                                   // times 3:  9*4 +  6*5 + 3*6 + 12*7 + 3*35 + 3*67
                                                   // times 4: 12*4 +  8*5 + 4*6 + 16*7 + 4*35 + 4*67
                                                   // times 5: 15*4 + 10*5 + 5*6 + 20*7 + 5*35 + 5*67
    }
}
