pub mod glidesort_merge;
mod partition_friendly;
mod quadsort;

use rayon::{current_num_threads, prelude::*};
use std::{cmp::max, mem::MaybeUninit, ops::Range};

use crate::{
    base_case::sort_simple_cases,
    crum_analyze::{
        glidesort_merge::{double_merge, par_double_merge, quad_merge, triple_merge},
        partition_friendly::std_partitioning,
    },
    is_less_to_compare, is_less_to_compare2,
    parallel::parallel,
    sequential::sequential,
    storage::{GlobalStorage, LocalStorage},
    Less, PLess, PSortable, Sortable,
};

// we assert that the number of chunks is very small compared to the length of v
// must be a power of 2
const CHUNKS: usize = 8;
const STREAK_LEN: usize = 32;

pub(crate) fn analyze<'a, T, F>(v: &mut [T], scratch: Option<&mut LocalStorage<'a, T, F>>, is_less: &'a F)
where
    T: Sortable,
    F: Less<T>,
{
    assert!(CHUNKS.is_power_of_two());

    let len = v.len();
    let chunk_len = len / CHUNKS;
    let chunk_bounds: [usize; CHUNKS] = core::array::from_fn(|i| i * chunk_len);

    // thresholds that help decide when to switch to which algorithm:
    // switch to merge if 75% of streaks are sorted (or reverse sorted)
    let merge_friendly_threshold = len * 3 / 4 / (CHUNKS * STREAK_LEN);
    // switch to partition based sort if 66% of elements are in order or
    // if 66% are in reverse order
    let partition_threshold = len / CHUNKS / 3;

    let mut idx: [usize; CHUNKS] = core::array::from_fn(|i| i * chunk_len);
    let mut balance = [0; CHUNKS];
    let mut streaks = [0; CHUNKS];
    let mut i = len;

    while i > CHUNKS * (STREAK_LEN + 1) {
        // TODO why `+ 1`
        let mut sums = [0; CHUNKS];
        for fract in 0..CHUNKS {
            for _ in 0..STREAK_LEN {
                sums[fract] += is_less(&v[idx[fract] + 1], &v[idx[fract]]) as usize;
                idx[fract] += 1;
            }
            balance[fract] += sums[fract];
            let streak_found = (sums[fract] == 0 || sums[fract] == 32) as usize;
            streaks[fract] += streak_found;
        }
        i -= CHUNKS * STREAK_LEN;
    }

    while i >= 2 * CHUNKS {
        for fract in 0..CHUNKS {
            balance[fract] += is_less(&v[idx[fract] + 1], &v[idx[fract]]) as usize;
            idx[fract] += 1;
        }
        i -= CHUNKS;
    }

    // checks if remainder is sorted and also the boundary
    // between the last chunk and the remainder
    let mut remainder_is_sorted = true;
    for i in CHUNKS * chunk_len - 1..len - 1 {
        if is_less(&v[i + 1], &v[i]) {
            remainder_is_sorted = false;
            break;
        }
    }
    // checks if boundaries between chunks are sorted
    let mut boundaries_are_sorted = true;
    for i in 1..CHUNKS {
        let boundary = i * chunk_len;
        if is_less(&v[boundary], &v[boundary - 1]) {
            boundaries_are_sorted = false;
            break;
        }
    }

    let mut sorted: [bool; CHUNKS] = balance.map(|b| b == 0);
    sorted[CHUNKS - 1] = sorted[CHUNKS - 1] && remainder_is_sorted;
    let is_sorted = sorted.iter().all(|b| *b) && remainder_is_sorted && boundaries_are_sorted;
    if is_sorted {
        // no work has to be done if input is already sorted
        return;
    }

    let reversed: [bool; CHUNKS] = balance
        .zip(sorted)
        .map(|(bal, so)| !so && chunk_len - bal == 1);
    let merge_friendly = streaks
        .zip(sorted)
        .map(|(st, so)| !so && st > merge_friendly_threshold);
    let partition_friendly =
        balance
            .zip(merge_friendly)
            .zip(sorted)
            .map(|((bal, rmf), so)| {
                !so && !rmf && (bal < partition_threshold || bal >= chunk_len - partition_threshold)
            });
    let unsorted = partition_friendly
        .zip(sorted)
        .zip(merge_friendly)
        .map(|((so, pf), mf)| !(so || pf || mf));

    
    let mut new_ls = LocalStorage::new(is_less);
    let ls = if let Some(ls) = scratch {
        ls
    } else {
        &mut new_ls
    };
    let unsorted_algorithm =
        |v: &mut [T], ls: &mut LocalStorage<T, F>, is_less: &F| sequential(v, ls, is_less);

    let nearly_sorted_algorithm = |v: &mut [T], buffer: &mut [MaybeUninit<T>], is_less: &F| {
        // let mut swap = [T::default(); 512];
        // quadsort::quadsort_swap(v, &mut swap, is_less);
        // sequential_ips4o(v, is_less);
        // v.sort_unstable();
        glidesort::sort_with_buffer_by(v, buffer, is_less_to_compare2!(is_less));
    };

    let mut partition_friendly_algorithm = |v: &mut [T], is_less: &F| {
        // core::slice::sort::quicksort(v, is_less);

        // partition into two partitions (and maybe a third partition in
        // the case of many equal elements) only once, continue with ips4o
        let (left, right) = std_partitioning(v, is_less);
        unsorted_algorithm(left, ls, is_less);
        unsorted_algorithm(right, ls, is_less);
    };

    let mut quads: [usize; CHUNKS + 1] = core::array::from_fn(|i| i * chunk_len);
    quads[CHUNKS] = len;

    for (o, s) in find_streaks(&reversed, &quads).zip(sorted.each_mut()) {
        if let Some(r) = o {
            v[r].reverse();
            *s = true;
        }
    }
    for range in find_streaks(&partition_friendly, &quads)
        .into_iter()
        .flatten()
    {
        partition_friendly_algorithm(&mut v[range], is_less);
    }
    for range in find_streaks(&merge_friendly, &quads).into_iter().flatten() {
        nearly_sorted_algorithm(&mut v[range], ls.bucket_buffers.get_raw(), is_less);
    }
    for range in find_streaks(&unsorted, &quads).into_iter().flatten() {
        unsorted_algorithm(&mut v[range], ls, is_less);
    }
    // All chunks should be sorted (including remainder)
    debug_assert!(v[chunk_bounds[CHUNKS - 1]..len].is_sorted_by(is_less_to_compare!(is_less)));
    for i in 0..CHUNKS - 1 {
        debug_assert!(
            v[chunk_bounds[i]..chunk_bounds[i + 1]].is_sorted_by(is_less_to_compare!(is_less))
        )
    }

    let scratch = ls.bucket_buffers.get_raw();
    let (mut runs_array, mut number_of_runs) = find_runs::<_, _, CHUNKS>(v, chunk_len, is_less);
    
    let mut runs = &mut runs_array[..];
    while number_of_runs >= 4 {
        let lengths = [
            runs[0].take().unwrap().len(),
            runs[1].take().unwrap().len(),
            runs[2].take().unwrap().len(),
            runs[3].take().unwrap().len(),
        ];
        let new_len = lengths.iter().sum();
        quad_merge(
            &mut v[..new_len],
            lengths[0],
            lengths[1],
            lengths[2],
            scratch,
            is_less,
        );
        runs = &mut runs[3..];
        runs[0] = Some(0..new_len);
        number_of_runs -= 3;
    }
    if number_of_runs == 3 {
        let lengths = [
            runs[0].take().unwrap().len(),
            runs[1].take().unwrap().len(),
            runs[2].take().unwrap().len(),
        ];
        let new_len = lengths.iter().sum();
        triple_merge(&mut v[..new_len], lengths[0], lengths[1], scratch, is_less);
        runs = &mut runs[2..];
        runs[0] = Some(0..new_len);
        number_of_runs -= 2;
    }
    if number_of_runs == 2 {
        let lengths = [runs[0].take().unwrap().len(), runs[1].take().unwrap().len()];
        let new_len = lengths.iter().sum();
        double_merge(&mut v[..new_len], lengths[0], scratch, is_less);
        runs = &mut runs[1..];
        runs[0] = Some(0..new_len);
        number_of_runs -= 1;
    }
    debug_assert_eq!(number_of_runs, 1);
    debug_assert_eq!(runs[0].clone().unwrap().len(), len);
    debug_assert!(runs[1..].iter().all(|opt| opt.is_none()));

    debug_assert!(v.is_sorted_by(is_less_to_compare!(is_less)));
}

pub(crate) fn parallel_analyze<T, F>(v: &mut [T], is_less: &F)
where
    T: PSortable,
    F: PLess<T>,
{
    let num_threads = current_num_threads();

    let stripe_len = max(1, (v.len() + num_threads - 1) / num_threads);
    let mut is_sorted = vec![false; num_threads];
    v.par_chunks_mut(stripe_len)
        .zip_eq(is_sorted.par_iter_mut())
        .for_each(|(c, s)| {
            *s = sort_simple_cases(c, is_less);
        });

    let mut sorted_streak = 0;
    for s in is_sorted {
        if s && (sorted_streak == 0 || !is_less(&v[sorted_streak], &v[sorted_streak - 1])) {
            sorted_streak += stripe_len;
        } else {
            break;
        }
    }

    sorted_streak = sorted_streak.min(v.len());
    let (sorted, unsorted) = v.split_at_mut(sorted_streak);

    let mut lss = Vec::new();
    lss.resize_with(num_threads, || LocalStorage::new(is_less));

    let mut gs: GlobalStorage<T, F> = GlobalStorage::new(is_less);

    parallel(unsorted, lss.as_mut_slice(), &mut gs, is_less);
    debug_assert!(sorted.is_sorted_by(is_less_to_compare!(is_less)));
    debug_assert!(unsorted.is_sorted_by(is_less_to_compare!(is_less)));

    let mut buffers = lss
        .iter_mut()
        .map(|ls| &mut ls.bucket_buffers.get_raw()[..])
        .collect::<Vec<_>>();

    par_double_merge(v, sorted_streak, &mut buffers[..], is_less);
}

fn find_streaks(
    properties: &[bool; CHUNKS],
    quads: &[usize; CHUNKS + 1],
) -> [Option<Range<usize>>; CHUNKS] {
    let mut streaks: [Option<Range<usize>>; CHUNKS] = core::array::from_fn(|_| None);
    let mut contiguous_predecessor = 0;
    if properties[0] {
        streaks[0] = Some(quads[0]..quads[1]);
    }
    for i in 1..CHUNKS {
        if properties[i] {
            if properties[i - 1] {
                streaks[contiguous_predecessor].as_mut().unwrap().end = quads[i + 1];
            } else {
                streaks[i] = Some(quads[i]..quads[i + 1]);
                contiguous_predecessor = i;
            }
        }
    }
    streaks
}

fn find_runs<T: Sortable, F: Less<T>, const C: usize>(
    v: &mut [T],
    chunk_len: usize,
    is_less: &F,
) -> ([Option<Range<usize>>; C], usize) {
    let mut runs: [Option<Range<usize>>; C] = core::array::from_fn(|_| None);
    let mut start = 0;
    let mut run_idx = 0;
    for i in 1..C {
        let fits_next = !is_less(&v[i * chunk_len], &v[i * chunk_len - 1]);
        // if fits_next {
        //     end = (i + 1) * chunk_len;
        // }
        if !fits_next {
            let end = i * chunk_len;
            runs[run_idx] = Some(start..end);
            start = end;
            run_idx += 1;
        }
    }
    runs[run_idx] = Some(start..v.len());
    (runs, run_idx + 1)
}

// only compiles if CHUNKS == 4
// #[test]
// fn test_find_streaks_4() {
//     let quads = [0, 1, 2, 3, 4];
//     let properties0 = [false, false, false, false];
//     let properties1 = [false, false, false, true];
//     let properties2 = [false, false, true, false];
//     let properties3 = [false, false, true, true];
//     let properties4 = [false, true, false, false];
//     let properties5 = [false, true, false, true];
//     let properties6 = [false, true, true, false];
//     let properties7 = [false, true, true, true];
//     let properties8 = [true, false, false, false];
//     let properties9 = [true, false, false, true];
//     let properties10 = [true, false, true, false];
//     let properties11 = [true, false, true, true];
//     let properties12 = [true, true, false, false];
//     let properties13 = [true, true, false, true];
//     let properties14 = [true, true, true, false];
//     let properties15 = [true, true, true, true];

//     let res0 = [None, None, None, None];
//     let res1 = [None, None, None, Some(3..4)];
//     let res2 = [None, None, Some(2..3), None];
//     let res3 = [None, None, Some(2..4), None];
//     let res4 = [None, Some(1..2), None, None];
//     let res5 = [None, Some(1..2), None, Some(3..4)];
//     let res6 = [None, Some(1..3), None, None];
//     let res7 = [None, Some(1..4), None, None];
//     let res8 = [Some(0..1), None, None, None];
//     let res9 = [Some(0..1), None, None, Some(3..4)];
//     let res10 = [Some(0..1), None, Some(2..3), None];
//     let res11 = [Some(0..1), None, Some(2..4), None];
//     let res12 = [Some(0..2), None, None, None];
//     let res13 = [Some(0..2), None, None, Some(3..4)];
//     let res14 = [Some(0..3), None, None, None];
//     let res15 = [Some(0..4), None, None, None];

//     assert_eq!(find_streaks(&properties0, &quads), res0);
//     assert_eq!(find_streaks(&properties1, &quads), res1);
//     assert_eq!(find_streaks(&properties2, &quads), res2);
//     assert_eq!(find_streaks(&properties3, &quads), res3);
//     assert_eq!(find_streaks(&properties4, &quads), res4);
//     assert_eq!(find_streaks(&properties5, &quads), res5);
//     assert_eq!(find_streaks(&properties6, &quads), res6);
//     assert_eq!(find_streaks(&properties7, &quads), res7);
//     assert_eq!(find_streaks(&properties8, &quads), res8);
//     assert_eq!(find_streaks(&properties9, &quads), res9);
//     assert_eq!(find_streaks(&properties10, &quads), res10);
//     assert_eq!(find_streaks(&properties11, &quads), res11);
//     assert_eq!(find_streaks(&properties12, &quads), res12);
//     assert_eq!(find_streaks(&properties13, &quads), res13);
//     assert_eq!(find_streaks(&properties14, &quads), res14);
//     assert_eq!(find_streaks(&properties15, &quads), res15);
// }
