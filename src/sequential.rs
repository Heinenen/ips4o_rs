use std::{cmp::max, mem::MaybeUninit};

use rand::Rng;

use crate::{
    base_case::base_case_sort,
    bucket_pointers::BucketPointer,
    constants::{
        log_buckets, ALLOW_EQUAL_BUCKETS, BASE_CASE_SIZE, BLOCK_SIZE, EQUAL_BUCKET_THRESHOLD,
        MAX_BUCKETS, OVERSAMPLING_FACTOR_PERCENT, SINGLE_LEVEL_THRESHOLD,
    },
    is_less_to_compare,
    permute_blocks::permute_blocks,
    storage::{BucketBoundaries, BucketBuffers, Ips4oRng, LocalStorage},
    util::{test_block_permutation, test_cleanup_margins},
    Less, Sortable,
};

fn oversampling_factor(n: usize) -> usize {
    max(
        1,
        (OVERSAMPLING_FACTOR_PERCENT / 100_f64 * n.ilog2() as f64) as usize,
    )
}

pub(crate) fn sequential_ips4o<T, F>(v: &mut [T], is_less: &F)
where
    T: Sortable,
    F: Less<T>,
{
    let mut ls = LocalStorage::<T, F>::new(is_less);
    sequential(v, &mut ls, is_less);
}

pub(crate) fn sequential<T, F>(v: &mut [T], ls: &mut LocalStorage<T, F>, is_less: &F)
where
    T: Sortable,
    F: Less<T>,
{
    if v.len() <= 2 * BASE_CASE_SIZE {
        base_case_sort(v, is_less);
        return;
    }
    seq_recurse(v, ls, false, is_less);
}

/// Entry point for sequential recursion.
fn seq_recurse<T, F>(v: &mut [T], ls: &mut LocalStorage<T, F>, unbalanced: bool, is_less: &F)
where
    T: Sortable,
    F: Less<T>,
{
    debug_assert!(v.len() > 2 * BASE_CASE_SIZE);
    partition(v, ls, unbalanced, is_less);

    let mut bucket_boundaries: [MaybeUninit<usize>; MAX_BUCKETS + 1] =
        [MaybeUninit::uninit(); MAX_BUCKETS + 1];
    let bucket_boundaries = MaybeUninit::write_slice(
        &mut bucket_boundaries[..ls.num_buckets + 1],
        &ls.bucket_boundaries[..ls.num_buckets + 1],
    );

    // Final base cases were executed in cleanup step, so we're done here
    if v.len() <= SINGLE_LEVEL_THRESHOLD {
        debug_assert!(v.is_sorted_by(is_less_to_compare!(is_less)));
        return;
    }
    let equal_buckets = ls.classifier.equal_buckets;
    let num_buckets = ls.num_buckets;
    let len = v.len();

    let mut recurse = |bucket: usize| {
        // let bucket_boundaries = &mut gs.bucket_boundaries[..gs.num_buckets + 1];
        let range = bucket_boundaries[bucket]..bucket_boundaries[bucket + 1];
        if range.len() > 2 * BASE_CASE_SIZE {
            let new_unbalanced = range.len() > 2 * len / num_buckets;
            seq_recurse(&mut v[range], ls, new_unbalanced, is_less);
        } else {
            // should already be sorted in cleanup_margins()
            debug_assert!(v[range].is_sorted_by(is_less_to_compare!(is_less)));
        }
    };

    // Recurse
    let step = 1 + equal_buckets as usize;
    for i in (0..num_buckets).step_by(step) {
        recurse(i);
    }
    if equal_buckets {
        recurse(num_buckets - 1);
    }
}

fn partition<T, F>(v: &mut [T], ls: &mut LocalStorage<T, F>, unbalanced: bool, is_less: &F)
where
    T: Sortable,
    F: Less<T>,
{
    let mut sorting_callback =
        |v: &mut [T], ls: &mut LocalStorage<T, F>| sequential(v, ls, is_less);
    get_splitters(v, ls, unbalanced, &mut sorting_callback, is_less); // TODO should be moved into Classifier::build()

    ls.classifier.build();
    debug_assert!(ls.classifier.test_classification(v, is_less));

    let total_elements_written_back = ls.classifier.classify_locally(
        v,
        &mut ls.bucket_buffers,
        &mut ls.elements_written_per_bucket,
        ls.num_buckets,
    );

    let elements_per_bucket = ls.elements_written_per_bucket;

    calculate_bucket_boundaries(
        &mut ls.bucket_boundaries,
        ls.num_buckets,
        &elements_per_bucket,
    );
    calculate_bucket_pointers(
        &ls.bucket_boundaries[..ls.num_buckets + 1],
        &mut ls.bucket_pointers[..ls.num_buckets],
        total_elements_written_back,
    );
    permute_blocks(
        v,
        &ls.classifier,
        &mut ls.swap_buffers,
        &mut ls.bucket_pointers[..ls.num_buckets],
        0,
    );
    debug_assert!(test_block_permutation(v, ls));
    cleanup_margins(
        v,
        &ls.bucket_buffers,
        &ls.bucket_boundaries[..ls.num_buckets + 1],
        &mut ls.bucket_pointers[..ls.num_buckets],
        is_less,
    );
    debug_assert!(test_cleanup_margins(v, ls))
}

pub(crate) fn select_random<T>(v: &mut [T], sample_size: usize, rng: &mut Ips4oRng)
where
    T: Sortable,
{
    debug_assert!(sample_size <= v.len());
    for i in 0..sample_size {
        v.swap(i, rng.rng.gen_range(i..v.len()));
    }
}

pub(crate) fn select_equidistant<T>(v: &mut [T], sample_size: usize) {
    debug_assert!(sample_size <= v.len());
    let step = v.len() / sample_size;
    for (i, index) in (0..v.len()).step_by(step).enumerate() {
        v.swap(i, index);
    }
}

// TODO maybe move inside classifier
pub(crate) fn get_splitters<T, F, S>(
    v: &mut [T],
    ls: &mut LocalStorage<T, F>,
    unbalanced: bool,
    sorting_callback: &mut S,
    is_less: &F,
) where
    T: Sortable,
    F: Less<T>,
    S: FnMut(&mut [T], &mut LocalStorage<T, F>),
{
    let n = v.len();
    let num_buckets = 1usize << log_buckets(n);
    let step = oversampling_factor(n);
    let sample_size = (step * num_buckets - 1).min(n / 2);

    // Select the sample
    // select_sample(v, sample_size, &mut ls.rng);

    if unbalanced {
        select_random(v, sample_size, &mut ls.rng);
    } else {
        select_equidistant(v, sample_size);
    }

    // Sort the sample
    sorting_callback(&mut v[0..sample_size], ls);
    // Choose the splitters
    let mut current = step - 1;
    let mut current_idx = 1;
    let set = ls.classifier.get_all_splitters_mut();
    set[0] = v[current].clone();
    for _ in 2..num_buckets {
        current += step;
        // Skip duplicates
        if is_less(&set[current_idx - 1], &v[current]) {
            set[current_idx] = v[current].clone();
            current_idx += 1;
        }
    }
    // TODO what happens if only one splitter is chosen, may not terminate

    let splitter_count = current_idx;
    let max_splitters = num_buckets - 1;
    debug_assert!(num_buckets <= MAX_BUCKETS);
    let use_equal_buckets =
        ALLOW_EQUAL_BUCKETS && max_splitters - splitter_count >= EQUAL_BUCKET_THRESHOLD;

    // Fill vec to the next power of 2
    let log_buckets = splitter_count.ilog2() + 1;
    let num_buckets = 1usize << log_buckets;

    for i in current_idx..num_buckets {
        set[i] = set[current_idx - 1].clone();
    }

    ls.classifier.set_splitter_len(num_buckets);
    ls.classifier.equal_buckets = use_equal_buckets;
    ls.num_buckets = num_buckets << use_equal_buckets as usize;

    debug_assert!(splitter_count < num_buckets);
    debug_assert!(ls.classifier.get_all_splitters_mut()[..num_buckets]
        .is_sorted_by(is_less_to_compare!(is_less)));
}

pub(crate) fn calculate_bucket_boundaries(
    bucket_boundaries: &mut BucketBoundaries,
    num_buckets: usize,
    elements_per_bucket: &[usize],
) {
    debug_assert!(bucket_boundaries[0] == 0);

    let prefix_sum = elements_per_bucket[..num_buckets].iter().scan(0, |sum, i| {
        *sum += i;
        Some(*sum)
    });
    for (bb, new_bb) in bucket_boundaries[1..num_buckets + 1]
        .iter_mut()
        .zip(prefix_sum)
    {
        *bb = new_bb;
    }
}

fn calculate_bucket_pointers(
    bucket_boundaries: &[usize],
    bucket_pointers: &mut [BucketPointer],
    first_empty_block: usize,
) {
    // Writing index, starts as bucket delimiters rounded down to previous block
    // Looks like [0, start_of_1st_bucket, start_of_2nd_bucket, ..., start_of_last_bucket]
    // Each time something is written to the bucket, is incremented by 1 * BLOCK_SIZE (w += BLOCK_SIZE)
    let w = bucket_boundaries[..bucket_boundaries.len() - 1]
        .iter()
        .map(|&boundary| boundary - boundary % BLOCK_SIZE);

    // Reading index, starts as bucket delimiters of next block rounded down
    // Each time something is written to the bucket, is decremented by 1 * BLOCK_SIZE (r -= BLOCK_SIZE)
    let r = bucket_boundaries[1..]
        .iter()
        .map(|&boundary| boundary - boundary % BLOCK_SIZE)
        .map(|item| item.min(first_empty_block));

    for ((bp, write), read) in bucket_pointers.iter_mut().zip(w).zip(r) {
        *bp = BucketPointer::new(write, read);
    }
}

fn cleanup_margins<T, F>(
    v: &mut [T],
    bucket_buffers: &BucketBuffers<T>,
    bucket_boundaries: &[usize],
    bucket_pointers: &mut [BucketPointer],
    is_less: &F,
) where
    T: Sortable,
    F: Less<T>,
{
    //        head                 tail
    //        <-->                 <--->
    // ------|------|------|------|------|------|------|------
    //           ][      bucket i      ][
    // ----------><--------------------><-------
    //            ^           write^   ^  ^write_after_cleanup
    //          start                 end
    //
    // - head.len() < block size
    // - tail.len() < 2 * block size
    // -
    //
    // - tail is empty
    // - head might be filled (is filled if at least one block was written back)
    //
    //
    // note: head and tail may overlap at the start, but only if bucket.len() < block.len() (at least sequential)
    // in this case, all elements of the bucket are still in buffers
    // make the tail shorter to match the bucket length
    //     <----->                    head
    //     <------------------------> tail
    //            <-----------------> new tail
    // ---|-------------------------------------|---
    //    ...    ][    bucket i     ][    ...
    //
    // ------|------|------|------|------|------|------|------
    //    ...        ][  bucket i  ][    ...

    let is_last_level = v.len() <= SINGLE_LEVEL_THRESHOLD;
    for i in (0..bucket_pointers.len()).rev() {
        let start = bucket_boundaries[i];
        let end = bucket_boundaries[i + 1];
        let (write, _read) = bucket_pointers[i].fetch();
        let head_range = (start - start % BLOCK_SIZE)..start;

        let tail_beginning;
        debug_assert!(head_range.len() < BLOCK_SIZE);
        if write == end {
            // end is block aligned and block was written
            // write only increases when block is written back => if no block was written back it would be smaller than end
            tail_beginning = write;
        } else if start < write {
            // first block was written back into v => head is filled

            // let head = &v[head_range.start] as *const T;
            // let dst = &mut v[write] as *mut T;
            // unsafe {
            //     copy_nonoverlapping(head, dst, head_range.len());
            // }

            // workaround, as slice::clone_within(&mut self, R, usize) doesn't exit (but copy_within does)
            let (head_slice, write_slice) = v.split_at_mut(write);
            write_slice[..head_range.len()].clone_from_slice(&head_slice[head_range.clone()]);

            tail_beginning = write + head_range.len();
        } else {
            // no block has been flushed
            debug_assert!(write < end);
            tail_beginning = start;
        }

        // case 1: start < write => head was filled during block permutation
        // case 2: head is empty => head and tail overlap (see above) and head is subset of tail
        // => in both cases only tail has to be filled
        let tail_range = tail_beginning..end;
        debug_assert_eq!(bucket_buffers.len(i), tail_range.len());
        let tail = &mut v[tail_range];
        tail.clone_from_slice(bucket_buffers.get(i));

        if is_last_level || end - start <= 2 * BASE_CASE_SIZE {
            base_case_sort(&mut v[start..end], is_less);
        }
    }
}
