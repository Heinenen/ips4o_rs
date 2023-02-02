mod empty_block_movement;

use std::{cmp::min, sync::Mutex, vec};

use rayon::{current_num_threads, current_thread_index, scope};

use crate::{
    base_case::base_case_sort,
    bucket_pointers::BucketPointer,
    constants::{BASE_CASE_SIZE, BLOCK_SIZE, MAX_BUCKETS, SINGLE_LEVEL_THRESHOLD},
    is_less_to_compare,
    parallel::empty_block_movement::move_empty_blocks,
    permute_blocks::permute_blocks_parallel,
    sequential::{calculate_bucket_boundaries, get_splitters, seq_recurse},
    storage::{GlobalStorage, LocalStorage},
    util::{round_up_to_block_size, test_block_permutation, test_cleanup_margins},
    Less, PLess, PSortable, Sortable,
};

pub(crate) fn parallel_ips4o<T, F>(v: &mut [T], is_less: &F)
where
    T: PSortable,
    F: PLess<T>,
{
    let num_threads = current_num_threads();

    // initialize storage
    let mut lss = Vec::new();
    lss.resize_with(num_threads, || LocalStorage::new(is_less));
    let mut gs: GlobalStorage<T, F> = GlobalStorage::new(is_less);

    parallel(v, &mut lss, &mut gs, is_less);
}

fn parallel<T, F>(
    v: &mut [T],
    lss: &mut [LocalStorage<T, F>],
    gs: &mut GlobalStorage<T, F>,
    is_less: &F,
) where
    T: PSortable,
    F: PLess<T>,
{
    if v.len() <= 2 * BASE_CASE_SIZE {
        base_case_sort(v, is_less);
        return;
    }
    par_recurse(v, lss, gs, is_less);
}

/// Entry point for sequential recursion.
fn par_recurse<T, F>(
    v: &mut [T],
    lss: &mut [LocalStorage<T, F>],
    gs: &mut GlobalStorage<T, F>,
    is_less: &F,
) where
    T: PSortable,
    F: PLess<T>,
{
    debug_assert!(v.len() > 2 * BASE_CASE_SIZE);
    partition(v, lss, gs, is_less);
    let bucket_boundaries = Vec::from(&gs.bucket_boundaries[..gs.num_buckets + 1]);

    // Final base cases were executed in cleanup step, so we're done here
    if v.len() <= SINGLE_LEVEL_THRESHOLD {
        debug_assert!(v.is_sorted_by(is_less_to_compare!(is_less)));
        return;
    }
    let equal_buckets = gs.classifier.equal_buckets;
    let num_buckets = gs.num_buckets;

    let mut parallel_queue = Vec::new();
    let mut sequential_queue = Vec::new();
    sequential_queue.reserve_exact(MAX_BUCKETS);
    let unbalancing_factor = current_num_threads() / 2; // TODO as f64?
    let len = v.len();
    let mut buckets: Vec<Option<&mut [T]>> =
        split_at_bounds(v, &gs.bucket_boundaries[..gs.num_buckets])
            .into_iter()
            .map(Some)
            .collect();

    let mut add_to_queue = |bucket: usize| {
        let range = bucket_boundaries[bucket]..bucket_boundaries[bucket + 1];
        if range.len() > 2 * BASE_CASE_SIZE {
            if range.len() > len / unbalancing_factor {
                parallel_queue.push(buckets[bucket].take().unwrap());
            } else {
                sequential_queue.push(buckets[bucket].take().unwrap());
            }
        } else {
            // should already be sorted in cleanup_margins()
            debug_assert!(buckets[bucket]
                .take()
                .unwrap()
                .is_sorted_by(is_less_to_compare!(is_less)));
        }
    };

    let step = 1 + equal_buckets as usize;
    for i in (0..num_buckets).step_by(step) {
        add_to_queue(i);
    }
    if equal_buckets {
        add_to_queue(num_buckets - 1);
    }

    for bucket in parallel_queue {
        par_recurse(bucket, lss, gs, is_less);
    }
    let lss = lss
        .iter_mut()
        .map(Mutex::new)
        .collect::<Vec<Mutex<&mut LocalStorage<T, F>>>>();
    scope(|s| {
        for bucket in sequential_queue.into_iter() {
            s.spawn(|_| seq_recurse_wrapper(bucket, &lss, is_less));
        }
    });
}

/// Must be called from inside a thread pool
pub(crate) fn seq_recurse_wrapper<T, F>(
    v: &mut [T],
    lss: &[Mutex<&mut LocalStorage<T, F>>],
    is_less: &F,
) where
    T: Sortable,
    F: Less<T>,
{
    let mut ls = lss[current_thread_index().unwrap()].lock().unwrap();
    seq_recurse(v, *ls, is_less)
}

fn partition<T, F>(
    v: &mut [T],
    lss: &mut [LocalStorage<T, F>],
    gs: &mut GlobalStorage<T, F>,
    is_less: &F,
) where
    T: PSortable,
    F: PLess<T>,
{
    let num_threads = current_num_threads();
    let mut sorting_callback =
        |v: &mut [T], gs: &mut GlobalStorage<T, F>| parallel(v, lss, gs, is_less);
    get_splitters(v, gs, &mut sorting_callback, is_less);

    gs.classifier.build();
    debug_assert!(gs.classifier.test_classification(v, is_less));

    // 0.5 is added to avoid rounding errors
    let stripe_len_temp = v.len() as f64 / num_threads as f64;
    let mut stripe_bounds = Vec::new();
    stripe_bounds.reserve_exact(num_threads);
    for i in 0..num_threads {
        let temp = (i as f64 * stripe_len_temp + 0.5) as usize;
        stripe_bounds.push(round_up_to_block_size(temp).min(v.len()));
    }
    debug_assert!(stripe_bounds[0] == 0);

    let mut stripes = split_at_bounds(v, &stripe_bounds);
    let mut results = vec![([0; MAX_BUCKETS], 0); num_threads];
    scope(|s| {
        // Give every thread an equal part of the input to classify locally
        for ((stripe, ls), r) in stripes
            .iter_mut()
            .zip(lss.iter_mut())
            .zip(results.iter_mut())
        {
            s.spawn(|_| {
                let elements_written = gs.classifier.classify_locally(
                    stripe,
                    &mut ls.bucket_buffers,
                    &mut ls.elements_written_per_bucket,
                    gs.num_buckets,
                );

                let elements_per_bucket = ls.elements_written_per_bucket;
                debug_assert!(gs.classifier.test_stripe_classification(
                    stripe,
                    &elements_per_bucket,
                    elements_written,
                ));
                *r = (elements_per_bucket, elements_written);
            });
        }
    });

    let elements_per_bucket = results
        .iter()
        .fold([0usize; MAX_BUCKETS], |mut acc, (x, _)| {
            for i in 0..acc.len() {
                acc[i] += x[i];
            }
            acc
        });

    let elements_written_per_thread = results.iter().map(|x| x.1).collect::<Vec<_>>();

    calculate_bucket_boundaries(
        &mut gs.bucket_boundaries,
        gs.num_buckets,
        &elements_per_bucket,
    );

    let mut stripe_ranges = Vec::new();
    for i in 0..num_threads - 1 {
        stripe_ranges.push(stripe_bounds[i]..stripe_bounds[i + 1]);
    }
    stripe_ranges.push(stripe_bounds[num_threads - 1]..v.len());

    let bounds = gs.bucket_boundaries[..gs.num_buckets]
        .iter()
        .map(|i| i - i % BLOCK_SIZE)
        .collect::<Vec<_>>();
    let buckets = split_at_bounds(v, &bounds);
    scope(|s| {
        for (i, bucket) in buckets.into_iter().enumerate() {
            let stripe_ranges = &stripe_ranges;
            let elements_written_per_thread = &elements_written_per_thread;
            let bucket_boundaries = &gs.bucket_boundaries;
            let bucket_pointers = &gs.bucket_pointers;
            s.spawn(move |_| {
                move_empty_blocks(
                    bucket,
                    i,
                    stripe_ranges,
                    elements_written_per_thread,
                    bucket_boundaries,
                    bucket_pointers,
                )
            })
        }
    });

    let buckets_per_thread = (gs.num_buckets + num_threads - 1) / num_threads;
    let bounds = gs.bucket_boundaries[..gs.num_buckets]
        .iter()
        .map(|i| i - i % BLOCK_SIZE)
        .collect::<Vec<_>>();
    let buckets = split_at_bounds(v, &bounds)
        .into_iter()
        .map(Mutex::new)
        .collect::<Vec<_>>();
    let my_buckets = &buckets[..];

    scope(|s| {
        for (i, ls) in lss.iter_mut().enumerate() {
            let my_first_bucket = i * buckets_per_thread;
            let my_buckets = &my_buckets;
            let bounds = &bounds;
            let c = &gs.classifier;
            let sb = &mut ls.swap_buffers;
            let bucket_pointers = &gs.bucket_pointers[..gs.num_buckets];
            let num_buckets = gs.num_buckets;
            s.spawn(move |_| {
                permute_blocks_parallel(
                    my_buckets,
                    bounds,
                    c,
                    sb,
                    bucket_pointers,
                    my_first_bucket,
                    num_buckets,
                )
            });
        }
    });
    debug_assert!(test_block_permutation(v, gs));

    let mut swaps: Vec<Option<usize>> = vec![None; num_threads];
    scope(|s| {
        for (i, (ls, swap)) in lss.iter_mut().zip(swaps.iter_mut()).enumerate() {
            let my_first_bucket = min(i * buckets_per_thread, gs.num_buckets);
            let v = &v;
            let gs = &gs;
            s.spawn(move |_| {
                *swap = save_margins(v, my_first_bucket, ls, gs);
            });
        }
    });

    let mut stripe_bounds = Vec::new();
    stripe_bounds.reserve_exact(num_threads);
    for i in 0..num_threads {
        let my_first_bucket = min(i * buckets_per_thread, gs.num_buckets);
        stripe_bounds.push(gs.bucket_boundaries[my_first_bucket])
    }

    debug_assert!(stripe_bounds[0] == 0);

    let v_len = v.len();
    let mut stripes = split_at_bounds(v, &stripe_bounds);

    scope(|s| {
        for (i, stripe) in stripes.iter_mut().enumerate() {
            let my_first_bucket = min(i * buckets_per_thread, gs.num_buckets);
            let my_last_bucket = min((i + 1) * buckets_per_thread, gs.num_buckets);
            let v_len = v_len;
            let bucket_boundaries = &gs.bucket_boundaries;
            let bucket_pointers = &gs.bucket_pointers;
            let lss = &lss;
            let swap = &swaps[i];
            s.spawn(move |_| {
                cleanup_margins(
                    stripe,
                    v_len,
                    bucket_boundaries,
                    bucket_pointers,
                    my_first_bucket,
                    my_last_bucket,
                    i,
                    lss,
                    swap,
                    is_less,
                )
            });
        }
    });
    for s in lss.iter_mut() {
        // reset buffers
        s.bucket_buffers.clear_buckets();
    }
    debug_assert!(test_cleanup_margins(v, gs));
    debug_assert!(v.len() > SINGLE_LEVEL_THRESHOLD || v.is_sorted_by(is_less_to_compare!(is_less)));
}

fn split_at_bounds<'a, T>(v: &'a mut [T], splitting_points: &[usize]) -> Vec<&'a mut [T]> {
    let mut stripes = Vec::new();
    let (mut temp_v, right) = v.split_at_mut(*splitting_points.last().unwrap());
    stripes.push(right);
    for i in (0..splitting_points.len() - 1).rev() {
        let (left, right) = temp_v.split_at_mut(splitting_points[i]);
        temp_v = left;
        stripes.push(right);
    }
    stripes.reverse();
    stripes
}

fn save_margins<T, F>(
    v: &[T],
    first_bucket: usize,
    ls: &mut LocalStorage<T, F>,
    gs: &GlobalStorage<T, F>,
) -> Option<usize>
where
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
    // - tail is empty
    // - head might be filled (is filled if at least one block was written back)

    let head_start =
        gs.bucket_boundaries[first_bucket] - gs.bucket_boundaries[first_bucket] % BLOCK_SIZE;
    let next_block_boundary = head_start + BLOCK_SIZE;
    let head_bucket;
    'block: {
        // Find bucket this first block belongs to
        // only occurrs if "first_bucket" is shorter than one block
        let mut bucket = first_bucket;
        while bucket < gs.num_buckets && gs.bucket_boundaries[bucket] < next_block_boundary {
            let size_of_bucket = gs.bucket_boundaries[bucket + 1] - gs.bucket_boundaries[bucket];
            if size_of_bucket >= BLOCK_SIZE {
                // if bucket is smaller than a block, the block cannot belong to the bucket
                head_bucket = bucket;
                break 'block;
            }
            bucket += 1;
        }
        // No bucket with blocks inside head exists
        // => no margins need to be saved
        return None;
    }
    let head_range = head_start..gs.bucket_boundaries[head_bucket];
    // Don't need to do anything if head is empty
    if head_range.is_empty() {
        return None;
    }

    let (write, _read) = gs.bucket_pointers[head_bucket].fetch();
    // No block was written => head doesn't contain any elements
    if write < next_block_boundary {
        return None;
    }

    // Read head elements
    ls.swap_buffers.fill_with(0, &v[head_range]);
    Some(head_bucket)
}

#[allow(clippy::too_many_arguments)]
fn cleanup_margins<T, F>(
    stripe: &mut [T],
    v_len: usize,
    bucket_boundaries: &[usize],
    bucket_pointers: &[BucketPointer],
    first_bucket: usize,
    last_bucket: usize,
    thread_id: usize,
    lss: &[LocalStorage<T, F>],
    swap: &Option<usize>,
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
    //
    //
    // - tail is empty
    // - head might be filled (is filled if at least one block was written back)
    //
    // note: nomenclature of "tail" differs from original paper, here it describes all elements between
    //       "write" and "end"
    //       in the original paper it describes only the last sub-block of our tail
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
    let is_last_level = v_len <= SINGLE_LEVEL_THRESHOLD;
    for i in (first_bucket..last_bucket).rev() {
        // as the indices saved in `bucket_boundaries` and `bucket_pointers` are "global" indices,
        // they have to be converted into "stripe local" indices
        // this in done not here, but at every access of the slice
        let start = bucket_boundaries[i];
        let end = bucket_boundaries[i + 1];
        let (write, _read) = bucket_pointers[i].fetch();
        let head_range = (start - start % BLOCK_SIZE)..start;
        let offset = bucket_boundaries[first_bucket];

        let mut tail_beginning;
        debug_assert!(head_range.len() < BLOCK_SIZE);
        if write == end {
            // end is block aligned and block was written
            // write only increases when block is written back
            // => if no block was written back it would be smaller than end
            tail_beginning = write;
        } else if let Some(head_bucket) = swap && *head_bucket == i {
            // head of this bucket was saved in save_margins()
            let swap_buffer = lss[thread_id].swap_buffers.get(0);

            stripe[write - offset..write - offset + swap_buffer.len()].clone_from_slice(swap_buffer);
            tail_beginning = write + swap_buffer.len();
        } else if start < write {
            // first block was written back into v => head is filled
            // workaround, as slice::clone_within(&mut self, R, usize) doesn't exit
            let (head_slice, write_slice) = stripe.split_at_mut(write - offset);
            write_slice[..head_range.len()].clone_from_slice(&head_slice[head_range.start - offset..head_range.end - offset]);

            tail_beginning = write + head_range.len();
        } else {
            // no block has been flushed
            debug_assert!(write < end);
            tail_beginning = start;
        }

        for ls in lss.iter() {
            let src = ls.bucket_buffers.get(i);
            let count = src.len();
            let tail = &mut stripe[tail_beginning - offset..tail_beginning - offset + count];
            tail.clone_from_slice(src);
            tail_beginning += count;
        }
        debug_assert_eq!(tail_beginning, end);

        if is_last_level || end - start <= 2 * BASE_CASE_SIZE {
            base_case_sort(&mut stripe[start - offset..end - offset], is_less);
        }
    }
}
