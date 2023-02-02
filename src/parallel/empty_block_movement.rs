use std::{
    cmp::{max, min},
    ops::Range,
};

use rayon::current_num_threads;

use crate::{bucket_pointers::BucketPointer, constants::BLOCK_SIZE, storage::BucketBoundaries};

/// Moves empty blocks to establish invariant:
/// All buckets must consist of full blocks followed by empty blocks.
/// single | is a block boundary, double | is a stripe boundary (assumes that stripe boundaries are aligned with block boundaries)
///
///  //                   stripe i-1                                          stripe i                                            stripe i+1
/// ||----------------|----------------|----------------||----------------|----------------|----------------||----------------|----------------|----------------||
///        full             full             empty             full             full             empty             full             full             empty
/// |***************************************|****************************************************************************|****************************|**********|
///               bucket k                                                            bucket k+1                                   bucket k+2          bucket k+3
pub(super) fn move_empty_blocks<T>(
    bucket: &mut [T],
    bucket_number: usize,
    stripe_ranges: &[Range<usize>],
    flushed_elements_in_stripes: &[usize],
    bucket_boundaries: &BucketBoundaries,
    bucket_pointers: &[BucketPointer],
) where
    T: Copy,
{
    debug_assert_eq!(stripe_ranges.len(), flushed_elements_in_stripes.len());
    let num_threads = current_num_threads();

    let first_empty_block =
        |thread: usize| stripe_ranges[thread].start + flushed_elements_in_stripes[thread];

    let align_to_prev_block = |x: usize| x - x % BLOCK_SIZE;
    let bucket_range = align_to_prev_block(bucket_boundaries[bucket_number])
        ..align_to_prev_block(bucket_boundaries[bucket_number + 1]);
    let offset = bucket_range.start;

    // first stripe ending in this bucket
    let stripe_range_start = {
        let mut i = 0;
        while i < num_threads && stripe_ranges[i].end <= bucket_range.start {
            i += 1;
        }
        i
    };
    // first stripe starting in the next bucket
    let stripe_range_end = {
        let mut i = 0;
        while i < num_threads && stripe_ranges[i].start < bucket_range.end {
            i += 1;
        }
        i
    };
    let mut flushed_elements_in_bucket = 0;
    for (s, stripe_range) in stripe_ranges
        .iter()
        .enumerate()
        .take(stripe_range_end)
        .skip(stripe_range_start)
    {
        if first_empty_block(s) < bucket_range.start {
            continue;
        }
        let flush_end = min(first_empty_block(s), bucket_range.end);
        flushed_elements_in_bucket += flush_end - max(stripe_range.start, bucket_range.start);
    }
    let first_empty_block_after = bucket_range.start + flushed_elements_in_bucket;

    let start = bucket_range.start;
    let end = bucket_range.end;
    let read;
    if first_empty_block_after <= start {
        // Bucket is completely empty
        read = start;
    } else if first_empty_block_after < end {
        // Bucket is partially empty
        read = first_empty_block_after;
    } else {
        read = end;
    }
    bucket_pointers[bucket_number].set(start, read);

    if bucket_range.is_empty() {
        return;
    }

    let mut reserved = 0;
    let mut currently_reserved: usize;
    for s in stripe_range_start..stripe_range_end {
        currently_reserved = reserved;
        let mut write_ptr = max(bucket_range.start, first_empty_block(s));
        let write_end = min(first_empty_block_after, stripe_ranges[s].end);
        let mut read_from_stripe = stripe_range_end;

        while write_ptr < write_end {
            read_from_stripe -= 1;

            let mut read_ptr = min(first_empty_block(read_from_stripe), bucket_range.end);
            let mut read_range_size = read_ptr - stripe_ranges[read_from_stripe].start;
            if currently_reserved >= read_range_size {
                currently_reserved -= read_range_size;
                continue;
            }
            read_ptr -= currently_reserved;
            read_range_size -= currently_reserved;
            currently_reserved = 0;
            let size = min(read_range_size, write_end - write_ptr);
            let range = read_ptr - size - offset..read_ptr - offset;
            let w = write_ptr - offset;
            bucket.copy_within(range, w);
            write_ptr += size;
            reserved += size;
        }
    }
}
