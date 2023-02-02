use std::sync::Mutex;

use crate::{
    bucket_pointers::BucketPointer,
    classifier::Classifier,
    constants::{BLOCK_SIZE, MAX_BUCKETS},
    storage::SwapBuffers,
    Less, Sortable,
};

pub(crate) fn permute_blocks<T, F>(
    v: &mut [T],
    c: &Classifier<T, F>,
    sb: &mut SwapBuffers<T>,
    bucket_pointers: &mut [BucketPointer],
    starting_bucket: usize,
) where
    T: Sortable,
    F: Less<T>,
{
    let mut current_swap;
    // for bucket in 0..MAX_BUCKETS {
    for bucket in 0..bucket_pointers.len() {
        // TODO maybe give bucket_pointers the fixed length of MAX_BUCKETS or MAX_BUCKETS * 2
        let current_bucket = (starting_bucket + bucket) % MAX_BUCKETS;
        while classify_and_read_block(v, sb, c, bucket_pointers, current_bucket).is_some() {
            current_swap = 0;
            loop {
                let dest = c.classify_single_element(&sb.get(current_swap)[0]);
                let performed_swap = swap_block(v, sb, bucket_pointers, dest, current_swap);
                current_swap = 1 - current_swap;
                if !performed_swap {
                    break;
                }
            }
        }
    }
}

fn classify_and_read_block<T, F>(
    v: &[T],
    s: &mut SwapBuffers<T>,
    c: &Classifier<T, F>,
    bucket_pointers: &mut [BucketPointer],
    read_bucket: usize,
) -> Option<usize>
where
    T: Sortable,
    F: Less<T>,
{
    match bucket_pointers[read_bucket].dec_read() {
        Ok((write, read)) => {
            if read < write {
                // No more blocks to read in this bucket
                return None;
            }
            s.fill_with(0, &v[read..read + BLOCK_SIZE]);

            Some(c.classify_single_element(&s.get(0)[0]))
        }
        Err(_) => None,
    }
}

fn swap_block<T>(
    v: &mut [T],
    swap: &mut SwapBuffers<T>,
    bucket_pointers: &mut [BucketPointer],
    dest: usize,
    current_swap: usize,
) -> bool
where
    T: Sortable,
{
    let (write, read) = bucket_pointers[dest].inc_write();
    if write > read {
        // Destination block is empty
        v[write - BLOCK_SIZE..write].clone_from_slice(swap.get(current_swap));
        return false;
    }

    // Swap blocks
    swap.fill_with(1 - current_swap, &v[write - BLOCK_SIZE..write]);
    v[write - BLOCK_SIZE..write].clone_from_slice(swap.get(current_swap));
    true
}

pub(crate) fn permute_blocks_parallel<T, F>(
    buckets: &[Mutex<&mut [T]>],
    bounds: &[usize],
    c: &Classifier<T, F>,
    sb: &mut SwapBuffers<T>,
    bucket_pointers: &[BucketPointer],
    starting_bucket: usize,
    num_buckets: usize,
) where
    T: Sortable,
    F: Less<T>,
{
    let mut current_swap;
    for bucket in 0..num_buckets {
        let current_bucket = (starting_bucket + bucket) % num_buckets;
        while classify_and_read_block_parallel(
            buckets,
            bounds,
            sb,
            c,
            bucket_pointers,
            current_bucket,
        )
        .is_some()
        {
            current_swap = 0;
            loop {
                let dest = c.classify_single_element(&sb.get(current_swap)[0]);
                let performed_swap =
                    swap_block_parallel(buckets, bounds, sb, bucket_pointers, dest, current_swap);
                current_swap = 1 - current_swap;
                if !performed_swap {
                    break;
                }
            }
        }
    }
}

fn classify_and_read_block_parallel<T, F>(
    buckets: &[Mutex<&mut [T]>],
    bounds: &[usize],
    s: &mut SwapBuffers<T>,
    c: &Classifier<T, F>,
    bucket_pointers: &[BucketPointer],
    read_bucket: usize,
) -> Option<usize>
where
    T: Sortable,
    F: Less<T>,
{
    // lock to bucket must be acquired before decreasing the read pointer, to prevent
    // other threads to write to the block before it is read
    let v = buckets[read_bucket].lock().unwrap();
    match bucket_pointers[read_bucket].dec_read() {
        Ok((write, mut read)) => {
            if read < write {
                // No more blocks to read in this bucket
                return None;
            }
            read -= bounds[read_bucket];
            s.fill_with(0, &v[read..read + BLOCK_SIZE]);

            Some(c.classify_single_element(&s.get(0)[0]))
        }
        Err(_) => None,
    }
}

fn swap_block_parallel<T>(
    buckets: &[Mutex<&mut [T]>],
    bounds: &[usize],
    swap: &mut SwapBuffers<T>,
    bucket_pointers: &[BucketPointer],
    dest: usize,
    current_swap: usize,
) -> bool
where
    T: Sortable,
{
    let mut v = buckets[dest].lock().unwrap();
    let (mut write, read) = bucket_pointers[dest].inc_write();
    if write > read {
        write -= bounds[dest];
        // Destination block is empty
        v[write - BLOCK_SIZE..write].clone_from_slice(swap.get(current_swap));
        return false;
    }
    write -= bounds[dest];

    // Swap blocks
    swap.fill_with(1 - current_swap, &v[write - BLOCK_SIZE..write]);
    v[write - BLOCK_SIZE..write].clone_from_slice(swap.get(current_swap));
    true
}
