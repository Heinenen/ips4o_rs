use std::{fmt::Debug, ops::Range};

use crate::{
    constants::{BATCH_SIZE, BLOCK_SIZE, LOG_MAX_BUCKETS, MAX_BUCKETS},
    storage::BucketBuffers,
    Less, Sortable,
};

const SPLITTERS_LEN: usize = 1 << LOG_MAX_BUCKETS;

#[derive(Debug)]
pub(crate) struct Classifier<'a, T, F>
where
    F: Less<T>,
{
    tree: [T; SPLITTERS_LEN],
    splitters: [T; SPLITTERS_LEN],
    splitter_len: usize,
    pub equal_buckets: bool,
    is_less: &'a F,
}

impl<'a, T, F> Classifier<'a, T, F>
where
    T: Sortable,
    F: Less<T>,
{
    pub(crate) fn new(is_less: &'a F) -> Self {
        let splitters = core::array::from_fn(|_| T::default());
        let tree = core::array::from_fn(|_| T::default());
        Self {
            splitters,
            tree,
            splitter_len: 0,
            is_less,
            equal_buckets: Default::default(),
        }
    }

    pub(crate) fn build(&mut self) {
        // increase size of tree by one, so it can be 1-indexed
        self.build_recurse(0..self.splitter_len - 1, 1);
    }

    fn build_recurse(&mut self, range: Range<usize>, pos: usize) {
        debug_assert!((range.len() + 1).is_power_of_two());
        if !range.is_empty() {
            let mid = range.start + range.len() / 2;
            self.tree[pos] = self.splitters[mid].clone();

            self.build_recurse(range.start..mid, pos * 2);
            self.build_recurse(mid + 1..range.end, pos * 2 + 1);
        }
    }

    pub fn get_splitters(&self) -> &[T] {
        &self.splitters[..self.splitter_len]
    }

    pub fn get_all_splitters_mut(&mut self) -> &mut [T; SPLITTERS_LEN] {
        &mut self.splitters
    }

    pub fn set_splitter_len(&mut self, splitter_len: usize) {
        self.splitter_len = splitter_len;
    }

    // returns bucket indices
    fn classify_batch<
        const EQUAL_BUCKETS: bool,
        const LOG_BUCKETS: usize,
        const BATCH_SIZE: usize,
    >(
        &self,
        v: &[T; BATCH_SIZE],
    ) -> [usize; BATCH_SIZE] {
        let len = self.splitter_len;
        let num_buckets = len << EQUAL_BUCKETS as u32;
        let mut bucket_indices = [1usize; BATCH_SIZE];
        for _ in 0..LOG_BUCKETS {
            for i in 0..BATCH_SIZE {
                let value = &v[i];
                let index = bucket_indices[i];
                bucket_indices[i] = 2 * index + (self.is_less)(&self.tree[index], value) as usize;
            }
        }
        if EQUAL_BUCKETS {
            for i in 0..BATCH_SIZE {
                let value = &v[i];
                let index = bucket_indices[i];
                let is_equal = !(self.is_less)(value, &self.splitters[index - len]);
                bucket_indices[i] = 2 * index + is_equal as usize;
            }
        }
        for bucket_index in bucket_indices.iter_mut() {
            *bucket_index -= num_buckets;
        }
        bucket_indices
    }

    // returns bucket index
    pub(crate) fn classify_single_element(&self, val: &T) -> usize {
        let log_buckets = self.splitter_len.ilog2();
        let len = self.splitter_len;
        let num_buckets = len << self.equal_buckets as u32;
        let mut b = 1;
        for _i in 0..log_buckets {
            b = 2 * b + (self.is_less)(&self.tree[b], val) as usize
        }
        if self.equal_buckets {
            let is_equal = !(self.is_less)(val, &self.splitters[b - len]);
            b = 2 * b + is_equal as usize;
        }
        b - num_buckets
    }

    pub(crate) fn classify_locally(
        &self,
        stripe: &mut [T],
        buckets: &mut BucketBuffers<T>,
        elements_per_bucket: &mut [usize; MAX_BUCKETS],
        num_buckets: usize,
    ) -> usize {
        let elements_per_bucket_slice = &mut elements_per_bucket[..num_buckets];
        if self.equal_buckets {
            self.classify_locally_helper::<true>(stripe, buckets, elements_per_bucket_slice)
        } else {
            self.classify_locally_helper::<false>(stripe, buckets, elements_per_bucket_slice)
        }
    }

    #[rustfmt::skip]
    pub(crate) fn classify_locally_helper<const EQUAL_BUCKETS: bool>(
        &self,
        stripe: &mut [T],
        buckets: &mut BucketBuffers<T>,
        elements_per_bucket: &mut [usize],
    ) -> usize
    {
        let log_buckets = self.splitter_len.ilog2();
        match log_buckets {
            1 => self.classify_locally_inner::<EQUAL_BUCKETS, 1>(stripe, buckets, elements_per_bucket),
            2 => self.classify_locally_inner::<EQUAL_BUCKETS, 2>(stripe, buckets, elements_per_bucket),
            3 => self.classify_locally_inner::<EQUAL_BUCKETS, 3>(stripe, buckets, elements_per_bucket),
            4 => self.classify_locally_inner::<EQUAL_BUCKETS, 4>(stripe, buckets, elements_per_bucket),
            5 => self.classify_locally_inner::<EQUAL_BUCKETS, 5>(stripe, buckets, elements_per_bucket),
            6 => self.classify_locally_inner::<EQUAL_BUCKETS, 6>(stripe, buckets, elements_per_bucket),
            7 => self.classify_locally_inner::<EQUAL_BUCKETS, 7>(stripe, buckets, elements_per_bucket),
            8 => self.classify_locally_inner::<EQUAL_BUCKETS, 8>(stripe, buckets, elements_per_bucket),
            9 => self.classify_locally_inner::<EQUAL_BUCKETS, 9>(stripe, buckets, elements_per_bucket),
            _ => unreachable!("Maximum number of log buckets, declared in constants.rs is 9"),
        }
    }

    fn classify_locally_inner<const EQUAL_BUCKETS: bool, const LOG_BUCKETS: usize>(
        &self,
        stripe: &mut [T],
        buckets: &mut BucketBuffers<T>,
        elements_per_bucket: &mut [usize],
    ) -> usize {
        buckets.clear_buckets();

        elements_per_bucket.iter_mut().for_each(|it| *it = 0);

        let mut elements_written = 0;

        let mut insert_into_bucket = |stripe: &mut [T], offset: usize, bucket_index: usize| {
            let new_len = unsafe {
                // SAFETY: caller must ensure that bucket_index <= MAX_BUCKETS,
                // bucket flushing below ensures not calling uncheck_push() too often
                buckets.unchecked_push(bucket_index, stripe.get_unchecked(offset).clone())
            };

            // if buffer is full, write buffer contents back into stripe
            if new_len >= BLOCK_SIZE {
                {
                    // let bucket_start = buckets[bucket_index].as_ptr();
                    // let stripe_start = stripe[write_index..].as_mut_ptr();
                    // unsafe {
                    //     copy_nonoverlapping(bucket_start, stripe_start, BLOCK_SIZE);
                    // }
                    // stripe[write_index..write_index + BLOCK_SIZE].copy_from_slice(&buckets[bucket_index]);
                    stripe[elements_written..elements_written + BLOCK_SIZE]
                        .clone_from_slice(buckets.get(bucket_index));
                    buckets.clear(bucket_index);
                }
                elements_per_bucket[bucket_index] += BLOCK_SIZE;
                elements_written += BLOCK_SIZE;
            }
        };

        let mut i = 0;
        if stripe.len() > BATCH_SIZE {
            let cutoff = stripe.len() - BATCH_SIZE;
            while i <= cutoff {
                let batch = (&stripe[i..i + BATCH_SIZE]).try_into().unwrap();
                let bucket_indices =
                    self.classify_batch::<EQUAL_BUCKETS, LOG_BUCKETS, BATCH_SIZE>(batch);
                for (j, bucket_index) in bucket_indices.iter().copied().enumerate() {
                    insert_into_bucket(stripe, i + j, bucket_index);
                }
                i += BATCH_SIZE;
            }
        }
        for i in i..stripe.len() {
            let batch = (&stripe[i..i + 1]).try_into().unwrap();
            let [bucket_index] = self.classify_batch::<EQUAL_BUCKETS, LOG_BUCKETS, 1>(batch);
            insert_into_bucket(stripe, i, bucket_index);
        }

        for (i, elements) in elements_per_bucket.iter_mut().enumerate() {
            *elements += buckets.len(i);
        }
        debug_assert!(self.test_stripe_classification(
            stripe,
            elements_per_bucket,
            elements_written,
        ));

        elements_written
    }
}
