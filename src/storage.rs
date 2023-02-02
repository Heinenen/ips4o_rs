use std::{fmt::Debug, mem::MaybeUninit, slice::from_raw_parts_mut};

use rand::{rngs::StdRng, SeedableRng};

use crate::{
    bucket_pointers::BucketPointers,
    classifier::Classifier,
    constants::{BLOCK_SIZE, MAX_BUCKETS},
    Less, Sortable,
};

#[derive(Debug)]
pub struct BucketBuffers<T: Clone> {
    buckets: Box<[[MaybeUninit<T>; BLOCK_SIZE]; MAX_BUCKETS]>,
    len: [usize; MAX_BUCKETS],
}

#[derive(Debug)]
pub(crate) struct SwapBuffers<T> {
    swap: Box<[[MaybeUninit<T>; BLOCK_SIZE]; 2]>,
    len: [usize; 2],
}

impl<T: Sortable> Default for SwapBuffers<T> {
    fn default() -> Self {
        // SAFETY: array of unit data does no need initialization
        let swap = unsafe { Box::new_uninit().assume_init() };
        let len = [0; 2];
        Self { swap, len }
    }
}

impl<T: Sortable> SwapBuffers<T> {
    pub fn fill_with(&mut self, index: usize, slice: &[T]) {
        for (a, b) in self.swap[index].iter_mut().zip(slice) {
            a.write(b.clone());
        }
        self.len[index] = slice.len();
    }

    pub fn get(&self, index: usize) -> &[T] {
        // SAFETY: len must be set correctly in fill_with()
        unsafe {
            &*(&self.swap[index][0..self.len[index]] as *const [MaybeUninit<T>] as *const [T])
        }
    }
}

impl<T: Sortable> Default for BucketBuffers<T> {
    fn default() -> Self {
        // SAFETY: array of unit data does no need initialization
        let buckets = unsafe { Box::new_uninit().assume_init() };
        let len = [0; MAX_BUCKETS];
        Self { buckets, len }
    }
}

impl<T: Clone> BucketBuffers<T> {
    /// Only call when buffer is empty, otherwise elements may leak
    pub(crate) fn get_raw(&mut self) -> &mut [MaybeUninit<T>; BLOCK_SIZE * MAX_BUCKETS] {
        let ptr = self.buckets.as_mut_ptr() as *mut MaybeUninit<T>;
        let len = BLOCK_SIZE * MAX_BUCKETS;
        debug_assert_eq!(self.buckets.len() * self.buckets[0].len(), len);

        let raw = unsafe { from_raw_parts_mut(ptr, len) };
        raw.try_into().unwrap()
    }

    pub(crate) fn clear_buckets(&mut self) {
        for i in self.len.iter_mut() {
            *i = 0;
        }
    }

    #[allow(unused)]
    pub fn push(&mut self, index: usize, elem: T) {
        self.buckets[index][self.len[index]].write(elem);
        self.len[index] += 1;
    }

    pub fn len(&self, index: usize) -> usize {
        self.len[index]
    }

    pub fn clear(&mut self, index: usize) {
        for i in 0..self.len[index] {
            unsafe { self.buckets[index][i].assume_init_drop() };
        }
        self.len[index] = 0;
    }

    pub fn get(&self, index: usize) -> &[T] {
        // SAFETY: len must be set correctly in clear() and unchecked_push()
        unsafe {
            &*(&self.buckets[index][0..self.len[index]] as *const [MaybeUninit<T>] as *const [T])
        }
    }

    pub unsafe fn unchecked_push(&mut self, index: usize, elem: T) -> usize {
        // SAFETY: idx <= MAX_BUCKETS && elem_idx <= BLOCK_SIZE
        // => unchecked_push(idx) may only be called BLOCK_SIZE
        // times before clear(idx) must be called
        let mut elem_idx = *self.len.get_unchecked(index);
        self.buckets
            .get_unchecked_mut(index)
            .get_unchecked_mut(elem_idx)
            .write(elem);

        elem_idx += 1;
        *self.len.get_unchecked_mut(index) = elem_idx;
        elem_idx
    }
}

impl<T: Clone> Drop for BucketBuffers<T> {
    fn drop(&mut self) {
        for i in 0..MAX_BUCKETS {
            self.clear(i);
        }
    }
}

#[derive(Debug)]
pub(crate) struct LocalStorage<'a, T, F>
where
    T: Sortable,
    F: Less<T>,
{
    pub bucket_buffers: BucketBuffers<T>,
    pub swap_buffers: SwapBuffers<T>,

    pub classifier: Classifier<'a, T, F>,
    pub bucket_pointers: BucketPointers,
    pub bucket_boundaries: BucketBoundaries,
    pub elements_written_per_bucket: [usize; MAX_BUCKETS],
    /// Number of buckets, with equal buckets; "length" of bucket_pointers and bucket_boundaries[1..]
    pub num_buckets: usize,
    pub rng: Ips4oRng,
}

impl<'a, T, F> LocalStorage<'a, T, F>
where
    T: Sortable,
    F: Less<T>,
{
    pub(crate) fn new(is_less: &'a F) -> Self {
        Self {
            classifier: Classifier::new(is_less),
            bucket_pointers: core::array::from_fn(|_| Default::default()),
            bucket_boundaries: [0; MAX_BUCKETS + 1],
            elements_written_per_bucket: [0; MAX_BUCKETS],
            bucket_buffers: Default::default(),
            swap_buffers: Default::default(),
            num_buckets: Default::default(),
            rng: Default::default(),
        }
    }
}

pub(crate) type BucketBoundaries = [usize; MAX_BUCKETS + 1]; // TODO maybe replace with newtype
pub(crate) type GlobalStorage<'a, T, F> = LocalStorage<'a, T, F>;

#[derive(Debug)]
pub(crate) struct Ips4oRng {
    pub rng: StdRng,
}

impl Default for Ips4oRng {
    fn default() -> Self {
        // Self { rng: thread_rng() }
        Self {
            rng: StdRng::seed_from_u64(0),
        }
    }
}
