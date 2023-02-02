#![allow(clippy::needless_lifetimes)] // Improves readability despite what clippy claims.
#![allow(clippy::type_complexity)] // Somethings things get complex...
#![allow(clippy::unnecessary_mut_passed)] // Exclusivity assertions.
#![allow(clippy::forget_non_drop)] // Forgetting soon-to-be-overlapped slices is important.

use std::mem::MaybeUninit;

use rayon::join;

use self::{
    mut_slice::MutSlice,
    physical_merges::{
        par_physical_merge, physical_merge, physical_quad_merge, physical_triple_merge,
    },
    util::Cmp,
};

mod branchless_merge;
mod gap_guard;
mod merge_reduction;
mod mut_slice;
mod physical_merges;
mod tracking;
mod util;

// If the total size of a merge operation is above this threshold glidesort will
// attempt to split it into (instruction-level) parallel merges when applicable.
const MERGE_SPLIT_THRESHOLD: usize = 32;

pub fn double_merge<T, F: Cmp<T>>(
    v: &mut [T],
    half1: usize,
    scratch: &mut [MaybeUninit<T>],
    is_less: &F,
) {
    MutSlice::from_maybeuninit_mut_slice(scratch, |scratch_space| {
        MutSlice::from_mut_slice(v, |mut s| {
            let r0 = s.split_off_begin(half1);
            let r1 = s;
            physical_merge(r0, r1, scratch_space.assume_uninit(), is_less);
        });
    });
}

pub fn triple_merge<T, F: Cmp<T>>(
    v: &mut [T],
    third1: usize,
    third2: usize,
    scratch: &mut [MaybeUninit<T>],
    is_less: &F,
) {
    MutSlice::from_maybeuninit_mut_slice(scratch, |scratch_space| {
        MutSlice::from_mut_slice(v, |mut s| {
            let r0 = s.split_off_begin(third1);
            let r1 = s.split_off_begin(third2);
            // let r2 = s.split_off_begin(quad3);
            let r2 = s;
            physical_triple_merge(r0, r1, r2, scratch_space.assume_uninit(), is_less);
            // physical_quad_merge(r0, r1, r2, r3, scratch_space.assume_uninit(), is_less);
        });
    });
}

pub fn quad_merge<T, F: Cmp<T>>(
    v: &mut [T],
    quad1: usize,
    quad2: usize,
    quad3: usize,
    scratch: &mut [MaybeUninit<T>],
    is_less: &F,
) {
    MutSlice::from_maybeuninit_mut_slice(scratch, |scratch_space| {
        MutSlice::from_mut_slice(v, |mut s| {
            let r0 = s.split_off_begin(quad1);
            let r1 = s.split_off_begin(quad2);
            let r2 = s.split_off_begin(quad3);
            let r3 = s;
            physical_quad_merge(r0, r1, r2, r3, scratch_space.assume_uninit(), is_less);
        });
    });
}

pub fn par_double_merge<T, F: Cmp<T> + Sync>(
    v: &mut [T],
    half1: usize,
    // scratches: &[Mutex<MutSlice<'sc, BS, T, Uninit>>],
    scratches: &mut [&mut [MaybeUninit<T>]],
    is_less: &F,
) {
    MutSlice::from_slice_of_maybeuninit_mut_slice_assumed_uninit(scratches, |scratch_spaces| {
        MutSlice::from_mut_slice(v, |mut s| {
            let r0 = s.split_off_begin(half1);
            let r1 = s;
            join(
                || par_physical_merge(r0, r1, scratch_spaces, is_less),
                || (),
            );
        })
    });
}
