use std::{mem, ptr};

/// Sorts a slice using insertion sort, which is *O*(*n*^2) worst-case.
#[allow(unused)]
pub(super) fn insertion_sort_std<T, F>(v: &mut [T], is_less: &F)
where
    F: Fn(&T, &T) -> bool,
{
    for i in 1..v.len() {
        shift_tail(&mut v[..i + 1], is_less);
    }
}

/// When dropped, copies from `src` into `dest`.
struct CopyOnDrop<T> {
    src: *const T,
    dest: *mut T,
}

impl<T> Drop for CopyOnDrop<T> {
    fn drop(&mut self) {
        // SAFETY: This is a helper class.
        //         Please refer to its usage for correctness.
        //         Namely, one must be sure that `src` and `dst` does not overlap as required by `ptr::copy_nonoverlapping`.
        unsafe {
            ptr::copy_nonoverlapping(self.src, self.dest, 1);
        }
    }
}

/// Shifts the last element to the left until it encounters a smaller or equal element.
fn shift_tail<T, F>(v: &mut [T], is_less: &F)
where
    F: Fn(&T, &T) -> bool,
{
    let len = v.len();
    // SAFETY: The unsafe operations below involves indexing without a bound check (by offsetting a
    // pointer) and copying memory (`ptr::copy_nonoverlapping`).
    //
    // a. Indexing:
    //  1. We checked the size of the array to >= 2.
    //  2. All the indexing that we will do is always between `0 <= index < len-1` at most.
    //
    // b. Memory copying
    //  1. We are obtaining pointers to references which are guaranteed to be valid.
    //  2. They cannot overlap because we obtain pointers to difference indices of the slice.
    //     Namely, `i` and `i+1`.
    //  3. If the slice is properly aligned, the elements are properly aligned.
    //     It is the caller's responsibility to make sure the slice is properly aligned.
    //
    // See comments below for further detail.
    unsafe {
        // If the last two elements are out-of-order...
        if len >= 2 && is_less(v.get_unchecked(len - 1), v.get_unchecked(len - 2)) {
            // Read the last element into a stack-allocated variable. If a following comparison
            // operation panics, `hole` will get dropped and automatically write the element back
            // into the slice.
            let tmp = mem::ManuallyDrop::new(ptr::read(v.get_unchecked(len - 1)));
            let v = v.as_mut_ptr();
            let mut hole = CopyOnDrop {
                src: &*tmp,
                dest: v.add(len - 2),
            };
            ptr::copy_nonoverlapping(v.add(len - 2), v.add(len - 1), 1);

            for i in (0..len - 2).rev() {
                if !is_less(&*tmp, &*v.add(i)) {
                    break;
                }

                // Move `i`-th element one place to the right, thus shifting the hole to the left.
                ptr::copy_nonoverlapping(v.add(i), v.add(i + 1), 1);
                hole.dest = v.add(i);
            }
            // `hole` gets dropped and thus copies `tmp` into the remaining hole in `v`.
        }
    }
}
