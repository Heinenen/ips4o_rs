use crate::is_less_to_compare;

mod insertion_sort_heinenen;
mod insertion_sort_jwiesler;
mod insertion_sort_std;

pub(crate) fn insertion_sort<T, F>(v: &mut [T], is_less: &F)
where
    F: Fn(&T, &T) -> bool,
{
    insertion_sort_std::insertion_sort_std(v, is_less);
    debug_assert!(v.is_sorted_by(is_less_to_compare!(is_less)));
}
