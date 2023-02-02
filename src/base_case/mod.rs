use crate::{is_less_to_compare, Less};

pub(crate) mod insertion_sort;

pub(crate) fn sort_simple_cases<T, F>(v: &mut [T], is_less: &F) -> bool
where
    F: Less<T>,
{
    if v.len() <= 1 {
        return true;
    }

    // If first element is smaller than last element, test if list is sorted
    if is_less(v.first().unwrap(), v.last().unwrap()) {
        for x in 1..v.len() {
            if is_less(&v[x], &v[x - 1]) {
                return false;
            }
        }
        true
    } else {
        for x in 1..v.len() {
            if is_less(&v[x - 1], &v[x]) {
                return false;
            }
        }
        v.reverse();
        true
    }
}

pub(crate) fn base_case_sort<T, F>(v: &mut [T], is_less: &F)
where
    F: Less<T>,
{
    insertion_sort::insertion_sort(v, is_less);
    debug_assert!(v.is_sorted_by(is_less_to_compare!(is_less)));
}
