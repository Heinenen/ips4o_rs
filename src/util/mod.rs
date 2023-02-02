mod debug_assertions;

use crate::{constants::BLOCK_SIZE, storage::LocalStorage, Less, Sortable};

#[macro_export]
macro_rules! is_less_to_compare {
    ( $x:ident ) => {{
        |a, b| {
            let ord = if $x(a, b) {
                std::cmp::Ordering::Less
            } else if $x(b, a) {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Equal
            };
            Some(ord)
        }
    }};
}

#[macro_export]
macro_rules! debug {
    ($($x:tt)*) => {
        {
            #[cfg(debug_assertions)]
            {
                std::println!("{:?}", $($x)*);
            }
        }
    };
}

pub(crate) fn test_block_permutation<T, F>(v: &[T], ls: &LocalStorage<T, F>) -> bool
where
    T: Sortable,
    F: Less<T>,
{
    for i in 0..ls.num_buckets {
        let bucket_start = ls.bucket_boundaries[i] - ls.bucket_boundaries[i] % BLOCK_SIZE;
        let (write, _read) = ls.bucket_pointers[i].fetch();
        for val in v.iter().take(write).skip(bucket_start) {
            let bucket = ls.classifier.classify_single_element(val);
            if bucket != i {
                println!("block permutation failed in bucket: {bucket}");
                return false;
            }
        }
    }
    true
}

pub(crate) fn test_cleanup_margins<T, F>(v: &[T], ls: &LocalStorage<T, F>) -> bool
where
    T: Sortable,
    F: Less<T>,
{
    for i in 0..ls.num_buckets {
        for (j, val) in v
            .iter()
            .enumerate()
            .take(ls.bucket_boundaries[i + 1])
            .skip(ls.bucket_boundaries[i])
        {
            let bucket = ls.classifier.classify_single_element(val);
            if bucket != i {
                println!("cleanup margins failed in bucket {i} at index {j} with element {:?} which gets classified as bucket {bucket}", v[j]);
                return false;
            }
        }
    }
    true
}

pub(crate) fn round_up_to_block_size(x: usize) -> usize {
    ((x + BLOCK_SIZE - 1) / BLOCK_SIZE) * BLOCK_SIZE
}

#[cfg(test)]
mod tests {
    #[test]
    fn is_less_to_compare() {
        let is_less = |a, b| a < b;
        let v = [1, 2, 3];
        debug_assert!(v.is_sorted_by(is_less_to_compare!(is_less)));
        let v = [1, 1, 1];
        debug_assert!(v.is_sorted_by(is_less_to_compare!(is_less)));
        let v = [3, 2, 1];
        debug_assert!(!v.is_sorted_by(is_less_to_compare!(is_less)));
    }
}
