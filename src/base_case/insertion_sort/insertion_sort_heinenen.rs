#[allow(unused)]
pub(super) fn insertion_sort_heinenen<T, F>(v: &mut [T], is_less: &F)
where
    F: Fn(&T, &T) -> bool,
{
    for current in 1..v.len() {
        if is_less(&v[current], &v[0]) {
            v[0..=current].rotate_right(1);
        } else {
            let mut insert = current;
            while is_less(&v[insert], &v[insert - 1]) {
                v.swap(insert, insert - 1);
                insert -= 1;
            }
        }
    }
}
