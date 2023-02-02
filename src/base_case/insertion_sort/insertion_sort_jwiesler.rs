#[allow(unused)]
pub(super) fn insertion_sort_jwiesler<T, F>(v: &mut [T], is_less: &F)
where
    T: Copy,
    F: Fn(&T, &T) -> bool,
{
    for i in 1..v.len() {
        let value = v[i];
        if is_less(&value, &v[0]) {
            // copy everything to the right by 1
            v.copy_within(0..i, 1);
            v[0] = value;
        } else {
            // make space
            let mut cur = i;
            let mut next = i - 1;
            while is_less(&value, &v[next]) {
                v[cur] = v[next];
                cur = next;
                next -= 1;
            }
            // place the value
            v[cur] = value;
        }
    }
}
