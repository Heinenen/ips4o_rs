use std::iter::repeat;

use num_traits::{Bounded, FromPrimitive, One, PrimInt, Zero};
use rand::{
    distributions::{uniform::SampleUniform, Uniform},
    thread_rng, Rng,
};

const ARRAY_LEN: usize = 10;
pub const DISTRIBUTIONS: [&dyn Fn(usize) -> Vec<u32>; ARRAY_LEN] = [
    &uniform,
    &ones,
    &sorted,
    &reverse,
    &almost_sorted,
    &unsorted_tail,
    &exponential,
    &root_dups,
    &root_center_dups,
    &p78center_dups,
];
pub const NAMES: [&'static str; ARRAY_LEN] = [
    "uniform",
    "ones",
    "sorted",
    "reverse",
    "almost_sorted",
    "unsorted_tail",
    "exponential",
    "root_dups",
    "root_center_dups",
    "p78center_dups",
];

pub fn uniform<T>(len: usize) -> Vec<T>
where
    T: Bounded + SampleUniform,
{
    let mut rng = thread_rng();
    let fun = move || {
        let uniform = Uniform::new(T::min_value(), T::max_value());
        Some(rng.sample(uniform))
    };
    std::iter::from_fn(fun).take(len).collect()
}

pub fn ones<T>(len: usize) -> Vec<T>
where
    T: One + Clone,
{
    repeat(T::one()).take(len).collect()
}

pub fn sorted<T>(len: usize) -> Vec<T>
where
    T: PrimInt + Zero + One,
{
    let mut count = T::zero();
    let fun = move || {
        count = count + T::one();
        Some(count)
    };
    std::iter::from_fn(fun).take(len).collect()
}

pub fn reverse<T>(len: usize) -> Vec<T>
where
    T: FromPrimitive + PrimInt + Zero + One,
{
    let mut count = T::from_usize(len).unwrap();
    let fun = move || {
        count = count - T::one();
        Some(count)
    };
    std::iter::from_fn(fun).take(len).collect()
}

pub fn almost_sorted<T>(len: usize) -> Vec<T>
where
    T: PrimInt + Zero + One,
{
    let mut rng = thread_rng();
    let prob = (len as f64).sqrt() / len as f64;
    let mut v = sorted(len);
    for i in 0..v.len() - 1 {
        if rng.sample(Uniform::new(0_f64, 1_f64)) < prob {
            v[i] = v[i] + T::one();
            v[i + 1] = v[i + 1] - T::one();
        }
    }
    v
}

pub fn unsorted_tail<T>(len: usize) -> Vec<T>
where
    T: FromPrimitive + PrimInt + Zero + One + SampleUniform,
{
    let tail_len = (len as f64).powf(7_f64 / 8_f64) as usize;
    let mut rng = thread_rng();
    let mut v = sorted(len);
    for i in v.len() - tail_len..v.len() {
        v[i] = rng.sample(Uniform::new(T::min_value(), T::max_value()));
    }
    v
}

pub fn exponential<T>(len: usize) -> Vec<T>
where
    T: FromPrimitive + PrimInt + Zero + One + SampleUniform,
{
    let mut v = Vec::new();
    let log = ((len as f64).ln() / 2f64.ln()).ceil() as u64 + 1;
    let log = T::from_u64(log).unwrap();
    let mut rng = thread_rng();
    for _ in 0..len {
        let range = T::from_u64(1 << rng.gen_range(T::zero()..log).to_u64().unwrap()).unwrap();
        v.push(range + rng.gen_range(T::zero()..range));
    }
    v
}

pub fn root_dups<T>(len: usize) -> Vec<T>
where
    T: FromPrimitive + PrimInt + Zero + One + SampleUniform,
{
    let root = (len as f64).sqrt() as u64;
    let mut rng = thread_rng();
    std::iter::from_fn(|| Some(rng.gen_range(T::zero()..T::from_u64(root).unwrap())))
        .take(len)
        .collect()
}

pub fn root_center_dups<T>(len: usize) -> Vec<T>
where
    T: FromPrimitive + PrimInt + Zero + One + SampleUniform,
{
    let mut v = Vec::new();
    let log = ((len as f64).ln() / 2f64.ln() - 1f64) as usize;
    let two = T::one() + T::one();
    let mut rng = thread_rng();
    for _ in 0..len {
        let x = rng.gen_range(T::min_value()..T::max_value());
        v.push((x * x + (T::one() << log)) % (two << log))
    }
    v
}

pub fn p78center_dups<T>(len: usize) -> Vec<T>
where
    T: FromPrimitive + PrimInt + Zero + One + SampleUniform,
{
    let mut v = Vec::new();
    let log = ((len as f64).ln() / 2f64.ln() - 1f64) as usize;
    let two = T::one() + T::one();
    let mut rng = thread_rng();
    for _ in 0..len {
        let r = rng.gen_range(T::min_value()..T::max_value());
        let x = (r * r) % (two << log);
        v.push((x * x + (T::one() << log)) % (two << log));
    }
    v
}
