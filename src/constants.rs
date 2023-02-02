use std::cmp::max;

pub const LOG_MAX_BUCKETS: usize = 7;
pub const LOG_BLOCK_SIZE: usize = 9;
pub const BASE_CASE_SIZE: usize = 16;
pub const BASE_CASE_MULTIPLIER: usize = 8;
pub const EQUAL_BUCKET_THRESHOLD: usize = 5;
pub const ALLOW_EQUAL_BUCKETS: bool = true;
pub const OVERSAMPLING_FACTOR_PERCENT: f64 = 25.0;
pub const SINGLE_LEVEL_THRESHOLD: usize = BASE_CASE_SIZE * (1 << LOG_MAX_BUCKETS);
pub const TWO_LEVEL_THRESHOLD: usize = SINGLE_LEVEL_THRESHOLD * (1 << LOG_MAX_BUCKETS);

#[cfg(feature = "no_analyze")]
pub const ENABLE_ANALYZER: bool = false;
#[cfg(not(feature = "no_analyze"))]
pub const ENABLE_ANALYZER: bool = true;
pub const BATCH_SIZE: usize = 6;
pub const MIN_PARALLEL_BLOCKS_PER_THREAD: usize = 4;

pub const BLOCK_SIZE: usize = 1usize << LOG_BLOCK_SIZE;

/// Maximum number of buckets, with equal buckets
pub const MAX_BUCKETS: usize = 1usize << (LOG_MAX_BUCKETS + ALLOW_EQUAL_BUCKETS as usize);

pub(crate) fn log_buckets(n: usize) -> usize {
    if n <= SINGLE_LEVEL_THRESHOLD {
        let res = (n / BASE_CASE_SIZE).ilog2();
        max(1, res as usize)
    } else if n <= TWO_LEVEL_THRESHOLD {
        let res = ((n / BASE_CASE_SIZE).ilog2() + 1) / 2;
        max(1, res as usize)
    } else {
        LOG_MAX_BUCKETS
    }
}
