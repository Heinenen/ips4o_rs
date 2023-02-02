use crate::{
    classifier::Classifier,
    constants::{BLOCK_SIZE, MAX_BUCKETS},
    Less, Sortable,
};

impl<'a, T, F> Classifier<'a, T, F>
where
    T: Sortable,
    F: Less<T>,
{
    pub(crate) fn test_classification(&self, v: &[T], is_less: &F) -> bool
    where
        F: Less<T>,
    {
        let splitters = &self.get_splitters();
        // returns false, if an element in list is classified into the wrong bucket, otherwise true
        for x in v {
            let bucket = self.classify_single_element(x);
            if !self.equal_buckets {
                if bucket == splitters.len() - 1 {
                    if !is_less(splitters.last().unwrap(), x) {
                        return false;
                    }
                    continue;
                }
                if is_less(&splitters[bucket], x) {
                    return false;
                }
                if bucket != 0 && !is_less(&splitters[bucket - 1], x) {
                    return false;
                }
            }
        }
        true
    }

    /// After the local classification phase a stripe should consist of correctly classified blocks followed by empty blocks
    /// In the parallel case, the empty blocks must be swapped to ends of buckets, see [parallel::empty_block_movement::move_empty_blocks]
    /// Assumes that [Self::classify_single_element] works correctly, as that should be tested seperately, see [Self::test_classification]
    pub(crate) fn test_stripe_classification(
        &self,
        stripe: &[T],
        elements_per_bucket: &[usize],
        elements_written: usize,
    ) -> bool {
        let blocks = stripe.chunks(BLOCK_SIZE);
        let mut elements_tested_per_bucket = [0; MAX_BUCKETS];
        let mut total_elements_tested = 0;
        for block in blocks {
            if elements_written == total_elements_tested {
                break;
            }
            let bucket_index = self.classify_single_element(&block[0]);
            let elem_classified_correctly = |v| self.classify_single_element(v) == bucket_index;
            let block_classified_correctly = block.iter().all(elem_classified_correctly);

            if !block_classified_correctly {
                return false;
            }
            elements_tested_per_bucket[bucket_index] += BLOCK_SIZE;
            total_elements_tested += BLOCK_SIZE;
        }
        for (elements, elements_tested) in elements_per_bucket
            .iter()
            .zip(elements_tested_per_bucket.iter())
        {
            if *elements != elements_tested + elements % BLOCK_SIZE {
                return false;
            }
        }
        true
    }
}
