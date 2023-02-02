use std::{cmp::max, fmt::Debug};

use portable_atomic::AtomicU128;

use crate::constants::{BLOCK_SIZE, MAX_BUCKETS};

pub(crate) type BucketPointers = [BucketPointer; MAX_BUCKETS]; // maybe replace with newtype

// #[derive(Debug, Default)]
// pub(crate) struct BucketPointerSeq {
//     write: usize,
//     read: usize,
// }

// impl BucketPointerSeq {
//     pub(crate) fn new(write: usize, read: usize) -> Self {
//         Self { write, read }
//     }

//     pub(crate) fn fetch(&self) -> (usize, usize) {
//         (self.write, self.read)
//     }

//     pub fn inc_write(&mut self) -> (usize, usize) {
//         self.write += BLOCK_SIZE;
//         (self.write, self.read)
//     }

//     pub(crate) fn dec_read(&mut self) -> Result<(usize, usize), ()> {
//         if self.read >= BLOCK_SIZE {
//             self.read -= BLOCK_SIZE;
//             Ok((self.write, self.read))
//         } else {
//             Err(())
//         }
//     }
// }

// #[derive(Default)]
// pub(crate) struct BucketPointerMutex {
//     data: Mutex<BucketPointerSeq>,
// }

// impl BucketPointerMutex {
//     pub(crate) fn new(write: usize, read: usize) -> Self {
//         Self {
//             data: Mutex::new(BucketPointerSeq::new(write, read)),
//         }
//     }

//     pub(crate) fn set(&self, write: usize, read: usize) {
//         let mut data = self.data.lock().unwrap();
//         data.write = write;
//         data.read = read;
//     }

//     pub(crate) fn fetch(&self) -> (usize, usize) {
//         self.data.lock().unwrap().fetch()
//     }

//     pub fn inc_write(&self) -> (usize, usize) {
//         self.data.lock().unwrap().inc_write()
//     }

//     pub(crate) fn dec_read(&self) -> Result<(usize, usize), ()> {
//         self.data.lock().unwrap().dec_read()
//     }
// }

// impl Debug for BucketPointerMutex {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         let data = self.data.lock().unwrap();
//         write!(f, "({} {})", data.write, data.read)
//     }
// }

#[derive(Default)]
pub(crate) struct BucketPointer {
    data: AtomicU128,
}

impl BucketPointer {
    const SHIFT: u128 = 64;
    const WRITE_MASK: u128 = -1_i64 as u128;

    pub(crate) fn new(write: usize, read: usize) -> Self {
        debug_assert_eq!(write % BLOCK_SIZE, 0);
        debug_assert_eq!(read % BLOCK_SIZE, 0);
        let data = ((read as u128) << Self::SHIFT) + write as u128;
        let data = AtomicU128::new(data);
        Self { data }
    }

    pub(crate) fn set(&self, write: usize, read: usize) {
        let val = ((read as u128) << Self::SHIFT) + write as u128;
        self.data.store(val, portable_atomic::Ordering::Relaxed);
    }

    fn write_read_from_u128(data: u128) -> (usize, usize) {
        let read = data >> Self::SHIFT;
        let write = data & Self::WRITE_MASK;
        (write as usize, max(0, read as isize) as usize)
    }

    pub(crate) fn fetch(&self) -> (usize, usize) {
        let data = self.data.load(portable_atomic::Ordering::Relaxed);
        Self::write_read_from_u128(data)
    }

    pub fn inc_write(&self) -> (usize, usize) {
        let data = self
            .data
            .fetch_add(BLOCK_SIZE as u128, portable_atomic::Ordering::Relaxed);
        let (write, read) = Self::write_read_from_u128(data);
        (write + BLOCK_SIZE, read)
    }

    pub(crate) fn dec_read(&self) -> Result<(usize, usize), ()> {
        let data = self.data.fetch_sub(
            (BLOCK_SIZE as u128) << Self::SHIFT,
            portable_atomic::Ordering::Relaxed,
        );
        let (write, mut read) = Self::write_read_from_u128(data);
        if read >= BLOCK_SIZE {
            read -= BLOCK_SIZE;
            Ok((write, read))
        } else {
            Err(())
        }
    }
}

impl Debug for BucketPointer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (write, read) = self.fetch();
        f.debug_tuple("").field(&write).field(&read).finish()
    }
}
