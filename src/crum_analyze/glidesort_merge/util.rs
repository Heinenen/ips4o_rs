// Copyright 2023 Orson Peters
//
// Permission is hereby granted, free of charge, to any
// person obtaining a copy of this software and associated
// documentation files (the "Software"), to deal in the
// Software without restriction, including without
// limitation the rights to use, copy, modify, merge,
// publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software
// is furnished to do so, subject to the following
// conditions:

// The above copyright notice and this permission notice
// shall be included in all copies or substantial portions
// of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF
// ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
// TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
// PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
// SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
// CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
// IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.


/// Trait alias for comparison functions.
pub trait Cmp<T>: Fn(&T, &T) -> bool {}
impl<T, F: Fn(&T, &T) -> bool> Cmp<T> for F {}

#[inline]
pub fn select<T>(cond: bool, if_true: *mut T, if_false: *mut T) -> *mut T {
    // let mut ret = if_false;
    // unsafe {
    //     core::arch::asm! {
    //         "test {cond}, {cond}",
    //         "cmovnz {ret}, {if_true}",
    //         cond = in(reg) (cond as usize),
    //         if_true = in(reg) if_true,
    //         ret = inlateout(reg) ret,
    //         options(pure, nomem, nostack)
    //     };
    // }
    // ret

    // let mut res = if_false as usize;
    // cmov::cmovnz(cond as usize, if_true as usize, &mut res);
    // res as *mut T

    // let ab = [if_false, if_true];
    // ab[cond as usize]

    // let tpi = if_true as usize;
    // let fpi = if_false as usize;

    // let xor = tpi ^ fpi;
    // let cond_mask = (-(cond as isize)) as usize;
    // let xor_if_true = xor & cond_mask;
    // return (fpi ^ xor_if_true) as *mut T;

    if cond {
        if_true
    } else {
        if_false
    }
}

#[inline]
#[cold]
pub fn abort() -> ! {
    // panic!("abort called");
    #[cfg(not(feature = "unstable"))]
    {
        std::process::abort();
    }
    #[cfg(feature = "unstable")]
    {
        core::intrinsics::abort();
    }
    // unsafe { std::hint::unreachable_unchecked() }
}

#[inline(always)]
pub fn assert_abort(b: bool) {
    if !b {
        abort();
    }
}

pub trait UnwrapAbort {
    type Inner;
    fn unwrap_abort(self) -> Self::Inner;
}

impl<T> UnwrapAbort for Option<T> {
    type Inner = T;

    #[inline]
    fn unwrap_abort(self) -> Self::Inner {
        if let Some(inner) = self {
            inner
        } else {
            abort()
        }
    }
}

impl<T, E> UnwrapAbort for Result<T, E> {
    type Inner = T;

    #[inline]
    fn unwrap_abort(self) -> Self::Inner {
        if let Ok(inner) = self {
            inner
        } else {
            abort()
        }
    }
}

/// # Safety
/// Only implemented for copy types.
pub unsafe trait IsCopyType {
    fn is_copy_type() -> bool;
}

#[cfg(not(feature = "unstable"))]
unsafe impl<T> IsCopyType for T {
    fn is_copy_type() -> bool {
        false
    }
}

#[cfg(feature = "unstable")]
unsafe impl<T> IsCopyType for T {
    default fn is_copy_type() -> bool {
        false
    }
}

#[cfg(feature = "unstable")]
unsafe impl<T: Copy> IsCopyType for T {
    fn is_copy_type() -> bool {
        true
    }
}

/// # Safety
/// Only implemented for types for which we may call Ord on (soon to be
/// forgotten) copies, even if T isn't Copy.
pub unsafe trait MayCallOrdOnCopy {
    fn may_call_ord_on_copy() -> bool;
}

#[cfg(not(feature = "unstable"))]
unsafe impl<T> MayCallOrdOnCopy for T {
    fn may_call_ord_on_copy() -> bool {
        false
    }
}

#[cfg(feature = "unstable")]
unsafe impl<T> MayCallOrdOnCopy for T {
    default fn may_call_ord_on_copy() -> bool {
        false
    }
}

#[cfg(feature = "unstable")]
#[marker]
unsafe trait SafeToCall {}

#[cfg(feature = "unstable")]
unsafe impl<T: SafeToCall> MayCallOrdOnCopy for T {
    fn may_call_ord_on_copy() -> bool {
        true
    }
}

#[cfg(feature = "unstable")]
unsafe impl<T: Copy> SafeToCall for T {}

#[cfg(feature = "unstable")]
unsafe impl<T: SafeToCall> SafeToCall for (T,) {}

#[cfg(feature = "unstable")]
unsafe impl<T: SafeToCall, U: SafeToCall> SafeToCall for (T, U) {}

#[cfg(feature = "unstable")]
unsafe impl<T: SafeToCall, U: SafeToCall, V: SafeToCall> SafeToCall for (T, U, V) {}

macro_rules! impl_safetocallord {
    ($($t:ty, )*) => {
        $(
        #[cfg(feature = "unstable")]
        unsafe impl SafeToCall for $t { }
        )*
    };
}

impl_safetocallord!(String,);
