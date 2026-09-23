use crate::executor::Executor;
use std::cell::Cell;
use std::ops::{Deref, DerefMut};
use std::{mem, ptr};

thread_local! {
     static EXECUTOR: Cell<Option<&'static mut Executor>> = const { Cell::new(None) };
}

#[must_use = "deref in order to access the executor"]
pub struct ExecutorGuard {
    executor: &'static mut Executor,
}

impl Drop for ExecutorGuard {
    fn drop(&mut self) {
        let executor = unsafe { ptr::read(&raw const self.executor) };
        EXECUTOR.set(Some(executor));
    }
}

impl Deref for ExecutorGuard {
    type Target = Executor;

    fn deref(&self) -> &Self::Target {
        self.executor
    }
}

impl DerefMut for ExecutorGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.executor
    }
}

pub fn try_executor() -> Option<ExecutorGuard> {
    Some(ExecutorGuard {
        executor: EXECUTOR.try_with(Cell::take).ok()??,
    })
}

#[cold]
const fn no_executor<T>() -> T {
    panic!("no executor present");
}

pub fn executor() -> ExecutorGuard {
    try_executor().unwrap_or_else(no_executor)
}

impl Executor {
    #[inline]
    pub fn run_in<O, F: FnOnce() -> O>(&mut self, fun: F) -> O {
        let prev = EXECUTOR.replace(Some(unsafe { mem::transmute(self) }));
        let ret = fun();
        EXECUTOR.replace(prev).unwrap_or_else(no_executor);
        ret
    }
}
