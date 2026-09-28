#![doc = include_str!("../README.md")]
//#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::missing_transmute_annotations)]

mod executor;
#[cfg(test)]
mod tests;
mod tls;

use crate::tls::executor;
use std::thread;

pub use crate::executor::{Executor, JoinHandle};
pub use crate::tls::EnterGuard;

#[inline]
pub fn spawn_local<F>(future: F) -> JoinHandle<F::Output>
where
    F: IntoFuture + 'static,
{
    #[cfg(feature = "tokio")]
    let future = async_compat::Compat::new(future.into_future());
    executor().spawn_local(future.into_future())
}

#[inline]
pub fn run_ready_tasks() {
    while let Some(ticker) = { executor().ticker() } {
        ticker.tick();
    }
}

#[inline]
pub fn exit() {
    executor().exit();
}

#[inline]
pub fn block_on<F>(future: F) -> F::Output
where
    F: IntoFuture + 'static,
{
    let thread = thread::current();
    let mut executor = Executor::new(move || thread.unpark());
    let _guard = executor.enter();
    let main = spawn_local(async move {
        let result = future.await;
        exit();
        result
    });
    loop {
        run_ready_tasks();
        if let Some(result) = main.result() {
            return result;
        }
        thread::park();
    }
}
