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
pub fn tick() -> bool {
    if let Some(ticker) = { executor().ticker() } {
        ticker.tick();
        true
    } else {
        false
    }
}

#[inline]
pub fn exit() {
    executor().exit();
}

fn run<F>(future: F) -> F::Output
where
    F: IntoFuture + 'static,
{
    let main = spawn_local(future);
    loop {
        while tick() {
            if let Some(result) = main.result() {
                return result;
            }
        }
        thread::park();
    }
}

#[inline]
pub fn block_on<F>(future: F) -> F::Output
where
    F: IntoFuture + 'static,
{
    let thread = thread::current();
    let mut executor = Executor::new(move || thread.unpark());
    executor.run_in(|| run(future))
}
