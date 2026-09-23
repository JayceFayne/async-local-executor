#![doc = include_str!("../README.md")]
//#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::missing_transmute_annotations)]

mod executor;
#[cfg(test)]
mod tests;
mod tls;

use crate::tls::executor;
use std::sync::mpsc;

pub use crate::executor::{Executor, JoinHandle, TaskHandle};

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
pub fn block_on<F>(future: F) -> F::Output
where
    F: IntoFuture + 'static,
{
    let (tx, rx) = mpsc::channel();
    let mut ex = Executor::new(move |task| tx.send(task).unwrap());
    ex.run_in(|| {
        let main = spawn_local(future);
        loop {
            rx.recv().unwrap().tick();
            if let Some(result) = main.result() {
                break result;
            }
        }
    })
}
