# async-local-executor &emsp; [![Action Badge]][actions] [![Version Badge]][crates.io] [![License Badge]][license] [![Docs Badge]][docs]

[Version Badge]: https://img.shields.io/crates/v/async-local-executor.svg
[crates.io]: https://crates.io/crates/async-local-executor
[Action Badge]: https://github.com/JayceFayne/async-local-executor/workflows/Rust/badge.svg
[actions]: https://github.com/JayceFayne/async-local-executor/actions
[License Badge]: https://img.shields.io/crates/l/async-local-executor.svg
[license]: https://github.com/JayceFayne/async-local-executor/blob/master/LICENSE.md
[Docs Badge]: https://docs.rs/async-local-executor/badge.svg
[docs]: https://docs.rs/async-local-executor

Lightweight executor for building single-threaded async runtimes.

## Why?

This crate provides an `Executor` for spawning and executing `!Send` futures without locks. It is meant to be integrated into an existing event loop and driven by that event loop whenever tasks need to make progress.

## Usage

Examples of how to use the library can be found [here](./examples).
A short example of building a runtime on top of `Executor` is shown below.

```rust
use async_io::Timer;
use async_local_executor::{spawn_local, Executor};
use std::sync::mpsc;
use std::time::Duration;

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

fn main() {
    let res = block_on(async {
        spawn_local(async {
            for _ in 0..10 {
                println!("Hello, again!");
                Timer::after(Duration::from_secs(1)).await;
            }
        })
        .detach();
        for _ in 0..3 {
            println!("Hello, world!");
            Timer::after(Duration::from_secs(1)).await;
        }
        1
    });
    assert_eq!(res, 1);
}

```

This is basically the implementation of [block_on](https://github.com/JayceFayne/async-local-executor/blob/master/src/lib.rs#L26)

## Contributing

If you find any errors in async-local-executor or just want to add a new feature feel free to [submit a PR](https://github.com/jaycefayne/async-local-executor/pulls).
