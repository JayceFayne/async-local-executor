use async_io::Timer;
use async_local_executor::{block_on, spawn_local};
use std::time::Duration;

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
