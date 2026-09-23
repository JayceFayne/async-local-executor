use super::*;

#[test]
fn simple() {
    let res = block_on(async {
        spawn_local(async {
            for _ in 0..10 {
                println!("Hello, again!");
            }
        })
        .detach();
        for _ in 0..3 {
            println!("Hello, world!");
        }
        1
    });
    assert_eq!(res, 1);
}
