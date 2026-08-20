// 声明宏
#[macro_export]
macro_rules! vec {
    ( $( $x:expr ),* ) => {
        {
            let mut temp_vec = Vec::new();
            $(
                temp_vec.push($x);
            )*
            temp_vec
        }
    };
}

trait Hello {
    fn hello(&self);
}

use hello_macro::{Hello, log_call, make_answer};

#[derive(Hello)]
struct Foo;

#[log_call]
fn add(a: i32, b: i32) -> i32 {
    a + b
}

fn main() {
    Foo.hello();
    let result = add(2, 3);
    println!("result = {}", result);

    let answer = make_answer!();
    println!("answer = {}", answer);
}
