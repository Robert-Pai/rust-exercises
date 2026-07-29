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

use hello_macro::Hello;

#[derive(Hello)]
struct Foo;

fn main() {
    Foo.hello();
}
