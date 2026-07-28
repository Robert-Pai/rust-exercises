// fn main() {
//     let s = String::new();

//     let update_string =   || println!("{}",s);

//     update_string();
// }

fn main() {
    let s = String::new();

    // 一个闭包实现了哪种 Fn 特征取决于该闭包如何使用被捕获的变量，而不是取决于闭包如何捕获它们
    let update_string =  move || println!("{}",s);

    exec(update_string);

    fn factory() -> Box<dyn Fn(i32) -> i32> {
        let num = 5;

        Box::new(move |x: i32| x + num)
    }

    let f = factory();

    let answer = f(1);
    assert_eq!(6, answer);
}

fn exec<F: Fn()>(f: F)  {
    f()
}