use std::fmt::Display;
use std::fmt::Debug;

// 特征就是Golang的interface，C++的concepts 如果不同的类型具有相同的行为，那么我们就可以定义一个特征，然后为这些类型实现该特征
// pub trait Summary {
//     fn summarize(&self) -> String;
// }
pub struct Post {
    pub title: String, // 标题
    pub author: String, // 作者
    pub content: String, // 内容
}

// impl Summary for Post {
//     fn summarize(&self) -> String {
//         format!("文章{}, 作者是{}", self.title, self.author)
//     }
// }

pub struct Weibo {
    pub username: String,
    pub content: String
}

// impl Summary for Weibo {
//     fn summarize(&self) -> String {
//         format!("{}发表了微博{}", self.username, self.content)
//     }
// }
// fn main() {
//     let post = Post{title: "Rust语言简介".to_string(),author: "Sunface".to_string(), content: "Rust棒极了!".to_string()};
//     let weibo = Weibo{username: "sunface".to_string(),content: "好像微博没Tweet好用".to_string()};

//     println!("{}",post.summarize());
//     println!("{}",weibo.summarize());
// }

// 如果你想要为类型 A 实现特征 T，那么 A 或者 T 至少有一个是在当前作用域中定义的！

// 默认实现
pub trait Summary {
    fn summarize(&self) -> String {
        String::from("(Read more...)")
    }
}

impl Summary for Post {}

impl Summary for Weibo {
    fn summarize(&self) -> String {
        format!("{}发表了微博{}", self.username, self.content)
    }
}

// 使用特征作为函数参数
// pub fn notify(item: &impl Summary) {
//     println!("Breaking news! {}", item.summarize());
// }

// 特征约束
// 语法糖
// pub fn notify<T: Summary>(item: &T) {
//     println!("Breaking news! {}", item.summarize());
// }

// 限制同一种类型
// pub fn notify<T: Summary>(item1: &T, item2: &T) {}


//多重约束
pub fn notify(item: &(impl Summary + Display)) {}
// 另一种写法
// pub fn notify<T: Summary + Display>(item: &T) {}

// Where 约束
fn some_function<T, U>(t: &T, u: &U) -> i32
    where T: Display + Clone,
          U: Clone + Debug
{
    1
}

// 使用特征约束有条件地实现方法或特征
struct Pair<T> {
    x: T,
    y: T,
}

impl<T> Pair<T> {
    fn new(x: T, y: T) -> Self {
        Self {
            x,
            y,
        }
    }
}

impl<T: Display + PartialOrd> Pair<T> {
    fn cmp_display(&self) {
        if self.x >= self.y {
            println!("The largest member is x = {}", self.x);
        } else {
            println!("The largest member is y = {}", self.y);
        }
    }
}

// 汉书中返回特征 静态分发
fn returns_summarizable() -> impl Summary {
    Weibo {
        username: String::from("sunface"),
        content: String::from(
            "m1 max太厉害了，电脑再也不会卡",
        )
    }
}
// 无法返回不同类型
// fn returns_summarizable(switch: bool) -> impl Summary {
//     if switch {
//         Post {
//             title: String::from(
//                 "Penguins win the Stanley Cup Championship!",
//             ),
//             author: String::from("Iceburgh"),
//             content: String::from(
//                 "The Pittsburgh Penguins once again are the best \
//                  hockey team in the NHL.",
//             ),
//         }
//     } else {
//         Weibo {
//             username: String::from("horse_ebooks"),
//             content: String::from(
//                 "of course, as you probably already know, people",
//             ),
//         }
//     }
// }

trait Pilot {
    fn fly(&self);
}

trait Wizard {
    fn fly(&self);
}

struct Human;

impl Pilot for Human {
    fn fly(&self) {
        println!("This is your captain speaking.");
    }
}

impl Wizard for Human {
    fn fly(&self) {
        println!("Up!");
    }
}

impl Human {
    fn fly(&self) {
        println!("*waving arms furiously*");
    }
}
// fn main() {
//     let person = Human;
//     Pilot::fly(&person); // 调用Pilot特征上的方法
//     Wizard::fly(&person); // 调用Wizard特征上的方法
//     person.fly(); // 调用Human类型自身的方法
// }

trait Animal {
    fn baby_name() -> String;
}

struct Dog;

impl Dog {
    fn baby_name() -> String {
        String::from("Spot")
    }
}

impl Animal for Dog {
    fn baby_name() -> String {
        String::from("puppy")
    }
}

fn main() {
    println!("A baby dog is called a {}", Dog::baby_name());
    // println!("A baby dog is called a {}", Animal::baby_name());
    println!("A baby dog is called a {}", <Dog as Animal>::baby_name());
    //<Type as Trait>::function(receiver_if_method, next_arg, ...);
}
