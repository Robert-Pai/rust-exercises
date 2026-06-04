// // 这个属性用于隐藏未使用代码的警告。
// #![allow(dead_code)]

// #[derive(Debug)]
// struct Person {
//     name: String,
//     age: u8,
// }

// // 单元结构体
// struct Unit;

// // 元组结构体
// struct Pair(i32, f32);

// #[derive(Debug)]
// // 带有两个字段的结构体
// struct Point {
//     x: f32,
//     y: f32,
// }

// #[derive(Debug)]
// // 结构体可以作为另一个结构体的字段
// struct Rectangle {
//     // 可以通过指定左上角和右下角的位置
//     // 来定义一个矩形。
//     top_left: Point,
//     bottom_right: Point,
// }

// fn rect_area(rect: Rectangle) -> f32 {
//     let Rectangle { top_left: Point{
//         x: left_edge, y: top_edge
//     }, bottom_right: Point{
//         x: right_edge, y: bottom_edge
//     } } = rect;
//     let width = (right_edge - left_edge).abs();
//     let height = (top_edge - bottom_edge).abs();
//     width * height
// }

// fn square(top_left: Point, size: f32) -> Rectangle {
//     Rectangle {
//         bottom_right: Point { x: top_left.x + size, y: top_left.y - size },
//         top_left,
//     }
// }

// fn main() {
//     // 使用字段初始化简写语法创建结构体
//     let name = String::from("Peter");
//     let age = 27;
//     let peter = Person { name, age };

//     // 打印结构体的调试信息
//     println!("{:?}", peter);

//     // 实例化一个 `Point`
//     let point: Point = Point { x: 5.2, y: 0.4 };
//     let another_point: Point = Point { x: 10.3, y: 0.2 };

//     // 访问点的字段
//     println!("点的坐标：({}, {})", point.x, point.y);

//     // 使用结构体更新语法创建新的点，
//     // 复用之前创建的点的字段
//     let bottom_right = Point { x: 10.3, ..another_point };

//     // `bottom_right.y` 与 `another_point.y` 相同，
//     // 因为我们使用了 `another_point` 的该字段
//     println!("第二个点：({}, {})", bottom_right.x, bottom_right.y);

//     // 使用 `let` 绑定解构点
//     let Point { x: left_edge, y: top_edge } = point;

//     let _rectangle = Rectangle {
//         // 结构体实例化也是一个表达式
//         top_left: Point { x: left_edge, y: top_edge },
//         bottom_right: bottom_right,
//     };

//     // 计算矩形的面积
//     println!("矩形的面积：{}", rect_area(_rectangle));
//     println!("正方形的面积：{:?}", square(Point { x: 1.0, y: 1.0 }, 2.0));

//     // 实例化一个单元结构体
//     let _unit = Unit;

//     // 实例化一个元组结构体
//     let pair = Pair(1, 0.1);

//     // 访问元组结构体的字段
//     println!("pair 包含 {:?} 和 {:?}", pair.0, pair.1);

//     // 解构一个元组结构体
//     let Pair(integer, decimal) = pair;

//     println!("pair 包含 {:?} 和 {:?}", integer, decimal);
// }
// 创建一个 `enum` 来分类网页事件。注意名称和类型信息如何共同指定变体：
// `PageLoad != PageUnload` 且 `KeyPress(char) != Paste(String)`。
// 每个变体都是不同且独立的。
// enum WebEvent {
//     // `enum` 变体可以类似单元结构体（`unit-like`），
//     PageLoad,
//     PageUnload,
//     // 类似元组结构体，
//     KeyPress(char),
//     Paste(String),
//     // 或类似 C 语言的结构体。
//     Click { x: i64, y: i64 },
// }

// // 一个接受 `WebEvent` 枚举作为参数且不返回任何值的函数。
// fn inspect(event: WebEvent) {
//     match event {
//         WebEvent::PageLoad => println!("页面已加载"),
//         WebEvent::PageUnload => println!("页面已卸载"),
//         // 从 `enum` 变体内部解构 `c`。
//         WebEvent::KeyPress(c) => println!("按下了'{}'键。", c),
//         WebEvent::Paste(s) => println!("粘贴了\"{}\"。", s),
//         // 将 `Click` 解构为 `x` 和 `y`。
//         WebEvent::Click { x, y } => {
//             println!("点击坐标：x={}, y={}。", x, y);
//         },
//     }
// }
// enum VeryVerboseEnumOfThingsToDoWithNumbers {
//     Add,
//     Subtract,
// }

// impl VeryVerboseEnumOfThingsToDoWithNumbers {
//     fn run(&self, x: i32, y: i32) -> i32 {
//         match self {
//             Self::Add => x + y,
//             Self::Subtract => x - y,
//         }
//     }
// }
// // 创建类型别名
// type Operations = VeryVerboseEnumOfThingsToDoWithNumbers;

// fn main() {
//     let pressed = WebEvent::KeyPress('x');
//     // `to_owned()` 从字符串切片创建一个拥有所有权的 `String`。
//     let pasted  = WebEvent::Paste("我的文本".to_owned());
//     let click   = WebEvent::Click { x: 20, y: 80 };
//     let load    = WebEvent::PageLoad;
//     let unload  = WebEvent::PageUnload;

//     inspect(pressed);
//     inspect(pasted);
//     inspect(click);
//     inspect(load);
//     inspect(unload);
//     let x = Operations::Add;

//     println!("结果: {}", x.run(5, 1));

//     use VeryVerboseEnumOfThingsToDoWithNumbers::{Add, Subtract};
//     // 自动 `use` `Role` 内的每个名称。
//     let add = Add;
//     let subtract = Subtract;
//     println!("结果: {}", add.run(5, 1));
//     println!("结果: {}", subtract.run(5, 1));
// }
// use crate::List::*;

// enum List {
//     // Cons：包含一个元素和指向下一节点指针的元组结构体
//     Cons(u32, Box<List>),
//     // Nil：表示链表末尾的节点
//     Nil,
// }

// // 可以为枚举实现方法
// impl List {
//     // 创建空链表
//     fn new() -> List {
//         // `Nil` 的类型是 `List`
//         Nil
//     }

//     // 消耗一个链表，并返回在其头部添加新元素后的链表
//     fn prepend(self, elem: u32) -> List {
//         // `Cons` 的类型也是 List
//         Cons(elem, Box::new(self))
//     }

//     // 返回链表长度
//     fn len(&self) -> u32 {
//         // 需要对 `self` 进行匹配，因为方法的行为取决于 `self` 的变体
//         // `self` 的类型是 `&List`，而 `*self` 的类型是 `List`
//         // 匹配具体类型 `T` 优于匹配引用 `&T`
//         // 在 Rust 2018 版本后，这里可以直接使用 self，下面可以使用 tail（无需 ref）
//         // Rust 会自动推断为 &s 和 ref tail
//         // 参见 https://doc.rust-lang.org/edition-guide/rust-2018/ownership-and-lifetimes/default-match-bindings.html
//         match *self {
//             // Can't take ownership of the tail, because `self` is borrowed;
//             // instead take a reference to the tail
//             // And it's a non-tail recursive call which may cause stack overflow for long lists.
//             Cons(_, ref tail) => 1 + tail.len(),
//             // 基本情况：空链表长度为零
//             Nil => 0
//         }
//     }

//     // 返回链表的字符串表示（堆分配）
//     fn stringify(&self) -> String {
//         match *self {
//             Cons(head, ref tail) => {
//                 // `format!` 类似于 `print!`，但返回堆分配的字符串，
//                 // 而不是打印到控制台
//                 format!("{}, {}", head, tail.stringify())
//             },
//             Nil => {
//                 format!("Nil")
//             },
//         }
//     }
// }

// fn main() {
//     // 创建空链表
//     let mut list = List::new();

//     // 在头部添加一些元素
//     list = list.prepend(1);
//     list = list.prepend(2);
//     list = list.prepend(3);

//     // 显示链表的最终状态
//     println!("链表长度为：{}", list.len());
//     println!("{}", list.stringify());
// }
// 全局变量在所有其他作用域之外声明。
static LANGUAGE: &str = "Rust";
const THRESHOLD: i32 = 10;

fn is_big(n: i32) -> bool {
    // 在函数中访问常量
    n > THRESHOLD
}

fn main() {
    let n = 16;

    // 在主线程中访问常量
    println!("这是 {}", LANGUAGE);
    println!("阈值为 {}", THRESHOLD);
    println!("{} 是 {}", n, if is_big(n) { "大的" } else { "小的" });

    // 错误！不能修改 `const`。
    // THRESHOLD = 5;
    // 修复：^ 注释掉此行
}