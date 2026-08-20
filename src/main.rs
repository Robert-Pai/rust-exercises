
// fn main() {
//     // 在控制台打印文本。
//     println!("Hello World!");
//     println!("I'm a Rustacean!")
// }

// fn main() {
//     // 通常，`{}` 会被自动替换为任何参数。
//     // 这些参数会被转换为字符串。
//     println!("{} 天", 31);

//     // 可以使用位置参数。在 `{}` 中指定一个整数
//     // 来决定替换哪个额外的参数。参数编号
//     // 从格式字符串后立即开始，从 0 开始。
//     println!("{0}，这是 {1}。{1}，这是 {0}", "Alice", "Bob");

//     // 还可以使用命名参数。
//     println!("{subject} {verb} {object}",
//              object="那只懒惰的狗",
//              subject="那只敏捷的棕色狐狸",
//              verb="跳过");

//     // 在 `:` 后指定格式字符，
//     // 可以调用不同的格式化方式。
//     println!("十进制：               {}",   69420); // 69420
//     println!("二进制：               {:b}", 69420); // 10000111100101100
//     println!("八进制：               {:o}", 69420); // 207454
//     println!("十六进制：             {:x}", 69420); // 10f2c

//     // 可以指定宽度来右对齐文本。这将输出
//     // "    1"。（四个空格和一个 "1"，总宽度为 5。）
//     println!("{number:>5}", number=1);

//     // 可以用额外的零来填充数字，
//     println!("{number:0>5}", number=1); // 00001
//     // 通过翻转符号来左对齐。这将输出 "10000"。
//     println!("{number:0<5}", number=1); // 10000

//     // 在格式说明符后添加 `$` 可以使用命名参数。
//     println!("{number:0>width$}", number=1, width=6);

//     // Rust 甚至会检查使用的参数数量是否正确。
//     println!("我的名字是 {0} {1} {0}", "Bond", "James");
//     // FIXME ^ 添加缺失的参数："James"

//     // 只有实现了 fmt::Display 的类型才能用 `{}` 格式化。
//     // 用户定义的类型默认不实现 fmt::Display。

//     #[allow(dead_code)] // 禁用 `dead_code`，它会警告未使用的模块
//     struct Structure(i32);

//     // 这无法编译，因为 `Structure` 没有实现 fmt::Display。
//     // println!("这个结构体 `{}` 无法打印...", Structure(3));
//     // TODO ^ 尝试取消注释这一行

//     // 在 Rust 1.58 及以上版本，你可以直接从周围的变量捕获参数。
//     // 就像上面一样，这将输出 "    1"，4 个空格和一个 "1"。
//     let number: f64 = 1.0;
//     let width: usize = 5;
//     println!("{number:>width$}");
//     let pi = 3.141592;
//     println!("{pi:>.3}");
// }

// #[allow(dead_code)]
// // 为 `Structure` 派生 `fmt::Debug` 实现。`Structure` 是一个包含单个 `i32` 的结构体。
// #[derive(Debug)]
// struct Structure(i32);

// // 在 `Deep` 结构体中放入一个 `Structure`。使其也可打印。
// #[derive(Debug)]
// #[allow(dead_code)]
// struct Deep(Structure);

// #[derive(Debug)]
// #[allow(dead_code)]
// struct Person<'a> {
//     name: &'a str,
//     age: u8
// }

// fn main() {
//     // 使用 `{:?}` 打印类似于使用 `{}`。
//     println!("{:?} 个月在一年中。", 12);
//     println!("{1:?} {0:?} 是这个 {actor:?} 的名字。",
//              "Slater",
//              "Christian",
//              actor="演员");

//     // `Structure` 现在可以打印了！
//     println!("现在 {:?} 将会打印！", Structure(3));

//     // `derive` 的问题是无法控制输出的样式。
//     // 如果我只想显示一个 `7` 怎么办？
//     println!("现在 {:?} 将会打印！", Deep(Structure(7)));
    
//     let name = "Peter";
//     let age = 27;
//     let peter = Person { name, age };

//     // 美化打印
//     println!("{:#?}", peter);
// }
// 通过 `use` 导入 `fmt` 模块使其可用。
// use std::fmt;

// // 定义一个结构体，我们将为其实现 `fmt::Display`。
// // 这是一个名为 `Structure` 的元组结构体，包含一个 `i32`。
// struct Structure(i32);

// // 要使用 `{}` 标记，必须为该类型手动实现 `fmt::Display` trait。
// impl fmt::Display for Structure {
//     // 这个 trait 要求 `fmt` 方法具有确切的签名。
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         // 将第一个元素严格写入提供的输出流 `f`。
//         // 返回 `fmt::Result`，表示操作是否成功。
//         // 注意 `write!` 的语法与 `println!` 非常相似。
//         write!(f, "{}", self.0)
//     }
// }

// fn main() {
//     let s = Structure(3);
//     println!("This struct `{}` won't print...", s);
//     // TODO ^ 尝试使用 `{}` 来打印 `Structure` 实例。
// // }
use std::fmt::{self, Formatter, Display};
#[derive(Debug)]
struct Color {
    red: u8,
    green: u8,
    blue: u8,
}

impl Display for Color {
    // `f` 是一个缓冲区，此方法必须将格式化的字符串写入其中。
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        // `write!` 类似于 `format!`，但它会将格式化后的字符串
        // 写入一个缓冲区（第一个参数）。
        write!(f, "RGB ({}, {}, {}) 0x{:06X}", self.red, self.green, self.blue, (self.red as u32) << 16 | (self.green as u32) << 8 | (self.blue as u32))
    }
}
fn main() {
    for color in [
        Color { red: 128, green: 255, blue: 90 },
        Color { red: 0, green: 3, blue: 254 },
        Color { red: 0, green: 0, blue: 0 },
    ] {
        // 一旦你为 fmt::Display 添加了实现，就把这里改成使用 {}。
        println!("{}", color);
    }
}