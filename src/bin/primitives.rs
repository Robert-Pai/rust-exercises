// fn main() {
//     // 变量可以被类型标注。
//     let logical: bool = true;

//     let a_float: f64 = 1.0;  // 常规标注
//     let an_integer   = 5i32; // 后缀标注

//     // 或者使用默认类型。
//     let default_float   = 3.0; // `f64`
//     let default_integer = 7;   // `i32`

//     // 类型也可以从上下文中推断。
//     let mut inferred_type = 12; // 从另一行推断出类型为 i64。
//     inferred_type = 4294967296i64;

//     // 可变变量的值可以改变。
//     let mut mutable = 12; // 可变的 `i32`
//     mutable = 21;

//     // 报错！变量的类型不能改变。
//     // mutable = true;

//     // 变量可以通过遮蔽（shadowing）来覆盖。
//     let mutable = true;

//     /* 复合类型 - 数组和元组 */

//     // 数组的签名由类型 T 和长度组成，表示为 [T; length]。
//     let my_array: [i32; 5] = [1, 2, 3, 4, 5];

//     // 元组是不同类型值的集合，
//     // 使用圆括号 () 构造。
//     let my_tuple = (5u32, 1u8, true, -5.04f32);
// // }
// fn main() {
//     // 整数加法
//     println!("1 + 2 = {}", 1u32 + 2);

//     // 整数减法
//     println!("1 - 2 = {}", 1i32 - 2);
//     // TODO ^ 尝试将 `1i32` 改为 `1u32`，看看为什么类型很重要

//     // 科学记数法
//     println!("1e4 is {}, -2.5e-3 is {}", 1e4, -2.5e-3);

//     // 短路布尔逻辑
//     println!("true AND false is {}", true && false);
//     println!("true OR false is {}", true || false);
//     println!("NOT true is {}", !true);

//     // 位运算
//     println!("0011 AND 0101 is {:04b}", 0b0011u32 & 0b0101);
//     println!("0011 OR 0101 is {:04b}", 0b0011u32 | 0b0101);
//     println!("0011 XOR 0101 is {:04b}", 0b0011u32 ^ 0b0101);
//     println!("1 << 5 is {}", 1u32 << 5);
//     println!("0x80 >> 2 is 0x{:x}", 0x80u32 >> 2);

//     // 使用下划线来提高可读性！
//     println!("One million is written as {}", 1_000_000u32);
// // }
// // 元组可以用作函数参数和返回值。
// fn reverse(pair: (i32, bool)) -> (bool, i32) {
//     // `let` 可以用来把元组的成员绑定到变量。
//     let (int_param, bool_param) = pair;

//     (bool_param, int_param)
// }

// // 以下结构体将用于后续练习。
// #[derive(Debug)]
// struct Matrix(f32, f32, f32, f32);


// impl std::fmt::Display for Matrix {
//     fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
//         write!(f, "({} {})\n({} {})", self.0, self.1, self.2, self.3)
//     }
// }

// fn transpose(matrix: Matrix) -> Matrix {
//     Matrix(matrix.0, matrix.2, matrix.1, matrix.3)
// }

// fn main() {
//     // 一个包含多种不同类型的元组。
//     let long_tuple = (1u8, 2u16, 3u32, 4u64,
//                       -1i8, -2i16, -3i32, -4i64,
//                       0.1f32, 0.2f64,
//                       'a', true);

//     // 可以使用元组索引来提取元组中的值。
//     println!("长元组的第一个值：{}", long_tuple.0);
//     println!("长元组的第二个值：{}", long_tuple.1);

//     // 元组可以作为元组的成员。
//     let tuple_of_tuples = ((1u8, 2u16, 2u32), (4u64, -1i8), -2i16);

//     // 元组是可打印的。
//     println!("元组的元组：{:?}", tuple_of_tuples);

//     // 但长元组（超过 12 个元素）无法打印。
//     //let too_long_tuple = (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13);
//     //println!("过长的元组：{:?}", too_long_tuple);
//     // TODO ^ 取消上面两行的注释以查看编译器错误

//     let pair = (1, true);
//     println!("对组是 {:?}", pair);

//     println!("反转后的对组是 {:?}", reverse(pair));

//     // 创建单元素元组时，需要使用逗号来区分它们
//     // 与被括号包围的字面量。
//     println!("单元素元组：{:?}", (5u32,));
//     println!("仅是一个整数：{:?}", (5u32));

//     // 可以通过解构元组来创建绑定。
//     let tuple = (1, "hello", 4.5, true);

//     let (a, b, c, d) = tuple;
//     println!("{:?}、{:?}、{:?}、{:?}", a, b, c, d);

//     let matrix = Matrix(1.1, 1.2, 2.1, 2.2);
//     println!("{}", matrix);
//     println!("转置后的矩阵：\n{}", transpose(matrix));
// }
use std::mem;

// 此函数借用一个切片。
fn analyze_slice(slice: &[i32]) {
    println!("切片的第一个元素：{}", slice[0]);
    println!("切片有 {} 个元素", slice.len());
}

fn main() {
    // 固定大小的数组（类型签名是多余的）。
    let xs: [i32; 5] = [1, 2, 3, 4, 5];

    // 所有元素可以初始化为相同的值。
    let ys: [i32; 500] = [0; 500];

    // 索引从 0 开始。
    println!("数组的第一个元素：{}", xs[0]);
    println!("数组的第二个元素：{}", xs[1]);

    // `len` 返回数组中元素的数量。
    println!("数组中的元素数量：{}", xs.len());

    // 数组是栈分配的。
    println!("数组占用 {} 字节", mem::size_of_val(&xs));

    // 数组可以自动借用为切片。
    println!("将整个数组借用为切片。");
    analyze_slice(&xs);

    // 切片可以指向数组的一部分。
    // 它们的形式是 [起始索引..结束索引]。
    // `起始索引` 是切片中的第一个位置。
    // `结束索引` 是切片中最后一个位置的后一个位置。
    println!("借用数组的一部分作为切片。");
    analyze_slice(&ys[1 .. 4]);

    // 空切片 `&[]` 的示例：
    let empty_array: [u32; 0] = [];
    assert_eq!(&empty_array, &[]);
    assert_eq!(&empty_array, &[][..]); // 同样的操作，但更详细

    // 可以使用 `.get` 安全地访问数组，它返回一个 `Option`。
    // 可以像下面这样对其进行匹配，或者使用 `.expect()`。
    // 如果你希望程序在访问越界时优雅地退出而不是继续执行，
    // 可以使用 `.expect()`。
    for i in 0..xs.len() + 1 { // 糟糕，访问超出了数组范围！
        match xs.get(i) {
            Some(xval) => println!("{}: {}", i, xval),
            None => println!("慢着！{} 超出范围了！", i),
        }
    }

    // 使用常量值对数组的越界索引会导致编译时错误。
    //println!("{}", xs[5]);
    // 对切片的越界索引会导致运行时错误。
    //println!("{}", xs[..][5]);
}