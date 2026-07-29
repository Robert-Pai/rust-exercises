// 规则一：每个输入引用获得独立生命周期
// 规则二：只有一个输入生命周期时，赋给所有省略的输出引用
// 规则三：方法存在 &self 或 &mut self 时，输出生命周期默认绑定到 self


// 这个结构体不能比它引用的数据活得更久。
struct Excerpt<'a> {
    text: &'a str,
}

// 返回值和结构体活得一样长 而不是self引用
impl<'a> Excerpt<'a> {
    fn text<'b>(&'b self) -> &'b str {
        self.text
    }
}

// &'static T 表示一个生命周期为整个程序的引用：
// T: 'static 表示 T 内部不包含短生命周期引用。

// 生命周期约束：'a: 'b

// &mut *生命周期结束自动drop

fn main() {
    let r;
    let x = 5;            // ----------+-- 'b
                          //           |
    r = &x;           // --+-- 'a  |
                          //   |       |
    println!("r: {}", r); //   |       |
                          // --+       |                     
}// ----------+
