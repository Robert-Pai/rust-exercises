use std::cell::{Cell, RefCell};
fn main() {
  let c = Cell::new("asdf");
  let one = c.get();
  c.set("qwer");
  let two = c.get();
  println!("{},{}", one, two);

  let s = RefCell::new(String::from("hello, world"));
    let s1 = s.borrow();
    let s2 = s.borrow_mut();

    println!("{},{}", s1, s2);
// Ref：共享借用
// 创建时：


// borrow = 0  ->  1
// borrow = 1  ->  2
// 多个 Ref 可以同时存在。

// 销毁一个 Ref：


// borrow -= 1
// 源码中的逻辑相当于：


// impl Drop for BorrowRef {
//     fn drop(&mut self) {
//         let count = self.borrow.get();
//         self.borrow.replace(count - 1);
//     }
// }
// RefMut：独占借用
// 创建时只允许：


// borrow = 0  ->  -1
// 如果当前已经有任何借用，就不能创建 RefMut。

// 销毁 RefMut：


// borrow = -1  ->  0
// 源码中的逻辑相当于：


// impl Drop for BorrowRefMut {
//     fn drop(&mut self) {
//         let count = self.borrow.get();
//         self.borrow.replace(count + 1);
//     }
// }
// 因此二者使用的是同一个借用计数器，但编码和更新方向不同：


// 正数：不可变借用数量
// 0：   没有借用
// 负数：可变借用
// 能力也不同

// Ref<T>
// 只实现类似：


// Deref<Target = T>
// 所以只能读：


// let r = cell.borrow();
// println!("{}", *r);

// RefMut<T>
// 额外实现：


// DerefMut
// 所以可以写：


// let mut r = cell.borrow_mut();
// *r += 1;
// marker 的作用
// 这个字段：


// marker: PhantomData<&'b mut T>
// 确实主要是编译期作用：

// 让 RefMut 表现得像拥有一个 &mut T
// 保证生命周期关系
// 让类型对 T 保持不变性
// 不占运行时空间
// 但它并不代表 RefMut 的全部区别。

// 总结

// 内存布局：
// Ref<T>    ≈ value 指针 + borrow 指针
// RefMut<T> ≈ value 指针 + borrow 指针 + 0 字节 marker

// 运行时：
// Ref    增加/减少共享借用计数
// RefMut 进入/退出独占借用状态

// 编译期：
// Ref    类似 &T
// RefMut 类似 &mut T
// 所以应当记成：

// Ref 和 RefMut 的物理结构大体相似，但它们的运行时借用计数逻辑、Drop 行为以及可访问能力都不同；只有 PhantomData 本身是纯编译期标记。

    let mq = MsgQueue {
        msg_cache: Cell::new(0),
    };
    mq.send("hello, world".to_string());
}
pub trait Messenger {
    fn send(&self, msg: String);
}

pub struct MsgQueue {
    msg_cache: Cell<i32>,
}

impl Messenger for MsgQueue {
    fn send(&self, msg: String) {
        self.msg_cache.set(self.msg_cache.get() + 1);
    }
}
// 只需要读写一个值，不需要内部引用
//     -> Cell<T>

// 需要直接修改 T 的内部内容
//     -> RefCell<T>

// T 是 Copy
//     -> 优先 Cell<T>

// T 是 String、Vec 等复杂类型
//     -> 通常 RefCell<T>
// 例如：


// Cell<Vec<i32>>       // 可以整体替换 Vec，但不适合直接 push
// RefCell<Vec<i32>>    // 可以借用后直接 push

// let values = RefCell::new(vec![1, 2, 3]);
// values.borrow_mut().push(4);
// 一句话总结：

// Cell 主要是“整体取出/替换”；RefCell 主要是“借用后访问/修改”。Copy 只是 Cell::get() 的要求，不是 Cell<T> 本身的硬性要求。

// 指向 Sized 类型的引用通常是瘦指针；指向 ?Sized 动态大小类型（DST）的引用通常是胖指针。

// 瘦指针
// 长度或布局已经由类型确定：


// &[i32; 3]  // 地址，长度 3 在类型中
// &String    // String 自身大小固定，通常 24 字节
// &Vec<i32>  // Vec 结构体大小固定
// &Cell<[i32; 3]>
// 它们通常只需要：


// 数据地址
// 注意，String 和 Vec 虽然管理着运行时长度的堆数据，但它们自身的结构体大小是固定的，所以引用仍然是瘦指针。

// 胖指针
// 目标类型本身的大小运行时才确定：


// &[i32]          // 地址 + 元素数量
// &str            // 地址 + 字节长度
// &dyn Trait      // 地址 + vtable 地址
// &Cell<[i32]>    // 地址 + 切片长度
// 通常包含：


// 数据地址 + 元数据
// 其中元数据可能是：


// 切片：长度
// 字符串：字节长度
// trait object：vtable 地址
// 所以：


// [i32; 3]：固定大小数组
// [i32]    ：动态大小切片

// &[i32; 3]：瘦指针
// &[i32]    ：胖指针
// 一句话总结：

// 判断胖瘦不要看“它管理的数据是否动态”，而要看“被引用的类型本身是否 Sized”。String 本身是固定大小的结构体，因此 &String 是瘦指针；[T] 本身是动态大小类型，因此 &[T] 是胖指针。