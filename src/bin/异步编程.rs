// `block_on`会阻塞当前线程直到指定的`Future`执行完成，这种阻塞当前线程以等待任务完成的方式较为简单、粗暴，
// 好在其它运行时的执行器(executor)会提供更加复杂的行为，例如将多个`future`调度到同一个线程上执行。
// use futures::executor::block_on;


// async fn hello_world() {
//     hello_cat().await;
//     println!("hello, world!");
// }

// async fn hello_cat() {
//     println!("hello, kitty!");
// }

// fn main() {
//     // hello_world().await;
//     let future = hello_world(); // 返回一个Future, 因此不会打印任何输出
//     block_on(future); // 执行`Future`并等待其运行完成，此时"hello, world!"会被打印输出
// }

use tokio::time::{sleep, Duration};

struct Song {
    author: String,
    name: String,
}

async fn learn_song() -> Song {
    Song {
        author: "曲婉婷".to_string(),
        name: String::from("《我的歌声里》"),
    }
}

async fn sing_song(song: Song) {
    println!(
        "给大家献上一首{}的{} ~ {}",
        song.author, song.name, "你存在我深深的脑海里~ ~"
    );
}

async fn dance() {
    println!("唱到情深处，身体不由自主的动了起来~ ~");
}

async fn learn_and_sing() {
    let song = learn_song().await;
    sing_song(song).await;
}

async fn async_main() {
    let f1 = learn_and_sing();
    let f2 = dance();
    futures::join!(f1, f2);
}

#[tokio::main]
async fn main() {
    async_main().await;
    parent().await;
}

async fn child_a() {
    sleep(Duration::from_secs(3)).await;
    println!("child_a done");
}

async fn child_b() {
    sleep(Duration::from_secs(2)).await;
    println!("child_b done");
}

// Executor poll 父 Task
//     ↓
// poll 父 Future
//     ↓
// 父 Future 当前停在 join! 状态
//     ↓
// join! poll A
//     ↓
// join! poll B
//     ↓
// 都完成 → Ready
// 有未完成 → Pending

// 网络事件到达
//     ↓
// A 对应的底层 Future 调用 Waker
//     ↓
// 父 Task 重新进入 Executor 运行队列
//     ↓
// 某个 worker 线程 poll 父 Future
//     ↓
// 父 Future 回到 join! 状态
//     ↓
// join! 再次 poll 未完成的 A、B
async fn parent() {
    println!("parent start");

    tokio::join!(
        child_a(),
        child_b(),
    );

    println!("parent end");
}

// 第一次 poll parent：
//     打印 parent start
//     poll A → 注册定时器 → Pending
//     poll B → 注册定时器 → Pending
//     parent → Pending

// 线程去运行其他 Task

// 2 秒后 B 唤醒 Parent Task：
//     poll parent
//     回到 join 状态
//     poll B → Ready
//     poll A → 仍然 Pending
//     parent → Pending

// 3 秒后 A 唤醒 Parent Task：
//     poll parent
//     回到 join 状态
//     poll A → Ready
//     B 已完成，不需要重新执行
//     join → Ready
//     打印 parent end
//     parent → Ready