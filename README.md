# Maker

基于 Rust 的 Binance USD-M Futures 多层级只做挂单（post-only）做市程序。

该程序使用真实 API 凭据并提交实盘订单。启动、正常关闭、流断开以及恢复时，
程序都会取消**配置交易品种的所有未完成订单**，包括其他程序创建的订单。普通的
流恢复会先解析已知订单并保留本地网格用途；交易规则发生变化时，则会显式重建网格。

## 行为

初始买卖价阶梯围绕当前最优买价和最优卖价的中点构建。所有价格距离都以交易所
最小价格变动单位（tick）配置。

当一个 Quote 卖方价位完全成交时，引擎会在
`filled_price - strategy.take_profit_ticks` 创建一个 bid TakeProfit 价位。当一个
Quote 买方价位成交时，引擎会在 `filled_price + strategy.take_profit_ticks` 创建镜像的
ask TakeProfit 价位。如果交易所中已经存在同方向、同价格的订单，则只会将其本地网格
用途从 `Quote` 改为 `TakeProfit`，不会发送远程取消或替换请求。缺失的 TakeProfit 会被
提交，同时移除该方向最远的 Quote，以保持配置的价位数量。

当 TakeProfit 成交时，该网格转换完成，并且只恢复远端方向的 Quote。`Quote`/
`TakeProfit` 是网格元数据，与交易所订单状态相互独立。部分成交不会回滚网格。
私有 Binance WebSocket 驱动成交处理；周期性协调器只负责修复失败的订单变更。
下单、取消单个订单以及查询状态不确定的订单使用 Binance USD-M WebSocket API。
按品种取消所有订单使用带签名的 REST `DELETE /fapi/v1/allOpenOrders` 接口，
因为 USD-M WebSocket API 没有提供取消全部订单的方法。REST 还负责交易所元数据、
初始快照、持仓模式校验、时钟同步以及 listen-key 生命周期管理。

交易规则会在构建第一个网格之前加载。随后，适配器会在只包含一个配置品种的
move-only 策略会话中保存该快照，并每隔 `runtime.instrument_refresh_interval_secs`
获取候选更新。如果规则发生变化，程序会取消该品种的订单并重建网格，避免旧的 tick
或 step 语义继续生效。

配置项 `levels_per_side` 同时是每个目标价阶梯的硬性容量。如果某一侧没有剩余的
Quote 价位来替换新的成交，或者待处理的止盈转换会使价阶梯失效，引擎会停止并取消
该品种的订单，而不是创建无界仓位。请为计划中的库存范围配置足够的价位数量。

## 配置

创建实盘配置并限制其权限：

```sh
cp config.example.toml config.toml
chmod 600 config.toml
```

设置一个拥有 USD-M Futures 交易权限的 Binance Ed25519 API key，并将其 PKCS#8 PEM
私钥放入 `exchange.private_key_pem`。应保持提现权限关闭，并使用 IP 白名单。

所有运行时设置和两项凭据都放在 `config.toml` 中；程序不会读取环境变量。真实配置
已被 Git 忽略。WebSocket 空闲超时用于保证连接连续性。当任一关键流停止产生事件时，
引擎会解析所有已知订单，保留当前网格版本及其 Quote/TakeProfit 用途，取消该品种
剩余的所有订单，然后重建订阅。交易规则变化仍会重建一个全新的网格。

日志写入配置的 `logging.directory`（默认为 `logs/`）。`maker.log.YYYY-MM-DD`
文件通过异步写入器接收结构化 JSON；`logging.stdout = true` 会额外启用紧凑的终端
输出。进程启动时会删除早于 `logging.retention_days` 的文件。每隔
`logging.telemetry_interval_secs`，CLI 会报告事件/请求数量、距上一个事件的时间以及
延迟分布（样本数、平均值、p50 桶上界、p99 桶上界和最大值）。同一心跳还会包含引擎
阶段、当前买卖 tick、活跃和进行中的生命周期数量、成交数、下单/取消结果以及恢复/
重建计数器。

对于传入的公共市场事件或私有用户事件，`t0` 在网络线程收到 WebSocket 文本帧时记录。
如果该事件触发订单请求，策略提交请求时记录 `t1`，交易 WebSocket 的发送 future
完成后记录 `t2`。`*_strategy_reaction` 为 `t1 - t0`，`event_request_dispatch_send`
为 `t2 - t1`，`*_receive_to_send` 为 `t2 - t0`；这些指标不包含等待响应的时间。
由启动、定时器、恢复或 Binance 内部重试触发的请求会单独统计，不会生成策略反应样本。
此外，系统会将 Binance 的 `E` 事件时间和 `T` 事务时间与本地接收墙上时钟进行比较。
当主机时钟领先或落后于 Binance 时，这些外部时钟指标出现负值是正常的。

专用的策略和网络运行路径只更新固定的原子计数器和直方图，不调用 tracing，也不执行
同步输出。生命周期和遥测格式化位于受覆盖的低延迟边界之外。API key、私钥和签名
绝不会写入日志。

## 运行

```sh
cargo run --release -p maker-cli -- --config config.toml
```

程序没有模拟运行或实盘确认开关。按 `Ctrl-C` 可正常关闭程序并取消该品种的全部订单。

## 架构

```text
maker-domain            精确数值和纯函数式滚动网格转换
maker-ports             与交易所无关的异步能力
maker-engine            单所有者生命周期和协调循环
exchange-binance-usdm   Binance REST 适配器及隔离的网络运行时
maker-cli               配置和依赖组合
```

Binance 适配器拥有两个相互隔离的网络操作系统线程，每个线程都运行一个
current-thread Tokio 运行时。`maker-network-market-data` 负责交易品种元数据、
book-ticker REST 以及公开的 book-ticker WebSocket。`maker-network-trading` 负责持仓
模式、私有用户数据流和 listen-key 保活、交易 WebSocket API 请求以及时钟同步。
两者共享同一个可克隆的 REST 客户端状态，但公共市场的突发流量不会占用延迟关键的
私有/交易运行时的调度时间。

`runtime.market_data_mode` 和 `runtime.trading_mode` 可以独立选择
`"event_driven"` 或 `"busy_spin"`。当没有就绪的 I/O 或定时器时，事件驱动运行时
会在 Tokio 中休眠。忙等待运行时会持续自行唤醒并占用一个 CPU 核心，以降低本地唤醒
抖动，但不会绕过 Tokio、套接字就绪机制或内核 TCP 栈。对应的 CPU 核心设置与所选
执行模式保持独立。

引擎拥有一个独立的 `maker-strategy` 操作系统线程和一个 current-thread Tokio 运行时。
策略线程与两个网络线程之间的周期性 FIFO 通信使用固定容量的 SPSC 环：每个网络任务
收件箱容量为 64，交易 WebSocket API 命令容量为 256，有序私有订单更新容量为 256。
只可移动的交易所会话直接拥有每个原始 SPSC 生产者，因此 Rust 所有权保证物理生产者
恰好只有一个，无需锁或运行时所有权守卫。队列饱和时绝不会阻塞或覆盖数据：任务/命令
接纳会立即失败；私有订单事件饱和时会先排空已接收的事件，然后强制恢复流。公共 BBO
采用不同方式：市场线程写入双槽最新值邮箱，策略线程直接读取最新的一致快照。

策略支持每侧 1--64 个网格价位。订单生命周期状态使用 256 个固定槽位，由引擎生成的
客户端订单 ID 的低字节选择；每次访问还会校验完整 ID，因此槽位复用不会接受过期事件。
协调和恢复会复用固定的内联临时缓冲区，容量耗尽时绝不会驱逐生命周期状态，而是停止
新的报价并进入现有的恢复和按品种取消全部订单路径。交易 WebSocket worker 独立使用
256 个固定传输请求槽位，因为重试和取消全部订单请求与客户端订单 ID 并非一一对应。

`runtime.strategy_mode` 可以独立控制策略运行时，取值同样为 `"event_driven"` 和
`"busy_spin"`。省略模式设置会保留当前的忙等待行为。示例配置让策略保持忙等待，
同时让两个网络运行时采用事件驱动。策略、市场数据和交易 CPU 选择器由
`core_affinity` 解析；它们应指定不同的逻辑 CPU。在四核 Linux 低延迟主机上，示例
为策略保留核心 1，为私有交易保留核心 2，为公共市场数据保留核心 3，并将核心 0
留给操作系统、日志和内核网络工作。在 Linux 上，每个选择器按允许进程使用的 CPU
进行索引，并设置硬性的单 CPU 亲和性掩码。由于项目只使用安全 Rust，且 Mach 亲和性
标签无法提供硬 CPU 固定，因此 macOS 上会拒绝 CPU 亲和性配置。

无阻塞保证适用于 `maker-strategy`、`maker-network-market-data` 和
`maker-network-trading` 上执行的项目自有生产代码：这些路径没有项目 mutex、读写锁、
阻塞式通道操作、watch/MPSC 控制通道或同步输出调用。配置为事件驱动的运行时可以在
等待 I/O 或定时器时停在 Tokio 内部；有界 SPSC 环、原子操作、`AtomicWaker`、有界的
启动自旋以及异步套接字/HTTP I/O 都是允许的。Tokio、reqwest/hyper、rustls、DNS、
内存分配器、操作系统和其他依赖项内部的同步不属于本项目的证明边界。源代码策略
集成测试会拒绝受覆盖生产模块中的禁用项目原语。

## 验证

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```
