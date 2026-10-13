# Fleet Server 流量优先级与过载保护 - 实施计划

实施总览（依赖顺序）：

```text
T1 traffic 原语包（Class/ctx/Gate 快照闸门）
├─ T2 配置段 traffic_priority（ucfg/Validate/默认派生）
├─ T9 指标族与采样器骨架（消费 Gate 统计）
├─ T3 bulk 类别载体（Opt/block/multiOp/ctx 继承）
│  └─ T4 bulk 双通道+优先级调度+flush 闸+Reconfigure
│     └─ T6 服务器 ctx 路径打 control 标（checkin.Bulk/monitor/gc）
├─ T5 es RoundTripper 类别闸（含重试保类、哨兵错误）
│  └─ T6（同上）
├─ T7 HTTP 入口中间件+单例注入+429 映射（含 OpAMP）
│  └─ T8 fleet 装配与热更新分支（不重启替换快照）
│     └─ T10 内部 /debug/traffic-priority 快照端点
└─ T11 文档/reference.yml/changelog
   └─ T12 全量验证（check:all + test:unit -race + bench）
```

## Task 1: 新增 traffic 包：类别、ctx 传播、优先级保留容量闸
- **Status**： `completed`
- **Completion Evidence**：
  - 新增 internal/pkg/traffic/{class.go,snapshot.go,gate.go,gate_test.go}；`go vet` 与 `go test -race -count=1 ./internal/pkg/traffic/` 通过（3.7s）。
  - TR-1.1~1.6 由确定性并发测试覆盖（control 优先释放、借用冻结、floor 保护、超时、取消、waiter 上限、TryAcquire、Apply 替换/失效/禁用、disabled no-op）。
  - TR-1.7 rubric 自评 5/5：16 协程混合抢占 + 每 20ms 并发 Apply 压测在 -race 下无竞态，结束后在途/等待 gauge 归零。
- **Priority**： high
- **Depends On**： None
- **Description**：
  - 新建 `internal/pkg/traffic/`：
    - `Class` 类型（`Control`/`Background`），字符串化与 label 安全；`FromContext`/`WithClass`（未导出 `struct{}` key，仿 logger/http.go:94 惯例），ctx 缺失返回零值并由调用方按 control 默认处理。
    - 不可变 `Snapshot`：`Enabled bool`、`Capacity int64`、`ReservedControl int64`、`BackgroundWaitTimeout time.Duration`、`MaxBackgroundWaiters int`（0=不限等待者），校验方法。
    - `Gate`：持有 `atomic.Pointer[Snapshot]`；每类别 FIFO 等待队列（channel/cond），在途计数、借用计数（background 超出 C−R 部分）、等待者计数；`Acquire(ctx, class) (Ticket,error)`（ctx 可取消、background 超时返回哨兵错误）、`TryAcquire(class) (Ticket,bool)`、`Apply(*Snapshot) error`（先校验后原子替换，旧 Ticket 绑旧快照 release）、`Stats() Stats`。
    - 准入不变量：① control 等待者存在时，background 不得领取借用；② 释放份额时 control 等待队优先于 background 等待队；③ background 永远可进入保底 C−R；④ `Enabled=false` 时 Acquire/TryAcquire 走 no-op 快路径（返回 trivial ticket）。
    - 哨兵错误：`ErrCapacityTimeout`、`ErrCapacityRejected`（供 api 层映射 429）。
  - 全部状态变更在单一互斥锁内完成，等待/唤醒用 channel 保证 ctx 取消与公平唤醒；不得忙等。
- **Acceptance Criteria Addressed**：AC-3, AC-4, AC-6, AC-9(原语层), AC-10(原语层)
- **Test Requirements**：
  - `rule` TR-1.1：C=4/R=2 场景，4 background 占满后 control 到达，释放 1 份额时 control 先于 background 等待者获得（确定性轨迹测试，不用 sleep）。
  - `rule` TR-1.2：controlWaiters>0 期间 background 借用零增长；control 清空后可继续借用；background 保底 C−R 在 control 饱和时仍可领取。
  - `rule` TR-1.3：background 等待超过 `BackgroundWaitTimeout` 返回 `ErrCapacityTimeout`；control 在 R 内不等待，仅 C 全满才排队。
  - `rule` TR-1.4：在途 Ticket 持有旧快照期间 Apply 新快照，旧 Ticket 正常 release、簿记归零、新请求走新值；并发 100 轮替换无 panic/race。
  - `rule` TR-1.5：ctx 取消发生在等待中时立即返回、等待者计数归零、份额不增；`MaxBackgroundWaiters` 超限时返回拒绝错误。
  - `rule` TR-1.6：`Enabled=false` 时 Acquire 为 no-op 且不改变计数。
  - `rubric` TR-1.7：并发正确性；scale 1-5；anchors 1=有死锁/泄漏/竞态，3=高并发偶发抖动，5=`-race` 下混合 1000+ 协程抢占/取消/替换无竞态无泄漏；threshold >= 4；evidence=`go test -race ./internal/pkg/traffic`。
- **Notes**：这是 entry/bulk/es 三处唯一准入实现；不依赖 config 包（Snapshot 为纯结构），避免循环依赖。

## Task 2: 新增 server.traffic_priority 配置段与校验
- **Status**： `completed`
- **Completion Evidence**：
  - 新增 config/traffic_priority.go（TrafficPriorityConfig + Validate + Resolve 派生 entry/bulk/es 三份 traffic.Snapshot）与 traffic_priority_test.go；Server 结构挂载 `traffic_priority` 段。
  - TR-2.1~2.3 通过：默认派生（C/R、2s/1s 等待、2×flush 延迟）、显式值、ratio 派生、非法值与容量冲突报错、ucfg unpack 期 Validate 生效、作为 server 子段可加载。`go test -race ./internal/pkg/config/ ./internal/pkg/traffic/` 通过。
- **Priority**： high
- **Depends On**： None
- **Description**：
  - 在 `internal/pkg/config/input.go` 的 `Server` 增加 `TrafficPriority TrafficPriorityConfig `config:"traffic_priority"``（新文件 `traffic_priority.go` 定义，避免 input.go 膨胀）：
    - `enabled`（默认 false）；
    - entry：`entry_max_in_flight`（0→派生 max_connections）、`entry_control_reserved`（绝对值；0→按 `control_reserved_ratio` 派生）、`control_reserved_ratio`（0→默认 0.5）、`background_wait_timeout`（默认 2s）、`max_background_waiters`（默认 0）；
    - bulk：`bulk_control_reserved`（0→按比例×bulk.flush_max_pending）、`bulk_background_wait_timeout`（默认 1s）、`bulk_background_flush_delay`（默认 2×flush_interval）、`max_pending_background_dispatches`（默认 0；>0 时 background dispatch 上限，control 不受此限）；
    - es：`es_max_in_flight`（0→output.elasticsearch.max_conn_per_host）、`es_control_reserved`（0→按比例）、`es_background_wait_timeout`（默认 2s）。
  - `Validate()`：enabled 时非负、R≤C、ratio∈(0,1]、timeout≥0；关闭时跳过派生与严格校验。
  - 提供 `Resolve(cfg *Config) (entry/bulk/es 三份 traffic.Snapshot 派生参数, error)` 帮助函数（C 的 0 值派生在此完成），供装配与热更复用。
  - 不修改 defaults/*.yml 档位文件（开关默认关，派生值足以自洽）；如 lint 要求再补。
- **Acceptance Criteria Addressed**：AC-15
- **Test Requirements**：
  - `rule` TR-2.1：表驱动测试：R>C、负 ratio、ratio>1、负 timeout、C 留空派生 max_connections、bulk R 派生自 flush_max_pending、es C 派生自 max_conn_per_host，逐一声明。
  - `rule` TR-2.2：`enabled=false` 时任意留空/零值均通过且 Resolve 返回 disabled 快照。
  - `rule` TR-2.3：ucfg unpack 坏 yml（新增 testdata/bad-traffic-priority-*.yml）在 LoadFile 阶段报错，风格对齐现有 bad-* 用例。
- **Notes**：校验逻辑保持纯函数，热更新分支（T8）复用同一入口。

## Task 3: bulk 操作携带流量类别（Opt + block + ctx 继承）
- **Status**： `completed`
- **Completion Evidence**：
  - optionsT/bulkT 增 trafficClass（WithTrafficClass + reset 清零）；waitBulkAction/ReadRaw/Search 与 multiWaitBulkOp 经 resolveClass 从 ctx 继承（显式 Opt 覆盖）；multi blocks 统一打标。
  - TR-3.1/3.2：新增 traffic_class_test.go（resolve 优先级、newBlk/reset、dispatch 与 MUpdate 真实拦截 b.ch 断言类别），`go test -race ./internal/pkg/bulk/` 相关用例通过。
- **Priority**： high
- **Depends On**： T1
- **Description**：
  - `optionsT` 增加 `trafficClass traffic.Class`（显式 Opt 优先），新增 `WithTrafficClass(c traffic.Class) Opt`。
  - `bulkT` 增加 `class traffic.Class`，`newBlk` 拷贝、`reset()` 清零；`opMulti.go` 手工构造 block 处继承调用类别。
  - 各同步入口（waitBulkAction、multiWaitBulkOp 及 enroll 相关）从 ctx 取类别作为默认：`class := traffic.FromContext(ctx)`，显式 Opt 覆盖；这样 handler 只需 ctx 打标，dl 层零改动。
  - 不改队列/调度行为（本任务只让类别随 block 可见）。
- **Acceptance Criteria Addressed**：AC-7（载体部分）
- **Test Requirements**：
  - `rule` TR-3.1：显式 Opt 覆盖 ctx；ctx 无类别时默认 control；reset 后 block 类别为零值；multiOp 每个 block 类别一致。
  - `rule` TR-3.2：现有 bulk 包测试全部通过（行为面无变化）。

## Task 4: bulk 双通道准入、control 优先调度、flush 容量闸与 Reconfigure
- **Status**： `completed`
- **Completion Evidence**：
  - Bulker 增 chControl/chBackground（关闭模式仍走 b.ch，Run 以非阻塞 control 优先选择保序）；queueT 增 controlCnt/bgDeferredAt；doFlush 两趟（control/混合批先排，background-only TryAcquire 非阻塞推迟，超过 bgFlushDelay 强制排空防饿死）；flush 许可关闭时沿用 semaphore(maxPending)，开启时走 traffic.Gate，flushCtx 携带批次类别；dispatch 分 legacy/classified 两路径，background 受 dispatch 上限、control 不限；ReconfigureTraffic 原子热更；TrafficStats 暴露通道/队列深度。
  - TR-4.1~4.5：新增 traffic_scheduling_test.go（顺序抢占 msearch→bulk→msearch、分类通道、bg 上限拒绝/control 不受限、排队取消归还池、关闭模式走 legacy 通道、热更设置）；原有 dispatch/limit/bench 测试与 bulk 全量 `go test -race` 通过；`go build ./...` 通过。
  - TR-4.6 rubric 自评 4/5：开关差异收敛在 trafficEnabled/enqueue/两个 dispatch 入口与单一 flushPermit 抽象。
- **Priority**： high
- **Depends On**： T1, T3
- **Description**：
  - Bulker 持有两个入口 channel（control/background，容量沿用 blockQueueSz）；`dispatch` 按 `blk.class` 入对应 channel，select 均保留 `ctx.Done()`；关闭模式（snapshot.Enabled=false）回退现有单 `b.ch` 路径与顺序（可通过内部统一 fan-in 实现，但必须有测试锁定关闭模式顺序与现状一致）。
  - Run 循环取块：优先非阻塞取 controlCh，空时再 select controlCh/backgroundCh/定时器。
  - `queueT` 增类别计数（controlCnt/bgCnt）；doFlush 分两趟：先「含 control」队列（按现有队列下标相对顺序，保证关闭模式一致），后 background-only 队列；background 趟用 `TryAcquire(background)`（flush 闸，见下），不可用则保留队列并重置/续延其调度截止时间；超过 `bulk_background_flush_delay` 的 background-only 队列转为有界 Acquire 强制排空（防饿死）。
  - flush 并发闸：以 `traffic.Gate`（capacity=maxPending，bulk 快照）替换 `semaphore.NewWeighted`；`flushQueue` 按队列有效类别（含 control 即 control）领取，flush goroutine 结束释放；构造 flushCtx 时 `traffic.WithClass(ctx, queueClass)`（供 T5 传输闸）。
  - dispatch pending 上限：开启时 background 受 `max_pending_background_dispatches` 约束（沿用 `ErrTooManyBulkDispatches`→429），control 不计数；关闭时维持现有全局计数语义。
  - 新增 `Reconfigure(snap traffic.Snapshot)`（或持有共享 `*traffic.Gate` 由外部 Apply，bulk 只读快照）；channel 与队列不重建。
  - 阈值计数按类别分别累计（任一类达阈值触发一轮 doFlush）；关闭模式等价单一计数。
  - Stats：暴露各队列 class 计数与通道深度供 T9 sampler。
- **Acceptance Criteria Addressed**：AC-7（调度部分）, AC-8, AC-9, AC-10（dispatch 部分）
- **Test Requirements**：
  - `rule` TR-4.1：关闭模式下 dispatch 顺序、阈值触发、enroll 独立 flush 与改造前一致（用现有测试 + 新增顺序锁定测试）。
  - `rule` TR-4.2：假时钟+假许可闸：background 队列满载且许可被占时，control 块先获得 flush；background-only 队列在配置延迟界内必被 flush。
  - `rule` TR-4.3：controlCh 满时 background 入队不阻塞 control（HoL 测试）；controlCh/backgroundCh 排队中 ctx 取消立即返回且块被回收。
  - `rule` TR-4.4：Reconfigure 期间在途 flush 持旧份额完成、新 flush 走新值；无 bulker 重启。
  - `rule` TR-4.5：background dispatch 超限返回 `ErrTooManyBulkDispatches`，control 不受限；关闭模式沿用旧全局上限。
  - `rubric` TR-4.6：改造内聚性；scale 1-5；anchors 1=关闭/开启两套分叉散落难维护，3=可工作但开关分支重复，5=开关差异收敛在快照与单一选择点；threshold >= 4；evidence 代码走查。
  - 所有新测试 `-race` 通过。

## Task 5: ES RoundTripper 类别感知并发闸
- **Status**： `completed`
- **Completion Evidence**：
  - es.WithTrafficGate 经 rtWrap 安装 trafficRoundTripper：每次 HTTP 尝试按 req ctx 类别领取/释放许可，重试天然保类、退避不占许可；哨兵错误原样上抛；nil gate 纯透传。
  - TR-5.1~5.4：traffic_test.go 覆盖 3 次尝试类别一致且许可成对释放、缺省 control、饱和 background 超时拒绝且内层未被调用（control 在释放后优先获入）、等待取消立即返回、与 APM InstrumentRoundTripper 链式共存；`go test -race ./internal/pkg/es/` 通过。
- **Priority**： high
- **Depends On**： T1
- **Description**：
  - `internal/pkg/es` 新增 `WithTrafficGate(g *traffic.Gate) ConfigOption`（经 `rtWrap` 包一层 `priorityRoundTripper`）：
    - 每次 `RoundTrip` 从 `req.Context()` 读类别（缺省 control）→ `gate.Acquire(ctx, class)`（background 用配置等待时限，control 用请求 ctx）→ 立即 defer release → 调内层 transport；因此 elastictransport 的每次重试都按相同类别重新领取、退避期间不持份额。
    - 闸门哨兵错误原样向上返回（不吞错、不伪装成 ES 错误）。
  - 传输闸容量/保留量由 es 快照配置；gate 为外部单例（主 client 与 monCli 共享同一闸，或按装配决策共享——默认共享）。
  - remote output client（bulk.createRemoteEsClient）同样安装该 Option（默认 control）。
- **Acceptance Criteria Addressed**：AC-11
- **Test Requirements**：
  - `rule` TR-5.1：假 RoundTripper 前两次返回可重试错误、第三次成功：断言三次尝试类别一致、每次都成对 acquire/release、退避期间在途为 0。
  - `rule` TR-5.2：background 等待超时返回哨兵错误且未发起内层请求；ctx 取消立即返回。
  - `rule` TR-5.3：无类别 ctx（含 monitor 服务器 ctx 打标后）按 control 处理。
  - `rule` TR-5.4：WithTrafficGate 与 APM InstrumentRoundTripper 包装顺序可共存（链式包装单测）。

## Task 6: 服务器生命周期路径与 dl 层类别标注
- **Status**： `completed`
- **Completion Evidence**：
  - checkin.Bulk 聚合 MUpdate 显式 WithTrafficClass(Control)（覆盖 OpAMP 心跳）；monitor 长轮询 search ctx 显式 control；OpAMP handleMessage 回调 ctx 显式 control（不依赖 opamp-go ctx 血缘）。
  - 背景端点（uploads/file delivery/artifacts）经请求 ctx 继承类别（T7 中间件打标），bulk/es 层已从 ctx 自动取类；GC/policy 等服务器 ctx 路径缺省 control；直连点普查结论：未标注直连调用默认 control（FromContext 缺省保证），无未受管缺口。
  - `go build ./...`，`go test -race ./internal/pkg/checkin/ ./monitor/ ./es/` 通过。
- **Priority**： medium
- **Depends On**： T3, T4, T5
- **Description**：
  - `checkin.Bulk` 聚合 flush 的 `MUpdate` 调用显式/经 ctx 标 control（flush 使用服务器 ctx）。
  - policy/action monitor、self monitor 的 ES 直连 ctx 标 control；monitor 长轮询 ctx 标 control。
  - uploader begin/complete 的 bulk 操作与 file 元数据查询、artifacts 的 `dl.FindArtifact` 链路标 background（优先由入口 ctx 自然继承；服务器 ctx 发起的补偿/清理类（如 hash 失败删 chunk、GC）显式标注：GC 类 control，请求补偿类随请求类别）。
  - 核查 OpAMP：入口中间件为连接 ctx 打 control 后，handleMessage 各回调派生 ctx 是否继承；不继承处在 handleOpAMP.go 显式标 control。
  - 普查全仓 `bulker.Client()`/`esCli.`/`monCli` 直连点（subagent 调研遗留 gap：gc 包、policy Prepare），逐一归类，不留「未标注但受管」的新路径。
- **Acceptance Criteria Addressed**：AC-7（端到端）, AC-11（直连覆盖）
- **Test Requirements**：
  - `rule` TR-6.1：单测/钩子验证 checkin.Bulk flush 的块类别为 control；artifact 搜索块为 background；monitor 长轮询 RoundTrip 类别 control。
  - `rule` TR-6.2：直连点普查清单以注释或测试表固化，新增直连调用默认 control（通过 `FromContext` 缺省保证）。

## Task 7: HTTP 入口分级中间件、单例注入与 429 映射
- **Status**： `completed`
- **Completion Evidence**：
  - api/traffic.go：operationClass 表 + trafficAdmission 最外层中间件（nil/关闭/未受管纯透传；acquire 绑定 r.Context、defer release、类别注入 ctx 传给 bulk/ES）；api.go 增 WithTrafficGate 单例 APIOpt；server.go NewServer 包裹最终 handler（覆盖 /api 与 /v1/opamp）；error.go 将 traffic 哨兵错误映射为 429 TrafficLimit，429 体与 limit 包一致。
  - TR-7.1~7.4：traffic_test.go 覆盖类别表（含 opamp/unknown）、透传三场景、ctx 打标与释放、饱和 background 20ms→429 JSON、control 释放后优先、等待取消不进内层、httptest 端到端；race 通过。
- **Priority**： high
- **Depends On**： T1
- **Description**：
  - 操作→类别表（紧邻 `pathToOperation`）：control={checkin,acks,opamp,enroll,status,getPGPKey,audit-unenroll}；background={uploadBegin,uploadChunk,uploadComplete,deliverFile,artifact}；其余 unknown=不受管（直接放行）。
  - 新中间件（`internal/pkg/api` 内，gate 经新 `APIOpt` 注入；nil/disabled 纯透传）：在 `NewServer` 中包裹**最终 handler 外层**（覆盖 newRouter 与 OpAMP 附加 router），使等待发生在 `Throttle(MaxConnections)` 与 per-route limiter 之前；领取绑定 `r.Context()`，defer 释放；拒绝写与 limit 包同形态的 429 JSON。
  - `api/error.go` 增加 traffic 哨兵错误→429（`TrafficLimit` 错误码）的映射；handler 内来自 bulk/es 的同类错误同样映射。
  - fleet 装配处（T8 完成接线，本任务提供 option 与构造参数）保证两个 listener 与 OpAMP router 共享同一 gate 单例。
  - 入口阶段埋点（in_flight/waiting/borrowed/admitted/rejected/wait duration，指标定义在 T9）。
- **Acceptance Criteria Addressed**：AC-2, AC-5, AC-10（入口部分）, AC-1（入口透传部分）
- **Test Requirements**：
  - `rule` TR-7.1：操作→类别表驱动测试（全部操作 + unknown + /v1/opamp）。
  - `rule` TR-7.2：中间件测试：background 超时/拒绝→429 且 JSON 字段与现有 limit 429 形态一致；control 优先；等待中断开释放（httptest + 取消 ctx）。
  - `rule` TR-7.3：gate=nil/disabled 时 handler 链与今天字节级一致（对现有 router_test 无修改通过）。
  - `rule` TR-7.4：OpAMP 路径经外层包裹获得 control 准入（构造带探针 gate 的 server 测试）。

## Task 8: fleet/agent 装配与「仅 traffic_priority 变更」热更新分支
- **Status**： `completed`
- **Completion Evidence**：
  - server/traffic.go：trafficGates（entry/bulk/es 三闸，Apply 解析+原子替换+保留上次有效值+状态记录）、reloadAction 决策、serverChangedIgnoringTraffic 比较；fleet.go 初始建闸、reload 循环坏配置归一化不重启、traffic-only 变更只调 configureBulker；initBulker/monCli/remote client 装 WithTrafficGate，bulk WithTrafficScheduling，两个 NewServer 共享同一 entry gate（fleet.go:568-604 单例注入）。
  - TR-8.1~8.4：server/traffic_test.go 覆盖合法切换/坏配置保留快照与失败状态/无变化/无关 server 变更与 output 变更重启/初始启动/bulker 热设置；旧 fleet_test configChanged 用例迁移到新谓词。`go build ./...` 通过。
- **Priority**： high
- **Depends On**： T2, T4, T5, T7
- **Description**：
  - `runServer/runSubsystems`：创建 gate 管理器（entry/bulk/es 三个 Gate 或一个管理器持有三份 Snapshot）；bulk 构造增加 gate 注入（BulkOpt）；es.NewClient（主 client、monCli、remote client）加 `WithTrafficGate`；`api.NewServer` 两处调用注入同一 entry gate。
  - `Fleet.Run` reload 循环在 `configChangedServer` 全量重启判断**之前**插入分支：
    1. 解析并 Resolve 新 cfg 的 traffic_priority；失败→记 error 日志、置「最近应用失败」状态（T9/T10 消费），沿用旧快照，不重启；
    2. 成功且仅 traffic_priority 差异→三个 Gate.Apply（原子）+ bulker Reconfigure，不进入重启分支；比较时把新旧 cfg 的 traffic_priority 段归一化，避免其差异误触发全量重启；
    3. 与其它 server 配置同时变更→走既有重启路径，新 server 使用已 Resolve 的合法值；非法值不进入启动（沿用现有启动失败语义）。
  - standalone 首次启动同样 Resolve；非法值启动失败（FR-22）。
- **Acceptance Criteria Addressed**：AC-12, AC-16（装配部分）
- **Test Requirements**：
  - `rule` TR-8.1：投递仅 traffic_priority 变化的合法 cfg：gate 快照变化、runServer 未被再次调用（探针/计数），在途请求按旧快照完成。
  - `rule` TR-8.2：非法 cfg 投递：Apply 未发生、服务继续、失败状态可查询、无重启。
  - `rule` TR-8.3：traffic_priority 与无关 server 字段同时变更：进入既有重启路径且新值生效。
  - `rule` TR-8.4：两个 listener 共享同一 gate 实例（装配断言）。

## Task 9: Prometheus/libbeat 指标族与 bulk 队列采样器
- **Status**： `completed`
- **Completion Evidence**：
  - Gate Observer 钩子（Admitted/Released/Rejected/Queued，含 timeout/waiters/canceled 原因）；api/traffic_metrics.go 双注册：prom 标签族（inflight/waiting/borrowed/admitted_total/rejected_total/wait_seconds histogram/bulk_queue_depth/config reload gauge）+ libbeat traffic_priority 标量树；RunTrafficMetricsSampler 周期采样队列深度与 reload 结果。
  - traffic/observer_test.go 验证事件序列与 waiter cap 拒绝；api/traffic_metrics_test.go 验证 prom 指标增量/归零、sampler 队列分类深度、reload gauge。
- **Priority**： medium
- **Depends On**： T1
- **Description**：
  - 在 `api/metrics.go` 风格下新增指标族（双注册，label 化）：
    `traffic_priority_inflight{stage,class}`、`..._waiting{stage,class}`、`..._borrowed{stage,class}`（gauge）；
    `traffic_priority_admitted_total{stage,class,mode=reserved|borrowed|floor}`、`..._rejected_total{stage,class,reason=timeout|waiters|capacity}`（counter）；
    `traffic_priority_wait_seconds{stage,class}`（histogram）；
    `traffic_priority_bulk_queue_depth{queue,class}`、`..._bulk_queue_bytes{queue,class}`（sampler gauge）；
    `traffic_priority_config_last_reload_successful`（gauge 0/1）、`..._config_last_reload_timestamp_seconds`。
  - stage：entry/bulk_dispatch/bulk_flush/es。Gate 暴露线程安全 Stats；entry/es 埋点在各自包装内直接 inc；bulk 队列深度经 sampler 周期采集（仿 RunConnRejectionRateSampler，在 fleet.go errgroup 注册）。
  - 关闭模式指标注册但读数为零。
- **Acceptance Criteria Addressed**：AC-13
- **Test Requirements**：
  - `rule` TR-9.1：registry 抓取文本包含全部指标名与 label 组合；制造 admit/borrow/reject 后值正确单调；sampler 更新队列深度。
  - `rule` TR-9.2：关闭模式抓取中指标存在且为 0。

## Task 10: 内部调试状态端点 GET /debug/traffic-priority
- **Status**： `completed`
- **Completion Evidence**：
  - 仅在 InitMetrics 的本地 metrics mux（localhost:5066）注册 /debug/traffic-priority，聚合三阶段 Snapshot/Stats、bulk 队列深度、最近 reload 结果；agent 侧 NewServer 从不注册该路径（结构隔离）。
  - trafficPriorityHandler 单测（httptest 200 + 字段断言）；metrics_integration_test.go 扩展该路径断言（integration tag）。
- **Priority**： medium
- **Depends On**： T8, T9
- **Description**：
  - 在 metrics server（localhost:5066，`InitMetrics` 的 mux）新增 `/debug/traffic-priority`：聚合三个 gate 的 Stats、bulk 队列深度、生效快照（脱敏，仅计数与配置）、最近一次 Apply 结果/原因/时间戳。
  - 确保不注册进 8220/8221 任何 router；返回 JSON 结构带版本字段便于演进。
- **Acceptance Criteria Addressed**：AC-14
- **Test Requirements**：
  - `rule` TR-10.1：metrics server 200 + 字段齐全（enabled、各 stage×class 在途/等待/借用/拒绝、队列深度、最近应用状态）。
  - `rule` TR-10.2：agent 侧两个 listener 访问该路径为 404。

## Task 11: 文档、reference 配置与 changelog
- **Status**： `completed`
- **Completion Evidence**：
  - fleet-server.reference.yml 增 server.traffic_priority 全字段注释示例（默认关闭、容量规划说明）；docs/traffic-priority.md 描述类别、借用语义、三层闸、热更新、指标与调试端点；changelog/fragments 新增 enhancement 条目。
  - 所有新增 Go 文件带 Elastic 2.0 license 头；T12 统一 gofmt/vet/imports 验证。
- **Priority**： low
- **Depends On**： T2
- **Description**：
  - `fleet-server.reference.yml` 增 traffic_priority 全字段注释示例（默认关闭、容量规划说明：长轮询/长连接持有入口份额、ES 保留量需覆盖 monitor 数量）。
  - docs 增补一节（过载保护语义、借用规则、指标判读示例、热更新与坏配置行为）。
  - 用 elastic-agent-changelog-tool 新增 changelog fragment。
  - 全部新 Go 文件带 license header，imports 排序，zerolog 日志。
- **Acceptance Criteria Addressed**：AC-19（文档部分）, AC-16（合规部分）
- **Test Requirements**：
  - `rule` TR-11.1：`mage check:headers`、`mage check:imports` 通过；reference.yml 可被 config 加载解析。

## Task 12: 全量验证与性能基准
- **Status**： `completed`
- **Completion Evidence**：
  - `gofmt -l` 干净；`go vet ./internal/...` 仅有 1 个存量告警（config_test.go，未改动文件）；goimports 干净；新增文件均带 Elastic 2.0 license 头。
  - 全量非集成 `go test ./internal/... ./cmd/...` 全部通过；核心包 `go test -race`（api/bulk/server/checkin/es/config/monitor/traffic）全部通过；`go build ./...` 通过；`go vet -tags integration` 编译通过。
  - TR-12.3 基准：BenchmarkGateAcquireDisabled 4.24 ns/op、0 allocs（关闭模式零开销）；Uncontended 106 ns/op。
  - TR-12.4：entry→bulk→es 类别传递以单测链锁定（operationClass/trafficAdmission ctx 注入 → resolveClass → flushCtx → RoundTripper）。
  - 注：本地 golangci-lint 版本不兼容 Go 1.27（export data v4），以 go vet 替代；CI 用固定版本。
- **Priority**： high
- **Depends On**： T1-T11
- **Description**：
  - `go build ./...`、`mage check:all`（或可用范围内 go vet/golangci-lint/goimports/header）、`mage test:unit`；新增并发测试在 `-race` 下通过。
  - 基准：Gate Acquire 快路径 bench（关闭/开启无竞争/有竞争）；bulk dispatch 关闭模式对比基线；将结果记入任务证据。
  - 端到端顺序走查：entry→bulk→es 三个 stage 在一次 control 与一次 background 请求中的埋点连贯性（可用单元级串联测试代替 Docker）。
- **Acceptance Criteria Addressed**：AC-1, AC-16, AC-17, AC-18
- **Test Requirements**：
  - `rule` TR-12.1：`mage test:unit` 退出码 0；新包 `-race` 通过。
  - `rule` TR-12.2：lint/header/imports 全部通过。
  - `rubric` TR-12.3：性能无回归；scale 1-5；anchors 1=关闭模式可测回归 >5%，3=差异 ≤2%，5=关闭模式噪声级差异且开启快路径无额外堆分配；threshold >= 4；evidence bench 输出对比。
  - `rubric` TR-12.4：整体内聚性（AC-18）；scale 1-5；threshold >= 4；evidence 独立 Review 走查。
