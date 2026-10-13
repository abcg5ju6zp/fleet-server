# Namespace 公平预算与故障隔离 - 产品需求文档

## Overview

- **Summary**：在 Fleet Server 中以"已授权身份携带的 namespace"为资源归属，对 HTTP 准入与连接、check-in 长轮询、策略/动作调度、Elasticsearch 批量读写队列及共享缓存实施统一的两级（全局 + 每 namespace）预算与公平次序；空闲份额可被借用，拥塞时可回收；ES 变慢、持续重试、批量部分失败、客户端断连时只向责任归属施加退避/丢弃/失败，其他 namespace 继续获得服务。
- **Purpose**：消除单 namespace 突发 check-in、Action 回执或文件请求占满全局限流器与共享批量队列、导致其他 namespace 心跳/策略/动作整体超时的"吵闹邻居"故障模式。
- **Target Users**：
  - 多 Kibana Space（多 namespace）共享 Fleet Server 的运维方与租户；
  - 负责容量规划与故障排查的 Fleet/Elastic 运维（通过指标与健康端点看到分租户占用、等待、拒绝、借用）；
  - 单 namespace 或无 namespace 部署的既有用户（行为必须保持兼容）。

## Goals

- 从**已经通过授权**的身份确定性地推导资源归属，并把归属快照沿调用链一致传播到准入、长轮询、调度、bulk 与缓存。
- 每类受管资源都具备：全局上限（兜底）+ 每 namespace 保证份额 + 空闲借用 + 拥塞回收 + 立即失败语义。
- 故障隔离：ES 变慢/429、某 namespace 持续重试、bulk 部分失败、客户端断连风暴只影响对应归属。
- 配置变更后预算组件原地续存；进程重启后按配置确定性重建为可解释的基线状态。
- 指标与健康端点分别展示全局与每 namespace 的占用、等待、拒绝、借用、退避。
- 对无 namespace 端点、单 namespace 部署、低负载路径、当前授权拒绝语义完全兼容；提供一键关闭开关。

## Non-Goals

- **不**做跨 Fleet Server 实例/Pod 的分布式公平（多 Pod 部署下每个进程独立记账；跨 Pod 容量均衡属于负载均衡层职责）。
- **不**持久化预算账本到 ES/磁盘（重启不恢复借用/退避历史，见"恢复语义"决策）。
- **不**改变 agent 与文档数据模型：不新增 namespace 字段、不修改 enroll 时 namespaces 继承自 enrollment key 的语义、不改变 action_result.namespaces 复制自 action 文档的语义。
- **不**做 TLS 握手前的 TCP 连接按 namespace 分账（此时加密载荷中的 API key 不可见）；TCP 硬限保留为全局兜底。
- **不**实现服务端有界排队等待（保持 fail-fast），不主动中断已准入的长轮询/在途请求。
- **不**改变 ES 客户端既有的传输层重试策略与对 agent 的错误码映射总则（401/403/429/503 语义保持）。
- 不覆盖 gc、scheduler 等后台任务（无单请求归属）。
- OpAMP 仅做连接级归属记账与准入，不重做其消息内 agent 解析模型。

## Background & Context

### 现状证据（调查所得，均为当前工作树事实）

1. **归属字段已存在但未用于任何准入**：`model.Agent.Namespaces []string`（json `namespaces`）、`Policy/Action/ActionResult/EnrollmentAPIKey` 同名字段（[schema.go L54/L111/L188/L408/L467](file:///Users/ding/Documents/swe/0910125/project-11/internal/pkg/model/schema.go#L54-L55)）；agent.namespaces 在 enroll 时从 enrollment key 原样拷贝（[handleEnroll.go L151/L395/L431](file:///Users/ding/Documents/swe/0910125/project-11/internal/pkg/api/handleEnroll.go#L151)）；action_result.namespaces 复制自 action 文档（[handleAck.go L370](file:///Users/ding/Documents/swe/0910125/project-11/internal/pkg/api/handleAck.go#L370)）。全仓无任何按 namespace 的过滤/配额逻辑。
2. **鉴权漏斗**：`authAgent`（[auth.go L90](file:///Users/ding/Documents/swe/0910125/project-11/internal/pkg/api/auth.go#L90)）是 checkin/ack/artifacts/file delivery/upload/audit 的统一入口，返回完整 `*model.Agent`（含 Namespaces，无额外 ES 成本——agent 文档本来就要读；agent 不进缓存）。enroll 解析 enrollment key 记录后可得 `EnrollmentAPIKey.Namespaces`；静态策略令牌分支该字段为 nil。status 鉴权可选且不查 agent；pgp 不鉴权；OpAMP 建连用 enrollment key、agent 在消息内按 UID 解析。
3. **三层全局闸全部先到先得**（[limiter.go L28-L97](file:///Users/ding/Documents/swe/0910125/project-11/internal/pkg/limit/limiter.go#L28-L97)、[listener.go L26-L97](file:///Users/ding/Documents/swe/0910125/project-11/internal/pkg/limit/listener.go#L26-L97)、[router.go L23-L92/L169-L202](file:///Users/ding/Documents/swe/0910125/project-11/internal/pkg/api/router.go#L169-L202)）：TCP 硬限（1.1×max_connections，TLS 前静默关连接）、chi Throttle（在途上限，429 纯文本）、每路由 rate.Limiter + 信号量（429 JSON）。限流中间件在鉴权**之前**执行。每路由信号量在整个 `ServeHTTP` 期间持有，因此一个空闲长轮询最长可占槽 5 分钟（客户端 poll_timeout 可协商到 1 小时，[handleCheckin.go L407-L428/L598-L638](file:///Users/ding/Documents/swe/0910125/project-11/internal/pkg/api/handleCheckin.go#L598-L638)）。两个监听地址（8220/8221）各自独立持有一套 Limiter，仅共享 handler T 对象与全局指标。
4. **bulk 单队列结构无租户维度**（[engine.go](file:///Users/ding/Documents/swe/0910125/project-11/internal/pkg/bulk/engine.go)、[queue.go L7-L26](file:///Users/ding/Documents/swe/0910125/project-11/internal/pkg/bulk/queue.go#L7-L26)、[block.go L18-L29](file:///Users/ding/Documents/swe/0910125/project-11/internal/pkg/bulk/block.go#L18-L29)）：单一入口 channel（生产固定容量 32）+ 8 类链表队列 + 单个 Run goroutine + 全局 flush 信号量（maxPending 默认 8，8 类队列共享）。channel 满默认阻塞到 ctx 取消；`max_pending_bulk_dispatches` 全局快速拒绝默认关闭。ES 变慢 → flush 槽占满（单次 flush/取槽 1 分钟超时）→ Run 停止取 op → 全部队列（含读/msearch/enroll）一起饿死，取槽超时甚至使 Run 退出并重启整个服务子系统（[engine.go L565-L580](file:///Users/ding/Documents/swe/0910125/project-11/internal/pkg/bulk/engine.go#L565-L580)）。整批失败把同一错误 fan-out 给批内全部 block（[engine.go L640-L652](file:///Users/ding/Documents/swe/0910125/project-11/internal/pkg/bulk/engine.go#L640-L652)）；item 级失败逐条回填（ack 映射 429）。客户端断连后已入链表的 block 仍占队列/flush 槽直到被 flush（排空最多 15s，[engine.go L747-L785](file:///Users/ding/Documents/swe/0910125/project-11/internal/pkg/bulk/engine.go#L747-L785)）。bulk 包无任何 prometheus 指标。
5. **调度全局串行点**：action dispatcher 仅有一个全局 rate.Limiter、按 agentID 订阅、非阻塞满即丢（[dispatcher.go L26-L195](file:///Users/ding/Documents/swe/0910125/project-11/internal/pkg/action/dispatcher.go#L26-L195)）；policy monitor 的 pendingQ 是全局唯一 FIFO，受 policy_limit（burst 1）逐个人工限速（[policy/monitor.go L245-L334/L425-L495](file:///Users/ding/Documents/swe/0910125/project-11/internal/pkg/policy/monitor.go#L245-L334)）；订阅扇出对每个订阅者起 goroutine 但 `wg.Wait()` 会让一个慢消费者阻塞整轮通知 60s（[subscription_monitor.go L137-L165](file:///Users/ding/Documents/swe/0910125/project-11/internal/pkg/monitor/subscription_monitor.go#L137-L165)）。
6. **缓存全局共享**：ristretto 全局 TinyLFU + 全局 cost 预算（[impl_ristretto.go L15-L23](file:///Users/ding/Documents/swe/0910125/project-11/internal/pkg/cache/impl_ristretto.go#L15-L23)），缓存 APIKey/EnrollmentKey/Action/Artifact/Upload/PGPKey，按字节计 cost，无 namespace 容量概念；大 namespace 可凭频率/体积挤掉其他 namespace 条目。
7. **指标无标签维度**：全部 prometheus 指标无 labels（[metrics.go L34-L96](file:///Users/ding/Documents/swe/0910125/project-11/internal/pkg/api/metrics.go#L34-L96)）；`/api/status` 不暴露任何 limiter/队列/缓存内部状态（[handleStatus.go L80-L146](file:///Users/ding/Documents/swe/0910125/project-11/internal/pkg/api/handleStatus.go#L80-L146)）。
8. **生命周期**：任何 server 配置变化都会停掉并整体重建 runServer（limiter/bulker/monitor/dispatcher 全部新建，[fleet.go L189-L204/L258-L274](file:///Users/ding/Documents/swe/0910125/project-11/internal/pkg/server/fleet.go#L189-L204)）；仅 cache 是挂在 Fleet 上跨重配续存、支持原地 Reconfigure 的长生命周期组件（[fleet.go L163-L171](file:///Users/ding/Documents/swe/0910125/project-11/internal/pkg/server/fleet.go#L163-L171)）。所有限流/队列状态均为进程内存态、无持久化。

### 关键设计决策（已与需求方确认）

- **D1 归属键**：以 agent.namespaces 排序去重后拼接成唯一规范化键；空 namespaces 的 legacy agent / 静态策略令牌 / 无身份路径归入 `default` 桶。
- **D2 份额模型**：每类受管资源在既有全局上限内，按近期活跃 namespace 数动态计算保证份额（带最小/最大封顶），支持配置文件对指定 namespace 静态加权或显式覆盖；空闲份额可借用，出现竞争时回收（不抢占在途，只停止新借入并拒绝超额新准入）。
- **D3 准入行为**：保持非阻塞立即失败，拒绝仍为 429 与既有错误体；对被拒归属附加 Retry-After 退避提示，不在服务端排队。
- **D4 恢复语义**：预算为纯内存态，预算注册表参照 cache 成为 Fleet 长生命周期组件、跨"配置变化导致的服务器重建"续存；进程重启后按配置确定性重建为基线（占用=0、借用=0、退避清零），配置与当前状态通过指标/健康端点可解释；不写持久化账本。

## Functional Requirements

### A. 身份与归属

- **FR-1**：在 `authAgent` 成功后，从 `agent.Namespaces` 计算规范化归属键（排序、去重、空→`default`），并以不可变快照写入请求 context；后续全部准入/记账只读取该快照（"领取时身份"），请求处理期间 agent 文档变化不改变本次归属。
- **FR-2**：enroll 在 enrollment key 记录解析完成后，以 `EnrollmentAPIKey.Namespaces`（空→`default`）确定归属；OpAMP 连接在建连鉴权后先以 enrollment key namespaces/default 建账，agent 在消息中解析成功后可把连接记账切换到 agent 归属（切换是一次性的；切换后身份同样快照化）。
- **FR-3**：status、pgp、未认证请求、未匹配路由归入 `default`（或显式的未认证共享桶），不因缺少身份而被新逻辑拒绝；其既有响应语义不变。
- **FR-4**：归属快照必须能随调用链到达 bulk 层（通过 bulk option 固化进 block，而非在 flush 时从可能已取消的 ctx 晚取）；checkin 批量时间戳写等后台路径同样带归属标签。
- **FR-5**：归属键规范化与 context 存取封装为单一工具包，全链路复用同一实现；禁止各层自行解析 namespaces。

### B. 预算内核（资源无关的通用机制）

- **FR-6**：新增 namespace 预算注册表（长生命周期、线程安全、两个 listener 与 bulker 共享同一实例），按"资源类"分别管理：全局容量、每 namespace 保证份额、当前占用、借用占用、等待/拒绝计数、退避状态。
- **FR-7**：份额计算：保证份额 = clamp(全局容量 / 活跃 namespace 数, 配置最小份额, 配置最大份额)，静态覆盖/权重优先；活跃集合按准入事件滑动维持，空闲超过 idle TTL 的 namespace 回收其注册表项与保留份额（计数归零后）。
- **FR-8**：准入判定为非阻塞 TryAcquire：namespace 占用 < 保证份额 → 永远允许（只要全局有余量）；占用 ≥ 保证份额时可使用空闲份额（借入），借入受借用上限约束；当全局容量被多个活跃 namespace 竞争（可回收余量不足）时，对超出保证份额的 namespace 拒绝新借入。单个 namespace 的占用在任何时刻不得超过"保证份额 + 允许借入上限"，且所有 namespace 占用之和不得超过全局容量。
- **FR-9**：租约（Lease）语义：一次准入返回一个不可转交他人的释放句柄；持有期间占用记账于领取时归属；释放幂等（sync.Once），且覆盖客户端断连、handler panic、长轮询超时等全部退出路径，杜绝泄漏。
- **FR-10**：支持原地 Reconfigure（容量、封顶、权重、退避参数变化时平滑生效，不清零在途占用）；开关关闭时预算内核退化为与现有限流器等价的单全局桶行为。
- **FR-11**：提供一致的快照查询接口（供健康端点/指标）：每资源类 × 每 namespace 的配置份额、在途占用、借用占用、累计允许/拒绝（分原因）、退避到期时间。

### C. HTTP 入口、连接与长轮询

- **FR-12**：保留现有三层全局闸（TCP 硬限、chi Throttle、每路由 rate+max）作为总量兜底，状态码/错误体/日志级别不变；在鉴权完成、归属可知后增加**第二阶段 namespace 准入**：失败时释放第一阶段预占并按现状返回 429（附加 Retry-After），成功则两阶段租约共同持有到请求结束。
- **FR-13**：checkin/ack/artifacts/file delivery/upload(begin/chunk/complete)/audit 使用 `authAgent` 归属；enroll 使用 enrollment key 归属；OpAMP 使用连接归属；status/pgp 走 default 桶；未匹配路由保持"不限流不计数"。
- **FR-14**：长轮询租约贯穿整个 poll（含协商后的 poll_timeout、30s 时间戳刷写、jitter），回收借用时**不**中断已准入的 poll（保持关停时 200+原 ackToken 等既有契约）；拥塞回收对新 check-in 立即生效，被回收 namespace 的在途 poll 在既有 poll 超时窗口内自然排空。
- **FR-15**：TCP 层保持全局硬限（TLS 前身份不可得），但连接级拒绝指标需与 HTTP 层分租户指标可关联解释（文档注明该层无法分账的原因）。
- **FR-16**：被 namespace 预算拒绝时，响应体/Content-Type 与现状 429 完全一致，仅允许新增 Retry-After 头与日志字段（namespace、reason、share、inuse）；401/403 授权失败路径与计数不发生变化。

### D. 策略与动作调度、订阅扇出

- **FR-17**：policy monitor 的全局 pendingQ 改为按归属分队列 + 公平出队（DRR/加权轮询），单个 namespace 的大量策略变更/慢 Clone 不能长期占住唯一调度 goroutine；全局 policy_limit 仍作为总速率天花板保留。
- **FR-18**：action dispatcher 的订阅以领取时归属建账；群发动作的扇出与全局 ActionLimit 之下增加每 namespace 速率/在途扇出预算；单订阅者满即丢的现有语义保留（依赖下次 checkin 重取）。
- **FR-19**：index 订阅扇出不得因一个 namespace 的慢消费者阻塞整轮通知：慢/阻塞订阅者的等待不得拖延其他 namespace 当轮通知的发送（消除 60s 整轮阻塞），超时丢弃仍按归属记指标。
- **FR-20**：policy 初始加载"一次全量查询 + Clone 下发"的共享模型保留；未知 policy reload/kick 合并需要防单 namespace 触发的 reload 风暴（合并仍是全局级，归属只用于下发排序）。

### E. 批量读写队列（bulk）

- **FR-21**：每个 bulk op 在 dispatch（领取队列位置）时通过 option 携带并在 block 上固化归属快照；ctx 取消或 multiOp 共享 ctx 均不改变其归属。
- **FR-22**：在进入入口 channel 前增加每 namespace 在途 dispatch 预算（非阻塞）：超额立即返回可重试错误并映射为与 `ErrTooManyBulkDispatches` 同形态的 429（ack 逐条、其他路径按现有错误映射），不再让单 namespace 阻塞式填满容量 32 的共享 channel。
- **FR-23**：flush 槽（现全局 8 个）实行按归属公平调度与借用记账：选择 flush 工作时跨归属轮转，被借用槽在竞争时优先归还（队列位置是秒级短命资源，下一轮 flush 选择即完成回收）；在途 ES HTTP 请求不取消。读/search/msearch 与写共同受公平调度约束，保证 ES 变慢时仍有归属能完成读写。
- **FR-24**：批次部分失败按 item 归属分别记账与结论（沿用现有逐条回填），不得把一个 namespace 的 item 失败扩大为其他 namespace 的失败；整批失败（连接错等）对各归属分别计数，据此只对失败率超阈值的归属开启退避。
- **FR-25**：每 namespace 退避状态：当该归属出现持续 item 429/5xx 或整批失败达到阈值时进入冷却期，冷却期内其**新** op 快速失败（可重试结论，不进入共享队列），其他归属不受影响；退避按指数退避并有上限与到期自动解除；ES 传输层既有重试不变。
- **FR-26**：客户端断连后，请求级租约立即释放；已进入 bulk 的 block 保持领取时归属并沿用现有排空机制，但其队列/flush 占用必须在 block 离开队列/flush 完成时确定释放（含现有 15s 排空兜底），不得因断连风暴造成任何归属的槽位泄漏或记账悬空。
- **FR-27**：checkin 时间戳批量写（10s flush）按归属记账；该路径被自身归属预算拒绝时保持现有"只 warn 不影响 check-in 响应"的最佳努力语义。
- **FR-28**：enroll 搜索去重批次、APIKeyUpdate 合并批次等跨 agent 聚合路径定义明确的混合归属规则（批次归属 = 参与归属集合；批次级预算向各参与归属分别校验/记账，拒绝只结论提交者），不破坏 dedupe canonical 与 role 合并语义；remote output bulker 的既有 output 分区保持正交（先按 output 分 bulker，bulker 内再按 namespace 公平）。

### F. 共享缓存

- **FR-29**：缓存按归属做有保证的 cost 份额划分：每活跃 namespace 保留保证 cost 份额，空闲份额可被借用（借放条目）；发生容量压力时，先驱逐借用条目/本归属条目，不得驱逐其他归属的保证份额条目。
- **FR-30**：鉴权前缓存（APIKey 有效性）与无归属对象放入共享（default）分片；缓存 TTL、key 结构、Reconfigure 整体重建语义与命中率行为在单 namespace/低负载下与现状等价。

### G. 指标与健康

- **FR-31**：新增带 `namespace` 标签的 CounterVec/GaugeVec（标签基数受注册表上限约束，超出的 namespace 聚合为 `_other`），至少覆盖：HTTP 各资源类在途占用、累计允许、累计拒绝（reason=`rate|max|borrow|backoff`）、当前借用占用、退避状态；bulk 每归属队列深度/等待、在途 flush、flush 时延、拒绝、item 失败/429；policy/action 每归属排队与扇出丢弃；缓存每归属 cost/驱逐。
- **FR-32**：现有全部指标名称、类型、无标签形态与采样器（limit_max_rate、tcp_rejected_rate 等）保持不变；新指标为新增项。
- **FR-33**：`/api/status` 已认证响应新增预算节：全局容量与总占用、活跃 namespace 数、每 namespace 的配置份额/在途/借用/拒绝/退避；未认证响应维持现状。

### H. 配置与生命周期

- **FR-34**：新增 namespace 预算配置段（挂在 server 配置下）：总开关、各资源类是否启用、保证份额最小/最大封顶、借用上限比例、静态权重/覆盖、活跃判定与 idle TTL、退避（初始间隔、上限、失败阈值）、指标标签基数上限；零值回退到随档位/代码默认；Validate 校验非负与合理性。
- **FR-35**：预算注册表在 Fleet 层构造（参照 cache），两个 listener 与 bulker/monitor/dispatcher 共享；server 配置变化导致的重建不丢注册表在途记账；预算参数变化走原地 Reconfigure。
- **FR-36**：进程重启后预算以配置基线确定性重建（占用/借用/退避/历史计数归零），启动日志与健康端点明确输出生效配置与基线状态（"可解释"）；不访问任何持久化账本。

## Non-Functional Requirements

- **NFR-1 兼容性**：开关关闭、单活跃 namespace、低负载三种情形下，可观测行为（拒绝率、延迟、错误体、吞吐）与改造前等价；既有全部单元测试不修改断言即通过（新增测试另算）。
- **NFR-2 公平性有效性**：在一个 namespace 持续占满其可用份额的负载下，另一活跃 namespace 的 check-in/ack/文件请求准入延迟与成功率不依赖于前者负载（在全局容量有余量时，其保证份额内的请求成功；全局真耗尽时按份额比例拒绝而非先到先得）。
- **NFR-3 开销**：准入热路径为 O(1) 摊销（map + 原子/短锁），不得为按 namespace 记账新增 ES 调用或网络往返；归属信息来自鉴权本就要读取的数据。
- **NFR-4 可观测性**：所有拒绝、借用、退避、回收事件可通过指标与（采样）日志定位到 namespace、资源类与原因；指标基数有界。
- **NFR-5 可靠性**：租约释放、注册表 GC、Reconfigure 不得引入死锁/泄漏；竞态条件下以 `-race` 通过；记账与真实在途的偏差（如断连排空窗口）可解释且有上限。
- **NFR-6 可维护性**：预算内核资源无关、有完整单元测试；各接入层只做薄接线；遵循仓库 lint/headers/imports 规范，`mage check:all` 通过。
- **NFR-7 安全**：namespace 标签值原样来自受控文档字段；日志/指标输出需防基数爆炸与注入（标签清洗、长度与数量上限）；不因此暴露跨 namespace 的额外信息（健康端点仍需认证）。

## Constraints

- **Technical**：Go 版本以 [.go-version](file:///Users/ding/Documents/swe/0910125/project-11/.go-version) 为准；Mage 构建；`pkg/api`、`internal/pkg/api` 生成代码不得手改（openapi 不变，故预计无需生成）；不得新增被 blocklist 的依赖；FIPS 模式下不得引入不合规加密（本特性预计不涉及新加密）。
- **身份时序**：鉴权前只能做全局准入；namespace 准入只能发生在身份解析之后，upload begin 需先读 body 中的 agent_id（现状如此），artifacts/delivery 按 key 反查 agent（现状如此），不新增读取。
- **TLS 前不可得身份**：TCP/TLS 层无法按 namespace 分账。
- **批内顺序契约**：bulk flush 依赖链表节点顺序映射 ES items；跨归属拼批只能选择"批次归属构成"，不得破坏批次内节点次序。
- **Business**：保持对既有 Elastic Agent 各版本的协议兼容（poll 协商、错误码、关停语义不变）。
- **Dependencies**：仅用仓库已有依赖（x/sync、x/time/rate、ristretto 等），不引入新的第三方队列/调度库。

## Assumptions

- agent.namespaces 由 Kibana 在 enroll 与改派时维护，fleet-server 信任鉴权后读到的文档值；本次不做 namespace 真实性的交叉校验。
- 多 namespace agent 与空 namespace agent 均真实存在但占比低；D1 的规范化单键足以表达归属。
- 每个 Fleet Server 进程独立记账已能满足故障隔离目标；多 Pod 场景由负载均衡与每 Pod 容量共同提供统计意义上的公平。
- 不抢占在途长轮询带来的回收滞后（默认 ≤5 分钟，极端 poll_timeout ≤1 小时）可接受，并通过"停止新借入 + 借用上限"限制其影响。

## Acceptance Criteria

### AC-1: 归属键从已授权身份一致推导
- **Type**: `rule`
- **Given**: 一个携带有效 agent API key 的 checkin/ack/artifacts/file/upload/audit 请求，agent.namespaces 为 `["b","a","a"]`
- **When**: 鉴权成功后进入任意受管资源（HTTP 准入、bulk block、policy/action 订阅、缓存）
- **Then**: 全部层读到的归属键都是同一个规范化值 `a|b`（排序去重）；空 namespaces 时为 `default`；处理期间手动修改 agent 文档 namespaces 不影响本次记账
- **Pass Condition**: 单元测试覆盖规范化（排序/去重/空值/多值）与 context/block 快照不可变性，断言各层取到同一键
- **Evidence**: 归属工具包与接入点单测；代码审查引用 FR-1/FR-4/FR-5

### AC-2: 无身份与无 namespace 路径保持兼容
- **Type**: `rule`
- **Given**: pgp 请求、未认证 status 请求、enroll（静态策略令牌，key 无 namespaces）、未匹配路由
- **When**: 特性开关开启时访问这些端点
- **Then**: 不出现新的 4xx；归属一律落 `default`/共享桶；未匹配路由仍不限流不计数；认证 status 的细节不泄露给未认证调用方
- **Pass Condition**: 既有 handler/router 测试在开关开启下全部通过；新增 default 桶归属测试通过
- **Evidence**: `mage test:unit`（api 包）结果与新增测试

### AC-3: 单 namespace 与低负载等价
- **Type**: `rule`
- **Given**: 只有一个活跃 namespace（或全部 agent 均在 default）且负载低于全局容量
- **When**: 运行与改造前相同的基准/测试
- **Then**: 无新增拒绝、无 Borrowed 占用长期为正、错误体/状态码/指标名与改造前一致
- **Pass Condition**: 既有全量单测不修改断言通过；预算指标显示 guarantee=全局容量、borrowed=0、rejected=0
- **Evidence**: `mage test:unit`；指标快照断言

### AC-4: HTTP 突发下的保证份额与借用回收
- **Type**: `rule`
- **Given**: namespace A、B 各有保证份额 G（全局容量 C=2G），A 持续发起 checkin 长轮询直到占满 C（B 空闲期间借用）
- **When**: B 开始发起 checkin，且 A 仍持续突发
- **Then**: B 在其保证份额内的新 checkin 成功（A 占用被压回约 G + 受限借入：A 新借被拒、在途 poll 不被中断）；A 超出部分收到与现状同形态的 429 + Retry-After；B 空闲时 A 可重新借满
- **Pass Condition**: 构造 A 占满→B 进入→A 被限、B 成功率达标→B 停止→A 可再借满 的时序单测/集成测试通过；429 响应体与现状逐字节一致（除新增头）
- **Evidence**: limit/预算内核测试 + checkin handler 准入测试；429 body 快照

### AC-5: 授权拒绝语义不变
- **Type**: `rule`
- **Given**: 无 key/坏 key/未启用 key/inactive agent/agent id 不匹配等请求
- **When**: 特性开启
- **Then**: 分别仍返回现有的 401/403 及错误体；这些拒绝不计入任何 namespace 预算拒绝指标；预算拒绝永远是 429
- **Pass Condition**: 既有 auth/error 映射测试全通过；新增"预算拒绝不改变 401/403"测试
- **Evidence**: `mage test:unit`（api 包）

### AC-6: ack/文件突发不影响其他 namespace 的心跳与分发
- **Type**: `rule`
- **Given**: A 以大 body 持续打 ack 与 file delivery（含慢下载占槽）
- **When**: B 同时进行 checkin、接收策略变更与 action
- **Then**: B 的 checkin 成功、policy/action 在有全局余量时按时送达；A 在超出份额时被 429；各资源类预算独立（ack 耗尽不扣减 checkin 保证份额）
- **Pass Condition**: 资源类隔离测试通过（A ack 饱和时 B checkin/分发成功）
- **Evidence**: handler 级与预算内核测试

### AC-7: bulk 队列位置按归属准入且不阻塞
- **Type**: `rule`
- **Given**: A 持续提交 bulk op 超过其每 namespace dispatch 预算
- **When**: A、B 并发 dispatch，入口 channel 容量有限
- **Then**: A 超额 op 立即得到可重试 429 结论（不阻塞在 channel send）；B 的 op 始终能进入队列；block 上固化的归属为领取时值；断连提交者的 block 归属不悬空
- **Pass Condition**: bulk 包单测：A 超额快速失败、B 不受影响、归属快照断言、ctx 取消后记账释放断言
- **Evidence**: bulk 包 `go test -race`

### AC-8: ES 变慢时 flush 槽公平与故障归属退避
- **Type**: `rule`
- **Given**: ES 对 A 的 op 持续返回 429/慢响应（用注入的慢/失败 transport）
- **When**: A、B 持续写入
- **Then**: flush 调度跨归属轮转，B 的 op 完成；A 达到阈值后进入 per-ns 退避（新 op 快速失败、指标记录 backoff），B 不进入退避；A 退避到期自动解除；整批失败不错误结论 B 的 item
- **Pass Condition**: 注入式单测覆盖：A 退避开启/解除、B 全程成功、批内混合 item 成败按归属分别回填
- **Evidence**: bulk engine 测试（-race）

### AC-9: 部分失败只结论责任归属
- **Type**: `rule`
- **Given**: 一个混合归属 bulk 批次中 ES 对部分 item 返回 429/4xx
- **When**: 批次响应回填
- **Then**: 仅失败 item 的提交归属收到失败/可重试结论与计数，成功 item（含其他归属）正常成功；无归属被"连带"退避（除非其自身失败率超阈值）
- **Pass Condition**: 复用/新增 opBulk 逐条回填测试，混合归属断言
- **Evidence**: bulk 包单测

### AC-10: 断连风暴不泄漏
- **Type**: `rule`
- **Given**: A 的大量请求在 op 入队后、flush 前取消连接
- **When**: 反复制造取消并等待排空窗口
- **Then**: 每 namespace 的在途 dispatch/flush 占用最终回到取消前基线（允许 ≤15s 排空窗口偏差），A、B 后续准入均正常；租约 Release 幂等
- **Pass Condition**: 压力式单测断言占用归零、`-race` 无竞态、Release 重复调用安全
- **Evidence**: bulk + 预算内核测试

### AC-11: 策略/动作调度公平与扇出隔离
- **Type**: `rule`
- **Given**: A 有大量策略更新且其订阅者消费缓慢；B 有一次策略变更/action
- **When**: 调度器持续下发
- **Then**: B 的 policy clone/action 扇出不被 A 的队列/慢消费者长期阻塞（公平出队、扇出轮次不全局等待）；A 超预算的扇出按现状丢弃并记 per-ns 指标；全局 policy/action limit 仍生效
- **Pass Condition**: policy monitor 与 dispatcher 单测：A 拥塞时 B 的下发时延有上界；慢订阅者不阻塞其他订阅者当轮通知
- **Evidence**: policy/action 包测试

### AC-12: 缓存份额保护
- **Type**: `rule`
- **Given**: A 灌入大量缓存对象占满全局 cost
- **When**: B 写入其保证份额内的对象并触发驱逐
- **Then**: B 保证份额内的键存活（A 借用条目先被驱逐）；共享分片（鉴权前 APIKey）行为不变；单 namespace 下命中率/驱逐与现状等价
- **Pass Condition**: cache 包分片/驱逐单测
- **Evidence**: cache 包测试

### AC-13: 指标与健康端点可解释
- **Type**: `rule`
- **Given**: 制造 A 借用、被拒、退避与 B 正常服务的混合负载
- **When**: 抓取 `/metrics` 并请求已认证 `/api/status`
- **Then**: 全局与每 namespace（含 `_other` 聚合）的在途/等待/拒绝分原因/借用/退避指标齐备且数值与造数一致；status 预算节逐 namespace 列出份额/占用/借用/拒绝/退避；现有指标无改名/改类型；未认证 status 无预算细节
- **Pass Condition**: metrics 测试对标签/数值断言；status handler 测试断言认证差异
- **Evidence**: api 包测试与抓取样例

### AC-14: 配置变更续存与重启基线可解释
- **Type**: `rule`
- **Given**: 预算注册表运行中有在途占用与借用
- **When**:（a）触发 server 配置热变（runServer 重建）；（b）进程重启
- **Then**: （a）注册表实例续存、在途记账不丢、新参数原地生效；（b）重启后按配置重建为占用=0/借用=0/退避清零的基线，启动日志与 status 明确显示生效配置；配置非法（负值等）启动校验失败
- **Pass Condition**: 配置校验单测 + Reconfigure 单测；fleet 层续存的注入式测试；重启语义由启动日志/状态断言（集成或构造测试）
- **Evidence**: config/server 包测试

### AC-15: 开关关闭时行为完全回退
- **Type**: `rule`
- **Given**: namespace 预算总开关关闭
- **When**: 运行全部既有测试与多 namespace 负载
- **Then**: 不存在任何 per-ns 拒绝/退避/缓存分片差异；新增代码路径退化为单全局桶；新增指标为零值/不影响抓取
- **Pass Condition**: `mage test:unit` 全量通过 + 开关关闭场景的预算内核测试
- **Evidence**: 全量单测与配置测试

### AC-16: 公平性整体效果（负载质量维度）
- **Type**: `rubric`
- **Dimension**: 在对抗性"吵闹邻居"负载（A 长轮询打满 + ack 风暴 + ES 慢/429 + 断连）下，B 的服务可用性（准入成功率、checkin/policy/action 时延、bulk 完成率）相对改造前的改善与稳定性
- **Scale**: 1-5
- **Anchors**: 1 = B 仍随 A 一起超时/被拒，无隔离；3 = B 大部分时间可用但在 A 极端突发时仍有明显饥饿或回收滞后不可控；5 = B 在保证份额内稳定可用，拒绝与退避精准限于 A，回收滞后有界且可观测
- **Pass Threshold**: >= 4
- **Evidence**: 集成/压力测试（可用 httptest + 注入 bulker/transport）报告：A/B 成功率与时延分布、借用/回收时序、指标对照

### AC-17: 工程质量与可维护性
- **Type**: `rubric`
- **Dimension**: 预算内核的资源无关性、接入层薄度、测试覆盖与并发正确性
- **Scale**: 1-5
- **Anchors**: 1 = 公平逻辑散落各包、重复实现、无测试；3 = 内核存在但接入耦合明显或测试有缺口；5 = 单一资源无关内核 + 薄接线 + 热路径/竞态/回收/重配完整单测，`mage check:all` 与 `mage test:unit` 通过
- **Pass Threshold**: >= 4
- **Evidence**: 代码结构、`mage check:all`、`mage test:unit`、-race 测试结果

## Open Questions

- [ ] Q1（实现期默认按此执行，可复核）：预算配置段命名倾向 `server.namespace_budgets`；资源类粒度默认对齐现有 12 条路由 + bulk(dispatch/flush) + policy + action + cache，不提供更细粒度自定义。
- [ ] Q2：默认开关默认开启（单 ns/低负载已被证明等价）还是先默认关闭一个版本用于灰度？倾向前者，但需在 Plan 评审时确认发布风险偏好。
- [ ] Q3：Retry-After 具体取值（退避剩余秒数 vs 固定 1s）与是否在所有 429 路径统一，实现时以兼容现有 agent 解析为准并做版本确认。
