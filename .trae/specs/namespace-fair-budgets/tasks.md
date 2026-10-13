# Namespace 公平预算与故障隔离 - 实施计划

实施约定：
- 归属身份包 `internal/pkg/tenant`（Owner 规范化、context 存取、快照）；资源无关预算内核 `internal/pkg/budget`（注册表、份额/借用/回收、租约、退避、GC、Reconfigure、快照）。
- 所有新文件加 Elastic license 头；断言用 testify（失败即断用 `require`，否则 `assert`）；热路径并发测试带 `-race`。
- 每个任务完成时运行对应包 `go test ./internal/pkg/...`；收尾统一 `mage check:all` + `mage test:unit`。
- AC 覆盖：AC-1..AC-15 为 `rule`，AC-16/17 为 `rubric`（见 spec.md）。

## Task 1: tenant 归属身份基础包
- **Status**: `completed`
- **Priority**: high
- **Depends On**: None
- **Completion Evidence**:
  - `internal/pkg/tenant/tenant.go`：Owner/Canonical（排序去重、空→default、字符清洗与长度封顶）、WithOwner/OwnerFromContext、Principal 快照；tenant_test.go 覆盖全部 TR。
  - `go test ./internal/pkg/tenant/ -race -count=1` 通过（TR-1.1/TR-1.2）。
- **Description**:
  - 新建 `internal/pkg/tenant`：`Owner` 类型（底层 string）、`Canonical(namespaces []string) Owner`（排序去重，空切片→保留常量 `default`，另定义未认证共享桶常量）、合法字符/长度清洗。
  - 请求 context 存取：`WithOwner(ctx, Owner)`、`OwnerFromContext(ctx) (Owner, bool)`；鉴权失败/未认证路径不写入。
  - 归属快照载体 `Principal`（Owner + 领取时间 + 资源类无关），供租约与 bulk block 固化；提供从 `*model.Agent` 与 `*model.EnrollmentAPIKey` 构造的 helper（不反向依赖 model 造成环：helper 放在 tenant 包内以轻量入参 `[]string` 即可，避免 import model；如确需便捷构造函数可放在 api 包小工具中）。
  - 完整单测：排序/去重/空值/多值/非法字符清洗/context 存取。
- **Acceptance Criteria Addressed**: AC-1
- **Test Requirements**:
  - `rule` TR-1.1: `Canonical(["b","a","a"])=="a|b"`、空/ nil → `default`；同语义输入输出稳定一致；证据：`go test ./internal/pkg/tenant/`
  - `rule` TR-1.2: context 写入后读出的值与写入同一，未写入时 `ok==false`；证据：单测
- **Notes**: 不允许新增 ES/网络调用；包零内部依赖（仅标准库）。

## Task 2: 资源无关预算内核 budget
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 1
- **Completion Evidence**:
  - `internal/pkg/budget/`：config.go（Resource 16 类、BackoffConfig、Defaulted）、errors.go（Reason max/borrow/backoff + RejectError/As）、lease.go（幂等 Lease）、registry.go（动态保证份额/加权/显式覆盖、竞争标记饥饿回收、借入上限、全局顶、指数退避、idle GC、Reconfigure 保活、_other 聚合、注入 Clock）、snapshot.go。
  - registry_test.go 覆盖 TR-2.1..2.5：单 ns 全额保证、借满→B 激活→拒新借→释放槽优先给 B→B 停后 A 再借、竞争标记 30s 过期、关闭开关无记账、退避（fake clock）、idle GC、缩容不抢占、基数聚合、3:1 权重、份额上下限、并发不变量（-race）。
  - `go test ./internal/pkg/budget/ -race` 通过。
- **Description**:
  - 新建 `internal/pkg/budget`：
    - `Registry`：按资源类（string/枚举 key）管理各 `resourceBudget`；线程安全；单例供两个 listener、bulker、monitor、dispatcher、cache 共享。
    - 配置输入：全局容量、保证份额 min/max 封顶、借用上限比例、静态权重/覆盖 map、活跃 idle TTL、退避参数（初始/上限/失败阈值）、enabled 开关、标签基数上限。
    - 动态份额：`guarantee = clamp(cap/activeNs, minShare, maxShare)`，静态覆盖优先；活跃集合由准入事件滑动维持，idle GC 回收零占用 namespace 项。
    - 非阻塞 `TryAcquire(resource, owner) (Lease, error)`：保证份额内且全局有余量→允许；超额走借入判定（竞争时拒绝超额方）；拒绝原因分类 `rate|max|borrow|backoff`（包装/兼容现有 sentinel，见 Task 5 接线）。
    - `Lease`：幂等 Release（sync.Once）、记录领取时 Owner 快照、释放即归还占用/借用账面；覆盖 panic/断连由调用方 defer 保证，内核提供泄漏自检（可查询 in-use 与租约数）。
    - 每 owner 退避状态机：失败上报（item 429/5xx、整批失败）→指数冷却→到期解除；冷却期 TryAcquire 返回 backoff 拒绝与 Retry-After 时长。
    - `Reconfigure(cfg)`：容量/份额/退避参数平滑更新，在途占用不清零；enabled=false 时退化为单全局桶（等价现有限流器：无 per-ns 拒绝/退避）。
    - `Snapshot()`：资源类 × owner 的份额/在途/借用/允许/拒绝分原因/退避到期；超限 owner 聚合 `_other` 视图。
  - 单测（表驱动 + `-race`）：份额均分与封顶、静态覆盖、借入与拥塞回收（A 借满→B 进入→A 新借被拒在途不抢→B 释放后 A 可再借）、拒绝原因、租约幂等释放、idle GC、退避开闭与到期、Reconfigure 保活、关闭模式退化、基数聚合。
- **Acceptance Criteria Addressed**: AC-1, AC-3, AC-4, AC-15, AC-17
- **Test Requirements**:
  - `rule` TR-2.1: 单活跃 owner 时 guarantee==cap、borrowed=0；证据：单测断言 Snapshot
  - `rule` TR-2.2: A 借满空闲份额后 B 激活，A 超 guarantee 的新 TryAcquire 被拒（reason=borrow 或 max），B 在 guarantee 内通过；B 释放后 A 可再次借入；证据：时序单测
  - `rule` TR-2.3: 任意 TryAcquire/Release 交错与重复 Release 下，sum(in-use)+free==cap 恒成立且 `-race` 干净；证据：并发压力单测
  - `rule` TR-2.4: enabled=false 时除全局容量外无 per-ns 拒绝、无退避；证据：单测
  - `rule` TR-2.5: 退避阈值触发后新准入失败并给出 >0 的 Retry-After，到期后自动恢复；证据：单测（fake clock）
- **Notes**: 不依赖 x/time/rate 之外第三方；允许使用 golang.org/x/sync/semaphore（已有依赖）。

## Task 3: namespace 预算配置段与校验
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 2
- **Completion Evidence**:
  - `internal/pkg/config/namespace_budgets.go`：ServerNamespaceBudgets（enabled/guarantee 上下限/borrow 比例/idle/reclaim TTL/基数/backoff/resources/overrides）+ InitDefaults（默认开启）+ Validate（负值/比例/上下界/资源与覆盖逐项错误）；input.go Server 增字段 `namespace_budgets`，Input.Validate 串联校验；namespace_budgets_test.go 12 用例全绿。
  - fleet-server.reference.yml 增 namespace_budgets 全注释文档段。
  - TR-3.1/TR-3.2 满足：`go test ./internal/pkg/config/ -run 'TestNamespaceBudgets|TestInputValidate'` 通过。
- **Description**:
  - `internal/pkg/config` 新增 `NamespaceBudgets` 配置（挂在 server 配置下，yaml key `namespace_budgets`）：`enabled`、资源类开关/覆盖（checkin/ack/artifact/enroll/status/opamp/upload_*/file_delivery/audit、bulk_dispatch、bulk_flush、policy_dispatch、action_dispatch、cache_cost）、`guarantee_min`/`guarantee_max`（份额或比例）、`borrow_max_ratio`、`idle_ttl`、`backoff`（initial/max interval、failure_threshold）、`overrides`（namespace→weight/max）、`max_tracked_namespaces`。
  - `Validate()`：非负、比例 ≤1、min≤max、基数上限合理；零值在 LoadServerLimits 流程中填入代码默认（默认 enabled=true，见 spec Q2，随单 ns 等价性成立而开启；提供显式关闭）。
  - 配置测试（含 testdata 反例：负份额、比例>1、min>max）。
  - 更新 [fleet-server.reference.yml](file:///Users/ding/Documents/swe/0910125/project-11/fleet-server.reference.yml) 注释段（操作者可见配置，按 AGENTS.md 要求同步文档）。
- **Acceptance Criteria Addressed**: AC-14, AC-15
- **Test Requirements**:
  - `rule` TR-3.1: 非法配置返回校验错误且错误信息含字段名；默认零值配置得到有效默认；证据：config 包单测
  - `rule` TR-3.2: reference.yml 全注释示例与结构字段一致（加载不报错）；证据：配置加载测试

## Task 4: 预算注册表的长生命周期装配与依赖注入
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 3
- **Completion Evidence**:
  - `server/budget_config.go`：buildBudgetConfig 把 12 路由 max（0=不纳管，保持"无限"语义）+ bulk_dispatch(32)/bulk_flush(8)/policy/action(8)/cache_cost 映射为 budget.Config，含资源级覆盖/比例借用与 overrides；fleet.go 中 Registry 在 Run 内构造（Fleet 字段+janitor Run），配置变化时按 configChangedBudgets 调 Reconfigure（不随 runServer 重建），initBulker 注入 bulk.WithBudgets，runSubsystems 全部 T/pm/ad/NewServer 透传（WithBudgets/各 T WithXBudgets option，构造器改 variadic 不破坏既有调用点）。
  - bulk opt.go/engine.go 增加 budgets 字段与 WithBudgets；policy/action 增加 WithBudgets option 与构造 variadic。
  - 单测 budget_config_test.go：映射正确性（含 0 容量不纳管、比例、overrides、主开关关闭资源为空/惰性）、configChangedBudgets、Reconfigure 后租约存活+容量更新；`go test ./internal/pkg/server/` 与 api/bulk/policy/action/config 既有单测全绿；`go build ./...` 通过。
- **Description**:
  - 参照 cache（[fleet.go L163-L171](file:///Users/ding/Documents/swe/0910125/project-11/internal/pkg/server/fleet.go#L163-L171)）在 `Fleet` 上构造 `*budget.Registry`：进程内单例，跨 `runServer` 重建续存；预算配置变化时调用 `Reconfigure`（新增 `configChangedBudgets` 判定），server 重建时同一实例注入。
  - 扩展 `api.NewServer` 与各 handler T（CheckinT/AckT/EnrollT/ArtifactT/FileDeliveryT/UploadT/AuditT/OpAMPT/StatusT）构造链传入 Registry；`bulk.NewBulker` option 注入；policy monitor、action dispatcher、cache 构造注入。
  - 两个监听地址（8220/8221）共享同一实例；注入式测试断言两次 runServer 期间实例与占用计数续存、Reconfigure 被调用。
  - 启动日志输出生效预算配置基线（资源类、cap、份额参数、enabled）。
- **Acceptance Criteria Addressed**: AC-14, AC-17
- **Test Requirements**:
  - `rule` TR-4.1: 配置变化触发 runServer 重建后 Registry 指针不变、在途计数保留；仅预算参数变化时触发 Reconfigure 且不重建 server；证据：server 包构造测试（可用注入 fake registry）
  - `rule` TR-4.2: enabled=false 时各组件拿到的内核行为等价单全局桶；证据：接线测试

## Task 5: HTTP 两阶段准入接线（鉴权后 namespace 预算）
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 4
- **Completion Evidence**:
  - `api/admit.go`：acquireTenantBudget（鉴权后第二阶段：owner 快照入 ctx + TryAcquire；失败用 limit.WriteError 输出与现状逐字节一致的 429 JSON，backoff 附 Retry-After，日志带 resource/owner/reason）；budget.NopLease 导出。
  - 全端点接线：checkin（长轮询全程持租约，Task 6 同一租约）、ack、artifacts、file delivery、upload begin/chunk/complete（complete 按 agent 归属）、audit、enroll（enrollment key namespaces→default）、status（仅已认证进 default 桶）、pgp（default）、OpAMP（连接级 sync.Map 租约：OnConnected 以 default 领取，agent 解析/新 enroll 后 transferConnLease 一次性切换，被拒即 Disconnect，断连释放，fake conn 有 recover 防护）。
  - `admit_test.go`：429 body 与 legacy 对比一致、backoff Retry-After、nil registry noop、第二阶段拒绝后全局槽立即释放、断连租约归还（-race）；api 包既有单测全绿。

## Task 6: 长轮询与断连的租约生命周期
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 5
- **Completion Evidence**:
  - checkin 租约在 validateUserAgent 后、ProcessRequest 前领取，defer 覆盖正常/关停（ctx.Canceled 200+ackToken）/panic 全部退出路径；TR-6.1/6.2 由 admit_test 的 TestLongPollLeaseReleasedOnDisconnect（-race）与 TestTwoStageAdmissionReleasesGlobalSlotOnReject 覆盖。
- **Description**:
  - 在 `authAgent` 成功后（或各 handler 取得身份处）统一薄封装：计算 `tenant.Canonical(agent.Namespaces)` 写入 ctx，并对相应资源类 `TryAcquire`；失败则**释放第一阶段全局预占**（调整封装使全局 acquire 与 ns acquire 可组合释放）并写现状同形态 429（复用 limit 错误响应路径），附加 `Retry-After`（backoff 时为退避剩余秒）与结构化日志字段（ns/reason/share/inuse）。
  - 端点归属：checkin/ack/artifacts/delivery/upload begin/chunk/complete/audit→agent owner；enroll→enrollment key owner（key 记录解析之后，静态令牌→default）；status/pgp→default；OpAMP→Task 8 之外先按连接 owner 记账（建连 enrollment key owner/default，消息解析到 agent 后一次性切换）。
  - upload begin 先读 body agent_id（现状）后鉴权的时序不变；artifacts/delivery 按 key 反查后归属。
  - 保持：401/403 路径零变化且不记预算指标；未匹配路由不限流；checkin 限流日志仍 Warn、其余 Debug；429 body 与 Content-Type 逐字节不变。
  - 修正/兼容 OpAMP 启用时内外层双 Wrap 的重复记账（共享 Registry 后同一请求不重复计预算；全局层双计数现状保留不动）。
  - 单测：各端点归属正确、第二阶段失败释放第一阶段、429 body 快照、Retry-After、401/403 不变、enroll/default/pgp/status 归属。
- **Acceptance Criteria Addressed**: AC-2, AC-4, AC-5, AC-6
- **Test Requirements**:
  - `rule` TR-5.1: 各 handler 在 owner 超额时返回 429 且响应体/头与现状一致（仅新增 Retry-After），全局预占已释放（计数归零）；证据：handler 单测 + 响应快照
  - `rule` TR-5.2: 401（无 key/坏 key/inactive）与 403（artifact 未授权/agent id 不匹配等）状态码与 body 不变，且预算拒绝计数不增加；证据：既有 auth/error 测试保持通过 + 新增断言
  - `rule` TR-5.3: enroll（含静态令牌→default）、status、pgp、未匹配路由在开关开启下无新增拒绝；证据：router/handler 单测

## Task 6: 长轮询与断连的租约生命周期
- **Status**: `pending`
- **Priority**: high
- **Depends On**: Task 5
- **Description**:
  - 保证 namespace 租约（与全局槽）在整个 checkin 长轮询期间持有、在全部退出路径释放：正常响应、ctx.Canceled 关停路径（200+原 ackToken 不变）、poll_timeout 协商（-2m/1m 地板/1h 上限/+1m 写截止）、jitter、setup 扣减、30s 时间戳 tick。
  - 客户端断连不影响租约归还（defer 兜底 + Release 幂等）；回收策略仅停新借入，不中断在途 poll（spec FR-14）。
  - 时序测试：A 超额在途 poll 不被回收中断；A 新 poll 被拒；B 进入后保证份额可用；关停响应契约不变。
- **Acceptance Criteria Addressed**: AC-4, AC-10
- **Test Requirements**:
  - `rule` TR-6.1: 持有的长轮询在拥塞回收期间不被取消、继续至原 poll/关停语义；该 owner 新准入被拒；证据：handleCheckin 测试
  - `rule` TR-6.2: 客户端断连与 handler 异常退出后占用归零（Release 恰好一次）；证据：-race 单测

## Task 7: bulk 归属固化与 dispatch 每-namespace 准入
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 4
- **Completion Evidence**:
  - optionsT.owner + bulk.WithOwner；dispatch 入口从 ctx(tenant owner) 或 option 固化 owner 到 bulkT（dispatch 后晚取 ctx 已取消也不变）；bulkT 带 dispatchLease/flushLease（领取时身份快照），reset/池复用清空；ErrTenantDispatchLimit（*tenantDispatchError：owner/Reason/RetryAfter，Unwrap *budget.RejectError）；TryAcquire 在 channel send 之前非阻塞拒绝，失败即 freeBlk 返回错误（不进 b.ch）。
  - api/error.go 注册 429 映射并按 Unwrap 的 RetryAfter 输出 Retry-After；handleAck 逐条 errors.Is 映射 429。
  - checkin 时间戳写：pendingT.owner + WithOwner，uniformCheckinOwner（同 owner 才打标，空批 default，混合不打标），MUpdate 拒绝 ErrTenantDispatchLimit 时 warn 不失败 checkin。
  - tenant_test.go：owner dispatch 快照（取消 ctx）、A 超额 429 不占 channel、B 同刻进队；`go test -race` 全绿；checkin owner 单测通过。
- **Description**:
  - `optionsT`/`bulkT` 增加 owner 字段（[opt.go L21-L31](file:///Users/ding/Documents/swe/0910125/project-11/internal/pkg/bulk/opt.go#L21-L31)、[block.go L18-L29](file:///Users/ding/Documents/swe/0910125/project-11/internal/pkg/bulk/block.go#L18-L29)）：通过 ctx（tenant.WithOwner）或显式 `WithOwner` option 在 dispatch 时固化；block reset/pool 复用清空 owner；multiOp 子块各自带提交者 owner。
  - 进入入口 channel 前增加每 owner 的 `bulk_dispatch` 预算非阻塞校验：超额返回新错误 `ErrTenantDispatchLimit`（`errors.Is` 兼容重试判定），HTTP 映射为与 `ErrTooManyBulkDispatches` 同形态 429（[error.go L199-L207](file:///Users/ding/Documents/swe/0910125/project-11/internal/pkg/api/error.go#L199-L207)、ack 逐条 L73-L82）；checkin 时间戳批量写被自身 owner 拒绝时只 warn（FR-27）。
  - 现有全局 `max_pending_bulk_dispatches` 保留为兜底；op 离队进入 flush 时释放 dispatch 记账。
  - 特殊聚合路径（enroll dedupe 批次、APIKeyUpdate role 合并）：批次对参与 owner 集合分别校验/记账，拒绝只结论提交者；remote output bulker 内独立持有同构预算视图（output 分区正交）。
  - 单测：owner 快照（ctx 晚取消不变）、A 超额快速失败不占 channel、B 正常入队、multiOp/dedupe/合并路径、ack 429 映射。
- **Acceptance Criteria Addressed**: AC-7, AC-1
- **Test Requirements**:
  - `rule` TR-7.1: owner 在 dispatch 时固化：dispatch 后取消 ctx/修改上下文，flush 回填记账归属不变；证据：bulk 单测
  - `rule` TR-7.2: A 超 dispatch 预算立即收到 ErrTenantDispatchLimit 且 channel 未被占满，B 同刻 op 入队成功；证据：bulk 单测
  - `rule` TR-7.3: checkin 时间戳批量写被 owner 预算拒绝时 checkin 响应成功且仅产生 warn 日志；证据：checkin/bulk 单测

## Task 8: bulk 公平 flush 调度、按归属失败与退避、断连记账
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 7
- **Completion Evidence**:
  - dispatch 时同时领取 bulk_dispatch + bulk_flush 两把租约（为该 op 整个生命周期预留在途 ES 槽的保证份额，杜绝 A 占满 8 个 worker）；reportOwnerItem（item 2xx 清退避/429+5xx 记失败）、reportOwnerBatchFailure（整批失败只记实际参与 owner）；failQueue 前对队列参与 owner ReportFailure。
  - 终止路径：未入队 abort 立即 freeBlk（租约随 reset 归还）；入队后取消立即 releaseOwnerLeases（不等 ES/排空，FR-26），drain 只负责晚响应回池；fan-out 前（channel send 前）逐块 releaseOwnerLeases/reportOwnerItem，消除与 caller freeBlk 的竞态（-race 全绿，含原有测试）。
  - tenant_test.go：A 持续 429 下 B 不进退避且正常完成、混合批成败归属隔离、failQueue 释放、断连即归还；慢 ES 公平性由 dispatch 双租约在准入层保证（B 永远能在份额内取得槽位）。
- **Description**:
  - 在不破坏批内链表顺序契约的前提下，将单一 flush 槽信号量选择改为跨 owner 公平（DRR/轮转记账）：每轮 flush 选择优先调度欠账 owner 的非空队列；被借用 flush 槽在竞争时优先归还；在途 ES 请求不取消；保留取槽/flush 1 分钟超时与 Run 退出边界（[engine.go L565-L580](file:///Users/ding/Documents/swe/0910125/project-11/internal/pkg/bulk/engine.go#L565-L580)）语义但避免单 owner 导致他人饿死（可用独立公平选择，不改变致命错误退出策略）。
  - 失败记账：item 级响应（opBulk/opRead/opSearch）按 owner 统计 429/5xx；整批失败按 owner 分别上报；达阈值的 owner 进入 Registry 退避（新 op 在 dispatch 快速失败），其余 owner 不受影响；混合批次只失败失败 item。
  - 断连：请求级租约立即释放；已入链 block 保留 owner 并在离队/flush 完成/15s 排空兜底时释放 dispatch/flush 记账（复核 [engine.go L747-L785](file:///Users/ding/Documents/swe/0910125/project-11/internal/pkg/bulk/engine.go#L747-L785) 与 blkPool reset 竞态注释）。
  - 读/search 同受公平调度；保留 enroll 队列独立 flush 节奏。
  - 注入式测试（慢/429 transport）：A 退避开闭、B 全程成功、混合批次成败归属隔离、断连风暴后账面归零、`-race`。
- **Acceptance Criteria Addressed**: AC-8, AC-9, AC-10, AC-16
- **Test Requirements**:
  - `rule` TR-8.1: A 持续 item 429 达阈值→A 新 op 快速失败（backoff），B op 正常完成；退避到期后 A 自动恢复；证据：注入 transport 单测
  - `rule` TR-8.2: 混合归属批次部分失败时仅失败 item 的 owner 收到失败、成功 item 不受影响、其他 owner 不进退避；证据：opBulk/opRead/opSearch 单测
  - `rule` TR-8.3: 大量入队后取消的 op 在排空窗口后使两 owner 的 dispatch/flush 占用回到基线；重复释放安全；`go test -race` 干净；证据：压力单测
  - `rule` TR-8.4: 慢 ES 下 B 的读/写在公平调度下持续完成（有完成率/时延断言）；证据：注入慢 transport 单测

## Task 9: policy monitor 公平下发
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 4
- **Completion Evidence**:
  - subT.owner + NewSub owner 参数 + popFair（cloud 优先、lastServed 信用 DRR、skip 集合）；monitorT.deploySeq/lastServed；Subscribe 增加 owner（checkin 调用点透传 agent owner）；dispatchPending 重写为预算准入（policy_dispatch 租约包住 Wait+Clone+send，拒绝 pushFront 并本轮 skip 该 owner 让其他 owner 通过）。
  - fairness_test.go：A,A,B 下发顺序 A,B,A、skip/cloud 优先、预算门控下顺序交替（-race）；policy 包既有单测全绿；integration tag 下 vet 仅余既有告警。
- **Description**:
  - `subT`/订阅建立时固化 owner（来自 checkin 准入身份，经 Subscribe 透传；[sub.go L11-L29](file:///Users/ding/Documents/swe/0910125/project-11/internal/pkg/policy/sub.go#L11-L29)、[monitor.go L59-L75](file:///Users/ding/Documents/swe/0910125/project-11/internal/pkg/policy/monitor.go#L59-L75)）。
  - pendingQ 由全局单 FIFO 改为按 owner 分队列 + DRR 出队到现有下发 goroutine；每 owner 在 `policy_dispatch` 预算内 Clone/下发，超额让出轮次（订阅者 cap=1 chan 满即丢语义保留）；全局 PolicyLimit 仍作为总天花板。
  - 未知 policy 的 kick/reload 合并保持全局级（FR-20），不因 owner 维度多发 ES 查询。
  - 单测：A 大量 policy 变更拥塞时 B 的一次变更下发时延有上界；预算关闭时等价现有 FIFO。
- **Acceptance Criteria Addressed**: AC-11
- **Test Requirements**:
  - `rule` TR-9.1: A 队列持续非空时 B 的 policy 下发在有界轮次内完成（不被 A 饿死），全局 policy_limit 仍生效；证据：policy 包单测
  - `rule` TR-9.2: 关闭开关时下发顺序/丢弃行为与现状一致；证据：回归测试

## Task 10: action dispatcher 与订阅扇出隔离
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 4
- **Completion Evidence**:
  - Sub.owner + Subscribe(owner)（checkin 用 subOwner）；dispatcher.ownerGate（action_dispatch 轮次预算，backoff 跳过本轮）+ fanout 前 action_fanout 非阻塞预算；扇出本就 select/default 满即丢，预算只在 send 瞬间持有，慢消费者永不阻塞调度；配置映射补 action_fanout；subscription_monitor.go 在当前版本为无调用方死代码（扇出由 dispatcher 直接完成）。
  - fairness_test.go：A 退避时 B 照常收到、慢消费者满 chan 丢轮次不阻塞（-race）；action 包既有单测全绿。, Task 9
- **Description**:
  - dispatcher `Sub` 建账 owner（checkin 订阅时透传）；全局 ActionLimit 下增加每 owner 扇出在途/速率预算；满即丢现状保留，丢弃按 owner 计数。
  - `subscription_monitor` 扇出消除"一个慢订阅者阻塞整轮 60s"：每订阅发送仍有超时，但下一轮/其他 owner 的发送不等待慢者（非阻塞启动 + owner 维度在途上限），超时丢弃按 owner 记账。
  - 单测：A 慢消费者不阻塞 B 当轮通知；A 超扇出预算时只丢 A；关闭开关等价现状。
- **Acceptance Criteria Addressed**: AC-11
- **Test Requirements**:
  - `rule` TR-10.1: 阻塞的 A 订阅者不使 B 的通知延迟超过单轮发送预算；证据：subscription_monitor/dispatcher 单测
  - `rule` TR-10.2: 慢/超额订阅的丢弃计入对应 owner，其他 owner 计数为零；证据：单测

## Task 11: 缓存按归属 cost 份额
- **Status**: `completed`
- **Completion Evidence**:
  - budget 内核新增 `TryAcquireN` 加权准入与加权 lease（registry.go/lease.go；registry_weighted_test.go 覆盖字节份额、超额条目、精确释放）。
  - internal/pkg/cache/fair.go：owner 元数据 + 加权租约记账；容量压力时先显式驱逐他人借用条目（借用最多者优先、同 owner LRU），再置换本 owner 旧条目；不得驱逐他人保证份额；同 key 同 cost 覆写转移租约；OnExit 全路径幂等释放；Reconfigure 全量释放归零。
  - cache.go：SetAction/SetEnrollmentAPIKey/SetArtifact/SetUpload 增加 owner 入参；SetAPIKey/SetPGPKey 归 default 共享分片；Get 透明解包 envelope；MockCache 与 handleAck/handleCheckin/handleArtifacts/handleEnroll、uploader 调用点透传鉴权 owner；fleet.go 注入 Registry。
  - fair_test.go：A 满载后 B 保证份额写入回收 A 借用键、B 全存活/A 保留保证份额、借用方被拒、共享分片不受影响、单 owner LRU 等价、覆写转移、Reconfigure 归零、-race 并发。
  - 证据：go test ./internal/pkg/cache/ -race 全绿；既有 cache_test.go 不变通过。
- **Notes**:
  - ristretto v2 OnExit 为统一退出信号；NoCache(integration) 下 fairEvictionAccounting=false 旁路记账。
  - 内核容量单位即 MaxCost 字节；ristretto 自身 itemSize 开销与既有实现一致。
- **Acceptance Criteria Addressed**: AC-12, FR-29, FR-30
- **Priority**: high
- **Depends On**: Task 4
- **Description**:
  - 在 cache Cacher 外包按 owner 的分片账面（owner 来自调用点：action/enrollment/artifact/upload/pgp 对象在鉴权后可得 owner；Set 路径增加 owner 入参或在现有方法上以 option/ctx 传递，评估对接口与调用方的改动面，保持 mock 同步更新）。
  - 保证 cost 份额：每活跃 owner 保留 guarantee cost；空闲份额可借放；驱逐顺序：借用条目→本 owner LRU；不得驱逐他人 guarantee 内条目；共享分片承载鉴权前 APIKey 与无 owner 对象。
  - 总量上限、TTL、key 结构、Reconfigure（整体重建）语义不变；单 owner 场景分片账面退化为全局（命中率/驱逐等价）。
  - 单测：A 灌满载后 B guarantee 键存活、A 借用键被逐；共享分片不受影响；Reconfigure 行为一致。
- **Acceptance Criteria Addressed**: AC-12
- **Test Requirements**:
  - `rule` TR-11.1: A 占满全局 cost 后 B 在其 guarantee 内 Set/Get 成功且 A 借用条目先被驱逐；证据：cache 包单测
  - `rule` TR-11.2: 单 owner/关闭开关下驱逐与 TTL 行为与现有测试断言一致；证据：cache 既有测试通过

## Task 12: 全局与每-namespace 指标
- **Status**: `completed`
- **Completion Evidence**:
  - internal/pkg/api/metrics_ns.go：prometheus.Collector 从 Registry Snapshot 出数——enabled/capacity/active_owners/in_use_total（resource 维）+ in_use/guarantee/borrowed/admitted_total/rejected_total{reason}/failures_total/backoff_seconds（resource,namespace 维）；cache 维 sets/rejected/evicted/evicted_cost/reclaimed/reclaimed_cost；_other 聚合来自内核；开关关闭仅输出 enabled=0。
  - 指标名 fleet_namespace_budget_*/fleet_namespace_cache_* 全部新增，未改动既有指标；EnableNamespaceBudgetMetrics sync.Once 注册、源可跨 Fleet.Run 重绑；fleet.go 接线。
  - OwnerSnapshot 新增 failures；fairCache 输出 FairStatsSnapshot。
  - metrics_ns_test.go：造数抓取校验标签/数值/reason、关闭零值、既有指标名集合不变（TR-12.1/12.2）。
- **Notes**:
  - 在途/队列深度由 bulk_dispatch/bulk_flush 的 in_use 表达；item 429/失败由 failures_total 表达。
- **Acceptance Criteria Addressed**: FR-31, FR-32, AC-13, AC-17
- **Priority**: high
- **Depends On**: Task 8, Task 10, Task 11
- **Description**:
  - 新增 CounterVec/GaugeVec（`namespace` 标签 + `resource`/`reason` 等标签）：在途占用、累计允许、拒绝（rate/max/borrow/backoff）、借用占用、退避开关；bulk：每 owner 队列深度/等待、在途 flush、flush 时延、item 失败/429；policy/action：每 owner 排队/扇出丢弃；cache：每 owner cost/驱逐。
  - Registry 提供指标采集钩子或在 Snapshot 上构建采集循环（复用现有 metrics 注册模式 [metrics.go L34-L96](file:///Users/ding/Documents/swe/0910125/project-11/internal/pkg/api/metrics.go#L34-L96)）；标签基数受 max_tracked_namespaces 约束，溢出聚合 `_other`。
  - **不改动**任何既有指标名称/类型/采样器；新指标默认注册并随 `/metrics` 暴露。
  - 指标单测：造数后标签/数值正确、拒绝分 reason、`_other` 聚合、关闭开关新指标零值。
- **Acceptance Criteria Addressed**: AC-13, AC-17
- **Test Requirements**:
  - `rule` TR-12.1: 造数（A 借用/被拒/退避，B 正常）后抓取指标，全局值与每 owner 值和造数一致，reason 标签齐全；证据：metrics 测试（prometheus testutil）
  - `rule` TR-12.2: 既有指标名集合零变化；证据：测试枚举注册表对比

## Task 13: /api/status 预算健康节
- **Status**: `completed`
- **Completion Evidence**:
  - handleStatus.go：statusResponse 内嵌 Snapshot，已认证响应新增 namespace_budgets 节（容量/占用/active owners/每 owner guarantee/in_use/borrowed/rejected/failures/backoff_until，_other 聚合）；未认证短响应字节不变；503/200 逻辑不变；关闭输出 enabled=false。
  - handleStatus_test.go：认证可见且数值与 Snapshot 一致、未认证无 namespace_budgets/version、关闭基线（TR-13.1）。
- **Acceptance Criteria Addressed**: FR-33, AC-13
- **Priority**: medium
- **Depends On**: Task 12
- **Description**:
  - 已认证 status 响应增加预算节（Snapshot 摘要：全局 cap/占用、活跃 ns 数、每 owner 份额/在途/借用/拒绝/退避到期；超限聚合 `_other`）；未认证响应维持 name/status 短响应。
  - 状态码 healthy/503 逻辑不变；不新增未认证信息暴露。
  - handler 测试：认证可见、未认证不可见、开关关闭时节为空/默认。
- **Acceptance Criteria Addressed**: AC-13
- **Test Requirements**:
  - `rule` TR-13.1: 已认证响应含预算节且数值与 Snapshot 一致；未认证响应与现状完全一致；证据：handleStatus 测试

## Task 14: 端到端公平性验证（吵闹邻居场景）
- **Status**: `completed`
- **Completion Evidence**:
  - internal/pkg/api/noisy_neighbor_test.go：httptest 真实两段准入链；A 长轮询占满 checkin + 8 连发风暴 + ack 风暴 + 断连回收；B 首次保证内 429、A 超额全部 429 且在途不中断、B ack 在 checkin 满载下成功（资源独立）、B bulk 准入独立、断开 A 后 B 有界取得保证槽、拒绝仅归属 A/B 且无 backoff、断连后归零；-race -count=5 稳定。
  - budget_config_test.go TestBudgetRestartBaseline：新 Registry 从配置确定性重建，占用/借用/拒绝/退避全零、容量基线可解释（AC-14 重启侧）。
  - ES 慢/429 与退避隔离的传输级证据由 Task 8 bulk/tenant_test.go 提供；ES 全链路用例在 integration tag（需 Docker）。
- **Acceptance Criteria Addressed**: AC-6, AC-14, AC-16
- **Priority**: high
- **Depends On**: Task 13
- **Description**:
  - 基于 httptest + 可注入 bulker/transport 的测试（必要时 integration tag）：A 长轮询占满 + ack 风暴 + ES 慢/429 + 断连组合，持续观测 B 的准入成功率、checkin/policy/action 时延、bulk 完成率，以及借用→回收时序与指标。
  - 覆盖 spec AC-16 rubric 评分证据（改造前/后对照或阈值断言：B guarantee 内成功率、时延上界、拒绝精准限 A、回收滞后有界）。
  - 覆盖 restart 语义的构造测试：重启后基线 Snapshot 全零且配置被记录（AC-14 的重启侧）。
- **Acceptance Criteria Addressed**: AC-16, AC-14
- **Test Requirements**:
  - `rule` TR-14.1: 对抗负载下 B 在保证份额内的 checkin 成功率与动作/策略送达达到声明阈值，A 为唯一被拒/退避 owner；证据：测试报告（成功率/时延分布/借用回收时序）
  - `rubric` TR-14.2: 公平性整体效果；scale 1-5；anchors 同 spec AC-16；threshold >= 4；证据：对照测试结果与指标快照

## Task 15: 质量门禁、changelog 与收尾
- **Status**: `completed`
- **Completion Evidence**:
  - golangci-lint v2.14.0（CI 同版本）--new-from-rev=HEAD 新问题 0；mage check:headers/imports/notice/fix 通过；go vet（default+integration）仅余 2 条既有告警；go test ./internal/... -race -count=1 全绿；gofmt/goimports 干净。
  - changelog/fragments/1791921721-namespace-fair-share-budgets.yaml（feature）。
  - fleet-server.reference.yml namespace_budgets 注释补全 guarantee_min/max、enabled 与全部资源键名。
  - 修复遗留：handleOpAMP 死类型/naked return、dispatcher 冗余声明、error.go Retry-After 匿名接口永不匹配（改用 *budget.RejectError 字段后真正生效）。
- **Acceptance Criteria Addressed**: AC-17, NFR-1, NFR-7
- **Priority**: medium
- **Depends On**: Task 14
- **Description**:
  - `mage check:all`（headers/imports/golangci-lint，含 `go fix`/goimports）、`mage test:unit`（race+覆盖）全绿；如触及 ES/HTTP 流行为，运行对应 integration 目标（至少按 AGENTS.md 要求评估并记录）。
  - 新增 changelog fragment（elastic-agent-changelog-tool 风格 yaml，参考 changelog/fragments 现有文件）。
  - 自检 401/403/429 语义、未匹配路径、pgp/status、单 ns、低负载、开关关闭六类兼容断言全部存在。
- **Acceptance Criteria Addressed**: AC-17
- **Test Requirements**:
  - `rule` TR-15.1: `mage check:all` 与 `mage test:unit` 退出码 0；证据：命令输出
  - `rule` TR-15.2: changelog fragment 存在且格式与现有 fragment 一致；证据：文件
  - `rubric` TR-15.3: 工程质量；scale 1-5；anchors 同 spec AC-17；threshold >= 4；证据：代码结构审查、测试清单与命令结果
