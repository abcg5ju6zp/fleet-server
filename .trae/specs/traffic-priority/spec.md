# Fleet Server 流量优先级与过载保护 - 产品需求文档

## Overview

- **Summary**：为 Fleet Server 增加一套可开关、可热更新的「控制流量 / 后台流量」分级过载保护机制。开启后，Agent check-in、Action 回执（acks）、OpAMP 心跳及其必需的 Elasticsearch 读写作为**控制流量（control）**，诊断文件上传、文件下载、制品（artifacts）获取作为**后台流量（background）**；在 HTTP 入口、bulk 批处理调度、Elasticsearch 传输（含重试）三个层面为 control 保留容量，空闲保留份额允许 background 借用，control 到达时新的 background 借用立即停止；在途工作不被重分类、不被强制终止。
- **Purpose**：大规模部署中运维集中上传诊断包、下发文件或拉取制品时，这些后台传输可能占满连接、bulk flush 并发与 ES 连接池，导致 Agent check-in/acks/OpAMP 心跳被拖慢甚至超时。需要保证即使系统过载，策略分发与动作确认仍持续推进，同时不牺牲低负载/未启用时的任何现有行为。
- **Target Users**：Fleet Server 运维人员（配置容量、查看状态与指标）；大规模 Fleet 部署中的已注册 Elastic Agent（间接受益）。

## Goals

- 过载保护开启时，control 流量在入口与下游都拥有可证明的保留容量与优先调度权。
- background 流量可借用 control 空闲份额，借用是「尽力而为」的：control 请求出现后新借用立即停止，已借工作自然完成。
- 流量类别穿过：HTTP 入口 → check-in 长轮询 → bulk 批处理 → ES transport 重试。
- background 队列长期繁忙或 ES 变慢时 control 仍有 bounded 处理机会；任一类都不能长期独占共享容量。
- 配置原子热更：只影响新请求；在途请求按领取时规则结束；坏配置沿用上一组有效限制，不重启、不致命。
- 等待/在途期间客户端断开，及时释放尚未使用的份额。
- 指标（Prometheus/libbeat）与实时状态快照分别暴露各级别在各阶段的占用、等待、借用、拒绝与下游排队。
- **未启用或低负载时，现有端点、响应、认证、每路由限制、处理顺序零变化。**

## Non-Goals

- 不改变任何对外 HTTP 契约：端点、请求/响应体、状态码语义、鉴权逻辑、OpenAPI 生成代码均不修改（过载拒绝仍返回与现有 429 一致的响应体形态）。
- 不调整现有 per-route 限流器（令牌桶 + `max` 并发信号量）的默认值与语义；新机制是叠加层而非替换层。
- 不做 TCP/TLS 层的流量分类：TLS 握手前看不到 HTTP 路径，`limit.Listener` 维持「总连接硬顶」角色不变。
- 不做网络带宽整形/QoS，不暂停、不取消、不重分类已经开始的后台传输。
- 不要求 elastic-agent SDK 侧做任何改动。
- 不为 remote output bulker 提供独立的分级容量配置（它们默认按 control 对待并共享同一套 ES 传输闸；按 output 调参留待后续）。
- GC/定时任务等内部 ES 流量不新增类别，默认按 control 对待，保证不被饿死。

## Background & Context

现状关键事实（均有代码依据）：

- 入口限流：每端点一个 `limit.Limiter`（令牌桶 + `semaphore.Weighted`，`Acquire` 立即返回 `ErrRateLimit/ErrMaxLimit` → HTTP 429，不排队），见 [limiter.go](file:///Users/ding/Documents/swe/0910125/project-12/internal/pkg/limit/limiter.go)；路由装配在 [router.go](file:///Users/ding/Documents/swe/0910125/project-12/internal/pkg/api/router.go#L60-L75)，按 `pathToOperation`（router.go:124-167）区分端点。另有全局 `middleware.Throttle(MaxConnections)`（router.go:30-32）与 TCP 连接硬顶 [listener.go](file:///Users/ding/Documents/swe/0910125/project-12/internal/pkg/limit/listener.go)。
- 每路由 limiter 在 router 构造时一次性创建、**构造后不可变**；且两个 listener（外部 8220/内部 8221）与 OpAMP 外层 router 各自 new 一份（[server.go:55-63](file:///Users/ding/Documents/swe/0910125/project-12/internal/pkg/api/server.go#L55-L63)）。
- bulk 引擎：单一共享入口 channel `b.ch`（容量 32），8 条内部 LIFO 链表队列，所有队列共享一个 flush 并发信号量 `w`（`flush_max_pending`，生产默认 8），`doFlush` 按固定队列下标顺序排空（[engine.go:411-563](file:///Users/ding/Documents/swe/0910125/project-12/internal/pkg/bulk/engine.go#L411-L563)）；无权重、无保留、无防饿死。`dispatch` 在 `b.ch` 满时阻塞在调用方 goroutine（受 ctx 取消约束），可选 `max_pending_bulk_dispatches` 超限立即 429（engine.go:713-730）。
- **bulk flush 使用 Run 循环生命周期 ctx，而非请求 ctx**（engine.go:594）：请求 ctx 上的 value 不会到达 ES 请求；类别必须落到操作载体 `bulkT`（[block.go:18-29](file:///Users/ding/Documents/swe/0910125/project-12/internal/pkg/bulk/block.go#L18-L29)），选项层仿照 `WithDedupeKey`（[opt.go:68-76](file:///Users/ding/Documents/swe/0910125/project-12/internal/pkg/bulk/opt.go#L68-L76)）。
- ES 重试发生在 elastictransport 内部（429/503/连接错误等，指数退避，默认 5 次），每次重试调用一次底层 `http.RoundTripper`；`es.NewClient` 已提供 `rtWrap` 包装扩展点（[client.go:39-45,107-177](file:///Users/ding/Documents/swe/0910125/project-12/internal/pkg/es/client.go#L107-L177)），请求 ctx 在重试间保留。
- 数据面直连 ES、绕过 bulk：upload chunk（CBOR 流式直写，5GB 文件约上千次请求）与 file delivery chunk 流式 `Get` 均使用无重试的 `monCli`（fleet.go:479,559-560）；monitor 的 `_fleet_msearch` 长轮询也直连该 client。制品元数据查询走 `bulker.Search`，另有进程内 cache 与固定并发 8 的 `esThrottle`。
- check-in 写经 `checkin.Bulk` 按 agent 聚合成 `MUpdate`（服务器 ctx，10s 批）；check-in 读与初始 pending actions 经 `kQueueFleetSearch` 批量 msearch（含 wait_for_checkpoints 长轮询）；OpAMP 心跳复用同一 `checkin.Bulk`。
- 热重载：agent 模式配置变更走 `Fleet.Run` reload 循环（[fleet.go:149-223](file:///Users/ding/Documents/swe/0910125/project-12/internal/pkg/server/fleet.go#L149-L223)），目前任何 `server` 段差异（含 Limits）都会**整体重建** bulker/router/listener（有 drain）；解析/校验失败的配置不会投递，旧服务继续运行（agent.go reconfigure）。
- 指标：`api/metrics.go` 在 `init()` 双注册到 libbeat registry（`/stats`）与独立 Prometheus registry（`/metrics`，默认仅 localhost:5066 的 metrics server），现有指标无 label；周期 gauge 有 sampler 模式（metrics.go:383-427），在 fleet.go:534-541 以 errgroup 启动。
- `/api/status` 是 agent 高频探活端点、软授权；不适合承载敏感实时状态。
- ctx key 惯例：未导出 `struct{}` key（[logger/http.go:94](file:///Users/ding/Documents/swe/0910125/project-12/internal/pkg/logger/http.go#L94)）。

已确认的设计决策（用户确认）：

1. background 准入采用「有限等待后 429」：可配置等待时限，等待期间客户端断开立即释放；超时或总容量耗尽返回与现有形态一致的 429。
2. 未点名端点 enroll、/api/status、PGP 密钥获取、audit-unenroll 启用时**归入 control**。
3. 实时状态快照通过**内部 metrics 端口（localhost:5066）新增 JSON 路由**暴露，不改 OpenAPI、不对 agent 暴露；Prometheus 指标仍走 `/metrics`。
4. 直连 ES 路径（chunk 上传/下载、monitor 长轮询）通过**包装 ES RoundTripper 的类别感知并发闸**覆盖；许可按「每次 HTTP 尝试」领取，重试保留类别并按原类别重新领取。

## Functional Requirements

### 类别划分（classification）

- **FR-1**：定义两类请求流量。
  - control：`checkin`、`acks`、`opamp`、`enroll`、`status`、`getPGPKey`、`audit-unenroll`，以及它们必需的 ES 读写（含 check-in 长轮询 msearch、checkin.Bulk 聚合 MUpdate、agent 文档 update、ack action_results、policy/action monitor 长轮询、OpAMP 消息处理中的 ES 操作）。
  - background：`uploadBegin`、`uploadChunk`、`uploadComplete`（诊断文件上传）、`deliverFile`（文件下载）、`artifact`（制品获取）。
  - 未匹配路径为「不受管」：不经过分级闸，行为与今天完全一致。
- **FR-2**：类别在入口由路径确定性判定（复用并扩展 `pathToOperation` 的操作→类别表），写入请求 context；bulk 操作选项支持显式 `WithTrafficClass`，未显式指定时从 ctx 继承，ctx 缺失时默认 control。
- **FR-3**：类别经操作载体透传：bulk `bulkT` 携带不可变类别字段并随对象池 reset 清零；multiOp 构造的每个 block 继承该次调用类别；flush 批次类别 = 批内最高类别（含任一 control block 即为 control，混合批中 background 搭车升级，不允许反向降级）。

### 准入原语（class gate）

- **FR-4**：新增统一的「优先级保留容量闸」原语（新包 `internal/pkg/traffic`），在 entry、bulk flush、ES transport 三处复用同一抽象与配置快照机制：
  - 容量 C，control 保留量 R（1 ≤ R ≤ C）；background 保底份额 = C − R。
  - `Acquire(ctx, control)`：总在途 < C 即可领取；满时按 FIFO 排队，可被 ctx 取消；control 等待者对「借用份额」有严格优先权。
  - `Acquire(ctx, background)`：在途 background < C − R 时走保底份额；超出保底的借用仅允许在「无 control 等待者」时发生；存在 control 等待者时新的借用请求排队。
  - background 排队超过可配置等待时限返回明确的超时拒绝错误；control 仅在 C 全部占满时才排队/拒绝。
  - 每次领取返回与**当时配置快照**绑定的 release 凭证；配置替换不影响已发凭证；release 后份额立即可供更高优先级等待者使用。
  - 提供非阻塞 `TryAcquire(class)`（bulk Run 单协程调度使用）与关闭开关时的零成本 no-op 快路径。
- **FR-5**：闸门保证双向公平：
  - background 饱和（占满 C）时，control 请求在「任一份额释放后、任何 background 等待者之前」获得份额。
  - 连续 control 流不能把 background 压到保底份额 C−R 以下；background-only 工作在可配置有界延迟内必被调度（防饿死）。
  - 任何类别都不能长期独占共享容量。
- **FR-6**：客户端断开（ctx 取消）时：等待中的领取立即放弃且不消耗任何份额；处理中的请求在 handler 返回时释放入口份额；bulk dispatch 等待入队/等待响应沿用现有 ctx 取消与 `drainAndFreeAbortedBlk` 回收；ES 单次 HTTP 尝试的等待与在途随 ctx 取消释放。所有路径不发生份额泄漏。

### HTTP 入口

- **FR-7**：分级闸作为**单例**在 `runSubsystems` 构造一次，经新的 `APIOpt` 注入，两个 listener 与 OpAMP 外层 router 共享同一实例；中间件位置在全局 `Throttle(MaxConnections)` 之外层（包裹最终 `s.handler`），使等待中的 background 不持有连接节流槽与 per-route 限流名额。
- **FR-8**：闸拒绝（超时/容量）返回 HTTP 429，错误体结构、日志字段风格与 `limit` 包现有 429 一致；认证、per-route 限流、body 限制、压缩、响应内容与处理顺序不变。
- **FR-9**：开关关闭时中间件为纯透传（无额外锁竞争语义变化）；开启但无竞争时所有领取走非阻塞快路径，低负载行为与关闭时一致。check-in 长轮询与 OpAMP 长连接在整个连接/轮询期间持有入口份额（与今天持有的 per-route 名额生命周期一致），容量需按并发连接规模配置。

### bulk 引擎下游

- **FR-10**：入口 channel 按类别拆分（control/background 两条），Run 循环以「control 非空优先、否则 background」的确定性选择取块；开关关闭时保持现有单 channel、现有入队/处理顺序。
- **FR-11**：flush 调度：doFlush 一轮中先排空含 control block 的队列，再处理 background-only 队列；background-only 队列采用非阻塞 `TryAcquire(background)`，份额不可用时保留到下一周期，并由 FR-5 的有界延迟保证最终排空；control 队列的领取可在现有 `defaultFlushContextTimeout` 内有界等待。enroll 独立 flush 路径行为不变。
- **FR-12**：flush 并发信号量 `w` 替换为 class gate（容量=`flush_max_pending`，保留量可配）。flush 的 ES 请求 ctx 携带批次类别（FR-3），供 ES transport 闸识别。
- **FR-13**：`max_pending_bulk_dispatches` 在开启时按类别生效：background 受配置上限约束（超限 429，沿用现有错误），control 使用独立上限（默认 0=不限）；开关关闭时维持现有全局语义。
- **FR-14**：checkin.Bulk 聚合 MUpdate、acks、OpAMP 心跳、check-in 文档 update 等 control 写路径默认/显式携带 control 类别；artifacts 查询、uploads begin/complete 的 bulk 操作携带 background。
- **FR-15**：remote output bulker 与其 ES client 纳入同一 ES 传输闸，类别默认 control。

### Elasticsearch 传输

- **FR-16**：新增 es client 配置项把 class gate 包装为 `http.RoundTripper`：每次 HTTP 尝试前按 `req.Context()` 中的类别（缺失=control）领取份额，尝试结束（含响应/错误）即释放；elastictransport 的重试因此天然「携带类别」：每次退避后重试按相同类别重新领取，退避期间不占份额。
- **FR-17**：主 bulker client 与无重试 `monCli`（upload chunk、file delivery chunk、monitor 长轮询）均安装该包装；monitor/policy/action 等服务器生命周期 ctx 派生的请求标记 control。
- **FR-18**：传输闸拒绝向调用方返回可识别的哨兵错误，经 handler/api 错误映射为 HTTP 429（control 仅在总容量饱和且等待超时后才可能收到）；不得导致数据面静默丢块。

### 配置与热更新

- **FR-19**：新增 `server.traffic_priority` 配置段（ucfg tag），包含：总开关；entry 容量 C 与 control 保留 R（支持绝对数量；C 留 0 时取 `max_connections`）、background 等待时限、等待者上限；bulk flush 保留量/background 等待上限/background 最大调度延迟/background dispatch 上限；ES 传输容量（留 0 时取 output `max_conn_per_host`）与保留量/等待时限。提供 `Validate`：开关开启时 R、C、超时等范围合法（非负、R ≤ C、比例折算合法等）；非法配置在 unpack/Validate 阶段报错。
- **FR-20**：容量/保留量/超时等规则保存在不可变快照中，经原子指针替换；`Apply(新快照)` 先校验后替换。已发凭证绑定旧快照直到释放；新领取使用新规则。
- **FR-21**：reload 循环中，在「整体重启 server」判断之前插入 traffic_priority 专用分支：
  - 仅 traffic_priority 变更且校验通过 → 原子替换快照（入口闸、bulk 闸、ES 闸同一管理器），**不重启** listener/bulker，不影响在途请求；
  - 校验失败 → 记录错误日志与状态标记，沿用上一组有效限制，不重启、不致命；
  - traffic_priority 与其它 server 配置同时变更时，其余变更走既有重启路径，新进程只应用合法的 traffic_priority 值。
- **FR-22**：启动时 traffic_priority 校验失败按现有无效配置惯例处理（standalone 启动失败退出；agent 模式单元报错并保留旧服务）。

### 可观测性

- **FR-23**：新增 Prometheus/libbeat 指标族（带 `stage`、`class`、`reason/mode` 等 label，沿用 metrics.go 双注册）：
  - 在途 gauge、等待者 gauge、当前借用 gauge、累计领取 counter（区分 reserved/borrowed/floor）、累计拒绝 counter（区分超时/容量）、等待时延 histogram；stage 覆盖 `entry`、`bulk_dispatch`、`bulk_flush`、`es`。
  - bulk 内部队列深度 gauge（按队列与类别，周期 sampler 采集）。
  - 最近一次配置应用结果 gauge/时间戳（成功/失败）。
  - 开关关闭时指标仍可注册，读数为零/空快照。
- **FR-24**：在内部 metrics server（localhost:5066）新增 `GET /debug/traffic-priority`，返回一次 JSON 快照：开关状态、生效配置（含来源：默认/配置）、各 stage×class 的在途/等待/借用/累计拒绝、bulk 各队列深度、最近配置应用结果与失败原因；该路由不得挂在 agent 可达的 8220/8221 listener 上。

## Non-Functional Requirements

- **NFR-1 兼容性**：开关默认关闭；关闭路径与现有代码路径等价（同 channel、同队列顺序、同限流器、同响应）。现有全部单元测试不修改即通过。
- **NFR-2 性能**：关闭时无可测量回归（关键路径基准测试对比，开销限于一次布尔判断）；开启且无竞争时为非阻塞原子操作 + 一次 defer。
- **NFR-3 并发正确性**：新原语及 bulk 调度改造必须通过 `go test -race`；压力/混沌场景（高频 control/background 混合、大量断连、并发配置替换、ES 慢响应）下无死锁、无 goroutine 泄漏、无份额泄漏（在途归零后 gauge 归零）。
- **NFR-4 可测性**：class gate、bulk 调度、transport 包装、热替换均可在不依赖 Docker/ES 的单元测试中确定性行为验证（假时钟、可注入 gate、通道钩子）。
- **NFR-5 安全/FIPS**：不引入新加密；调试快照仅绑定 localhost 内部端口；不泄漏密钥/PII，队列与快照内容只含计数。
- **NFR-6 可运维性**：更新 [fleet-server.reference.yml](file:///Users/ding/Documents/swe/0910125/project-12/fleet-server.reference.yml) 配置示例与开发者文档；按仓库约定添加 changelog fragment；日志使用 zerolog（禁止 fmt.Print*），新 Go 文件带 license header、通过 goimports/go vet/golangci-lint。

## Constraints

- **Technical**：Go 版本以 [.go-version](file:///Users/ding/Documents/swe/0910125/project-12/.go-version) 为准；构建/测试走 Mage；不手改 generated 文件（`pkg/api`、`internal/pkg/api/openapi.gen.go`）；配置走 ucfg（`config:"..."` tag + `Validate()`）；错误映射走 `internal/pkg/api/error.go` 既有模式。
- **Business**：默认关闭、行为可回退；不得提高任何现有默认限制的激进程度。
- **Dependencies**：仅使用现有依赖（`golang.org/x/sync/semaphore`、`golang.org/x/time/rate`、prometheus client、zerolog、chi）；不引入新第三方依赖。

## Assumptions

- 两个 HTTP listener 共享同一个 gate 单例；运维按预期并发长轮询/长连接规模配置 entry C 与 R（默认 C 派生自 `max_connections`）。
- bulk flush 的 ES 调用同时经过「flush 闸」与「ES transport 闸」两层，两者容量独立配置（分别对应 flush 并发与 ES 连接池）。
- 混合类别 flush 升级为 control 是有意为之：它只可能让 background 搭车，不会牺牲 control。
- monitor 长轮询在 ES 侧占用连接的时长可达长轮询超时；ES 传输闸的 control 保留量默认需覆盖 monitor 数量与 checkin msearch flush 突发（默认按 `max_conn_per_host` 比例派生，文档给出容量规划建议）。
- upload chunk 以「单分片单 HTTP 请求」粒度领取/释放入口份额（5GB 上传是上千个独立 chunk 请求）；file delivery 单个请求在整个流式传输期间持有一份入口份额，与今天持有 per-route 名额的生命周期一致。

## Acceptance Criteria

### AC-1：开关关闭时行为零变化
- **Type**： `rule`
- **Given**：`traffic_priority.enabled=false`（默认）
- **When**：运行现有 router/limiter/bulk/handler 单元测试与入口黄金路径测试
- **Then**：全部测试不做任何修改即通过；bulk 仍为单入口 channel、固定队列顺序；入口中间件为透传；新指标读数为零
- **Pass Condition**：`mage test:unit` 通过，且新增的「关闭模式等价性」测试（channel/调度路径、429 响应、顺序）通过
- **Evidence**：测试命令输出与新增测试代码

### AC-2：端点→类别映射确定且覆盖 OpAMP
- **Type**： `rule`
- **Given**：特性开启
- **When**：分别请求 checkin/acks/opamp/enroll/status/pgp/audit（control）、uploadBegin/uploadChunk/uploadComplete/deliverFile/artifact（background）及其它未知路径
- **Then**：前两组分别获得正确类别并进入对应准入/调度路径；未知路径不受管；`/v1/opamp` 长连接被识别为 control
- **Pass Condition**：映射表驱动单测覆盖全部操作与未知路径，OpAMP 经注入单例的端到端测试验证
- **Evidence**：映射单测与路由装配测试

### AC-3：饱和时 control 优先于 background 获得释放份额
- **Type**： `rule`
- **Given**：entry 闸 C=4、R=2；4 个 background 已占满全部份额（2 保底 + 2 借用）
- **When**：1 个 control 请求到达并排队，随后任一份额释放
- **Then**：释放份额先分配给 control（即使有 background 等待者）；control 在有界时间内被准入，metrics 记录 borrowed 回收与 control admitted
- **Pass Condition**：可注入时钟的并发单测确定性复现该顺序
- **Evidence**：gate 单测（含执行轨迹断言）

### AC-4：control 等待期间新的 background 借用冻结
- **Type**： `rule`
- **Given**：background 已用满保底份额 C−R，且存在 control 等待者
- **When**：新的 background 请求尝试领取
- **Then**：它不能获得借用份额，只能排队；等待期间断开则立即移除且不占份额；control 清空后借用恢复
- **Pass Condition**：单测断言 controlWaiters>0 期间借用计数零增长、断连后等待者归零
- **Evidence**：gate 单测

### AC-5：background 有限等待后 429，control 仅总容量饱和才拒绝
- **Type**： `rule`
- **Given**：配置 background 等待时限 T
- **When**：background 请求在 T 内未获得份额
- **Then**：返回闸门超时错误 → HTTP 429，响应体形态与现有 limit 429 一致；同类 control 请求在保留量内始终被受理，只有 C 全部占满才排队
- **Pass Condition**：中间件单测断言状态码、错误码字段与 JSON 形态；control 受理断言
- **Evidence**：HTTP 中间件测试

### AC-6：在途工作不被配置变更或类别变化影响
- **Type**： `rule`
- **Given**：N 个 background 已按旧快照领取份额在执行
- **When**：并发执行新快照 Apply（改 C/R/超时）并让新请求进入
- **Then**：在途 N 个持旧凭证正常结束并释放回旧快照账面；新请求按新快照领取；任何请求不被重分类、不被中止
- **Pass Condition**：替换并发单测：在途全部成功、新旧快照簿记各自归零、无 panic
- **Evidence**：gate 热替换测试

### AC-7：流量类别贯穿 bulk 载体与长轮询链路
- **Type**： `rule`
- **Given**：特性开启
- **When**：control ctx 发起 checkin pending-action 搜索/acks/MUpdate，background ctx 发起 artifact 搜索/upload bulk 操作
- **Then**：对应 `bulkT.class` 正确（含 multiOp 与对象池 reset 后无脏值）；control 块进入 control 通道与 control 批次，background 反之；flushCtx 携带批次类别供传输闸读取
- **Pass Condition**：bulk 包单测断言通道去向、队列类别计数、flush ctx 类别；checkin/ack/artifact handler 层测试断言 ctx/Opt 类别
- **Evidence**：bulk 与 handler 单测

### AC-8：ES 变慢/background flush 堆积时 control 的下游有界优先
- **Type**： `rule`
- **Given**：flush 闸 C=4/R=2；ES 传输以可注入假实现人为变慢；background 队列持续满载
- **When**：control 块与 background 块持续到达
- **Then**：control 队列先于 background-only 队列获得 flush 许可与 ES 尝试许可；background-only 队列被推迟但在配置的最大调度延迟内必被 flush；control 块端到端延迟有上界（由测试时钟度量）
- **Pass Condition**：bulk 调度确定性单测（假时钟 + 许可轨迹）验证顺序与延迟上界
- **Evidence**：engine 调度测试

### AC-9：双向防饿死
- **Type**： `rule`
- **Given**：entry 与 bulk 两阶段分别做两轮压测：① control 持续到达；② background 持续饱和
- **When**：系统长期高负载运行
- **Then**：① background 始终能获得保底份额 C−R，background-only bulk 队列在有界延迟内排空；② control 始终能在份额释放后插队获得保留量；无任一类别连续 N 个调度周期零进展（N 取配置上界对应的周期数）
- **Pass Condition**：带假时钟的长周期调度模拟断言两类进展计数均 > 0 且间隔有界
- **Evidence**：公平性单测

### AC-10：断连及时释放未使用份额（全链路）
- **Type**： `rule`
- **Given**：特性开启
- **When**：客户端分别在「入口排队中」「bulk dispatch 排队中」「ES 传输尝试排队中」取消 ctx，以及在「传输在途」时取消
- **Then**：等待者立即移除、未消耗份额；在途尝试结束/handler 返回后份额归还；各阶段 in-flight/waiting gauge 回到取消前基线；无 goroutine 泄漏
- **Pass Condition**：各层取消单测 + goroutine 计数/泄漏检查 + `-race`
- **Evidence**：gate/bulk/es 取消测试与 race 输出

### AC-11：ES RoundTripper 闸覆盖直连路径且重试保类
- **Type**： `rule`
- **Given**：主 client 与 monCli 均安装包装；假 transport 对第 1、2 次尝试返回 503/429、第 3 次成功
- **When**：control 与 background 请求分别执行（含 upload chunk 直写、file delivery chunk 直读、monitor 长轮询）
- **Then**：每次尝试按 ctx 类别领取/释放份额，3 次尝试类别一致；退避期间不持份额；传输闸饱和时 background 按等待时限收到哨兵错误并映射 429
- **Pass Condition**：transport 包装单测（假 RoundTripper 计数尝试次数与每次类别）+ 错误映射测试
- **Evidence**：es 包与 api 错误映射测试

### AC-12：配置热更原子生效、坏配置沿用上次有效值且不重启
- **Type**： `rule`
- **Given**：服务运行中且已应用快照 V1
- **When**：先后下发①合法 V2（仅 traffic_priority 变更）②非法 V3
- **Then**：① 只影响新请求、在途按 V1 结束、无 listener/bulker 重建（有探针证明未触发 runServer 重启）；② V3 被拒绝、日志+状态指标记录失败原因、运行规则仍为 V2、服务不重启不退出；重启类其它配置与 traffic_priority 同时变更时行为不回退
- **Pass Condition**：reload 分支单测（fake cfgCh 投递，观察 gate 快照与重启探针）
- **Evidence**：server/fleet 热更测试

### AC-13：指标完整且数值正确
- **Type**： `rule`
- **Given**：特性开启
- **When**：在测试中制造各阶段的领取/借用/等待超时/拒绝/排队
- **Then**：FR-23 列出的每个指标族按 label 正确出现并单调/瞬时正确；队列深度经 sampler 更新；关闭开关读数为零
- **Pass Condition**：metrics 测试抓取 registry 文本/采集器值逐系列断言
- **Evidence**：metrics 单测

### AC-14：内部状态快照端点可用且不外暴露
- **Type**： `rule`
- **Given**：服务运行
- **When**：访问 localhost:5066 `/debug/traffic-priority`，并从外部/内部 agent listener 访问同路径
- **Then**：前者 200 返回 FR-24 全部字段（含生效配置、各 stage×class 计数、队列深度、最近应用结果）；后者路由不存在（404）
- **Pass Condition**：路由挂载与 JSON 字段测试 + 两 listener 隔离测试
- **Evidence**：metrics server 测试

### AC-15：配置校验与默认值派生
- **Type**： `rule`
- **Given**：构造非法 traffic_priority（R>C、负值、非法比例、负超时）与留空配置
- **When**：ucfg unpack + Validate / 启动派生
- **Then**：非法值报明确错误；留空项按 FR-19 派生（entry C=max_connections、ES C=max_conn_per_host 等）且派生值自洽
- **Pass Condition**：config 包表驱动测试，含 bad-* testdata 风格用例
- **Evidence**：config 测试

### AC-16：质量门禁
- **Type**： `rule`
- **When**：实现完成
- **Then**：`mage check:all`（或至少 go build ./...、go vet、goimports、license header、golangci-lint 新包范围）与 `mage test:unit` 通过；新增并发测试在 `-race` 下通过；reference 配置与 changelog fragment 齐备
- **Pass Condition**：命令输出无错误
- **Evidence**：CI 命令输出截图/日志、文件清单

### AC-17：低负载与关闭模式性能无回归
- **Type**： `rubric`
- **Dimension**：入口与 dispatch 热路径的相对开销
- **Scale**：1-5
- **Anchors**：1 = 关闭模式新增锁竞争/分配导致可测回归；3 = 关闭模式仅有廉价判断但开启快路径有偶发阻塞；5 = 关闭模式零差异，开启无竞争时为单次非阻塞原子操作且无额外堆分配
- **Pass Threshold**：>= 4
- **Evidence**：benchmark（关闭 vs 开启无竞争 vs 基线）对比报告

### AC-18：并发设计的内聚与可维护性
- **Type**： `rubric`
- **Dimension**：方案复用现有接缝（limit/bulk opt/rtWrap/APIOpt/ucfg/sampler）、改动面克制、无 generated 手改、无重复限流逻辑
- **Scale**：1-5
- **Anchors**：1 = 多套并行实现、散落硬编码；3 = 抽象成立但部分重复；5 = 单一 gate 原语三处复用、配置/指标/错误各只有一套约定
- **Pass Threshold**：>= 4
- **Evidence**：代码评审与结构走查

### AC-19：可观测性对运维排障的有效性
- **Type**： `rubric`
- **Dimension**：运维仅靠 `/metrics` 与 `/debug/traffic-priority` 即可判断「谁占用、谁等待、谁借用、谁被拒、下游堵在哪」
- **Scale**：1-5
- **Anchors**：1 = 指标缺失或无法区分类别/阶段；3 = 计数齐全但缺借用/下游队列视角；5 = 指标与快照完整覆盖 FR-23/FR-24 全部维度且文档给出判读示例
- **Pass Threshold**：>= 4
- **Evidence**：指标清单核对 + 文档片段

## Open Questions

- 无阻塞性开放问题（4 个架构分叉已确认，见 Background & Context）。
- 实现阶段可在评审中微调的非阻塞事项：各保留量的默认百分比、background 默认等待时限具体取值、bulk background 最大调度延迟的默认周期倍数；取值需在 reference.yml 注释与文档中说明并允许运维覆盖。
