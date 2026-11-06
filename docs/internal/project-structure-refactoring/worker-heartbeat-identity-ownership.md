# Worker 心跳与自动注册修复的进程身份边界

本批继续完整模块化计划的 Coordination / Persistence / Worker 能力闭环，不作为全仓生产验收。
基线为上一批 `worker-membership-fence` 结束后的实际工作区；保留已有未提交修改，不创建兼容 FQN
或旧身份入口，不新增数据库、微服务、历史数据迁移工具。

## 已实现的生产路径

```text
TaskManager 的真实 WorkerExecutionIdentity
  → WorkerHeartbeatBootstrap / HeartbeatPayload
  → 认证 HTTP /worker/heartbeat / HeartbeatRequest
  → WorkerManager（本地单 key 发布与 tenant usage 更新）
  → WorkerRegistry.heartbeat(expectedIdentity, metrics)
  → Memory 原子 compute / PostgreSQL 条件 UPDATE RETURNING
  → ACCEPTED / REGISTRATION_REQUIRED / FENCED
```

领域身份由 Coordination API 拥有；Wire 身份转换在 HTTP 入口。Persistence 不接收 HTTP DTO 或
Native handle。Registry 返回实际条件更新后的快照；不存在或已替换返回 empty，数据库异常必须
传播。旧身份不得刷新新进程的指标、时间戳、缓存摘要或 tenant usage。Core 先完成持久化，再发布
本地观测；发生数据库错误时不能先把本地状态更新为成功。

旧 Coordinator 本地仍缓存旧进程时，也以持久化操作判断所有权。检测到真实替换后同步当前身份，
清除旧 tenant usage；相同进程的 DECOMMISSIONING 状态继续保留。本批没有把 DECOMMISSIONING
持久化，因此不声称解决跨 Coordinator 的持久化排空状态。

## 自动注册不是无条件接管

`WorkerRegistrationClient` 在首次发送前保留唯一逻辑 ID / admission domain / incarnation：

- 首次尝试使用 `/v1/workers/register`，保持显式初始注册的接管语义。
- 后续重试与自动修复统一使用 `/v1/workers/refresh`；不存在可插入，相同身份可刷新，另一进程
  持有时必须拒绝。客户端不能切换身份。
- PostgreSQL 通过 `INSERT ... ON CONFLICT ... DO UPDATE ... WHERE` 原子核验完整身份；Memory
  在同一 per-key compute 内完成核验和写入。禁止 `get → 无条件 upsert`。
- 两个入口都要求内部身份认证、相同配置校验和有界 metadata body。刷新拒绝返回 `409/fenced`。
- 初次提交已成功、但确认失败后，后续请求也只能条件刷新。丢失确认不会把重试变成新的无条件接管。

这关闭了“missing 心跳响应 → 新进程注册 → 旧进程自动重新注册”的覆盖竞态。**首次无条件注册
请求之间仍没有全局 admission 顺序**，例如首次请求长时间延迟与另一独立进程首次注册交错；这需要
后续持久化 membership/admission authority，不能由本批 UUID 比较推导已经解决。

## 被替换进程的准入与资源

`HeartbeatSender` 解析 `WorkerControlResponse.action` 的精确值，不再对响应文本做 `contains`。
收到 `fenced` 后，生产装配调用 `TaskManager.fenceAdmission()`：在实际 admission registry 同一锁下
永久封闭新 reservation/publication，并使 task drain owner 进入 draining。完成封闭后才把 Sender
标记为永久 fenced；之后不能自动修复或恢复发送。回调失败不能伪装成已经完成封闭。

此动作不在心跳线程等待任务结束，也不关闭正在使用的 Native handle、线程池、连接或结果缓冲。
已有任务的 status、cancel、seal 和资源释放仍走原 owner。未知的执行/输出排空仍为未知，不把
“拒绝新任务”作为“可以关闭进程”的证明。真实 canonical task 在 draining 时抛 `CLOSED`，由 HTTP
映射到 503；保留已有取消、精确 seal 和 drain receipt 语义。

暂时网络故障的 degradation callback 仍是既有独立机制，本批没有把未装配的瞬时降级回调宣称为
生产级停止准入或自动恢复。健康/readiness 尚未纳入独立 admission 状态，后续角色生命周期工作
必须解决；不通过把 HTTP 存活伪装为失败来强制杀死尚有清理责任的进程。

## 验证设计与证据范围

- Memory 与 PostgreSQL：缺失、相同身份重放、不同 incarnation、不同 admission domain、更新后
  快照、拒绝时完整记录不变。
- PostgreSQL：真实事务持有替换行锁，等待旧心跳/条件刷新确实进入锁等待，再提交替换。两条
  操作醒来后必须拒绝，不能依靠操作前查一次身份。
- Core：两个真实管理器共享同一后端、旧本地缓存、tenant usage 隔离、missing→replacement→repair
  竞态、持久化失败不发布本地成功。
- HTTP：真实注册客户端、鉴权资源、PostgreSQL、提交后确认失败、缺失身份拒绝、错误 token 拒绝、
  条件修复、生产心跳装配与永久 fenced。测试容器使用与 CoordinatorHttpServer 相同的 JSON factory。
- Admission：实际 registry 的 pending publication 封闭；真实 SQL/Native Worker 的延迟任务 HTTP
  被隔离后拒绝，取消仍获得精确清理回执。Native 执行测试独立 JVM，不能在永久 fence 后继续复用。
- 两组原 Worker Registry JDBC 测试删除手工平行 schema 和清空路径，统一使用临时 PostgreSQL
  schema 的 `MigratedPostgres.create()`；生产 schema owner 不变。

最终本批按类选取 286 项：285 通过，1 项既有 Shuffle inventory 装配失败，0 error、0 skip。
最终 package 75 项全部通过；两个独立 JVM 的真实 Native 隔离/取消测试共 6 项通过。全 reactor
发行审计 97 个 JAR / 97 个唯一模块名 / 826 个包，无违规；820 个源码包无环。三个角色核验
当前 API、Protocol 和对应实现 JAR，而不声称角色依赖隔离已经全部完成。

具体执行状态以 [验证汇总](worker-heartbeat-identity-validation-summary.json) 和
[逐测试结果](worker-heartbeat-identity-tests.tsv) 为准；设计中的验证项不自动视为通过。

## 初次失败与限制

首次近端验证通过。扩大验证首次新增 HTTP 测试超时：测试只注册默认 JacksonFeature，未使用生产
JSON 配置，旧有可选 Native 指标字段导致 400；修正测试装配，不放宽 mandatory identity。收敛
schema 初始化时误删了仍被断言使用的 JDBC import，testCompile 失败后恢复。扩大鉴权测试发现
新错误消息遗漏 Worker ID，恢复诊断身份信息后重跑；保留每次实际失败证据。静态检查另发现测试插入空行的行尾空格，已清理，首次检查结果单独保留。

既有 `InMemoryShuffleDataInventory` 未接通生产路径的门禁不豁免、不加假装配。本批未实现通用
Shuffle session/receipt、Local 生产装配、Celeborn、持久化安全缩容或 incarnation 级故障回调。
C++、JNI、Proto schema、Native bundle 输入未修改；Java 并发与实际执行链独立验证，不声称 ASan、
跨平台、性能或全仓生产回归已完成。

[逐类处置](worker-heartbeat-identity-fqn-dispositions.tsv)、
[语义审查](worker-heartbeat-identity-semantic-evidence.json)、
[调用引用](worker-heartbeat-identity-callers.tsv)、
[输入变更](worker-heartbeat-identity-input-evidence.json)、
[文件处置](worker-heartbeat-identity-file-dispositions.tsv)。
