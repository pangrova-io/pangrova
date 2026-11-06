# Shuffle 契约、授权编码与执行绑定的独立所有权

本批以工作区 8,205 个文本文件为基线。新增四个真实制品，不创建空 Provider，
不新增持久化后端，不改变 SQL、Proto、JNI、Native 或历史初始化 SQL。

| 制品 | 实际能力与包 | 调用与装配 | 状态、事实与资源 |
| --- | --- | --- | --- |
| `pangrova-shuffle-api` | 原 Core 中 25 个 `.api` 类型：注册、分区、handle、能力、保留、传输与 drain 契约 | Planner、Operations、Coordinator、Worker、Server 和 Core 显式依赖；原实现继续调用同一 FQN | 类型与契约拥有者改变；allocation、数据与关闭责任仍在现有实现，未增加状态机 |
| `pangrova-security-codec` | `.codec.authorization.DeferredAuthorizationScopeCodec` | Metadata、Indexing、Persistence JDBC、Coordinator 及 Execution Core 共用唯一编码实现 | 纯确定性编解码；不打开连接、不查询权限、不拥有事务；当前权限仍由既有事务参与者读取 |
| `pangrova-execution-api` | `.stage.TaskStage/TaskPurpose`，`.deadline.StageTaskDeadline` | 普通查询和持久化分区提交端传入阶段身份与用途 | 仅不可变值与局部校验，Common/JDK classpath；没有 Planner、Proto、Generation Store 或 Provider |
| `pangrova-execution-core` | `.binding` 请求/选项绑定，`.authority` 活跃/持久化授权上下文，`.identity` 确定性任务 ID，`.deadline` lease 截止时间，`.split` canonical split 解析 | LeafStageDispatcher、DurablePartitionRequest 与既有下游调用方直接使用；没有注册表、全局 setter 或默认后端 | 借用不可变当前授权与 lease；不创建线程、网络、连接或 Native handle；不拥有调度、查询状态机、lease 续租或提交事务 |

## 保持的执行约束

- 普通阶段由既有 `StageNode` 显式投影出 `TaskStage`。任务构造器不再携带 Calcite/Planner 树。
- `TaskPurpose` 穷举现有六种用途；COMPUTE/OUTPUT 仍映射 READ。未支持的字符串在调用边界拒绝。
- 持久化分区不再为了取得 stage ID 和用途而创建 `PersistedStagePlan` 与 `StageNode`。
  `GenerationPartitionBindings` 仍检查 page kind 与分区上界，空 SOURCE_MAP 的零并行度不能产生任务。
  page kind 到任务用途的映射保持一致；真正的 durable task kind 仍取实际 lease 的 page kind。
- job 级 `Durable` authority 不能直接提交任务，必须先取得当前精确 partition claim。
- task ID 仍由 execution/page 身份、owner fence、partition fence 和 attempt 编码；没有随机替代持久化身份。
- deadline 仍为实际 lease 到期时间和本次 timeout 的最小值，checked arithmetic 保持不变。
  数据库续租不能延长已经签发的任务凭据。
- 当前权限摘要、声明观察、allocation epoch、execution owner、operator budget、split 校验、
  result logical types 和 canonical 协议身份仍走原路径；没有无条件更新或失主后的继续执行。
- 原 DML task record 与 builder 因跨制品调用成为 public；它们仍是 Execution Core 实现入口，
  没有被放进 API，mutable 输入仍防御性复制。

## 编码与持久化

原 PostgreSQL 包中的授权 codec 实际不包含数据库操作。分离为独立 codec 制品符合
目标 ADR 对“真实复用的编码基础设施”允许独立制品的原则，避免把 Jackson 或
后端类型放入 Security API，也避免让执行绑定依赖 PostgreSQL Provider。

编码算法逐 token 等价；迁移原固定 fixture 与测试，校验 raw authority 的规范字节和
SHA-256，以及 generated write receipt 的不可变 declaration/model 观察。严格重复字段、
尾随内容、大小上限和 canonical receipt 检查保持原实现。没有数据迁移或兼容壳。
PostgreSQL 自己的 `PolicyRoleCodec` 仍属于 Provider；它没有被合并进公共编码模块。

## 验证与可复核证据

- `contract-foundations-fqn-dispositions.tsv`：每个修改的生产类型及其目标制品、职责、调用、资源与验证。
- `contract-foundations-semantic-evidence.json`：每个生产文件的基线与结果 SHA、规范化 token 比较、非等价修改的人工说明。
- `contract-foundations-production-review.diff`：排除明确 FQN 替换后的语义修改，供逐项复核。
- `contract-foundations-algorithm-equivalence.json`：授权 codec 全体方法、lease deadline 提取方法的独立等价检查。
- `contract-foundations-callers.tsv`：实际源码引用；不将引用数量等同于端到端支持证明。
- `contract-foundations-backend-evidence.json`：Native、Schema、Protocol、Client 的本批基线一致性。
- `contract-foundations-validation.tsv` 与 `contract-foundations-tests.tsv`：实际测试命令和逐 case 结果，保留首次失败。
- `contract-foundations-artifacts.json`：clean 产物中的类、模块名、split package、旧类/旧引用与契约泄漏检查。

## 明确尚未完成

这不是 P7 通用 Shuffle 完成声明。现有 `ShuffleTransport.startServer`、服务地址和
endpoint 模型仍在原契约；Native Adapter、通用 coordinator/worker session、receipt、
精确选中 attempt 与能力交集、Provider 分离仍需后续批次落实。
现有 exchange/spool 扩展点也尚未全部迁入最终领域 API/SPI 包布局。
Celeborn 没有生产实现，本批不新建占位模块或宣称集成通过。

Execution Core 的持久化授权绑定暂时依赖现有 Persistence API 的强类型端口，
后续领域 Persistence 拆分继续遵循唯一事实 owner。Coordinator 的剩余调度、
Generation 运行编排与包环也没有被这次迁移掩盖；源码图中的单个循环由 7 包缩小为 6 包，
Shuffle handler 已脱离该循环，剩余依赖见 `contract-foundations-package-cycles.txt`。
完整回归仍按现有门禁执行。

迁入 API 后还新增暴露五个真实未装配的 Shuffle 实现，详见
[生产接线审查](contract-foundations-shuffle-wiring-audit.md)。本批没有修改该门禁或新增豁免。


## 首次失败与修正

1. 初次依赖扫描把 Common/Scheduler 的 Javadoc 链接和 Security API 隔离测试中的
   类名字符串误计为依赖，Maven 在进入测试前报告循环。删除这些非代码依赖；
   实际 API/codec 调用方保留直接依赖，契约仍保持最小 classpath。
2. 临时执行脚本出现 `elf` 拼写错误，未运行 Maven；修正后执行定向测试，
   没有把该次调用记作通过。
3. 首次 clean package 在结构门禁发现空迁移目录和 JDBC → Security Codec 的
   manifest 声明遗漏。移除本批留下的空目录，并精确定位顶层模块声明补上真实依赖；
   中间一次只清理目录后的运行仍报告该依赖遗漏，保留这次失败。
   没有增加 baseline violation、忽略项或关闭架构测试。

4. 静态稳定性门禁要求新契约目录中的三个既有接口显式标注稳定性。
   ShuffleExecutionOwnerResolver、ShuffleLifecycleManager、ShuffleTransport 标记为
   Experimental，原因分别是 session 身份归属、通用提交 receipt 和 Native Adapter
   边界尚在演进；方法签名与行为不变。未给孤立实现加注解，重跑 SPI 接线门禁后
   同样的五项缺口仍然失败。静态脚本只识别短注解写法，因此使用正常 import。
