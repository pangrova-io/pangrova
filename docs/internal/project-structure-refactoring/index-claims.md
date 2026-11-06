# Scalar index claim 端口与 attempt 所有权

本批把 claim 从 IndexMetadataStore 中提取为独立 Persistence 端口，并修复旧构建
释放或续租新构建 claim 的真实 PostgreSQL 缺陷。整体模块化与生产级验收仍在进行。

## 端口、后端与装配

- `IndexBuildClaimRepository` 位于 Persistence Indexing API 的 `api.build`，标为
  Experimental；四个操作都必须实现，没有默认成功、默认拒绝或空续租/释放。
- `JdbcIndexBuildClaimRepository` 位于同领域 PostgreSQL 制品的 `postgresql.build`。
  它只拥有 claim SQL，不持有描述符、mapping、发布事实，也不自行初始化 schema。
- 删除 Core 的 BuildClaimHandler、Coordinator 的 MetadataBackedBuildClaimHandler、
  PostgreSQL Metadata 内的 JdbcIndexStateStore；删除 Metadata API/facade 的四个 claim 入口。
- CoordinatorBootstrap 选择 PostgreSQL provider，经 ModuleIntegrationBootstrap、
  IndexingBootstrap 显式注入 IndexBuildContext。业务 Core 只依赖持久化契约。
- Server 的索引装配要求非空 claim repository；原有隔离本地构建入口仍可不配置它。
  不把这个本地入口视为分布式互斥实现。后续完整能力配置仍需进一步收敛。

## 所有权与失败语义

每次提交创建独立、不可复用的 attempt owner，不再让同一协调器的所有提交共用一个 owner。
抢占使用唯一键 `(index_id, generation)`；重复抢占即使 owner 相同也返回 false。
释放和续租同时匹配 index、generation、owner。旧 attempt 的延迟操作返回 false，
不能改变新 attempt；续租使用 GREATEST，避免乱序心跳倒退。过期边界严格小于 cutoff。

数据库故障传播为异常，不能伪装成竞争失败或清理成功。构建器的心跳失败或失主是
终止性错误，发生在加入 mapping batch 之前，不会被“单文件失败继续处理”吞掉。
释放失败也不能产生成功构建结果；已有主异常保留，释放异常作为 suppressed 附加。
完成日志与耗时指标延后到 claim 清理成功后，避免清理失败时重复计时。
正常成功、取消、stale generation 和异常路径仍按实际获得的 owner 释放 claim。

Provider 借用 Server 管理的 DataSource；每次操作创建并关闭 Connection/Statement，
不关闭连接池。构建器、借用 executor、reader 和 filesystem 的原生命周期策略未改变。

## 真实失败复现与验证

- 修改前两个真实 PostgreSQL 用例均失败：旧 release 删除替代 claim，旧 heartbeat
  修改替代 claim 的时间。修复后这两个用例通过。
- 第一轮编译暴露测试签名替换误改辅助方法，修复后通过，没有绕过编译检查。
- 扩展边界验证 52 个实例通过，覆盖真实 Hash 构建/编码/文件路径、失主停止、
  不提交 mapping、后续文件停止、独立 owner、错误传播及 suppressed cleanup。
- 首轮 owner 链 214 个实例通过；静态链因两处长度常量命名失败，另有 7 个既有
  Checkstyle 警告。66 份 SpotBugs 报告没有发现项。修正常量后，三个直接受影响
  模块静态检查通过，零发现、零 Checkstyle 诊断；分析器 missing-class 提示保留。
- 初次仓库检查发现新端口缺少稳定性注解；补上 Experimental 后八项检查均通过。
- PostgreSQL 用例还覆盖同 owner 重复抢占、generation 隔离、四线程恰一胜者、
  乱序心跳、严格过期和数据库故障。每例使用独立初始化 schema，不清空用户数据库。
- 指标回归先复现释放失败记录两次耗时（预期一次），随后修复。最终边界检查
  53 个实例通过，三个直接模块静态复核仍零发现、零诊断。
- 第一轮 clean 消费者运行 249 个实例全部通过，但 E2E 夹具编译发现一处遗漏的
  显式 claim 注入，补齐后重新运行 clean reactor。
- PostgreSQL claim 测试已按能力归入 build 包，接入统一 ContainerTestBase。
- 原资源闭合属性测试只运行模拟 try/finally，本批改为真实 IndexBuildCoordinator
  的成功、pin 失败和 stale generation。snapshot pin 按契约由 retention/GC 释放，
  不在构建结束时提前 unpin。第二轮统一回归也通过这三条真实路径。

第二轮 clean 全 reactor 选定回归完成：293 个测试实例全部通过，无失败、无跳过。
包含 SQL 创建/重建/删除索引、增量正确性、构建恢复场景、启动 smoke、模块与包约束。
该命令使用 full-regression/native-runtime profile，但带测试筛选，不等于无筛选生产回归。
最终审查通过：112 个制品、112 个唯一模块名、856 个包；无本批旧类/引用、重复
服务注册或 split package 违规。五个修改制品的 891 个 class 与编译输出逐字节一致，
三个角色的实际启动 classpath 与当前制品一致，三份 Native 库哈希相同。
clean install 的发行物已通过核对，无需重复打包。该结论不代表总体角色隔离已完成。
详细命令、退出状态与当前测试记录见
[验证汇总](index-claims-validation-summary.json)、[逐测试记录](index-claims-tests.tsv)、
[逐类处置](index-claims-fqn-dispositions.json)、[测试文件覆盖](index-claims-owner-coverage.json)、
[初次静态结果](index-claims-owner-analysis.json)、[最终直接模块静态结果](index-claims-owner3-analysis.json)、
[仓库检查](index-claims-final-checks.json)、[能力状态](index-claims-capabilities.json)、
[JAR 审查](index-claims-artifacts.json)、[class 一致性](index-claims-class-parity.json)、
[启动 classpath](index-claims-runtime-classpath.json)、[Native 哈希](index-claims-native-artifacts.json)、
[逐文件台账](index-claims-file-ledger.json)。

## 明确未完成的安全恢复边界

本批只完成 claim 记录的精确 owner 操作，不声称完整 generation recovery 已安全。

1. claim 在构建结果 Future 完成前释放；DDL/增量刷新随后通过单独的 publication CAS 发布。
   claim 的获取、mapping 写入、失败状态转换和最终发布尚不是同一个原子工作流。
2. 心跳检查和 mapping callback 之间仍有竞争窗口；必须由持久化事务校验 attempt，
   不能用调用前的 Java 检查替代数据库 fencing。
3. 文件路径仍按 index/generation/data-file 生成，缺少 attempt 隔离。晚到旧写入可能
   与替代尝试共用路径；未发布文件的安全清理、引用与 retention 仍需补齐。
4. StuckBuildRecovery 仍依据描述符 BUILDING/updatedAt，尚未结合 claim heartbeat；
   staging generation 与原描述符 READY 的恢复也需统一审查。
5. expireBefore 保留已有显式管理操作，未接自动调度。只有在对象、mapping、发布及
   恢复 fencing 完整后才能启用自动过期/接管；本批不新增 unsafe takeover 路径。
6. 取消仍为协作式；连接/文件在途操作、未知提交确认和完整进程 drain 不是本批完成项。

后续按“原语义测试 → 原子工作流端口 → PostgreSQL 实现 → Core/Server 切换 → 故障验证”
继续。其余模块拆分、Shuffle 三种真实 Provider 通用化、最终角色隔离、无筛选全量回归、
跨平台与性能对比仍属于原目标；这些不能由本批测试通过代替。

## 基线与处置证据

本批编辑前捕获 8,980 个文本文件，排除实际 Native 产物目录，保留 Java 的 build 源码包。
12 个生产类逐一记录职责、调用与装配、持久化事实、资源 owner 和验证范围；
[input evidence](index-claims-input-evidence.json) 以该工作区快照比较，不以 HEAD 代替
原有未提交改动。本批没有修改 Native、JNI、Proto 或 schema migration 源码，
也没有创建新的存储产品、数据迁移工具或 Celeborn 占位模块。
