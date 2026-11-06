# 任务 Spill 预算与 Worker 本地目录的所有权

本批基于 DAG/Generation 职责收敛之后的 8,265 个工作区文本文件实施，保留已有改动。
修复实际配置缺口，不新增存储后端、进程、数据迁移或兼容转发类。

## 问题与实施

旧 SpillBudget 和 CoordinatorBootstrapConfig.SpillConfig 同时保存字节数与目录，但
DefaultWorkerSelector 从未消费 Coordinator 的目录。真实目录来自 Worker ResourceConfig，
Native 的 resolveWorkerLocalSpillPolicy 同时检查任务正数配额、Worker 开关和本地目录。
Coordinator Main 却始终使用默认零预算，生产 YAML 无法授予任务 Spill 配额；旧测试通过
直接创建非零预算绕过了这个入口。

现在：

- Scheduler API `SpillBudget(long maxSpillBytes)` 是唯一预算值，移除无效路径字段。
- Server QueryConfig 直接持有该不可变值，删除重复的嵌套 SpillConfig。
- `coordinator.task.spill.max-bytes` 经 typed config → Coordinator Main → QueryConfig →
  Query/Materialization 的 Worker selector → StageResourceGrant 进入既有任务提交链。
- EmbeddedCoordinator/Standalone 继续使用同一配置工厂。没有另建配置解析器。
- 默认零仍禁用任务 spilling；负数、溢出和非整数字符串在启动配置阶段拒绝。
- Worker 路径、资源生命周期与 Native 节点/任务上限交集算法不变。正数预算不保证发生落盘，
  也不预留磁盘空间；这是每任务上限，不是查询总量或集群总量配额。
- 真实执行测试夹具通过生产配置工厂取得预算，使 SQL/Native 测试不再绕开这个配置入口。

Native 下游的内部 `max_spill_bytes=0` 可以表示无限，但 Pangrova 的任务授权先拒绝
非正数 grant；不能将这两个层次的零值含义混为一谈。制品构建需要有效临时目录与正数预算，
本批补齐配置入口，但不以普通查询结果正确证明实际发生了内存压力落盘。

## 验证边界

- 77 个最近定向用例通过，包含配置到真实任务 grant、Worker 选择器和进程配置检查。
- Config/Scheduler API 全部测试类选择运行，213 个用例通过；API 最小 classpath 加载通过。
- 7 个现有 Native 策略/runner 测试通过，覆盖零禁用、两侧策略要求、节点与任务上限取小值。
- 跨角色 clean package 58 个用例中 57 通过；真实双 Worker、SQL 写入、动态 DAG、
  生成代际隔离、Native activation、Worker 目录与资源管理测试通过。
- 唯一失败仍为 SPI 装配门禁的四项既有缺口：InMemoryShuffleDataInventory、
  SpoolingExchangeManager、LocalSpillLifecycleManager、DefaultShuffleByteEgress。
  不增加豁免，不把这些实现的类存在当作生产支持。

发行物阶段在同一次跨角色 clean 编译结果上继续 package（未再次 clean），28 个选定用例通过。
94 个 JAR、94 个唯一模块名和 821 个编译包审查无违规；已额外确认重复的 SpillConfig 类和
Scheduler 预算中的 spillDirectory 访问器均未残留。按测试类取最近执行结果为 331 个 Java
用例：330 通过、1 个既有 SPI 装配门禁失败；7 个 Native 用例单列，不计入 Java 数量。

本批尚不是完整全仓回归、压力落盘或性能验收。上一批完整 clean verify 的失败及其修复
保留为历史证据，不继承为本批通过。Native、Native Bridge、Worker Core、Protocol、Schema、
Persistence 与 Client 输入未变；未修改 JNI 或 Native 内存/并发实现，不报告新 ASan 结果。

## 证据

- [FQN 与重复类型处置](spill-budget-fqn-dispositions.tsv)
- [逐类职责与输入 SHA](spill-budget-semantic-evidence.json)
- [生产代码差异](spill-budget-production-review.diff)
- [实际调用与配置入口](spill-budget-callers.tsv)
- [未修改的执行和后端输入](spill-budget-backend-evidence.json)
- [命令与结果](spill-budget-validation.tsv)
- [逐测试结果](spill-budget-tests.tsv)
- [Native 策略测试](spill-budget-native-tests.json)
- [发行物检查](spill-budget-artifacts.json)
- [静态门禁](spill-budget-static-evidence.json)
- [Native 制品](spill-budget-native-artifacts.json)
- [逐文件处置](spill-budget-file-dispositions.tsv)
