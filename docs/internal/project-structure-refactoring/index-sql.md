# Indexing PostgreSQL 固定 SQL 与数组资源所有权

本批从上一批完整工作区的 8,904 个文本文件建立新基线，继续解决真实静态门禁和
后端实现边界。此前制品拆分结果保持，未引入新后端、数据迁移或 API 兼容壳。

## 根因与实现

先前三处 SQL_INJECTION_JDBC 来自可变长度的 IN 占位符、列条件和 JDBC 执行转发。
值原本经过参数绑定，未据此认定存在直接拼接用户输入的漏洞；但动态 SQL 构造使
可执行语句形状依赖运行数据，且难以用静态规则验证。没有新增 SQL 忽略规则。

现在 PostgreSQL 实现使用固定 SQL：

| 能力 | 固定语句与绑定 |
|---|---|
| Descriptor 批量 ID 读取 | `index_id = ANY (?)`，单个 text array |
| READY/STALE 列选择 | 固定 tenant/catalog/schema/table 谓词、布尔 includeStale、`jsonb_exists_any(column_names, ?)` |
| 映射、artifact、计数 | 两个独立 text array 分别选择 index ID 与文件路径 |
| 可选 snapshot | 布尔参数表示无 snapshot 条件，实际 snapshot 仍独立绑定 |
| 已索引文件过滤 | 固定 index ID 加路径 text array |
| 孤儿标记 | 固定 index ID 加路径 text array，保留每批原有提交边界 |
| generation copy 排除项 | 原复制事务内，固定 index ID、目标 generation 和路径 text array |

保留原有 1,000 条文件批次划分、重复值集合选择行为、不同 index 隔离、完整 generation
选择、发布前不可见规则和 copy 原子事务。没有把业务值转成标识符或 SQL 片段。
空列选择仍返回空结果；includeStale 不引入 BUILDING 或其他租户的索引。
没有预设 PostgreSQL 查询性能改善，性能结论需要正式比较。

`JdbcTextSelection` 是 PostgreSQL 模块私有的 JDBC array owner。一个调用的关闭顺序为
ResultSet → PreparedStatement → Array.free → Connection.close；共享 DataSource 不被关闭。
prepareStatement 失败时已创建的 array 也会释放；array 清理失败附加到主异常，不能覆盖
原失败。Statistics Store 复用同一 owner，原统计 SQL 和参数顺序保持不变。

删除两个不再调用的 placeholders 方法和动态 SQL 生成器。JdbcIndexSql 的唯一剩余用途
是 generation 转换校验，已原样内聚到唯一调用方 JdbcIndexDescriptorStore，旧类删除。
这不新增第二个 generation 状态机或事务 owner。

## 行为验证

新增真实 PostgreSQL 用例覆盖：

- 引号、反斜杠、逗号、数组括号、Unicode 和类似 SQL 的文本均作为字面数据绑定。
- ID 重复与缺失、空选择、租户隔离、READY/STALE/BUILDING 状态选择。
- 未发布 generation 拒绝读取；发布后精确 generation/snapshot 读取，旧 generation 不冒充当前。
- 1,005 条路径跨批次计数、读取、标记；copy 排除 1,001 条路径且不删除源 generation 或其他 index。
- 语句创建失败后数组和连接各释放一次；数组释放异常不替换主异常。

故障测试包裹真实 PostgreSQL Connection/Array，仅在 prepareStatement 和清理点注入错误。
正常 SQL 行为由真实数据库验证，不用 mock 查询结果证明支持。

## 首次失败与处理

1. 新测试的空库初始化方法抛 SQLException，首次 test-compile 缺少声明；补充测试声明。
2. 初始 generation 读取用例没有建立发布事实，生产 facade 正确返回空结果。修正 fixture：
   先断言未发布不可读，再真实发布，随后验证 generation/snapshot。没有绕过发布入口或降低断言。
3. 所有 74 项模块测试通过后，静态检查发现移除旧 import 留下的多余空行；仅修正空白。
4. PostgreSQL 和 Parser API 的静态复验退出 0：Checkstyle 错误和 SpotBugs 发现均为零。
   原三处 SQL 告警消失，未增加 SQL 静态过滤规则。分析器 missing-class 提示另行保留。

5. 完整静态链继续发现 Parser Core 九处既有格式/Javadoc 错误；仅调整空白、换行和说明。
6. 随后发现 AstBuilder.tokenStream 与 RecoveryParseDetails.partialTree 两处构造器借用
   告警。实际所有权为单次调用的 ANTLR stream/tree，API ParseResult 仅含分离后的值。
   补充线程约束与生命周期说明，将原诊断 getter 的整类例外收窄为精确 getter/字段/类型，
   并对两个构造器作精确分类。完整过滤器测试验证三个分类各拒绝八种边界变化，SQL 检查仍开启。
   22 项 Parser 测试通过，包括诊断树修改不影响 API 结果/另一轮解析、保留补充 Unicode
   字符和原始注释。Parser Core 静态验证退出 0，错误和发现均为零，missing-class 提示保留。

7. Query Core 既有格式/Javadoc 错误修正后，51 项状态、清理、续租和保留测试通过。
   静态检查指出共享 registry/state 与已构建 protobuf 值的所有权分类未随包迁移更新。
   registry 必须指向实际查询注册表，cleanup 必须操作原状态机；复制会破坏已有清理语义。
   将九处分类精确到类、方法完整签名、字段、类型与告警，每处验证八种边界变化；
   可变 modelSignature 数组仍受检查，旧 Coordinator 类型不再获豁免。
   Query Core 复验 51 项通过，静态错误/发现为零；没有改动可执行查询逻辑。
8. 后续依赖链发现 Query PostgreSQL 一处异常消息行长、Security PostgreSQL 十一处
   格式/常量命名问题。仅展开回滚格式、换行和命名现有 255 字符 principal 上限，
   锁顺序、当前授权、receipt、提交结果未知与主异常保留语义均不变。
9. Security 的抽象 JDBC helper 构造器分类仍指向旧 FQN。审查确认 null 校验发生在
   borrowed DataSource 赋值前，构造阶段没有创建资源、发布自身或 finalizer。
   分类迁到精确构造器，不再通过旧整类规则处理；测试拒绝七种其他类/方法/签名/告警。
   同时删除该类已无对应 SQL 执行路径的整类 SQL 忽略规则，未增加 SQL 例外。

10. Generation Core 删除一个同包冗余 import 及其留下的多余空行；File PostgreSQL
    删除未使用的 URI import；Table PostgreSQL 三处授权锁调用仅换行。
11. Table PostgreSQL 的借用分类仍使用旧 JDBC 包正则。替换为四个真实 Store 的准确
    构造器、字段、类型和告警规则；每个验证八种改变或退役 owner 不匹配。
    Store 只关闭自己获取的 Connection，不关闭装配层拥有的 DataSource，未复制连接池。

12. Metadata Core 十处既有样式错误：补充查表的缺失/失败区分、ifExists 行为、
    显式 INSERT 列顺序与 borrowed session 的 Javadoc；其余仅空白/长行处理。
    本批没有改变 CatalogRegistry 既有装配或 Source snapshot 权威校验。

Parser API 的五处既有空行错误也已修正；没有改动 AST、诊断模型或方言语义。
完整依赖链、最新测试、最终制品结果以本批证据为准，不沿用上一批通过记录。

## 完整静态复验

最终从头执行 `-pl :pangrova-persistence-indexing-postgresql -am -Pstatic-analysis verify`
（选择 API 隔离、状态 fencing 与 scalar 权限测试）退出 0，15 项测试全部通过、无跳过。
67 份 SpotBugs 报告零发现；88 份 Checkstyle 报告零错误、7 条 warning，原样保留。
warning 涉及既有长参数、TableControlService 大类/复杂度、协议 admission seals、
Connector read binding 和 privilege grant revoke 复杂度，仍需按领域所有权进一步重构。
分析器 missing-class 提示随报告保留，本轮静态通过不等于无分析盲区。

[完整静态报告](index-sql-static5-analysis.json) 的时间窗口只包含本次执行产生的报告。
全 reactor `-Pnative-runtime,full-regression clean install` 使用本批相关测试选择，
854 项全部通过，无失败、错误或跳过，约 19 分钟。它包括固定 SQL/数组故障、Indexing
各模块、权限 mutation、Query 状态/清理、Parser、构建/发布 fencing、结构检查、启动
smoke 和真实 Native/Iceberg 索引 SQL 回归。跨本批最新去重正式用例为 856 项，均通过。
这仍是选定回归，不等于无筛选 `full-regression` 或生产级总体验收。
发行检查完成情况见以下制品证据；独立取消探针的失败另列，尚未修复。

## 发行复核

发行装配构建退出 0。112 个 JAR、112 个唯一 Automatic-Module-Name 和 854 个包通过
退役引用、服务注册与 split package 检查。10 个本批受影响制品的 1,009 个 class 与
各自编译输出逐字节一致，退役 JdbcIndexSql 与旧 Statistics 私有 TextSelection 不再存在。
Coordinator、Worker、Standalone 实际启动 classpath 的当前制品哈希核对通过；这不
替代尚未完成的最终运行角色重构。Native build、Bridge resource、bundle 三份库哈希一致。
本批没有修改 Proto/JNI/Native/schema 输入，没有声称重新构建 Native 或完成跨平台 ASan。

八项仓库检查均退出 0，见 [最终检查](index-sql-final-checks.json)。

证据：[JAR 审查](index-sql-artifacts.json)、[class 一致性](index-sql-class-parity.json)、
[实际 classpath](index-sql-runtime-classpath.json)、[Native 库](index-sql-native-artifacts.json)。
Indexing 与 Parser API 的 66 个测试源码文件都有本批执行记录；40 个生产类的处置及
直接源码调用引用记录于台账，源码引用扫描不等于完整运行可达性证明。

## 证据与未完成项

[命令及结果](index-sql-validation-summary.json)、[逐测试记录](index-sql-tests.tsv)、
[逐类处置](index-sql-fqn-dispositions.json)、[模块覆盖](index-sql-owner-coverage.json)、
[局部静态结果](index-sql-ownercheck-analysis.json)、[Parser Core 静态结果](index-sql-parser-analysis.json)、
[Query Core 静态结果](index-sql-query2-analysis.json)、
[所有权分类增删及源码](index-sql-policy-ownership.json)。

本批未完成 IndexMetadataStore 能力端口拆分和弱默认方法清理。下一步需追踪 build claim
实际调用：当前续租/释放未传 owner，expireStaleBuilds 没有生产调用；必须先验证与真实
构建、失主、重试和发布权威的关系，再确定实现或退役路径。仅增加新接口不能解决这些问题。
[后续只读审查](index-sql-next-review.json) 还记录了取消后完成结果、按 indexId 共享取消标志、
claim 早于发布释放、映射写入与发布锁分离、确定性文件路径未区分 attempt 的待复现边界。
这些不是本批已修复的能力，也未通过改变默认值掩盖。
其中取消问题已用独立 Java 探针在本次 clean 构建产生的 Core 类上确定性复现：
snapshot pin barrier 内取消，放行后返回 `COMPLETED, successCount=0, failureCount=0`，
拒绝成功发布的断言失败（[结果](index-sql-cancel-probe.json)）。探针不修改生产源码，
不能算正式测试通过；下一批将其转换为仓库回归用例并修复 attempt 生命周期。
其余竞态仍需正式故障测试验证。
通用 Shuffle、其余领域拆分、无筛选生产回归、跨平台验证和性能验收仍在总计划范围内。
