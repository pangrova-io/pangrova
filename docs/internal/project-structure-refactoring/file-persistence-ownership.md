# File Persistence 实现与验证边界

基于前批 Table 一致性工作区建立 8,528 个文本文件的快照，继续原全仓目标。
迁移 17 个 Java 源码：6 个契约类型、6 个 PostgreSQL 实现、4 个原后端测试，以及
1 个只用于测试的 Memory registry。迁移后逐文件包名归一化与基线完全相同。
没有修改 SQL、schema、JNI、协议或 Native 输入，没有新增数据库后端或数据迁移工具。

## 制品、包与事实 owner

| 制品 | 实际职责 |
| --- | --- |
| persistence-file-api | space/profile/directory/claim 契约及不可变事实；仅依赖 Common、File API、Filesystem API |
| persistence-file-postgresql | 既有 6 个 SQL 实现；连接、事务、序列化及后端权限协作 |
| file-core | 面向新窄 API 的业务调用，移除通用 Persistence API 依赖 |
| testing-kit | 测试专用 InMemoryStorageProfileRegistry，退出 Coordinator 生产包 |

通用 Persistence API/JDBC 的 File 源码和包根已移除，两个通用制品目前仅余 Generation。
现有 Canonical schema 和 PostgreSQL revision-history trigger 继续作为唯一初始化/历史写入路径。
移除了 SPI 门禁中关于 Memory registry 的旧豁免及不准确的生产装配说明，没有新增豁免。

Provider 逐类的操作、调用方与资源 owner 见 [处置台账](file-persistence-fqn-dispositions.tsv)、
[语义证据](file-persistence-semantic-evidence.json) 和 [引用记录](file-persistence-callers.tsv)。
26 个生产/测试设施源码处置均只有包名、引用或测试设施位置变化，不代表全仓语义审核已经完成。

## 实际支持链与未完成边界

| 能力 | 已有事实与调用 | 本批后仍需处理 |
| --- | --- | --- |
| Storage Profile | 生产 DDL 使用 PostgreSQL 当前定义及不可变历史 revision | longest-prefix 的多个候选等长判断需补充排序无关的语义测试 |
| Volume/Stage | 持久化唯一 namespace identity；mutation 与目录失效保持原事务 | 完整 Provider factory 与后续 Server 装配收敛 |
| Directory | exact Stage + attempt + expected page + terminal publish；不持有文件 bytes | 领域运行时与多实现能力描述继续按原计划完成 |
| File authority | 当前 membership、tenant 与权限共享锁阻止撤权越过 issuance | 后端协作端口/workflow 仍需收敛；本批不改锁或权限语义 |
| COPY source read | issue/redeem/stage proof/frozen-input 与 execution fence 由原 PostgreSQL 操作持有 | public nested DTO/类型仍被两个 Coordinator 类引用，下一批抽为调用契约 |
| COPY file claim | 后端实现及竞争测试真实存在，原显式未接线状态不变 | 不声称 URI publication 已完整支持，不用移动文件掩盖缺口 |
| Worker 文件解析 | 旧 FileSpaceObjectStore 仍有 API 引用；真实 HTTP 路径使用授权 descriptor | 旧 resolver 的生产可达性/退役需要独立审核；Worker 不装配 PG 后端 |
| Memory profile | 测试 fixture 的 instance-local maps | 无生产支持声明，不新增空 Memory 制品 |

现有部分 File 事务清理仅捕获 SQLException/RuntimeException，异常清理一致性仍需后续补齐。
这些不是本批新引入的行为，也不是通过现有测试后可以忽略的生产验收项。

## 验证记录

基线 176 PASS，包含真实 PostgreSQL、HTTP 和 5 GiB 上传/下载及跨 4 GiB 读取。
迁移后近端 clean 87 PASS，包含最小契约 classpath 与真实 Provider classpath 的新增检查。
首次全 reactor 消费者验证 29 / 28 PASS / 1 FAIL：迁移后旧源码空目录违反门禁；仅删除
确认为空的旧目录，保留失败记录并重新执行。后续结果见实际汇总，不沿用旧批次通过结论。

证据：[测试汇总](file-persistence-validation-summary.json)、[逐测试](file-persistence-tests.tsv)、
[源码包图](file-persistence-package-graph.txt)、[输入未变更](file-persistence-input-evidence.json)、
[静态检查](file-persistence-static-evidence.json)、[Native 制品](file-persistence-native-artifacts.json)、
[实际 JAR](file-persistence-artifacts.json)、[角色 classpath](file-persistence-runtime-classpath.json)、
[逐文件处置](file-persistence-file-dispositions.tsv)。

完整目标仍未达成，当前批次不替代通用 Shuffle、跨域 workflow、全量能力闭环、最终生产回归
或性能/跨平台验收。所有原有工作区改动保留；没有提交 Git commit。

最终消费者重跑为 233 / 232 PASS / 1 既有 SPI FAIL，0 ERROR、0 SKIP；包括 File DDL 5 项、
File 控制面 3 项、大文件 1 项、真实双 Worker 1 项与 Native smoke 3 项。
COPY 事务/权限/fencing/发布及 TLS 调用者补充验证 27 PASS；独立 HTTP/Native COPY 4 PASS。
最终全 reactor package 的 21 项架构测试通过。按每类最后一次实际执行，选择范围合计
264 / 263 PASS / 1 FAIL / 0 ERROR / 0 SKIP，唯一当前失败仍是未接线的
InMemoryShuffleDataInventory，没有添加豁免。

角色 classpath 检查通过：Coordinator/Standalone 使用最新 File API 与 PostgreSQL 制品；
Worker 只携带现有解析器实际引用的 File API，没有 File PostgreSQL；三个角色均不包含
Testing Kit。源码 825 个包零循环，8 项静态检查与三处 Native SHA 校验通过。
本批基于 macOS arm64/JDK 17/Maven 3.9.16/Docker；未构建 Linux 镜像、追加 ASan、
完成性能对比或全平台生产回归。

实际制品审核：106 个 JAR、106 个唯一模块名、831 个编译包，零违规；无旧 File FQN、重复服务或 split package。
