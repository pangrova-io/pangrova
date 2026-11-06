# 查询写授权契约与生成列 codec 的归属

本批基于当前工作区 8,169 个文本文件建立基线，不回退此前改动。
这是 P3/P6 的一个完整写入能力边界；整体模块化尚未完成。

## 归属和调用链

| 类型 | 目标与职责 | 事实、资源和装配 |
| --- | --- | --- |
| WriteAuthorityRequest | Query API `.api.write`；普通/生成列写意图、实际编译观察、原始写权限、目标声明 | 防御性复制集合；借用 ConnectorSession/实际查询 scope，不创建或关闭资源 |
| WriteAuthorityResolver | 同一个 Query API 制品 `.spi.write`；同步读取当前 INVOKER/tenant/声明授权 | Server 选择已有真实实现并在构造时注入；接口不选择后端、不重试或发布 |
| WriteAuthorityAdmission | Query Core `.core.write`；分配前校验完整声明、生成列存在性及内容摘要 | 不拥有 SQL、状态机、网络、Connector allocation 或提交 |
| GeneratedColumnMetadataCodec | Generation Codec `.column`；现有 Catalog 属性编码/解码、版本、大小和定义校验 | 纯 codec，无资源；协议 DTO 保留在 Protocol，Protocol 不反向依赖 codec |
| QueryDispatcher / QuerySchedulingEngine | 现有调度流程 | 授权 resolver 是 final 构造依赖；删除运行时 setter。无授权的读路径可用，所有写入分配前拒绝 |
| GeneratedWriteAuthorityResolver（现有具体实现） | 暂仍在 Coordinator AI implementation | 现有当前权限/tenant/模型事务和 borrowed Catalog scope；未声称已完成 Persistence workflow 拆分 |

实际链路：Server QueryExecutionBootstrap → 构造 QueryDispatcher → QuerySchedulingEngine →
Query Core WriteAuthorityAdmission → Query SPI WriteAuthorityResolver → 当前权限事务回执 →
唯一 write reservation → Connector allocation → 既有提交 fencing。
直接 DML 入口同样调用 Query Core admission。GeneratedValueMaterializationCoordinator 的
真实实现引用 Query 契约，已经不再引用 Coordinator 内部的旧写授权类型。

原子性顺序保持：编译观察不等于当前授权；当前表声明、Table ownership 和权限校验先于
reservation/allocation；只有 reservation winner 分配。取消、失败、未知 allocation、提交前权限
重查仍由既有 owner 处理。没有新增数据迁移、数据库、默认允许或 Memory fallback。

resolver 同步且必须支持并发调用，没有独立 cancellation/deadline API。
查询生命周期 owner 在授权结束后仍须执行既有 allocation/commit fencing，不能把 receipt
当成绕过取消、失主或当前权限检查的凭据。本批不扩张现有超时保证。

## 逐文件审查与验证

- [FQN 处置](write-authority-fqn-dispositions.tsv) / [调用方](write-authority-callers.tsv)。
- [逐生产文件语义证据](write-authority-semantic-evidence.json) / [非纯移动差异](write-authority-production-review.diff)。
- [文件处置](write-authority-file-dispositions.tsv) / [后端未变证据](write-authority-backend-evidence.json)。
- [逐次命令和结果](write-authority-validation.tsv) / [逐测试记录](write-authority-tests.tsv)。
- [最新定向验证](write-authority-latest-selected-tests.tsv) / [验证汇总](write-authority-validation-summary.json)。
- [JAR 审计](write-authority-artifacts.json) / [包环证据](write-authority-package-cycles.txt)。

测试覆盖最小 API classpath、请求集合不可变、错误权限证据拒绝、声明/摘要不匹配、缺失/失败
resolver、不分配、PostgreSQL 撤权、并发 reservation、真实普通写入和生成列 Native 发布。
测试专用 fixture 通过创建新 engine 注入各场景，不能替换已运行的 production resolver。

新 API 测试首次构造了不合法的空 SELECT 列观察；修复为真实结构的明确列与 policy observation，
未放宽生产校验。首次消费者编译的导入误插入许可证，以及移动后残留空目录门禁失败，均保留
在逐次验证结果中。后续通过不能覆盖首次失败记录。
