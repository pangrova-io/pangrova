# COPY source admission：调用契约与后端分离

在 File Persistence 已验证的工作区上建立 8,548 个文本文件基线，继续实现原计划。
本批不是新 COPY 功能：把已经实际运行的 PostgreSQL source-read authority 从调用方隔离，
使 Coordinator 调用接口，Server 选择后端。没有新增制品或空 SPI 包。

## 所有权与源码处置

`pangrova-persistence-file-api` 新增 `api.copy.CopySourceReadAdmissions`，包含四个实际原子操作：
issue、issueFrozenInput、redeem、recordStagedSource。六个既有不可变值类型 Ticket、Scope、
StagedSource、Redemption、FrozenInputAttempt、FrozenInput 迁入该接口，删除后端中的定义。
原构造校验和 command digest 值语义保留；JDK 本地值校验放在同制品 package-private helper。
没有引入 JDBC、JSON framework、SDK、配置加载或默认成功实现。

API 明确并发调用、精确 attempt 重放、当前 tenant/profile/READ/execution authority、资源
由 assembler 持有，以及失败/超时不代表已回滚。它没有伪造新的异步取消或提交查询能力。
当前 managed S3 URI 和版本限制照旧，不把接口抽取冒充新增 Filesystem backend 支持。

PostgreSQL 类实现新接口。连接、SQL、权限 gate、execution fence、当前权限读取、nonce
生成和存储继续由同一个原子操作持有；Server 的两个生产装配位置仍显式创建真实实现。
Scope/record/nonce 的实际入库与确认顺序不变。已经由不可变 Ticket 构造器完整校验的 nonce
由后端直接 Base64 解码；没有重新接受未校验的字符串或提供绕过构造器的入口。

`CopySourceTaskBinder` 和 `CopySourceReadResource` 的字段、构造器和值转换只使用 API，
移除 Coordinator Core 对 File PostgreSQL 的生产依赖，保留真实后端消费者测试的 test scope。
新增生产依赖闭包门禁覆盖 File API、File Core、Coordinator Core、Worker Core；没有将
旧通用 Generation 后端或其他尚未收敛的领域隐瞒为全部隔离完成。

逐段核验覆盖余下 Provider 主体、迁出的六个 record、四组校验函数和 digest helper：
除了接口声明、导入、Override 标记及对已校验 Ticket 的解码，Provider 主体与基线完全一致。
SQL、事务、锁顺序、失败处理和当前权限语义没有借本次类型移动改写。

## 验证与剩余工作

近端 clean 29 PASS，含最小契约 classpath、实际 Provider classpath、真实 PostgreSQL COPY
权限/fencing/不可变 proof/发布与 TLS 消费者测试。最小 classpath 不只加载类型，还实际
构造 nonce 和 staged proof，拒绝非法值，并验证泛型方法签名。
全 reactor clean 消费者验证 61 / 60 PASS / 1 既有 SPI FAIL，0 ERROR、0 SKIP；
包括生产装配可达性、真实双 Worker 与 Native smoke。新 API/实现没有新增门禁失败。
当前唯一失败仍是 InMemoryShuffleDataInventory 未接线，未修改其检查或添加豁免。

证据：[FQN 与 record 处置](copy-admission-fqn-dispositions.tsv)、
[逐类语义](copy-admission-semantic-evidence.json)、[源码等价核验](copy-admission-extraction-proof.json)、
[调用引用](copy-admission-callers.tsv)、[测试汇总](copy-admission-validation-summary.json)、
[逐测试](copy-admission-tests.tsv)、[源码包图](copy-admission-package-graph.txt)、
[实际 JAR](copy-admission-artifacts.json)、[角色 classpath](copy-admission-runtime-classpath.json)、
[静态检查](copy-admission-static-evidence.json)、[输入未变更](copy-admission-input-evidence.json)、
[Native 哈希](copy-admission-native-artifacts.json)、[文件处置](copy-admission-file-dispositions.tsv)。

File 最长前缀歧义与异常清理、旧 Worker resolver、未接线 claim、完整 Provider 工厂、
Generation/Persistence 剩余拆分、跨域 workflow 和通用 Shuffle 仍需继续完成。
没有宣称完整生产回归、全平台或性能验收通过；没有提交 Git commit。

抽取后独立 HTTP/Native COPY 4 PASS；最终全 reactor package 的 22 项架构测试通过。
最新选择范围 65 / 64 PASS / 1 FAIL / 0 ERROR / 0 SKIP。三种运行角色 classpath 审核通过：
Coordinator/Standalone 包含本批 API 与真实 PostgreSQL 实现，Worker 没有 File PostgreSQL，
三个角色均未引入 Testing Kit。8 项静态检查通过，826 个源码包零循环，Native 三处 SHA 一致。
验证平台为 macOS arm64/JDK 17/Maven 3.9.16/Docker；没有扩大为性能、Linux 镜像或全平台结论。

最终 JAR 检查：106 个制品、106 个唯一模块名、832 个编译包，零违规；旧 PostgreSQL 嵌套值类和 Core 中的 File Provider 引用均已消失。
