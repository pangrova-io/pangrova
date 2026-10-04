---
title: "质量门禁审计与精简依据"
description: "逐项核对门禁的实际行为、重复约束、误报风险和验证成本，说明保留、放宽及删除的依据。"
---

# 质量门禁审计与精简依据

本次审计的结论是：验证范围应按影响边界选择，结构度量由 Checkstyle 统一执行，历史过程约束不应成为永久代码限制。协议、JNI、安全、资源生命周期、真实执行证据和生产回归继续保留。较少的结构告警不等于较少的真实缺陷，也不等于完整 CI 已通过。

## 审计范围与证据

审计日期：2026-10-04。检查了 `AGENTS.md`、七份 `.pangrova/rules/` 规则、门禁脚本、实际 CI 调用、父 POM、Checkstyle/SpotBugs 配置、对应的源码与字节码检查测试，以及开发和测试文档。

结构比较使用仓库锁定的 Checkstyle 10.12.7、Maven Checkstyle Plugin 3.3.1 和 JDK 17，不使用手写方法长度估算。分别扫描当前工作区及提交基线 `e92532a77fcd813009e659a62e7691f08e972899`。提交基线通过 `git cat-file --batch` 导出至临时目录，没有 stash、切换分支或修改共享源码。

当前工作区含大量既有修改，因此两组结果分别报告。扫描仅包含各模块的 `src/main/java`，排除 `target` 中的生成代码；保留与实际配置相同的 `switchBlockAsSingleDecisionPoint=true` 和嵌套深度设置。当前工作区额外比较了应用现有豁免与不应用豁免两种结果。基线比较使用该提交自己的豁免文件。新加入的 `NestedForDepth=3` 在边界测试中单独验证；下表的前后比较不将新增循环检查混入原有指标。

原始摘要、示例位置和脚本观察结果保存在 [审计数据](quality-gate-audit-data.json)。这些是一次审计快照，不能当作未来提交的通用性能基线。

## 结构分布与阈值依据

| 指标 | 当前工作区 | 提交基线 |
|---|---:|---:|
| 生产 Java 文件 | 3,087 | 2,977 |
| 文件行数 P50 | 61 | 61 |
| 文件行数 P90 | 237 | 237 |
| 文件行数 P95 | 337 | 338 |
| 文件行数 P99 | 488 | 490 |
| 超过 500 行 | 11 | 13 |
| 超过 800 行 | 0 | 2 |
| 最长文件 | 779 | 1,057 |

800 行是少数较大且职责完整文件的硬上限，不是日常目标。绝大多数文件仍应保持现有的短小结构。不能仅凭行数断言这 11 个文件都是误报，也不能因放宽后通过而免除职责审查。

### 同一源码、不同阈值的真实解析结果

| 门禁 | 原阈值 → 新阈值 | 工作区原始告警，前 → 后 | 工作区应用豁免，前 → 后 | 基线应用豁免，前 → 后 |
|---|---|---:|---:|---:|
| FileLength | 500 → 800 | 11 → 0 | 6 → 0 | 8 → 2 |
| MethodLength | 50 → 100 | 14 → 0 | 12 → 0 | 15 → 0 |
| CyclomaticComplexity | 8 → 15 | 238 → 22 | 198 → 17 | 137 → 15 |
| ParameterNumber | 7 → 10 | 36 → 13 | 28 → 10 | 28 → 10 |
| 合计 | — | 299 → 35 | 244 → 27 | 188 → 27 |

主要压力来自复杂度 8，而不是文件长度。旧阈值会拦截复杂度为 9 的 JDBC 驱动逻辑、复杂度为 10 的资源所有权和客户端配置代码；也会拦截 97 行的启动装配方法。是否值得拆分，必须结合职责、错误处理和资源生命周期判断。

新阈值仍会拦截复杂度 17 的快照/模型解析、复杂度 19 的写入资源分配，以及 11–13 参数的装配方法。保留这些告警供对应改动审查，未为它们增加豁免，也未把这些工作区既有实现纳入本次重构。

Checkstyle 将圈复杂度定义为决策点加一，包含布尔运算和循环；原正则扫描仅计算部分 `if/switch/catch`，两者不能互称同一个指标。[官方说明](https://checkstyle.sourceforge.io/checks/metrics/cyclomaticcomplexity.html)允许将 15 作为现实代码的检查上限，但这不证明每个复杂度 15 的方法都易读。文件 800、方法 100、参数 10 是结合本仓库分布选择的项目政策，不是官方统一推荐值。方法长度的计数规则见 [MethodLength](https://checkstyle.sourceforge.io/checks/sizes/methodlength.html)。

### 可读性保护

- 维持单一职责、明确控制流和能表达业务动作的名称。
- `if` 和 `try` 的原有深度参数保持为 3、2。删除正则缩进估算后，循环深度交由同一 Java 解析器的 `NestedForDepth=3` 检查；混合控制流的可读性仍需评审。
- 构造器超过 6 个依赖需要审查职责，但不再强制引入任意 `Dependencies` 包装对象；方法和构造器的参数硬上限统一为 10。
- 接口超过 12 个方法提示按消费者职责审查。取消枚举 30 个值的任意上限，因为领域集合的大小不能证明设计失当。
- 允许准确描述职责的 Adapter、Observer、Visitor 等名字；禁止框架类型泄漏、模块方向错误和运行时完全限定类名冲突的约束继续保留。
- 不通过转发类、泛化上下文袋、缩写、压缩多条语句或隐藏分支来达标。
- 结构指标不能证明正确性；行为、错误路径、资源清理和并发测试仍负责其各自边界。

## 门禁逐项决策

| 限制或检查 | 实际机制与问题 | 决策与保护措施 |
|---|---|---|
| 每次修改必须全量回归 | workflow/testing/build-pipeline 多处绝对要求，与 development 的按影响范围验证冲突；甚至 native 目录的文档也被要求重建 | 合并到 development 的验证政策。生产 CI/发布回归保留；运行时跨边界改动验证相关链路，重大执行改动本地全链验证 |
| 每次 Java 修改先 compile 再 test | Maven test/verify 本身执行前置编译生命周期 | 删除强制重复步骤，保留编译诊断入口 |
| 所有 native 目录修改都跑 ASan | 把目录位置与内存/所有权风险等同 | 内存、所有权、JNI、并发修改仍要求 ASan；文档及工具修改跑其自身检查 |
| 所有历史失败都必须当场修复 | 使局部任务扩展成全仓修复，并要求操作共享脏工作区来证明基线 | 修改引起的失败必须修复；无关失败用日志、基线导出或隔离复现记录，不宣称未运行/阻塞的检查通过 |
| 每个类/方法/错误分支一份测试 | 把实现结构与测试行为覆盖等同，纯重构也被要求新增测试 | 改为行为与风险覆盖；相关正常、边界、错误、并发和清理路径保留；现有覆盖可验证纯重构 |
| 核心逻辑必须属性测试 | 无论是否存在有价值的不变量都要求 jqwik | 按不变量是否强于枚举样例选择；不取消业务回归测试 |
| 所有集成测试必须起容器 | 本地组件装配也被要求引入 PostgreSQL/Ollama/native | 需要对应外部服务时使用真实容器；不把 in-process integration 伪称为分布式 E2E |
| Docker 不可用必须修主机 | 本地前置条件失败可能阻断任何可独立验证的任务 | 诊断并报告具体阻塞，禁止禁用测试或声称通过；无关主机整改不强制扩大任务 |
| Checkstyle 和 SpotBugs 重复执行 | CI 先单独运行二者，再经 static-analysis 重跑 | 合并为一次 profile 执行；编译前置条件由 verify 满足 |
| CleanCodeMetricsPropertyTest | 906 行的第二套正则解析器；跳过不可读文件、仅识别部分方法/分支、存在大量简单类名豁免；指标还与 Checkstyle 不一致 | 删除重复实现，由 Checkstyle 统一执行结构检查；新增真实解析器的允许/拒绝边界测试 |
| 历史重命名清单 | 每次测试抽样搜索旧重构方案中的名称，永久禁止某些今天仍可能合理的类名 | 删除一次性重命名账本及其扫描；保留当前类型身份、框架隔离和缩写约束 |
| 跨模块简单类名不得重复 | Java 类型身份是完全限定名，简单名重复可能属于不同领域；旧随机抽样还可能漏报 | 动态发现现有 Maven 叶模块，读取源码 package 声明并遍历完全限定名唯一性，取代固定模块清单和随机配对；保留命名约束改为穷举检查，读取失败不再静默跳过。校正规则 C1 的模块范围：observability 允许 Flight 适配器，protocol 允许 Arrow；取消简单类名豁免，不扩大框架禁令 |
| 包文件数量 8/15/25 门槛 | 同时维护 11 个历史拆分包、28 个豁免包和 3 个钉死数量的包；当前 452 个声明包中有 27 个超过 15、11 个超过 25。AST statement 有 57 个类型，connector 有 47 个类型；数量本身不能证明职责不内聚 | 删除三个数量测试及历史清单，不以新增豁免放行。按能力、消费者和依赖方向审查拆包；保留包命名检查并改为动态发现、实际 package 声明穷举，读取失败报错；模块方向和依赖检查保持 |
| 模式后缀禁止 | 名称与职责脱钩，可能迫使准确的 Observer/Visitor 改成泛化名称 | 放开真实模式名称；继续要求职责明确、公共契约不暴露不必要实现细节 |
| 固定 BOM 使用检查 | 脚本只维护八个历史依赖的 artifactId/import 清单，不校验版本属性；递归 find 对每项重复扫描，本次 120 秒超时 | 删除脚本及 CI 调用，纠正规则描述；父 POM 版本管理和依赖评审保留，不再要求无需求的旧库一直被使用 |
| 文档 60 天年龄检查 | 仅查 git 提交时间，不比较关联源码，更不证明文档事实正确；本次 25 份文档都“新鲜” | 删除脚本及 CI 调用；行为文档同步与类型引用检查保留 |
| ADR trailer grep | 仓库没有它扫描的 docs/architecture/adr 目录，也没有参与的 trailer；实际立即返回 0 | 删除空门禁及 CI 调用；架构决策仍通过 canonical rules 和文档评审维护 |
| 分布式清单按模块目录触发 | 文档、普通测试也触发；原脚本清空 PR_BODY、使用 macOS 不兼容截取且漏掉删除；CI 直接插入 PR 正文会被引号破坏 | 改为生产/config/build 变化触发，运行时 Markdown 资源与名为 docs 的源码包仍触发；修复环境输入、段落截取、删除检测及两处 CI 正文传参；允许有理由的 N/A，拒绝裸 N/A 及其他段落的勾选冒充证据 |
| P0 证据必须粘贴全部 grep | 重复日志增加上下文，逐行同类解释未必增加审查价值 | 保留同类缺陷搜索；提交命令、命中数、代表样本、分类和完整输出链接 |
| 所有 PR 跑额外 TPC-H/JMH | 不区分文档和运行时修改；短采样性能波动未经稳定复测不能直接认定阈值过紧 | 仅对生产、依赖、benchmark 和相关工具变化触发；未知输入保守触发；10%/15% 等既有回归阈值不改，生产正确性链不删 |
| 旧工具规则入口 | .claude/.qoder 只含到 canonical rules 的两个符号链接，setup 会自动重建 | 删除两目录和重建逻辑；仅保留 AGENTS.md + .pangrova，setup 改为只读布局验证 |

## 保留门禁及其实际边界

| 检查 | 本次观察 | 为什么保留，以及不能证明什么 |
|---|---|---|
| SPI annotations | 139 个公共类型有注解，基线缺口 0；约 5 秒 | 明确契约稳定性是有效政策。注解本身不证明提供者已装配 |
| SPI stability 诊断 | 22 个类型通过；约 12 秒；未作为独立 CI job 调用 | 保留可选本地诊断。文本 implements/extends、间接实现和 lambda 需要人工理解，不能当作运行时完备性证明 |
| 文档类名引用 | 174 项通过；约 2 秒 | 便宜且能发现删除/重命名后的文档漂移；PascalCase 抽取与白名单仍是启发式，不证明整篇文档正确 |
| Stub patterns | 本次通过；约 65 秒 | 防止未说明的未实现方法进入生产路径。它检查空 UnsupportedOperationException/日志模式及上下文，不分析实际调用图，不能据此声称“无死代码、所有方法都已实现” |
| ArchUnit wiring/reachability | 检查字节码引用图、入口、注册和已有基线 | 与 stub 模式检查不同；保留二者的职责区分。静态可达不等于运行时必然执行，仍需集成/E2E |
| 包命名及模块方向 | 当前命名、源码导入/模块映射及已有例外保持 | 移除数量配额不等于取消所有权约束；源码规则和基线并非完整的运行时隔离证明 |
| PackageDependencyAcyclicityPropertyTest | 检查指定模块内的双向 import，并有数据模型例外 | 保留原门禁，但它不证明任意长度的环都不存在，也不覆盖所有新增模块；Maven/字节码边界检查与评审仍需互补 |
| E2E truthfulness | 通过；约 0.08 秒 | 拒绝禁用、常量断言和单端点冒充多 worker 等已知模式；模式扫描不是所有测试质量的充分证明 |
| 环境路径检查 | 5 个禁止样本、8 个允许样本通过；约 0.03 秒 | 防止机器路径写入活跃文件；本次仅测模式自检，未将其声称为完整仓库扫描通过 |
| Proto descriptor | 本次核对实现，未以修改后的 descriptor 重新接受协议 | 比较 descriptor 字节和 canonical source 所有权，不是语义兼容分析；安全增加字段也会造成 drift，更新基线不证明 wire/API 兼容。官方区分 [wire-safe 与应用兼容](https://protobuf.dev/programming-guides/proto3/#updating) |
| JNI symbol symmetry | 保留本地脚本和包装/加载验证要求 | 符号存在不证明所有权、类型和生命周期正确；与 JNI/E2E 测试互补 |
| Diff coverage ≥60% | 门禁和真实报告来源保持 | 报告缺失、过期、错误源码映射仍失败；不为 token 成本而取消行为覆盖 |
| SpotBugs/FindSecBugs | profile、分析强度、过滤器保持 | 配置的 Medium 是检测优先级/置信度，不等于运行时严重性。没有误报样本和安全分类证据时不整体降级 |
| Native/WERROR/sanitizers | 构建和相关风险场景保持 | 编译警告、内存与未定义行为直接影响 JVM/查询执行，不与格式清单合并删除 |
| 生产同源构建和真实 E2E/smoke | 原有 production-regression 与 artifact provenance 保留 | 静态分析/工作流语法通过不能代替实际镜像、部署和 SQL→Native→结果的执行证据 |

保留脚本的观察时长是同机一次并发审计的墙钟时间，不是可移植基准，也不是 token 消耗估计。没有使用这些时长推断“节省了某个百分比的 token”。

## 验证记录与实际限制

- Checkstyle 边界 fixtures 使用真实锁定解析器，验证 800/100/15/10 的允许边界及超限拒绝；同时验证构造器、if/for/try 深度。解析器仍必须返回失败来拒绝负例。
- 门禁回归覆盖文档/测试免触发、生产和 benchmark 触发、未知输入保守处理、删除生产文件、缺失 base、PR_BODY 环境输入、最后一个 Markdown 段落、N/A 理由及 canonical layout 不重建旧目录。
- 穷举命名时复现了 FlightClientTracingInterceptor 误报；核对客户端/服务端拦截器实现、服务端装配和 C1 模块范围后修正测试，新增适配器允许及 SPI/common/protocol 拒绝的边界回归。没有增加简单类名豁免。
- 包结构的三个数量门禁被有依据地移除，保留的命名检查仍穷举所有发现的包；测试项数因此减少，不把数量减少解释为缺陷或行为测试减少。
- 结构比较四次工作区扫描、两次基线扫描均实际解析对应文件集合；存在告警的扫描正常返回失败，未把它们当作 CI 通过。
- 首次完整模块测试因 subprocess 使用系统 Python 3.9 而失败；生成器要求 Python ≥3.10。后续改用已有 Python 3.12 重跑，未禁用或修改该测试；最终清理所属模块后重新构建，78 项测试及所需上游模块通过，零失败、错误和跳过。包命名检查穷举 454 个包（包含仅有 package-info 的包），类型命名检查穷举 3,029 个生产类型。
- 广域 static-analysis 在已有未跟踪的 `ExecutorDrain.java` 上遇到三项 EmptyLineSeparator/LeftCurly/RightCurly 格式问题。该文件不属于本次门禁修改，未修改、删除或豁免。此项广域验证不能宣称通过。
- 门禁策略回归 17 项通过，Checkstyle 14 个真实边界 fixtures 通过允许/拒绝验证；工作流 YAML、job 依赖、shell 语法和 canonical layout 验证通过。
- 实际 hosted CI、生产镜像和全套 Native/ASan/E2E 没有在本次规则/工具修改中执行；其门禁仍由原有生产 CI 负责。本次结果不构成发布或部署验收。

## 复核方法

2026-10-04 生产整改复证补充：Checkstyle 的 `com.sun.*` 全包禁令误挡
`jdk.httpserver` 导出的公开 HTTP/HTTPS API。根据
[JDK17 官方 API](https://docs.oracle.com/en/java/javase/17/docs/api/jdk.httpserver/com/sun/net/httpserver/package-summary.html)，
现有 IllegalImport 仍拒绝 `sun.*`、`jdk.internal.*` 和其他 `com.sun.*`，
只按确切包边界区分 `com.sun.net.httpserver` 与其公开 SPI。真实锁版本 Checkstyle
红测拒绝了3种公开API；修正后9个允许、14个结构/内部API拒绝 fixture 实际通过，
包括 static import 和相似包前缀负例。没有源码级 suppression 或全限定名绕过。

同轮 SpotBugs4.8.3 将 immutable `ByteString.writeTo(...):void` 误判为 setter。
唯一 SHA256 getter 保留 `ByteString.copyFrom` 的输入所有权，不新增防御性重复复制；
真实 canonical digest、导出 byte-array 修改隔离与只读 buffer 回归通过。
误报分类仅匹配 `FunctionBindingDeclarations.declarationSha256()` 的零参数
ByteString 返回签名、同名 ByteString 字段及 `MS_EXPOSE_REP`。真实 pinned
Filter API 验证该新增 Match 的1正例与8个类型/方法/字段/类/规则变更负例。
这不消除原 filter 的 protocol 包级 `EI_EXPOSE_REP` 排除局限，也不等于实际
可变性漏洞已被修复；后续须单独复核这些既有广域排除。

1. 导出 `git ls-tree -r --name-only <ref>` 中各模块的生产 Java 源码，用 `git cat-file --batch` 读取 blob；工作区集合使用 `rg --files -g '*.java'` 后筛选 `/src/main/java/`。
2. 建立隔离 Maven 工程，使用父 POM 锁定的 Checkstyle plugin 和 engine。把每个模块 source root 配为 sourceDirectories，只启用 FileLength、MethodLength、ParameterNumber、CyclomaticComplexity、NestedIfDepth、NestedTryDepth。
3. 在完全相同源文件集合上分别运行原始 500/50/8/7 与拟议 800/100/15/10；保留 switch 计数和嵌套参数。依次加入或移除相同 SuppressionFilter，解析 checkstyle-result.xml，确认 parsed_files 与输入文件数一致。
4. 不通过修改业务源码、增加豁免或过滤失败报告来减少计数。分别记录提交基线与工作区，不把未提交的既有变更归因于本次政策。
5. 用 `.pangrova/scripts/tests/checkstyle-boundaries-test.py` 复核现行门禁的实际允许/拒绝行为，用 `test_gate_policy.py` 复核触发范围与保留约束。

官方参考：[Maven Checkstyle 的输入范围](https://maven.apache.org/plugins/maven-checkstyle-plugin/check-mojo.html)、[SpotBugs check 在 verify 阶段的行为](https://spotbugs.github.io/spotbugs-maven-plugin/check-mojo.html)、[循环嵌套检查](https://checkstyle.sourceforge.io/checks/coding/nestedfordepth.html)。这些资料用于解释工具语义，不用于替代仓库执行证据。
