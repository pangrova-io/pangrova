# BTree 多列键契约、编码与真实构建（2026-11-03）

本批基线为修改前工作区 8,863 个文本文件，保留已有改动。针对原计划的契约所有权、
Provider 隔离和能力完整性，处理多列 BTree 建索引在编码阶段失败的真实缺口。

## 实际问题与处置

生产构建链为 `IndexFileBuilder` 按 descriptor 列顺序调用 `ColumnValueReader`，
再经 `IndexStrategyProvider.build`、`IndexSerializer`、存储和原有发布流程。
旧 BTree builder 会将 builtin 的 `CompositeKey` 放入公共 `BTreePage` 的 Comparable
字段，通用 codec 不认识这个实现类。修复前测试实际报错，无法写出多列索引文件。

另一个复现是全 NULL 输入构建为空键集合后，Provider 无条件返回 SKIP，包括 IS NULL。
这违反无假阴性契约；多列部分 NULL 行同样不能因被 builder 忽略就断言标量条件不匹配。

本批变化：

| Owner | 实现 |
| --- | --- |
| Indexing API | `CompositeIndexKey` 拥有完整元组、不可变组件、形状校验和字典序比较 |
| Indexing Codec | tag 11 编码完整元组；限制组件数，拒绝嵌套、NULL、未知标签及截断 |
| BTree Provider | 构建公共键；验证正页大小、非空/一致行宽和键形状；分页计算避免整数溢出 |
| BTree evaluator | 比较前检查形状；缺少覆盖证明时返回 UNKNOWN，不把标量当作完整元组 |
| BTree payload serializer | 验证 positions 对齐；按剩余输入检查声明计数后分配；拒绝尾随字节 |

原 builtin 键类删除，不保留转发壳。Codec 只依赖 API，不反向引用 Provider；没有新增
制品、SDK、服务注册、状态机、事务 owner 或外部资源。Codec 注册方法补充 Javadoc，
其可执行行为未改变。

## 值与格式边界

完整键为 2–1024 个非 NULL 标量，接受 String、Integer、Long、Double、Float、Short、
Byte、BigDecimal、BigInteger、UUID 的精确类。容器由键拥有；组件均为已知不可变值。
拒绝可变/custom Comparable、数字子类和嵌套元组，防止将任意 Comparable 等同于不可变值。
比较要求相同列数和逐列类型，不提供隐式类型转换、前缀匹配或 SQL 列绑定。

公共文件封装、原 scalar tags、CRC、其他算法布局保持不变。tag 11 是此前不可编码的
元组表示；旧 reader 遇到未知标签会拒绝读取，不把数据解释成别的键。没有新增历史
数据迁移或兼容转发。Native 当前只对该封装中的受支持 Bloom payload 执行裁剪，
本批未改变其输入布局、Proto、JNI 或 C++。

NULL 行仍按已有 builder 规则不建键。因为当前 BTree payload 没有 null-row coverage
和空索引 arity，空键集合只能返回 UNKNOWN。非空多列键也不能用于直接比较单列谓词。
这保证保留可能匹配的文件，并不宣称完成 SQL 元组/左前缀条件规划。

## 真实支持状态

- 完整多列键构建、持久化编码、解码及 Java 完整键成员判断：本批补齐。
- SQL CREATE INDEX 到 READY、published mapping 到实际文件内容：新增端到端验证。
- Native 查询结果：单独验证 NULL 行和单列条件在发布后仍保留。
- Native BTree pruning、Java `IndexEvaluationService` 业务调用及多列前缀执行：仍未接通。
  SQL 正确结果不能证明发生了 BTree 裁剪，组件测试也不能代替真实执行证明。
- 普通单列 Comparable 值所有权、其他数据模型、Indexing Persistence 拆分、总体
  全量生产回归和同环境性能比较：仍需后续实施。

## 验证与失败记录

先在旧实现运行两项回归，分别复现编码异常和 NULL 错误 SKIP。修复后定向 66 项通过。
首次正式静态验证通过 765 项测试后，被新增类的一处行长拦截；修正后第二次通过 779 项
测试，被 Codec 注册方法已有的缺失 Javadoc 拦截。均直接修复，没有增加 suppression、
禁用测试或降低门禁。第三次正式 `-am -Pstatic-analysis verify` 通过，866 项测试
全部通过。32 份静态报告没有 SpotBugs finding，保留分析器 missing-class 提示与
Connector API 一项既有复杂度 warning；不把通过写成全部诊断为零。
随后全 reactor clean install 的 400 项选定测试全部通过；包括完整 Indexing 模块测试、
结构门禁、启动 smoke、Index DDL 和索引查询回归。新增多列 SQL 回归实际通过，验证
READY、发布文件解码以及 NULL/标量查询结果。本批最新去重记录 1,091 项全部通过、
无跳过，62 个 Indexing owner 测试文件均有报告。八项仓库检查通过。
发行检查覆盖 110 个 JAR、110 个唯一模块名及 853 个编译包，没有所检查的旧 FQN、
split package 或服务注册违规；三个受影响制品的 86 个 class 与编译输出逐字节一致。
三种角色实际 classpath 和三份现有 Native 二进制哈希核验通过。
这是选择执行，不是无筛选全量生产回归；没有新增性能、ASan 或跨平台结论。
最终命令、终态和最新去重测试见下列证据。

属性测试覆盖不同页大小、重复键、跨页查找与编码后无假阴性；其他新增测试覆盖所有
已支持标量类型、逐字节截断、超大声明、嵌套/NULL/未知标签、可变输入、形状不匹配、
NULL 覆盖、页大小、row positions 与尾随数据。SQL 回归读取真实数据库发布 mapping，
打开实际文件并检查解码后的键；同时检查部分 NULL 行查询结果。

## 证据

- [逐类处置](btree-composite-fqn-dispositions.json)
- [调用引用定位](btree-composite-callers.json)
- [输入基线](btree-composite-input-evidence.json)
- [验证汇总](btree-composite-validation-summary.json)
- [逐项测试](btree-composite-tests.tsv)
- [最新测试](btree-composite-latest-selected-tests.tsv)
- [Indexing 测试覆盖](btree-composite-owner-coverage.json)
- [静态报告](btree-composite-static-evidence.json)
- [仓库检查](btree-composite-final-checks.json)
- [JAR 审计](btree-composite-artifacts.json)
- [class 一致性](btree-composite-class-parity.json)
- [角色 classpath](btree-composite-runtime-classpath.json)
- [Native 制品](btree-composite-native-artifacts.json)
- [执行输入](btree-composite-execution-inputs.json)
- [文件处置](btree-composite-file-dispositions.tsv)
