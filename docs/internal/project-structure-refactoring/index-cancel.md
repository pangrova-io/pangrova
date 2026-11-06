# Scalar index 构建取消与 attempt 所有权

本批修复 snapshot pin 中取消仍返回成功、重提交清除旧取消状态、借用 executor 时关闭
未取消工作，以及读取中取消仍继续写文件的问题。总模块化计划仍未完成。

## 实现与所有权

- 每次提交有独立 BuildAttempt；取消按本次调用捕获的请求集合传播，新提交不复活旧请求。
- lifecycle lock 拥有接收、暂停、恢复和关闭；锁内不执行 Future 回调。
- attempt 串行化开始、取消意图和终态选择，锁外完成 Future，避免跨 attempt 回调死锁。
- CANCELLED 的 isFullySuccessful 为 false，DDL/增量刷新不会据此发布 generation。
- 未开始请求可直接取消；活跃请求退出当前借用操作并释放 claim 后交付普通取消结果。
  Future.cancel 保留 Java Future 的立即取消语义，物理清理继续执行。
- IndexFileBuilder 在读取前、读取后、行迭代、Provider 构建后、编码后和新文件写入前检查取消。
  Coordinator 在文件和映射批次之间检查取消；已经提交的批次不会冒充完整构建。
- close 拒绝新请求、取消本协调器请求；不关闭借用 executor。OutputStream 仍由
  IndexStorageManager 关闭，reader/filesystem 的借用关系保持不变。

## 失败复现与验证

1. 修改前 8 项测试中，3 项取消回归失败，5 项原 pin 测试通过。
2. 初始修复后 8 项通过；回调锁外化、20 次关闭/提交竞争等扩展后 31 项通过。
3. 加入 claim 清理顺序后全 Core 123 项通过。
4. 非空测试使用真实 Hash strategy、codec 和临时文件，复现读取后取消仍写文件：3 项中
   1 项失败。阶段检查修复后，正常构建、读取、行迭代、写完、映射回调五种场景通过。
5. 最终全 Core 128 项通过，正式静态检查零发现、零 Checkstyle 诊断；分析器
   missing-class 提示保留于 [静态报告](index-cancel-owner3-analysis.json)。
6. 第一轮 clean 消费者 198 项通过。新增测试后第二轮被旧模块清单快照阻断：构建读取
   的清单尚无 Hash provider/serializer 引用，随后才生成新清单。没有降低门禁；当前
   清单单独 18 项架构测试通过，之后第三轮 clean 全 reactor 203 项全部通过、无跳过。
   覆盖建索引、重建、删除、增量查询、启动及依赖/包结构；仍是选定回归，不是无筛选生产回归。

[完整命令及结果](index-cancel-validation-summary.json)、[逐测试记录](index-cancel-tests.tsv)、
[Core 测试文件覆盖](index-cancel-owner-coverage.json)、[逐类处置](index-cancel-fqn-dispositions.json)。

## 发行与仓库复核

装配构建退出 0；112 个制品的旧引用、重复服务与 split package 检查通过，
Indexing Core 54 个 class 与编译输出逐字节相同；旧 PendingBuild 嵌套类已删除。
三种角色实际启动 classpath 和三份 Native 库哈希一致；本批未修改 Native/JNI/Proto 输入。
八项仓库检查通过；Core 19 个测试文件都有执行证据。

[JAR](index-cancel-artifacts.json)、[class 一致性](index-cancel-class-parity.json)、
[classpath](index-cancel-runtime-classpath.json)、[Native](index-cancel-native-artifacts.json)、
[仓库检查](index-cancel-final-checks.json)。这些不等于总体运行角色隔离或生产级验收完成。

## 台账捕获限制

最初捕获 8,936 个工作区文本文件，但 `!**/build*/**` 规则误排了源码 build 包，
三个生产类的变更前源码未保存。它不是完整基线，不能宣称历史逐文件哈希验证完成。
没有补造 before hash，也没有把 HEAD 当成有未提交改动的原工作区。当前源文件哈希、
真实失败复现和测试证据单独记录；其他未捕获路径不自动认定为本批新增文件。

[限制清单](index-cancel-baseline-limitations.json)、
[当前 build 包快照](index-cancel-current-build-package.json)、[本批输入](index-cancel-input-evidence.json)。
后续扫描只排除实际产物目录，最终全仓处置审查不能沿用这一错误排除规则。

## 剩余边界

取消是协作式的，已进入的 Connector/数据库调用没有被强制中断。借用线程池场景的
close 不等待在途调用退出；完整进程 drain、Provider 关闭顺序仍待运行角色验收。
没有更改原 claim 的后端协议：owner/attempt fencing、失主后的映射和发布原子性、
attempt 文件隔离、未发布对象清理/retention 仍需进一步实现。已落盘但未发布文件不会
在未知提交状态下被盲目删除；本批没有声称解决其全部回收路径。

其余领域模块、统一 Shuffle 生命周期与真实 Provider 装配、最终角色隔离、无筛选
生产回归、跨平台和性能对比继续属于原目标，均不能用本批通过结果替代。
