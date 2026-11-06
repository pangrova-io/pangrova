# Local Shuffle 目录与文件资源所有权（2026-11-03）

本批基于修改前工作区的 8,844 个文本文件继续实施，不回退已有工作。
范围是 Local Provider 内部的读取、发布、清理和文件预算；没有变更公开 Spool SPI、
数据格式、Proto、JNI、Native、数据库初始化或进程装配。

## 发现与修复

上一批 Catalog 静态依赖检查在 Local Store/Writer 的三个可空 parent 诊断和一个临时
文件名路径诊断停止。逐路径检查还复现了真实错误：`read(selection)` 原来只保存路径，
第一批读取前替换分区目录后，reader 会打开另一目录中的合法段，返回错误数据。
复现测试先在原实现失败，修复后通过；不是只通过调整代码形状消除诊断。

本批处理：

- Reader 在返回前打开所有明确选中的 producer/attempt 文件，并直接从这些 channel
  读取，不在后续 batch 中重新打开路径。某个段缺失时整体失败，关闭已获取的 channel。
- 新内部 `LocalSpoolPaths` 唯一负责路径映射、根目录和 partition 身份校验。
  按根目录相对组件检查，取消 nullable parent 回溯。临时文件使用固定私有前缀。
- Writer 在发布和临时文件清理前检查绑定的目录身份。目录被替换时拒绝操作，保留
  可重试清理状态；不删除替换目录中的同名文件。原 force、atomic move 和未知结果
  错误保留，没有非原子回退。
- Store 统一计数文件预留，默认上限 1024，可通过 factory 属性
  `spool.local-fs.max-open-files` 设置。每 writer 一个，每 reader 按全量选中段计数；
  目录锁另计。超限立即拒绝，不丢 producer、不创建等待队列。
- 资源先登记再获取，失败路径尝试归还；只有成功 close 后归还预留。
  EOF/commit 不自动释放 query cleanup lease。重复 close 不重复归还。

目录布局与 version 1 framing 未改变，没有历史数据迁移需求。

## 精确保证及未完成事项

已固定的是已打开的文件 channel，而不是 OS 目录句柄。配置根必须为进程私有且稳定的
目录，不允许外部进程并发改写。身份检查能拒绝两次操作之间的目录替换，但不消除检查
与文件系统操作之间的全部 TOCTOU 窗口，也不防止外部进程直接改写同一个已打开 inode。

JDK 的 [SecureDirectoryStream 文档](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/nio/file/SecureDirectoryStream.html)
明确其相对操作绑定已打开目录，且依赖平台支持。本机 JDK 17/macOS 实际探测未提供
SecureDirectoryStream。本批没有声称实现该保证，没有引入平台专属 JNI，也没有使用
失败后静默降级。目录 `fileKey()` 不可用时明确拒绝创建/访问。其他平台尚未验证。

Local 仍是同步文件原语，I/O 在 Store monitor 内串行执行。Core 的 Java exchange adapter
已有真实组件测试，但生产角色仍未装配 Local Provider，也没有 Native Local 模式、
通用异步背压/取消、exchange receipt、跨节点恢复或 Celeborn 集成。
本批不能代替计划 P7 的三种实现契约与真实执行链验收。

使用约束见 [Local Provider 说明](../../../pangrova-shuffle/pangrova-shuffle-local/README.md)。

## 验证范围

新增真实文件测试覆盖：读前目录重定向、多个选中段预先固定、根/partition 替换、
最终文件符号链接、发布与失败清理保留外部文件、读写共享预算、EOF 后预留、
重复 close、部分打开失败归还、超大 selection 整体拒绝及配置校验。
未使用测试替身冒充 OS 行为，没有取消已有测试或增加告警忽略。

首次修复后的聚合测试曾因 delete guard 的异常类型变化失败；保持原 guard 契约后
重跑 45 项全部通过。正式 Local `-am -Pstatic-analysis verify` 的 431 项测试全部通过。
随后完整 Catalog `-am` 静态验证退出 0，选定的 45 项测试全部通过。
82 份静态报告中没有 SpotBugs finding；保留分析器 missing-class 信息与四项既有
Checkstyle warning，不把门禁通过写成所有静态诊断为零。

最终全 reactor clean install 选定 91 项全部通过，包括真实双 Worker 查询、八项
SQL 回归及三项启动 smoke；最新去重测试记录共 482 项，全部通过、没有跳过。
八项仓库检查通过，Local 制品九个 class 与编译输出逐字节一致。
发行审计覆盖 110 个 JAR、110 个唯一模块名及 853 个编译包，未发现所检查的旧引用、
split package 或服务注册违规。三种实际角色 classpath 核验通过，均未误装配 Local；
三份现有 Native 二进制哈希一致。消费者验证和制品审计的逐项结果见以下机器可读证据。
选择执行不等于无筛选的全量生产回归；没有新增性能比较、ASan 或跨平台结论。

## 证据

- [逐类职责、调用与资源处置](spool-directory-fqn-dispositions.json)
- [源码变更基线](spool-directory-input-evidence.json)
- [命令与验证汇总](spool-directory-validation-summary.json)
- [逐项测试与首次失败](spool-directory-tests.tsv)
- [最新测试结果](spool-directory-latest-selected-tests.tsv)
- [所选 owner 测试覆盖](spool-directory-owner-coverage.json)
- [静态报告](spool-directory-static-evidence.json)
- [平台目录能力实际探测](spool-directory-platform-evidence.json)
- [最终仓库检查](spool-directory-final-checks.json)
- [JAR 审计](spool-directory-artifacts.json)
- [class 字节一致性](spool-directory-class-parity.json)
- [实际角色 classpath](spool-directory-runtime-classpath.json)
- [Native 制品核验](spool-directory-native-artifacts.json)
- [执行输入变更检查](spool-directory-execution-inputs.json)
- [逐文件处置](spool-directory-file-dispositions.tsv)
