# Storage Profile 最长前缀选择

本批基于当前工作区 8,566 个文本文件快照，修改一个契约说明、真实 PostgreSQL registry
和测试专用 Memory registry。先复现两个缺陷：PostgreSQL 在遇到较短前缀重复时提前拒绝，
漏掉随后出现的唯一更长匹配；测试 registry 对最长前缀重复任意取一个结果。

两个实现现在完整遍历候选：更长匹配替换候选并清除歧义；同长度匹配记录歧义；最后仅拒绝
全局最长前缀重复。无匹配仍为空。测试覆盖名字排序、插入顺序、无关候选和最终重复。
API 明确这一规则和使用时授权责任。SQL、历史 revision、事务和资源所有权不变。
Testing Kit 直接声明 File API 依赖；未增加生产 Memory Provider。

当前源码搜索只找到 resolveByUrl 的声明和测试调用，没有生产调用者。本批验证该契约和
真实后端行为，不宣称自动 URL 路由已接入 SQL 执行链。

修改前 PostgreSQL 9 项中 1 FAIL，fixture 3 项中 1 FAIL；修复后近端 14 PASS，
全 reactor package 中选择的相关消费者与架构测试 58 PASS，0 FAIL/ERROR/SKIP。
8 项静态检查通过；826 个源码包零循环。106 个 JAR、106 个唯一模块名、832 个编译包
审核零违规；Coordinator/Worker/Standalone classpath 审核通过。Native 三处 SHA 相同，
本批未修改 Native/Bridge/Protocol/schema/Shuffle/client，不宣称新 Native 或性能验收。

未重跑全部生产回归；已有 InMemoryShuffleDataInventory 未接线门禁问题继续开放，
本批选择范围通过不覆盖该问题。下一批处理 File 事务失败清理。

证据：[类处置](file-profile-selection-fqn-dispositions.tsv)、
[调用搜索](file-profile-selection-callers.tsv)、[语义](file-profile-selection-semantic-evidence.json)、
[测试汇总](file-profile-selection-validation-summary.json)、[逐测试](file-profile-selection-tests.tsv)、
[包图](file-profile-selection-package-graph.txt)、[JAR](file-profile-selection-artifacts.json)、
[角色 classpath](file-profile-selection-runtime-classpath.json)、
[静态检查](file-profile-selection-static-evidence.json)、[输入](file-profile-selection-input-evidence.json)、
[Native 哈希](file-profile-selection-native-artifacts.json)、[文件处置](file-profile-selection-file-dispositions.tsv)。
