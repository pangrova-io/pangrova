# AI Native 通用函数契约单一真源实施规格

状态：待主线完整审阅后授权生产实现；本文件不是已实现能力声明。
源码复证日期：2026-10-02。范围：数据库内核函数与分布式准入，不含 Agent。

## 1. 已复证事实与完成边界

最近同一 15 类 Java 专项运行 186 项：0 assertion failure、3 ROUND admission
errors、0 skipped，15.554s。因此整体结果仍是 FAIL。普通 ARRAY/DECIMAL/BIGINT、
scalar/CASE、SUBSTRING、vector signature 与 ANN cast 两项回归已经通过；三个
ROUND SQL→生产 admission→proto 用例原样保留，不能调整默认名单或测试绕过校验。
这些结果不是 Native 最终值、跨进程 Worker 或大规模性能验收。

真实断点及证据：

| 位置 | 当前行为 | 必须消除的缺陷 |
|---|---|---|
| `NativeCommandDispatcher.cpp::handleGetCapabilities` | 返回统一 v2 inventory，填 rel/expr，不填 functions | 实际函数注册没有进入控制面 |
| `NativeCapabilityAdvertisement.load` | `loadInventory()`、配置模型补充、原字节注册 | 此链已接通，不能误述为整个握手缺失 |
| `PlannerWiring.buildSqlCompiler` → `SqlCompiler` 三参构造 | 静态 `NativeCapabilityRegistry.defaults()` | 编译准入与真实 Worker 库存脱离 |
| `FunctionMapper` | 手写 SQL/URN 映射、`any1` placeholder variant | 不构成真实 overload/返回类型证据 |
| `VeloxPlanConverter::convert` | 去掉函数签名后缀，按短名字映射 | 忽略 URN、独立 alias 表可漂移 |
| `SubstraitPlanValidator` | scalar/window 仅查名字 | 未验证输入/输出、重载、kind、约束 |
| `collectAggregateExpressions` | 仅 grouping 与 measure filter | measure 函数及参数表达式漏检 |
| `LeafStageDispatcher`、`DurableStageRuntime.prepare` | 无 typed operator bindings 时按资源选 Worker | 普通 SQL 函数能力不参与调度/重试 |

`NativeCapabilityLoader.loadCapabilities(CapabilityConsumer)` 与 registry.update
没有生产 caller；生产实际使用 `loadInventory()`。后续删除死入口，不新增新的
并行 registry。Native 已有 `checkedScalarCall` 精确绑定及输出类型检查必须保留。

## 2. 架构不变量

1. Coordinator JVM 只处理签名、类型、权限、库存、调度和终态元数据；不执行行、
   向量距离、融合、聚合或 provider 推理。类型公式计算是控制面元数据运算。
2. 所有具体物理类型复用 `substrait.Type`，逻辑 VECTOR/FILE 等复用统一 v2
   `LogicalType`；不创建 vector task proto、第二 primitive enum 或第二类型映射表。
3. 实际可用性只来自完成注册后的 Native 库存。SQL operator 声明、编译成功、
   generated catalog 收录均不等于某 Worker 当下可执行。
4. 一个规范定义产生 SQL/URN 映射、参数转换政策、Native target/decomposition、
   签名计算和语义身份；生成物不能手改。真实上游注册是 overload/公式权威。
5. 无 silent downgrade、运行时 implicit coercion、unknown-name passthrough、
   静态 defaults 冒充库存或缺失 provider/Secret 的匿名 bypass。
6. 一个 canonical descriptor/capability epoch。迁移两端同时生成、部署、验证；
   旧 epoch 显式拒绝，不维持双协议或兼容执行路径。

## 3. 源、所有权和生成链

### 3.1 三类信息不能混为一个手填名单

**真实实现签名源**：固定 Native 依赖的实际注册对象。

- scalar：`getFunctionSignatures()`；返回 `FunctionSignature.variables()`、
  `argumentTypes()`、`returnType()`、`constantArguments()`、`variableArity()`。
- aggregate：`getAggregateFunctionSignatures()`，额外导出
  `AggregateFunctionSignature.intermediateType()` 及 metadata。
- window：`windowFunctions()` / `getWindowFunctionSignatures()`，导出
  process mode、isAggregate 和 actual signature。
- scalar metadata：实际 `isDeterministic()`、`isDefaultNullBehavior()`。
  后者仅证明 NULL 输入处理，不能推断非 NULL 输入永不产 NULL。
- configured governed callback：现有 FunctionFactory→NativeFunctionBridge→
  `NativeFunctionDescriptor` 完成注册后，再从 Native 实际 registry 复证。
  `FunctionMetadata` 的字符串不能覆盖精确库存或模型逻辑约束。

**规范语义源**：建议由 `pangrova-protocol` 拥有单个非运行配置资源
`src/main/resources/function-contracts/sql-bindings.yaml`。它替代现有独立
SQL alias/Native alias/nullability 手写表，而不是再添加一份支持名单。条目只含：

- SQL operator kind/names、规范 extension URN/name、scalar/aggregate/window kind；
- 实际 Native target，或有类型约束的 decomposition DAG；
- SQL 层允许的显式 operand conversion 政策及错误/NULL/session 语义；
- public arguments、governed binding argument layout 和 model output 约束；
- 无法从上游签名反射得到的语义前置条件及其实现位置/验证 oracle。

普通 DECIMAL 返回公式不得写入该 YAML；它必须从实际注册 constraint 导出。
文件不拥有 provider 配置、Secret、租户授权或运行可用性。
Native signature的`bigint/integer/varchar/array/row`等名称不是新的wire类型。
同资源只定义到canonicalSubstrait descriptor的技术名称投影，生成Native类型
export/import与Planner TypeMapper所需投影；迁移原分散映射，不能另添一张只供
函数export使用的类型表。具体DECIMAL/temporal limits从actualNative类型实现
导出并复证，不从未消费的YAML默认值推断。无法双向保真的类型显式阻断。

**唯一规范产物**：`FunctionCatalogManifest`，确定排序、确定编码、SHA-256。
生成器把真实注册和语义资源合并，拒绝 dangling target、重复 URN/kind/overload、
未知参数或无法表示的公式。所有原有已声明 operator 均须出现在生成 coverage
账本：已注册/明确未实现及缺失原因；不能静默删条目以维持构建通过。

### 3.2 构建顺序与责任

- IDL 与 schema：`pangrova-protocol/src/main/proto/pangrova/protocol/v2/`。
  Java/C++ protoc 输入仍只有该目录及同一 pinned Substrait imports。
- Native exporter：`pangrova-worker-native` 内部构建工具，调用生产纯注册过程，
  不建立 JVM、数据库、provider 请求或 fake function。先导出 builtin actual
  registration；callback 的通用签名定义随同 catalog，运行注册后必须复证。
- catalog generator：`pangrova-protocol/tools/function-contracts/` 的构建工具；
  不引入运行期 YAML/ANTLR dependency 到 common/SPI，不依赖 Worker Java。
- CMake 先编译 registration/export 核心目标，生成 manifest，再生成 canonical
  Native dispatch metadata，最后链接实际 shared library；禁止 shared library
  先依赖尚未产生的 manifest、exporter 又反向依赖该 library 的构建环。
- Native package 同时包含 library、manifest、descriptor hash、依赖 revision、
  编译 feature profile、内容 hash。manifest 不能从另一构建目录或机器拷入。
- Java 的受控资源 staging 读取同一 Native Release 产物，缺失则构建失败；
  不用一份手写 JSON 替代。完整流水线 Native build 在 Java full compile 前。
- runtime export 在 builtin 与 callback 注册均完成后进行，冻结 availability
  snapshot。启用的函数签名必须匹配 manifest；未启用的插件不 advertise。

当前复证 Native 依赖 revision：`3da0390118fa4b0addbfbdeb03f438ec1b1237a6`。
生成器必须把 revision 与 feature profile 放入 provenance；它们不是语义等价性
判据。可兼容 binary upgrade 用 per-contract semantic hash，不强绑整个 build id。

## 4. 参数化签名与公式：具体可实现方案

### 4.1 SDK 与 Native 的已核验边界

当前 Substrait Java core 0.95.0 的
`io.substrait.type.TypeExpressionEvaluator.evaluateExpression` 仅返回已 concrete
`Type`，其他表达式直接 `UnsupportedOperationException("NYI")`。不能把 SDK
当作现成泛型 binder。其 `ParameterizedType`、`TypeExpression` 有建模 AST，但
BinaryOperation 没有 MODULO；SimpleExtension.loadDefaults 还有现有 ANTLR
classpath 冲突理由。生成 Java variant 应直接建对象，不在 runtime 加载 YAML。

Native `FunctionSignature` 暴露类型/整数/enum variable 与结构化 `TypeSignature`。
整数变量 constraint 是实际注册的计算表达式；不是所有 constraint 都是布尔
predicate。`SignatureBinder` 输入绑定后，按 return/intermediate 的整数变量依赖
计算参数。整数常量约束还可能在输入绑定时要求与实际值相等。

已核验的 `TypeCalculation.yy/.ll` 语法包含 `+ - * / %`、一元负号、min/max、
比较、`== !=`、三元条件及变量；先求 long long 中间值，再写入 int 参数表。
它不等同于 Substrait 文本语法，不能直接替换 `==` 后交给 SDK求值。

### 4.2 同一计算 AST，不手填各函数公式

在统一 v2 capability IDL 定义受限 `TypeParameterExpression`：
integer literal、variable reference、unary negate、binary operation、conditional。
只计算类型参数，不承载用户表达式、SQL、脚本、provider 代码或任意 eval 字符串。
operator 枚举 <=30，包含上述实际 grammar；未知 operator 拒绝。

构建期严格 lexer/parser 从 `SignatureVariable.constraint()` 生成该 AST，检查
完整 token 消费、变量存在、参数类别、依赖 DAG、无循环、节点/深度/文本长度
上限。Native scanner 对未知字符的宽松跳过不能成为规范 parser 行为。

Java 唯一 `FunctionContractBinder` 执行该 AST：

1. 从实际参数结构绑定 type/int/enum variable；重复变量必须一致，不用 LUB 猜。
2. 检查 known/orderable/comparable 的实际注册约束；primitive/复杂类型性质从
   同 manifest type-property 表导出，不另写 Java 类型名单。
3. exact variadic 的最后参数重复规则必须匹配 actual binder；`any` 不等于
   任意其它声明的泛型变量，zero variadic 与固定前缀分别验证。
4. 根据需要求 return/intermediate 参数。所有整数操作 checked signed 64-bit，
   division/remainder 向零截断，参数赋值 checked int32；禁止靠溢出 wrap 得类型。
5. Native 三元计算的 parser 会先计算操作数；差分测试验证 eager 语义。不能
   擅用 Java 短路而接受 Native 会出错的 constraint。注册公式域必须证明不会
   触及 undefined overflow/divide-zero；不满足则拒绝该契约发布并报告原因。
6. concrete DECIMAL 输出继续走严格工厂：1<=p<=38、0<=s<=p。precision 的
   min(38,...) 是 actual 注册公式的一部分；不是 converter 擅自 clamp。

Native 生产执行仍使用自己的 exact SignatureBinder/resolver；导出型 Java binder
是规划/准入用途，两者用同一输入矩阵做 differential oracle。仅源码公式看似
相同不够。默认不允许 `resolveFunctionWithCoercions`。

反射签名不是执行优先级的完整反射：pinned `getFunctionSignatures()` 合并simple
与vector，实际resolver先simple再vector；simple内部还检查physicalTypeMatches
及metadata priority，这些细节不在公开签名API内。禁止把排序后first-match当作
runtime结果。统一binder必须收集exact可绑定候选；结果不一致时fail-closed，
canonical binding policy需以明确selector/SQL casts消除公开入口歧义，并由真实
resolver差分验证。库存可以保留未公开签名，但不能据此自动宣称SQL支持。

### 4.3 参数化类型不复制具体类型 IDL

`FunctionTypeTemplate` 使用：`substrait.Type skeleton`、whole-type variable，
以及 descriptor 校验的 typed substitution slots。slot path 用 protobuf field
number 与 repeated index，必须属于当前 canonical descriptor，并以具体 Type
或整数参数字段结束。不是任意 JSON path，也不得修改任意 protocol 消息。

例：DECIMAL precision `[24,2]`、scale `[24,1]`；LIST component `[27,1]`。
MAP、STRUCT、FUNC、precision timestamp、UserDefined 的 slot 均从同一
`substrait.Type` descriptor 验证，不能再定义 primitive/vector kind enum。
模板尚未实例化时允许参数 placeholder，但不得当作合法 concrete Type使用。
whole-type variable 与参数化嵌套模板递归实例化后，由已有 logical/physical type
validator 完整校验 nullability、variation、精度及复杂结构。

Native注册的`timestamp`不承载canonical precision/timezone/nullability，不能自动
推出这些参数；当前Native converter把precision timestamp与TZ都映到TIMESTAMP，
这仅说明物理表示相同，不证明语义等价。这类参数的输入/输出policy只能在同一
binding declaration声明，保留canonical原类型；未经实际最终值oracle的TZ不发布
为已支持。exporter不得自行补6/9或把TZ默默归普通timestamp。

enum、named row field、homogeneous variadic row、lambda 参数不可忽略：export
必须保留 actual TypeSignature 属性。当前公开 operator 若涉及某项，必须实现
并验证；未公开 Native 函数也在 exporter 完整性账本中标记，不宣称 SQL支持。
无法安全表达到 canonical UserDefined 参数的 enum/opaque type 不发布虚假契约。
`substrait.Type.Struct`本身不承载fieldnames：named-row constraint须作为template
结构约束保留，并从输入NamedStruct/已验证LogicalType获取真实名称，不能按ordinal
猜名。lambda未接通的公开入口必须给具体实现/验收，不能注册ANY掩盖。

### 4.4 Calcite 的推导及 SQL coercion

所有普通 operator checker 和 return inference 使用同一个 binder；TypeMapper
是 Calcite↔concrete Substrait 的唯一边界，补反向能力时复用其现有映射责任，
不在各 operator/new binder 再造 mapping。

- SQL 验证先按唯一 canonical binding policy 选择 permitted overload与显式
  SQL CAST，随后用 actual converted operand types exact bind，得到完整返回类型。
- `PlannerTypeSystem` DECIMAL arithmetic hooks调用同一 binder，移除当前
  DecimalArithmeticTypeDerivation 的手填 multiply/divide/mod 公式；ROUND
  resultType也替换为manifest evaluator。保留现有严格声明范围拒绝和测试oracle。
- 类型工厂显式获得 immutable contract definition view。schema-only factory
  仍使用同 manifest type limits，不要求运行 Worker；production compiler必须
  另持 availability view。不通过可变 static singleton切换不同查询的公式。
- SUBSTRING INTEGER/BIGINT positions、ROUND INTEGER position、vector REAL/DOUBLE
  common overload conversion政策写一次 binding resource，生成operator逻辑；
  converter只保留结果 Rex CAST，不按 literal representation/valuewidth猜类型。
- CASE/ARRAY/CAST 是结构性 SQL 类型规则，不伪装成可执行 scalar函数；其具体
  类型仍通过严格 TypeMapper与同一 canonical type validation。
- `FunctionNullabilityRegistry` 手填 RULES移入同 canonical semantics，删除
  unknown→ALWAYS_NULLABLE“支持”回退。NULL input behavior、may-return-NULL、
  empty aggregate/window、three-valued logic不能只由 physical binder推导。
  未知 null semantics是缺失契约，不能直接声称 NEVER_NULL。
- timezone/session dependence、overflow/divide-zero政策纳入semantic hash；
  Native实现必须差分证明或明确拒绝，不把官方示例公式当当前runtime公式。

## 5. Alias、decomposition、special form

SQL alias（CEIL/CEILING、LENGTH/CHAR_LENGTH）映射到同规范身份；Native alias
（add→plus、substring→substr、modulus→mod）从同resource生成。
Native解析 extension URN anchor及function anchor；unknown URN、错kind、重复
anchor、未知signature后缀拒绝。删除短name→identity默认pass-through。

`is_not_null` 是 `not(is_null(arg))`，`is_not_distinct_from` 是
`not(distinct_from(a,b))`。decomposition 是一个 typed DAG，每个 target须有
真实注册/输出oracle，整个rewrite规则进入semantic hash，不能把不存在的
原始 Native函数 advertised为单个注册函数。

special form不能从`getFunctionSignatures()`自动恢复完整规则。真实API只有
`specialFormRegistry().getSpecialFormNames()` 与 `resolveType(args)`。
因此 COALESCE、AND/OR、IF的 public signature/NULL与evaluation order定义只在
canonicalresource一处；生成实现/validator消费它，并对真实specialform resolver
和最终值做证明。不能宣称黑盒代码已经自动导出。不可表达的specialform作为
当前完整单元的具体阻断，不能补一份宽泛any signature蒙混通过。

`sum0→sum`不得当无条件alias：空集合/空window语义不同。应使用真实
zero-on-empty语义实现或typed decomposition，普通/partial/window均验证。
本单元同时检查已有aggregate AVG converter 强制DOUBLE等输出改写；若actual
signature不相容，按同contract修根因，不在manifest抄下错误输出。

## 6. 统一协议、库存与fragment需求

以下为待实现规格，不是已生成代码。修改所有权仅限canonical v2 IDL。

| 对象 | 必须字段与用途 |
|---|---|
| `FunctionCatalogManifest` | descriptor identity、schema version、semantic binding resource hash、provenance、排序后的完整 definitions |
| `SubstraitFunctionDefinition` | URN/name/kind、overload variables/arguments/output/intermediate、AST、variadic/constant规则、NULL/session/error语义、rewrite、governed布局、per-contract SHA |
| `FunctionAvailability` | exact definition identity+SHA、当前真实implementation identity、支持的aggregate phases/window模式 |
| `FunctionRequirement` | definition identity+SHA、具体LogicalType arguments/output/intermediate、phase、constant证据、invocation/path身份、所需options |

新增 `CapabilityInventory.function_availability`（建议新field11）及完整catalog
definition载荷/身份（建议field12，canonical size bounds）；删除/保留reserved
旧functions字段9，不能继续让名字驱动准入。manifest可按完整内容hash去重，
inventory必须携带实际definition内容或Coordinator已验证的同hash产物；未知hash
不凭name补全。不是每个task重复传整个catalog。

`TaskRequest.required_functions`建议field18；`FunctionRequirement`复用具体
`LogicalType/substrait.Type`，不重定义argument类型。该集合与最终plan checksum
绑定，Worker须重算相等，拒绝遗漏、多余、错类型、错URN/hash及伪造constant证据。
常量证明引用真实plan表达式路径及canonical literal digest，不把user声称的
“constant=true”当事实。NULL literal与row字段存在不同必须区分。

protocol层通用 `FunctionRequirementCollector` 遍历 generated Substrait proto，
不依赖Calcite/Worker/Coordinator。scalar、aggregate measure（含args、filter、
ordering）、WindowRel/RexOver序列化、sort comparison、所有嵌套表达式均覆盖。
交换输入schema与aggregate intermediate类型从实际fragment解析，不用whole-query
输入schema。unknown expression/rel仅当已明确不承载表达式才可跳过，否则拒绝。

所有关系rewrite、generated value overlay、retrieval/rerank plan binding结束后，
对**最终**plan重算requirements。`SerializedPlanFragment`或
`BoundExecutionPlan`不能保留rewrite之前的旧集合。durable执行持久化最终bytes、
requirements及checksum，恢复保持原契约，不按当前SQL重编译后偷偷替换。
collector类型上下文遵守各Rel作用域：Filter condition在input row上解析且必须
BOOLEAN，Rel.common.emit在operator完成后应用；Project默认input+expressions
append，再应用emit。不能先用输出schema解析condition或把表达式数当输入arity。
这些边界与并行Native emit/Filter修复保持同一规范，不另写Java例外规则。

## 7. 聚合、窗口、AI governed args

### 7.1 Aggregate

导出 raw inputs、intermediate、final；phase复用Substrait四种阶段。INITIAL→
INTERMEDIATE输出actual中间类型，INTERMEDIATE→RESULT输入actual中间状态，
不能每阶段都按SQL AVG的最终DOUBLE或DECIMAL校验。companion registration未
启用时不能宣称partial/merge可用。DISTINCT、FILTER、ordering、state encoding
与semantic contract一致；state格式/hash必须在跨Worker exchange两端兼容。

NULL/empty输入 oracle包括COUNT0、SUMNULL、SUM00、AVGNULL，以及partial→
merge→final与single阶段等价；必要emptyframe不能归为永不为空。

### 7.2 Window

WINDOW kind与aggregate-as-window metadata分别导出，不凭同名字推断可执行。
对参数、offset/default、partition/order、ROWS/RANGE、NULLS FIRST/LAST、frame
边界逐层验收；保留已完成Window共享literal/root类型修复。未支持的frame
提前拒绝，不能自动换ROWS或丢排序/null方向。

### 7.3 AI

必须区分三层：SQL public args → planner附加invocation_id → Worker/Native
通过已有 exact OperatorBinding补充hidden model/policy/budget/reuse args。
当前 `AiInvocationBinder.bind` 和 `appendGovernedAiArguments` 都是实际caller。
统一definition给出这些参数的唯一order/type/layout hash，生成两端代码。

public AI availability = 通用invoke capability + exact model capability contract
+ 已成功注册callback physical signature；不能只拿Native隐藏参数签名校验SQL
public arity，也不能把callback可用当model/provider可用。AI_EMBED输出逻辑VECTOR
dimension与模型contract绑定，物理ARRAY REAL不取代dimension约束。

compiler对AI的deferred admission改为exact converter-produced invocation身份、
public typed contract及后续modelbinding，不按function name泛化豁免。最终Native
补齐hidden args后仍走exactbinding；调用context、权限、Secret、deadline、并发、
bytes及请求执行次数等内核资源上界
现有failclosed边界不能因统一catalog被删除。资源/授权绑定不属于offlinemanifest。
用户明确排除Usage Ledger与LLM usage statistics，统一函数单元不保留/新增计费或
LLM消耗统计要求。token参数若目前参与实际执行准入，必须先追完grant、provider、
JNI及校验caller，明确与统计字段区分后设计等效内核请求上界替代；不能直接删掉
某处检查以降低安全性。purpose与hiddenlayout由主线另行完成详细caller设计，
此时只让通用contract支持normalized可选常量参数，不并行改这些生产caller。

Embedding purpose是显式规范参数：QUERY/DOCUMENT，允许的enum值、可选SQL参数、
唯一默认值、normalized public argument顺序和physicalrepresentation由同definition
声明。SQL省略参数仅在验证时归一化一次，随后必须在canonicalplan中可观察；
generated SYNC/artifact使用DOCUMENT，search使用QUERY。用途不得由scalar/batch
模式、batch长度或singleton拆分推断，provider也不能自行追加字符串默认值。
logicalenum到实际Native value参数的转换作为同contract生成规则，不直接把未支持
的Substrait enum argument交给仅接收value的converter。purpose及实际provider前缀
绑定进入invocation/reuse/model执行contract，单条与batch必须同义。prefix由精确
model配置拥有，不写进通用offline catalog或作为Secret外泄。

## 8. 冻结视图、生产注入与eligible cohort

现有路径保留：Native GET_CAPABILITIES→loadInventory→
NativeCapabilityAdvertisement→WorkerComponentRefs→WorkerHeartbeatBootstrap→
WorkerRegistrationRequest→WorkerResource.decode→WorkerInfo→WorkerManager/
WorkerSnapshot→真实PG库存；新增typed fields沿此链lossless保留。

Coordinator query入口冻结 immutable definition view、protocol epoch与可见
Worker inventory generation，注入PlannerWiring→SqlCompiler→PangrovaPlanner/
strict factory→converter/validator。query cache/plan digest包含semantic contract
hash，不仅SQL text。不能共享可变registry.update导致一次validate读到两个版本。

先生成各fragment typedrequirements，再选择：

`eligible(fragment) = active ∩ tenant-authorized ∩ resource-admitted ∩ all-required-contracts`

这是候选cohort内的全需求交集，不是所有Worker函数名单的“最弱全局交集”。
不同stage可选不同compatiblecohort。新增函数可由支持它的Worker执行；不能
把一台旧Worker不支持当全cluster不支持，也不能把不同Worker各有一半函数
的union当存在一台能执行完整fragment。

无eligible worker显式NO_CAPABLE_WORKER，未dispatch前失败，不转默认执行。
选定Worker后直到admission发生restart/下线，Worker重算实际requirement拒绝；
合法重试只选择满足冻结signature语义的其它Worker。允许build id改变但per-
contract hash一致；hash改变需要明确replan/new epoch，不复用durable旧winner。

改齐 `DefaultWorkerSelector.selectForBindings` 的普通SQL路径、TaskScheduler所有
select入口、LeafStageDispatcher、intermediate/root dispatch、
DurableStageRuntime.prepare、资源准入和retry/failover。resource/tenant eligibility
与capability筛选必须使用同一次snapshot。cache affinity只能在capable集合内打分。
不在持PG租约/registry锁时做I/O；immutable view发布后读取无锁。
这里的signature SHA是规范内容身份/一致性hash，不等于公钥签名或认证。
Worker注册仍须使用现有可信传输、身份验证与租户边界；hash不能替代这些权限。

Worker HTTP `CanonicalTaskRequests.validateForAdmission`验证canonicalfields，
与本机冻结Nativeinventory exactmatch，再排队。Native TaskRequestAdmission重新
验证epoch/plan/checksum/requirements，并在actualcall处保持exactsignature检查。
不能只校验Worker启动时“曾经具备”或由网络请求指定可信inventory。

## 9. 离线确定编译与发布迁移

显式 `offline compilation`加载Nativepackage的同一manifest，得到typed plan与
requirements，但没有execution availability、model authority或resource grant。
该输出不是已admitted任务；production compiler必须明确bound runtime view。
测试的默认SqlCompiler迁移为明确offline fixture，ROUND原测试从真实generated
manifest准入成功后继续检查proto，不能注入手写round函数set。

manifest有protocolhash、sourcehash、version和尺寸限制；缺失/损坏/不匹配
一律失败。不使用System.loadLibrary、local path或Java fallback猜manifest。
跨OS生成结果比较semanticdefinition hash，不把build timestamp/绝对路径写进
规范编码；feature差异只改变actualavailability/provenance，不混淆signed语义。

发布采用一个新canonicalrollout epoch，原field编号不复用、删除字段reserved；
两端descriptor hash重新生成。PG已存旧inventory不能通过新decode成为active。
未结束durablejob旧epoch明确阻断并执行授权rebuild/replan，不透明migration。
无需新函数库存数据库表：复用已有完整库存列及durableplan存储，但验证其容量、
序列化与恢复不会截断新增内容。运行库4MiB编码限制须以实际产物测量；不拍脑袋
增大限制或截去签名，必要catalog字典去重并保留完整hash引用闭合。

## 10. 原子实施清单

| 所有者 | 现有入口/迁移点 | 同一单元必须删除或替换 |
|---|---|---|
| Protocol | capability.proto/task.proto、CanonicalProtocolManifestV2、CapabilityInventoryCodec、CanonicalTaskRequests、BoundExecutionPlan | functions name authority；字段旧编号保留reserved |
| Native | registration/export、GET_CAPABILITIES、VeloxPlanConverter anchor解析、TaskRequestAdmission | 独立alias map、unknown name passthrough；保留checkedScalarCall |
| Planner | FunctionMapper、TypeMapper、PlannerTypeFactory/System、DecimalArithmeticTypeDerivation、operator declarations、FunctionNullabilityRegistry、SubstraitPlanValidator、SqlCompiler | placeholder any1、手填公式/NULL表/default名单、漏掉aggregatemeasure的验证 |
| Worker | NativeCapabilityLoader、NativeCapabilityAdvertisement、NativeFunctionBridge/Descriptor、启动顺序、HTTPadmission | 死CapabilityConsumer入口；actualinventory缺失不能ready |
| Coordinator | PlannerWiring/query compiler scope、WorkerCapabilityMatcher、WorkerSelector、TaskScheduler、DAGdispatch/durableprepare | 普通SQL不检查函数能力的所有select路径 |
| Packaging | Native CMake/packaging、native-bundle资源、Java资源staging、CI epoch gates | 混用旧native/新manifest或手写manifest路径 |

生产修改前再验证具体caller及exports/.pangrova/modules.yaml；没有任何implementation
module反向进入protocol/SPI。命名无框架泄漏；Java500/50/8阈值，分binder、
template、formula、collector、frozen-view职责，不建立Godregistry。

顺序：IDL/spec与generator golden→实际Nativeexport与差分oracle→同manifest
planner推导→inventorylosslessadmission→所有调度/retry→packaging与E2E。
这是开发子步骤，不允许中间版本作为production完成态。每次freeze记录已变文件
及前序全仓验收失效点。其他agent当前拥有serializer/deserializer迁移，不重叠编辑。

## 11. 验收矩阵与判定

| 层 | 可执行验收（新增与保留原断言） |
|---|---|
| Generator | 同输入重复生成bytes/hash相同；源码或constraint变化改变semantichash；所有旧mapped operators覆盖账本；不支持语法/target/enum/slot/cycle明确失败；不忽略token |
| Formula | 从实际constraints生成；四则/modulo/negative/比较/ternary/overflow/div-zero；p1,p2=1..38与s<=p完整DECIMAL参数域分片差分；invalidscale/rescale原拒绝保留 |
| Native | actualregistryexport与manifest exactmatch；scalar/aggregate/window/specialforms；错误URN/kind/input/output/unknownsignature拒绝；late mutation拒绝 |
| Planner | 原15类186项不缩scope；ROUND3项通过真实manifestclosure；SUBSTRINGmixed整数；普通/nestedARRAYtypedNULL、BIGINT、DEC38、case、comparisoncasts、全部distance与ANNcastoracle |
| Aggregate/Window | measureargs/filter/order真实SQL→proto；AVGDecimal实际输入/输出与phase-state；SUM0empty集合/emptyframe；partialmergefinal等价；offset/default/frame/sort负向 |
| Protocol | inventory/cataloghash、duplicate/unknown/malformedfields、typeparameterbounds、size/cycle/depth、reqset与planchecksum一致性property；Java/C++descriptor与hash对称 |
| Worker | actualpackagedJNI→fullinventory→注册payload保真；先callbackfinalize再库存；缺失manifest/provider绑定阻止ready；任务少报/伪造签名拒绝 |
| Coordinator | 实际registeredworkers驱动compiler注入；同名错signature、每Worker只有半套函数、资源/tenant不合格、rollingupgrade、新worker、断线/retry/durable恢复均failclosed且不误拒compatiblecohort |
| E2E | realCoordinator SQL→多个独立Worker进程→打包JNI→Native→Arrow→client最终值/schema；Ollama实际AI含精确模型、权限、deadline/concurrency/bytes/请求执行上界；restart后冻结需求兼容重试；不要求LLMusage/计费统计 |
| 性能 | 实测inventorybytes、启动export CPU、编译/collector CPU与alloc、snapshot发布成本、调度P99及每taskhash成本；warmcache只缓存immutabledefinition，不隐藏库存变化；增加Worker时不线性扫描整个catalog每task |

引用最近现有test入口：NativeCapabilityRegistryTest、SubstraitPlanValidatorTest、
NativeAdmissionTest、CanonicalExpressionSqlProtocolTest、VectorSearchRewriterCastTest、
CapabilityInventoryCodecTest、NativeCapabilityLoaderTest、
WorkerBootstrapCapabilityAdvertisementTest、WorkerCapabilityMatcherTest、
TypedCapabilityPlanBindingTest、VeloxPlanConverterTest。新公共对象每个方法、错误
分支有最近test；pure/component用例标记清楚，不计作跨进程E2E。

先最近同命令复测，再执行build-pipeline完整Step0–8、ASan、staticanalysis、
descriptor-diff、dead-stubs、diffcoverage、E2Etruthfulness、真实productionimage。
所有结果分别记录PASS/FAIL/SKIPPED/BLOCKED；环境或尚未实现不记PASS。
生产级完成还要求真实目标cluster的SLO/规模实测；规范无法保证“未来任何问题
都不会发生”，只能把约束、测量和upgrade/失败语义变成可持续验证门槛。

## 12. 一手资料与适用边界

- [Substrait严格类型系统](https://substrait.io/types/type_system/)：类型边界无隐式coercion；具体CAST必须在plan表达。
- [Substrait参数化函数](https://substrait.io/expressions/scalar_functions/)：签名包含常量/variadic/nullability/参数与返回推导；网页示例不是当前Native公式。
- [Substrait聚合阶段](https://substrait.io/expressions/aggregate_functions/)：intermediate与phase属于聚合binding。
- [Substrait窗口binding](https://substrait.io/expressions/window_functions/)：frame/order属于窗口契约；实际行为以本项目pinnedIDL及Native支持复证。
- [Native函数注册说明](https://facebookincubator.github.io/velox/develop/scalar-functions.html)：注册签名与执行实现分工；最终实现采用上述pinnedrevisionAPI，不追随未固定main新接口。

主线已完整审阅并批准实现，先实施IDL/实际export/严格formula/template/generator
及真实differentialoracle，再贯通Planner/cohort/packaging。实现期间不由分线运行
Maven/Cargo/CMake或修改target；冻结后由主线统一执行编译验证。所有接口及语义
仍须实测，不把相同per-contracthash当身份认证或已证明的行为兼容。

## 13. 当前原子单元实现检查点（非生产完成声明）

唯一`pangrova-protocol/src/main/resources/function-contracts/sql-bindings.json`
已迁入93项scalar/aggregate/window SQL名称与operator映射（含真实ARRAY/MAP cardinality），真实Native别名、
typed rewrite、runtime callback注册依赖、结构lowering和optional enum规范化均在
同一声明。当前`FunctionMapper`和Native lowering仍待逐步迁移；不能因有声明就
发布不存在的实现。与实际877项注册identity（470scalar/320aggregate/87window）
及9specialforms比较，仍有8项非callback注册缺口：ARRAY_CONTAINS_ALL、5项Spatial、
aggregate/window SUM0。CLI未注册AI/FILE callback是显式runtime依赖，不伪造callback。

`BuiltInCapabilityCatalog.unnestPlanRef()`读取同一声明的TABLE结构语义，Native
`StructuralContractIdentities.h`由CMake从该资源生成，未手填另一套生产hash。
UNNEST使用ARRAY→1/MAP→2列、zip-longest NULL padding、BIGINT 1-based ordinality、
显式with_ordinality/outer options及typed output authority；SQL ARRAY<ROW> flatten、
INTEGER ordinality和right Project保留由真实Planner显式Project/CAST实现，不由
Native猜类型。结构hash仅表示兼容内容，不表示鉴权或已经完成值语义验证。

`FunctionBindingDeclarations.normalizedArguments`提供AI_EMBED第三public参数的唯一
RAW默认与RAW/QUERY/DOCUMENT合法域。`NormalizedAiEmbeddingSqlFunction`验证常量目的；
`FunctionArgumentNormalizer`在追加invocationId前用已有LiteralConverter显式生成
purpose，允许无损VARCHAR常量CAST并拒绝截断、NULL、非法域及row-dependent目的。
`AiInvocationBinder`的独立AI名称集合已删除，governed membership读取同一声明。
旧SqlCompilerTest保留调用身份断言并新增purpose，未以单测代替provider或E2E。

`NativeCapabilityLoader`已删除无真实caller的旧字符串CapabilityConsumer/loadCapabilities
接口，ParsedCapabilities保留typed FunctionAvailability与FunctionCatalogManifest；
Worker广告本就携带完整Inventory。严格manifest验证、实际Native发布、Planner精确
binder、eligible cohort、task需求与打包仍未闭环，不能把typed字段可编译视为准入完成。

Native类型转换已抽取为`CanonicalPhysicalTypes`唯一forward authority，执行type/
literal converter委托，完整NamedStruct DFS缺失/多余拒绝保留。接下来生成物理桥与
模板仍须调用这份authority，不复制类型映射；现有timestamp/TZ物理折叠不能作为
逻辑precision/timezone已保真的证据。

已取得的证据：actual exporter首次编译PASS、actual AST/descriptor导出PASS、纯工具
38tests PASS（0.035s，仅工具单元）；主线callback protocol最近11/11 PASS，Worker
production compile 16reactor PASS（11.666s），仅确认15个public callback同源metadata可编译。
目的SQL最近仍有合法VALUES被现ANTLR前端拒绝；负向回归必须先通过同形合法RAW查询，
再检查精确PLANNER_VALIDATION_FAILURE及purpose原因，不得把无关parser错误计为通过。之后新增
Java目的规范化及Native authority/生成依赖使旧全仓验证失效，最近验证由主线统一
执行并记录真实结果。完整DECIMAL域actual resolver差分、最终值oracle、ROUND原3项
准入错误整改、多WorkerE2E、全Step0–8与生产SLO仍待执行，不标记PASS。

## 14. 内核资源与请求限制原子迁移（已批准方向，尚未贯通验证）

附件排除全部LLM usage statistics。当前task/operator token累计预算及provider usage
退款是旧耦合，不保留AiUsage/TokenUsage作为核算依据。输入与生成token上限可作为
单次model request限制，不是累计账、计费统计或任务资源。字节必须在真实HTTP
body编码（包含prefix、JSON、media序列化）后计量；response按实际读取body流有界
计量，不能以原始text UTF-8长度冒充wire body大小。

唯一v2迁移已落源码字段（尚未全链路验证）：

- `ModelBinding.request_limits = 6` 引用 `ModelRequestLimits`，字段1–4分别
  `max_input_tokens/max_output_tokens/max_request_bytes/max_response_bytes`。
- `OperatorResourceRequirement` reserve旧token字段5/6与含义混用的invocations字段10；
  新字段11/12/13为 `provider_attempts/provider_request_bytes/provider_response_bytes`，
  逻辑 `provider_items = 14`。
- `StageResourceGrant` reserve旧token字段13/14与invocations字段17；新字段19/20/21为
  相同三维，逻辑 `provider_items = 22`。
- 旧`provider_invocations`的历史caller混用逻辑row与物理request，不允许只改注释
  静默换义。统一IDL、Java/Native/config/JNI全部改为明确items与attempts，并通过
  canonical descriptor/rollout检查拒绝旧wire契约，不维护旧字段兼容读写。
- `CapabilityLimits` reserve旧invocations字段10，字段11–14分别为max_request_bytes、
  max_response_bytes、max_provider_attempts_per_task、max_provider_items_per_task；由真实provider
  serving配置声明，不通过token×任意字节系数推算。Rerank必须持有同一完整limits，
  batch ceil(rows/batch)仅最少request数量，不能作为有retry时的attempt上界。

稳定Java方向：SPI `AiRequestConstraints(maxInputTokens,maxOutputTokens,maxRequestBytes,
maxResponseBytes)`，Worker `BoundAiInvocation`保持精确身份/模型/幂等/结果维度，
资源分为concurrency、maxProviderItems、maxProviderAttempts、maxProviderRequestBytes、
maxProviderResponseBytes、maxMediaBytes及单次request constraints。Worker task context
只携带task grant，不混入model request token限制。

JNI已在Java/Native源码替换位置参数ABI为 `evaluateBatch(String,Object[][],byte[]) -> Object[]`，
精确descriptor `(Ljava/lang/String;[[Ljava/lang/Object;[B)[Ljava/lang/Object;`。
唯一`task.proto`中的通用 `ExecutionCallbackContext`复用ProtocolVersion、SecurityContext、
TaskAttemptId、StageResourceGrant、deadline和TraceContext；Native从可信task构建，
Worker decode后消费唯一`CanonicalTaskRequests.validateCallback`验证版本/完整grant/
非负可表示域，再建立task context。`callbackFor(TaskRequest)`先验证完整任务后才做
相同canonical authority projection，不构造临时残缺TaskRequest。不是新AI
专属feature protocol；Java/native不得继续维护十多个位置参数维度映射。

sole binding声明中的governedLayouts现明确AI_MODEL 26个隐藏VARCHAR、AI_RERANK
17个完整internal callback VARCHAR，public AI_EMBED三个参数中的purpose与后追加
invocation id不混淆。count/signature/ordinal及binding protobuf path由同一声明生成，
Native反射编码与Worker解码消费；reuse threshold用精确浮点往返编码，
禁止`std::to_string(double)`六位小数损失。未知字段/路径/type/encoding均拒绝。

资源账户以tenant与完整TaskAttemptId为身份，包括plan/control epoch、retry、fences和
execution owner。真实Task admission注册，完成后移除，晚到callback只查找并拒绝，
不得lazy重建账户。Native每task仅序列化一次callback authority，JNI每batch复制byte[]，
不按row重序列化。物理attempt/encoded request body/response body只由执行HTTP的Worker
transport核算：每次send独立持有task/operator/model并发permit直至响应结束或失败。
Native报告provider四维和provider media为0，Worker按同一完整attempt合并真实观察，
不能根据row数或预算推测usage，不能双重计数。

动态ARRAY/ROW已改用官方Nested.List/Nested.Struct，child按声明类型规范化并写显式CAST；
普通DECIMAL数组仍保留精度/scale/typedNULL。Native通过实际array_constructor/
row_constructor exact binding，不造私有类型映射。SDK0.95不提供NestedMap，公开MAP
constructor后续必须在唯一binding declaration中重写为真实注册map(array(K),array(V))，
保留duplicate/null key失败语义，并用真实Native Task值oracle验收；typedMAP读取或
typedNULL UNNEST不能冒充公开MAP构造闭环。新增Native动态ROW数组值oracle尚未运行。

验收必须涵盖编码后request body超过上限、chunked response超过单次及task累计上限、
失败尝试仍计数和保留已发送/已读字节、retry再预留attempt、deadline/concurrency、
同任务多operator相加溢出/超额、items与attempts不同、混合旧新descriptor拒绝，以及
真实HTTP+打包JNI调用链。无provider usage的执行不能变为无限资源或静默放宽准入。

## 15. 窗口与真实注册绑定检查点（源码已落，Native 尚未验证）

`RegisteredFunctionBinding` 是 scalar/aggregate/window 的同一实际注册绑定入口。
scalar 调用实际 resolver，不搜索“恰好返回声明类型”的另一重载；aggregate/window
保留注册向量的 first-match 顺序，验证 exact output、constant arguments 及真实
intermediate type。Java 不允许通过修改 defaults 名单把 ROUND 准入错误变成通过。
Exporter 的 schemaVersion 已提升为 3，并显式区分 `ORDERED_FIRST_MATCH` 与
`SCALAR_RUNTIME_PRIORITY`。Aggregate/window 的重载顺序和重复项均保留；scalar
canonical 排序只是库存编码，不代表 SimpleFunction 的实际 physical filters/priority。
旧 schemaVersion 1/2 的 actual 导出仍是历史事实，不能输入新生成器假装已经重新验证。

pinned SDK 的公开 signature/VectorFunctionMetadata 投影原来不包含实际
SimpleFunction entry 的 priority、argument/result physical layout。仓库现管理精确
`velox-function-metadata-snapshot.patch`，限定提交
`3da0390118fa4b0addbfbdeb03f438ec1b1237a6` 和完整 pre/post source blobs：
`getFunctionMetadataSnapshot` 在 registry 读锁下复制每个真实 entry 的 immutable
shared metadata owner，锁外仍安全读取，不公开 factory、不改变 resolver。Exporter
schema3 保留实际 simple traversal/priority/physical filters 与 vector signature 顺序；
这些 diagnostic execution tags 尚需通过唯一 canonical physical type bridge 转为
manifest，不能视为另一个 SQL 类型系统或直接发布能力。Release/ASan setup 同源应用
该补丁、实际重建并验证 archive 定义了新 API；Docker 在依赖构建前复制同一 patch。
FindVelox 校验选定 header、source commit/blobs、patch SHA、archive SHA 与严格三行
provenance，拒绝旧 archive 配新 header。来源 hash 是构建一致性证据，不是身份认证
或实际 SQL 行为兼容性证明。新的 SDK archive/export/Native tests 尚未实际验证。

`null_treatment` 是 Pangrova binding extension，不是声称为标准 Substrait option。
唯一 `sql-bindings.json` 声明 LEAD/LAG/FIRST_VALUE/LAST_VALUE 的
IGNORE_NULLS/RESPECT_NULLS；Java producer 和 generated Native table 都消费该声明。
选项按官方有序 preference 协商，重复项、空 preference、未知字段、不支持该行为的
函数明确拒绝。LogicalWindow/RexOver 的 DISTINCT 与 ignoreNulls 均保留到 canonical
expression；不再统一写 ALL 或 ignoreNulls=false。新测试包含真实 FIRST_VALUE 结果
差异，但 Native 编译与执行未发生，不能把源码 oracle 当成已经通过。

`DistinctAggregateWindow` 复用实际 Aggregate、DistinctAggregations、RowContainer、
WindowPartition；没有第二套 tuple equality、NULL 去重、accumulator layout。实际 registry
中 aggregate window 的签名派生 DISTINCT 执行器，保留原类型和真实 empty aggregate
identity。固定下界的增长帧仅添加新输入，同帧重用结果；下界移动时重新建立该帧 distinct
集合，不能把已经离开帧的值留在状态中。新 canonical plan→production converter→Velox
Task oracle 覆盖 NULL、重复值、常量 tuple、prefix/sliding/empty frame。滑动帧仍可能
反复扫描，峰值内存及 spill/SLO 未实测；该正确性路径不是“最优复杂度/生产规模通过”
声明。Manifest 的 invocations/options 字段已经定义，完整实际发布/消费仍待闭环。

具体性能根因已复证 pinned `exec/DistinctAggregations.cpp:243–283`：每次
extractValues 都提取整个 unique set，重新 addSingleGroupRawInput，然后销毁并
初始化 aggregate 状态。因此即便 prefix 只添加新行，all-unique prefix 的整体最坏
时间仍为 Θ(N²)，不能写成线性增量执行。完整正确性 checkpoint 后必须设计同源
incremental distinct/extraction 或可撤销状态，保留真实 tuple/NULL 等价性，并测
prefix/sliding 的增长曲线、峰值内存及取消延迟；该路径当前不是规模验收完成。

RANGE 的根因是 pinned `exec/Window.cpp:121–153` 要求 frame boundary 为预计算的
非 Constant 列，且其类型必须与唯一排序列完全一致；旧 converter 直接写 BIGINT
constant。`WindowRangeBounds` 现于同一个 preparation Project 构造真实边界列：
ASC PRECEDING 为减、DESC PRECEDING 为加，FOLLOWING 相反；NULL 留给真实 peer
比较。整型键精确提升到 Native `LongDecimalType::kMaxPrecision`、scale=0，涵盖任意
int64 键±正 int64 offset，避免窄型 key 导致本可表达的范围错误。DECIMAL 保留完整
scale，扩展 precision；浮点保持原精度。算术使用实际注册 plus/minus resolver 和
checked CAST，不手填 decimal 公式、不截断、不饱和。超出真正 DECIMAL38 域仍由
实际 arithmetic overflow 明确失败。日期/interval 的完整 typed-offset 入口不在当前
官方 integer-bound 契约中，不宣称时间范围偏移已经贯通。

官方 Bound.Preceding/Following.offset 必须严格正，零值要用 CURRENT_ROW。Java 原
Preceding(0) producer 已规范化，两方向断言均增强，Native 对伪造零/负值仍拒绝。
phase 是 required，UNSPECIFIED 官方意味着 INTERMEDIATE_TO_RESULT，不意味着 single；
当前公开 window producer 是 INITIAL_TO_RESULT，Native 精确验证该真实执行模式。
state aggregate/window 必须持有 authoritative original raw argument schema；新增
FunctionRequirement.original_raw_argument_schema 为该 provenance 的位置，当前
collector/task admission 仍未落完，不允许从 opaque intermediate state 猜 raw scale。

最新 pure-tool 53/53 PASS（0.063s）仅验证生成/规范化及 bounded oracle transport
规则（逐响应30s/4MiB、严格UTF8/EOF）；最近 Protocol/Worker
204/204 PASS（19.561s，主线日志）验证资源投影及实际 admitted/observed response-byte
边界，不是新的 Native Window/Catalog/JNI 或多 Worker E2E 通过。上述 Native 新源使
早先全 CTest 基线失效。完整 manifest、eligible cohort、ROUND 原3项、AVG Decimal、
SUM0 empty-frame、SINGLE_VALUE cardinality、public MAP constructor 仍需按真实注册及
值 oracle 整改单元完成，不能换弱输入或以拒绝合法能力凑绿。

SUM0 源码已改为实际 SUM delegate 的 aggregate wrapper：完整复用 accumulator、
allocator、raw/intermediate 更新及 overflow 行为，仅最终 extractValues 将真实空
结果变为0，partial state 仍是原 SUM NULL state。注册签名与 metadata 复制实际 SUM，
并从该注册派生普通与 DISTINCT window。新增 canonical VT→converter→真实 Task
oracle 覆盖 empty/all-NULL/mixed values、partial/final component 和 empty-window；
源码未编译、值 oracle 未运行，不能记为通过。UNSPECIFIED aggregate phase 按官方
FINAL 处理；缺 raw provenance 不得伪作 raw SINGLE。公开 Java producer 仍明确
INITIAL_TO_RESULT，跨 fragment state provenance 尚需完整 collector/admission。

无 Native 消费的 WindowOptimizationHint JSON 已删去 producer/class/attach block。
ROW_NUMBER/TopN 仍由 Native 根据 canonical function/frame/filter 检测，新增同一
WindowProjectionContractTest 保留 schema 断言并覆盖两种 ROW_NUMBER，要求无私有
optimization payload。主线最近九类 C 与其他专项共192/192 PASS 是删除前的检查点；
本次 Planner 源变更需要再次验证，不将该历史检查点当当前新源通过。

主线已复验原范围并包含两个新增 ROW_NUMBER：194/194 PASS、0 skip（1m36s）。
这是 Java 检查点；实际 Native type/value、manifest/cohort 和公共 SQL E2E 尚不因此通过。

ResourceUsage 的 provider_response_bytes 是已准入/保留 HTTP entity bytes，新增
observed_provider_response_bytes 记录实际交付 bytes（含被限额拒绝的 chunk）。两项
必须可表示、observed>=admitted；observed 不受 retention grant 的成功上界比较，确保
真实超限 FAILED 报告可以终结。Native 两项保持0，Worker 真实 transport 唯一合并；
该指标是内核资源观测，不是已删除的 LLM usage statistics。

### 15.1 实际选中签名与 constant-argument 准入的下一检查点

pinned `FunctionRegistry.cpp::resolveFunctionOrCallableSpecialForm` 的实际顺序为
callable special form → SimpleFunction → VectorFunction。库存中的签名排序、简单函数
priority 或声明期望返回值都不能替代该选择过程。当前 scalar helper 尚未读取真实选中
signature 的 constantArguments，因此实际 `empty_approx_set(Constant<double>)` 的
非 Constant 表达式可能直到执行初始化才失败；不能将“exact output 已匹配”记为
该调用已经完整准入。

下一 repo-owned SDK 补丁在当前 dependency checkpoint 完成后原子升级：

- `ResolvedSimpleFunction::metadataSnapshot()` 只返回已有实际选中 entry 的 immutable
  shared metadata，不重新匹配、不解析 debugString、不暴露 factory。
- Vector 的只读 selected-signature API 应由现有 `resolveVectorFunctionWithMetadata`
  的同一 first-match 实现产生 `type/signature/metadata`；原公开方法投影该结果。不得在
  Pangrova 复制另一遍 vector selector，或搜寻恰好满足声明 output 的另一签名。
- scalar helper 优先调用真实 special-form resolver；若不是 special form，再读取实际
  simple/vector selection，校验其 exact output 和 constantArguments。special-form
  执行规则仍须来自唯一声明，不借不存在的 registered signature 推断。
- 真实注册回归使用 stock `empty_approx_set` 的 constant DOUBLE/非 constant DOUBLE、
  vector `in` 的 constant ARRAY/field ARRAY、ROUND short/long Decimal 和未知调用；
  不新增自制优先级表或为了测试登记虚构函数签名。

升级时严格保留实际构建证据：先在上一补丁仍可精确校验时撤回该补丁，再替换仓库
唯一 patch、严格 pre/post blobs 并重新 apply/rebuild；不在安装路径留下两版 fallback。
若增加非 inline Vector API，archive provenance 必须证明两个 API 都具有实际 global
定义（GNU/Darwin nm 的 T/W，不接受 U 引用），旧 snapshot archive 不能成为新版
selected-signature API 的构建证据。当前新 selected-signature API 尚未实施、编译或
执行，上述为可落地规格，不是验收通过。

### 15.2 聚合生产入口的待落地精确语义

当前 `AggregateConverter` 两个 measure 构造入口仍将全部 AVG 输出覆盖为 FP64，
将 SINGLE_VALUE/LITERAL_AGG 映为 MIN，且所有零参数聚合都补 Boolean TRUE；这些
不是已经解决的 manifest 问题。pinned Native
`functions/prestosql/aggregates/AverageAggregate.cpp::registerAverageAggregate` 的
真实签名为 INT→DOUBLE、REAL→REAL、DECIMAL(p,s)→同 DECIMAL(p,s)、日时间隔→同
间隔。完整整改必须由真实签名生成的 sole binder 推导 output/intermediate，并保留
SQL 所需显式 CAST；不能只删除 override 后把 Calcite 默认推导当 Native 真相。

Calcite 1.42 `SqlLiteralAggFunction` 明确 LITERAL_AGG 不收普通输入列参数，唯一
pre-argument 是 `AggregateCall.rexList` 中的 RexLiteral，结果即该 literal。整改可
在唯一 aggregate lowering 中移除此 measure，再以聚合后 Project 还原 exact literal
及原输出 ordinal，复用 LiteralConverter；需要验证 global-empty、非空 group、多个
grouping set 和非 Boolean/typed NULL literal，不能以 MIN(TRUE) 保留第二含义。

`SqlSingleValueAggFunction` 的条件是最多一条物理输入行，第二行即报错（NULL 也
计为行）。Native 没有对应 stock registered aggregate；MIN、arbitrary 或 DISTINCT
不能代替 cardinality enforcement。后续实际执行器须同时保存可合并的行数/值状态、
完整支持输入复杂类型和空输入 NULL；源 raw signature provenance 不得从 partial
state 猜测。验收至少覆盖空/一 NULL/一非 NULL/两个 NULL/相同两值/不同两值及真实
partial-final 合并错误传播，不以只拒绝合法 scalar-subquery 入口作为修复完成。

### 15.3 完整 Native 失败复证与唯一 ownership 整改单元

主线完整 Native Release r9 已实际编译、链接（0 warning）；随后完整 CTest 3612 项
实际 1995 PASS / 1617 FAIL / 0 SKIPPED，耗时 517.38s。日志为
`/tmp/pangrova-production-native-frozen-ctest-r1.log`。最初失败按完整 block 区分为
1502 个缺显式 reference root、54 个缺 Root DFS names、61 个其他失败；其中6个
segfault 是真实 ownership 缺陷，不能归为环境阻塞或仅修改测试持有 converter。

实际 LLDB 堆栈显示 ValuesNode/Task 析构经过 FlatVector、AlignedBuffer::freeToPool
访问失效 pool，而非 SUM0 或 Window.apply。converter 返回 plan 后局部持有者销毁，
旧 `valuesPoolHolders`/`attachPoolHolders` API 要求每个 caller 手工保活；独立 child、
Array/Map elements、FlatVector.slice 及 Arrow C export 又会越过 graph/root 生命周期。
因此该 getter 并不是执行结果所有权契约。真实 pinned SDK slice<bool> 非8位对齐还
会从 raw pool 新分配 bitmap，不能仅 pin 原 vector 的共享 BufferView 就宣称安全。

整改集中于唯一 repository-owned Velox patch（固定 commit
`3da0390118fa4b0addbfbdeb03f438ec1b1237a6`），不是第二执行 IR：

- BaseVector 保存实际 pool.shared_from_this，owner 的成员声明早于 nulls/buffers，
  派生与 base buffers 先析构、pool 最后析构。每个新 slice/child 自动拥有其实际 pool；
  不降为 weak/raw fallback。MemoryPool parent 为 strong、children 为 weak，task
  reclaimer 使用 weak task，已核实这些路径不构成新增循环。
- Arrow C export holder 保实际 source pool、export allocation pool；递归 child 可使用
  不同 pool。顶层导出还保实际 VectorPtr，避免 inline Constant value viewer 仅保 pool
  但值对象已析构。零列非零行 vector 仍持真实 pool，不伪造 allocation。
- Arrow owner import 先完成 make_shared owner 分配，再立即将两份原始 release 置空；
  alias shared_ptr 指向 owner 内 schema/array，递归失败与最后 child release 均只释放
  一次。allocation 失败保留 caller 原 owner；无临时 aggregate 引发提前析构。
- 删除全部 converter/driver/runner 的旧 holder getter/storage/attach 路径；literal
  pool 只在构造期间由本地 shared_ptr 保活，后续属于 SDK Vector 实际 owner。
  任意独立 raw BufferPtr 提取不在这次已核实的 Vector/Arrow 边界承诺内。

新增 CanonicalVectorPoolOwnershipTest 覆盖局部 converter 已销毁后的真实 Task、
独立 BOOL projection child 与非对齐 slice、Array/Map children、跨不同 pool 的 Arrow
导出、inline Constant storage 和零列 N 行。每项保留真实值并以 weak pool/vector 最后
释放作为 oracle；A 的 ColumnarImportOwnershipTest 覆盖 C release each-once、真实
unsupported schema 和实际 task-cap OOM。该批源码尚待统一 SDK/Native 重建和执行，
不能记为 PASS，ASan 仍是后续完整验收门槛。

另实际 DISTINCT constant-tuple 执行触发 SDK `Accumulator::destroy` 调用空 std::function：
pinned ConstantDistinctAggregations 按真实声明提供 nullable destroy callback，但
RowContainer.eraseRows 无条件调用它。第六同源 patch 仅对存在的 destroyFunction 调用，
不在 Window 增加 special case、不以 ALL 替代 DISTINCT。当前该 hunk 为临时审阅稿，
主线五文件 SDK rebuild 期间保持 patch/tool/FindVelox 冻结；完成后统一升级、同 target
重建并重新 record provenance，旧五文件构建不是六文件修改后的通过证据。

严格 Root/DFS decoder 不放宽。合法 production producer 与 Native fixtures 迁为显式
root_reference、完整 Root DFS labels；逻辑结果标签和 task-local physical ordinal alias
分别校验。TopNRowNumber 原参数、limit、partition/order、去 Filter/RowNumber 和 scan
叶全部保留，新增实际 ASC/NULLS LAST 检查；Join 的右输入唯一 ordinal rebase Project
保留原 scans/列序/条件索引；GroupId 内部 BIGINT discriminator 与公开 INTEGER
grouping_set_index 分别校验，不能把删除隐式名字断言当语义通过。

最近原 broadened Java 范围已复验 Protocol50 + Planner三个 artifact sink类10，共60/60
PASS / 0 FAIL / 0 ERROR / 0 SKIPPED，23.131s；日志
`/tmp/pangrova-production-strict-root-layout-nearest-r5.log`。此前 r1–r4 保留真实失败：
canonical count_expr、FanOut score nullability、GeneratedValue 实际 Project→Read 顺序
均按根因修复并保留原 schema/emit/列授权 oracle。此结果仅是 Java 近端检查点，未
证明 Native ownership、DISTINCT 窗口、JNI、公共 SQL 或多 Worker 全链路已通过。

ordinary registered table-function 同批消除第二个 schema 含义：Java relation authority、
Native directLogicalType 与 TableFunctionDetail.output_schema 都是完整输出；旧 Native
ExtensionMulti lowerer 又追加 input prefix。原9参数的 standalone/lateral 两组共18项
因 physical 名字包装与 lateral schema arity 漂移失败，另外9项 unknown-name 原负例保留。
当前 lowerer 只消费完整 schema，逐 ordinal 以 actual logicalNodeType(input) 强匹配
输入 prefix 的名字与 nested Type，执行 schema 使用唯一 physicalRowType；拒绝缺失、
改名、改型前缀和多余 inputs。保原 standalone/lateral 参数、参数表达式、输入叶和
类型断言，新增 precise prefix negatives；原 timestamp projection 补官方 emit，只选择
新增 timestamp literal，不把官方默认 input+expressions 误作 expression-only。
这些源码尚未编译或执行。Native RowType 仅证明 physical layout/嵌套名字，不表达完整
wire nullability/字符长度等约束；不能将这一比较替代尚待完整接线的 semantic contract。
