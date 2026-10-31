# AI Native 通用函数契约单一真源实施规格

状态：部分实施，正式 Planner → query cohort → TaskRequest → Native 需求消费尚未闭合；
本文件不是已实现能力声明。历史审计日期：2026-10-02；状态更新：2026-10-04。
范围：数据库内核函数与分布式准入，不含 Agent。

当前与历史断点应区分：Native 实际 catalogue 的发布、只读库存导出、Java
inventory codec 和同 owner 的精确 Native binding 已有实现；不能继续把
“functions 未填”或“启动未接线”当作当前事实。下文保留 SDK59、4021 项及各次
source-only 检查点的历史，不由旧标题推断当前状态。最新同源 Release 未过滤
CTest 为 **4041 PASS / 0 FAIL / 0 SKIPPED**；完整匹配依赖 cohort 的 Main ASan
构建通过，但未过滤 CTest 为 **4036 PASS / 5 FAIL / 0 SKIPPED**，整个 ASan
门禁仍为 FAIL。这不是重新包装后的 JNI/E2E 或生产规模通过证据。
Planner 仍有静态 defaults；统一 `TaskRequest.function_requirements = 18` 尚无
生产 writer/Native 比较者。后续须按实际 catalogue 冻结 query cohort，收集最终
IR 的精确 typed requirements，并与同一 published Native owner 比较。固定版本
字符串不是实际 packaged binary 身份，aggregate 中间状态还必须有真实来源。
详细最新证据与 PASS/FAIL/UNRUN 边界见 `ai-native-roadmap.md` 第 0.53 节；
当前生产消费审计及下一原子单元见本文件第 16 节。IDL 消息已经存在不代表消费已实现。

## 1. 2026-10-02 历史审计事实与当时的完成边界

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
与vector；`FunctionRegistry.cpp::resolveFunction` 的类型解析先simple再vector，
但 `ExprCompiler.cpp::compileCall` 实际执行编译先special-form、再vector、再simple。
simple内部还检查physicalTypeMatches及metadata priority。类型resolver输出相同
不证明执行选中同一entry、constantArguments或metadata；禁止把签名排序或类型
resolver first-match当作runtime准入结果。统一binder必须消费与实际compiler共享
的execution selection，并以真实执行差分验证，不能在Pangrova再写第二selector。
库存可以保留未公开签名，但不能据此自动宣称SQL支持；当前没有同输入重叠实现
产生错误值的实测证据，不将该接缝直接报告为已经发生的数据损坏。

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

pinned `FunctionRegistry.cpp::resolveFunctionOrCallableSpecialForm` 的类型解析顺序为
callable special form → SimpleFunction → VectorFunction，但
`ExprCompiler.cpp::compileCall` 真正编译执行的顺序是special-form → VectorFunction
factory → SimpleFunction。这是两个现有SDK入口的差异，不应把前者的类型结果或
name-level metadata当成后者的实际选中实现。当前
`RegisteredFunctionBinding.cpp::bindRegisteredScalar` 和
`FunctionResolverOracle.cpp::resolveRegisteredScalarOracle` 仍使用前者；schema3
库存与 `SCALAR_RUNTIME_PRIORITY` 标签尚未消除此断点。

一个不依赖假注册的具体准入缺口是stock `in(T, Constant<array(T)>)`：
`InPredicate.cpp` 声明第二参数必须常量，factory明确拒绝非constant IN list；当前
scalar helper仅检查输入/输出类型，直接FieldAccess ARRAY仍可先通过helper，直到
Task表达式编译时才失败。stock `empty_approx_set(Constant<double>)` 也有实际
initialize常量要求，但没有因此宣称它已经接入public SQL。当前最近
`RegisteredFunctionBindingTest` 的ROUND/SUM/LEAD三类断言没有覆盖该常量约束或
执行选中entry。这里是源码确认的准入完整性缺口，不是已经实测的错误值。

常量不能仅按原始 `ConstantTypedExpr` 判定：实际 `ExprCompiler::compileExpression`
先调用 `expression::optimize`，`getConstantInputs`读取优化后的 `ConstantExpr`。
因此直接把现有 `requireConstants` 接到scalar会误拒合法无损CAST/确定性常量，
也不能证明Native最终优化产生的常量与准入一致。优化不得提前调用有副作用的
callback，constant值的安全计算与限额仍须由同一真实优化/编译入口控制。

下一 repo-owned SDK 补丁在当前 dependency checkpoint 完成后原子升级：

- SDK提供immutable `ExecutionScalarSelection`，区分special-form/vector/simple，
  保留actual signature、concrete return、metadata及entry生命周期。选择顺序由当前
  `compileCall`抽取共享，不由Pangrova重演；runtime compiler必须消费同一结果。
  已选vector的factory抛错或返回空实例不得再fallback到simple。已有simple
  initialize用户错误/AlwaysFailingVectorFunction必须保留active-row与NULL/TRY
  错误语义，不能为了准入检查改为全部提前抛错；runtime错误仍按真实SDK传播。
- `ResolvedSimpleFunction::metadataSnapshot()`仅返回已有选中entry的immutable
  shared metadata；vector signature/type/metadata从真实first-match entry投射。
  entry的只读选择与factory使用共用authority，不解析debugString、不搜寻恰好符合
  declared output的另一个overload；原类型resolver仅作类型诊断，不能冒充execution
  selection。晚注册/overwrite必须受冻结catalog与entry生命周期围栏约束。
- Native binder、export及differential oracle消费execution selection，校验exact
  output、constantArguments与selected metadata；special-form来自唯一声明和真实
  SDK规则。constant验收使用同一安全优化/编译结果，不另写常量表达式解释器。
- 真实stock `in`以`[1,2,NULL]`对输入`1/3/NULL`执行，分别验证`TRUE/NULL/NULL`；
  constant ARRAY及可无损优化的CAST通过，field ARRAY在执行准入拒绝。验证stock
  `empty_approx_set`合法常量与FieldAccess拒绝，并保留实际初始化错误分类。
- stock `regexp_replace(VARCHAR,VARCHAR,VARCHAR)`验证实际选中simple entry的
  metadata；同名lambda vector注册`defaultNullBehavior=false`，name-level API会
  合并各实现，不能当作此具体调用的metadata。另保留ROUND short/long Decimal真实
  值/输出、未知签名、错误output与exact未隐式cast的负例。不登记虚构函数来凑覆盖。
- 真实Task值/constant/selection验证之后，再逐层贯通唯一manifest、Planner需求、
  eligible cohort与Worker admission；只通过导出或binding组件不算SQL/E2E闭环。

升级时严格保留实际构建证据：先在上一补丁仍可精确校验时撤回该补丁，再替换仓库
唯一 patch、严格 pre/post blobs 并重新 apply/rebuild；不在安装路径留下两版 fallback。
若增加非 inline Vector API，archive provenance 必须证明两个 API 都具有实际 global
定义（GNU/Darwin nm 的 T/W，不接受 U 引用），旧 snapshot archive 不能成为新版
selected-signature API 的构建证据。2026-10-03此修订仅纠正内部规格，当前新
execution-selection API尚未实施、编译或执行；冻结中的六文件SDK/Native源码不作
此单元的实现或验收证据，上述均是下一原子单元规格，不是验收通过。

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

2026-10-03 更新：既有源码已提交为 `e92532a`，保留该基线。主线五文件 ownership SDK
整库重建实际1036/1036、0 warning/error，随后以 `bash apply-velox-patches.sh record`
记录该真实检查点；首次直接执行脚本的 permission/126 失败保留，未修改机器权限。
当前同一 repository-owned patch 已追加 RowContainer nullable destroy，严格校验六个
pre/post blobs；完整源码、旧两文件 metadata 或旧五文件 ownership 检查点只能按
精确 hash 升级，不接受任意 partial 状态。补丁 SHA256 为
`fc8929f25c22fbef65ebfd461f1e9ec1d40921e3ffaad0b5602c301bcd821643`。

record 同时严格读取既有 provenance 的三行：固定40位 commit、两个64位小写 hex，
缺行、额外空行/partial第四行、格式漂移均拒绝。若 patch digest 改变而 archive digest
仍等于上次记录，必须拒绝旧 archive，不得重新记账冒充新构建。该 known-archive
barrier 只识别已记录的原样旧产物，不提供 source/archive 认证或执行行为兼容证明，
不引入孤立 marker API、另一套函数注册表或 Window-local 特判。

目前已通过的只是 `bash -n`、补丁 diff whitespace 校验和对实际 pinned RowContainer
base 的 `git apply --check`；线 C 未修改外部 SDK、未运行 build/record。主线下一步
必须实际 apply 六文件、证明旧五文件 archive record 被拒、同一 SDK target 增量重建，
再 record 六文件并执行 DISTINCT constant tuple、Vector/Arrow lifetime 和完整 Native
CTest/ASan。上述六文件行为验证尚未通过；五文件1036/1036不是新的六文件结果。

### 15.4 真实 MARK 三值语义与 Arrow owner 粒度（2026-10-03）

主线随后实际完成六文件apply，旧五文件archive record按预期拒绝，同SDK target
增量4/4重建0 warning/error并record成功；Native完整Release r5也实际编译/链接成功，
0 warning/error。最近真实Native142项为139 PASS / 3 FAIL（无skip），日志
`/tmp/pangrova-production-native-abi12-six-nearest-r1.log`。Vector pool五项与DISTINCT
窗口六项已经实际执行通过；不能由这些近端结果宣布完整CTest、ASan或公共SQL/E2E通过。

其中两个失败已定位：

- Arrow无自身bitmap的ROW导入递归为leaf单独创建C array/schema co-owner，root无
  BufferView引用因而立即各释放一次。旧positive fixture误观察root，不是数据复制
  证据。当前新增DOUBLE values/NULL bitmap pointer identity，并观察真实leaf在child
  存活时0、最后释放后1，root始终各一次。Unsupported/OOM原each-once断言保留。
- 任意predicate MARK经dummy constant-key null-aware HashJoin降低，真实HashProbe
  把predicate当WHERE filter：其UNKNOWN跟踪针对join-key NULL，不能替代整个predicate
  的TRUE > UNKNOWN > FALSE reduction。最近左右MARK四个UNKNOWN输出实测变FALSE。
  不通过把UNKNOWN期望改FALSE、只提取等值key或拒绝合法非等值MARK修复。

本次追加同一SDK patch的四个文件（六→十）：`core/PlanNode.h/.cpp`与
`exec/NestedLoopJoinProbe.h/.cpp`。NestedLoopJoinNode显式nullAwareMarker，仅允许
LeftSemiProject；普通SEMI/ANTI默认false。Builder/serialize/create完整保留同字段，
create缺失字段必须拒绝，不增加旧JSON兼容分支。实际Probe逐probe row记录UNKNOWN，
跨build vectors保持，任一TRUE照旧短路，无TRUE则输出NULL或FALSE；advanceProbe
清状态。空build输出FALSE，包括NULL probe；没有新增operator、private IR或第二registry。
Pangrova唯一buildNestedJoin MARK消费此节点，删除dummy hash keys，不做初始化fallback。

新增真实Task最近oracle覆盖非等值、NULL probe、全部FALSE/UNKNOWN/empty、TRUE
覆盖UNKNOWN不同顺序和多个实际build/probe batches、普通两值marker，以及Builder/
serde重新执行相同值。原左右MARK及SEMI/ANTI全部断言保留，另补canonical左/右非等值
转换后的值。任意predicate算法仍是O(P*B)，不把此正确性修复声称为大规模hash线性
性能；纯等值MARK优化需要独立的真实值/NULL/资源与规模验证。

十文件补丁SHA256为`393b242913ff5ffae1748e316200474be1e1828dcef8e8ac19612c945cb0eafd`。
工具逐文件严格校验精确pre/post blobs，支持原base/metadata2/ownership5/destroy6到
唯一十文件检查点的明确升级；新增hunks使用zero context，仅在完整source hash吻合后
以`--unidiff-zero`正反apply核验。known unchanged-archive barrier与三行provenance
严格语法保留，不把hash当认证/行为证明。线C只做syntax/diff/实际旧六文件source的
四个新hunk apply-check，未修改安装SDK、未构建；仍需主线apply10→旧6 record拒绝→
SDK同源重建/record10→Native最近/完整链。以上新MARK和owner观察源码尚未执行，
旧r5二进制的完整CTest运行不能当新十文件源码通过。

### 15.5 Full CTest后精确计划形状与实际Values验收（source-only，2026-10-03）

实际日志`/tmp/pangrova-production-native-abi12-six-full-ctest-r1.log`中：timestamp
precision投影12项、dynamic-filter shape6项、RowNumber shape81项、Expand shape81项
及Merge shape48项不能仅更新节点数量期待。timestamp的canonical emit[1]在真实
append-input Project之外添加ordinal projection，外层field不是constant；测试逐层
校验精确field/type→内部timestamp常量，并对原12 precision边界×两timezone形态执行
实际Task秒/纳秒值。dynamic-filter原Root只声明1名却真实输出3列；补完整Root，
检查原logical labels、join key materialization/rebase的每列ordinal/type和真实scan，
仍只算组件结构/外部filter reception，不算真实推送/E2E。

RowNumber保留原135参数测试，唯一lowerer的真实结构为末尾ordinal Project→RowNumber
→LocalPartition→输入准备Project→scan；新27个Values测试真正消费10/100/1000行及
uniform/skew/all-same分布，逐payload验证输入键和每组1..N编号。无ORDER BY时不声称
payload到编号的确定顺序。Expand保留原486参数测试，原single-node期待不符合真实
partial→exchange→final；两者须共同证明唯一GroupId、实际sum的raw/intermediate
类型、final只读state而不重新raw聚合，global-empty声明由partial真实globalGroupingSets
和groupId持有。新增81个Values测试各验证非空及空输入的全列sum/NULL/set-index结果，
全局空组不丢失/凭空增加，不能把Aggregation数量2单独当正确性证明。

两个已确认生产根因同批修复：`convertFilterRel`只识别直接RowNumber，但唯一window
lowerer始终产生末尾投影，合法rn<=K失去TopN；`extractTopNLimit`对strict INT64_MIN
先减1存在signed overflow。只沿精确typed FieldAccess、无重复的一对一映射追真实
marker，替换同RowNumber并完整重建原所有输出投影；只消除逐key/type/order证实为窗口
准备的直接排序，不越其它算子。先检查整数下界再减1。原216参数保持，新增36实际
Values TopK全列值测试与计算marker/重复marker/strict-min三个负向值测试，不能以
任意Project搜索假定列身份。

Merge旧fixture的sorted布尔与真实SortRel恰反，且只排序首key/NULLS_LAST；生产又
无条件重复排序。修fixture以全部keys的真实ASC_NULLS_FIRST SortRel作为唯一证据；
生产只沿typed field lineage抵达实际非partial OrderBy，required完整key prefix/type/
ASC/NULLS_FIRST全等才复用，其它必须新sort。原120参数测试保持、全部48 shape失败
方向保留；新增24组实际Values重复/NULL/INNER/LEFT/RIGHT/FULL多key值测试和wrong
direction/null-order/incomplete-prefix负向验证。树中恰2个排序还需证明来自原输入或
新插入的正确位置，不靠Read标签或布尔值猜有序。

当前上述源码仅diff/header/API静态检查，**未编译/运行**；SDK10和旧Native full CTest
通过/失败均不能替代本批行为。Root统一重编译后须原完整范围与新增值测试一并执行。
另`convertSortRel`当前对非FieldAccess排序项静默丢弃是独立未修生产缺陷：后续最小
设计应按原expression context唯一准备Project物化每个排序表达式，全部ASC/DESC及
NULL方向保留，Sort后精确还原原输出schema，不能丢项或转为任意列；须有computed
primary+field secondary/NULL/多driver值oracle。此项本批只记录设计，未实现/未验收。

### 15.6 唯一Sort计算键与真实partial/consumer边界（source-only，2026-10-03）

紧接§15.5独立小单元，`convertSortRel`现在复用已有`aggregateInputValue`唯一typed
表达式准备：原字段精确passthrough，computed/nested/constant key在真实Project每项
求值一次，OrderBy只读已物化typed field；后置前缀ordinal Project精确还原原schema。
四种普通direction明确支持；missing kind、UNSPECIFIED、CLUSTERED、未知enum/custom
comparator拒绝，不默认为ASC/NULLS_FIRST。Java普通/Window producer统一
`SortDirectionMapper`只输出这四种，NULL默认由Calcite已解析方向确定，Native不猜。

Fetch只穿过已证明为typed direct FieldAccess的前缀ordinal恢复投影，替换实际OrderBy
并重建所有原恢复层；computed/reordered/duplicate列或其它算子不作为排序身份。新增
四参数实际Values（各有无Fetch）保全computed primary+secondary、真实NULL、全部
原输出字段、精确有序值及LIMIT前缀；另有方向/comparator/缺expr/空keys负向与computed
输出投影不能误优化的真实结果。未削减已有参数和测试。

Pinned SDK `TopNNode/LimitNode::requiresSingleThread()`均为`!isPartial`；LocalPlanner
在整个pipeline检查此标志。因此原无exchange的partial→final虽然值可正确，却把
producer整条pipeline压为单driver，并不是注释所称的并行merge。普通Fetch TopN/Limit
现在为partial→真实LocalPartition gather→final；Root同根因修retrieval-source，线C
只在rerank同位置接gather，保实际detail.plan_node_id。全部四TypedRetrieval shape
oracle保原keys/count/score/callback声明，增加真实gather及上下游requiresSingleThread
合同。Root单独维护普通Fetch原fixture的offset/count/order完整断言。

新Task Values测试证明实际值和真实pipeline topology，不证明scan多driver吞吐：Values
默认不parallelizable，其producer依然单driver。真实scan/typed exchange多driver、分布式
汇总和目标集群性能/内存实测仍是后续验收，不能以这组测试声称大规模部署SLO通过。
本小单元仅源码及diff/pinned API核查，**未编译/运行**；§15.5“computedSort未实现”是
前检查点历史记录，不能作当前运行证明，须由Root同源Native重编译及完整回归复证。

### 15.7 Callback确定性字段的无损wiring（已验证的有限检查点，2026-10-03）

已定位完整生产链：唯一`runtimeSignature.deterministic`已被Java
`callbackMetadata`/`NativeFunctionBridge`写入canonical `NativeFunctionDescriptor`，
但JNI注册此前只取name/arguments/return；`makeJvmScalarFunctionSpec`和
`NativeUdfRegistry`未带metadata，Velox两个注册入口均使用默认deterministic=true。
实际`ExprOptimizer`→`ExprSet`→`Expr::isConstantExpr`消费这个默认值，因此不能以Java
描述符false证明Native不会折叠callback。尚未测出模型成功调用或错误值，不作该声称。

当前有限源码修复使用已有字段：`ScalarUdfSpec.deterministic`默认true保留显式Native
函数行为；JVM spec构造必须显式第四bool参数，JNI原样传descriptor值，不保留旧重载。
两个真实Velox注册入口均消费`VectorFunctionMetadataBuilder.deterministic`。SDK每name
只有一个metadata，注册时拒绝不同determinism的overloads，拒绝不污染原集合、不能取
首项或合并flags。默认NULL行为不在本批扩张，仍保留原注册语义。

`RuntimeCallbackSignature.deterministic`由同一必填JSON字段生成，既有两个Native
fixture caller消费该字段，不手写callback名单/false，不手改生成header。新增实际
生产JvmScalarFunction spec的stateful/stateless × true/false metadata验证；混合
overload拒绝后仍可注册合法overload；真实FILE_FROM_URL常量表达式以constant folding
启用构造ExprSet，必须保持非确定性call/非constant，同时原constant input保持constant。
该检查不安装Java callback、不调用模型或文件I/O；它证明真实编译不提前执行callback，
不是JNI成功执行或SQL E2E。另补canonical descriptor的true/false精确parse往返，以及
生成器的bool生成/缺字段/非法类型拒绝。

Root统一执行后的真实证据：Release构建PASS且零compiler warning/error；生成器
54/54 PASS；Java Descriptor12＋Bridge25＋Bootstrap10＝47/47 PASS、0 skip、23.643s；
Native最近85/85 PASS、31.19s，包含全部六个metadata/编译oracle；未过滤CTest
3846/3846 PASS、284.30s。日志分别为
`/tmp/pangrova-production-determinism-generator-r1.log`、
`/tmp/pangrova-production-determinism-java-nearest-r1.log`、
`/tmp/pangrova-production-native-determinism-release-r1.log`、
`/tmp/pangrova-production-native-determinism-nearest-r1.log`和
`/tmp/pangrova-production-native-determinism-full-ctest-r1.log`。
该检查点没有最新matching-ASan、package、JNI/provider E2E证明，也不覆盖后续Arrow
迁移。完整execution-selected manifest/cohort仍按§15.1待闭合，不因本字段修复宣称完成。

#### 函数契约生成环境（2026-10-03）

唯一生成器最低要求Python 3.10，不能依赖macOS系统`python3`必然满足要求。Root实际
系统3.9.6在最近完整脚本范围失败；使用本机已存在3.12.14后67/67 PASS、0 skip。
同源生成到临时目录后第二次四文件逐字节一致；生成Java使用JDK17
`-Xlint:all -Werror`、C++20 selection模板使用`-Wall -Wextra -Werror`均通过。这些检查
不证明实际Native factory、execution-selected目录或Planner/Worker闭环。

Maven通过`-Dpangrova.function-contract.python=<Python可执行文件>`选择解释器，CMake通过
`-DPython3_EXECUTABLE=<Python可执行文件>`选择解释器；两者仍由同一声明/生成器输出，
不提交机器绝对路径、生成文件或第二生成流程。干净checkout须先generate-sources再编译。
生产Linux builder源码固定Ubuntu22.04并安装其发行版`python3`，最低版本属于构建
入口条件而非业务回退；macOS/自托管CI不能据此推断自己的默认解释器，必须显式配置
符合最低版本的解释器。生成器拒绝旧解释器且传播失败，不降级语法或忽略generation失败。
CI实际Native/Maven构建job和nightly集群构建/测试job显式用官方
[`actions/setup-python@v6`](https://github.com/actions/setup-python/blob/v6/README.md)
将3.12加入PATH，保留原job/平台矩阵；自托管runner须满足该action要求的runner
版本（v2.327.1或更高）。CI源码配置不是其远程执行PASS，也不能把本地3.12检查写成
所有CI runner已运行成功。

### 15.8 Arrow stage边界的唯一canonical schema（源码检查点，尚未验证）

旧batch enum数值1与名字`BATCH_SERDE_NATIVE_ROW`均reserved；唯一可执行值为2
`BATCH_SERDE_ARROW_BATCH`，不保留`PRESTO_PAGES`或`NATIVE_ROW`别名。Java fragment、
StageTransfer/StageEdge、REST config、任务准入和持久化graph均消费Arrow；REST省略项
仍可取显式canonical默认，传入未知/大小写别名拒绝。任务compression必须精确为
`none/lz4/zstd`，batch行数必须为正且在signed-int32执行域内。LeafStageDispatcher
从真实StageEdge选择格式，不用独立默认掩盖两端差异。IPC自身压缩与opaque外层
transport codec不是同一层；Worker relay的精确透传由Root/B单元完成，不在本节宣称通过。

原根因不是physical alias本身：实际SDK Project/Aggregate输出使用canonical computed
名字，而consumer此前按Calcite SQL alias重新构造NamedStruct。`Root.names`仅用于结果
展示，不是wire authority。AuthorizedStageGraph.compile在拓扑冻结中从实际producer
完整canonical关系（包括emit）通过唯一SubstraitRelationLayout获取NamedStruct，并只在
该编译入口把typed ExchangeInput leaf绑定到源快照。类型Struct必须先完全一致，包括
precision、nullability、variation和嵌套shape；不允许靠rename修复类型错配。随后constructor
及replay只验证两端完整NamedStruct严格相等，不再修补计划。完整DFS名字、源交换
distribution/key ordinal和partitionCount必须保留；Root展示标签保持原样。

DurableStageRuntime原先绕过AuthorizedStageGraph constructor，从opaque metadata bytes
直接创建stage。现在所有含exchange的持久化graph在dag发布前调用同一个
`AuthorizedStageGraph.validateExchangeContracts`，由execution边界读取实际plan bytes，
不跨入metadata模块放SQL parser/schema decoder、不重新编译、不以checksum代替schema
比较；旧wire或错schema必须显式rebuild。模板/模型/bindings/authorization仍走原冻结入口。

新增完整canonical graph测试覆盖ROW/LIST/MAP/DECIMAL38/TIMESTAMP6的DFS名字、
nullability/precision/scale/variation漂移、producer emit顺序、Root展示标签不覆盖wire、
missing/stray/duplicate inputs、真正零列三行Values，以及64次固定seed名字扰动。
实际SQL computed/aggregate alias经public compiler→fragment→serialized graph证明源schema
绑定，持久化encode/decode后的错schema即使重新计算正确checksum仍拒绝；这些是计划/
控制契约oracle，不是Native值、JNI或分布式传输E2E。新增源码尚未编译/运行，descriptor
golden须在Java/Native双端冻结后由官方脚本统一生成；不能拿先前adapter CTest PASS
宣称actual PartitionedOutput→Arrow exchange链路或大规模部署SLO已通过。

### 15.9 实际签名模板与schema所有权（源码检查点，尚未验证）

`RegisteredFunctionSignature`从真实owned registry entry保留type变量、digit-only输入整数
约束、实际calculator AST、variadic/constant掩码及中间态。整数Native参数路径从既有
`physicalTypeBridge.nativeArgument`生成，不能把timestamp的SQL precision误当Native
参数；未知注册扩展保留完整opaque AST作诊断，不伪造可执行SQL类型或删掉目录项。
实际ANY是独立marker，不会给type变量赋值。Java精确模板解释器只绑定同一manifest
声明，不扩大数值类型、不按调用者期望输出搜索其它entry；selected输出不可表示仍失败。
这些解释器和Native projection尚未接成GET_CAPABILITIES→Planner→cohort→task-owned
factory全链，本节不把新组件或注册目录等同SQL可用。

命名ROW约束从唯一SubstraitRelationLayout读取完整DFS名字，不读取Root结果展示标签。
匿名computed ROW命名规则仍来自同一sql-bindings.json的physicalTypeBridge，现在由现有
structural生成器输出到schema producer-owned `substrait.generated`包，schema producer和
函数声明reader均消费这一输出，避免function与substrait双向包依赖。Maven在generate-sources
使用同一可配置Python解释器与generated sourceRoot，禁止手改生成类。新增ordinal负值/
MAX边界、同声明生成、实际声明变更传播、精确签名绑定及FanOut score准入回归；最近
正式执行结果由Root统一记录，源码检查点与完整生产验收分开。

#### 实际注册目录的集合路径修复（2026-10-03有限验证）

首次实际Native最近范围59项中58 PASS/1 FAIL；唯一失败为完整owned注册目录投影。
根因是`FunctionTypeProjectionChildPath`声明ROW的`Type.struct_.types`集合时没有ordinal，
`RegisteredTypeTemplate`却把它当作已经索引的`FunctionTypeSlot`检查repeated cardinality，
在真实child ordinal写入之前拒绝合法ROW。实际`map_entries`的
`map(K,V) -> array(row(K,V))`和`variance`的`row(bigint,double,double)`中间态均受影响。
修复直接读取有类型的集合声明路径：中间仅允许singular message，terminal允许完整
child集合，随后每个真实child的最终slot仍写准确repeated ordinal，未放宽decoder。
原全部真实scalar/aggregate/window注册遍历保留；新增上述两个真实注册的精确正测，
并保留name/signature异常上下文。Root同全目标Release构建r3零warning/error通过，
增强最近61/61 PASS、0 skip、195ms，日志为
`/tmp/pangrova-production-main-native-release-build-r3.log`及
`/tmp/pangrova-production-native-owned-signatures-nearest-r2.log`。这不证明GETCAP/SQL闭环。

### 15.10 唯一启动期发布与实际catalogue接线（待实施规格）

本单元不引入生产hot-UDF/hot-reload API，也不以CAS generation兼容共享测试生命周期。
当前真实NativeUdfRegistry完成注册后finalized；正式Worker每个incarnation仅在启动期
发布一次。SDK正常future注册不被永久seal，但已发布的Pangrova owner不会二读live
registry；后续合法产品热更新须另有完整creation/admission fence设计。

统一v2 RuntimeCommand增加显式`FINALIZE_FUNCTION_CATALOG`及有类型的空marker request，
不增加另一JNI/proto通道。现有enum21为RECOVER_EXECUTOR_CREATION，新增ordinal在实施前
与owner核对；未知request字段、非startup生命周期、重复发布均fail-closed。response为
typed publication receipt，仅包含发布manifest身份和构建身份；完整目录仍由GETCAP读取。
receipt/hash是同源兼容与一致性证据，不是认证、factory行为或SQL成功证明。

真实调用顺序必须为NativeRuntimeBootstrap初始化所有stock/custom/aggregate/window/
special注册，然后NativeFunctionBootstrap发现并安装实际callback，最后显式finalize，
之后才NativeCapabilityAdvertisement、TaskManager、HTTP/heartbeat及Coordinator READY。
`autoRegister=false`也执行finalize；发现零descriptor时不调用拒绝empty batch的
`nativeRegisterFunctions`，但仍发布真实builtin目录。非空JNI batch保留完整预检、实际
candidate登记、callback owner保活与失败不清除active callback语义。

publication只在独立准备完成后单次提交：capture SDK immutable owner，投影全部实际
entries及唯一声明语义，校验完整typed manifest和实际transport大小，构造完整immutable
publication，再提交其shared owner。准备过程不调用factory、不执行values、不持注册锁
进入JNI；失败不发布空目录、不READY、不在GETCAP重试capture。GETCAP只读取该owner
内冻结的目录/能力，不再用globalUdfRegistry/live scalar/signature读取补齐能力。

Task creation固定同一publication，严格对照canonical function requirements及实际
selected implementation，再传给VeloxPlanConverter、所有registered binding helper、
VeloxTaskRunner的QueryConfig。`require_function_registry_snapshot`由内核写入且session
不能覆盖。实际Scalar/Aggregate/Window compile/create及DISTINCT window intermediate
均消费同owner；metadata-only schema投影不创建执行factory。缺owner不能沿stock fallback。

精确迁移入口为WorkerBootstrap.startPhases、NativeFunctionBootstrap、
NativeCapabilityAdvertisement/NativeCapabilityLoader、NativeCommandBuilder及dispatcher、
JNI注册publication门、Native task creation/config、PlanExecutionDriver.prepareExecution、
VeloxPlanConverter及RegisteredFunctionBinding、VeloxTaskRunner创建QueryConfig。
Planner的SqlCompiler两个defaults constructor和SubstraitPlanValidator须替换为同一实际
immutable catalogue view；Coordinator从已认证Worker注册库存冻结eligible cohort，不取
全局最弱intersection或每函数/每Worker RPC，不以预期output另挑implementation。

真实fixture也需遵循startup边界。SqlE2ETestBase现先DISABLED GETCAP、执行普通SQL，后
enableAuthenticatedAiFunctions在同Native进程安装callback并重登记，这是待清除的fixture
生命周期债务，不是需要保留的生产hot-reload能力。AI E2E应使用正式独立startup/owned
fixture，provider配置先于READY；保留全部原真实SQL/治理/故障oracle，不加测试解封。
Native direct组件fixture可显式capture真实SDK owner验证值，但不能冒充Task startup准入。

Root实际schemaVersion4 registration export为3,454,762 JSON bytes，973个kind/name分组：
scalar481/1466 overloads、aggregate327/1833、window165/1543，另9 special forms。
原始JSON大小不是wire protobuf大小。完整投影后须分别实测serializedSize与Base64尺寸，
保留现4MiB encoded hard bound，不截断目录、抬阈值或假空。若真实库存超过transport界限，
须先批准统一v2有界transport迁移；目前没有该超限的实测结论。

SDK特殊表单尚缺同源typed program以及ROW companion factory owner，whole unit需要
实际resolveType/Java规则来自同一声明后才能执行；未知special/custom只可diagnostic，不
伪EXECUTABLE、不省略正常3VL能力。本节规格与前述纯解释器/61项近测分开，完整目录、
public SQL、JNI/package、matching-ASan及分布式SLO尚未验收。

### 15.11 实际生成输入与archive证明（2026-10-03源码检查点）

旧三行provenance只覆盖pin commit、SDK patch和archive，唯一execution-selection声明或
其Python导入闭包变更并不会改变patch SHA，因而不能证明archive执行的programme来自
当前生成输入。本轮改为严格五行：`pangrova-function-provenance-v2`、pin commit、
patch SHA256、generation-input SHA256、archive SHA256；旧三行必须显式重建迁移，
没有版本兼容解析。输入digest对声明原始bytes和实际repository-local Python递归导入
闭包作有长度的确定性编码，不仅hash语义JSON；Python最低3.10约束不放宽。

实际SDK `FunctionRegistryIdentity.cpp`独立TU返回其编译时生成的input digest。record
调用链接到指定mono archive的只读inspector，比较该out-of-line函数返回值及实际archive
SHA，不读取当前header就给旧archive盖章，不访问factory/registry或执行values。
已有proof若patch或inputs变而archive不变仍拒绝；proof是同源一致性证据，不是认证、
任意函数语义兼容证明或性能实测。Root负责真实编译/链接inspector、SDK rebuild和record。

SDK54首次source upgrade已apply，但verify因并行blob表错位失败，未开始SDK构建。
修复从固定pin导出的独立tree实际apply唯一patch，54个最终blob与已升级source逐个
一致，再修expected表，并增加所有checkpoint表cardinality相等的先验门槛；没有用
installed tree反向创造期望值。该SOURCE_READY不代表archive已匹配或目录已经发布。
Root最近Python3.12完整85/85 PASS、0 skip、0.193s，日志
`/tmp/pangrova-production-function-generator-input-provenance-nearest-r1.log`；这只是
生成输入/解释器测试，不是SDK54、Native factory、Planner或SQL闭环证据。

### 15.12 nullable authority与结果边界（待接通）

Native default-null metadata保留实际事实，不猜成SQL ANY_INPUT公式；IF/COALESCE/TRY、
aggregate空输入/空frame的具体nullable由唯一Calcite semantic validator推导，并保存
在canonical表达式与关系Type中。物理Type等价会擦除SQL nullability，不能因此允许
required字段包含实际NULL。Native需在现有IR decoder中并列保留逻辑NamedStruct，
再把同快照交输出stream边界，不新增TaskRequest第二份schema authority。

新增通用值验证器源码按actual DecodedVector index、有效父row及ARRAY/MAP实际offset
遍历；nullable父ROW为NULL时不误拒其required子字段，未引用的NULL元素不误拒，MAP
真实NULL key拒绝。无null metadata的scalar快路径不能推断嵌套子节点无NULL。新增六个
真实vector正反oracle尚未编译，且验证器尚未接完整输出schema-owner；不能以新枚举、
组件测试或typed inventory证明nullable已闭环。结果边界检查成本需实测，不声称免费。

## 16. 2026-10-04 当前消费断点复审与下一原子单元（尚未实施）

本节是当前源码的只读审计，纠正前文历史检查点的适用时间，不删除其失败记录或
原设计。**已实现 publication/binding 不等于已实现 query cohort、Planner 或 field18。**
完整路径相对于仓库根，同一单元内省略共同前缀；行号为本次审计检查点，
不是生成源码中的位置。

### 16.1 实际生产链与断点

| 环节 | 实际生产证据 | 当前判定 |
|---|---|---|
| Native publication | `pangrova-worker-native/src/capability/FunctionCatalogAssembler.cpp:149–165` 从真实 owned snapshot 投影、封存 manifest；`src/jni/command/NativeCommandDispatcher.cpp:532–542` 返回同一 publication | 已实现，不能再称 functions 未发布 |
| Worker 库存 | `pangrova-worker/pangrova-worker-native-bridge/src/main/java/org/pangrova/worker/nativebridge/jni/NativeCapabilityLoader.java:20–80` finalize→GET_CAPABILITIES→严格解析；`bootstrap/WorkerBootstrap.java:140–159` 保存编码库存，`bootstrap/phase/WorkerHeartbeatBootstrap.java:100` 写入注册请求 | 已实现，不是静态名字表 |
| 认证注册与持久保留 | `pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/http/endpoint/WorkerResource.java:103–152,288–295` 校验 internal identity、解码库存；`worker/WorkerInfo.java:36,53–55` 与 `worker/WorkerManager.java:398,429` 保留/编码完整 inventory | 已实现；内容 SHA 不是认证或 Worker incarnation |
| Query frozen cohort | `pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/execution/dispatch/dag/DefaultWorkerSelector.java` 的 select/selectForBindings/selectExcluding 动态读取 active workers；binding matcher 只覆盖现有 operator/model/artifact 契约 | **UNIMPLEMENTED**：没有 query-frozen function view 或全 fragment 函数交集 |
| Planner 消费 | `pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/bootstrap/PlannerWiring.java:51–59` 使用三参 compiler；`pangrova-planner/src/main/java/org/pangrova/planner/SqlCompiler.java:40–49,203–224` 安装静态 defaults 并据此拒绝计划 | **UNIMPLEMENTED**：`FunctionCatalogView` 无 Planner/Coordinator 生产 consumer |
| 最终 fragment 需求 | `pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/execution/dispatch/dag/binding/DAGStagePlanBinder.java:48–76` 在序列化后执行 overlay/retrieval/AI binding；`dispatch/dag/TaskRequestFactory.java:238–268` 写最终 bytes/checksum 等，却不写 function_requirements | **UNIMPLEMENTED**：必须在最后 rewrite 后收集，而非复制早期需求 |
| Native field18 准入 | `pangrova-worker-native/src/jni/command/TaskRequestParser.cpp:1556–1690` 未读取 field18；`src/jni/bridge/JniBridge.cpp:72–85` 已注入 published registry，但没有需求集合比较 | **UNIMPLEMENTED**：无 path/coverage/checksum/view/selected identity 比较 |
| Native 实际调用 | `pangrova-worker-native/src/capability/RegisteredFunctionBinding.cpp:54–86`；`src/velox/plan/SubstraitFunctionConversion.cpp:57–61,144–168`；`SubstraitAggregateConversion.cpp:49–76`；`SubstraitWindowConversion.cpp:125–145` | 已消费 actual snapshot 精确绑定；不能退回 global registry 或按 output 换 overload |

可复核缺口的只读检索：

```sh
rg -n 'FunctionCatalogView' pangrova-planner/src/main/java pangrova-coordination/pangrova-coordination-core/src/main/java
rg -n 'function_requirements|getFunctionRequirements|setFunctionRequirements|FunctionRequirementCollector' \
  pangrova-protocol/src/main pangrova-planner/src/main pangrova-coordination/pangrova-coordination-core/src/main \
  pangrova-worker/pangrova-worker-core/src/main pangrova-worker-native/src \
  --glob '*.{java,cpp,h,proto}'
```

本次检索第一项没有生产 consumer，第二项只有 `task.proto:98` 的 field18 声明。
`capability.proto:494–523` 的 FunctionRequirement/FunctionRequirements 已存在；
本单元应消费这些字段，不因消息已定义而宣布 writer/validator 已实施。

### 16.2 ROUND 与 nested ROW/ARRAY 的真实失败

`/tmp/pangrova-production-sdk64-reviewed-golden-planner-unfiltered-r2.log:23419–23445`
保留 `CanonicalExpressionSqlProtocolTest` 的四个真实 admission errors：三个 ROUND
被静态名称表拒绝，一个 runtime ARRAY<ROW> 被当成不支持的 NestedList。
其他 VALUES/public parser 错误属于唯一 Calcite 前端迁移，不与本根因混合。

- `pangrova-planner/src/main/java/org/pangrova/planner/substrait/ExpressionConverter.java:275–316`
  已输出 canonical NestedList/NestedStruct，保留 context type、显式 CAST 和 typed NULL；
  不是缺少 constructor producer。
- `validation/SubstraitPlanValidator.java:61–67,117–170,217–237` 未覆盖 nested 节点及
  children，aggregate measure 调用/参数也未完整遍历；scalar/window 仅按名称准入。
- `validation/RoundSqlFunction.java:31–47` 仍有独立 DECIMAL 返回公式；当前测试期望与
  已核实际 Native 示例一致，但没有消费 actual catalogue。不能以新增 whitelist 条目
  或复制一份签名/公式作为修复。
- `pangrova-protocol/src/main/java/org/pangrova/protocol/function/catalog/FunctionCatalogView.java:38–56,90–140`
  已提供 immutable view、scalar actual runtime selection、raw aggregate/window ordered
  first-match；`FunctionCatalogViewTest` 保留 ROUND 精确参数、kind、constants、named ROW
  相等性和 immutable snapshot 的组件 oracle。这不是 SQL/任务链已经接线的证明。

### 16.3 同一原子单元的必要前置契约

1. **Intrinsic 需求不能漏检。** Native `SubstraitNestedExpressionConversion.cpp:10–40`
   对无 function anchor 的 Nested.Struct/List 实际调用 row_constructor/array_constructor。
   sole `pangrova-protocol/src/main/resources/function-contracts/sql-bindings.json:132`
   已声明 ROW→row_constructor，但当前没有 array_constructor 的 SQL definition。
   collector 必须覆盖 intrinsic；在同一声明 authority 内定义其精确身份及 lowering
   对应，不临时伪造 ref、不把 ARRAY 类型桥当函数可用性、不新建 private 名字表。
2. **Selected identity 须双端统一。** `RegisteredFunctionCatalog.cpp:64` 已为实际 scalar
   implementation 设置 contract_sha256；aggregate/window 保留 ordered signatures 和
   registration identity，但 field13 的选中 ordinal 身份规范/helper 尚未接通。
   同一 canonical authority 必须定义并验证该身份；不能 Java/C++ 各自拼 digest，
   也不能以同名字、同返回类型或 name-level registration hash 代替选中 entry。
3. **最终 IR path 必须稳定且完整。** 以 checksum-pinned generated Plan 为根，采用经
   descriptor 验证的 protobuf field-number/repeated-index 路径，不用 Native plan ID、
   Calcite 类名或输出名字。由唯一 layout tree 提供各表达式的作用域、完整 DFS names，
   覆盖 nested、measure args/filter/order、window 和全部合法表达式位置。
4. **原始聚合来源不能猜。** field12 保存 Planner 实际 raw provenance；Native 当前
   `SubstraitAggregateConversion.cpp:68–72` 明确拒绝缺来源的 state phase。接通后按
   same owner 验证真实 state/intermediate/result，不能从 FINAL 的 VARBINARY 重建 p/s。
5. **Manifest 身份明确。** definition_view_sha256 使用实际 manifest.manifest_sha256，
   不是不存在的 manifest.contract_sha256，也不是 binary attestation。NamedStruct names、
   constants/options/invocation/phase/window process 均不能在物理类型投影时丢弃。

### 16.4 可实施最小原子范围（代码尚未落地）

1. Coordinator 从 authenticated WorkerInfo 捕获 query-local immutable definition/
   availability view 和 eligible 集合；PlannerWiring→SqlCompiler query scope→PangrovaPlanner
   operator inference/validator 消费同一 view。替换静态 registry 准入和普通函数独立公式，
   保留 sole SQL CAST policy；不修改共享 compiler 的全局可变 registry。
2. Protocol 新 collector 复用 SubstraitRelationLayout.describeTree/typeOf/nameAtTypePath、
   FunctionCatalogView、实际 selection/binder/physical projector。完整拒绝 missing/stray/
   duplicate path，不另建类型 decoder 或 availability table；统一 intrinsic/selected identity。
3. DAGStagePlanBinder/BoundExecutionPlan→TaskRequestFactory 在所有 overlay、retrieval、
   governed binding、AQE rewrite 后收集最终 requirements/checksum。write/artifact、
   intermediate/root、durable prepare/replay 和 retry 同迁；持久恢复不能换成当下动态库存。
4. Worker admission 与 Native TaskRequestParser/config/converter 对照同 publication 的
   manifest 和 registry；完整重算 path/call coverage，逐调用保持 actual exact binding。
   零函数计划也要显式完整空 calls envelope，字段缺失不能默认通过。Native 隐藏 governed
   args 和 structural lowering 需依原精确 binding 校验，不泛化函数名字豁免。
5. Scheduler 所有真实 select/retry 入口只在冻结集合内筛选满足完整 fragment 的 Worker；
   无候选明确失败。query cache/plan 及 durable bytes 保留契约身份，不用固定版本字符串
   代替内容一致性；不得添加 fallback 或未被调用的第二 planner registry。

上述是一个共同 SOURCE_READY 的原子范围，不发布只有 Native 拒绝而 Java writer
仍缺失的半单元。现有 IDL 能表达需求，不预先增加协议。Worker incarnation/HA
fencing、durable issued/outbox 和唯一 Calcite frontend 仍是独立缺口，不能由本单元
的 immutable WorkerInfo 或动态 active list 冒充已解决。

### 16.5 可执行验收与当前结果边界

以下命令由 Root 在同源完整单元冻结后运行；本次文档更新未执行它们。先最近，
再 owning suite/full chain；所有已有 SQL、值、型、NULL、cleanup/EOF oracle 保留。

```sh
mvn -B -Pfull-regression -pl pangrova-protocol -am \
  -Dtest=FunctionCatalogViewTest,FunctionImplementationSelectorTest,FunctionSignatureBinderTest,FunctionPhysicalTypeProjectorTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
mvn -B -Pfull-regression -pl pangrova-planner -am \
  -Dtest=CanonicalExpressionSqlProtocolTest,SubstraitPlanValidatorTest,NativeCapabilityRegistryTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
ctest --test-dir pangrova-worker-native/build --output-on-failure \
  -R 'RegisteredFunctionBindingTest|FunctionRegistryOwnedSnapshotTest|CanonicalNestedExecutionTest|NativeCommandGetCapabilitiesTest|ParseTaskConfigTest'
```

| 必须验收 | 真实 oracle / 新增边界 |
|---|---|
| ROUND | 原三条公开 SQL 类型断言不变；actual short/long DECIMAL unary/binary 值、NULL；错误声明 output 和未显式 CAST 的参数准确拒绝 |
| Nested | 动态 ARRAY<ROW> 的精确 DECIMAL/NULL/ordinal 值及 EOF；named-row mismatch、异构元素需显式 CAST；intrinsic 和 child calls 都有完整需求 |
| Requirements | 最后 rewrite 后正确 checksum/path；漏 call、多 call、重复 path、错 kind/URN/ref/hash/view/selected entry、constants/options 篡改准确拒绝；零 call envelope 正例 |
| Aggregate/window | 原 single/partial/final 及 DISTINCT/filter/order 值 oracle；原 raw 来源、中间类型和 state phase 严格对称，不由 state schema 猜 raw |
| 认证/cohort | 实际 GETCAP→认证注册→query frozen view→final fragment→field18→真实 Native Task；库存漂移、每 Worker 只有半套函数、失效/retry 不可偷偷换 authority |
| 全链 | 上述 owning suites、同源 Step0–8、完整 Release/ASan、重新打包 JNI、独立进程 SQL E2E；组件 snapshot/codec 通过不能代替这些证据 |

保留旧摘要中的历史检查点：SDK59 实际 Release archive/provenance 和 Main build
通过时，未筛选4009项CTest仍有6个失败。随后聚合元数据与复杂 MAP schema 问题由
64-file sole SDK patch整改，Release setup、七项 linked archive provenance 和 Main
完整重建通过且零 compiler warnings；该 cohort 的4021项首轮有一个 tracked-pool
fixture 失败，修复后的31项专项和完整4021项均通过且零跳过。该时点尚无匹配 ASan
或重新包装 JNI/E2E 的通过证据；不能用后来的结果回写这些历史失败。

依 `ai-native-roadmap.md` 第 0.52–0.53 节，当前实际结果分别为：

- Release 未过滤 **4041/4041 PASS，0 skip，636.46s**：
  `/tmp/pangrova-production-sdk64-pool-re2-release-unfiltered-r1.log`。
- 完整匹配依赖 cohort 的 ASan 未过滤 **4036 PASS / 5 FAIL / 0 skip，1841.98s，exit8**：
  `/tmp/pangrova-production-sdk64-main-asan-complete-cohort-unfiltered-r1.log`。
  五项进程退出各报告256字节；独立 oracle 与真实 Lance 的 LLDB 证据定位到 Darwin
  dyld TLS 分配。没有改线程、关闭 leaks 或增加 suppression；原 ASan FAIL 保留，
  Linux 同源结果仍缺失，不能记成跨平台泄漏验收通过。

以上 Native 回归不关闭本节明列的 UNIMPLEMENTED 消费链，也不等于新 packaged
JNI/E2E、HA 或大规模性能通过。
