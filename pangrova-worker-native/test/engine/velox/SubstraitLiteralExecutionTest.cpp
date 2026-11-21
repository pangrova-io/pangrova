// Semantic oracles: canonical Substrait -> production converter -> real Velox Task.
// These are Native component tests, not JNI or distributed SQL E2E evidence.
#include <gtest/gtest.h>
#include "CanonicalTestPlan.h"
#include "velox/expression/FunctionRegistrySnapshot.h"
#include <google/protobuf/message.h>

#include <atomic>
#include <cstdint>
#include <exception>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "TestMemoryHelper.h"
#include "jni/RuntimeInitializer.h"
#include "engine/velox/plan/VeloxPlanConverter.h"
#include "substrait/plan.pb.h"
#include "velox/core/PlanFragment.h"
#include "velox/core/QueryCtx.h"
#include "velox/exec/Task.h"
#include "velox/common/base/VeloxException.h"
#include "velox/type/DecimalUtil.h"
#include "velox/type/Timestamp.h"
#include "velox/type/Type.h"
#include "velox/functions/prestosql/types/UuidType.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace pangrova::velox::test {
namespace {

using facebook::velox::int128_t;
using facebook::velox::RowVectorPtr;
using Literal = substrait::Expression_Literal;

substrait::Type decimalType(int precision, int scale) {
  substrait::Type type;
  type.mutable_decimal()->set_precision(precision);
  type.mutable_decimal()->set_scale(scale);
  type.mutable_decimal()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  return type;
}

Literal decimalLiteral(int precision, int scale, int128_t value) {
  Literal literal;
  auto* decimal = literal.mutable_decimal();
  decimal->set_precision(precision);
  decimal->set_scale(scale);
  __uint128_t bits = static_cast<__uint128_t>(value);
  std::string bytes(16, '\0');
  for (int i = 0; i < 16; ++i) {
    bytes[i] = static_cast<char>((bits >> (8 * i)) & 0xff);
  }
  decimal->set_value(bytes);
  return literal;
}

Literal nullLiteral(const substrait::Type& type) {
  Literal literal;
  literal.mutable_null()->CopyFrom(type);
  return literal;
}

substrait::Expression expression(const Literal& literal) {
  substrait::Expression result;
  result.mutable_literal()->CopyFrom(literal);
  return result;
}

substrait::Plan projection(
    const std::vector<substrait::Expression>& expressions,
    const std::vector<std::string>& functions = {}) {
  substrait::Plan plan;
  for (size_t i = 0; i < functions.size(); ++i) {
    auto* function = plan.add_extensions()->mutable_extension_function();
    function->set_function_anchor(i);
    function->set_name(functions[i]);
  }
  auto* root = plan.add_relations()->mutable_root();
  auto* project = root->mutable_input()->mutable_project();
  auto* read = project->mutable_input()->mutable_read();
  read->mutable_base_schema()->add_names("seed");
  read->mutable_base_schema()->mutable_struct_()->add_types()->mutable_i64()
      ->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  read->mutable_virtual_table()->add_expressions()->add_fields()
      ->mutable_literal()->set_i64(1);
  for (size_t i = 0; i < expressions.size(); ++i) {
    project->add_expressions()->CopyFrom(expressions[i]);
    project->mutable_common()->mutable_emit()->add_output_mapping(i + 1);
    root->add_names("result_" + std::to_string(i));
  }
  return plan;
}

substrait::Plan values(
    const std::vector<substrait::Type>& types,
    const std::vector<std::vector<Literal>>& rows) {
  substrait::Plan plan;
  auto* root = plan.add_relations()->mutable_root();
  auto* read = root->mutable_input()->mutable_read();
  read->mutable_base_schema()->mutable_struct_();
  for (size_t i = 0; i < types.size(); ++i) {
    const auto name = "column_" + std::to_string(i);
    root->add_names(name);
    read->mutable_base_schema()->add_names(name);
    read->mutable_base_schema()->mutable_struct_()->add_types()->CopyFrom(types[i]);
  }
  read->mutable_virtual_table();
  for (const auto& row : rows) {
    auto* encoded = read->mutable_virtual_table()->add_expressions();
    for (const auto& literal : row) {
      encoded->add_fields()->mutable_literal()->CopyFrom(literal);
    }
  }
  return plan;
}

std::vector<uint8_t> serialize(const substrait::Plan& plan) {
  const auto bytes = pangrova::test::canonicalTestPlan(plan).SerializeAsString();
  return {bytes.begin(), bytes.end()};
}

substrait::Expression binary(
    const Literal& left, const Literal& right, const substrait::Type& output) {
  substrait::Expression expression;
  auto* function = expression.mutable_scalar_function();
  function->set_function_reference(0);
  function->mutable_output_type()->CopyFrom(output);
  function->add_arguments()->mutable_value()->mutable_literal()->CopyFrom(left);
  function->add_arguments()->mutable_value()->mutable_literal()->CopyFrom(right);
  return expression;
}

class SubstraitLiteralExecutionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_ = pangrova::test::testLeafPool("SubstraitLiteralExecutionTest");
  }

  RowVectorPtr execute(const substrait::Plan& plan) {
    const auto converter_registry =
        facebook::velox::exec::FunctionRegistrySnapshot::capture();
    VeloxPlanConverter converter(converter_registry);
    auto converted = converter.convert(serialize(plan), {});
    // Actual vectors own their creating pools independently of the converter.
    static std::atomic<uint64_t> id{0};
    auto task = facebook::velox::exec::Task::create(
        "canonical-literal-" + std::to_string(id.fetch_add(1)),
        facebook::velox::core::PlanFragment{converted}, 0,
        facebook::velox::core::QueryCtx::create(pangrova::jni::globalExecutor().get(),
            facebook::velox::core::QueryConfig({{"require_function_registry_snapshot", "true"}}, converter_registry)),
        facebook::velox::exec::Task::ExecutionMode::kSerial);
    EXPECT_TRUE(task->supportSerialExecutionMode());
    RowVectorPtr result;
    while (auto batch = task->next()) {
      if (result) {
        ADD_FAILURE() << "small literal oracle unexpectedly emitted multiple batches";
      }
      result = std::dynamic_pointer_cast<facebook::velox::RowVector>(
          facebook::velox::BaseVector::copy(*batch, pool_.get()));
    }
    EXPECT_FALSE(task->error()) << task->errorMessage();
    EXPECT_EQ(task->state(), facebook::velox::exec::TaskState::kFinished);
    EXPECT_EQ(task->numRunningDrivers(), 0);
    return result;
  }

  void reject(const substrait::Plan& plan, const std::string& reason) {
    const auto converter_registry =
        facebook::velox::exec::FunctionRegistrySnapshot::capture();
    VeloxPlanConverter converter(converter_registry);
    try {
      converter.convert(serialize(plan), {});
      ADD_FAILURE() << "invalid canonical plan was accepted: " << reason;
    } catch (const std::exception& failure) {
      EXPECT_NE(std::string(failure.what()).find(reason), std::string::npos)
          << failure.what();
    }
  }

  void rejectSpecialFormInput(const substrait::Plan& plan) {
    const auto registry = facebook::velox::exec::FunctionRegistrySnapshot::capture();
    VeloxPlanConverter converter(registry);
    try {
      converter.convert(serialize(plan), {});
      ADD_FAILURE() << "Invalid special-form input contract was accepted";
    } catch (const facebook::velox::VeloxUserError& failure) {
      EXPECT_EQ(failure.errorSource(), "USER");
      EXPECT_EQ(failure.errorCode(), "INVALID_ARGUMENT");
      EXPECT_STREQ(failure.function(), "resolveDeclaredType");
      EXPECT_NE(failure.message().find("Special form input contract mismatch"), std::string::npos);
      EXPECT_FALSE(failure.isRetriable());
    }
  }

  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
};

TEST_F(SubstraitLiteralExecutionTest, UuidLiteralsPreserveEveryBitAndTypedNullInActualTask) {
  substrait::Type type;
  type.mutable_uuid()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  Literal zero, all, mixed;
  zero.set_uuid(std::string(16, '\0'));
  all.set_uuid(std::string(16, '\xff'));
  mixed.set_uuid(std::string("\x00\x11\x22\x33\x44\x55\x66\x77\x88\x99\xaa\xbb\xcc\xdd\xee\xff", 16));
  auto result = execute(values({type}, {{zero}, {all}, {mixed}, {nullLiteral(type)}}));
  ASSERT_TRUE(result);
  ASSERT_EQ(result->size(), 4);
  ASSERT_TRUE(result->childAt(0)->type()->equivalent(*facebook::velox::UUID()));
  const auto* ids = result->childAt(0)->asFlatVector<int128_t>();
  EXPECT_EQ(ids->valueAt(0), 0);
  EXPECT_EQ(ids->valueAt(1), -1);
  EXPECT_EQ(ids->valueAt(2), (static_cast<int128_t>(0x0011223344556677LL) << 64) |
      static_cast<int128_t>(0x8899aabbccddeeffULL));
  EXPECT_TRUE(ids->isNullAt(3));
}

TEST_F(SubstraitLiteralExecutionTest, MalformedUuidLiteralsRejectInsteadOfPaddingOrTruncating) {
  substrait::Type type;
  type.mutable_uuid()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  for (size_t width : {15, 17}) {
    Literal literal;
    literal.set_uuid(std::string(width, '\0'));
    reject(values({type}, {{literal}}), "exactly 16 bytes");
  }
}

struct ArithmeticOracle {
  const char* name;
  int left_precision;
  int left_scale;
  int right_precision;
  int right_scale;
  int output_precision;
  int output_scale;
  int64_t left;
  int64_t right;
  int64_t expected;
};

class SubstraitDecimalArithmeticExecutionTest
    : public SubstraitLiteralExecutionTest,
      public ::testing::WithParamInterface<ArithmeticOracle> {};

TEST_P(SubstraitDecimalArithmeticExecutionTest, ExactValueAndResultType) {
  const auto& oracle = GetParam();
  auto call = binary(
      decimalLiteral(oracle.left_precision, oracle.left_scale, oracle.left),
      decimalLiteral(oracle.right_precision, oracle.right_scale, oracle.right),
      decimalType(oracle.output_precision, oracle.output_scale));
  auto output = execute(projection({call}, {oracle.name}));
  ASSERT_NE(output, nullptr);
  ASSERT_EQ(output->size(), 1);
  auto result = output->childAt(0);
  EXPECT_TRUE(result->type()->equivalent(
      *facebook::velox::DECIMAL(oracle.output_precision, oracle.output_scale)));
  if (oracle.output_precision <= 18) {
    EXPECT_EQ(result->asFlatVector<int64_t>()->valueAt(0), oracle.expected);
  } else {
    EXPECT_EQ(result->asFlatVector<int128_t>()->valueAt(0), oracle.expected);
  }
}

INSTANTIATE_TEST_SUITE_P(
    ShortLongAndPrecisionCap, SubstraitDecimalArithmeticExecutionTest,
    ::testing::Values(
        ArithmeticOracle{"add", 10, 2, 25, 4, 26, 4, 123, 20000, 32300},
        ArithmeticOracle{"subtract", 10, 2, 25, 4, 26, 4, 123, 20000, -7700},
        ArithmeticOracle{"multiply", 10, 2, 25, 4, 35, 6, 123, 20000, 2460000},
        ArithmeticOracle{"divide", 10, 2, 25, 4, 16, 4, 123, 20000, 6150},
        ArithmeticOracle{"modulus", 10, 2, 25, 4, 12, 4, 123, 20000, 12300},
        ArithmeticOracle{"add", 25, 4, 10, 2, 26, 4, 20000, 123, 32300},
        ArithmeticOracle{"subtract", 25, 4, 10, 2, 26, 4, 20000, 123, 7700},
        ArithmeticOracle{"multiply", 25, 4, 10, 2, 35, 6, 20000, 123, 2460000},
        ArithmeticOracle{"divide", 25, 4, 10, 2, 27, 4, 20000, 200, 10000},
        ArithmeticOracle{"modulus", 25, 4, 10, 2, 12, 4, 20000, 123, 7700},
        ArithmeticOracle{"multiply", 10, 2, 10, 2, 20, 4, 123, 200, 24600},
        ArithmeticOracle{"divide", 10, 2, 10, 2, 12, 2, 123, 200, 62},
        ArithmeticOracle{"add", 38, 2, 38, 2, 38, 2, 123, 200, 323},
        ArithmeticOracle{"multiply", 25, 4, 25, 4, 38, 8, 12300, 20000, 246000000},
        ArithmeticOracle{"divide", 25, 4, 25, 4, 29, 4, 12300, 20000, 6150},
        ArithmeticOracle{"modulus", 25, 4, 25, 4, 25, 4, 12300, 20000, 12300}),
    [](const ::testing::TestParamInfo<ArithmeticOracle>& info) {
      const auto& oracle = info.param;
      return std::string(oracle.name) + "_L" +
          std::to_string(oracle.left_precision) + "S" +
          std::to_string(oracle.left_scale) + "_R" +
          std::to_string(oracle.right_precision) + "S" +
          std::to_string(oracle.right_scale) + "_O" +
          std::to_string(oracle.output_precision) + "S" +
          std::to_string(oracle.output_scale);
    });

TEST_F(SubstraitLiteralExecutionTest, ScalarDecimalTypedNullAndSignedValues) {
  auto output = execute(projection({
      expression(decimalLiteral(18, 2, -123)),
      expression(decimalLiteral(25, 4, -12300)),
      expression(nullLiteral(decimalType(25, 4)))}));
  ASSERT_NE(output, nullptr);
  ASSERT_EQ(output->childrenSize(), 3);
  EXPECT_EQ(output->childAt(0)->asFlatVector<int64_t>()->valueAt(0), -123);
  EXPECT_EQ(output->childAt(1)->asFlatVector<int128_t>()->valueAt(0), -12300);
  EXPECT_TRUE(output->childAt(2)->isNullAt(0));
  EXPECT_TRUE(output->childAt(2)->type()->equivalent(*facebook::velox::DECIMAL(25, 4)));
}

TEST_F(SubstraitLiteralExecutionTest, MapLiteralPreservesTypedKeyValuePairs) {
  Literal map;
  auto* first = map.mutable_map()->add_key_values();
  first->mutable_key()->set_string("width");
  first->mutable_value()->set_i32(100);
  auto* second = map.mutable_map()->add_key_values();
  second->mutable_key()->set_string("height");
  second->mutable_value()->set_i32(200);

  auto output = execute(projection({expression(map)}));
  ASSERT_NE(output, nullptr);
  ASSERT_EQ(output->size(), 1);
  auto result = output->childAt(0)->as<facebook::velox::MapVector>();
  ASSERT_NE(result, nullptr);
  ASSERT_EQ(result->sizeAt(0), 2);
  EXPECT_TRUE(result->type()->equivalent(*facebook::velox::MAP(
      facebook::velox::VARCHAR(), facebook::velox::INTEGER())));
  EXPECT_EQ(result->mapKeys()->asFlatVector<facebook::velox::StringView>()->valueAt(0), "width");
  EXPECT_EQ(result->mapValues()->asFlatVector<int32_t>()->valueAt(1), 200);
}

TEST_F(SubstraitLiteralExecutionTest, DayIntervalLiteralExecutesDateAdditionWithoutLosingPrecision) {
  Literal date;
  date.set_date(19'723);  // 2024-01-01.
  Literal interval;
  auto* value = interval.mutable_interval_day_to_second();
  value->set_days(30);
  value->set_seconds(0);
  value->set_precision(6);
  value->set_subseconds(0);
  substrait::Type dateType;
  dateType.mutable_date()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);

  auto output = execute(projection({binary(date, interval, dateType)}, {"add"}));
  ASSERT_NE(output, nullptr);
  EXPECT_EQ(output->childAt(0)->asFlatVector<int32_t>()->valueAt(0), 19'753);

  // Exercise the deprecated wire field without relying on its deprecated setter.
  const auto* legacy = value->GetDescriptor()->FindFieldByName("microseconds");
  ASSERT_NE(legacy, nullptr);
  value->GetReflection()->SetInt32(value, legacy, 0);
  output = execute(projection({binary(date, interval, dateType)}, {"add"}));
  ASSERT_NE(output, nullptr);
  EXPECT_EQ(output->childAt(0)->asFlatVector<int32_t>()->valueAt(0), 19'753);

  value->set_precision(6);
  interval.mutable_interval_day_to_second()->set_subseconds(1);
  reject(projection({expression(interval)}), "loses sub-millisecond precision");
}

TEST_F(SubstraitLiteralExecutionTest, DecimalArraysRetainTypedNullsAndScale) {
  Literal array;
  auto* list = array.mutable_list();
  list->add_values()->CopyFrom(nullLiteral(decimalType(25, 4)));
  list->add_values()->CopyFrom(decimalLiteral(25, 4, 12300));
  list->add_values()->CopyFrom(decimalLiteral(25, 4, -7700));
  list->add_values()->CopyFrom(nullLiteral(decimalType(25, 4)));
  auto output = execute(projection({expression(array)}));
  ASSERT_NE(output, nullptr);
  auto* result = output->childAt(0)->as<facebook::velox::ArrayVector>();
  ASSERT_NE(result, nullptr);
  EXPECT_TRUE(result->type()->equivalent(
      *facebook::velox::ARRAY(facebook::velox::DECIMAL(25, 4))));
  ASSERT_EQ(result->sizeAt(0), 4);
  auto* elements = result->elements()->asFlatVector<int128_t>();
  const auto offset = result->offsetAt(0);
  EXPECT_TRUE(elements->isNullAt(offset));
  EXPECT_EQ(elements->valueAt(offset + 1), 12300);
  EXPECT_EQ(elements->valueAt(offset + 2), -7700);
  EXPECT_TRUE(elements->isNullAt(offset + 3));
}

TEST_F(SubstraitLiteralExecutionTest, StructLiteralsUseTheSameNestedDecimalAndNullDecoder) {
  Literal record;
  auto* fields = record.mutable_struct_();
  fields->add_fields()->CopyFrom(nullLiteral(decimalType(25, 4)));
  auto* list = fields->add_fields()->mutable_list();
  list->add_values()->CopyFrom(decimalLiteral(25, 4, -7700));
  list->add_values()->CopyFrom(nullLiteral(decimalType(25, 4)));
  auto plan = projection({expression(record)});
  auto* root = plan.mutable_relations(0)->mutable_root();
  root->add_names("field_0");
  root->add_names("field_1");
  auto output = execute(plan);
  ASSERT_NE(output, nullptr);
  auto* result = output->childAt(0)->as<facebook::velox::RowVector>();
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->type()->asRow().nameOf(0), "field_0");
  EXPECT_EQ(result->type()->asRow().nameOf(1), "field_1");
  EXPECT_TRUE(result->childAt(0)->isNullAt(0));
  EXPECT_TRUE(result->childAt(0)->type()->equivalent(*facebook::velox::DECIMAL(25, 4)));
  auto* array = result->childAt(1)->as<facebook::velox::ArrayVector>();
  ASSERT_NE(array, nullptr);
  ASSERT_EQ(array->sizeAt(0), 2);
  auto* elements = array->elements()->asFlatVector<int128_t>();
  ASSERT_NE(elements, nullptr);
  EXPECT_EQ(elements->valueAt(array->offsetAt(0)), -7700);
  EXPECT_TRUE(elements->isNullAt(array->offsetAt(0) + 1));
}

TEST_F(SubstraitLiteralExecutionTest, AllNullAndNestedTypedEmptyArrays) {
  Literal null_array;
  null_array.mutable_list()->add_values()->CopyFrom(nullLiteral(decimalType(10, 2)));
  null_array.mutable_list()->add_values()->CopyFrom(nullLiteral(decimalType(10, 2)));
  Literal empty;
  empty.mutable_empty_list()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  empty.mutable_empty_list()->mutable_type()->CopyFrom(decimalType(10, 2));
  Literal nested;
  nested.mutable_list()->add_values()->CopyFrom(empty);
  nested.mutable_list()->add_values()->CopyFrom(empty);
  auto output = execute(projection({expression(null_array), expression(nested)}));
  ASSERT_NE(output, nullptr);
  auto* nulls = output->childAt(0)->as<facebook::velox::ArrayVector>();
  ASSERT_NE(nulls, nullptr);
  EXPECT_EQ(nulls->sizeAt(0), 2);
  EXPECT_TRUE(nulls->elements()->isNullAt(nulls->offsetAt(0)));
  EXPECT_TRUE(nulls->elements()->isNullAt(nulls->offsetAt(0) + 1));
  auto* arrays = output->childAt(1)->as<facebook::velox::ArrayVector>();
  ASSERT_NE(arrays, nullptr);
  EXPECT_TRUE(arrays->type()->equivalent(*facebook::velox::ARRAY(
      facebook::velox::ARRAY(facebook::velox::DECIMAL(10, 2)))));
  auto* children = arrays->elements()->as<facebook::velox::ArrayVector>();
  ASSERT_NE(children, nullptr);
  EXPECT_EQ(children->sizeAt(arrays->offsetAt(0)), 0);
  EXPECT_EQ(children->sizeAt(arrays->offsetAt(0) + 1), 0);
}

TEST_F(SubstraitLiteralExecutionTest, ValuesShareDecimalAndNullDecoder) {
  auto output = execute(values(
      {decimalType(18, 2), decimalType(25, 4)},
      {{decimalLiteral(18, 2, 123), decimalLiteral(25, 4, -7700)},
       {nullLiteral(decimalType(18, 2)), nullLiteral(decimalType(25, 4))}}));
  ASSERT_NE(output, nullptr);
  ASSERT_EQ(output->size(), 2);
  EXPECT_EQ(output->childAt(0)->asFlatVector<int64_t>()->valueAt(0), 123);
  EXPECT_EQ(output->childAt(1)->asFlatVector<int128_t>()->valueAt(0), -7700);
  EXPECT_TRUE(output->childAt(0)->isNullAt(1));
  EXPECT_TRUE(output->childAt(1)->isNullAt(1));
  EXPECT_TRUE(output->childAt(1)->type()->equivalent(*facebook::velox::DECIMAL(25, 4)));
}

TEST_F(SubstraitLiteralExecutionTest, ValuesPreserveTinySmallAndBinaryLiterals) {
  substrait::Type tiny, small, binary_type;
  tiny.mutable_i8()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  small.mutable_i16()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  binary_type.mutable_binary()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  Literal tiny_value, small_value, binary_value;
  tiny_value.set_i8(-7);
  small_value.set_i16(1234);
  binary_value.set_binary(std::string("a\0b", 3));
  auto output = execute(values(
      {tiny, small, binary_type}, {{tiny_value, small_value, binary_value}}));
  ASSERT_NE(output, nullptr);
  EXPECT_EQ(output->childAt(0)->asFlatVector<int8_t>()->valueAt(0), -7);
  EXPECT_EQ(output->childAt(1)->asFlatVector<int16_t>()->valueAt(0), 1234);
  EXPECT_EQ(output->childAt(2)->asFlatVector<facebook::velox::StringView>()
                ->valueAt(0).str(), std::string("a\0b", 3));
}

TEST_F(SubstraitLiteralExecutionTest, ScalarPrimitiveLiteralsUseDeclaredPhysicalTypes) {
  Literal boolean, tiny, small, integer, bigint, real, double_value, text, date, binary_value;
  boolean.set_boolean(true);
  tiny.set_i8(-7);
  small.set_i16(1234);
  integer.set_i32(42);
  bigint.set_i64(12345678901);
  real.set_fp32(1.25f);
  double_value.set_fp64(2.5);
  text.set_string("canonical");
  date.set_date(10);
  binary_value.set_binary(std::string("a\0b", 3));
  auto output = execute(projection({
      expression(boolean), expression(tiny), expression(small), expression(integer),
      expression(bigint), expression(real), expression(double_value), expression(text),
      expression(date), expression(binary_value)}));
  ASSERT_NE(output, nullptr);
  EXPECT_TRUE(output->childAt(0)->asFlatVector<bool>()->valueAt(0));
  EXPECT_EQ(output->childAt(1)->asFlatVector<int8_t>()->valueAt(0), -7);
  EXPECT_EQ(output->childAt(2)->asFlatVector<int16_t>()->valueAt(0), 1234);
  EXPECT_EQ(output->childAt(3)->asFlatVector<int32_t>()->valueAt(0), 42);
  EXPECT_EQ(output->childAt(4)->asFlatVector<int64_t>()->valueAt(0), 12345678901);
  EXPECT_FLOAT_EQ(output->childAt(5)->asFlatVector<float>()->valueAt(0), 1.25f);
  EXPECT_DOUBLE_EQ(output->childAt(6)->asFlatVector<double>()->valueAt(0), 2.5);
  EXPECT_EQ(output->childAt(7)->asFlatVector<facebook::velox::StringView>()
                ->valueAt(0).str(), "canonical");
  EXPECT_TRUE(output->childAt(8)->type()->equivalent(*facebook::velox::DATE()));
  EXPECT_EQ(output->childAt(8)->asFlatVector<int32_t>()->valueAt(0), 10);
  EXPECT_EQ(output->childAt(9)->asFlatVector<facebook::velox::StringView>()
                ->valueAt(0).str(), std::string("a\0b", 3));
}

TEST_F(SubstraitLiteralExecutionTest, TemporalLiteralsRetainUnitsAndNegativeEpoch) {
  Literal timestamp, timestamp_tz, millis, micros;
  timestamp.mutable_precision_timestamp()->set_precision(6);
  timestamp.mutable_precision_timestamp()->set_value(-1);
  timestamp_tz.mutable_precision_timestamp_tz()->set_precision(6);
  timestamp_tz.mutable_precision_timestamp_tz()->set_value(-1000000);
  millis.mutable_precision_time()->set_precision(3);
  millis.mutable_precision_time()->set_value(1234);
  micros.mutable_precision_time()->set_precision(6);
  micros.mutable_precision_time()->set_value(1234567);
  auto output = execute(projection({
      expression(timestamp), expression(timestamp_tz), expression(millis), expression(micros)}));
  ASSERT_NE(output, nullptr);
  EXPECT_EQ(output->childAt(0)->asFlatVector<facebook::velox::Timestamp>()->valueAt(0),
            facebook::velox::Timestamp(-1, 999999000));
  EXPECT_EQ(output->childAt(1)->asFlatVector<facebook::velox::Timestamp>()->valueAt(0),
            facebook::velox::Timestamp(-1, 0));
  EXPECT_TRUE(output->childAt(2)->type()->equivalent(*facebook::velox::TIME()));
  EXPECT_EQ(output->childAt(2)->asFlatVector<int64_t>()->valueAt(0), 1234);
  EXPECT_TRUE(output->childAt(3)->type()->equivalent(*facebook::velox::TIME_MICRO_UTC()));
  EXPECT_EQ(output->childAt(3)->asFlatVector<int64_t>()->valueAt(0), 1234567);
  timestamp.mutable_precision_timestamp()->set_precision(10);
  reject(projection({expression(timestamp)}), "precision must be in [0, 9]");
  millis.mutable_precision_time()->set_precision(7);
  reject(projection({expression(millis)}),
         "Canonical bridge precision_time.precision must be in [0, 6]; actual=7");
}

TEST_F(SubstraitLiteralExecutionTest, StrictDecimalLengthAndTypeParameters) {
  for (int length : {0, 8, 15, 17}) {
    auto literal = decimalLiteral(18, 2, 123);
    literal.mutable_decimal()->set_value(std::string(length, '\0'));
    reject(projection({expression(literal)}), "exactly 16 bytes");
    reject(values({decimalType(18, 2)}, {{literal}}), "exactly 16 bytes");
  }
  for (const auto& [precision, scale, reason] : std::vector<std::tuple<int, int, std::string>>{
           {0, 0, "Canonical bridge decimal.precision must be in [1, 38]; actual=0"},
           {39, 0, "Canonical bridge decimal.precision must be in [1, 38]; actual=39"},
           {256, 0, "Canonical bridge decimal.precision must be in [1, 38]; actual=256"},
           {18, -1, "Canonical bridge decimal.scale must be in [0, 38]; actual=-1"},
           {18, 19, "Canonical bridge decimal parameter constraint failed: (s <= p); precision=18; scale=19"}}) {
    auto literal = decimalLiteral(precision, scale, 0);
    reject(projection({expression(literal)}), reason);
    reject(values({decimalType(precision, scale)}, {}), reason);
  }
}

TEST_F(SubstraitLiteralExecutionTest, DecimalRangeCheckedBeforeNarrowing) {
  for (int precision : {1, 18, 19, 38}) {
    const auto boundary = facebook::velox::DecimalUtil::kPowersOfTen[precision];
    for (auto value : {boundary, -boundary}) {
      reject(projection({expression(decimalLiteral(precision, 0, value))}),
             "exceeds declared precision");
    }
    auto output = execute(projection({
        expression(decimalLiteral(precision, 0, boundary - 1)),
        expression(decimalLiteral(precision, 0, -boundary + 1))}));
    ASSERT_NE(output, nullptr);
    for (int column = 0; column < 2; ++column) {
      const auto expected = column == 0 ? boundary - 1 : -boundary + 1;
      if (precision <= 18) {
        EXPECT_EQ(output->childAt(column)->asFlatVector<int64_t>()->valueAt(0), expected);
      } else {
        EXPECT_EQ(output->childAt(column)->asFlatVector<int128_t>()->valueAt(0), expected);
      }
    }
  }
  const auto maximum = static_cast<int128_t>((static_cast<__uint128_t>(1) << 127) - 1);
  reject(projection({expression(decimalLiteral(38, 0, maximum))}),
         "exceeds declared precision");
  reject(projection({expression(decimalLiteral(38, 0, -maximum - 1))}),
         "exceeds declared precision");
}

TEST_F(SubstraitLiteralExecutionTest, RejectHeterogeneousArraysAndSchemaDrift) {
  Literal array;
  array.mutable_list()->add_values()->CopyFrom(decimalLiteral(10, 2, 123));
  array.mutable_list()->add_values()->CopyFrom(decimalLiteral(10, 3, 1230));
  reject(projection({expression(array)}), "element types differ");
  Literal null_array;
  null_array.mutable_list()->add_values()->CopyFrom(nullLiteral(decimalType(10, 2)));
  null_array.mutable_list()->add_values()->set_fp64(1.0);
  reject(projection({expression(null_array)}), "element types differ");
  reject(values({decimalType(10, 2)}, {{decimalLiteral(10, 3, 1230)}}),
         "differs from schema");
  reject(values({decimalType(10, 2)}, {{nullLiteral(decimalType(10, 3))}}),
         "differs from schema");
}

TEST_F(SubstraitLiteralExecutionTest, RejectGuessedScalarOutputAndUnboundAnchor) {
  auto left = decimalLiteral(10, 2, 123);
  auto right = decimalLiteral(25, 4, 20000);
  reject(projection({binary(left, right, decimalType(35, 5))}, {"multiply"}),
         "output contract mismatch");
  reject(projection({binary(left, right, decimalType(35, 6))}),
         "Unbound canonical function anchor");
  auto unknown = pangrova::test::canonicalTestPlan(
      projection({binary(left, right, decimalType(35, 6))}, {"add"}));
  // Qualify a valid declaration before corrupting its name. The fixture adapter
  // must not intercept this negative production-decoder oracle.
  unknown.mutable_extensions(0)->mutable_extension_function()->set_name("missing_scalar");
  reject(unknown, "Unbound canonical function declaration");
  Literal integer;
  integer.set_i32(2);
  reject(projection({binary(left, integer, decimalType(10, 2))}, {"add"}),
         "no exact scalar signature");
}

TEST_F(SubstraitLiteralExecutionTest, ExplicitCastIsTheOnlyNumericCoercion) {
  Literal integer;
  integer.set_i32(2);
  auto call = binary(decimalLiteral(10, 2, 123), integer, decimalType(11, 2));
  auto* argument = call.mutable_scalar_function()->mutable_arguments(1)->mutable_value();
  auto original = *argument;
  argument->Clear();
  argument->mutable_cast()->mutable_input()->CopyFrom(original);
  argument->mutable_cast()->mutable_type()->CopyFrom(decimalType(10, 2));
  auto output = execute(projection({call}, {"add"}));
  ASSERT_NE(output, nullptr);
  EXPECT_TRUE(output->childAt(0)->type()->equivalent(*facebook::velox::DECIMAL(11, 2)));
  EXPECT_EQ(output->childAt(0)->asFlatVector<int64_t>()->valueAt(0), 323);
}

TEST_F(SubstraitLiteralExecutionTest, RejectCaseBranchPrecisionDrift) {
  substrait::Expression conditional;
  auto* clause = conditional.mutable_if_then()->add_ifs();
  clause->mutable_if_()->mutable_literal()->set_boolean(true);
  clause->mutable_then()->mutable_literal()->CopyFrom(decimalLiteral(10, 2, 123));
  conditional.mutable_if_then()->mutable_else_()->mutable_literal()
      ->CopyFrom(decimalLiteral(10, 3, 1230));
  rejectSpecialFormInput(projection({conditional}));
}

TEST_F(SubstraitLiteralExecutionTest, RejectCaseNonBooleanCondition) {
  substrait::Expression conditional;
  auto* clause = conditional.mutable_if_then()->add_ifs();
  clause->mutable_if_()->mutable_literal()->set_i64(1);
  clause->mutable_then()->mutable_literal()->set_i64(2);
  conditional.mutable_if_then()->mutable_else_()->mutable_literal()->set_i64(3);
  rejectSpecialFormInput(projection({conditional}));
}

TEST_F(SubstraitLiteralExecutionTest, ExplicitArrayCastRunsDistanceWithoutGuessing) {
  Literal left, right;
  left.mutable_list()->add_values()->CopyFrom(decimalLiteral(10, 2, 100));
  left.mutable_list()->add_values()->CopyFrom(decimalLiteral(10, 2, 200));
  right.mutable_list()->add_values()->CopyFrom(decimalLiteral(10, 2, 400));
  right.mutable_list()->add_values()->CopyFrom(decimalLiteral(10, 2, 600));
  substrait::Type output_type;
  output_type.mutable_fp64()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  auto call = binary(left, right, output_type);
  reject(projection({call}, {"l2_distance"}), "no exact scalar signature");
  for (int index = 0; index < 2; ++index) {
    auto* argument = call.mutable_scalar_function()->mutable_arguments(index)
                         ->mutable_value();
    auto original = *argument;
    argument->Clear();
    argument->mutable_cast()->mutable_input()->CopyFrom(original);
    auto* list = argument->mutable_cast()->mutable_type()->mutable_list();
    list->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
    auto* element = list->mutable_type();
    if (index == 0) {
      element->mutable_fp32()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
    } else {
      element->mutable_fp64()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
    }
  }
  auto output = execute(projection({call}, {"l2_distance"}));
  ASSERT_NE(output, nullptr);
  EXPECT_TRUE(output->childAt(0)->type()->equivalent(*facebook::velox::DOUBLE()));
  EXPECT_DOUBLE_EQ(output->childAt(0)->asFlatVector<double>()->valueAt(0), 5.0);
}

TEST_F(SubstraitLiteralExecutionTest, RejectUnusedDefaultsAndMalformedValues) {
  Literal tiny, small, missing;
  tiny.set_i8(128);
  small.set_i16(-32769);
  reject(projection({expression(tiny)}), "8-bit range");
  reject(projection({expression(small)}), "16-bit range");
  reject(projection({expression(missing)}), "Unsupported Substrait literal");
  reject(values({}, {{decimalLiteral(10, 2, 123)}}), "row width");
  auto plan = values({decimalType(10, 2)}, {{decimalLiteral(10, 2, 123)}});
  auto* table = plan.mutable_relations(0)->mutable_root()->mutable_input()
                    ->mutable_read()->mutable_virtual_table();
  table->mutable_expressions(0)->mutable_fields(0)->Clear();
  table->mutable_expressions(0)->mutable_fields(0)->mutable_selection();
  reject(plan, "requires literal fields");
  // Build deliberately noncanonical input using public reflection, without
  // invoking a deprecated generated accessor or suppressing diagnostics.
  const auto* legacy_field = table->GetDescriptor()->FindFieldByName("values");
  ASSERT_NE(legacy_field, nullptr);
  auto* legacy_row = table->GetReflection()->AddMessage(table, legacy_field);
  const auto* fields = legacy_row->GetDescriptor()->FindFieldByName("fields");
  ASSERT_NE(fields, nullptr);
  legacy_row->GetReflection()->AddMessage(legacy_row, fields)
      ->CopyFrom(decimalLiteral(10, 2, 123));
  reject(plan, "deprecated values encoding is forbidden");
  table->clear_expressions();
  reject(plan, "deprecated values encoding is forbidden");
}

TEST_F(SubstraitLiteralExecutionTest, ExplicitEmptySchemaAndMissingSchemaContracts) {
  auto empty = values({}, {{}});
  auto output = execute(empty);
  ASSERT_NE(output, nullptr);
  EXPECT_EQ(output->size(), 1);
  EXPECT_EQ(output->childrenSize(), 0);
  empty.mutable_relations(0)->mutable_root()->mutable_input()
      ->mutable_read()->clear_base_schema();
  reject(empty, "requires an explicit base_schema struct");
  auto invalid = values({decimalType(10, 2)}, {{decimalLiteral(10, 2, 123)}});
  invalid.mutable_relations(0)->mutable_root()->mutable_input()
      ->mutable_read()->mutable_base_schema()->clear_struct_();
  reject(invalid, "requires an explicit base_schema struct");
}

}  // namespace
}  // namespace pangrova::velox::test
