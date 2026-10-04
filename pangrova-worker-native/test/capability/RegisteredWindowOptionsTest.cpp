#include <gtest/gtest.h>

#include <initializer_list>
#include <stdexcept>

#include "capability/RegisteredWindowOptions.h"

namespace pangrova::capability::test {
namespace {
using Options = google::protobuf::RepeatedPtrField<substrait::FunctionOption>;

Options treatment(std::initializer_list<const char*> preferences) {
  Options options;
  auto* option = options.Add();
  option->set_name("null_treatment");
  for (const auto* preference : preferences) option->add_preference(preference);
  return options;
}

TEST(RegisteredWindowOptionsTest, DeclaredNullTreatmentUsesOrderedPreferences) {
  EXPECT_FALSE(registeredWindowIgnoreNulls("first_value", {}));
  EXPECT_TRUE(registeredWindowIgnoreNulls("first_value", treatment({"IGNORE_NULLS"})));
  EXPECT_FALSE(registeredWindowIgnoreNulls("first_value", treatment({"RESPECT_NULLS"})));
  EXPECT_FALSE(registeredWindowIgnoreNulls("lead", treatment({"RESPECT_NULLS", "IGNORE_NULLS"})));
  EXPECT_TRUE(registeredWindowIgnoreNulls("lag", treatment({"not_supported", "IGNORE_NULLS"})));
}

TEST(RegisteredWindowOptionsTest, UnknownFunctionOptionOrUnusablePreferencesFailClosed) {
  EXPECT_THROW(registeredWindowIgnoreNulls("sum", treatment({"IGNORE_NULLS"})), std::runtime_error);
  EXPECT_THROW(registeredWindowIgnoreNulls("first_value", treatment({"ignore_nulls"})), std::runtime_error);
  EXPECT_THROW(registeredWindowIgnoreNulls("first_value", treatment({})), std::runtime_error);
  auto unknown = treatment({"IGNORE_NULLS"});
  unknown.Mutable(0)->set_name("unregistered_option");
  EXPECT_THROW(registeredWindowIgnoreNulls("first_value", unknown), std::runtime_error);
}

TEST(RegisteredWindowOptionsTest, DuplicateOrUnknownWireFieldsAreNotSilentlyIgnored) {
  auto options = treatment({"IGNORE_NULLS"});
  options.Add()->CopyFrom(options.Get(0));
  EXPECT_THROW(registeredWindowIgnoreNulls("first_value", options), std::runtime_error);
  options.RemoveLast();
  options.Mutable(0)->GetReflection()->MutableUnknownFields(options.Mutable(0))->AddVarint(999, 1);
  EXPECT_THROW(registeredWindowIgnoreNulls("first_value", options), std::runtime_error);
}
}  // namespace
}  // namespace pangrova::capability::test
