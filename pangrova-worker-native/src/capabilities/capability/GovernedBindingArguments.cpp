#include "capabilities/capability/GovernedBindingArguments.h"
#include "StructuralContractIdentities.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <iomanip>
#include <limits>
#include <locale>
#include <sstream>
#include <stdexcept>

#include <openssl/evp.h>

namespace pangrova::capability {
namespace {

using google::protobuf::FieldDescriptor;
using google::protobuf::Message;

std::string base64(const std::string& bytes) {
  if (bytes.size() != 32) throw std::invalid_argument("Governed binding SHA-256 must have 32 bytes");
  // EVP writes a trailing NUL in addition to the 44 encoded bytes.
  std::string encoded(45, '\0');
  const auto size = EVP_EncodeBlock(reinterpret_cast<unsigned char*>(encoded.data()),
      reinterpret_cast<const unsigned char*>(bytes.data()), static_cast<int>(bytes.size()));
  if (size != 44) throw std::runtime_error("Cannot encode exact binding SHA-256");
  encoded.resize(44);
  return encoded;
}

std::string exactDouble(double value) {
  if (!std::isfinite(value)) throw std::invalid_argument("Binding floating value must be finite");
  std::ostringstream encoded;
  encoded.imbue(std::locale::classic());
  encoded << std::setprecision(std::numeric_limits<double>::max_digits10) << value;
  if (!encoded) throw std::runtime_error("Cannot encode exact binding floating value");
  return encoded.str();
}

std::string encodeField(const Message& message, const FieldDescriptor& field, std::string_view encoding) {
  const auto* reflection = message.GetReflection();
  if ((encoding == "STRING" || encoding == "BASE64") && field.cpp_type() == FieldDescriptor::CPPTYPE_STRING) {
    const auto value = reflection->GetString(message, &field);
    if (encoding == "BASE64" && field.type() == FieldDescriptor::TYPE_BYTES) return base64(value);
    if (encoding == "STRING" && field.type() == FieldDescriptor::TYPE_STRING && !value.empty() &&
        !std::all_of(value.begin(), value.end(), [](unsigned char character) { return std::isspace(character); })) return value;
  }
  if (encoding == "UNSIGNED_DECIMAL") {
    if (field.cpp_type() == FieldDescriptor::CPPTYPE_UINT32) {
      const auto value = reflection->GetUInt32(message, &field);
      if (value > static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) {
        throw std::invalid_argument("Governed binding uint32 exceeds Java accounting domain");
      }
      return std::to_string(value);
    }
    if (field.cpp_type() == FieldDescriptor::CPPTYPE_UINT64) {
      const auto value = reflection->GetUInt64(message, &field);
      if (value > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        throw std::invalid_argument("Governed binding uint64 exceeds Java accounting domain");
      }
      return std::to_string(value);
    }
  }
  if (encoding == "BOOLEAN" && field.cpp_type() == FieldDescriptor::CPPTYPE_BOOL) {
    return reflection->GetBool(message, &field) ? "true" : "false";
  }
  if (encoding == "EXACT_FLOAT" && field.cpp_type() == FieldDescriptor::CPPTYPE_DOUBLE) {
    return exactDouble(reflection->GetDouble(message, &field));
  }
  throw std::invalid_argument("Governed binding field type differs from its declared encoding: " + field.full_name());
}

std::string encodeBindingPath(const protocol::v2::OperatorBinding& binding, const generated::GovernedArgument& argument) {
  const Message* current = &binding;
  for (size_t ordinal = 0; ordinal < argument.pathSize; ++ordinal) {
    if (current->GetReflection()->GetUnknownFields(*current).field_count() != 0) {
      throw std::invalid_argument("Governed binding authority contains unknown fields");
    }
    const auto* field = current->GetDescriptor()->FindFieldByName(argument.path[ordinal]);
    if (!field || field->is_repeated()) throw std::invalid_argument("Unknown or repeated governed binding path");
    if (ordinal + 1 == argument.pathSize) return encodeField(*current, *field, argument.encoding);
    if (field->cpp_type() != FieldDescriptor::CPPTYPE_MESSAGE ||
        (!argument.allowAbsentMessage && !current->GetReflection()->HasField(*current, field))) {
      throw std::invalid_argument("Governed binding required message is absent or has a wrong type");
    }
    current = &current->GetReflection()->GetMessage(*current, field);
  }
  throw std::invalid_argument("Governed argument requires a nonempty exact binding path");
}

}  // namespace

std::string governedBindingArgument(const generated::GovernedArgument& argument,
    const protocol::v2::OperatorBinding& binding, std::string_view invocation_id,
    uint32_t output_vector_dimension) {
  const std::string_view source(argument.source);
  if (source == "OPERATOR_BINDING") return encodeBindingPath(binding, argument);
  if (source == "PLANNER_INVOCATION_ID" && !invocation_id.empty()) return std::string(invocation_id);
  if (source == "OUTPUT_VECTOR_DIMENSION") return std::to_string(output_vector_dimension);
  throw std::invalid_argument("Unknown or absent constant governed argument source");
}

std::vector<std::string> governedBindingArguments(
    std::string_view layout, const protocol::v2::OperatorBinding& binding,
    std::string_view invocation_id, uint32_t output_vector_dimension) {
  std::vector<std::string> result;
  for (const auto& argument : generated::governedLayout(layout)) {
    result.push_back(governedBindingArgument(argument, binding, invocation_id, output_vector_dimension));
  }
  return result;
}

}  // namespace pangrova::capability
