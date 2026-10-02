#include "FunctionResolverOracle.h"

#include <algorithm>
#include <cctype>
#include <stdexcept>
#include <string>
#include <vector>

#include "google/protobuf/struct.pb.h"
#include "google/protobuf/util/json_util.h"
#include "velox/expression/SignatureBinder.h"
#include "velox/functions/FunctionRegistry.h"
#include "velox/type/Type.h"

namespace pangrova::capability {
namespace {

using Object = google::protobuf::Struct;
using TypePtr = facebook::velox::TypePtr;

std::string lower(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(),
                 [](unsigned char character) { return std::tolower(character); });
  return value;
}

void text(Object& object, const char* name, const std::string& value) {
  (*object.mutable_fields())[name].set_string_value(value);
}

Object concreteType(const TypePtr& source) {
  Object result;
  text(result, "base", lower(source->name()));
  (*result.mutable_fields())["variableArity"].set_bool_value(false);
  auto* parameters = (*result.mutable_fields())["parameters"].mutable_list_value();
  for (const auto& parameter : source->parameters()) {
    Object child;
    if (parameter.kind == facebook::velox::TypeParameterKind::kType) {
      child = concreteType(parameter.type);
      if (parameter.rowFieldName) {
        text(child, "rowFieldName", *parameter.rowFieldName);
      }
    } else if (parameter.kind == facebook::velox::TypeParameterKind::kLongLiteral) {
      text(child, "base", std::to_string(*parameter.longLiteral));
      (*child.mutable_fields())["variableArity"].set_bool_value(false);
      (*child.mutable_fields())["parameters"].mutable_list_value();
    } else {
      throw std::runtime_error("Resolver oracle requires explicit enum-parameter projection");
    }
    *parameters->add_values()->mutable_struct_value() = std::move(child);
  }
  return result;
}

void resolvedType(Object& result, const TypePtr& type) {
  text(result, "status", type ? "BOUND" : "UNBOUND");
  if (type) {
    *(*result.mutable_fields())["output"].mutable_struct_value() = concreteType(type);
    (*result.mutable_fields())["known"].set_bool_value(!type->isUnknown());
    (*result.mutable_fields())["orderable"].set_bool_value(type->isOrderable());
    (*result.mutable_fields())["comparable"].set_bool_value(type->isComparable());
  }
}

template <typename Resolver>
void captureResolution(Object& result, Resolver resolver) {
  try {
    resolvedType(result, resolver());
  } catch (const std::exception& error) {
    // This is a diagnostic oracle outcome, not a production execution
    // fallback. Invalid registered output parameters must remain visible.
    text(result, "status", "ERROR");
    text(result, "error", error.what());
  }
}

std::vector<TypePtr> argumentTypes(const Object& request) {
  const auto found = request.fields().find("arguments");
  if (found == request.fields().end() || !found->second.has_list_value()) {
    throw std::runtime_error("Resolver oracle arguments must be an array");
  }
  const auto& values = found->second.list_value().values();
  if (values.size() > 256) {
    throw std::runtime_error("Resolver oracle argument bound exceeded");
  }
  std::vector<TypePtr> result;
  for (const auto& value : values) {
    if (!value.has_string_value() || value.string_value().size() > 65536) {
      throw std::runtime_error("Resolver oracle argument must be a registered type signature");
    }
    auto signature = facebook::velox::exec::parseTypeSignature(value.string_value());
    auto type = facebook::velox::exec::SignatureBinder::tryResolveType(signature, {}, {});
    if (!type) {
      throw std::runtime_error("Resolver oracle argument is not a concrete registered type");
    }
    result.push_back(std::move(type));
  }
  return result;
}

}  // namespace

std::string resolveRegisteredScalarOracle(const std::string& request_json) {
  if (request_json.size() > 1024 * 1024) {
    throw std::runtime_error("Resolver oracle request byte bound exceeded");
  }
  Object request;
  const auto parsed = google::protobuf::util::JsonStringToMessage(request_json, &request);
  const auto name = request.fields().find("name");
  if (!parsed.ok() || request.fields_size() != 2 || name == request.fields().end() ||
      !name->second.has_string_value() || name->second.string_value().empty()) {
    throw std::runtime_error("Resolver oracle requires exactly name and arguments");
  }
  const auto& function_name = name->second.string_value();
  const auto args = argumentTypes(request);
  Object result;
  text(result, "name", function_name);
  captureResolution(result, [&]() {
    return facebook::velox::resolveFunctionOrCallableSpecialForm(function_name, args);
  });
  auto* candidates = (*result.mutable_fields())["candidates"].mutable_list_value();
  for (const auto* signature : facebook::velox::getFunctionSignatures(function_name)) {
    auto* candidate = candidates->add_values()->mutable_struct_value();
    facebook::velox::exec::SignatureBinder binder(*signature, args);
    captureResolution(*candidate, [&]() {
      return binder.tryBind() ? binder.tryResolveReturnType() : nullptr;
    });
  }
  std::string json;
  const auto encoded = google::protobuf::util::MessageToJsonString(result, &json);
  if (!encoded.ok()) {
    throw std::runtime_error("Resolver oracle JSON encoding failed: " + encoded.ToString());
  }
  return json;
}

}  // namespace pangrova::capability
