#include "RegisteredFunctionExport.h"

#include <algorithm>
#include <map>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "google/protobuf/struct.pb.h"
#include "google/protobuf/util/json_util.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/WindowFunction.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/expression/SpecialFormRegistry.h"
#include "velox/expression/SimpleFunctionRegistry.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/FunctionRegistry.h"
#include "velox/window/DistinctAggregateWindow.h"

namespace pangrova::capability {
namespace {

using Object = google::protobuf::Struct;
using Signature = facebook::velox::exec::FunctionSignature;
using TypeSignature = facebook::velox::exec::TypeSignature;

void text(Object& target, const std::string& name, const std::string& value) {
  (*target.mutable_fields())[name].set_string_value(value);
}

void boolean(Object& target, const std::string& name, bool value) {
  (*target.mutable_fields())[name].set_bool_value(value);
}

Object type(const TypeSignature& signature) {
  Object result;
  text(result, "base", signature.baseName());
  boolean(result, "variableArity", signature.variableArity());
  if (signature.rowFieldName()) {
    text(result, "rowFieldName", *signature.rowFieldName());
  }
  auto* parameters = (*result.mutable_fields())["parameters"].mutable_list_value();
  for (const auto& parameter : signature.parameters()) {
    *parameters->add_values()->mutable_struct_value() = type(parameter);
  }
  return result;
}

Object variable(const facebook::velox::exec::SignatureVariable& source) {
  Object result;
  text(result, "name", source.name());
  text(result, "constraint", source.constraint());
  if (source.isTypeParameter()) {
    text(result, "kind", "TYPE");
    boolean(result, "knownTypesOnly", source.knownTypesOnly());
    boolean(result, "orderableTypesOnly", source.orderableTypesOnly());
    boolean(result, "comparableTypesOnly", source.comparableTypesOnly());
  } else {
    text(result, "kind", source.isIntegerParameter() ? "INTEGER" : "ENUM");
  }
  return result;
}

Object signature(const Signature& source) {
  Object result;
  *(*result.mutable_fields())["output"].mutable_struct_value() = type(source.returnType());
  boolean(result, "variableArity", source.variableArity());
  auto* args = (*result.mutable_fields())["arguments"].mutable_list_value();
  for (const auto& argument : source.argumentTypes()) {
    *args->add_values()->mutable_struct_value() = type(argument);
  }
  auto* constants = (*result.mutable_fields())["constantArguments"].mutable_list_value();
  for (bool constant : source.constantArguments()) {
    constants->add_values()->set_bool_value(constant);
  }
  std::map<std::string, const facebook::velox::exec::SignatureVariable*> sorted;
  for (const auto& [name, entry] : source.variables()) {
    sorted.emplace(name, &entry);
  }
  auto* variables = (*result.mutable_fields())["variables"].mutable_list_value();
  for (const auto& [name, entry] : sorted) {
    (void)name;
    *variables->add_values()->mutable_struct_value() = variable(*entry);
  }
  return result;
}

template <typename SignatureCollection>
Object function(const std::string& name, const char* kind,
                const SignatureCollection& signatures) {
  Object result;
  text(result, "name", name);
  text(result, "kind", kind);
  // Aggregate/window resolution traverses this exact registered vector.
  // Scalar signatures alone do not expose SimpleFunction metadata priority
  // and physical implementation filters. A consumer must not invent ordering.
  text(result, "resolutionPolicy", std::string(kind) == "SCALAR"
      ? "SCALAR_RUNTIME_PRIORITY" : "ORDERED_FIRST_MATCH");
  auto* overloads = (*result.mutable_fields())["overloads"].mutable_list_value();
  for (const auto& entry : signatures) {
    auto exported = signature(*entry);
    if (const auto* aggregate = dynamic_cast<
            const facebook::velox::exec::AggregateFunctionSignature*>(entry)) {
      *(*exported.mutable_fields())["intermediate"].mutable_struct_value() =
          type(aggregate->intermediateType());
    }
    *overloads->add_values()->mutable_struct_value() = std::move(exported);
  }
  return result;
}

template <typename SignatureCollection>
std::vector<const Signature*> pointers(const SignatureCollection& source) {
  std::vector<const Signature*> result;
  for (const auto& entry : source) {
    if constexpr (std::is_pointer_v<std::decay_t<decltype(entry)>>) {
      result.push_back(entry);
    } else {
      result.push_back(entry.get());
    }
  }
  return result;
}

void scalarMetadata(Object& target, const std::string& name) {
  auto deterministic = facebook::velox::isDeterministic(name);
  auto default_null = facebook::velox::isDefaultNullBehavior(name);
  if (deterministic) {
    boolean(target, "deterministic", *deterministic);
  }
  if (default_null) {
    boolean(target, "defaultNullBehavior", *default_null);
  }
}

Object physicalLayout(const facebook::velox::TypePtr& type) {
  Object result;
  // These are diagnostic execution layout tags, not logical SQL types. The
  // canonical manifest compiler must project through the sole type bridge.
  text(result, "kind", std::string(facebook::velox::TypeKindName::toName(type->kind())));
  auto* children = (*result.mutable_fields())["children"].mutable_list_value();
  for (size_t index = 0; index < type->size(); ++index) {
    *children->add_values()->mutable_struct_value() = physicalLayout(type->childAt(index));
  }
  return result;
}

void scalarImplementations(Object& target, const std::string& name) {
  auto* simple = (*target.mutable_fields())["simpleImplementations"].mutable_list_value();
  for (const auto& metadata : facebook::velox::exec::simpleFunctions().getFunctionMetadataSnapshot(name)) {
    auto* entry = simple->add_values()->mutable_struct_value();
    *(*entry->mutable_fields())["signature"].mutable_struct_value() = signature(*metadata->signature());
    (*entry->mutable_fields())["priority"].set_number_value(metadata->priority());
    boolean(*entry, "deterministic", metadata->isDeterministic());
    boolean(*entry, "defaultNullBehavior", metadata->defaultNullBehavior());
    *(*entry->mutable_fields())["outputPhysical"].mutable_struct_value() = physicalLayout(metadata->resultPhysicalType());
    auto* arguments = (*entry->mutable_fields())["argumentsPhysical"].mutable_list_value();
    for (const auto& argument : metadata->argPhysicalTypes()) {
      *arguments->add_values()->mutable_struct_value() = physicalLayout(argument);
    }
  }
  auto* vector = (*target.mutable_fields())["vectorOverloads"].mutable_list_value();
  facebook::velox::exec::vectorFunctionFactories().withRLock([&](const auto& entries) {
    const auto found = entries.find(name);
    if (found == entries.end()) return;
    for (const auto& entry : found->second.signatures) {
      *vector->add_values()->mutable_struct_value() = signature(*entry);
    }
  });
}

}  // namespace

std::string exportRegisteredFunctions() {
  Object result;
  (*result.mutable_fields())["schemaVersion"].set_number_value(3);
  std::map<std::pair<std::string, std::string>, Object> functions;
  for (const auto& [name, entries] : facebook::velox::getFunctionSignatures()) {
    auto exported = function(name, "SCALAR", pointers(entries));
    scalarMetadata(exported, name);
    scalarImplementations(exported, name);
    functions.emplace(std::make_pair("SCALAR", name), std::move(exported));
  }
  for (const auto& [name, entries] : facebook::velox::exec::getAggregateFunctionSignatures()) {
    functions.emplace(std::make_pair("AGGREGATE", name),
                      function(name, "AGGREGATE", pointers(entries)));
  }
  for (const auto& [name, entry] : facebook::velox::exec::windowFunctions()) {
    auto exported = function(name, "WINDOW", pointers(entry.signatures));
    boolean(exported, "aggregateWindow", entry.metadata.isAggregate);
    text(exported, "windowProcessMode",
         entry.metadata.processMode == facebook::velox::exec::WindowFunction::ProcessMode::kRows
             ? "ROWS" : "PARTITION");
    if (entry.metadata.isAggregate && facebook::velox::exec::getAggregateFunctionEntry(name)) {
      const auto distinct = pangrova::velox::distinctAggregateWindowName(name);
      if (facebook::velox::exec::getWindowFunctionSignatures(distinct)) {
        // This is an actual registered execution dependency, not proof that
        // the SQL manifest or any distributed value oracle has passed.
        text(exported, "distinctImplementation", distinct);
      }
    }
    functions.emplace(std::make_pair("WINDOW", name), std::move(exported));
  }
  auto* list = (*result.mutable_fields())["functions"].mutable_list_value();
  for (const auto& [identity, entry] : functions) {
    (void)identity;
    *list->add_values()->mutable_struct_value() = entry;
  }
  auto names = facebook::velox::exec::specialFormRegistry().getSpecialFormNames();
  std::sort(names.begin(), names.end());
  auto* special = (*result.mutable_fields())["specialForms"].mutable_list_value();
  for (const auto& name : names) {
    special->add_values()->set_string_value(name);
  }
  std::string json;
  const auto status = google::protobuf::util::MessageToJsonString(result, &json);
  if (!status.ok()) {
    throw std::runtime_error("Registered function export JSON encoding failed: " + status.ToString());
  }
  return json;
}

}  // namespace pangrova::capability
