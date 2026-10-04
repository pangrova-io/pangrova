#include "CanonicalTypeDescriptorExport.h"

#include <map>
#include <stdexcept>
#include <string>

#include "google/protobuf/descriptor.h"
#include "google/protobuf/struct.pb.h"
#include "google/protobuf/util/json_util.h"
#include "substrait/type.pb.h"
#include "CanonicalPhysicalTypeProjection.h"

namespace pangrova::capability {
namespace {

using Descriptor = google::protobuf::Descriptor;
using Object = google::protobuf::Struct;

void stringField(Object& object, const char* name, const std::string& value) {
  (*object.mutable_fields())[name].set_string_value(value);
}

void collect(const Descriptor* descriptor,
             std::map<std::string, const Descriptor*>& reachable) {
  if (!reachable.emplace(descriptor->full_name(), descriptor).second) {
    return;
  }
  for (int index = 0; index < descriptor->field_count(); ++index) {
    const auto* field = descriptor->field(index);
    if (field->message_type()) {
      collect(field->message_type(), reachable);
    }
  }
}

Object fieldDescriptor(const google::protobuf::FieldDescriptor& field) {
  Object result;
  stringField(result, "name", field.name());
  stringField(result, "type", field.type_name());
  (*result.mutable_fields())["number"].set_number_value(field.number());
  (*result.mutable_fields())["repeated"].set_bool_value(field.is_repeated());
  if (field.message_type()) {
    stringField(result, "message", field.message_type()->full_name());
  }
  if (field.containing_oneof()) {
    stringField(result, "oneof", field.containing_oneof()->name());
  }
  if (field.enum_type()) {
    auto* values = (*result.mutable_fields())["enumValues"].mutable_list_value();
    for (int index = 0; index < field.enum_type()->value_count(); ++index) {
      values->add_values()->set_number_value(field.enum_type()->value(index)->number());
    }
  }
  return result;
}

}  // namespace

std::string exportCanonicalTypeDescriptor() {
  const auto* root = substrait::Type::descriptor();
  Object result;
  stringField(result, "root", root->full_name());
  std::map<std::string, const Descriptor*> reachable;
  collect(root, reachable);
  auto* messages = (*result.mutable_fields())["messages"].mutable_list_value();
  for (const auto& [name, descriptor] : reachable) {
    auto* message = messages->add_values()->mutable_struct_value();
    stringField(*message, "name", name);
    auto* fields = (*message->mutable_fields())["fields"].mutable_list_value();
    for (int index = 0; index < descriptor->field_count(); ++index) {
      *fields->add_values()->mutable_struct_value() = fieldDescriptor(*descriptor->field(index));
    }
  }
  std::string json;
  const auto status = google::protobuf::util::MessageToJsonString(result, &json);
  if (!status.ok()) {
    throw std::runtime_error("Canonical type descriptor JSON encoding failed: " + status.ToString());
  }
  return json;
}

std::string exportCanonicalPhysicalTypeProjections() {
  pangrova::protocol::v2::FunctionCatalogManifest bridge;
  generated::appendCanonicalPhysicalProjections(&bridge);
  std::string json;
  const auto status = google::protobuf::util::MessageToJsonString(bridge, &json);
  if (!status.ok()) {
    throw std::runtime_error("Canonical type projection JSON encoding failed: " + status.ToString());
  }
  return json;
}

}  // namespace pangrova::capability
