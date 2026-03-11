/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "pandaproxy/schema_registry/compatibility.h"

namespace pandaproxy::schema_registry {

std::string_view description_for_type(avro_incompatibility_type t) {
    switch (t) {
    case avro_incompatibility_type::name_mismatch:
        return "The name of the schema has changed (path '{path}')";
    case avro_incompatibility_type::fixed_size_mismatch:
        return "The size of FIXED type field at path '{path}' in the "
               "{{reader}} schema does not match with the {{writer}} schema";
    case avro_incompatibility_type::missing_enum_symbols:
        return "The {{reader}} schema is missing enum symbols '{additional}' "
               "at path '{path}' in the {{writer}} schema";
    case avro_incompatibility_type::reader_field_missing_default_value:
        return "The field '{additional}' at path '{path}' in the {{reader}} "
               "schema has "
               "no default value and is missing in the {{writer}} schema";
    case avro_incompatibility_type::type_mismatch:
        return "The type (path '{path}') of a field in the {{reader}} schema "
               "does not match with the {{writer}} schema";
    case avro_incompatibility_type::missing_union_branch:
        return "The {{reader}} schema is missing a type inside a union field "
               "at path '{path}' in the {{writer}} schema";
    case avro_incompatibility_type::unknown:
        return "{{reader}} schema is not compatible with {{writer}} schema: "
               "check '{path}'";
    };
    __builtin_unreachable();
}

fmt::iterator avro_incompatibility::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{errorType:'{}', description:'{}', additionalInfo:'{}'}}",
      format_as(_type),
      describe(),
      _additional_info);
}

ss::sstring avro_incompatibility::describe() const {
    return fmt::format(
      fmt::runtime(description_for_type(_type)),
      fmt::arg("path", _path.string()),
      fmt::arg("additional", _additional_info));
}

std::string_view description_for_type(proto_incompatibility_type t) {
    switch (t) {
    case proto_incompatibility_type::message_removed:
        return "The {{reader}} schema is missing a field of type MESSAGE at "
               "path '{path}' in the {{writer}} schema";
    case proto_incompatibility_type::field_kind_changed:
        return "The type of a field at path '{path}' in the {{reader}} "
               "schema does  not match the {{writer}} schema";
    case proto_incompatibility_type::field_scalar_kind_changed:
        return "The kind of a SCALAR field at path '{path}' in the {{reader}} "
               "schema does not match its kind in the {{writer}} schema";
    case proto_incompatibility_type::field_named_type_changed:
        return "The type of a MESSAGE field at path '{path}' in the {{reader}} "
               "schema does not match its type in the {{writer}} schema ";
    case proto_incompatibility_type::required_field_added:
        return "A required field  at path '{path}' in the {{reader}} schema "
               "is missing in the {{writer}} schema";
    case proto_incompatibility_type::required_field_removed:
        return "The {{reader}} schema is missing a required field at path: "
               "'{path}' in the {{writer}} schema";
    case proto_incompatibility_type::oneof_field_removed:
        return "The {{reader}} schema is missing a oneof field at path "
               "'{path}' in the {{writer}} schema";
    case proto_incompatibility_type::multiple_fields_moved_to_oneof:
        return "Multiple fields in the oneof at path '{path}' in the "
               "{{reader}} schema are outside a oneof in the {{writer}} "
               "schema ";
    case proto_incompatibility_type::unknown:
        return "{{reader}} schema is not compatible with {{writer}} schema: "
               "check '{path}'";
    }
    __builtin_unreachable();
}

fmt::iterator proto_incompatibility::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      R"({{errorType:"{}", description:"{}"}})",
      format_as(_type),
      describe());
}

ss::sstring proto_incompatibility::describe() const {
    return fmt::format(
      fmt::runtime(description_for_type(_type)),
      fmt::arg("path", _path.string()));
}

std::string_view description_for_type(json_incompatibility_type t) {
    switch (t) {
    case json_incompatibility_type::maximum_added:
    case json_incompatibility_type::minimum_added:
    case json_incompatibility_type::exclusive_maximum_added:
    case json_incompatibility_type::exclusive_minimum_added:
    case json_incompatibility_type::multiple_of_added:
    case json_incompatibility_type::max_length_added:
    case json_incompatibility_type::min_length_added:
    case json_incompatibility_type::pattern_added:
    case json_incompatibility_type::required_attribute_added:
    case json_incompatibility_type::max_properties_added:
    case json_incompatibility_type::min_properties_added:
    case json_incompatibility_type::dependency_array_added:
    case json_incompatibility_type::dependency_schema_added:
    case json_incompatibility_type::max_items_added:
    case json_incompatibility_type::min_items_added:
    case json_incompatibility_type::unique_items_added:
    case json_incompatibility_type::additional_items_removed:
    case json_incompatibility_type::additional_properties_removed:
        return "The keyword at path '{path}' in the {{reader}} schema is not "
               "present in the {{writer}} schema";
    case json_incompatibility_type::min_length_increased:
    case json_incompatibility_type::minimum_increased:
    case json_incompatibility_type::exclusive_minimum_increased:
    case json_incompatibility_type::min_properties_increased:
    case json_incompatibility_type::multiple_of_expanded:
    case json_incompatibility_type::min_items_increased:
        return "The value at path '{path}' in the {{reader}} schema is more "
               "than its value in the {{writer}} schema";
    case json_incompatibility_type::max_length_decreased:
    case json_incompatibility_type::maximum_decreased:
    case json_incompatibility_type::max_items_decreased:
    case json_incompatibility_type::exclusive_maximum_decreased:
    case json_incompatibility_type::max_properties_decreased:
        return "The value at path '{path}' in the {{reader}} schema is less "
               "than its value in the {{writer}} schema";
    case json_incompatibility_type::additional_items_narrowed:
    case json_incompatibility_type::enum_array_narrowed:
    case json_incompatibility_type::sum_type_narrowed:
    case json_incompatibility_type::additional_properties_narrowed:
        return "An array or combined type at path '{path}' has fewer elements "
               "in the {{reader}} schema than the {{writer}} schema";
    case json_incompatibility_type::pattern_changed:
    case json_incompatibility_type::multiple_of_changed:
    case json_incompatibility_type::dependency_array_changed:
        return "The value at path '{path}' is different between the {{reader}} "
               "and {{writer}} schema";
    case json_incompatibility_type::type_changed:
    case json_incompatibility_type::type_narrowed:
    case json_incompatibility_type::combined_type_changed:
    case json_incompatibility_type::combined_type_subschemas_changed:
    case json_incompatibility_type::enum_array_changed:
        return "A type at path '{path}' is different between the {{reader}} "
               "schema and the {{writer}} schema";
    case json_incompatibility_type::dependency_array_extended:
    case json_incompatibility_type::product_type_extended:
    case json_incompatibility_type::sum_type_extended:
    case json_incompatibility_type::not_type_extended:
        return "An array or combined type at path '{path}' has more elements "
               "in the {{reader}} schema than the {{writer}} schema";
    case json_incompatibility_type::property_added_to_open_content_model:
    case json_incompatibility_type::item_added_to_open_content_model:
        return "The {{reader}} schema has an open content model and has a "
               "property or item at path '{path}' which is missing in the "
               "{{writer}} schema";
    case json_incompatibility_type::
      required_property_added_to_unopen_content_model:
        return "The {{reader}} schema has an unopen content model and has a "
               "required property at path '{path}' which is missing in the "
               "{{writer}} schema";
    case json_incompatibility_type::property_removed_from_closed_content_model:
    case json_incompatibility_type::item_removed_from_closed_content_model:
        return "The {{reader}} has a closed content model and is missing a "
               "property or item present at path '{path}' in the {{writer}} "
               "schema";
    case json_incompatibility_type::
      property_removed_not_covered_by_partially_open_content_model:
    case json_incompatibility_type::
      item_removed_not_covered_by_partially_open_content_model:
        return "A property or item is missing in the {{reader}} schema but "
               "present at path '{path}' in the {{writer}} schema and is not "
               "covered by its partially open content model";
    case json_incompatibility_type::
      property_added_not_covered_by_partially_open_content_model:
    case json_incompatibility_type::
      item_added_not_covered_by_partially_open_content_model:
        return "The {{reader}} schema has a property or item at path '{path}' "
               "which is missing in the {{writer}} schema and is not covered "
               "by its partially open content model";
    case json_incompatibility_type::reserved_property_removed:
        return "The {{reader}} schema has reserved property '{path}' removed "
               "from its metadata which is present in the {{writer}} schema.";
    case json_incompatibility_type::reserved_property_conflicts_with_property:
        return "The {{reader}} schema has property at path '{path}' that "
               "conflicts with the reserved properties which is missing in the "
               "{{writer}} schema.";

    case json_incompatibility_type::unknown:
        return "{{reader}} schema is not compatible with {{writer}} schema: "
               "check '{path}'";
    }
    __builtin_unreachable();
}

fmt::iterator json_incompatibility::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      R"({{errorType:"{}", description:"{}"}})",
      format_as(_type),
      describe());
}

ss::sstring json_incompatibility::describe() const {
    return fmt::format(
      fmt::runtime(description_for_type(_type)),
      fmt::arg("path", _path.string()));
}

compatibility_result
raw_compatibility_result::operator()(verbose is_verbose) && {
    compatibility_result result = {.is_compat = !has_error()};
    if (is_verbose) {
        result.messages.reserve(_errors.size());
        std::transform(
          std::make_move_iterator(_errors.begin()),
          std::make_move_iterator(_errors.end()),
          std::back_inserter(result.messages),
          [](auto e) {
              return std::visit(
                [](auto&& e) { return fmt::format("{{{}}}", e); }, e);
          });
    }
    return result;
}

void raw_compatibility_result::merge(raw_compatibility_result&& other) {
    _errors.reserve(_errors.size() + other._errors.size());
    std::move(
      other._errors.begin(), other._errors.end(), std::back_inserter(_errors));
}

} // namespace pandaproxy::schema_registry
