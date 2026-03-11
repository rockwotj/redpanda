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

#include "enterprise_features.h"

#include "base/vassert.h"

#include <iostream>

namespace features {

std::string_view format_as(license_required_feature f) {
    switch (f) {
    case license_required_feature::audit_logging:
        return "audit_logging";
    case license_required_feature::cloud_storage:
        return "cloud_storage";
    case license_required_feature::partition_auto_balancing_continuous:
        return "partition_auto_balancing_continuous";
    case license_required_feature::core_balancing_continuous:
        return "core_balancing_continuous";
    case license_required_feature::gssapi:
        return "gssapi";
    case license_required_feature::oidc:
        return "oidc";
    case license_required_feature::schema_id_validation:
        return "schema_id_validation";
    case license_required_feature::rbac:
        return "rbac";
    case license_required_feature::fips:
        return "fips";
    case license_required_feature::datalake_iceberg:
        return "datalake_iceberg";
    case license_required_feature::leadership_pinning:
        return "leadership_pinning";
    case license_required_feature::shadow_linking:
        return "shadow_linking";
    case license_required_feature::cloud_topics:
        return "cloud_topics";
    case license_required_feature::topic_deletion_disabled:
        return "topic_deletion_disabled";
    }
    __builtin_unreachable();
}

std::ostream& operator<<(std::ostream& os, license_required_feature f) {
    return os << format_as(f);
}

void enterprise_feature_report::set(
  license_required_feature feat, bool enabled) {
    auto insert = [feat](vtype& dest, const vtype& other) {
        vassert(
          !other.contains(feat),
          "feature {{{}}} cannot be both enabled and disabled",
          feat);
        dest.insert(feat);
    };

    if (enabled) {
        insert(_enabled, _disabled);
    } else {
        insert(_disabled, _enabled);
    }
}

bool enterprise_feature_report::test(license_required_feature feat) {
    auto en = _enabled.contains(feat);
    auto di = _disabled.contains(feat);
    vassert(
      en != di, "Enterprise features should be either enabled xor disabled");
    return en;
}

} // namespace features
