/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage_clients/s3_error.h"

#include <boost/lexical_cast.hpp>

#include <map>

namespace cloud_storage_clients {

// NOLINTBEGIN(bugprone-switch-missing-default-case)
std::string_view format_as(s3_error_code code) {
    switch (code) {
    case s3_error_code::access_denied: return "AccessDenied";
    case s3_error_code::account_problem: return "AccountProblem";
    case s3_error_code::all_access_disabled: return "AllAccessDisabled";
    case s3_error_code::ambiguous_grant_by_email_address: return "AmbiguousGrantByEmailAddress";
    case s3_error_code::authentication_required: return "AuthenticationRequired";
    case s3_error_code::authorization_header_malformed: return "AuthorizationHeaderMalformed";
    case s3_error_code::bad_digest: return "BadDigest";
    case s3_error_code::bucket_already_exists: return "BucketAlreadyExists";
    case s3_error_code::bucket_already_owned_by_you: return "BucketAlreadyOwnedByYou";
    case s3_error_code::bucket_not_empty: return "BucketNotEmpty";
    case s3_error_code::credentials_not_supported: return "CredentialsNotSupported";
    case s3_error_code::cross_location_logging_prohibited: return "CrossLocationLoggingProhibited";
    case s3_error_code::entity_too_small: return "EntityTooSmall";
    case s3_error_code::entity_too_large: return "EntityTooLarge";
    case s3_error_code::expired_token: return "ExpiredToken";
    case s3_error_code::illegal_location_constraint_exception: return "IllegalLocationConstraintException";
    case s3_error_code::illegal_versioning_configuration_exception: return "IllegalVersioningConfigurationException";
    case s3_error_code::incomplete_body: return "IncompleteBody";
    case s3_error_code::incorrect_number_of_files_in_post_request: return "IncorrectNumberOfFilesInPostRequest";
    case s3_error_code::inline_data_too_large: return "InlineDataTooLarge";
    case s3_error_code::internal_error: return "InternalError";
    case s3_error_code::invalid_access_key_id: return "InvalidAccessKeyId";
    case s3_error_code::invalid_access_point: return "InvalidAccessPoint";
    case s3_error_code::invalid_addressing_header: return "InvalidAddressingHeader";
    case s3_error_code::invalid_argument: return "InvalidArgument";
    case s3_error_code::invalid_bucket_name: return "InvalidBucketName";
    case s3_error_code::invalid_bucket_state: return "InvalidBucketState";
    case s3_error_code::invalid_digest: return "InvalidDigest";
    case s3_error_code::invalid_encryption_algorithm_error: return "InvalidEncryptionAlgorithmError";
    case s3_error_code::invalid_location_constraint: return "InvalidLocationConstraint";
    case s3_error_code::invalid_object_state: return "InvalidObjectState";
    case s3_error_code::invalid_part: return "InvalidPart";
    case s3_error_code::invalid_part_order: return "InvalidPartOrder";
    case s3_error_code::invalid_payer: return "InvalidPayer";
    case s3_error_code::invalid_policy_document: return "InvalidPolicyDocument";
    case s3_error_code::invalid_range: return "InvalidRange";
    case s3_error_code::invalid_request: return "InvalidRequest";
    case s3_error_code::invalid_security: return "InvalidSecurity";
    case s3_error_code::invalid_soaprequest: return "InvalidSOAPRequest";
    case s3_error_code::invalid_storage_class: return "InvalidStorageClass";
    case s3_error_code::invalid_target_bucket_for_logging: return "InvalidTargetBucketForLogging";
    case s3_error_code::invalid_token: return "InvalidToken";
    case s3_error_code::invalid_uri: return "InvalidURI";
    case s3_error_code::key_too_long_error: return "KeyTooLongError";
    case s3_error_code::malformed_aclerror: return "MalformedACLError";
    case s3_error_code::malformed_postrequest: return "MalformedPOSTRequest";
    case s3_error_code::malformed_xml: return "MalformedXML";
    case s3_error_code::max_message_length_exceeded: return "MaxMessageLengthExceeded";
    case s3_error_code::max_post_pre_data_length_exceeded_error: return "MaxPostPreDataLengthExceededError";
    case s3_error_code::metadata_too_large: return "MetadataTooLarge";
    case s3_error_code::method_not_allowed: return "MethodNotAllowed";
    case s3_error_code::missing_attachment: return "MissingAttachment";
    case s3_error_code::missing_content_length: return "MissingContentLength";
    case s3_error_code::missing_request_body_error: return "MissingRequestBodyError";
    case s3_error_code::missing_security_element: return "MissingSecurityElement";
    case s3_error_code::missing_security_header: return "MissingSecurityHeader";
    case s3_error_code::no_logging_status_for_key: return "NoLoggingStatusForKey";
    case s3_error_code::no_such_bucket: return "NoSuchBucket";
    case s3_error_code::no_such_bucket_policy: return "NoSuchBucketPolicy";
    case s3_error_code::no_such_key: return "NoSuchKey";
    case s3_error_code::no_such_lifecycle_configuration: return "NoSuchLifecycleConfiguration";
    case s3_error_code::no_such_tag_set: return "NoSuchTagSet";
    case s3_error_code::no_such_upload: return "NoSuchUpload";
    case s3_error_code::no_such_version: return "NoSuchVersion";
    case s3_error_code::not_implemented: return "NotImplemented";
    case s3_error_code::not_signed_up: return "NotSignedUp";
    case s3_error_code::operation_aborted: return "OperationAborted";
    case s3_error_code::permanent_redirect: return "PermanentRedirect";
    case s3_error_code::precondition_failed: return "PreconditionFailed";
    case s3_error_code::redirect: return "Redirect";
    case s3_error_code::request_header_section_too_large: return "RequestHeaderSectionTooLarge";
    case s3_error_code::request_is_not_multi_part_content: return "RequestIsNotMultiPartContent";
    case s3_error_code::request_timeout: return "RequestTimeout";
    case s3_error_code::request_time_too_skewed: return "RequestTimeTooSkewed";
    case s3_error_code::request_torrent_of_bucket_error: return "RequestTorrentOfBucketError";
    case s3_error_code::restore_already_in_progress: return "RestoreAlreadyInProgress";
    case s3_error_code::server_side_encryption_configuration_not_found_error: return "ServerSideEncryptionConfigurationNotFoundError";
    case s3_error_code::service_unavailable: return "ServiceUnavailable";
    case s3_error_code::signature_does_not_match: return "SignatureDoesNotMatch";
    case s3_error_code::slow_down: return "SlowDown";
    case s3_error_code::temporary_redirect: return "TemporaryRedirect";
    case s3_error_code::token_refresh_required: return "TokenRefreshRequired";
    case s3_error_code::too_many_access_points: return "TooManyAccessPoints";
    case s3_error_code::too_many_buckets: return "TooManyBuckets";
    case s3_error_code::unexpected_content: return "UnexpectedContent";
    case s3_error_code::unresolvable_grant_by_email_address: return "UnresolvableGrantByEmailAddress";
    case s3_error_code::user_key_must_be_specified: return "UserKeyMustBeSpecified";
    case s3_error_code::no_such_access_point: return "NoSuchAccessPoint";
    case s3_error_code::invalid_tag: return "InvalidTag";
    case s3_error_code::malformed_policy: return "MalformedPolicy";
    case s3_error_code::no_such_configuration: return "NoSuchConfiguration";
    case s3_error_code::authorization_query_parameters_error: return "AuthorizationQueryParametersError";
    case s3_error_code::access_point_already_owned_by_you: return "AccessPointAlreadyOwnedByYou";
    case s3_error_code::access_control_list_not_supported: return "AccessControlListNotSupported";
    case s3_error_code::endpoint_not_found: return "EndpointNotFound";
    case s3_error_code::device_not_active_error: return "DeviceNotActiveError";
    case s3_error_code::conditional_request_conflict: return "ConditionalRequestConflict";
    case s3_error_code::connection_closed_by_requester: return "ConnectionClosedByRequester";
    case s3_error_code::client_token_conflict: return "ClientTokenConflict";
    case s3_error_code::bucket_has_access_points_attached: return "BucketHasAccessPointsAttached";
    case s3_error_code::invalid_access_point_alias_error: return "InvalidAccessPointAliasError";
    case s3_error_code::incorrect_endpoint: return "IncorrectEndpoint";
    case s3_error_code::invalid_http_method: return "InvalidHttpMethod";
    case s3_error_code::invalid_host_header: return "InvalidHostHeader";
    case s3_error_code::invalid_bucket_owner_aws_account_id: return "InvalidBucketOwnerAWSAccountID";
    case s3_error_code::invalid_bucket_acl_with_object_ownership: return "InvalidBucketAclWithObjectOwnership";
    case s3_error_code::invalid_session_exception: return "InvalidSessionException";
    case s3_error_code::invalid_signature: return "InvalidSignature";
    case s3_error_code::kms_disabled_exception: return "KMS.DisabledException";
    case s3_error_code::kms_invalid_key_usage_exception: return "KMS.InvalidKeyUsageException";
    case s3_error_code::kms_invalid_state_exception: return "KMS.KMSInvalidStateException";
    case s3_error_code::kms_not_found_exception: return "KMS.NotFoundException";
    case s3_error_code::missing_authentication_token: return "MissingAuthenticationToken";
    case s3_error_code::no_such_async_request: return "NoSuchAsyncRequest";
    case s3_error_code::no_such_cors_configuration: return "NoSuchCORSConfiguration";
    case s3_error_code::no_such_multi_region_access_point: return "NoSuchMultiRegionAccessPoint";
    case s3_error_code::no_such_object_lock_configuration: return "NoSuchObjectLockConfiguration";
    case s3_error_code::no_such_website_configuration: return "NoSuchWebsiteConfiguration";
    case s3_error_code::not_modified: return "NotModified";
    case s3_error_code::not_device_owner_error: return "NotDeviceOwnerError";
    case s3_error_code::no_transformation_defined: return "NoTransformationDefined";
    case s3_error_code::object_lock_configuration_not_found_error: return "ObjectLockConfigurationNotFoundError";
    case s3_error_code::ownership_controls_not_found_error: return "OwnershipControlsNotFoundError";
    case s3_error_code::permanent_redirect_control_error: return "PermanentRedirectControlError";
    case s3_error_code::response_interrupted: return "ResponseInterrupted";
    case s3_error_code::token_code_invalid_error: return "TokenCodeInvalidError";
    case s3_error_code::too_many_multi_region_access_pointregions_error: return "TooManyMultiRegionAccessPointregionsError";
    case s3_error_code::too_many_multi_region_access_points: return "TooManyMultiRegionAccessPoints";
    case s3_error_code::unauthorized_access_error: return "UnauthorizedAccessError";
    case s3_error_code::unexpected_ip_error: return "UnexpectedIPError";
    case s3_error_code::unsupported_signature: return "UnsupportedSignature";
    case s3_error_code::unsupported_argument: return "UnsupportedArgument";
    case s3_error_code::_unknown: return "_unknown_error_code_";
    }
}
// NOLINTEND(bugprone-switch-missing-default-case)

std::ostream& operator<<(std::ostream& o, s3_error_code code) {
    return o << format_as(code);
}

// NOLINTNEXTLINE
static const std::map<ss::sstring, s3_error_code> known_aws_error_codes = {
  {"AccessDenied", s3_error_code::access_denied},
  {"AccountProblem", s3_error_code::account_problem},
  {"AllAccessDisabled", s3_error_code::all_access_disabled},
  {"AmbiguousGrantByEmailAddress",
   s3_error_code::ambiguous_grant_by_email_address},
  {"AuthenticationRequired", s3_error_code::authentication_required},
  {"AuthorizationHeaderMalformed",
   s3_error_code::authorization_header_malformed},
  {"BadDigest", s3_error_code::bad_digest},
  {"BucketAlreadyExists", s3_error_code::bucket_already_exists},
  {"BucketAlreadyOwnedByYou", s3_error_code::bucket_already_owned_by_you},
  {"BucketNotEmpty", s3_error_code::bucket_not_empty},
  {"CredentialsNotSupported", s3_error_code::credentials_not_supported},
  {"CrossLocationLoggingProhibited",
   s3_error_code::cross_location_logging_prohibited},
  {"EntityTooSmall", s3_error_code::entity_too_small},
  {"EntityTooLarge", s3_error_code::entity_too_large},
  {"ExpiredToken", s3_error_code::expired_token},
  {"IllegalLocationConstraintException",
   s3_error_code::illegal_location_constraint_exception},
  {"IllegalVersioningConfigurationException",
   s3_error_code::illegal_versioning_configuration_exception},
  {"IncompleteBody", s3_error_code::incomplete_body},
  {"IncorrectNumberOfFilesInPostRequest",
   s3_error_code::incorrect_number_of_files_in_post_request},
  {"InlineDataTooLarge", s3_error_code::inline_data_too_large},
  {"InternalError", s3_error_code::internal_error},
  {"InvalidAccessKeyId", s3_error_code::invalid_access_key_id},
  {"InvalidAccessPoint", s3_error_code::invalid_access_point},
  {"InvalidAddressingHeader", s3_error_code::invalid_addressing_header},
  {"InvalidArgument", s3_error_code::invalid_argument},
  {"InvalidBucketName", s3_error_code::invalid_bucket_name},
  {"InvalidBucketState", s3_error_code::invalid_bucket_state},
  {"InvalidDigest", s3_error_code::invalid_digest},
  {"InvalidEncryptionAlgorithmError",
   s3_error_code::invalid_encryption_algorithm_error},
  {"InvalidLocationConstraint", s3_error_code::invalid_location_constraint},
  {"InvalidObjectState", s3_error_code::invalid_object_state},
  {"InvalidPart", s3_error_code::invalid_part},
  {"InvalidPartOrder", s3_error_code::invalid_part_order},
  {"InvalidPayer", s3_error_code::invalid_payer},
  {"InvalidPolicyDocument", s3_error_code::invalid_policy_document},
  {"InvalidRange", s3_error_code::invalid_range},
  {"InvalidRequest", s3_error_code::invalid_request},
  {"InvalidSecurity", s3_error_code::invalid_security},
  {"InvalidSOAPRequest", s3_error_code::invalid_soaprequest},
  {"InvalidStorageClass", s3_error_code::invalid_storage_class},
  {"InvalidTargetBucketForLogging",
   s3_error_code::invalid_target_bucket_for_logging},
  {"InvalidToken", s3_error_code::invalid_token},
  {"InvalidURI", s3_error_code::invalid_uri},
  {"KeyTooLongError", s3_error_code::key_too_long_error},
  {"MalformedACLError", s3_error_code::malformed_aclerror},
  {"MalformedPOSTRequest", s3_error_code::malformed_postrequest},
  {"MalformedXML", s3_error_code::malformed_xml},
  {"MaxMessageLengthExceeded", s3_error_code::max_message_length_exceeded},
  {"MaxPostPreDataLengthExceededError",
   s3_error_code::max_post_pre_data_length_exceeded_error},
  {"MetadataTooLarge", s3_error_code::metadata_too_large},
  {"MethodNotAllowed", s3_error_code::method_not_allowed},
  {"MissingAttachment", s3_error_code::missing_attachment},
  {"MissingContentLength", s3_error_code::missing_content_length},
  {"MissingRequestBodyError", s3_error_code::missing_request_body_error},
  {"MissingSecurityElement", s3_error_code::missing_security_element},
  {"MissingSecurityHeader", s3_error_code::missing_security_header},
  {"NoLoggingStatusForKey", s3_error_code::no_logging_status_for_key},
  {"NoSuchBucket", s3_error_code::no_such_bucket},
  {"NoSuchBucketPolicy", s3_error_code::no_such_bucket_policy},
  {"NoSuchKey", s3_error_code::no_such_key},
  {"NoSuchLifecycleConfiguration",
   s3_error_code::no_such_lifecycle_configuration},
  {"NoSuchTagSet", s3_error_code::no_such_tag_set},
  {"NoSuchUpload", s3_error_code::no_such_upload},
  {"NoSuchVersion", s3_error_code::no_such_version},
  {"NotImplemented", s3_error_code::not_implemented},
  {"NotSignedUp", s3_error_code::not_signed_up},
  {"OperationAborted", s3_error_code::operation_aborted},
  {"PermanentRedirect", s3_error_code::permanent_redirect},
  {"PreconditionFailed", s3_error_code::precondition_failed},
  {"Redirect", s3_error_code::redirect},
  {"RequestHeaderSectionTooLarge",
   s3_error_code::request_header_section_too_large},
  {"RequestIsNotMultiPartContent",
   s3_error_code::request_is_not_multi_part_content},
  {"RequestTimeout", s3_error_code::request_timeout},
  {"RequestTimeTooSkewed", s3_error_code::request_time_too_skewed},
  {"RequestTorrentOfBucketError",
   s3_error_code::request_torrent_of_bucket_error},
  {"RestoreAlreadyInProgress", s3_error_code::restore_already_in_progress},
  {"ServerSideEncryptionConfigurationNotFoundError",
   s3_error_code::server_side_encryption_configuration_not_found_error},
  {"ServiceUnavailable", s3_error_code::service_unavailable},
  {"SignatureDoesNotMatch", s3_error_code::signature_does_not_match},
  {"SlowDown", s3_error_code::slow_down},
  {"TemporaryRedirect", s3_error_code::temporary_redirect},
  {"TokenRefreshRequired", s3_error_code::token_refresh_required},
  {"TooManyAccessPoints", s3_error_code::too_many_access_points},
  {"TooManyBuckets", s3_error_code::too_many_buckets},
  {"UnexpectedContent", s3_error_code::unexpected_content},
  {"UnresolvableGrantByEmailAddress",
   s3_error_code::unresolvable_grant_by_email_address},
  {"UserKeyMustBeSpecified", s3_error_code::user_key_must_be_specified},
  {"NoSuchAccessPoint", s3_error_code::no_such_access_point},
  {"InvalidTag", s3_error_code::invalid_tag},
  {"MalformedPolicy", s3_error_code::malformed_policy},
  {"NoSuchConfiguration", s3_error_code::no_such_configuration},
  {"AuthorizationQueryParametersError",
   s3_error_code::authorization_query_parameters_error},
  {"AccessPointAlreadyOwnedByYou",
   s3_error_code::access_point_already_owned_by_you},
  {"AccessControlListNotSupported",
   s3_error_code::access_control_list_not_supported},
  {"EndpointNotFound", s3_error_code::endpoint_not_found},
  {"DeviceNotActiveError", s3_error_code::device_not_active_error},
  {"ConditionalRequestConflict", s3_error_code::conditional_request_conflict},
  {"ConnectionClosedByRequester",
   s3_error_code::connection_closed_by_requester},
  {"ClientTokenConflict", s3_error_code::client_token_conflict},
  {"BucketHasAccessPointsAttached",
   s3_error_code::bucket_has_access_points_attached},
  {"InvalidAccessPointAliasError",
   s3_error_code::invalid_access_point_alias_error},
  {"IncorrectEndpoint", s3_error_code::incorrect_endpoint},
  {"InvalidHttpMethod", s3_error_code::invalid_http_method},
  {"InvalidHostHeader", s3_error_code::invalid_host_header},
  {"InvalidBucketOwnerAWSAccountID",
   s3_error_code::invalid_bucket_owner_aws_account_id},
  {"InvalidBucketAclWithObjectOwnership",
   s3_error_code::invalid_bucket_acl_with_object_ownership},
  {"InvalidSessionException", s3_error_code::invalid_session_exception},
  {"InvalidSignature", s3_error_code::invalid_signature},
  {"KMS.DisabledException", s3_error_code::kms_disabled_exception},
  {"KMS.InvalidKeyUsageException",
   s3_error_code::kms_invalid_key_usage_exception},
  {"KMS.KMSInvalidStateException", s3_error_code::kms_invalid_state_exception},
  {"KMS.NotFoundException", s3_error_code::kms_not_found_exception},
  {"MissingAuthenticationToken", s3_error_code::missing_authentication_token},
  {"NoSuchAsyncRequest", s3_error_code::no_such_async_request},
  {"NoSuchCORSConfiguration", s3_error_code::no_such_cors_configuration},
  {"NoSuchMultiRegionAccessPoint",
   s3_error_code::no_such_multi_region_access_point},
  {"NoSuchObjectLockConfiguration",
   s3_error_code::no_such_object_lock_configuration},
  {"NoSuchWebsiteConfiguration", s3_error_code::no_such_website_configuration},
  {"NotModified", s3_error_code::not_modified},
  {"NotDeviceOwnerError", s3_error_code::not_device_owner_error},
  {"NoTransformationDefined", s3_error_code::no_transformation_defined},
  {"ObjectLockConfigurationNotFoundError",
   s3_error_code::object_lock_configuration_not_found_error},
  {"OwnershipControlsNotFoundError",
   s3_error_code::ownership_controls_not_found_error},
  {"PermanentRedirectControlError",
   s3_error_code::permanent_redirect_control_error},
  {"ResponseInterrupted", s3_error_code::response_interrupted},
  {"TokenCodeInvalidError", s3_error_code::token_code_invalid_error},
  {"TooManyMultiRegionAccessPointregionsError",
   s3_error_code::too_many_multi_region_access_pointregions_error},
  {"TooManyMultiRegionAccessPoints",
   s3_error_code::too_many_multi_region_access_points},
  {"UnauthorizedAccessError", s3_error_code::unauthorized_access_error},
  {"UnexpectedIPError", s3_error_code::unexpected_ip_error},
  {"UnsupportedSignature", s3_error_code::unsupported_signature},
  {"UnsupportedArgument", s3_error_code::unsupported_argument}};

std::istream& operator>>(std::istream& i, s3_error_code& code) {
    ss::sstring c;
    i >> c;
    auto it = known_aws_error_codes.find(c);
    if (it != known_aws_error_codes.end()) {
        code = it->second;
    } else {
        code = s3_error_code::_unknown;
    }
    return i;
}

rest_error_response::rest_error_response(
  std::string_view code,
  std::string_view message,
  std::string_view request_id,
  std::string_view resource)
  : _code(
      code.empty() ? s3_error_code::_unknown
                   : boost::lexical_cast<s3_error_code>(code))
  , _code_str(code)
  , _message(message)
  , _request_id(request_id)
  , _resource(resource) {}

const char* rest_error_response::what() const noexcept {
    return _message.c_str();
}
s3_error_code rest_error_response::code() const noexcept { return _code; }
std::string_view rest_error_response::code_string() const noexcept {
    return _code_str;
}
std::string_view rest_error_response::message() const noexcept {
    return _message;
}
std::string_view rest_error_response::request_id() const noexcept {
    return _request_id;
}
std::string_view rest_error_response::resource() const noexcept {
    return _resource;
}

fmt::iterator rest_error_response::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "code: {}, message: {}, request_id: {}, resource: {}",
      _code_str,
      _message,
      _request_id,
      _resource);
}

} // namespace cloud_storage_clients
