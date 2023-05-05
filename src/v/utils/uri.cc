#include "uri.h"

#include <regex>

namespace util {

std::optional<uri> parse_uri(std::string_view str) {
    // TODO: Create a proper uri parser
    static const char* SCHEME_REGEX = "((http[s]?)://)?";
    static const char* HOST_REGEX = "([^@/:\\s]+)";
    static const char* PORT_REGEX = "(:([0-9]{1,5}))?";
    static const char* PATH_REGEX = "(/[^:#?\\s]*)?";
    static const char* QUERY_REGEX
      = "(\\?(([^?;&#=]+=[^?;&#=]+)([;|&]([^?;&#=]+=["
        "^?;&#=]+))*))?";

    static const std::regex re(
      std::string("^") + SCHEME_REGEX + HOST_REGEX + PORT_REGEX + PATH_REGEX
      + QUERY_REGEX + "$");
    std::match_results<std::string_view::const_iterator> match;
    if (!std::regex_match(str.cbegin(), str.cend(), match, re)) {
        return std::nullopt;
    }
    auto protocol = ss::sstring(match[2].first, match[2].second);
    auto domain = ss::sstring(match[3].first, match[3].second);
    auto port = ss::sstring(match[5].first, match[5].second);
    auto path = ss::sstring(match[6].first, match[6].second);
    auto query = ss::sstring(match[8].first, match[8].second);
    return uri{
      .scheme = protocol,
      .host = domain,
      .port = port,
      .path = path,
      .query = query,
    };
}

} // namespace util
