

#pragma once

#include <system_error>

namespace wasm {
enum class errc {
    success = 0,
    // When the user's code fails to be loaded
    load_failure,
    // When the engine is fails to be created
    engine_creation_failure,
};

struct errc_category final : public std::error_category {
    const char* name() const noexcept final { return "wasm::errc"; }

    std::string message(int c) const final {
        switch (static_cast<errc>(c)) {
        case errc::success:
            return "wasm::errc::success";
        case errc::load_failure:
            return "wasm::errc::load_failure";
        case errc::engine_creation_failure:
            return "wasm::errc::engine_creation_failure";
        default:
            return "wasm::errc::unknown(" + std::to_string(c) + ")";
        }
    }
};
inline const std::error_category& error_category() noexcept {
    static errc_category e;
    return e;
}
inline std::error_code make_error_code(errc e) noexcept {
    return {static_cast<int>(e), error_category()};
}
} // namespace wasm

namespace std {
template<>
struct is_error_code_enum<wasm::errc> : true_type {};
} // namespace std
