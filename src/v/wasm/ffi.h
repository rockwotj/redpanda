
#pragma once

#include "utils/hdr_hist.h"

#include <cstdint>
#include <span>
#include <string_view>
#include <type_traits>
#include <vector>

namespace wasm::ffi {

template<typename T>
class array {
public:
    using element_type = T;

    array()
      : _ptr(nullptr)
      , _size(0) {}
    array(T* ptr, uint32_t size)
      : _ptr(ptr)
      , _size(size) {}

    array(array<T>&&) noexcept = default;
    array& operator=(array<T>&&) noexcept = default;

    array(const array<T>&) noexcept = default;
    array& operator=(const array<T>&) noexcept = default;

    ~array() = default;

    explicit operator bool() const noexcept { return bool(_ptr); }

    T* raw() noexcept { return _ptr; }

    const T* raw() const noexcept { return _ptr; }

    T& operator[](uint32_t index) noexcept { return _ptr[index]; }

    const T& operator[](uint32_t index) const noexcept { return _ptr[index]; }

    uint32_t size() const noexcept { return _size; }

private:
    T* _ptr;
    uint32_t _size;
};

class memory {
public:
    memory() = default;
    virtual ~memory() = default;
    memory(const memory&) = delete;
    memory& operator=(const memory&) = delete;
    memory(memory&&) = default;
    memory& operator=(memory&&) = default;

    /* returns the host pointer for a given guest ptr and length. Throws if out
     * of bounds. */
    virtual void* translate(size_t guest_ptr, size_t len) = 0;
};

enum class val_type { i32, i64, f32, f64 };

std::ostream& operator<<(std::ostream& o, val_type vt);

struct val {
    val_type type;
    uint64_t value;
};

inline std::string_view array_as_string_view(array<uint8_t> arr) {
    return {reinterpret_cast<char*>(arr.raw()), arr.size()};
}

template<class T>
struct dependent_false : std::false_type {};

template<typename T>
struct is_array {
    static constexpr bool value = false;
};
template<template<typename...> class C, typename U>
struct is_array<C<U>> {
    static constexpr bool value = std::is_same<C<U>, array<U>>::value;
};

template<typename Type>
void transform_type(std::vector<val_type>& types) {
    if constexpr (std::is_same_v<ffi::memory*, Type> || std::is_void_v<Type>) {
        // We don't bind this type over the FFI boundary, but make the runtime
        // provide it
    } else if constexpr (ffi::is_array<Type>::value) {
        // Push back an arg for the pointer
        types.push_back(val_type::i32);
        // Push back an other arg for the length
        types.push_back(val_type::i32);
    } else if constexpr (
      std::is_same_v<Type, int64_t> || std::is_same_v<Type, uint64_t>) {
        types.push_back(val_type::i64);
    } else if constexpr (std::is_pointer_v<Type> || std::is_integral_v<Type>) {
        types.push_back(val_type::i32);
    } else {
        static_assert(dependent_false<Type>::value, "Unknown type");
    }
}

template<typename... Args>
concept EmptyPack = sizeof...(Args) == 0;

template<typename... Rest>
void transform_types(std::vector<val_type>&) requires EmptyPack<Rest...> {
    // Nothing to do
}

template<typename Type, typename... Rest>
void transform_types(std::vector<val_type>& types) {
    transform_type<Type>(types);
    transform_types<Rest...>(types);
}

template<typename Type>
std::tuple<Type> extract_parameter(
  ffi::memory* mem, std::span<const uint64_t> raw_params, unsigned& idx) {
    if constexpr (std::is_same_v<ffi::memory*, Type>) {
        return std::tuple(mem);
    } else if constexpr (ffi::is_array<Type>::value) {
        auto guest_ptr = static_cast<uint32_t>(raw_params[idx++]);
        auto ptr_len = static_cast<uint32_t>(raw_params[idx++]);
        void* host_ptr = mem->translate(
          guest_ptr, ptr_len * sizeof(typename Type::element_type));
        if (host_ptr == nullptr) {
            return std::tuple<Type>();
        }
        return std::make_tuple(ffi::array<typename Type::element_type>(
          reinterpret_cast<typename Type::element_type*>(host_ptr), ptr_len));
    } else if constexpr (
      std::is_same_v<Type, const void*> || std::is_same_v<Type, void*>) {
        ++idx;
        // TODO: Remove this temporary hack
        return std::make_tuple(static_cast<Type>(nullptr));
    } else if constexpr (std::is_pointer_v<Type>) {
        // Assume this is an out val
        auto guest_ptr = static_cast<uint32_t>(raw_params[idx++]);
        uint32_t ptr_len = sizeof(typename std::remove_pointer_t<Type>);
        void* host_ptr = mem->translate(guest_ptr, ptr_len);
        if (host_ptr == nullptr) {
            return std::tuple<Type>();
        }
        return std::make_tuple(reinterpret_cast<Type>(host_ptr));
    } else if constexpr (std::is_integral_v<Type>) {
        return std::make_tuple(static_cast<Type>(raw_params[idx++]));
    } else {
        static_assert(dependent_false<Type>::value, "Unknown type");
    }
}

template<typename... Rest>
std::tuple<> extract_parameters(
  ffi::memory*,
  std::span<const uint64_t>,
  unsigned) requires EmptyPack<Rest...> {
    return std::make_tuple();
}

template<typename Type, typename... Rest>
std::tuple<Type, Rest...> extract_parameters(
  ffi::memory* mem, std::span<const uint64_t> params, unsigned idx) {
    auto head_type = extract_parameter<Type>(mem, params, idx);
    return std::tuple_cat(
      std::move(head_type), extract_parameters<Rest...>(mem, params, idx));
}
} // namespace wasm::ffi
