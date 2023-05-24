#pragma once

#include "ffi.h"

#include <string_view>

namespace wasm::wasi {

constexpr std::string_view preview_1_start_function_name = "_start";

// https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L110-L113C1
constexpr uint16_t ERRNO_SUCCESS = 0;

// https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L250-L253C1
constexpr uint16_t ERRNO_INVAL = 16;

// https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L370-L373
constexpr uint16_t ERRNO_NOSYS = 52;
//
// https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L370-L373
constexpr uint16_t ERRNO_BADF = 8;

class preview1_module {
public:
    preview1_module() = default;
    preview1_module(const preview1_module&) = delete;
    preview1_module& operator=(const preview1_module&) = delete;
    preview1_module(preview1_module&&) = default;
    preview1_module& operator=(preview1_module&&) = default;
    ~preview1_module() = default;

    static constexpr std::string_view name = "wasi_snapshot_preview1";

    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1453-L1469
    uint16_t clock_time_get(uint32_t, uint64_t, uint64_t*);

    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1409-L1418C1
    int32_t args_sizes_get(uint32_t*, uint32_t*);

    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1400-L1408
    int16_t args_get(uint8_t**, uint8_t*);

    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1419-L1427
    int32_t environ_get(uint8_t**, uint8_t*);

    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1428-L1437
    int32_t environ_sizes_get(uint32_t*, uint32_t*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1504-L1510
    int16_t fd_close(int32_t);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1518-L1527
    int16_t fd_fdstat_get(int32_t, void*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1612-L1620
    int16_t fd_prestat_get(int32_t fd, void*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1621-L1631
    int16_t fd_prestat_dir_name(int32_t, uint8_t*, uint32_t);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1654-L1671
    int16_t fd_read(int32_t, const void*, uint32_t, uint32_t*);

    /**
     * A region of memory for scatter/gather writes.
     */
    struct ciovec_t {
        /** The address of the buffer to be written. */
        uint32_t buf;
        /** The length of the buffer to be written. */
        uint32_t buf_len;
    };

    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1750-L1765
    int16_t fd_write(ffi::memory*, int32_t, ffi::array<ciovec_t>, uint32_t*);

    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1715-L1732
    int16_t fd_seek(int32_t, int64_t, uint8_t, uint64_t*);

    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1845-L1884
    int16_t path_open(
      int32_t,
      uint32_t,
      ffi::array<uint8_t>,
      uint16_t,
      uint64_t,
      uint64_t,
      uint16_t,
      void*);

    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1982-L1992
    void proc_exit(int32_t exit_code);

    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1993-L1999
    int32_t sched_yield();
};

} // namespace wasm::wasi
