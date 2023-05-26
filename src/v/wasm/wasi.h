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

// https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L370-L373
constexpr uint16_t ERRNO_BADF = 8;

// https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L70C1-L97
using clock_id_t = uint32_t;
constexpr clock_id_t REALTIME_CLOCK_ID = 0;
constexpr clock_id_t MONOTONIC_CLOCK_ID = 1;
constexpr clock_id_t PROCESS_CPUTIME_CLOCK_ID = 2;
constexpr clock_id_t THREAD_CPUTIME_CLOCK_ID = 3;
// A timestamp in nanoseconds
using timestamp_t = uint64_t;
// A file descriptor
using fd_t = int32_t;
/**
 * A region of memory for scatter/gather writes.
 */
struct iovec_t {
    /** The (guest) address of the buffer to be written. */
    uint32_t buf;
    /** The length of the buffer to be written. */
    uint32_t buf_len;
};

/**
 * Implementation of the wasi preview1 which is a snapshot of the wasi spec from 2020.
 */
class preview1_module {
public:
    preview1_module() = default;
    preview1_module(const preview1_module&) = delete;
    preview1_module& operator=(const preview1_module&) = delete;
    preview1_module(preview1_module&&) = default;
    preview1_module& operator=(preview1_module&&) = default;
    ~preview1_module() = default;

    static constexpr std::string_view name = "wasi_snapshot_preview1";

    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1446-L1452
    uint16_t clock_res_get(uint32_t, timestamp_t*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1453-L1469
    uint16_t clock_time_get(uint32_t, timestamp_t, timestamp_t*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1409-L1418
    int32_t args_sizes_get(uint32_t*, uint32_t*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1400-L1408
    int16_t args_get(uint8_t**, uint8_t*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1419-L1427
    int32_t environ_get(uint8_t**, uint8_t*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1428-L1437
    int32_t environ_sizes_get(uint32_t*, uint32_t*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1470-L1488
    int16_t fd_advise(fd_t, uint64_t, uint64_t, uint8_t);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1493-L1503
    int16_t fd_allocate(fd_t, uint64_t, uint64_t);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1504-L1510
    int16_t fd_close(fd_t);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1504-L1510
    int16_t fd_datasync(fd_t);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1518-L1527
    int16_t fd_fdstat_get(fd_t, void*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1528-L1538
    int16_t fd_fdstat_set_flags(fd_t, uint16_t);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1551-L1559
    int16_t fd_filestat_get(fd_t, void*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1560-L1570
    int16_t fd_filestat_set_size(fd_t, uint64_t);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1571-L1589
    int16_t fd_filestat_set_times(fd_t, timestamp_t, timestamp_t, uint16_t);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1590-L1611
    int16_t fd_pread(fd_t, ffi::array<iovec_t>, uint64_t, uint32_t*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1612-L1620
    int16_t fd_prestat_get(fd_t, void*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1621-L1631
    int16_t fd_prestat_dir_name(fd_t, uint8_t*, uint32_t);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1632-L1653
    int16_t fd_pwrite(fd_t, ffi::array<iovec_t>, uint64_t, uint32_t*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1654-L1671
    int16_t fd_read(fd_t, ffi::array<iovec_t>, uint32_t*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1672-L1697
    int16_t fd_readdir(fd_t, ffi::array<uint8_t>, uint64_t, uint32_t*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1698-L1714
    int16_t fd_renumber(fd_t, fd_t);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1715-L1732
    int16_t fd_seek(fd_t, int64_t, uint8_t, uint64_t*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1733-L1739
    int16_t fd_sync(fd_t);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1740-L1749
    int16_t fd_tell(fd_t, uint64_t*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1750-L1765
    int16_t fd_write(ffi::memory*, fd_t, ffi::array<iovec_t>, uint32_t*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1766-L1776
    int16_t path_create_directory(fd_t, ffi::array<uint8_t>);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1777-L1794
    int16_t path_filestat_get(fd_t, uint32_t, ffi::array<uint8_t>, void*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1795-L1821
    int16_t path_filestat_set_times(
      fd_t, uint32_t, ffi::array<uint8_t>, timestamp_t, timestamp_t, uint16_t);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1822-L1844
    int16_t
      path_link(fd_t, uint32_t, ffi::array<uint8_t>, fd_t, ffi::array<uint8_t>);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1845-L1884
    int16_t path_open(
      fd_t,
      uint32_t,
      ffi::array<uint8_t>,
      uint16_t,
      uint64_t,
      uint64_t,
      uint16_t,
      fd_t*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1885-L1903
    int16_t path_readlink(
      fd_t, ffi::array<uint8_t> path, ffi::array<uint8_t>, uint32_t*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1904-L1915
    int16_t path_remove_directory(fd_t, ffi::array<uint8_t>);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1916-L1934
    int16_t path_rename(fd_t, ffi::array<uint8_t>, fd_t, ffi::array<uint8_t>);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1935-L1949
    int16_t path_symlink(ffi::array<uint8_t>, fd_t, ffi::array<uint8_t>);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1950-L1961
    int16_t path_unlink_file(fd_t, ffi::array<uint8_t>);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1962-L1981
    int16_t poll_oneoff(void*, void*, uint32_t, uint32_t*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L2000-L2014
    int16_t random_get(ffi::array<uint8_t>);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1982-L1992
    void proc_exit(int32_t exit_code);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1993-L1999
    int32_t sched_yield();
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L2015-L2031
    int16_t sock_accept(fd_t, uint16_t, fd_t*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L2032-L2055
    int16_t
    sock_recv(fd_t, ffi::array<iovec_t>, uint16_t, uint32_t*, uint16_t*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L2056-L2078
    int16_t sock_send(fd_t, ffi::array<iovec_t>, uint16_t, uint32_t*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L2079-L2090
    int16_t sock_shutdown(fd_t, uint8_t);
};

} // namespace wasm::wasi
