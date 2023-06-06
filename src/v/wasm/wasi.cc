/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "wasi.h"

#include "ffi.h"
#include "seastarx.h"

#include <seastar/util/log.hh>

#include <sstream>

namespace wasm::wasi {

static ss::logger wasi_log("wasi");

uint16_t preview1_module::clock_res_get(uint32_t, uint64_t*) {
    return ERRNO_NOSYS;
}
uint16_t preview1_module::clock_time_get(uint32_t, uint64_t, uint64_t*) {
    return ERRNO_NOSYS;
}

int32_t
preview1_module::args_sizes_get(uint32_t* count_ptr, uint32_t* size_ptr) {
    *count_ptr = 0;
    *size_ptr = 0;
    return ERRNO_SUCCESS;
}

int16_t preview1_module::args_get(uint8_t**, uint8_t*) { return ERRNO_SUCCESS; }

int32_t preview1_module::environ_get(uint8_t**, uint8_t*) {
    return ERRNO_SUCCESS;
}

int32_t
preview1_module::environ_sizes_get(uint32_t* count_ptr, uint32_t* size_ptr) {
    *count_ptr = 0;
    *size_ptr = 0;
    return ERRNO_SUCCESS;
}
int16_t preview1_module::fd_advise(fd_t, uint64_t, uint64_t, uint8_t) {
    return ERRNO_NOSYS;
}
int16_t preview1_module::fd_allocate(fd_t, uint64_t, uint64_t) {
    return ERRNO_NOSYS;
}
int16_t preview1_module::fd_close(int32_t) { return ERRNO_NOSYS; }
int16_t preview1_module::fd_datasync(fd_t) { return ERRNO_NOSYS; }
int16_t preview1_module::fd_fdstat_get(int32_t, void*) { return ERRNO_NOSYS; }
int16_t preview1_module::fd_fdstat_set_flags(fd_t, uint16_t) {
    return ERRNO_NOSYS;
}
int16_t preview1_module::fd_filestat_get(fd_t, void*) { return ERRNO_NOSYS; }
int16_t preview1_module::fd_filestat_set_size(fd_t, uint64_t) {
    return ERRNO_NOSYS;
}
int16_t preview1_module::fd_filestat_set_times(
  fd_t, timestamp_t, timestamp_t, uint16_t) {
    return ERRNO_NOSYS;
}
int16_t
preview1_module::fd_pread(fd_t, ffi::array<iovec_t>, uint64_t, uint32_t*) {
    return ERRNO_NOSYS;
}
int16_t preview1_module::fd_prestat_dir_name(fd_t, uint8_t*, uint32_t) {
    return ERRNO_NOSYS;
}
int16_t
preview1_module::fd_pwrite(fd_t, ffi::array<iovec_t>, uint64_t, uint32_t*) {
    return ERRNO_NOSYS;
}
int16_t preview1_module::fd_read(fd_t, ffi::array<iovec_t>, uint32_t*) {
    return ERRNO_NOSYS;
}
int16_t
preview1_module::fd_readdir(fd_t, ffi::array<uint8_t>, uint64_t, uint32_t*) {
    return ERRNO_NOSYS;
}
int16_t preview1_module::fd_renumber(fd_t, fd_t) { return ERRNO_NOSYS; }
int16_t preview1_module::fd_seek(fd_t, int64_t, uint8_t, uint64_t*) {
    return ERRNO_NOSYS;
}
int16_t preview1_module::fd_sync(fd_t) { return ERRNO_NOSYS; }
int16_t preview1_module::fd_tell(fd_t, uint64_t*) { return ERRNO_NOSYS; }
int16_t preview1_module::fd_prestat_get(int32_t fd, void*) {
    if (fd == 0 || fd == 1 || fd == 2) {
        // stdin, stdout, stderr are fine but unimplemented
        return ERRNO_NOSYS;
    }
    // We don't hand out any file descriptors and this is needed for wasi_libc
    return ERRNO_BADF;
}

int16_t preview1_module::fd_write(
  ffi::memory* mem, int32_t fd, ffi::array<iovec_t> iovecs, uint32_t* written) {
    if (written == nullptr || !iovecs) [[unlikely]] {
        return ERRNO_INVAL;
    }
    if (fd == 1) {
        std::stringstream ss;
        for (unsigned i = 0; i < iovecs.size(); ++i) {
            const auto& vec = iovecs[i];
            const void* data = mem->translate(vec.buf, vec.buf_len);
            if (data == nullptr) [[unlikely]] {
                return ERRNO_INVAL;
            }
            ss << std::string_view(
              reinterpret_cast<const char*>(data), vec.buf_len);
        }
        // TODO: We should be buffering these until a newline or something and
        // emitting logs line by line Also: rate limit logs
        auto str = ss.str();
        wasi_log.info("Guest stdout: {}", str);
        *written = str.size();
        return ERRNO_SUCCESS;
    }
    return ERRNO_NOSYS;
}
int16_t preview1_module::path_create_directory(fd_t, ffi::array<uint8_t>) {
    return ERRNO_NOSYS;
}
int16_t
preview1_module::path_filestat_get(fd_t, uint32_t, ffi::array<uint8_t>, void*) {
    return ERRNO_NOSYS;
}
int16_t preview1_module::path_filestat_set_times(
  fd_t, uint32_t, ffi::array<uint8_t>, timestamp_t, timestamp_t, uint16_t) {
    return ERRNO_NOSYS;
}
int16_t preview1_module::path_link(
  fd_t, uint32_t, ffi::array<uint8_t>, fd_t, ffi::array<uint8_t>) {
    return ERRNO_NOSYS;
}
int16_t preview1_module::path_open(
  fd_t,
  uint32_t,
  ffi::array<uint8_t>,
  uint16_t,
  uint64_t,
  uint64_t,
  uint16_t,
  fd_t*) {
    return ERRNO_NOSYS;
}
int16_t preview1_module::path_readlink(
  fd_t, ffi::array<uint8_t>, ffi::array<uint8_t>, uint32_t*) {
    return ERRNO_NOSYS;
}
int16_t preview1_module::path_remove_directory(fd_t, ffi::array<uint8_t>) {
    return ERRNO_NOSYS;
}
int16_t preview1_module::path_rename(
  fd_t, ffi::array<uint8_t>, fd_t, ffi::array<uint8_t>) {
    return ERRNO_NOSYS;
}
int16_t
preview1_module::path_symlink(ffi::array<uint8_t>, fd_t, ffi::array<uint8_t>) {
    return ERRNO_NOSYS;
}
int16_t preview1_module::path_unlink_file(fd_t, ffi::array<uint8_t>) {
    return ERRNO_NOSYS;
}
int16_t preview1_module::poll_oneoff(void*, void*, uint32_t, uint32_t*) {
    return ERRNO_NOSYS;
}
int16_t preview1_module::random_get(ffi::array<uint8_t> buf) {
    // https://imgur.com/uR4WuQ0
    for (size_t i = 0; i < buf.size(); ++i) {
        buf[i] = 9;
    }
    return ERRNO_SUCCESS;
}
int16_t preview1_module::sock_accept(fd_t, uint16_t, fd_t*) {
    return ERRNO_NOSYS;
}
int16_t preview1_module::sock_recv(
  fd_t, ffi::array<iovec_t>, uint16_t, uint32_t*, uint16_t*) {
    return ERRNO_NOSYS;
}
int16_t
preview1_module::sock_send(fd_t, ffi::array<iovec_t>, uint16_t, uint32_t*) {
    return ERRNO_NOSYS;
}
int16_t preview1_module::sock_shutdown(fd_t, uint8_t) { return ERRNO_NOSYS; }
void preview1_module::proc_exit(int32_t exit_code) {
    throw std::runtime_error(ss::format("Exiting: {}", exit_code));
}
int32_t preview1_module::sched_yield() {
    // TODO: actually yield
    return ERRNO_SUCCESS;
}
} // namespace wasm::wasi
