#include "wasi.h"

#include "ffi.h"
#include "seastarx.h"

#include <seastar/util/log.hh>

#include <sstream>

namespace wasm::wasi {

static ss::logger wasi_log("wasi");

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
int16_t preview1_module::fd_close(int32_t) { return ERRNO_NOSYS; }
int16_t preview1_module::fd_fdstat_get(int32_t, void*) { return ERRNO_NOSYS; }
int16_t preview1_module::fd_prestat_get(int32_t fd, void*) {
    if (fd == 0 || fd == 1 || fd == 2) {
        // stdin, stdout, stderr are fine but unimplemented
        return ERRNO_NOSYS;
    }
    // We don't hand out any file descriptors and this is needed for wasi_libc
    return ERRNO_BADF;
}
int16_t preview1_module::fd_prestat_dir_name(int32_t, uint8_t*, uint32_t) {
    return ERRNO_NOSYS;
}
int16_t preview1_module::fd_read(int32_t, const void*, uint32_t, uint32_t*) {
    return ERRNO_NOSYS;
}

int16_t preview1_module::fd_write(
  ffi::memory* mem,
  int32_t fd,
  ffi::array<ciovec_t> iovecs,
  uint32_t* written) {
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
int16_t preview1_module::fd_seek(int32_t, int64_t, uint8_t, uint64_t*) {
    return ERRNO_NOSYS;
}
int16_t preview1_module::path_open(
  int32_t,
  uint32_t,
  ffi::array<uint8_t>,
  uint16_t,
  uint64_t,
  uint64_t,
  uint16_t,
  void*) {
    return ERRNO_NOSYS;
}
void preview1_module::proc_exit(int32_t exit_code) {
    throw std::runtime_error(ss::format("Exiting: {}", exit_code));
}
int32_t preview1_module::sched_yield() {
    // TODO: actually yield
    return ERRNO_SUCCESS;
}
} // namespace wasm::wasi
