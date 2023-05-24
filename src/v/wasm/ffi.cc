#include "ffi.h"

namespace wasm::ffi {

std::ostream& operator<<(std::ostream &o, val_type vt) {
  switch (vt) {
  case val_type::i32:
      o << "i32";
      break;
  case val_type::i64:
      o << "i64";
      break;
  case val_type::f32:
      o << "f32";
      break;
  case val_type::f64:
      o << "f64";
      break;
  }
  return o;
}

}
