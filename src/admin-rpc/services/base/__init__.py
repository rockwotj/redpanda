from dataclasses import dataclass
from functools import wraps
from typing import Annotated, TypeVar, NoReturn, Callable, Any

def rpc(id: int) -> Callable[..., Any]:
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        setattr(func, 'is_redpanda_admin_rpc', True)
        setattr(func, 'redpanda_admin_rpc_id', id)
        @wraps(func)
        def wrapper() -> None:
            func()
        return wrapper
    return decorator

def serde(version: int, compat_version: int) -> Callable[..., Any]:
    def decorator(original_class: type) -> Callable[..., type]:
        setattr(original_class, 'redpanda_serde_version', version)
        setattr(original_class, 'redpanda_serde_compat_version', compat_version)
        return original_class
    return decorator

def stub() -> NoReturn:
    raise RuntimeError('stub')

@dataclass
class IntegralType:
    size: int
    signed: bool

i32 = Annotated[int, IntegralType(size=4, signed=True)]
u32 = Annotated[int, IntegralType(size=4, signed=False)]
i64 = Annotated[int, IntegralType(size=8, signed=True)]
u64 = Annotated[int, IntegralType(size=8, signed=False)]

@dataclass
class Memory:
    fragmented: bool

T = TypeVar("T")
ChunkedFIFO = Annotated[list[T], Memory(fragmented=True)]
IOBuf = Annotated[bytes, Memory(fragmented=True)]
