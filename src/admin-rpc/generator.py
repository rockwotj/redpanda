from dataclasses import dataclass
from services import hello
import inspect
import typing

# Add each module you'd like code generated for to this list
modules = [
    hello,
]

modules = {module.__name__.removeprefix("services."): module for module in modules}

@dataclass
class Member:
    name: str
    typing: type|typing.Annotated[typing.Any, ...]

@dataclass
class SerdeStruct:
    serde_version: int
    serde_compat_version: int
    module: str
    name: list[str]
    members: list[Member]

def collect_structs(clazz: type, module_name: str) -> list[SerdeStruct]:
    """
    Walk over a class type and collect it's type information, as well as nested classes.
    """
    if not hasattr(clazz, 'redpanda_serde_version'):
        return []
    hints = typing.get_type_hints(clazz, include_extras=True)
    m: list[Member] = []
    for name, t in hints.items():
        m.append(Member(name=name, typing=t))
    structs = [SerdeStruct(
        module=module_name,
        name=clazz.__qualname__.split("."), 
        members=m,
        serde_version=getattr(clazz, 'redpanda_serde_version'),
        serde_compat_version=getattr(clazz, 'redpanda_serde_compat_version'),
    )]
    innerclasses = inspect.getmembers(clazz, predicate=inspect.isclass)
    for name, clazz in innerclasses:
        if name.startswith("__") and name.endswith("__"):
            continue
        structs += collect_structs(clazz, module_name=module_name)
    return structs

# Phase 1 collect all structs
all_structs: list[SerdeStruct] = []
for module_name, module in modules.items():
    classes = inspect.getmembers(module, predicate=inspect.isclass)
    for _, clazz in classes:
        all_structs += collect_structs(clazz, module_name=module_name)
    print(all_structs)

# Phase 2 collect all RPC methods for each service
for module_name, module in modules.items():
    if not hasattr(module, 'SERVICE_ID'):
        continue
    service_id = module.SERVICE_ID
    funcs = inspect.getmembers(module, predicate=inspect.isfunction)
    for func_name, func in funcs:
        if getattr(func, 'is_redpanda_admin_rpc', False):
            id = getattr(func, 'redpanda_admin_rpc_id')
            hints = typing.get_type_hints(func)
            print(module_name, func_name, id, hints)

# Phase 3 generate code
