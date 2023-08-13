from .base import rpc, i32, serde, stub, ChunkedFIFO

SERVICE_ID = 0

@serde(version=1,compat_version=1)
class HelloRequest:
    """
    An example request saying hello
    """
    id: i32
    msg: str

@serde(version=1,compat_version=1)
class BatchHelloRequest:
    """
    Example that depends on another type
    """
    msg: ChunkedFIFO[HelloRequest]


@serde(version=1,compat_version=1)
class HelloResponse:
    """
    An example response to saying hello
    """
    id: i32
    msg: str

    @serde(version=1,compat_version=1)
    class Batched:
        """
        Example nested type
        """
        ids: ChunkedFIFO[i32]
        msgs: ChunkedFIFO[str]

@rpc(id=1)
def SayHello(req: HelloRequest) -> HelloResponse:
    """
    The simplest example of an admin RPC
    """
    stub()


@rpc(id=2)
def BatchSayHello(req: BatchHelloRequest) -> HelloResponse.Batched:
    stub()
