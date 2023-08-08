CGO_CFLAGS="-I$PWD/WasmEdge-0.13.2-Linux/include" CGO_LDFLAGS="-L$PWD/WasmEdge-0.13.2-Linux/lib64" LD_LIBRARY_PATH="$PWD/WasmEdge-0.13.2-Linux/lib64/" go test -bench=. -tags=wasmedge
