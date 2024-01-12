module github.com/rockwotj/wasm-transform-testdata

go 1.20

require (
	github.com/actgardner/gogen-avro/v10 v10.2.1
	github.com/rockwotj/redpanda/src/transform-sdk/go/transform v0.0.1
)

replace github.com/rockwotj/redpanda/src/transform-sdk/go/transform => ../../
