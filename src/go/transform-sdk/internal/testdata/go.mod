module example.com/schema-registry

go 1.20

require (
	github.com/actgardner/gogen-avro/v10 v10.2.1
	github.com/rockwotj/redpanda/src/go/sdk v0.0.0
)

replace github.com/rockwotj/redpanda/src/go/sdk => ../../