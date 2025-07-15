# Redpanda Protocol Buffers

This directory contains Protocol Buffer definitions for Redpanda services and APIs. These protobuf files are used to generate C++ code for internal services and external APIs.

## Directory Structure

```
proto/redpanda/
├── core/           # Core Redpanda messages and types go here
│   ├── admin/      # Admin service definitions
│   └── testing/    # Example and test protobuf definitions
└── pbgen/          # Protobuf generation options and RPC configurations
    ├── options.proto # Custom field and file options for C++ generation
    └── rpc.proto    # RPC method options and authentication levels
```

## Writing Redpanda Protobufs

### Basic Structure

Every Redpanda protobuf file should follow this basic structure:

```proto
// Copyright header
syntax = "proto3";

package redpanda.your.package;

import "proto/redpanda/pbgen/options.proto";
// Other imports as needed

option (redpanda.pbgen.cpp_namespace) = "proto::your_namespace";

// Your message and service definitions
```

### Required Imports and Options

1. **Always import the options file**: `import "proto/redpanda/pbgen/options.proto";`

2. **Set the C++ namespace**: Use `option (redpanda.pbgen.cpp_namespace) = "proto::your_namespace";` to control the generated C++ namespace.

### Custom Field Options

Redpanda provides several custom field options for C++ code generation:

#### `ptr` Option
Use `[(redpanda.pbgen.ptr) = true]` for message fields that need indirection (e.g., recursive structures) or you want to know if it's unset:

```proto
message Company {
    message Department {
        optional Budget budget = 3 [(redpanda.pbgen.ptr) = true];
    }
}
```

#### `iobuf` Option  
Use `[(redpanda.pbgen.iobuf) = true]` for string fields that may be larger than 128KiB (bytes fields are already iobufs by default):

```proto
message LargeData {
    string content = 1 [(redpanda.pbgen.iobuf) = true];
}
```

### RPC Services

When defining RPC services, you must import the RPC options and specify authentication requirements:

```proto
import "proto/redpanda/pbgen/rpc.proto";
import "proto/redpanda/core/version.proto";

service YourService {
    rpc YourMethod(YourRequest) returns (YourResponse) {
        option (redpanda.pbgen.rpc) = {
            version: V25_2_0,              // Required: Redpanda version when added
            authz: SUPERUSER,              // Required: Authorization level
            http_route: "/api/v1/your-endpoint";  // Optional: Custom HTTP route
        };
    }
}
```

#### Authentication Levels

- `PUBLIC`: No authentication required
- `USER`: Requires user-level authentication  
- `SUPERUSER`: Requires superuser privileges (default for admin APIs)
- `LEVEL_UNSPECIFIED`: Should not be used

**Note**: APIs should default to `SUPERUSER`. Consult the Redpanda core team before using other authentication levels for admin APIs.

### Version Management

All RPC methods must specify the Redpanda version when they were added using the `redpanda.core.Version` enum:

```proto
import "proto/redpanda/core/version.proto";

option (redpanda.pbgen.rpc) = {
    version: V25_2_0,  // Version when this RPC was added
    // ...
};
```

Add new versions to `proto/redpanda/core/version.proto` as needed.

### Message Design Guidelines

1. **Use descriptive field names**: Choose clear, unambiguous field names
2. **Add comments**: Document your messages and fields, especially public APIs
3. **Use appropriate field numbers**: Start from 1, avoid gaps, reserve deprecated numbers
4. **Consider backward compatibility**: Use `optional` for new fields in existing messages
5. **Use nested messages appropriately**: Group related fields into nested messages when it makes sense

### Example: Complete Service Definition

```proto
// Copyright 2025 Redpanda Data, Inc.
syntax = "proto3";

package redpanda.example.api;

import "proto/redpanda/pbgen/options.proto";
import "proto/redpanda/pbgen/rpc.proto";
import "proto/redpanda/core/version.proto";

option (redpanda.pbgen.cpp_namespace) = "proto::example";

message GetConfigRequest {
    string config_key = 1;
}

message GetConfigResponse {
    string config_value = 1;
    bool is_default = 2;
}

service ConfigService {
    rpc GetConfig(GetConfigRequest) returns (GetConfigResponse) {
        option (redpanda.pbgen.rpc) = {
            version: V25_2_0,
            authn: USER,
            http_route: "/v2/config";
        };
    }
}
```

## Code Generation

The protobuf files in this directory are processed by the custom Redpanda protoc plugin located in `bazel/pbgen/`. This plugin generates C++-specific code that integrates with Redpanda's internal systems.

## Testing

Example protobuf definitions for testing can be found in:
- `proto/redpanda/core/testing/example.proto` - Comprehensive example with various protobuf features
- `src/v/serde/protobuf/tests/` - Test protobuf files used in the test suite

## See Also

- [Protoc Plugin Documentation](../../bazel/pbgen/README.md) - Details about the custom protoc plugin
- [Protocol Buffers Language Guide](https://protobuf.dev/programming-guides/proto3/) - Official protobuf documentation
