const std = @import("std");

pub const log_level: std.log.Level = .debug;

const in_memory_store = @import("in_memory_store.zig");
const log_structured_store = @import("log_structured.zig");

test {
    _ = in_memory_store;
    _ = log_structured_store;
    std.testing.refAllDeclsRecursive(@This());
    // or refAllDeclsRecursive
}
