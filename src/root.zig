const std = @import("std");

const in_memory_store = @import("in_memory_store.zig");

test {
    _ = in_memory_store;
    std.testing.refAllDecls(@This());
    // or refAllDeclsRecursive
}
