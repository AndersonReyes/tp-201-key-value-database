const std = @import("std");

pub const log_level: std.log.Level = .debug;

test {
    _ = @import("in_memory_store.zig");
    _ = @import("log_structured.zig");
    _ = @import("cmdline.zig");
    std.testing.refAllDeclsRecursive(@This());
    // or refAllDeclsRecursive
}
