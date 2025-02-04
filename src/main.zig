const std = @import("std");
const log = @import("log_structured.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    const dir = try std.fs.cwd().openDir("test-log", .{});
    var store = try log.LogStructuredStore.init(dir, allocator);
    defer store.deinit();

    try store.set("1", "one");
    try store.set("2", "two");
    try store.set("3", "three");

    try std.testing.expectEqualStrings("two", (try store.get("2")).?);

    try store.set("3", "three-again");
    try store.remove("2");
}

test "simple test" {}
