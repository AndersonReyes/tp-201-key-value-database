const std = @import("std");

pub fn main() !void {
    var dir = try std.fs.cwd().openDir("test-data", .{});
    var log = try dir.createFile("log.ndjson", .{ .read = true, .truncate = false, .exclusive = false });
    defer log.close();

    var buf_reader = std.io.bufferedReader(log.reader());
    const reader = buf_reader.reader();

    var buf: [1024]u8 = undefined;

    while (try reader.readUntilDelimiterOrEof(&buf, '\n')) |line| {
        std.debug.print("line = {s}, \n", .{line});
    }
}

test "simple test" {
    var list = std.ArrayList(i32).init(std.testing.allocator);
    defer list.deinit(); // try commenting this out and see if zig detects the memory leak!
    try list.append(42);
    try std.testing.expectEqual(@as(i32, 42), list.pop());
}
