const std = @import("std");
const in_memory_store = @import("in_memory_store.zig");

const LogStructuredStore = struct {
    data_dir: std.fs.Dir,

    const Self = @This();

    const LogEntry = struct { key: []const u8, value: ?[]const u8 = null, op: []const u8 };
    const max_row_size: usize = 1024; // 1mb row size (key + value)

    pub fn init(data_dir: std.fs.Dir) LogStructuredStore {
        return LogStructuredStore{
            .data_dir = data_dir,
        };
    }

    /// remove key from the store. Returns boolean if key exist and it was removed
    pub fn remove(self: *Self, key: []const u8) !void {
        var log = try self.data_dir.createFile("log.ndjson", .{ .truncate = false, .exclusive = false });
        defer log.close();
        try log.seekFromEnd(0);

        // TODO: does not check if they key actually exist in the log
        try std.json.stringify(.{ .key = key, .op = "remove" }, .{}, log.writer());
        _ = try log.write("\n");
    }

    /// put a key in the store
    pub fn set(self: *Self, key: []const u8, value: []const u8) !void {
        var log = try self.data_dir.createFile("log.ndjson", .{ .truncate = false, .exclusive = false });
        defer log.close();
        try log.seekFromEnd(0);

        try std.json.stringify(.{ .key = key, .value = value, .op = "set" }, .{}, log.writer());
        _ = try log.write("\n");
    }

    /// retrieve a key from the store
    pub fn get(self: Self, key: []const u8) !?[]const u8 {
        var log = try self.data_dir.createFile("log.ndjson", .{ .read = true, .truncate = false, .exclusive = false });
        defer log.close();

        var gpa = std.heap.GeneralPurposeAllocator(.{}){};
        defer _ = gpa.deinit();
        const allocator = gpa.allocator();

        var buf_reader = std.io.bufferedReader(log.reader());
        const reader = buf_reader.reader();

        var value: ?[]const u8 = null;
        var buf: [1024]u8 = undefined;

        while (try reader.readUntilDelimiterOrEof(&buf, '\n')) |line| {
            // defer allocator.free(line);
            std.log.debug("line = {s}, \n", .{line});

            const parsed = try std.json.parseFromSlice(LogEntry, allocator, line, .{});
            defer parsed.deinit();

            // if the log entry is not the key we looking for, reset to null
            if (std.mem.eql(u8, parsed.value.key, key)) {
                value = parsed.value.value;
            }
        }

        return value;
    }

    /// destroy
    pub fn deinit(_: *Self) void {}
};

test "remove should true when removing a real value" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var store = LogStructuredStore.init(tmp.dir);
    defer store.deinit();

    var log = try tmp.dir.createFile("log.ndjson", .{ .truncate = false, .read = true, .exclusive = false });
    defer log.close();

    try log.seekTo(0);
    try store.remove("2");

    try log.seekTo(0);
    var buf_reader = std.io.bufferedReader(log.reader());
    const reader = buf_reader.reader();
    var buf: [1024]u8 = undefined;
    while (try reader.readUntilDelimiterOrEof(&buf, '\n')) |line| {
        try std.testing.expectEqualStrings("{\"key\":\"2\",\"op\":\"remove\"}", line);
    }
    // else try std.testing.expect(false);
}

test "set should store the value" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var store = LogStructuredStore.init(tmp.dir);
    defer store.deinit();

    var log = try tmp.dir.createFile("log.ndjson", .{ .truncate = false, .exclusive = false });
    defer log.close();

    try log.seekFromEnd(0);

    const writer = log.writer();

    try std.json.stringify(.{ .key = "2", .value = "123456", .op = "set" }, .{}, writer);
    _ = try log.write("\n");

    const actual1 = (try store.get("2")).?;
    try std.testing.expectEqualStrings("123456", actual1);

    try std.json.stringify(.{ .key = "1", .value = "one", .op = "set" }, .{}, writer);
    _ = try log.write("\n");

    const actual2 = (try store.get("1")).?;
    try std.testing.expectEqualStrings("one", actual2);
}

test "get should retrieve the value at key" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var store = LogStructuredStore.init(tmp.dir);
    defer store.deinit();

    var log = try tmp.dir.createFile("log.ndjson", .{ .truncate = true, .exclusive = false });
    defer log.close();

    try log.seekFromEnd(0);

    try std.json.stringify(.{ .key = "2", .value = "1234", .op = "set" }, .{}, log.writer());
    _ = try log.write("\n");

    const actual = (try store.get("2")).?;
    try std.testing.expectEqualStrings("1234", actual);
}

test "get value that does not exist should return null" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var store = LogStructuredStore.init(tmp.dir);
    defer store.deinit();

    var log = try tmp.dir.createFile("log.ndjson", .{ .truncate = true, .exclusive = false });
    defer log.close();

    try log.seekFromEnd(0);

    try std.json.stringify(.{ .key = "2", .value = "1234", .op = "set" }, .{}, log.writer());
    _ = try log.write("\n");

    try std.testing.expectEqual(null, store.get("1"));
    try std.testing.expectEqual(null, store.get("12"));
}
