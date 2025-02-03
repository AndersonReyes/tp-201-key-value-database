const std = @import("std");
const in_memory_store = @import("in_memory_store.zig");

const LogStructuredStore = struct {
    inner_store: in_memory_store.InMemoryStore,
    data_dir: std.fs.Dir,

    const Self = @This();

    const LogEntry = struct { key: []const u8, value: ?[]const u8 = null, op: []const u8 };
    const max_row_size: usize = 1024; // 1mb row size (key + value)

    pub fn init(data_dir: std.fs.Dir, allocator: std.mem.Allocator) LogStructuredStore {
        return LogStructuredStore{
            .inner_store = in_memory_store.InMemoryStore.init(allocator),
            .data_dir = data_dir,
        };
    }

    /// remove key from the store. Returns boolean if key exist and it was removed
    pub fn remove(self: *Self, key: []const u8) !bool {
        var log = try self.data_dir.createFile("log.ndjson", .{ .truncate = false, .exclusive = false });
        defer log.close();
        try log.seekFromEnd(0);

        // TODO: does not check if they key actually exist in the log
        try std.json.stringify(.{ .key = key, .op = "remove" }, .{}, log.writer());
        _ = try log.write("\n");

        return self.inner_store.remove(key);
    }

    /// put a key in the store
    pub fn set(self: *Self, key: []const u8, value: []const u8) !void {
        var log = try self.data_dir.createFile("log.ndjson", .{ .truncate = false, .exclusive = false });
        defer log.close();
        try log.seekFromEnd(0);

        try std.json.stringify(.{ .key = key, .value = value, .op = "set" }, .{}, log.writer());
        _ = try log.write("\n");

        try self.inner_store.set(key, value);
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

        // std.debug.print("key = {s}, value = {s}, op = {s}\n", .{ log_entry.key, log_entry.value, log_entry.op });
        return value;
    }

    /// destroy
    pub fn deinit(self: *Self) void {
        self.data_dir.close();
        self.inner_store.deinit();
    }
};

test "remove should return false when empty" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var store = LogStructuredStore.init(tmp.dir, std.testing.allocator);

    try std.testing.expectEqual(store.remove("invalid"), false);
}

test "remove should true when removing a real value" {
    // var tmp = std.testing.tmpDir(.{});
    // defer tmp.cleanup();
    const dir = try std.fs.cwd().openDir("test-data", .{});
    var store = LogStructuredStore.init(dir, std.testing.allocator);
    defer store.deinit();

    try store.inner_store.set("1", "123456");
    try std.testing.expectEqual(true, store.remove("1"));
    // calling again should not remove anything
    try std.testing.expectEqual(false, store.remove("1"));
}

test "set should store the value" {
    // var tmp = std.testing.tmpDir(.{});
    // defer tmp.cleanup();
    const dir = try std.fs.cwd().openDir("test-data", .{});
    var store = LogStructuredStore.init(dir, std.testing.allocator);
    defer store.deinit();

    try store.set("1", "123456");
    try std.testing.expectEqual(1, store.inner_store.storage.count());
    try std.testing.expectEqual("123456", store.inner_store.get("1"));

    try store.set("1", "123");
    try std.testing.expectEqual("123", store.inner_store.get("1"));
    try std.testing.expectEqual(1, store.inner_store.storage.count());

    try store.set("2", "12322");
    try std.testing.expectEqual("12322", store.inner_store.get("2"));
    try std.testing.expectEqual(2, store.inner_store.storage.count());
}

test "get should retrieve the value at key" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    // const dir = try std.fs.cwd().openDir("test-data", .{});
    var store = LogStructuredStore.init(tmp.dir, std.testing.allocator);
    defer store.deinit();

    try store.inner_store.set("1", "123456");
    try std.testing.expectEqual("123456", store.get("1"));
}

test "get value that does not exist should return null" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var store = LogStructuredStore.init(tmp.dir, std.testing.allocator);
    defer store.deinit();

    try std.testing.expectEqual(null, store.get("1"));
}
