const std = @import("std");
const in_memory_store = @import("in_memory_store.zig");

pub const LogStructuredStore = struct {
    logs_dir: std.fs.Dir,
    index: std.StringHashMap(u64),
    allocator: std.mem.Allocator,
    log_file: []const u8,
    prev_compaction_size: u64,

    const Self = @This();

    // TODO: increase later 1MB compaction trigger
    const log_file_size_limit_bytes: u64 = 100000; // 1Kb

    const LogEntry = struct { key: []const u8, value: ?[]const u8 = null, op: []const u8 };
    const max_row_size: usize = 1024; // 1mb row size (key + value)

    pub fn init(db_dir: std.fs.Dir, allocator: std.mem.Allocator) !LogStructuredStore {
        db_dir.makeDir("logs") catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };
        const log_file = "current.ndjson";
        const logs_dir = try db_dir.openDir("logs", .{});

        _ = try logs_dir.createFile(log_file, .{ .truncate = false, .exclusive = false });
        const curr_size = (try (try logs_dir.openFile("current.ndjson", .{})).stat()).size;

        return LogStructuredStore{ .logs_dir = logs_dir, .index = std.StringHashMap(u64).init(allocator), .allocator = allocator, .log_file = log_file, .prev_compaction_size = curr_size };
    }

    /// check if the log has passed the max size and rotate if yes
    fn checkAndRotateLog(self: *Self) !void {
        const new_name_for_old_log = try std.fmt.allocPrint(
            self.allocator,
            "{d}.ndjson",
            .{std.time.microTimestamp()},
        );
        defer self.allocator.free(new_name_for_old_log);

        const old_log = try self.logs_dir.openFile(self.log_file, .{});
        const stat = try old_log.stat();

        if (stat.size >= (self.prev_compaction_size + log_file_size_limit_bytes)) {
            try self.logs_dir.rename(self.log_file, new_name_for_old_log);
            const new_log = try self.logs_dir.createFile(self.log_file, .{ .truncate = true, .exclusive = true });

            try self.compaction(old_log, new_log);

            new_log.close();
            old_log.close();

            // delete the old file
            try self.logs_dir.deleteFile(new_name_for_old_log);

            self.prev_compaction_size = (try (try self.logs_dir.openFile(self.log_file, .{})).stat()).size;
        }
    }

    /// move the index to the new log
    fn compaction(self: *Self, old_log: std.fs.File, new_log: std.fs.File) !void {
        var iterator = self.index.iterator();

        while (iterator.next()) |entry| {
            try old_log.seekTo(entry.value_ptr.*);

            var buf_reader = std.io.bufferedReader(old_log.reader());
            const reader = buf_reader.reader();
            var buf: [1024]u8 = undefined;

            if (try reader.readUntilDelimiterOrEof(&buf, '\n')) |line| {
                _ = try new_log.write(line);
                _ = try new_log.write("\n");
            }
        }
    }

    /// remove key from the store. Returns boolean if key exist and it was removed
    pub fn remove(self: *Self, key: []const u8) !void {
        var log = try self.logs_dir.openFile(self.log_file, .{ .mode = std.fs.File.OpenMode.write_only });
        defer log.close();
        try log.seekFromEnd(0);

        if (self.index.contains(key)) {
            try std.json.stringify(.{ .key = key, .op = "remove" }, .{}, log.writer());
            _ = try log.write("\n");
        }
        _ = self.index.remove(key);
        try self.checkAndRotateLog();
    }

    /// put a key in the store
    pub fn set(self: *Self, key: []const u8, value: []const u8) !void {
        // var log = try self.logs_dir.createFile(self.log_file, .{ .truncate = false, .exclusive = false });
        var log = try self.logs_dir.openFile(self.log_file, .{ .mode = std.fs.File.OpenMode.write_only });
        defer log.close();
        try log.seekFromEnd(0);

        try self.index.put(key, try log.getPos());
        try std.json.stringify(.{ .key = key, .value = value, .op = "set" }, .{}, log.writer());
        _ = try log.write("\n");
        try self.checkAndRotateLog();
    }

    /// retrieve a key from the store
    pub fn get(self: Self, key: []const u8) !?[]const u8 {
        if (self.index.get(key)) |offset| {
            var log = try self.logs_dir.openFile(self.log_file, .{});
            defer log.close();

            try log.seekTo(offset);

            var buf_reader = std.io.bufferedReader(log.reader());
            const reader = buf_reader.reader();

            var value: ?[]const u8 = null;
            var buf: [1024]u8 = undefined;

            if (try reader.readUntilDelimiterOrEof(&buf, '\n')) |line| {
                const parsed = try std.json.parseFromSlice(LogEntry, self.allocator, line, .{});
                defer parsed.deinit();

                // if the log entry is not the key we looking for, reset to nul
                if (std.mem.eql(u8, parsed.value.key, key)) {
                    value = parsed.value.value;
                }
            }

            return value;
        }

        return null;
    }

    /// destroy
    pub fn deinit(self: *Self) void {
        self.index.deinit();
    }
};

test "compaction should work by reducing directory size" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var store = try LogStructuredStore.init(tmp.dir, std.testing.allocator);
    defer store.deinit();

    var prev_size = (try (try tmp.dir.openFile("logs/current.ndjson", .{})).stat()).size;

    var compacted = false;

    for (0..100000) |i| {
        const k = try std.fmt.allocPrint(
            std.testing.allocator,
            "{d}",
            .{i},
        );
        defer std.testing.allocator.free(k);

        try store.set("1", k);

        const curr_size = (try (try tmp.dir.openFile("logs/current.ndjson", .{})).stat()).size;

        // if compaction was triggered, the size of the directory should decrease
        if (curr_size < prev_size) {
            compacted = true;
            break;
        } else {
            prev_size = curr_size;
        }
    }

    try std.testing.expect(compacted);
}

test "remove should true when removing a real value" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var store = try LogStructuredStore.init(tmp.dir, std.testing.allocator);
    defer store.deinit();

    var log = try tmp.dir.createFile("logs/current.ndjson", .{ .truncate = false, .read = true, .exclusive = false });
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
    var store = try LogStructuredStore.init(tmp.dir, std.testing.allocator);
    defer store.deinit();

    var log = try tmp.dir.createFile("logs/current.ndjson", .{ .truncate = false, .exclusive = false });
    defer log.close();

    try log.seekFromEnd(0);

    const writer = log.writer();

    const pos1 = try log.getPos();
    try store.index.put("2", pos1);
    try std.json.stringify(.{ .key = "2", .value = "123456", .op = "set" }, .{}, writer);
    _ = try log.write("\n");

    const actual1 = (try store.get("2")).?;
    try std.testing.expectEqualStrings("123456", actual1);

    const pos2 = try log.getPos();
    try store.index.put("1", pos2);
    try std.json.stringify(.{ .key = "1", .value = "one", .op = "set" }, .{}, writer);
    _ = try log.write("\n");

    const actual2 = (try store.get("1")).?;
    try std.testing.expectEqualStrings("one", actual2);
    try std.testing.expectEqual(pos2, store.index.get("1"));
}

test "get should retrieve the value at key" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var store = try LogStructuredStore.init(tmp.dir, std.testing.allocator);
    defer store.deinit();

    var log = try tmp.dir.createFile("logs/current.ndjson", .{ .truncate = true, .exclusive = false });
    defer log.close();

    try log.seekFromEnd(0);

    try store.index.put("2", try log.getPos());

    try std.json.stringify(.{ .key = "2", .value = "1234", .op = "set" }, .{}, log.writer());
    _ = try log.write("\n");

    const actual = (try store.get("2")).?;
    try std.testing.expectEqualStrings("1234", actual);
}

test "get value that does not exist should return null" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var store = try LogStructuredStore.init(tmp.dir, std.testing.allocator);
    defer store.deinit();

    var log = try tmp.dir.createFile("logs/current.ndjson", .{ .truncate = true, .exclusive = false });
    defer log.close();

    try log.seekFromEnd(0);

    try store.index.put("2", try log.getPos());
    try std.json.stringify(.{ .key = "2", .value = "1234", .op = "set" }, .{}, log.writer());
    _ = try log.write("\n");

    try std.testing.expectEqual(null, store.get("1"));
    try std.testing.expectEqual(null, store.get("12"));
}
