const std = @import("std");

pub const LogStructured = struct {
    logs_dir: std.fs.Dir,
    index: std.StringHashMap(u64),
    allocator: std.mem.Allocator,
    /// use this value to keep track of the file sizes as we rotate them. We need to ensure the file
    /// size limit is based on the starting size of the file after compaction.
    /// Otherwise we will always compact the file once it reaches the max size with all
    /// unique entries.
    prev_compaction_size: u64,
    count: usize,

    const Self = @This();

    const log_file = "current.ndjson";
    // TODO: increase later 1MB compaction trigger
    const log_file_size_limit_bytes: u64 = 100000; // 1Kb

    const LogEntry = struct { key: []const u8, value: ?[]const u8 = null, op: []const u8 };
    const max_row_size: usize = 1024; // 1mb row size (key + value)

    var lock: std.Thread.RwLock = .{};

    pub fn init(db_dir: std.fs.Dir, allocator: std.mem.Allocator) !LogStructured {
        db_dir.makeDir("logs") catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };
        const logs_dir = try db_dir.openDir("logs", .{});

        _ = try logs_dir.createFile(log_file, .{ .truncate = false, .exclusive = false });
        const curr_size = (try (try logs_dir.openFile("current.ndjson", .{})).stat()).size;

        var db = LogStructured{ .logs_dir = logs_dir, .index = std.StringHashMap(u64).init(allocator), .allocator = allocator, .prev_compaction_size = curr_size, .count = 0 };
        try db.hydrate_db();

        return db;
    }

    /// move the index (latest update only) to the new log
    fn compaction(self: *Self) !void {
        const old_log = try self.logs_dir.openFile(log_file, .{});
        defer old_log.close();

        const stat = try old_log.stat();

        // only compact if we reached the file size limit
        if (stat.size >= (self.prev_compaction_size + log_file_size_limit_bytes)) {
            const new_name_for_old_log = try std.fmt.allocPrint(
                self.allocator,
                "{d}.ndjson",
                .{std.time.microTimestamp()},
            );
            defer self.allocator.free(new_name_for_old_log);

            try self.logs_dir.rename(log_file, new_name_for_old_log);

            const new_log = try self.logs_dir.createFile(log_file, .{ .truncate = true, .exclusive = true });
            defer new_log.close();

            self.prev_compaction_size = (try (try self.logs_dir.openFile(log_file, .{})).stat()).size;

            // move the index to the new file
            var iterator = self.index.iterator();

            while (iterator.next()) |entry| {
                try old_log.seekTo(entry.value_ptr.*);

                var buf_reader = std.io.bufferedReader(old_log.reader());
                const reader = buf_reader.reader();

                if (try reader.readUntilDelimiterOrEofAlloc(self.allocator, '\n', 1024)) |line| {
                    defer self.allocator.free(line);
                    _ = try new_log.write(line);
                    _ = try new_log.write("\n");
                }
            }

            // delete the old file
            try self.logs_dir.deleteFile(new_name_for_old_log);
        }
    }

    /// remove key from the store. Returns boolean if key exist and it was removed
    pub fn remove(self: *Self, key: []const u8) !void {
        lock.lock();
        defer lock.unlock();

        var log = try self.logs_dir.openFile(log_file, .{ .mode = std.fs.File.OpenMode.write_only });
        defer log.close();
        try log.seekFromEnd(0);

        if (self.index.fetchRemove(key)) |entry| {
            try std.json.stringify(.{ .key = key, .op = "remove" }, .{}, log.writer());
            _ = try log.write("\n");
            self.allocator.free(entry.key);
            self.count -= 1;
            try self.compaction();
        }
    }

    /// put a key in the store
    pub fn set(self: *Self, key: []const u8, value: []const u8) !void {
        // var log = try self.logs_dir.createFile(log_file, .{ .truncate = false, .exclusive = false });
        lock.lock();
        defer lock.unlock();
        var log = try self.logs_dir.openFile(log_file, .{ .mode = std.fs.File.OpenMode.write_only });
        defer log.close();
        try log.seekFromEnd(0);

        const entry = try self.index.getOrPut(key);
        if (!entry.found_existing) {
            entry.key_ptr.* = try self.allocator.dupe(u8, key);
        }
        entry.value_ptr.* = try log.getPos();
        self.count += 1;

        try std.json.stringify(.{ .key = key, .value = value, .op = "set" }, .{}, log.writer());
        _ = try log.write("\n");
        try self.compaction();
    }

    fn hydrate_db(self: *Self) !void {
        self.count = 0;
        var log = try self.logs_dir.openFile(log_file, .{});
        defer log.close();

        // manually keep track of the start position of each line.
        // log.getPos() will not work because we use a buffered reader
        var current_line_start: u64 = 0;
        try log.seekTo(current_line_start);

        var buf_reader = std.io.bufferedReader(log.reader());
        const reader = buf_reader.reader();

        while (try reader.readUntilDelimiterOrEofAlloc(self.allocator, '\n', 1024)) |line| {
            defer self.allocator.free(line);
            const parsed = try std.json.parseFromSlice(LogEntry, self.allocator, line, .{});
            defer parsed.deinit();

            if (std.mem.eql(u8, parsed.value.op, "removed")) {
                _ = self.index.remove(parsed.value.key);
                continue;
            }

            const entry = try self.index.getOrPut(parsed.value.key);
            if (!entry.found_existing) {
                entry.key_ptr.* = try self.allocator.dupe(u8, parsed.value.key);
            }
            entry.value_ptr.* = current_line_start;
            self.count += 1;

            current_line_start += line.len + 1;
        }
    }

    /// retrieve a key from the store. caller owns the memory of
    /// the returned value.
    pub fn get(self: Self, key: []const u8) !?[]const u8 {
        lock.lockShared();
        const offset = self.index.get(key);
        lock.unlockShared();

        if (offset != null) {
            var log = try self.logs_dir.openFile(log_file, .{});
            defer log.close();

            try log.seekTo(offset.?);

            const reader = log.reader();

            if (try reader.readUntilDelimiterOrEofAlloc(self.allocator, '\n', 1024)) |line| {
                defer self.allocator.free(line);
                const parsed = try std.json.parseFromSlice(LogEntry, self.allocator, line, .{});
                defer parsed.deinit();

                if (std.mem.eql(u8, parsed.value.key, key)) {
                    const value = try self.allocator.dupe(u8, parsed.value.value.?);
                    return value;
                }
            }
        }

        return null;
    }

    /// destroy
    pub fn deinit(self: *Self) void {
        var it = self.index.keyIterator();
        while (it.next()) |key| {
            self.allocator.free(key.*);
        }

        self.index.deinit();
        self.count = 0;
    }

    /// number items in the db
    pub fn size(self: Self) usize {
        return self.count;
    }
};

test "db should lookup from disk / hydrate when the value is not in the index" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var store = try LogStructured.init(tmp.dir, std.testing.allocator);
        defer store.deinit();

        try store.set("1", "11");
        try store.set("2", "22");

        const actual1 = (try store.get("2")).?;
        defer std.testing.allocator.free(actual1);
        try std.testing.expectEqualStrings("22", actual1);

        const actual2 = (try store.get("1")).?;
        defer std.testing.allocator.free(actual2);
        try std.testing.expectEqualStrings("11", actual2);
    }

    var new_store = try LogStructured.init(tmp.dir, std.testing.allocator);
    defer new_store.deinit();

    {
        const actual1 = (try new_store.get("2")).?;
        defer std.testing.allocator.free(actual1);
        try std.testing.expectEqualStrings("22", actual1);

        const actual2 = (try new_store.get("1")).?;
        defer std.testing.allocator.free(actual2);
        try std.testing.expectEqualStrings("11", actual2);
    }

    // read again to check the offset put in index is correct
    {
        const actual1 = (try new_store.get("2")).?;
        defer std.testing.allocator.free(actual1);
        try std.testing.expectEqualStrings("22", actual1);

        const actual2 = (try new_store.get("1")).?;
        defer std.testing.allocator.free(actual2);
        try std.testing.expectEqualStrings("11", actual2);
    }
}

test "compaction should work by reducing directory size" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store = try LogStructured.init(tmp.dir, std.testing.allocator);
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
    var store = try LogStructured.init(tmp.dir, std.testing.allocator);
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
    var store = try LogStructured.init(tmp.dir, std.testing.allocator);
    defer store.deinit();

    try store.set("2", "123456");
    try store.set("1", "11");

    var log = try tmp.dir.createFile("logs/current.ndjson", .{ .read = true, .truncate = false, .exclusive = false });
    defer log.close();

    try log.seekTo(0);

    var buf: [1024]u8 = undefined;
    const reader = log.reader();

    const line1 = (try reader.readUntilDelimiterOrEof(&buf, '\n')).?;

    try std.testing.expect(line1.len > 0);
    try std.testing.expectEqualStrings("{\"key\":\"2\",\"value\":\"123456\",\"op\":\"set\"}", line1);
    try std.testing.expectEqual(0, store.index.get("2"));

    const line2 = (try reader.readUntilDelimiterOrEof(&buf, '\n')).?;

    try std.testing.expect(line2.len > 0);
    try std.testing.expectEqualStrings("{\"key\":\"1\",\"value\":\"11\",\"op\":\"set\"}", line2);
    try std.testing.expectEqual(line1.len + 1, store.index.get("1"));
}

test "get should retrieve the value at key" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var store = try LogStructured.init(tmp.dir, std.testing.allocator);
    defer store.deinit();

    try store.set("2", "22");
    try store.set("1", "11");

    const actual = (try store.get("1")).?;
    defer std.testing.allocator.free(actual);
    try std.testing.expectEqualStrings("11", actual);

    const actual2 = (try store.get("2")).?;
    defer std.testing.allocator.free(actual2);
    try std.testing.expectEqualStrings("22", actual2);
}

test "get value that does not exist should return null" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var store = try LogStructured.init(tmp.dir, std.testing.allocator);
    defer store.deinit();

    try std.testing.expectEqual(null, store.get("doesnotexist"));
}
