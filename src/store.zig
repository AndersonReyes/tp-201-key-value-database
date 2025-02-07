const log_store = @import("log_structured.zig");
const in_memory_store = @import("in_memory_store.zig");
const std = @import("std");

const GetFn = *const fn (ptr: *anyopaque, key: []const u8) anyerror!?[]const u8;
const RemoveFn = *const fn (ptr: *anyopaque, key: []const u8) anyerror!void;
const SetFn = *const fn (ptr: *anyopaque, key: []const u8, value: []const u8) anyerror!void;

/// Store interface. Does not hold any state so no need to memory manage.
pub const Store = union(enum) {
    log: *log_store.LogStructured,
    in_memory: *in_memory_store.InMemoryStore,

    const Self = @This();

    fn get(self: Self, key: []const u8) !?[]const u8 {
        return switch (self) {
            .log => |log| return log.get(key),
            .in_memory => |in_memory| return in_memory.get(key),
        };
    }

    fn set(self: Self, key: []const u8, value: []const u8) !void {
        return switch (self) {
            .log => |log| return log.set(key, value),
            .in_memory => |in_memory| return in_memory.set(key, value),
        };
    }

    fn remove(self: Self, key: []const u8) !void {
        return switch (self) {
            .log => |log| return log.remove(key),
            .in_memory => |in_memory| return in_memory.remove(key),
        };
    }

    pub fn logStore(log: *log_store.LogStructured) Store {
        return Store{
            .log = log,
        };
    }

    pub fn inMemoryStore(in_memory: *in_memory_store.InMemoryStore) Store {
        return Store{
            .in_memory = in_memory,
        };
    }
};

test "log store get should retrieve the value at key" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var inner = try log_store.LogStructured.init(tmp.dir, std.testing.allocator);
    defer inner.deinit();
    try inner.set("1", "11");

    var store = Store.logStore(&inner);

    const actual = (try store.get("1")).?;
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings("11", actual);
}

test "log store set should set the value" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var inner = try log_store.LogStructured.init(tmp.dir, std.testing.allocator);
    defer inner.deinit();
    try inner.set("1", "11");

    const store = Store.logStore(&inner);

    try store.set("1", "321");
    const actual = (try store.get("1")).?;
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings("321", actual);
}

test "log store remove should remove the set value" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var inner = try log_store.LogStructured.init(tmp.dir, std.testing.allocator);
    defer inner.deinit();
    try inner.set("1", "11");

    const store = Store.logStore(&inner);
    try store.remove("1");

    try std.testing.expectEqual(null, try store.get("1"));
}

test "memory store get should retrieve the value at key" {
    var inner = in_memory_store.InMemoryStore.init(std.testing.allocator);
    defer inner.deinit();
    try inner.set("1", "11");

    var store = Store.inMemoryStore(&inner);

    const actual = (try store.get("1")).?;

    try std.testing.expectEqualStrings("11", actual);
}

test "memory store set should set the value" {
    var inner = in_memory_store.InMemoryStore.init(std.testing.allocator);
    defer inner.deinit();
    try inner.set("1", "11");

    const store = Store.inMemoryStore(&inner);

    try store.set("1", "321");
    const actual = (try store.get("1")).?;

    try std.testing.expectEqualStrings("321", actual);
}

test "memory store remove should remove the set value" {
    var inner = in_memory_store.InMemoryStore.init(std.testing.allocator);
    defer inner.deinit();
    try inner.set("1", "11");

    const store = Store.inMemoryStore(&inner);
    try store.remove("1");

    try std.testing.expectEqual(null, try store.get("1"));
}
