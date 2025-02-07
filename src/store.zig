const log_store = @import("log_structured.zig");
const std = @import("std");

/// wrapper interface for store
pub const Store = struct {
    fn_get: *const fn (key: []const u8) anyerror!?[]const u8,
    fn_remove: *const fn (key: []const u8) anyerror!void,
    fn_set: *const fn (key: []const u8, value: []const u8) anyerror!void,

    const Self = @This();

    /// remove key from the store. Returns boolean if key exist and it was removed
    pub fn remove(self: *Self, key: []const u8) !void {
        try self.remove(key);
    }

    /// put a key in the store
    pub fn set(self: *Self, key: []const u8, value: []const u8) !void {
        try self.fn_set(key, value);
    }

    /// retrieve a key from the store. caller owns the memory of
    /// the returned value.
    pub fn get(self: *Self, key: []const u8) !?[]const u8 {
        return try self.fn_get(key);
    }

    pub fn init(store: log_store.LogStructuredStore) !Store {
        return try Store{
            .fn_get = store.get,
            .fn_remove = store.remove,
            .fn_set = store.set,
        };
    }
};

test "get should retrieve the value at key" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var inner = try log_store.LogStructuredStore.init(tmp.dir, std.testing.allocator);
    defer inner.deinit();

    const store = Store.init(inner);
    std.testing.expectEqual(inner.get, store.get);
}
