const std = @import("std");

/// simple in memory key, value store
pub const InMemoryStore = struct {
    storage: std.StringHashMap([]const u8),

    const Self = @This();

    /// create store in memory. Store is backed by a hashmap with default
    /// generic allocator
    pub fn init(allocator: std.mem.Allocator) InMemoryStore {
        return InMemoryStore{ .storage = std.StringHashMap([]const u8).init(allocator) };
    }

    /// destroy
    pub fn deinit(self: *Self) void {
        self.storage.deinit();
    }

    /// remove key from the store. Returns boolean if key exist and it was removed
    pub fn remove(self: *Self, key: []const u8) !void {
        _ = self.storage.remove(key);
    }

    /// put a key in the store
    pub fn set(self: *Self, key: []const u8, value: []const u8) !void {
        try self.storage.put(key, value);
    }

    /// retrieve a key from the store
    pub fn get(self: *Self, key: []const u8) !?[]const u8 {
        return self.storage.get(key);
    }
};

test "get() should return correct key" {
    var store = InMemoryStore.init(std.testing.allocator);
    defer store.deinit();

    const expected = "567";

    try store.storage.put("5", expected);
    try std.testing.expectEqual((try store.get("5")).?, expected);
}

test "get() should return empty if key is missing" {
    var store = InMemoryStore.init(std.testing.allocator);
    defer store.deinit();

    try std.testing.expectEqual(store.get("5"), null);
}

test "set() should set the key to it's proper value" {
    var store = InMemoryStore.init(std.testing.allocator);
    defer store.deinit();

    const expected = "value";

    try std.testing.expectEqual(store.storage.count(), 0);

    try store.set("test", expected);

    try std.testing.expectEqual(store.storage.get("test").?, expected);
}

test "set() should overwrite existing value" {
    var store = InMemoryStore.init(std.testing.allocator);
    defer store.deinit();

    const expected = "value";

    try std.testing.expectEqual(store.storage.count(), 0);

    try store.set("test", expected);

    try std.testing.expectEqual(store.storage.get("test").?, expected);

    const expected2 = "new value";

    try store.set("test", expected2);

    try std.testing.expectEqual(store.storage.count(), 1);
    try std.testing.expectEqual(store.storage.get("test").?, expected2);
}

test "remove() should remove existing key" {
    var store = InMemoryStore.init(std.testing.allocator);
    defer store.deinit();

    try std.testing.expectEqual(store.storage.count(), 0);
    try store.set("test", "value");
    try std.testing.expectEqual(store.storage.count(), 1);

    try std.testing.expectEqual(store.storage.remove("test"), true);
    try std.testing.expectEqual(store.storage.count(), 0);
    try std.testing.expectEqual(store.storage.remove("test"), false);
}
