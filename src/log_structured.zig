const std = @import("std");
const in_memory_store = @import("in_memory_store.zig");

const LogStructuredStore = struct {

	inner_store: in_memory_store.InMemoryStore,

	const Self = @This();

	pub fn init() LogStructuredStore {
		return LogStructuredStore {
			.inner_store = in_memory_store.InMemoryStore.init(),
		};
	}

	/// remove key from the store. Returns boolean if key exist and it was removed
  pub fn remove(self: *Self, key: []const u8) bool {
		return self.inner_store.remove(key);
	}

	/// put a key in the store
  pub fn set(self: *Self, key: []const u8, value: []const u8) !void {
		try self.inner_store.set(key, value);
	}


	/// retrieve a key from the store
  pub fn get(self: Self, key: []const u8) ?[]const u8 {
		return self.inner_store.get(key);
	}
};

test "remove should return false when empty" {
	var store = LogStructuredStore.init();
	try std.testing.expectEqual(store.remove("invalid"), false);
}

test "remove should true when removing a real value" {
	var store = LogStructuredStore.init();

	try  store.inner_store.set("1", "123456");
	try std.testing.expectEqual(true, store.remove("1"));
	// calling again should not remove anything
  try std.testing.expectEqual(false, store.remove("1"));
}

test "set should store the value" {
	var store = LogStructuredStore.init();

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
	var store = LogStructuredStore.init();

	try  store.inner_store.set("1", "123456");
	try std.testing.expectEqual("123456", store.get("1"));
}

test "get value that does not exist should return null" {
	var store = LogStructuredStore.init();
	try std.testing.expectEqual(null, store.get("1"));
}
