const std = @import("std");
const log = @import("log_structured.zig");
const server = @import("server.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    const dir = try std.fs.cwd().openDir("test-log", .{});
    var store = try log.LogStructured.init(dir, allocator);
    defer store.deinit();

    const addr = try std.net.Address.parseIp("127.0.0.1", 54321);
    var s = try server.DbServer.init(store, addr, allocator);
    std.log.info("starting server at: {any}", .{addr});

    while (true) {
        try s.accept();
    }
}

test "simple test" {}
