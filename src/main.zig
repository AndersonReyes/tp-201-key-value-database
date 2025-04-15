const std = @import("std");
const log = @import("log_structured.zig");
const server = @import("server.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    const dir = try std.fs.cwd().openDir("test-log", .{});
    var store = try log.LogStructured.init(dir, allocator);
    defer store.deinit();

    var pool: std.Thread.Pool = undefined;
    try std.Thread.Pool.init(&pool, .{ .allocator = allocator, .n_jobs = 3, .track_ids = true });
    defer pool.deinit();

    const addr = try std.net.Address.parseIp("127.0.0.1", 54321);
    var s = try server.DbServer.init(&store, addr, allocator, .{ .thread_pool = &pool });
    std.log.info("starting server at: {any}", .{addr});

    try s.start();
}

test "simple test" {}
