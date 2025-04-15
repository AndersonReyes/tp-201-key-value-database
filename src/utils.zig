const std = @import("std");

/// write a message to socket with '\n' as delimier
pub fn write(msg: []const u8, socket: std.net.Stream, allocator: std.mem.Allocator) !void {
    const out = try std.fmt.allocPrint(allocator, "{s}\n", .{msg});
    defer allocator.free(out);
    _ = try socket.writer().write(out);
}

/// read a message from socket until '\n' or eof. caller owns the memory, make sure to free
pub fn read(socket: std.net.Stream, allocator: std.mem.Allocator) !?[]u8 {
    return try socket.reader().readUntilDelimiterOrEofAlloc(allocator, '\n', 1024);
}

pub fn free_optional(allocator: std.mem.Allocator, opt: ?[]const u8) void {
    if (opt) |v| {
        allocator.free(v);
    }
}

/// send a message, wait for reply but discard the reply
pub fn send(msg: []const u8, socket: std.net.Stream, allocator: std.mem.Allocator) !void {
    _ = try write(msg, socket, allocator);
    free_optional(allocator, try read(socket, allocator));
}

/// send a message, wait for reply and return the reply. caller owns the memory
pub fn send_and_receive(msg: []const u8, socket: std.net.Stream, allocator: std.mem.Allocator) !?[]u8 {
    _ = try write(msg, socket, allocator);
    return try read(socket, allocator);
}
