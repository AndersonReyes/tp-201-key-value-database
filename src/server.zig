const std = @import("std");
const log = @import("log_structured.zig");

/// Write msg to socket with tries
pub const DbServer = struct {
    db: log.LogStructured,
    server: std.net.Server,
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(db: log.LogStructured, host: std.net.Address, allocator: std.mem.Allocator) !DbServer {
        const server = try host.listen(.{ .reuse_address = true });

        const store_server = DbServer{ .db = db, .server = server, .allocator = allocator };

        return store_server;
    }

    pub fn run_command(self: *Self, values: []const []const u8, socket: std.net.Stream) !void {
        var i: usize = 0;

        while (i < values.len) {
            const cmd = values[i];

            if (std.mem.eql(u8, cmd, "get")) {
                if (i + 1 > values.len) {
                   try socket.writeAll("missing key to get");
                }

                if (try self.db.get(values[i + 1])) |v| {
                    defer self.allocator.free(v);
                    try socket.writeAll(v);
                    i += 1;
                } else {
                    const err = try std.fmt.allocPrint(self.allocator, "key not found!: {s}", .{values[i + 1]});
                    defer self.allocator.free(err);
                    try socket.writeAll(err);
                }
            }

            i += 1;
        }
    }

    pub fn deinit(self: *Self) void {
        self.server.deinit();
    }

    pub fn accept(self: *Self) !void {
        var client = try self.server.accept();
        defer client.stream.close();

        var buf: [1024]u8 = undefined;
        _ = try client.stream.reader().read(&buf);

        var commands = std.ArrayList([]const u8).init(self.allocator);
        defer {
            for (commands.items) |str| {
                std.testing.allocator.free(str);
            }
            commands.deinit();
        }

        var values = std.mem.splitScalar(u8, buf[0..], ' ');

        while (values.next()) |v| {
            try commands.append(v);
        }

        const slice = try commands.toOwnedSlice();
        defer self.allocator.free(slice);

        try self.run_command(slice, client.stream);
    }
};

test "server can receive requests" {
    const localhost = try std.net.Address.parseIp("127.0.0.1", 54321);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var inner = try log.LogStructured.init(tmp.dir, std.testing.allocator);
    defer inner.deinit();

    try inner.set("hello", "world");

    var server = try DbServer.init(inner, localhost, std.testing.allocator);
    defer server.deinit();

    const command = "can you be reached?";

    const S = struct {
        fn clientFn(server_address: std.net.Address) !void {
            const socket = try std.net.tcpConnectToAddress(server_address);
            defer socket.close();

            _ = try socket.writer().write(command);
        }
    };

    const t = try std.Thread.spawn(.{}, S.clientFn, .{server.server.listen_address});
    defer t.join();

    var client = try server.server.accept();
    defer client.stream.close();

    var buf: [command.len]u8 = undefined;
    _ = try client.stream.reader().read(&buf);

    try std.testing.expectEqualSlices(u8, command, buf[0..]);
}

test "run command: get" {
    const localhost = try std.net.Address.parseIp("127.0.0.1", 54321);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var inner = try log.LogStructured.init(tmp.dir, std.testing.allocator);
    defer inner.deinit();

    try inner.set("hello", "world");

    var server = try DbServer.init(inner, localhost, std.testing.allocator);
    defer server.deinit();

    const S = struct {
        fn clientFn(server_address: std.net.Address) !void {
            const socket = try std.net.tcpConnectToAddress(server_address);
            defer socket.close();

            _ = try socket.writer().write("get hello");

            var buf: [40]u8 = undefined;
            _ = try socket.reader().readAll(&buf);
            std.debug.print("slice allocated: {s}", .{buf});

            const expected = "world";

            try std.testing.expectEqualStrings(expected, buf[0..]);
        }
    };

    const t = try std.Thread.spawn(.{}, S.clientFn, .{server.server.listen_address});
    defer t.join();

    _ = try server.accept();
}
