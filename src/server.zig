const std = @import("std");
const log = @import("log_structured.zig");
const utils = @import("utils.zig");

pub const Options = struct {
    thread_pool: ?*std.Thread.Pool = null,
};

/// Write msg to socket with tries
pub const DbServer = struct {
    db: *log.LogStructured,
    server: std.net.Server,
    allocator: std.mem.Allocator,
    thread_pool: ?*std.Thread.Pool,
    wait_group: std.Thread.WaitGroup,

    const Self = @This();

    pub fn init(db: *log.LogStructured, host: std.net.Address, allocator: std.mem.Allocator, options: Options) !DbServer {
        const server = try host.listen(.{ .reuse_address = true });

        const store_server = DbServer{
            .db = db,
            .server = server,
            .allocator = allocator,
            .thread_pool = options.thread_pool,
            .wait_group = .{},
        };

        return store_server;
    }

    pub fn run_command(self: *Self, values: []const []const u8, socket: std.net.Stream) !void {
        const cmd = values[0];

        if (std.mem.eql(u8, cmd, "get")) {
            if (try self.db.get(values[1])) |v| {
                defer self.allocator.free(v);
                try utils.write(v, socket, self.allocator);
            } else {
                const err = try std.fmt.allocPrint(self.allocator, "key not found!: {s}", .{values[1]});
                defer self.allocator.free(err);
                try utils.write(err, socket, self.allocator);
            }
        } else if (std.mem.eql(u8, cmd, "set")) {
            try self.db.set(values[1], values[2]);
            try utils.write("key set!", socket, self.allocator);
        } else if (std.mem.eql(u8, cmd, "remove")) {
            try self.db.remove(values[1]);
            try utils.write("key removed!", socket, self.allocator);
        } else {
            try utils.write("invalid command! Try again.", socket, self.allocator);
        }
    }

    pub fn deinit(self: *Self) void {
        self.wait_group.wait();
        self.server.deinit();
    }

    fn handle(self: *Self, conn: std.net.Server.Connection) !void {
        const line_opt = try utils.read(conn.stream, self.allocator);
        defer utils.free_optional(self.allocator, line_opt);

        if (line_opt) |line| {
            var commands = std.ArrayList([]const u8).init(self.allocator);
            defer {
                for (commands.items) |v| {
                    self.allocator.free(v);
                }
                commands.deinit();
            }

            var values = std.mem.splitScalar(u8, line, ' ');

            while (values.next()) |v| {
                try commands.append(v);
            }

            const slice = try commands.toOwnedSlice();
            defer self.allocator.free(slice);

            try self.run_command(slice, conn.stream);
        }
    }

    pub fn accept(self: *Self) !void {
        var client = try self.server.accept();
        defer client.stream.close();
        try self.handle(client);
    }

    pub fn start(self: *Self) !void {
        while (true) {
            const conn = self.server.accept() catch |err| {
                std.log.err("accept error: {?}", .{err});

                continue;
            };

            self.thread_pool.?.spawnWgId(&self.wait_group, struct {
                fn run(id: usize, s: *DbServer, connection: std.net.Server.Connection) void {
                    defer connection.stream.close();
                    std.log.debug("[thread {d}] connection started.", .{id});
                    s.handle(connection) catch |err| {
                        std.log.err("error handling request: {any}", .{err});
                    };
                }
            }.run, .{ self, conn });
        }
    }
};

test "server can receive requests" {
    const localhost = try std.net.Address.parseIp("127.0.0.1", 54321);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var inner = try log.LogStructured.init(tmp.dir, std.testing.allocator);
    defer inner.deinit();

    try inner.set("hello", "world");

    var server = try DbServer.init(&inner, localhost, std.testing.allocator, .{});
    defer server.deinit();

    const command = "can you be reached?";

    const S = struct {
        fn clientFn(server_address: std.net.Address) !void {
            const socket = try std.net.tcpConnectToAddress(server_address);
            defer socket.close();

            try utils.write(command, socket, std.testing.allocator);
        }
    };

    const t = try std.Thread.spawn(.{}, S.clientFn, .{server.server.listen_address});
    defer t.join();

    var client = try server.server.accept();
    defer client.stream.close();

    const actual = try utils.read(client.stream, std.testing.allocator);
    defer utils.free_optional(std.testing.allocator, actual);

    try std.testing.expectEqualStrings(command, actual.?);
}

test "run command: get" {
    const localhost = try std.net.Address.parseIp("127.0.0.1", 54321);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var inner = try log.LogStructured.init(tmp.dir, std.testing.allocator);
    defer inner.deinit();

    try inner.set("hello", "world");

    var server = try DbServer.init(&inner, localhost, std.testing.allocator, .{});
    defer server.deinit();

    const S = struct {
        fn clientFn(server_address: std.net.Address) !void {
            const socket = try std.net.tcpConnectToAddress(server_address);
            defer socket.close();

            try utils.write("get hello", socket, std.testing.allocator);

            const actual = try utils.read(socket, std.testing.allocator);
            defer utils.free_optional(std.testing.allocator, actual);

            const expected = "world";

            try std.testing.expectEqualStrings(expected, actual.?);
        }
    };

    const t = try std.Thread.spawn(.{}, S.clientFn, .{server.server.listen_address});
    defer t.join();

    _ = try server.accept();
}

test "run command: set" {
    const localhost = try std.net.Address.parseIp("127.0.0.1", 54321);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var inner = try log.LogStructured.init(tmp.dir, std.testing.allocator);
    defer inner.deinit();

    try std.testing.expectEqual(0, inner.size());

    var server = try DbServer.init(&inner, localhost, std.testing.allocator, .{});
    defer server.deinit();

    const S = struct {
        fn clientFn(server_address: std.net.Address) !void {
            const socket = try std.net.tcpConnectToAddress(server_address);
            defer socket.close();

            const reply1 = try utils.send_and_receive("set hello thisismyworld", socket, std.testing.allocator);
            defer utils.free_optional(std.testing.allocator, reply1);
            try std.testing.expectEqualStrings("key set!", reply1.?);
        }
    };

    // scoped thread run so we can continue asserting after the accept.
    {
        const t = try std.Thread.spawn(.{}, S.clientFn, .{server.server.listen_address});
        defer t.join();
        try server.accept();
    }

    try std.testing.expectEqual(1, inner.size());
    const actual = try inner.get("hello");
    defer utils.free_optional(std.testing.allocator, actual);
    try std.testing.expectEqualStrings("thisismyworld", actual.?);
}

test "run command: remove" {
    const localhost = try std.net.Address.parseIp("127.0.0.1", 54321);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var inner = try log.LogStructured.init(tmp.dir, std.testing.allocator);
    defer inner.deinit();
    try inner.set("hello", "this is a new value");

    var server = try DbServer.init(&inner, localhost, std.testing.allocator, .{});
    defer server.deinit();

    const S = struct {
        fn clientFn(server_address: std.net.Address) !void {
            const socket = try std.net.tcpConnectToAddress(server_address);
            defer socket.close();

            const reply1 = try utils.send_and_receive("remove hello", socket, std.testing.allocator);
            defer utils.free_optional(std.testing.allocator, reply1);
            try std.testing.expectEqualStrings("key removed!", reply1.?);
        }
    };

    // scoped thread run so we can continue asserting after the accept.
    {
        const t = try std.Thread.spawn(.{}, S.clientFn, .{server.server.listen_address});
        defer t.join();
        try server.accept();
    }

    try std.testing.expectEqual(0, inner.size());
}
