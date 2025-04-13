const std = @import("std");
const log = @import("log_structured.zig");
const cmdline = @import("cmdline.zig");

pub fn main() !void {
    // var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    // const allocator = gpa.allocator();
    //

    // const dir = try std.fs.cwd().openDir("test-log", .{});
    // var store = try log.LogStructured.init(dir, allocator);
    // defer store.deinit();
    //
    // var prev_size = (try (try dir.openFile("logs/current.ndjson", .{})).stat()).size;
    //
    // var compacted = false;
    //
    // for (0..1000) |i| {
    //     const k = try std.fmt.allocPrint(
    //         allocator,
    //         "{d}",
    //         .{i},
    //     );
    //     defer allocator.free(k);
    //
    //     try store.set("1", k);
    //
    //     const current_size = (try (try dir.openFile("logs/current.ndjson", .{})).stat()).size;
    //
    //     // if compaction was triggered, the size of the directory should decrease
    //     if (current_size < prev_size) {
    //         compacted = true;
    //         break;
    //     } else {
    //         prev_size = current_size;
    //     }
    // }
    cmdline.help();
}

test "simple test" {}
