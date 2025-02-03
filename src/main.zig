const std = @import("std");
const os = std.os;
const posix = std.posix;
const linux = os.linux;
const IO_Uring = linux.IoUring;
const io_uring = @import("io/io_uring.zig");
const Loop = io_uring.Loop;
const BufferPool = @import("buffer_pool.zig").BufferPool(.io_uring);

const allocator = std.heap.page_allocator;

const DefaultLoop = Loop(.{});
const Completion = DefaultLoop.Completion;

pub fn SendQueue(comptime on_error: *const fn () void) type {
    return struct {
        completion: Completion = .{},

        const Self = @This();

        /// Initializes a send queue.
        pub fn init() Self {
            return .{};
        }

        fn onWrite(
            userdata: *Self,
            loop: *DefaultLoop,
            completion: *Completion,
            buffer: []const u8,
            /// u31 is preferred for coercion
            result: DefaultLoop.SendError!u31,
        ) void {
            _ = userdata;
            _ = loop;
            _ = completion;
            _ = buffer;

            _ = result catch |err| {
                on_error();
                @panic(@errorName(err));
            };
        }

        /// Queues a send operation.
        pub fn write(self: *Self, loop: *DefaultLoop, socket: linux.fd_t, buffer: []const u8, comptime link: DefaultLoop.Link) void {
            loop.send(link, &self.completion, Self, self, socket, buffer, posix.MSG.WAITALL, onWrite);
        }
    };
}

fn onError() void {
    std.debug.print("got error!\n", .{});
}

pub fn main() !void {
    var loop = try DefaultLoop.init();
    defer loop.deinit();

    const stream = try std.net.tcpConnectToHost(allocator, "www.google.com", 80);
    defer stream.close();

    // queues write operations by their calling order
    // this can be implemented via MSG_WAITALL and IOSQE_IO_LINK flags on io_uring
    var sq = SendQueue(onError){};
    sq.write(&loop, stream.handle, "GET / ", .linked);
    sq.write(&loop, stream.handle, "HTTP/1.1", .linked);
    sq.write(&loop, stream.handle, "\r\n\r\n", .unlinked);

    try loop.run();

    var buffer: [8192]u8 = undefined;
    const len = try stream.readAll(&buffer);

    std.debug.print("{s}\n", .{buffer[0..len]});
}
