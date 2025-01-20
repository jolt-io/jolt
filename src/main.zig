const std = @import("std");
const os = std.os;
const posix = std.posix;
const linux = os.linux;
const IO_Uring = linux.IoUring;
const io_uring = @import("io/io_uring.zig");
const Loop = io_uring.Loop;
const BufferPool = @import("buffer_pool.zig").BufferPool(.io_uring);

const allocator = std.heap.page_allocator;

const DefaultLoop = Loop(.{
    .io_uring = .{
        .direct_descriptors_mode = true,
        .buffer_pool_mode = true,
    },
});

const Completion = DefaultLoop.Completion;

pub fn main() !void {
    const a = std.math.maxInt(u64);
    std.debug.print("{}\n", .{@as(u8, @intCast(a >> 56))});

    var loop = try DefaultLoop.init();
    defer loop.deinit();

    // sparsely initialize some direct descriptors
    try loop.directDescriptors(.sparse, 128);

    // initialize a buffer pool to be used with recv operations
    var recv_pool = try BufferPool.init(allocator, &loop, 0, 1024, 2);
    defer recv_pool.deinit(allocator, &loop);

    // Create socket
    const socket = try std.posix.socket(std.posix.AF.INET, std.posix.SOCK.STREAM, std.posix.IPPROTO.TCP);
    defer std.posix.close(socket);

    try std.posix.setsockopt(socket, std.posix.SOL.SOCKET, std.posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));

    // has no effect atm
    //if (@hasDecl(std.posix.SO, "REUSEPORT")) {
    //    try std.posix.setsockopt(socket, std.posix.SOL.SOCKET, std.posix.SO.REUSEPORT, &std.mem.toBytes(@as(c_int, 1)));
    //}

    // Start listening for connections
    const addr = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, 8080);
    try std.posix.bind(socket, &addr.any, addr.getOsSockLen());
    try std.posix.listen(socket, 128);

    // From now on, socket is a direct descriptor that can be accessed via int 0.
    // Closing it with `posix.close` has no effect since it belongs to ring now.
    try loop.updateDescriptors(0, &[_]linux.fd_t{socket});

    // Start accepting connections
    var accept_c = Completion{};
    //loop.accept(BufferPool, &recv_pool, &accept_c, 0, onAccept);
    loop.close(&accept_c, 0);

    var timer_c = Completion{};
    loop.timeout(&timer_c);
    //loop.timeout(&timer_c, BufferPool, recv_pool, 1000, onTimeout);

    try loop.run();
}

fn onAccept(recv_pool: *BufferPool, loop: *DefaultLoop, c: *Completion, result: DefaultLoop.AcceptError!io_uring.Socket) void {
    _ = c;
    const fd = result catch unreachable;

    //std.debug.print("got connection, fd: {}\n", .{fd});

    const recv_c = allocator.create(Completion) catch unreachable;
    loop.recv(recv_c, BufferPool, recv_pool, fd, recv_pool, onRecv);
}

fn onRecv(
    userdata: *BufferPool,
    loop: *DefaultLoop,
    completion: *Completion,
    socket: io_uring.Socket,
    buffer_pool: *BufferPool,
    buffer_id: u16,
    result: DefaultLoop.RecvError!u32,
) void {
    _ = userdata;
    _ = loop;
    _ = completion;
    _ = socket;

    const len = result catch |e| switch (e) {
        error.EndOfStream => return,
        else => unreachable,
    };
    _ = len;

    std.debug.print("{}\n", .{buffer_id});

    buffer_pool.put(buffer_id);
}
