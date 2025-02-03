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
    },
});
const Completion = DefaultLoop.Completion;

pub fn main() !void {
    var loop = try DefaultLoop.init();
    defer loop.deinit();

    // create direct descriptors table
    try loop.directDescriptors(.sparse, 2);

    const server = try posix.socket(posix.AF.INET, posix.SOCK.STREAM, posix.IPPROTO.TCP);
    const addr = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, 8080);

    try posix.bind(server, &addr.any, addr.getOsSockLen());
    try posix.listen(server, 128);

    try loop.updateDescriptorsSync(0, &[_]linux.fd_t{server});
    // we don't need the user-space fd anymore
    posix.close(server);

    const server_in_kernel: linux.fd_t = 0;

    var accept_c = Completion{};
    loop.accept(&accept_c, Completion, &accept_c, server_in_kernel, onAccept);

    try loop.run();
}

fn onAccept(
    _: *Completion,
    _: *DefaultLoop,
    _: *Completion,
    result: Completion.OperationType.returnType(.accept),
) void {
    const client = result catch |err| @panic(@errorName(err));

    std.debug.print("{}\n", .{client});
}
