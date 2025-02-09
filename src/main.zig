const std = @import("std");
const os = std.os;
const posix = std.posix;
const linux = os.linux;
const io_uring = @import("io/io_uring.zig");
const Loop = io_uring.Loop;
const BufferPool = @import("buffer_pool.zig").BufferPool(.io_uring);

const allocator = std.heap.page_allocator;

const DefaultLoop = Loop(.{
    .io_uring = .{
        .direct_descriptors_mode = true,
        .zero_copy_sends = false,
    },
});

const Completion = DefaultLoop.Completion;

//fn onRequest(userdata: *UserData, ctx: *Blitz.HttpContext) !void {
//    const body = try ctx.parseBody(.json);
//
//    ctx.writeHead(.ok, &.{
//        .{ .key = "Content-Type", .value = "text/plain" },
//    });
//
//    // size of a single header
//    // header_key_length + 2 (: ) + header_value_length + 2 (\r\n)
//
//    // has to be in order currently
//    ctx.writeHeader("Connection", "keep-alive");
//
//    // can't send headers from now on
//    ctx.writeBytes("hello, world!");
//    ctx.writeBytes("Goodbye!");
//}

pub fn main() !void {
    var loop = try DefaultLoop.init();
    defer loop.deinit();

    //try Blitz(UserData, .{
    //    .read_buffer_size = 4096,
    //    .write_buffer_size = 4096,
    //    .on_request = onRequest,
    //})
    //    .init(userdata)
    //    .run(try std.net.Address.initIp4(.{ 127, 0, 0, 1 }, 8080));

    // get file descriptor limits
    const rlimit = try posix.getrlimit(.NOFILE);

    // create direct descriptors table
    try loop.directDescriptors(.sparse, @intCast(rlimit.max & std.math.maxInt(u32)));

    const server = try posix.socket(posix.AF.INET, posix.SOCK.STREAM, posix.IPPROTO.TCP);
    const addr = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, 8080);

    try posix.bind(server, &addr.any, addr.getOsSockLen());
    try posix.listen(server, 128);
    try std.posix.setsockopt(server, posix.SOL.SOCKET, posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));

    try loop.updateDescriptorsSync(0, &[_]linux.fd_t{server});
    // we don't need the user-space fd anymore
    posix.close(server);

    const server_in_kernel: linux.fd_t = 0;

    // create a buffer pool for multishot receives
    var pool = try BufferPool.init(allocator, loop, 0, 4096, 4);
    defer pool.deinit(allocator, loop);

    var accept_c = Completion{};
    loop.accept(&accept_c, BufferPool, &pool, server_in_kernel, onAccept);

    std.debug.print("Listening on :8080\n", .{});
    try loop.run();
}

fn onAccept(
    pool: *BufferPool,
    loop: *DefaultLoop,
    _: *Completion,
    result: Completion.OperationType.returnType(.accept),
) void {
    const client = result catch |err| @panic(@errorName(err));

    const c = allocator.create(Completion) catch unreachable;

    loop.recvBufferPool(c, BufferPool, pool, client, pool, onRecvBp);
    //loop.send(.unlinked, c, Completion, c, client, buffer, onSend);
}

fn onRecvBp(
    _: *BufferPool,
    loop: *DefaultLoop,
    completion: *Completion,
    socket: DefaultLoop.Socket,
    buffer_pool: *BufferPool,
    buffer_id: u16,
    result: DefaultLoop.RecvBufferPoolError!usize,
) void {
    _ = loop;
    _ = completion;
    _ = socket;

    const len = result catch |err| @panic(@errorName(err));

    const buffer = buffer_pool.get(buffer_id);
    defer buffer_pool.put(buffer_id);

    std.debug.print("{}\n{s}\n", .{ buffer_id, buffer[0..len] });
}

fn onSend(
    c: *Completion,
    loop: *DefaultLoop,
    _: *Completion,
    socket: DefaultLoop.Socket,
    buffer: []const u8,
) void {
    const len = c.getResult(.send) catch unreachable;
    loop.send(.unlinked, c, Completion, c, socket, buffer[0..len], onSend);
}
