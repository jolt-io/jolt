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
        .direct_descriptors_mode = false,
        .buffer_pool_mode = false,
    },
});

const Completion = DefaultLoop.Completion;
const Fiber = @import("fiber.zig").Fiber(Completion);

threadlocal var loop1: DefaultLoop = undefined;

pub const tcp = struct {
    pub fn connect(socket: linux.fd_t, addr: std.net.Address) !void {
        // request a connect operation
        loop1.connect(Fiber, Fiber.current().?, Fiber.completion(), socket, addr, invoker);
        // stop the execution of the calling fiber
        Fiber.yield();

        // when we got here, operation is finished

        return;
    }

    pub fn write(socket: linux.fd_t, buffer: []const u8) !void {
        loop1.send(Fiber.completion(), Fiber, Fiber.current().?, socket, buffer, onSend1);

        Fiber.yield();

        return;
    }

    pub fn read(socket: linux.fd_t, buffer: []u8) !void {
        loop1.recv(Fiber.completion(), Fiber, Fiber.current().?, socket, buffer, onRecv1);

        Fiber.yield();

        return;
    }
};

fn invoker(state: *Fiber, _: *DefaultLoop, _: *Completion) void {
    // requested operation has finished, resume the fiber back
    Fiber.switchTo(state);
}

fn onSend1(
    state: *Fiber,
    loop: *DefaultLoop,
    completion: *Completion,
    buffer: []const u8,
    /// u31 is preferred for coercion
    result: DefaultLoop.SendError!u31,
) void {
    _ = loop;
    _ = completion;
    _ = buffer;
    _ = result catch unreachable;

    Fiber.switchTo(state);
}

fn onRecv1(
    state: *Fiber,
    loop: *DefaultLoop,
    completion: *Completion,
    socket: linux.fd_t,
    buffer: []u8,
    /// u31 is preferred for coercion
    result: DefaultLoop.RecvError!u31,
) void {
    _ = loop;
    _ = completion;
    _ = socket;
    _ = buffer;
    _ = result catch unreachable;

    Fiber.switchTo(state);
}

fn fiberFn(ip: []const u8) void {
    const socket = posix.socket(posix.AF.INET, posix.SOCK.STREAM, posix.IPPROTO.TCP) catch unreachable;
    defer posix.close(socket);

    const addr = std.net.Address.parseIp(ip, 80) catch unreachable;

    _ = tcp.connect(socket, addr) catch unreachable;

    _ = tcp.write(socket, "GET / HTTP/1.1\r\n\r\n") catch unreachable;

    var buffer: [1024]u8 = undefined;
    tcp.read(socket, &buffer) catch unreachable;

    std.debug.print("{s}\n", .{buffer[0..]});
}

pub fn main() !void {
    loop1 = try DefaultLoop.init();
    defer loop1.deinit();

    const stack = try allocator.alignedAlloc(u8, Fiber.stack_alignment, 16 * 1024);
    defer allocator.free(stack);

    const state = try Fiber.init(stack, fiberFn, .{"142.250.187.174"});

    Fiber.switchTo(state);

    while (loop1.hasIo()) {
        try loop1.tick(1);
    }
}

//pub fn main() !void {
//    const socket = try jolt.tcp.init();
//
//    const addr = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, 8080);
//    try socket.bind(addr);
//    try socket.listen(128);
//
//    while (true) {
//        const client = try socket.accept();
//    }
//
//    std.debug.print("{s}\n", .{buf[0..len]});
//}

pub fn main2() !void {
    var loop = try DefaultLoop.init();
    defer loop.deinit();

    try loop.directDescriptors(.sparse, 1);

    const file = try std.fs.cwd().openFile("test.txt", .{ .mode = .read_only });
    // register to ring
    try loop.updateDescriptors(0, &[_]linux.fd_t{file.handle});
    // file handle is at offset 0
    const handle: linux.fd_t = 0;
    // we don't need this anymore
    file.close();

    var read_c = Completion{};
    var buffer: [1024]u8 = undefined;
    loop.read(&read_c, Completion, &read_c, handle, &buffer, onRead);

    try loop.run();
}

fn onRead(
    userdata: *Completion,
    loop: *DefaultLoop,
    completion: *Completion,
    buffer: []u8,
    result: DefaultLoop.ReadError!u31,
) void {
    _ = userdata;
    _ = loop;
    _ = completion;

    const len = result catch unreachable;

    std.debug.print("{s}\n", .{buffer[0..len]});
}

pub fn main1() !void {
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
    loop.accept(BufferPool, &recv_pool, &accept_c, 0, onAccept);
    //loop.close(&accept_c, 0);

    // fire after 2s
    //var timer_c = Completion{};
    //loop.timeout(&timer_c, BufferPool, &recv_pool, 2 * std.time.ns_per_s, onTimeout);

    try loop.run();
}

threadlocal var cancel_c = Completion{};

fn onTimeout(
    recv_pool: *BufferPool,
    loop: *DefaultLoop,
    c: *Completion,
    result: DefaultLoop.TimeoutError!void,
) void {
    _ = recv_pool;

    // cancel right away!
    //var cancel_c = Completion{};
    loop.cancel(.completion, c, &cancel_c);

    result catch |e| switch (e) {
        error.Cancelled => {
            std.debug.print("operation cancelled\n", .{});
            return;
        },
        error.Unexpected => @panic(@errorName(e)),
    };

    std.debug.print("successful timeout\n", .{});
}

threadlocal var send_c = Completion{};
threadlocal var hello = "hello";

fn onAccept(recv_pool: *BufferPool, loop: *DefaultLoop, c: *Completion, result: DefaultLoop.AcceptError!io_uring.Socket) void {
    _ = c;
    const fd = result catch unreachable;

    //std.debug.print("got connection, fd: {}\n", .{fd});

    //const recv_c = allocator.create(Completion) catch unreachable;
    //loop.recv(recv_c, BufferPool, recv_pool, fd, recv_pool, onRecv);

    loop.send(&send_c, BufferPool, recv_pool, fd, hello, onSend);
}

fn onSend(
    userdata: *BufferPool,
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
    const len = result catch unreachable;

    std.debug.print("{}\n", .{len});
}

fn onRecv(
    userdata: *BufferPool,
    loop: *DefaultLoop,
    completion: *Completion,
    socket: io_uring.Socket,
    buffer_pool: *BufferPool,
    buffer_id: u16,
    result: DefaultLoop.RecvError!u31,
) void {
    _ = userdata;
    _ = loop;
    _ = completion;
    _ = socket;

    const len = result catch |e| switch (e) {
        error.EndOfStream => return,
        else => unreachable,
    };

    std.debug.print("{s}\n", .{buffer_pool.get(buffer_id)[0..len]});

    buffer_pool.put(buffer_id);
}
