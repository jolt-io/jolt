//! kqueue backend.
//! Only tested on MacOS; not sure if it works on other BSDs.

const std = @import("std");
const testing = std.testing;
const os = std.os;
const posix = std.posix;
const system = posix.system;

const queue = @import("../queue.zig");
const sys = @import("bsd/sys.zig");

const Loop = @This();
/// kqueue instance.
kq: posix.fd_t,
/// Count of pending operations.
io_pending: usize = 0,
/// Handles waiting for registration.
/// Helps batching registration and
submissions: queue.Intrusive(Handle) = .{},
/// Operations completed and ready for their callbacks to be run.
/// We could execute callbacks in-place but it may cause unbounded stack usage.
completed: queue.Intrusive(Completion) = .{},

/// Initializes a new event loop backed by kqueue.
pub fn init() !Loop {
    return .{ .kq = try posix.kqueue() };
}

pub fn deinit(loop: *Loop) void {
    // TODO: Deinitializing should do more than this.
    posix.close(loop.kq);
}

pub fn socket(
    loop: *Loop,
    handle: *Handle,
    domain: u32,
    socket_type: u32,
    protocol: u32,
) !void {
    // Prepare handle.
    const flags = socket_type | posix.SOCK.NONBLOCK | posix.SOCK.CLOEXEC;
    const fd = try posix.socket(domain, flags, protocol);
    handle.* = .{ .fd = fd };
    // Handle will be registered to kq by the next iteration of `run`.
    loop.submissions.push(handle);
}

/// Queues a connect operation.
pub fn connect(
    loop: *Loop,
    completion: *Completion,
    comptime T: type,
    userdata: *T,
    handle: *Handle,
    addr: std.net.Address,
    comptime on_done: *const fn (
        userdata: *T,
        loop: *Loop,
        completion: *Completion,
        handle: *Handle,
        addr: std.net.Address,
        result: posix.ConnectError!void,
    ) void,
) void {
    const socklen = addr.getOsSockLen();
    completion.* = .{
        .next = null,
        .userdata = userdata,
        .callback = @ptrCast(&(struct {
            fn wrap(c: *Completion, _loop: *Loop, _handle: *Handle) void {
                const data = c.data.connect;
                const _addr = std.net.Address{ .any = data.addr };
                // Retrieve Zig error type out of `data.result`.
                const result: posix.ConnectError!void = switch (c.err) {
                    .SUCCESS => {},
                    .ACCES => error.AccessDenied,
                    .PERM => error.PermissionDenied,
                    .ADDRINUSE => error.AddressInUse,
                    .ADDRNOTAVAIL => error.AddressNotAvailable,
                    .AFNOSUPPORT => error.AddressFamilyNotSupported,
                    .AGAIN => error.SystemResources,
                    .ALREADY => error.ConnectionPending,
                    .BADF => unreachable, // sockfd is not a valid open file descriptor.
                    .CONNREFUSED => error.ConnectionRefused,
                    .FAULT => unreachable, // The socket structure address is outside the user's address space.
                    .ISCONN => unreachable, // The socket is already connected.
                    .HOSTUNREACH => error.NetworkUnreachable,
                    .NETUNREACH => error.NetworkUnreachable,
                    .NOTSOCK => unreachable, // The file descriptor sockfd does not refer to a socket.
                    .PROTOTYPE => unreachable, // The socket type does not support the requested communications protocol.
                    .TIMEDOUT => error.ConnectionTimedOut,
                    .CONNRESET => error.ConnectionResetByPeer,
                    else => |err| posix.unexpectedErrno(err),
                };

                @call(
                    .always_inline,
                    on_done,
                    .{ c.userdatum(T), _loop, c, _handle, _addr, result },
                );
            }
        }.wrap)),
        .data = .{
            .connect = .{
                .addr = addr.any,
                .handle = handle,
                .socklen = socklen,
            },
        },
        .type = .connect,
        .err = @enumFromInt(0),
    };

    // Start an async connect.
    while (true) {
        switch (posix.errno(system.connect(handle.fd, &addr.any, socklen))) {
            .SUCCESS => break,
            .INTR => continue,
            // Operation will be completed in the future.
            .AGAIN, .INPROGRESS => {
                // Why the unshift? you may ask. This allows us to push
                // write requests after this one. Meaning connect has
                // higher prio.
                handle.write_queue.unshift(completion);
                loop.io_pending += 1;
                return;
            },
            // Encountered with an error; push to `completed` w/ error.
            else => |err| {
                completion.err = err;
                loop.completed.push(completion);
                return;
            },
        }
    }

    // Immediate connect is very unlikely AFAIK; though we should handle such case.
    @panic("TODO: immediate connect");
}

/// Queue a send operation.
pub fn send(
    loop: *Loop,
    completion: *Completion,
    comptime T: type,
    userdata: *T,
    handle: *Handle,
    buffer: []const u8,
    flags: u32,
    comptime on_done: *const fn (
        userdata: *T,
        loop: *Loop,
        completion: *Completion,
        handle: *Handle,
        buffer: []const u8,
        result: posix.SendError!usize,
    ) void,
) void {
    completion.* = .{
        .next = null,
        .userdata = userdata,
        .callback = @ptrCast(&(struct {
            fn wrap(_completion: *Completion, _loop: *Loop, _handle: *Handle) void {
                const data = _completion.data.rw_const;
                const slice = data.base[0..data.len];

                @call(.always_inline, on_done, .{
                    _completion.userdatum(T),
                    _loop,
                    _completion,
                    _handle,
                    slice,
                    // TODO: result.
                });
            }
        }.wrap)),
        .data = .{
            .rw_const = .{
                .base = buffer.ptr,
                .len = buffer.len,
                .written = 0,
                .flags = flags,
            },
        },
        .type = .send,
        .err = @enumFromInt(0),
    };

    handle.write_queue.push(completion);
    loop.io_pending += 1;
}

/// Queue a recv operation.
pub fn recv(
    loop: *Loop,
    completion: *Completion,
    comptime T: type,
    userdata: *T,
    handle: *Handle,
    buffer: []u8,
    flags: u32,
    comptime on_done: *const fn (
        userdata: *T,
        loop: *Loop,
        completion: *Completion,
        handle: *Handle,
        buffer: []const u8,
        result: posix.RecvFromError!usize,
    ) void,
) void {
    completion.* = .{
        .next = null,
        .userdata = userdata,
        .callback = @ptrCast(&(struct {
            fn wrap(_: *Completion, _: *Loop, _: *Handle) void {
                _ = on_done;
            }
        }.wrap)),
        .data = .{
            .rw = .{
                .base = buffer.ptr,
                .len = buffer.len,
                .written = 0,
                .flags = flags,
            },
        },
        .type = .recv,
    };

    handle.read_queue.push(completion);
    loop.io_pending += 1;
}

pub inline fn hasIo(loop: *const Loop) bool {
    return !loop.submissions.isEmpty() or loop.io_pending > 0 or !loop.completed.isEmpty();
}

/// Populates `events`, returns a length of events put.
fn registerHandles(loop: *Loop, events: []sys.Kevent) usize {
    var i: usize = 0;
    while (i < events.len) : (i += 2) {
        const handle = loop.submissions.pop() orelse break;
        std.debug.assert(handle.fd >= 0);
        // Watch for read events.
        events[i] = .{
            .ident = @intCast(handle.fd),
            .filter = system.EVFILT.READ,
            .flags = system.EV.ADD | system.EV.ENABLE,
            .fflags = 0,
            .data = 0,
            .udata = @intFromPtr(handle),
            .ext = undefined,
        };
        // Watch for write events.
        events[i + 1] = .{
            .ident = @intCast(handle.fd),
            .filter = system.EVFILT.WRITE,
            .flags = system.EV.ADD | system.EV.ENABLE,
            .fflags = 0,
            .data = 0,
            .udata = @intFromPtr(handle),
            .ext = undefined,
        };
    }

    return i;
}

/// Perform recv operations of a handle.
/// This include accept, recv, recvfrom and recvmsg.
fn performRecvs(loop: *Loop, handle: *Handle) void {
    while (handle.read_queue.peek()) |c| {
        switch (c.type) {
            .recv => {
                const data = c.data.rw;
                const slice = data.base[0..data.len];
                const len = posix.recv(handle.fd, slice, 0) catch |err| switch (err) {
                    error.WouldBlock => break,
                    else => @panic("TODO"),
                };

                handle.read_queue.removeAssumeHead();
                loop.io_pending -= 1;
                _ = len;
            },
            else => unreachable,
        }
    }
}

/// Perform send operations of a handle.
/// This include connect, send, sendto and sendmsg.
fn performSends(loop: *Loop, handle: *Handle) void {
    while (handle.write_queue.peek()) |c| {
        switch (c.type) {
            .connect => {
                // In order to complete the connect operation,
                // we have to check for errors once socket is
                // writable.
                c.err = sys.getsockoptError(handle.fd);
                handle.write_queue.removeAssumeHead();
                loop.io_pending -= 1;
                c.execute(loop, handle);
            },
            .send => {
                const data = &c.data.rw_const;
                write_all: while (data.written < data.len) {
                    const slice = (data.base + data.written)[0 .. data.len - data.written];
                    // Similar to what stdlib do but w/ more control.
                    const written: u31 = blk: while (true) {
                        const rc = sys.sendto(handle.fd, slice, 0, null, 0);
                        switch (posix.errno(rc)) {
                            .SUCCESS => break :blk @intCast(rc), // Written bytes.
                            .INTR => continue, // Interrupted.
                            .AGAIN => break :write_all, // Continue some other time.
                            else => |err| c.data.rw_const.result = err,
                        }
                    };

                    data.written += written;
                }

                // TODO: Execute here...

                // Keep writing until everything has written.
                while (data.written < data.len) {
                    const slice = (data.base + data.written)[0 .. data.len - data.written];
                    const written = posix.send(handle.fd, slice, 0) catch |err| switch (err) {
                        // Either had a partial write or unusual block.
                        error.WouldBlock => break,
                        // Some other error.
                        else => @panic("TODO"),
                    };

                    data.written += written;
                }

                // Execute.
                handle.write_queue.removeAssumeHead();
                loop.io_pending -= 1;
                c.execute(loop, handle);
            },
            else => unreachable,
        }
    }
}

/// Run the event loop once. This may or may not complete events.
pub fn tick(loop: *Loop) !void {
    var events: [256]u8 = undefined;
    // Run events that're completed before tick.
    // Copy is necessary to inhibit recursion.
    var completed = loop.completed;
    loop.completed = .{};
    while (completed.pop()) |c| {
        // Currently, only connect calls can be in this queue.
        std.debug.assert(c.type == .connect);
        c.execute(loop, c.data.connect.handle);
    }

    // This populates `events` with read and write events.
    const events_len = loop.registerHandles(&events);
    // In order to emulate polling, we need zeroed timeout.
    var timeout = posix.timespec{ .sec = 0, .nsec = 0 };
    // Register handles & receive ready events together.
    const ready_len = try sys.kevent(loop.kq, events[0..events_len], &events, &timeout);

    // Iterate over received events.
    for (events[0..ready_len]) |event| {
        const handle: *Handle = @ptrFromInt(event.udata);
        // Fill completed queue with completions of this handle.
        switch (event.filter) {
            system.EVFILT.READ => loop.performRecvs(handle),
            system.EVFILT.WRITE => loop.performSends(handle),
            else => unreachable,
        }
    }
}

pub const Handle = struct {
    next: ?*Handle = null,
    //prev: ?*Handle = null,
    fd: posix.fd_t = -1,
    /// Linked list of write and connect operations.
    write_queue: queue.Intrusive(Completion) = .{},
    /// Linked list of read and accept operations.
    read_queue: queue.Intrusive(Completion) = .{},
};

/// Represents a single operation.
pub const Completion = extern struct {
    /// Intrusively linked to next operation.
    next: ?*Completion = null,
    /// Type-erased userdata.
    userdata: ?*anyopaque = null,
    /// Type-erased function pointer.
    /// NOTE: This is not null after a prep call.
    callback: ?*const anyopaque = null,
    /// Varying operation data; this is specific to operation kind.
    data: Data = .{ .none = {} },
    // TODO: flags.
    type: enum(u8) {
        none = 0,
        connect,
        recv,
        send,
        recvfrom,
        sendto,
    } = .none,
    __pad: u8 = 0,
    /// Prefer i32 instead?
    err: posix.E = @enumFromInt(0),

    /// Operation specific data of Completion.
    pub const Data = extern union {
        none: void,
        rw: extern struct {
            base: [*]u8,
            len: usize,
            written: usize,
            flags: u32,
        },
        rw_const: extern struct {
            base: [*]const u8,
            len: usize,
            written: usize,
            flags: u32,
        },
        rw_addr: extern struct {
            base: [*]u8,
            len: usize,
            written: usize,
            addr: posix.sockaddr,
            socklen: posix.socklen_t,
            flags: u32,
        },
        rw_addr_const: extern struct {
            base: [*]const u8,
            len: usize,
            written: usize,
            addr: posix.sockaddr,
            socklen: posix.socklen_t,
            flags: u32,
        },
        connect: extern struct {
            addr: posix.sockaddr,
            /// Owner of this completion; will be needed on error situations.
            handle: *Handle,
            socklen: posix.socklen_t,
        },
    };

    /// Internal.
    /// Type of type-erased `callback`.
    pub const Callback = *const fn (completion: *Completion, loop: *Loop, handle: *Handle) void;

    /// Internal function.
    /// `userdata` with a type.
    pub inline fn userdatum(completion: *const Completion, comptime T: type) *T {
        return @as(*T, @ptrCast(@alignCast(completion.userdata)));
    }

    /// Internal function.
    /// Run the completion's callback.
    pub inline fn execute(completion: *Completion, loop: *Loop, handle: *Handle) void {
        return @call(
            .auto,
            @as(Callback, @ptrCast(@alignCast(completion.callback))),
            .{ completion, loop, handle },
        );
    }
};

test "basic" {
    var loop = try Loop.init();
    defer loop.deinit();

    std.debug.print("{}\n", .{@sizeOf(Completion)});

    var handle = Handle{};
    try loop.socket(&handle, posix.AF.INET, posix.SOCK.STREAM, posix.IPPROTO.TCP);

    const addr_list = try std.net.getAddressList(testing.allocator, "www.google.com", 80);
    defer addr_list.deinit();

    var completion = Completion{};
    loop.connect(&completion, Handle, &handle, &handle, addr_list.addrs[0], struct {
        fn on_done(
            _: *Handle,
            _: *Loop,
            _: *Completion,
            _: *Handle,
            _: std.net.Address,
            result: posix.ConnectError!void,
        ) void {
            std.debug.print("{}\n", .{result catch unreachable});
            std.debug.print("connected\n", .{});
        }
    }.on_done);

    try loop.tick();
}
