//! kqueue backend.
//! Only tested on MacOS; not sure if it works on other BSDs.

const std = @import("std");
const testing = std.testing;
const os = std.os;
const posix = std.posix;
const system = posix.system;

const queue = @import("queue.zig");
const sys = @import("bsd/sys.zig");

const invalid_fd: posix.fd_t = -1;

const Loop = @This();
/// kqueue instance.
kq: posix.fd_t,
/// Changes pending to be executed.
/// This tracks registers/deregisters, and I/O events we're interested.
changes: [256]sys.Kevent = undefined,
/// Points to next writable spot in `changes`.
changes_idx: u32 = 0,
/// Queue of registered handles.
registered: queue.DoublyLinked(Handle) = .{},
/// Operations completed inline or out of kqueue bounds and ready for their callbacks to be run.
/// We could execute callbacks in-place but it may cause unbounded stack usage.
completed: queue.Intrusive(Completion) = .{},

/// Initializes a new event loop backed by kqueue.
pub fn init() !Loop {
    const kq = try posix.kqueue();
    return .{ .kq = kq };
}

pub fn deinit(loop: *Loop) void {
    // TODO: Deinitializing should do more than this.
    posix.close(loop.kq);
}

/// Advances the `changes_idx` by 1. Wraps around if index >= changes slice length.
fn advanceChanges(loop: *Loop) !void {
    loop.changes_idx = (loop.changes_idx + 1) % @as(u32, loop.changes.len);
    if (loop.changes_idx == 0) {
        // Changes index wrapped around, we have to flush.
        // Don't receive events here though; we want to do it in next tick.
        const len = try sys.kevent(loop.kq, &loop.changes, @constCast(&.{}), &posix.timespec{ .nsec = 0, .sec = 0 });
        std.debug.assert(len == 0);
    }
}

pub fn enable(loop: *Loop, handle: *Handle) !void {
    // Handle is being used by this loop.
    loop.registered.push(handle);
    // Prepare monitoring entries.
    const filters = .{ system.EVFILT.READ, system.EVFILT.WRITE };
    inline for (filters) |filter| {
        loop.changes[loop.changes_idx] = .{
            .ident = @intCast(handle.fd),
            .filter = filter,
            .flags = Event.Add | Event.Enable,
            .fflags = 0,
            .data = 0,
            .udata = @intFromPtr(handle),
            .ext = undefined,
        };

        try loop.advanceChanges();
    }
}

pub fn disable(loop: *Loop, handle: *Handle) !void {
    const filters = .{ system.EVFILT.READ, system.EVFILT.WRITE };
    inline for (filters) |filter| {
        loop.changes[loop.changes_idx] = .{
            .ident = @intCast(handle.fd),
            .filter = filter,
            .flags = Event.Add | Event.Disable,
            .fflags = 0,
            .data = 0,
            .udata = @intFromPtr(handle),
            .ext = undefined,
        };

        try loop.advanceChanges();
    }

    // Unregister from the loop.
    loop.registered.remove(handle);

    // Cancel reads.
    while (handle.read_queue.pop()) |c| {
        c.err = .CANCELED;
        c.execute(loop, handle);
    }
    // Cancel writes.
    while (handle.write_queue.pop()) |c| {
        c.err = .CANCELED;
        c.execute(loop, handle);
    }
}

pub fn initSocket(
    loop: *Loop,
    handle: *Handle,
    domain: u32,
    socket_type: u32,
    protocol: u32,
) !void {
    // Create a socket.
    const flags = socket_type | posix.SOCK.NONBLOCK | posix.SOCK.CLOEXEC;
    const fd = try posix.socket(domain, flags, protocol);
    handle.* = .{ .fd = fd };

    return loop.enable(handle);
}

/// Calls `on_done` for each incoming handle.
pub fn acceptStart(
    _: *Loop,
    completion: *Completion,
    comptime T: type,
    userdata: *T,
    handle: *Handle,
    comptime on_done: *const fn (
        userdata: *T,
        loop: *Loop,
        completion: *Completion,
        handle: *Handle,
        result: posix.AcceptError!posix.socket_t,
    ) void,
) void {
    completion.* = .{
        .next = null,
        .userdata = userdata,
        .callback = @ptrCast(&(struct {
            fn wrap(c: *Completion, l: *Loop, h: *Handle) void {
                const result = c.result(.accept);

                @call(
                    .always_inline,
                    on_done,
                    .{ c.userdatum(T), l, c, h, result },
                );
            }
        }.wrap)),
        .data = .{
            .accept = .{
                .addr = std.mem.zeroes(posix.sockaddr),
                .socklen = 0,
                .sockfd = invalid_fd,
            },
        },
        .type = .accept,
        .err = @enumFromInt(0),
    };

    handle.read_queue.push(completion);
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
                const result = c.result(.connect);

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
    _: *Loop,
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
            fn wrap(c: *Completion, l: *Loop, h: *Handle) void {
                const data = c.data.rw_const;
                const slice = data.base[0..data.len];
                const result = c.result(.send);

                @call(
                    .always_inline,
                    on_done,
                    .{ c.userdatum(T), l, c, h, slice, result },
                );
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
}

/// Queue a recv operation.
pub fn recv(
    _: *Loop,
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
        buffer: []u8,
        result: posix.RecvFromError!usize,
    ) void,
) void {
    completion.* = .{
        .next = null,
        .userdata = userdata,
        .callback = @ptrCast(&(struct {
            fn wrap(c: *Completion, l: *Loop, h: *Handle) void {
                const data = c.data.rw;
                const slice = data.base[0..data.len];
                const result = c.result(.recv);

                @call(
                    .always_inline,
                    on_done,
                    .{ c.userdatum(T), l, c, h, slice, result },
                );
            }
        }.wrap)),
        .data = .{
            .rw = .{
                .base = buffer.ptr,
                .len = buffer.len,
                .bytes_read = 0,
                .flags = flags,
            },
        },
        .type = .recv,
    };

    handle.read_queue.push(completion);
}

pub inline fn hasIo(loop: *const Loop) bool {
    return loop.changes_idx > 0 or !loop.registered.isEmpty() or !loop.completed.isEmpty();
}

/// Perform recv operations of a handle.
/// This include accept, recv, recvfrom and recvmsg.
fn performRecvs(loop: *Loop, handle: *Handle) void {
    run_reads: while (handle.read_queue.peek()) |c| {
        switch (c.type) {
            // TODO: Currently its not possible to stop this lol, requires cancelation support.
            .accept => {
                // Accept until blocked.
                while (true) {
                    const data = &c.data.accept;
                    // TODO: Handle error.
                    // Accept a socket.
                    const rc = system.accept(handle.fd, &data.addr, &data.socklen);
                    switch (posix.errno(rc)) {
                        .SUCCESS => {
                            // Configure socket fd.
                            const sockfd: posix.socket_t = @intCast(rc);
                            const flags = posix.fcntl(sockfd, posix.F.GETFL, 0) catch 0 | posix.SOCK.NONBLOCK | posix.SOCK.CLOEXEC;
                            _ = posix.fcntl(sockfd, posix.F.SETFL, flags) catch unreachable;
                            // Let completion know about socket.
                            data.sockfd = sockfd;
                        },
                        .INTR => continue, // Try immediately.
                        .AGAIN => return, // Try some other time.
                        else => |err| {
                            handle.read_queue.removeAssumeHead();
                            c.err = err;
                            c.execute(loop, handle);
                            return;
                        },
                    }

                    c.execute(loop, handle);
                }
            },
            .recv => {
                const data = c.data.rw;
                const bytes_read: usize = blk: while (true) {
                    const rc = system.recvfrom(handle.fd, data.base, data.len, data.flags, null, null);
                    switch (posix.errno(rc)) {
                        .SUCCESS => break :blk @intCast(rc),
                        .INTR => continue,
                        .AGAIN => break :run_reads, // Continue some other time.
                        else => |err| {
                            c.err = err;
                            break :blk 0;
                        },
                    }
                };
                // Completion can be executed now, remove from the queue.
                handle.read_queue.removeAssumeHead();
                // Execute.
                c.data.rw.bytes_read = bytes_read;
                c.execute(loop, handle);
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
            },
            .send => {
                const data = &c.data.rw_const;
                write_all: while (data.written < data.len) {
                    const slice = (data.base + data.written)[0 .. data.len - data.written];
                    // Similar to what stdlib do but w/ more control.
                    const written: usize = blk: while (true) {
                        const rc = sys.sendto(handle.fd, slice, 0, null, 0);
                        switch (posix.errno(rc)) {
                            .SUCCESS => break :blk @intCast(rc), // Written bytes.
                            .INTR => continue, // Interrupted.
                            .AGAIN => return, // Continue some other time.
                            else => |err| {
                                c.err = err;
                                break :write_all;
                            },
                        }
                    };

                    data.written += written;
                }
            },
            else => unreachable,
        }

        // Remove from the queue.
        handle.write_queue.removeAssumeHead();
        // Perform.
        c.execute(loop, handle);
    }
}

/// Single tick of event loop.
/// If `blocking` is true, kevent syscall will wait indefinitely.
pub fn tick(loop: *Loop, comptime blocking: bool) !void {
    // Run events that're completed before tick.
    // Copy is necessary to inhibit recursion.
    var completed = loop.completed;
    loop.completed = .{};
    while (completed.pop()) |c| {
        // Currently, only connect calls can be in this queue.
        std.debug.assert(c.type == .connect);
        c.execute(loop, c.data.connect.handle);
    }

    const changes = loop.changes[0..loop.changes_idx];
    loop.changes_idx = 0;
    // Can't use the same buffer since it may get overwritten by completion callbacks.
    var events: [256]sys.Kevent = undefined;
    // Register handles & receive ready events together.
    const ready_len = try sys.kevent(
        loop.kq,
        changes,
        &events,
        // NULL cause kevent to wait indefinitely,
        // zeroed timeout cause polling.
        if (blocking) null else &posix.timespec{ .sec = 0, .nsec = 0 },
    );

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

pub const RunMode = enum {
    /// Runs the event loop once; this may or may not complete events.
    /// Might block until a single operation has finished.
    once,
    /// Runs the event loop once; this may or may not complete events.
    /// Only interested with events that're ready now; never blocks.
    non_blocking,
    /// Runs the event loop until all operations are completed, blocks
    /// the process if needed.
    until_done,
};

/// Runs the event loop by desired mode.
pub fn run(loop: *Loop, comptime mode: RunMode) !void {
    switch (mode) {
        .once => if (loop.hasIo()) try loop.tick(true),
        .non_blocking => if (loop.hasIo()) try loop.tick(false),
        .until_done => while (loop.hasIo()) try loop.tick(true),
    }
}

const Event = struct {
    const Add = system.EV.ADD;
    const Delete = system.EV.DELETE;
    const Enable = system.EV.ENABLE;
    const Disable = system.EV.DISABLE;
    const OneShot = system.EV.ONESHOT;
    const Clear = system.EV.CLEAR;
};

pub const Handle = struct {
    next: ?*Handle = null,
    prev: ?*Handle = null,
    /// File descriptor belong to handle.
    fd: posix.fd_t = invalid_fd,
    /// Used when (de)registering a handle to loop.
    loop_flags: u16 = 0,
    state: enum(u8) {
        /// Free from all loops; can be registered again.
        unregistered = 0,
        /// In update queue; no need to queue again. `loop_flags` can be modified though.
        in_queue,
        /// Registered to a loop.
        registered,
        /// Handle is about to close.
        closing,
        /// Handle is closed.
        closed,
    } = .unregistered,
    /// Linked list of write and connect operations.
    write_queue: queue.Intrusive(Completion) = .{},
    /// Linked list of read and accept operations.
    read_queue: queue.Intrusive(Completion) = .{},

    pub const invalid = Handle{ .fd = invalid_fd };

    pub inline fn setReuseAddr(handle: *const Handle, toggle: bool) !void {
        return posix.setsockopt(handle.fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, @intFromBool(toggle))));
    }

    pub inline fn bind(handle: *const Handle, addr: std.net.Address) !void {
        return posix.bind(handle.fd, &addr.any, addr.getOsSockLen());
    }

    pub inline fn listen(handle: *const Handle, kernel_backlog: u31) !void {
        return posix.listen(handle.fd, kernel_backlog);
    }

    pub fn format(handle: Handle, writer: *std.Io.Writer) std.Io.Writer.Error!void {
        return writer.print("Handle{{ .fd = {} }}", .{handle.fd});
    }
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
    /// Active payload.
    type: Type = .none,
    /// Error payload of this completion.
    err: posix.E = @enumFromInt(0),
    /// Reserved.
    __pad: u32 = 0,
    /// Varying operation data; this is specific to operation kind.
    data: Data = .{ .none = {} },

    pub const Type = enum(u8) {
        none = 0,
        accept,
        connect,
        recv,
        send,
        recvfrom,
        sendto,
    };

    /// Operation specific data of Completion.
    pub const Data = extern union {
        none: void,
        accept: extern struct {
            addr: posix.sockaddr,
            socklen: posix.socklen_t,
            sockfd: posix.socket_t,
        },
        rw: extern struct {
            base: [*]u8,
            len: usize,
            bytes_read: usize,
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

    pub fn format(completion: Completion, writer: *std.Io.Writer) std.Io.Writer.Error!void {
        return writer.print("Completion{{ .type = {s} }}", .{@tagName(completion.type)});
    }

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

    /// Internal.
    /// Returns the type for completion result.
    fn Result(comptime completion_type: Type) type {
        return switch (completion_type) {
            .none => unreachable,
            .accept => posix.AcceptError!posix.socket_t,
            .connect => posix.ConnectError!void,
            .recv, .recvfrom => posix.RecvFromError!usize,
            .send => posix.SendError!usize,
            .sendto => posix.SendToError!usize,
        };
    }

    /// Internal.
    /// Returns the result of completion by its type.
    /// Only valid after completion has fulfilled.
    pub fn result(
        completion: *const Completion,
        comptime completion_type: Type,
    ) Result(completion_type) {
        return switch (completion_type) {
            .none => unreachable,
            .accept => switch (completion.err) {
                .SUCCESS => completion.data.accept.sockfd,
                .INTR => unreachable, // Already handled.
                .AGAIN => unreachable, // Already handled.
                .BADF => unreachable, // always a race condition
                .CONNABORTED => error.ConnectionAborted,
                .FAULT => unreachable,
                .INVAL => error.SocketNotListening,
                .NOTSOCK => unreachable,
                .MFILE => error.ProcessFdQuotaExceeded,
                .NFILE => error.SystemFdQuotaExceeded,
                .NOBUFS => error.SystemResources,
                .NOMEM => error.SystemResources,
                .OPNOTSUPP => unreachable,
                .PROTO => error.ProtocolFailure,
                .PERM => error.BlockedByFirewall,
                else => |err| posix.unexpectedErrno(err),
            },
            .connect => switch (completion.err) {
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
            },
            .recv => switch (completion.err) {
                .SUCCESS => completion.data.rw.bytes_read,
                .BADF => unreachable, // Always a race condition.
                .FAULT => unreachable,
                .INVAL => unreachable,
                .NOTCONN => error.SocketNotConnected,
                .NOTSOCK => unreachable,
                .INTR => unreachable, // Already handled this.
                .AGAIN => unreachable, // Already handled this.
                .NOMEM => error.SystemResources,
                .CONNREFUSED => error.ConnectionRefused,
                .CONNRESET => error.ConnectionResetByPeer,
                .TIMEDOUT => error.ConnectionTimedOut,
                else => |err| posix.unexpectedErrno(err),
            },
            .recvfrom => unreachable,
            .send => switch (completion.err) {
                .SUCCESS => completion.data.rw_const.written,
                .ACCES => error.AccessDenied,
                .AGAIN => unreachable, // Already handled.
                .ALREADY => error.FastOpenAlreadyInProgress,
                .BADF => unreachable, // Always a race condition.
                .CONNREFUSED => error.ConnectionRefused,
                .CONNRESET => error.ConnectionResetByPeer,
                .DESTADDRREQ => unreachable, // The socket is not connection-mode, and no peer address is set.
                .FAULT => unreachable, // An invalid user space address was specified for an argument.
                .INTR => unreachable, // Already handled.
                .INVAL => unreachable,
                .ISCONN => unreachable, // connection-mode socket was connected already but a recipient was specified
                .MSGSIZE => error.MessageTooBig,
                .NOBUFS => error.SystemResources,
                .NOMEM => error.SystemResources,
                .NOTSOCK => unreachable, // The file descriptor sockfd does not refer to a socket.
                .OPNOTSUPP => unreachable, // Some bit in the flags argument is inappropriate for the socket type.
                .PIPE => error.BrokenPipe,
                .AFNOSUPPORT => unreachable,
                .LOOP => unreachable,
                .NAMETOOLONG => unreachable,
                .NOENT => unreachable,
                .NOTDIR => unreachable,
                .HOSTUNREACH => unreachable,
                .NETUNREACH => unreachable,
                .NOTCONN => unreachable,
                .NETDOWN => error.NetworkSubsystemFailed,
                else => |err| posix.unexpectedErrno(err),
            },
            .sendto => unreachable,
        };
    }
};

test "basic" {
    var loop = try Loop.init();
    defer loop.deinit();

    var handle = Handle{};
    try loop.socket(&handle, posix.AF.INET, posix.SOCK.STREAM, posix.IPPROTO.TCP);

    const addr_list = try std.net.getAddressList(testing.allocator, "www.google.com", 80);
    defer addr_list.deinit();

    var c1 = Completion{};
    loop.connect(&c1, Handle, &handle, &handle, addr_list.addrs[0], struct {
        fn on_done(
            _: *Handle,
            _: *Loop,
            _: *Completion,
            _: *Handle,
            _: std.net.Address,
            result: posix.ConnectError!void,
        ) void {
            _ = result catch unreachable;
            std.debug.print("connected\n", .{});
        }
    }.on_done);

    var c2 = Completion{};
    loop.send(&c2, Handle, &handle, &handle, "GET / HTTP/1.1\r\n\r\n", 0, struct {
        fn on_done(
            _: *Handle,
            _: *Loop,
            _: *Completion,
            _: *Handle,
            _: []const u8,
            result: posix.SendError!usize,
        ) void {
            std.debug.print("sent {} bytes\n", .{result catch unreachable});
        }
    }.on_done);

    var c3 = Completion{};
    var buffer: [1024]u8 = undefined;
    loop.recv(&c3, Handle, &handle, &handle, &buffer, 0, struct {
        fn on_done(
            _: *Handle,
            _: *Loop,
            _: *Completion,
            _: *Handle,
            slice: []u8,
            result: posix.RecvFromError!usize,
        ) void {
            std.debug.print("{s}\n", .{slice[0 .. result catch unreachable]});
        }
    }.on_done);

    try loop.run(.complete);
}

test "TCP server" {
    var loop = try Loop.init();
    defer loop.deinit();

    // Setup listener.
    var handle = Handle{};
    try loop.socket(&handle, posix.AF.INET, posix.SOCK.STREAM, posix.IPPROTO.TCP);
    defer posix.close(handle.fd);
    try posix.setsockopt(handle.fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));
    const addr = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, 8081);
    try posix.bind(handle.fd, &addr.any, addr.getOsSockLen());
    try posix.listen(handle.fd, 128);

    // Accept incoming.
    var c1 = Completion{};
    var conns: usize = 0;
    loop.acceptStart(&c1, usize, &conns, &handle, struct {
        fn on_done(
            _conns: *usize,
            _: *Loop,
            _: *Completion,
            _: *Handle,
            result: posix.AcceptError!posix.socket_t,
        ) void {
            if (_conns.* > 3) posix.exit(0);

            std.debug.print("got connection\t{}\n", .{result catch unreachable});
            _conns.* += 1;
        }
    }.on_done);

    try loop.run(.complete);
}
