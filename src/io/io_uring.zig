//! io_uring backend.
//!
//! Code here is heavily based on both tigerbeetle/io and libxev.
//! Thanks TigerBeetle Team and @mitchellh!
//!
//! https://github.com/tigerbeetle/tigerbeetle/blob/main/src/io/linux.zig
//! https://github.com/mitchellh/libxev/blob/main/src/backend/io_uring.zig

const std = @import("std");
const os = std.os;
const assert = std.debug.assert;
const posix = std.posix;
const linux = os.linux;
const IO_Uring = linux.IoUring;
const io_uring_cqe = linux.io_uring_cqe;
const io_uring_sqe = linux.io_uring_sqe;
const Intrusive = @import("../queue.zig").Intrusive;
const Options = @import("options.zig").Options;
const ex = @import("linux/io_uring_ex.zig");
const builtin = @import("builtin");
const testing = std.testing;

const block_size = @sizeOf(usize);

pub const IoUring = struct {
    const Loop = @This();
    /// io_uring instance.
    ring: IO_Uring,
    /// count of CQEs that we're waiting to receive, includes;
    /// * I/O operations that're submitted successfully (not in unqueued),
    /// * I/O operations that create multiple CQEs (multishot operations, zero-copy send etc.).
    io_pending: u32 = 0,
    /// I/O operations that're not queued yet.
    unqueued: Intrusive(Completion) = .{},
    /// Completions that're ready for their callbacks to run.
    completed: Intrusive(Completion) = .{},

    /// TODO: Check io_uring capabilities of kernel.
    /// Initializes a new event loop backed by io_uring.
    pub fn init() !Loop {
        // https://man7.org/linux/man-pages/man2/io_uring_setup.2.html
        const flags: u32 = linux.IORING_SETUP_SINGLE_ISSUER | linux.IORING_SETUP_DEFER_TASKRUN | linux.IORING_SETUP_COOP_TASKRUN | linux.IORING_SETUP_TASKRUN_FLAG;

        // Put a ring on it.
        var ring = try IO_Uring.init(256, flags);
        errdefer ring.deinit();

        // Try to increase fd limit.
        posix.setrlimit(.NOFILE, .{ .cur = 1048576, .max = 1048576 }) catch |err| switch (err) {
            error.PermissionDenied => {},
            error.LimitTooBig => unreachable,
            else => return err,
        };

        // Allocate space for direct descriptors.
        const rlimit = try posix.getrlimit(.NOFILE);
        ex.io_uring_register_files_sparse(&ring, @intCast(rlimit.max & std.math.maxInt(linux.fd_t))) catch |err| switch (err) {
            error.FileDescriptorInvalid => unreachable, // Kernel must support sparse sets.
            error.FilesAlreadyRegistered => unreachable, // First and only time this function called.
            error.UserFdQuotaExceeded => unreachable, // We obtain the limit via getrlimit.
            else => return err,
        };

        return .{ .ring = ring };
    }

    /// Deinitializes the event loop.
    /// TODO: Cancel any pending events.
    pub fn deinit(self: *Loop) void {
        self.ring.deinit();
    }

    /// NOTE: experimental
    pub inline fn hasIo(self: *const Loop) bool {
        return self.io_pending > 0 and self.unqueued.isEmpty();
    }

    /// Runs the event loop until all operations are completed.
    pub fn run(loop: *Loop) !void {
        var cqes: [512]io_uring_cqe = undefined;

        while (loop.hasIo()) {
            // Flush any queued SQEs and reuse the same syscall to wait for completions if required:
            try loop.flushSubmissions(&cqes);
            // We can now just peek for any CQEs without waiting and without another syscall:
            try loop.flushCompletions(&cqes, loop.io_pending);

            // Retry queueing completions that were unable to be queued before.
            {
                var copy = loop.unqueued;
                loop.unqueued = .{};
                while (copy.pop()) |c| loop.enqueue(c);
            }

            // Run completions.
            while (loop.completed.pop()) |completion| {
                // Since we've erased type of our callback, we give it a type here.
                @call(.auto, @as(Completion.Callback, @ptrCast(completion.callback)), .{ loop, completion });
            }
        }
    }

    fn flushCompletions(self: *Loop, slice: []io_uring_cqe, wait_nr: u32) !void {
        var remaining = wait_nr;

        while (true) {
            const completed = self.ring.copy_cqes(slice, remaining) catch |err| switch (err) {
                error.SignalInterrupt => continue,
                else => return err,
            };

            if (completed > remaining) remaining = 0 else remaining -= completed;

            // Decrement as much as completed events.
            // Currently, `io_pending` is only decremented here.
            self.io_pending -= completed;

            for (slice[0..completed]) |*cqe| {
                const completion: *Completion = @ptrFromInt(cqe.user_data);
                completion.result = cqe.res;
                // SQE flags (if given) are invalid from now on, we store CQE flags instead.
                completion.flags = cqe.flags;

                // We do not run the completion here (instead appending to a linked list) to avoid:
                // * recursion through `flush_submissions()` and `flush_completions()`,
                // * unbounded stack usage, and
                // * confusing stack traces.
                self.completed.push(completion);
            }

            if (completed < slice.len) break;
        }
    }

    fn flushSubmissions(self: *Loop, slice: []io_uring_cqe) !void {
        while (true) {
            _ = self.ring.submit_and_wait(1) catch |err| switch (err) {
                error.SignalInterrupt => continue,
                error.CompletionQueueOvercommitted, error.SystemResources => {
                    try self.flushCompletions(slice, 1);
                    continue;
                },
                else => return err,
            };

            break;
        }
    }

    /// Queues a completion to the loop.
    fn enqueue(loop: *Loop, c: *Completion) void {
        // Get an SQE.
        const sqe = loop.ring.get_sqe() catch |err| switch (err) {
            // If SQE ring is full, the completion will be added to unqueued.
            error.SubmissionQueueFull => return loop.unqueued.push(c),
        };

        // Prepare the completion.
        switch (c.operation) {
            .none => unreachable,
            .connect => {
                sqe.prep_connect(c.fd(), &c.metadata.connect.addr, c.metadata.connect.socklen);
                sqe.flags |= linux.IOSQE_FIXED_FILE;
            },
            .bind => {
                // FIXME: Use proper enum (.BIND).
                sqe.prep_rw(@enumFromInt(56), c.fd(), @intFromPtr(&c.metadata.connect.addr), 0, c.metadata.connect.socklen);
                sqe.flags |= linux.IOSQE_FIXED_FILE;
            },
            .listen => {
                // FIXME: Use proper enum (.LISTEN).
                sqe.prep_rw(@enumFromInt(57), c.fd(), 0, @as(usize, c.metadata.listen.kernel_backlog), 0);
                sqe.flags |= linux.IOSQE_FIXED_FILE;
            },
            .accept => {
                // Accept operations are always multishot.
                sqe.prep_multishot_accept(
                    c.metadata.accept.fd,
                    &c.metadata.accept.addr,
                    &c.metadata.accept.socklen,
                    posix.SOCK.CLOEXEC,
                );
                // prep_multishot_accept_direct.
                sqe.splice_fd_in = @bitCast(@as(u32, linux.IORING_FILE_INDEX_ALLOC));
            },
            .recv => {
                const data = c.metadata.rw;
                sqe.prep_rw(.RECV, data.fd, @intFromPtr(data.base), data.len, 0);
                // NOTE: Flags are not supported for recv operations.
                //sqe.rw_flags = data.flags;
                sqe.flags |= linux.IOSQE_FIXED_FILE;
            },
            .send => {
                const data = c.metadata.rw_const;
                // TODO: SEND_ZC
                sqe.prep_rw(.SEND, data.fd, @intFromPtr(data.base), data.len, 0);
                // Send flags.
                sqe.rw_flags = data.flags;
                sqe.flags |= linux.IOSQE_FIXED_FILE;
            },
            .socket => {
                const data = c.metadata.socket;
                sqe.prep_rw(.SOCKET, @intCast(data.domain), 0, data.protocol, data.type);
                sqe.rw_flags = data.flags;
                sqe.splice_fd_in = @bitCast(@as(u32, linux.IORING_FILE_INDEX_ALLOC));
            },
            .setsockopt => {
                const data = c.metadata.cmdsock;

                sqe.prep_rw(.URING_CMD, data.fd, 0, 0, 0);
                sqe.addr3 = @intFromPtr(data.opt_value);
                // https://github.com/axboe/liburing/blob/bc8776af071656b47114d777b56f0e598431cf5d/src/include/liburing/io_uring.h#L43-L50
                sqe.addr = @as(u64, data.opt_name) << 32 | data.level;
                sqe.splice_fd_in = @bitCast(data.opt_len);
                // TODO: Proper enum.
                sqe.off = 3;

                // NOTE: Direct descriptors.
                sqe.flags |= linux.IOSQE_FIXED_FILE;
            },
            else => @panic("TODO"),
        }

        // userdata of an SQE is always set to a completion object.
        sqe.user_data = @intFromPtr(c);

        // we got a pending io.
        loop.io_pending += 1;
    }

    pub const ConnectError = error{
        AccessDenied,
        AddressInUse,
        AddressNotAvailable,
        AddressFamilyNotSupported,
        OpenAlreadyInProgress,
        FileDescriptorInvalid,
        ConnectionRefused,
        ConnectionResetByPeer,
        AlreadyConnected,
        NetworkUnreachable,
        HostUnreachable,
        FileNotFound,
        FileDescriptorNotASocket,
        PermissionDenied,
        ProtocolNotSupported,
        ConnectionTimedOut,
        SystemResources,
    } || CancelationError || UnexpectedError;

    /// Queue a connect operation.
    pub fn connect(
        loop: *Loop,
        completion: *Completion,
        comptime T: type,
        userdata: *T,
        socket: Socket,
        addr: std.net.Address,
        comptime on_done: *const fn (
            userdata: *T,
            loop: *Loop,
            completion: *Completion,
            socket: Socket,
            addr: std.net.Address,
            result: ConnectError!void,
        ) void,
    ) void {
        completion.* = .{
            .next = null,
            .userdata = userdata,
            .result = 0,
            .flags = 0,
            .operation = .connect,
            // Completion is considered active if queued to any of loop's lists.
            .active = true,
            .callback = @ptrCast(&(struct {
                /// Logic to run when we receive a CQE for this operation.
                fn wrap(_loop: *Loop, _completion: *Completion) void {
                    const res = _completion.result;

                    const result: ConnectError!void = blk: {
                        if (res < 0) {
                            break :blk switch (@as(posix.E, @enumFromInt(-res))) {
                                .INTR => return _loop.enqueue(_completion),
                                .ACCES => error.AccessDenied,
                                .ADDRINUSE => error.AddressInUse,
                                .ADDRNOTAVAIL => error.AddressNotAvailable,
                                .AFNOSUPPORT => error.AddressFamilyNotSupported,
                                .AGAIN, .INPROGRESS => unreachable,
                                .ALREADY => error.OpenAlreadyInProgress,
                                .BADF => error.FileDescriptorInvalid,
                                .CANCELED => error.Canceled,
                                .CONNREFUSED => error.ConnectionRefused,
                                .CONNRESET => error.ConnectionResetByPeer,
                                .FAULT => unreachable,
                                .ISCONN => error.AlreadyConnected,
                                .NETUNREACH => error.NetworkUnreachable,
                                .HOSTUNREACH => error.HostUnreachable,
                                .NOENT => error.FileNotFound,
                                .NOTSOCK => error.FileDescriptorNotASocket,
                                .PERM => error.PermissionDenied,
                                .PROTOTYPE => error.ProtocolNotSupported,
                                .TIMEDOUT => error.ConnectionTimedOut,
                                else => error.Unexpected,
                            };
                        } else {
                            break :blk {};
                        }
                    };

                    // Completed.
                    _completion.active = false;

                    const data = _completion.metadata.connect;
                    @call(.always_inline, on_done, .{ _completion.userdatum(T), _loop, _completion, _completion.fd(), std.net.Address{ .any = data.addr }, result });
                }
            }.wrap)),
            // Connect op metadata.
            .metadata = .{
                .connect = .{
                    .addr = addr.any,
                    .fd = socket,
                    .socklen = addr.getOsSockLen(),
                },
            },
        };

        loop.enqueue(completion);
    }

    // Error that's not really expected; if this is returned by any operation, it should be investigated closely.
    // Since additional errors should be included in the error set.
    pub const UnexpectedError = error{Unexpected};
    // Error that's used on operations that can be canceled.
    pub const CancelationError = error{Canceled};

    // possible errors accept can create
    pub const AcceptError = error{
        ConnectionAborted,
        SocketNotListening,
        ProcessFdQuotaExceeded,
        SystemFdQuotaExceeded,
        SystemResources,
        OperationNotSupported,
        PermissionDenied,
        ProtocolFailure,
    } || CancelationError || UnexpectedError;

    /// Starts accepting connections for a given socket.
    /// This a multishot operation, meaning it'll keep on running until it's either canceled or encountered with an error.
    /// Completions given to multishot operations MUST NOT be reused until the multishot operation is either canceled or encountered with an error.
    pub fn acceptStart(
        loop: *Loop,
        completion: *Completion,
        comptime T: type,
        userdata: *T,
        socket: Socket,
        comptime on_done: *const fn (
            userdata: *T,
            loop: *Loop,
            completion: *Completion,
            socket: Socket,
            result: AcceptError!Socket,
        ) void,
    ) void {
        completion.* = .{
            .next = null,
            .userdata = userdata,
            .result = 0,
            .flags = 0,
            .operation = .accept,
            .active = true,
            .callback = @ptrCast(&(struct {
                fn wrap(_loop: *Loop, _completion: *Completion) void {
                    const res = _completion.result;

                    const result = if (res < 0) switch (@as(posix.E, @enumFromInt(-res))) {
                        .INTR => return loop.enqueue(_completion), // We're interrupted, try again.
                        .AGAIN => unreachable,
                        .BADF => error.Unexpected,
                        .CONNABORTED => error.ConnectionAborted,
                        .FAULT => error.Unexpected,
                        .INVAL => error.SocketNotListening,
                        .MFILE => error.ProcessFdQuotaExceeded,
                        .NFILE => error.SystemFdQuotaExceeded,
                        .NOBUFS => error.SystemResources,
                        .NOMEM => error.SystemResources,
                        .NOTSOCK => error.Unexpected,
                        .OPNOTSUPP => error.OperationNotSupported,
                        .PERM => error.PermissionDenied,
                        .PROTO => error.ProtocolFailure,
                        .CANCELED => error.Canceled,
                        else => error.Unexpected,
                    } else res; // Valid socket.

                    // We're accepting via `io_uring_prep_multishot_accept` so we have to increment pending for each successful accept request.
                    // Since we're expecting to receive more CQEs.
                    //
                    // This must be done no matter what `cqe.res` we got.
                    if (_completion.flags & linux.IORING_CQE_F_MORE != 0) {
                        _loop.io_pending += 1;
                    } else {
                        // If we got here, no more CQEs will be received, completion can be reused.
                        _completion.active = false;
                    }

                    // Invoke.
                    @call(.always_inline, on_done, .{ _completion.userdatum(T), loop, _completion, _completion.fd(), result });
                }
            }.wrap)),
            .metadata = .{
                .accept = .{ .fd = socket },
            },
        };

        loop.enqueue(completion);
    }

    pub const RecvError = error{
        SystemResources,
        OperationNotSupported,
        PermissionDenied,
        SocketNotConnected,
        ConnectionResetByPeer,
        ConnectionTimedOut,
        ConnectionRefused,
    } || CancelationError || UnexpectedError;

    /// Queues a recv operation.
    pub fn recv(
        loop: *Loop,
        completion: *Completion,
        comptime T: type,
        userdata: *T,
        socket: Socket,
        buffer: []u8,
        comptime on_done: *const fn (
            userdata: *T,
            loop: *Loop,
            completion: *Completion,
            socket: Socket,
            buffer: []u8,
            result: RecvError!usize,
        ) void,
    ) void {
        completion.* = .{
            .next = null,
            .userdata = userdata,
            .result = 0,
            .flags = 0,
            .operation = .recv,
            .active = true,
            .callback = @ptrCast(&(struct {
                fn wrap(_loop: *Loop, _completion: *Completion) void {
                    const res = _completion.result;

                    const result = if (res < 0) switch (_completion.err()) {
                        .INTR => return _loop.enqueue(_completion), // We're interrupted, try again.
                        .AGAIN => unreachable,
                        .BADF => error.Unexpected,
                        .CONNREFUSED => error.ConnectionRefused,
                        .FAULT => unreachable,
                        .INVAL => unreachable,
                        .NOBUFS => error.SystemResources,
                        .NOMEM => error.SystemResources,
                        .NOTCONN => error.SocketNotConnected,
                        .NOTSOCK => error.Unexpected,
                        .CONNRESET => error.ConnectionResetByPeer,
                        .TIMEDOUT => error.ConnectionTimedOut,
                        .OPNOTSUPP => error.OperationNotSupported,
                        .CANCELED => error.Canceled,
                        else => error.Unexpected,
                    } else @as(usize, @intCast(res)); // Valid.

                    _completion.active = false;

                    @call(.always_inline, on_done, .{ _completion.userdatum(T), _loop, _completion, _completion.fd(), _completion.slice(false), result });
                }
            }.wrap)),
            .metadata = .{
                .rw = .{
                    .base = buffer.ptr,
                    .len = buffer.len,
                    .fd = socket,
                    // Flags are not supported for recv operations.
                    .flags = 0,
                },
            },
        };

        loop.enqueue(completion);
    }

    pub const SendError = error{
        AccessDenied,
        FastOpenAlreadyInProgress,
        AddressFamilyNotSupported,
        FileDescriptorInvalid,
        ConnectionResetByPeer,
        MessageTooBig,
        SystemResources,
        SocketNotConnected,
        FileDescriptorNotASocket,
        OperationNotSupported,
        BrokenPipe,
        ConnectionTimedOut,
    } || CancelationError || UnexpectedError;

    /// Queues a send operation.
    pub fn send(
        loop: *Loop,
        completion: *Completion,
        comptime T: type,
        userdata: *T,
        socket: Socket,
        buffer: []const u8,
        /// NOTE: linux.MSG.WAITALL flag can be used on io_uring backend.
        /// https://github.com/axboe/liburing/issues/1337
        flags: u32,
        comptime on_done: *const fn (
            userdata: *T,
            loop: *Loop,
            completion: *Completion,
            socket: Socket,
            buffer: []const u8,
            result: SendError!usize,
        ) void,
    ) void {
        completion.* = .{
            .next = null,
            .userdata = userdata,
            .result = 0,
            .flags = 0,
            .operation = .send,
            .active = true,
            .callback = @ptrCast(&(struct {
                fn wrap(_loop: *Loop, _completion: *Completion) void {
                    const res = _completion.result;

                    const result = if (res < 0) switch (_completion.err()) {
                        .INTR => return _loop.enqueue(completion),
                        .ACCES => error.AccessDenied,
                        .AGAIN => unreachable,
                        .ALREADY => error.FastOpenAlreadyInProgress,
                        .AFNOSUPPORT => error.AddressFamilyNotSupported,
                        .BADF => error.FileDescriptorInvalid,
                        // Can happen when send()'ing to a UDP socket.
                        .CONNREFUSED => error.ConnectionRefused,
                        .CONNRESET => error.ConnectionResetByPeer,
                        .DESTADDRREQ => unreachable,
                        .FAULT => unreachable,
                        .INVAL => unreachable,
                        .ISCONN => unreachable,
                        .MSGSIZE => error.MessageTooBig,
                        .NOBUFS => error.SystemResources,
                        .NOMEM => error.SystemResources,
                        .NOTCONN => error.SocketNotConnected,
                        .NOTSOCK => error.FileDescriptorNotASocket,
                        .OPNOTSUPP => error.OperationNotSupported,
                        .PIPE => error.BrokenPipe,
                        .TIMEDOUT => error.ConnectionTimedOut,
                        .CANCELED => error.Canceled,
                        else => error.Unexpected,
                    } else @as(usize, @intCast(res)); // Valid.

                    _completion.active = false;
                    @call(.always_inline, on_done, .{ _completion.userdatum(T), _loop, _completion, _completion.fd(), _completion.slice(true), result });
                }
            }.wrap)),
            .metadata = .{
                .rw_const = .{
                    .base = buffer.ptr,
                    .len = buffer.len,
                    .fd = socket,
                    .flags = flags,
                },
            },
        };

        loop.enqueue(completion);
    }

    pub const SocketError = error{
        PermissionDenied,
        AddressFamilyNotSupported,
        ProtocolFamilyNotAvailable,
        ProcessFdQuotaExceeded,
        SystemFdQuotaExceeded,
        SystemResources,
        ProtocolNotSupported,
        SocketTypeNotSupported,
    } || CancelationError || UnexpectedError;

    /// Queues a socket opening operation.
    /// NOTE: Prefer this instead of `socket` syscall; io_uring backend takes advantage of direct descriptors.
    pub fn openSocket(
        loop: *Loop,
        completion: *Completion,
        comptime T: type,
        userdata: *T,
        domain: u32,
        _type: u32,
        protocol: u32,
        flags: u32,
        comptime on_done: *const fn (
            userdata: *T,
            loop: *Loop,
            completion: *Completion,
            result: SocketError!Socket,
        ) void,
    ) void {
        completion.* = .{
            .next = null,
            .userdata = userdata,
            .result = 0,
            .flags = 0,
            .operation = .socket,
            .active = true,
            .callback = @ptrCast(&(struct {
                fn wrap(_loop: *Loop, _completion: *Completion) void {
                    const res = _completion.result;

                    const result: SocketError!Socket = blk: {
                        if (res < 0) {
                            break :blk switch (_completion.err()) {
                                .INTR, .AGAIN => return _loop.enqueue(_completion),
                                .ACCES => error.PermissionDenied,
                                .AFNOSUPPORT => error.AddressFamilyNotSupported,
                                .INVAL => error.ProtocolFamilyNotAvailable,
                                .MFILE => error.ProcessFdQuotaExceeded,
                                .NFILE => error.SystemFdQuotaExceeded,
                                .NOBUFS => error.SystemResources,
                                .NOMEM => error.SystemResources,
                                .PROTONOSUPPORT => error.ProtocolNotSupported,
                                .PROTOTYPE => error.SocketTypeNotSupported,
                                .CANCELED => error.Canceled,
                                else => error.Unexpected,
                            };
                        }

                        // Valid socket.
                        break :blk res;
                    };

                    _completion.active = false;
                    @call(.always_inline, on_done, .{ _completion.userdatum(T), _loop, _completion, result });
                }
            }.wrap)),
            .metadata = .{
                .socket = .{
                    .domain = domain,
                    .type = _type,
                    .protocol = protocol,
                    .flags = flags,
                },
            },
        };

        loop.enqueue(completion);
    }

    pub const SetSockOptError = error{
        TimeoutTooBig,
        AlreadyConnected,
        InvalidProtocolOption,
        SystemResources,
        PermissionDenied,
        NoDevice,
        OperationNotSupported,
    } || CancelationError || UnexpectedError;

    /// Queue a setsockopt operation.
    pub fn setsockopt(
        loop: *Loop,
        completion: *Completion,
        comptime T: type,
        userdata: *T,
        _socket: Socket,
        level: u32,
        optname: u32,
        opt: []const u8,
        comptime on_done: *const fn (
            userdata: *T,
            loop: *Loop,
            completion: *Completion,
            _socket: Socket,
            result: SetSockOptError!void,
        ) void,
    ) void {
        completion.* = .{
            .next = null,
            .userdata = userdata,
            .result = 0,
            .flags = 0,
            .operation = .setsockopt,
            .active = true,
            .callback = @ptrCast(&(struct {
                fn wrap(_loop: *Loop, _completion: *Completion) void {
                    const res = _completion.result;

                    const result: SetSockOptError!void = blk: {
                        if (res < 0) {
                            break :blk switch (_completion.err()) {
                                .INTR, .AGAIN => return _loop.enqueue(_completion),
                                .BADF => unreachable, // Always a race condition.
                                .NOTSOCK => unreachable, // Always a race condition.
                                .INVAL => unreachable,
                                .FAULT => unreachable,
                                .DOM => error.TimeoutTooBig,
                                .ISCONN => error.AlreadyConnected,
                                .NOPROTOOPT => error.InvalidProtocolOption,
                                .NOMEM => error.SystemResources,
                                .NOBUFS => error.SystemResources,
                                .PERM => error.PermissionDenied,
                                .NODEV => error.NoDevice,
                                .OPNOTSUPP => error.OperationNotSupported,
                                .CANCELED => error.Canceled,
                                else => error.Unexpected,
                            };
                        }

                        break :blk {};
                    };

                    _completion.active = false;
                    @call(.always_inline, on_done, .{ _completion.userdatum(T), _loop, _completion, _completion.fd(), result });
                }
            }.wrap)),
            .metadata = .{
                .cmdsock = .{
                    .level = level,
                    .opt_name = optname,
                    .opt_value = opt.ptr,
                    .fd = _socket,
                    .opt_len = @intCast(opt.len),
                },
            },
        };

        loop.enqueue(completion);
    }

    /// Queues an SO_REUSEADDR operation.
    pub inline fn setReuseAddr(
        loop: *Loop,
        completion: *Completion,
        comptime T: type,
        userdata: *T,
        _socket: Socket,
        activate: bool,
        comptime on_done: *const fn (
            userdata: *T,
            loop: *Loop,
            completion: *Completion,
            _socket: Socket,
            result: SetSockOptError!void,
        ) void,
    ) void {
        return loop.setsockopt(
            completion,
            T,
            userdata,
            _socket,
            linux.SOL.SOCKET,
            linux.SO.REUSEADDR,
            &std.mem.toBytes(@as(c_int, @intFromBool(activate))),
            on_done,
        );
    }

    /// Queues an SO_REUSEPORT operation.
    pub inline fn setReusePort(
        loop: *Loop,
        completion: *Completion,
        comptime T: type,
        userdata: *T,
        _socket: Socket,
        activate: bool,
        comptime on_done: *const fn (
            userdata: *T,
            loop: *Loop,
            completion: *Completion,
            _socket: Socket,
            result: SetSockOptError!void,
        ) void,
    ) void {
        return loop.setsockopt(
            completion,
            T,
            userdata,
            _socket,
            linux.SOL.SOCKET,
            linux.SO.REUSEPORT,
            &std.mem.toBytes(@as(c_int, @intFromBool(activate))),
            on_done,
        );
    }

    pub const BindError = anyerror || CancelationError || UnexpectedError;

    /// Queue a bind operation.
    pub fn bind(
        loop: *Loop,
        completion: *Completion,
        comptime T: type,
        userdata: *T,
        _socket: Socket,
        addr: std.net.Address,
        comptime on_done: *const fn (
            userdata: *T,
            loop: *Loop,
            completion: *Completion,
            _socket: Socket,
            addr: std.net.Address,
            result: BindError!void,
        ) void,
    ) void {
        completion.* = .{
            .next = null,
            .userdata = userdata,
            .result = 0,
            .flags = 0,
            .operation = .bind,
            .active = true,
            .callback = @ptrCast(&(struct {
                fn wrap(_loop: *Loop, _completion: *Completion) void {
                    const res = _completion.result;

                    const result: BindError!void = blk: {
                        if (res < 0) {
                            break :blk switch (_completion.err()) {
                                .INTR, .AGAIN => return _loop.enqueue(_completion),
                                .ACCES, .PERM => error.AccessDenied,
                                .ADDRINUSE => error.AddressInUse,
                                .BADF => unreachable, // Always a race condition.
                                .INVAL => unreachable, // Invalid parameters.
                                .NOTSOCK => unreachable, // Invalid `sockfd`.
                                .AFNOSUPPORT => error.AddressFamilyNotSupported,
                                .ADDRNOTAVAIL => error.AddressNotAvailable,
                                .FAULT => unreachable, // Invalid `addr` pointer.
                                .LOOP => error.SymLinkLoop,
                                .NAMETOOLONG => error.NameTooLong,
                                .NOENT => error.FileNotFound,
                                .NOMEM => error.SystemResources,
                                .NOTDIR => error.NotDir,
                                .ROFS => error.ReadOnlyFileSystem,
                                .CANCELED => error.Canceled,
                                else => error.Unexpected,
                            };
                        }

                        break :blk {};
                    };

                    _completion.active = false;
                    const data = _completion.metadata.connect;
                    @call(.always_inline, on_done, .{ _completion.userdatum(T), _loop, _completion, _completion.fd(), std.net.Address{ .any = data.addr }, result });
                }
            }.wrap)),
            .metadata = .{
                .connect = .{
                    .addr = addr.any,
                    .fd = _socket,
                    .socklen = addr.getOsSockLen(),
                },
            },
        };

        loop.enqueue(completion);
    }

    pub const ListenError = error{
        AddressInUse,
        FileDescriptorNotASocket,
        OperationNotSupported,
    } || CancelationError || UnexpectedError;

    /// Queue a listen operation.
    pub fn listen(
        loop: *Loop,
        completion: *Completion,
        comptime T: type,
        userdata: *T,
        _socket: Socket,
        kernel_backlog: u31,
        comptime on_done: *const fn (
            userdata: *T,
            loop: *Loop,
            completion: *Completion,
            _socket: Socket,
            result: ListenError!void,
        ) void,
    ) void {
        completion.* = .{
            .next = null,
            .userdata = userdata,
            .result = 0,
            .flags = 0,
            .operation = .listen,
            .active = true,
            .callback = @ptrCast(&(struct {
                fn wrap(_loop: *Loop, _completion: *Completion) void {
                    const res = _completion.result;

                    const result: ListenError!void = blk: {
                        if (res < 0) {
                            break :blk switch (_completion.err()) {
                                .INTR, .AGAIN => return _loop.enqueue(_completion),
                                .ADDRINUSE => error.AddressInUse,
                                .BADF => unreachable,
                                .NOTSOCK => error.FileDescriptorNotASocket,
                                .OPNOTSUPP => error.OperationNotSupported,
                                .CANCELED => error.Canceled,
                                else => error.Unexpected,
                            };
                        }

                        break :blk {};
                    };

                    _completion.active = false;
                    @call(.always_inline, on_done, .{ _completion.userdatum(T), _loop, _completion, _completion.fd(), result });
                }
            }.wrap)),
            .metadata = .{
                .listen = .{
                    .fd = _socket,
                    .kernel_backlog = @as(u32, kernel_backlog),
                },
            },
        };

        loop.enqueue(completion);
    }

    //pub const TimeoutError = CancelationError || UnexpectedError;

    /// TODO: we might want to prefer IORING_TIMEOUT_ABS here.
    /// Queues a timeout operation.
    /// nanoseconds are given as u63 for coercion to i64.
    //pub fn timeout(
    //    self: *Self,
    //    completion: *Completion,
    //    comptime T: type,
    //    userdata: *T,
    //    ns: u63,
    //    comptime callback: *const fn (
    //        userdata: *T,
    //        loop: *Self,
    //        completion: *Completion,
    //        result: TimeoutError!void,
    //    ) void,
    //) void {
    //    completion.* = .{
    //        .next = null,
    //        .operation = .{
    //            .timeout = .{ .sec = 0, .nsec = ns },
    //        },
    //        .userdata = userdata,
    //        .callback = comptime struct {
    //            fn wrap(loop: *Self, c: *Completion) void {
    //                const cqe = c.cqe.?;
    //                const res = cqe.res;
    //
    //                const result: TimeoutError!void = if (res < 0) switch (@as(posix.E, @enumFromInt(-res))) {
    //                    .TIME => {}, // still a success
    //                    .INVAL => unreachable,
    //                    .FAULT => unreachable,
    //                    .CANCELED => error.Cancelled,
    //                    else => |err| posix.unexpectedErrno(err),
    //                } else {};
    //
    //                @call(.always_inline, callback, .{ @as(*T, @ptrCast(@alignCast(c.userdata))), loop, c, result });
    //            }
    //        }.wrap,
    //    };
    //
    //    self.enqueue(completion);
    //}

    /// Queues a close operation.
    /// Supports both file descriptors and direct descriptors.
    //pub fn close(
    //    self: *Loop,
    //    completion: *Completion,
    //    comptime T: type,
    //    userdata: *T,
    //    handle_or_socket: HandleOrSocket,
    //    comptime callback: *const fn (
    //        userdata: *T,
    //        loop: *Loop,
    //        completion: *Completion,
    //    ) void,
    //) void {
    //    completion.* = .{
    //        .next = null,
    //        .operation = .{ .close = handle_or_socket },
    //        .userdata = userdata,
    //        .callback = comptime struct {
    //            fn wrap(loop: *Self, c: *Completion) void {
    //                const cqe = c.cqe.?;
    //                const res = cqe.res;
    //
    //                // According to Zen, close operations should not fail.
    //                // So we do not return any errors here.
    //                switch (@as(posix.E, @enumFromInt(-res))) {
    //                    .BADF => unreachable, // Always a race condition.
    //                    .INTR => {}, // This is still a success. See https://github.com/ziglang/zig/issues/2425
    //                    else => {},
    //                }
    //
    //                // execute the user provided callback
    //                @call(.always_inline, callback, .{ @as(*T, @ptrCast(@alignCast(c.userdata))), loop, c });
    //            }
    //        }.wrap,
    //    };
    //
    //    self.enqueue(completion);
    //}

    /// Sets REUSEADDR flag on a given socket.
    //pub fn setReuseAddr(_: Self, socket: Socket) posix.SetSockOptError!void {
    //    return posix.setsockopt(socket, posix.SOL.SOCKET, posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));
    //}

    /// Sets REUSEPORT flag on a given socket if possible.
    //pub fn setReusePort(_: Self, socket: Socket) posix.SetSockOptError!void {
    //    if (@hasDecl(std.posix.SO, "REUSEPORT")) {
    //        return posix.setsockopt(socket, posix.SOL.SOCKET, posix.SO.REUSEPORT, &std.mem.toBytes(@as(c_int, 1)));
    //    }
    //}

    /// Enables/disables TCP_NODELAY setting on a given socket.
    //pub fn setTcpNoDelay(_: Self, socket: Socket, on: bool) posix.SetSockOptError!void {
    //    return posix.setsockopt(socket, posix.IPPROTO.TCP, posix.TCP.NODELAY, &std.mem.toBytes(@as(c_int, @intFromBool(on))));
    //}

    /// Enables/disables SO_KEEPALIVE setting on a given socket.
    //pub fn setTcpKeepAlive(_: Self, socket: Socket, on: bool) posix.SetSockOptError!void {
    //    return posix.setsockopt(socket, posix.SOL.SOCKET, posix.SO.KEEPALIVE, &std.mem.toBytes(@as(c_int, @intFromBool(on))));
    //}

    /// Represents operations.
    /// Having completions larger than 64 bytes is banned, keep it under or equal to 64 whatever the backend, arch etc.
    pub const Completion = extern struct {
        /// Intrusively linked to next operation.
        next: ?*Completion = null,
        /// Type-erased userdata, use userdatum to give it a type.
        userdata: ?*anyopaque = null,
        /// Type-erased function pointer.
        /// NOTE: This is not null after a prep call.
        callback: ?*const anyopaque = null,
        /// Result from the CQE, filled after the operation is completed.
        result: i32 = 0,
        /// This will be used in two ways:
        /// * When a completion is submitted, SQE flags can be set here (currently not preferred),
        /// * When a completion is finished, CQE flags will be set here.
        flags: u32 = 0,
        /// Operation intended by this completion.
        operation: OperationType = .none, // 1
        /// Whether or not the completion is in the works.
        /// TODO: Not implemented yet.
        active: bool = false,
        /// This'll be given by compiler anyway, reserved here for future usage.
        /// NOTE: We may have this inside `metadata` too.
        __pad: [6]u8 = undefined,
        /// Various operation metadata.
        metadata: Operation = .{ .none = {} },

        /// Userdata with a type.
        pub inline fn userdatum(completion: *const Completion, comptime T: type) *T {
            return @as(*T, @ptrCast(@alignCast(completion.userdata)));
        }

        /// Whether completion is pending to perform an operation or free to use.
        pub inline fn isPending(completion: *const Completion) bool {
            return completion.active;
        }

        /// Internal.
        /// Returns the file descriptor this completion has.
        /// # SAFETY: Only valid if the `metadata` of the operation contains `fd` field.
        pub inline fn fd(completion: *const Completion) linux.fd_t {
            return completion.metadata.rw.fd;
        }

        /// Internal.
        /// Returns the full buffer provided for the operation.
        /// # SAFETY: Only valid for read/write/recv/send operations.
        inline fn slice(completion: *const Completion, comptime is_const: bool) if (is_const) []const u8 else []u8 {
            const data = if (comptime is_const) completion.metadata.rw_const else completion.metadata.rw;
            return data.base[0..data.len];
        }

        /// Internal.
        /// Returns the system error for the current result.
        fn err(completion: *const Completion) posix.E {
            return @enumFromInt(-completion.result);
        }

        /// Callback to be invoked when an operation is complete.
        pub const Callback = *const fn (*Loop, *Completion) void;

        /// Operation kind.
        pub const OperationType = enum(u8) {
            none = 0,
            connect,
            bind,
            listen,
            accept,
            recv,
            send,
            timeout,
            close,
            socket,
            setsockopt,
        };

        // Data needed by operations.
        pub const Operation = extern union {
            none: void,
            /// recv/send.
            rw: extern struct {
                base: [*]u8,
                len: usize,
                fd: linux.fd_t,
                flags: u32,
            },
            /// Same as before, but const pointer for base field.
            rw_const: extern struct {
                base: [*]const u8,
                len: usize,
                fd: linux.fd_t,
                flags: u32,
            },
            connect: extern struct {
                addr: posix.sockaddr,
                fd: linux.fd_t,
                socklen: posix.socklen_t,
            },
            accept: extern struct {
                addr: posix.sockaddr = undefined,
                fd: linux.fd_t, // Listener socket.
                socklen: posix.socklen_t = @sizeOf(posix.sockaddr),
            },
            timeout: extern struct {
                timespec: linux.kernel_timespec,
            },
            listen: extern struct {
                // Doing this to be on par with other metadata.
                __pad: [block_size * 2]u8 = undefined,
                fd: linux.fd_t,
                kernel_backlog: u32, // Only 31-bits used to be on par with i32.
            },
            socket: extern struct {
                domain: u32,
                type: u32,
                protocol: u32,
                flags: u32,
            },
            cmdsock: extern struct {
                level: u32,
                opt_name: u32,
                opt_value: [*]const u8,
                fd: linux.fd_t,
                opt_len: posix.socklen_t,
            },
            close: extern struct {
                // Doing this to be on par with other metadata.
                __pad: [block_size * 2]u8 = undefined,
                fd: linux.fd_t,
            },
        };
    };

    // On unix, there's a unified fd type that's used for both sockets and file, pipe etc. handles.
    // Since this library is intended for cross platform usage and windows separates file descriptors by different kinds, I've decided to adapt the same mindset to here.
    pub const Handle = linux.fd_t;
    pub const Socket = linux.fd_t;
    // Some operations may support either.
    pub const HandleOrSocket = linux.fd_t;
};

// TODO: More tests.
test "io_uring: misc" {
    const Loop = IoUring;
    const Completion = Loop.Completion;

    try testing.expect(@sizeOf(Completion) == 64);

    std.debug.print("Passed io_uring: misc\n", .{});
}

test "io_uring: socket/setsockopt/bind/listen" {
    const Loop = IoUring;
    const Completion = Loop.Completion;

    var loop = try Loop.init();
    defer loop.deinit();

    const Callbacks = struct {
        fn on_socket_open(
            _: *Completion,
            _loop: *Loop,
            _completion: *Completion,
            result: Loop.SocketError!Loop.Socket,
        ) void {
            const socket = result catch |err| @panic(@errorName(err));
            // We must've allocated the first spot for the socket.
            testing.expect(socket == 0) catch |err| @panic(@errorName(err));
            // Completion must be reusable at this point.
            testing.expect(_completion.isPending() == false) catch |err| @panic(@errorName(err));

            _loop.setReuseAddr(_completion, Completion, _completion, socket, true, on_set_reuse_addr);
        }

        fn on_set_reuse_addr(
            _: *Completion,
            _loop: *Loop,
            _completion: *Completion,
            _socket: Loop.Socket,
            result: Loop.SetSockOptError!void,
        ) void {
            result catch |err| @panic(@errorName(err));
            testing.expect(_completion.isPending() == false) catch |err| @panic(@errorName(err));

            // Bind.
            const addr = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, 7777);
            _loop.bind(_completion, Completion, _completion, _socket, addr, on_socket_bind);
        }

        fn on_socket_bind(
            _completion: *Completion,
            _loop: *Loop,
            _: *Completion,
            _socket: Loop.Socket,
            _: std.net.Address,
            result: Loop.BindError!void,
        ) void {
            result catch |err| @panic(@errorName(err));
            testing.expect(_completion.isPending() == false) catch |err| @panic(@errorName(err));

            _loop.listen(_completion, Completion, _completion, _socket, 128, on_socket_listen);
        }

        fn on_socket_listen(
            _: *Completion,
            _: *Loop,
            _completion: *Completion,
            _: Loop.Socket,
            result: Loop.ListenError!void,
        ) void {
            result catch unreachable;
            testing.expect(_completion.isPending() == false) catch |err| @panic(@errorName(err));
        }
    };

    // Opening a direct descriptor.
    var c = Completion{};
    loop.openSocket(&c, Completion, &c, posix.AF.INET, posix.SOCK.STREAM, posix.IPPROTO.TCP, 0, Callbacks.on_socket_open);

    try loop.run();
    std.debug.print("Passed io_uring: socket/setsockopt/bind/listen\n", .{});
}
