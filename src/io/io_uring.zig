// Code here is heavily based on both tigerbeetle/io and libxev.
// Thanks TigerBeetle team and @mitchellh!
// https://github.com/tigerbeetle/tigerbeetle/blob/main/src/io/linux.zig
// https://github.com/mitchellh/libxev/blob/main/src/backend/io_uring.zig
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
const BufferPool = @import("../buffer_pool.zig").BufferPool(.io_uring);

// TODO: add support for this in zig std.
inline fn io_uring_prep_cancel_fd(sqe: *io_uring_sqe, fd: linux.fd_t, flags: u32) void {
    sqe.prep_rw(.ASYNC_CANCEL, fd, 0, 0, 0);
    sqe.rw_flags = flags | linux.IORING_ASYNC_CANCEL_FD;
}

// TODO: add support for this in zig std.
inline fn io_uring_prep_files_update(sqe: *io_uring_sqe, fds: []const linux.fd_t, offset: u32) void {
    sqe.prep_rw(.FILES_UPDATE, -1, @intFromPtr(fds.ptr), fds.len, @intCast(offset));
}

// TODO: add support for this in zig std.
// FIXME: implementation here doesn't match with liburing, might want to change that.
// https://github.com/axboe/liburing/blob/76bb80a36107e3808c4770c8112583813a4e511b/src/register.c#L139
fn io_uring_register_files_sparse(ring: *IO_Uring, nr: u32) !void {
    const reg = &linux.io_uring_rsrc_register{
        .nr = nr,
        .flags = linux.IORING_RSRC_REGISTER_SPARSE,
        .resv2 = 0,
        .data = 0,
        .tags = 0,
    };

    const res = linux.io_uring_register(
        ring.fd,
        .REGISTER_FILES2,
        @ptrCast(reg),
        @as(u32, @sizeOf(linux.io_uring_rsrc_register)),
    );

    return switch (linux.E.init(res)) {
        .SUCCESS => {},
        // One or more fds in the array are invalid, or the kernel does not support sparse sets:
        .BADF => error.FileDescriptorInvalid,
        .BUSY => error.FilesAlreadyRegistered,
        .INVAL => error.FilesEmpty,
        // Adding `nr_args` file references would exceed the maximum allowed number of files the
        // user is allowed to have according to the per-user RLIMIT_NOFILE resource limit and
        // the CAP_SYS_RESOURCE capability is not set, or `nr_args` exceeds the maximum allowed
        // for a fixed file set (older kernels have a limit of 1024 files vs 64K files):
        .MFILE => error.UserFdQuotaExceeded,
        // Insufficient kernel resources, or the caller had a non-zero RLIMIT_MEMLOCK soft
        // resource limit but tried to lock more memory than the limit permitted (not enforced
        // when the process is privileged with CAP_IPC_LOCK):
        .NOMEM => error.SystemResources,
        // Attempt to register files on a ring already registering files or being torn down:
        .NXIO => error.RingShuttingDownOrAlreadyRegisteringFiles,
        else => |errno| posix.unexpectedErrno(errno),
    };
}

// event notifier implementation based on io_uring.
// FIXME: add error types
pub fn Loop(comptime options: Options) type {
    return struct {
        const Self = @This();
        /// io_uring instance
        ring: IO_Uring,
        /// io operations that're not queued yet
        unqueued: Intrusive(Completion) = .{},
        /// io operations that're completed and ready to fly
        completed: Intrusive(Completion) = .{},
        /// count of operations that we're waiting to be done
        io_pending: usize = 0,

        /// Initializes a new event loop backed by io_uring.
        pub fn init() !Self {
            var flags: u32 = 0;

            // SQPOLL is disabled for now, I'm looking for ways to better utilize it.
            //if (comptime options.io_uring.sqe_polling) {
            //    flags |= linux.IORING_SETUP_SQPOLL;
            //}

            if (comptime options.io_uring.single_issuer) {
                flags |= linux.IORING_SETUP_SINGLE_ISSUER;
                flags |= linux.IORING_SETUP_DEFER_TASKRUN;
            }

            if (comptime options.io_uring.cooperative_task_running) {
                flags |= linux.IORING_SETUP_COOP_TASKRUN;
                flags |= linux.IORING_SETUP_TASKRUN_FLAG;
            }

            return .{ .ring = try IO_Uring.init(256, flags) };
        }

        // Deinitializes the event loop.
        pub fn deinit(self: *Self) void {
            self.ring.deinit();
        }

        // Runs the event loop 'till all submitted operations are completed.
        pub fn run(self: *Self) !void {
            while (self.io_pending > 0 and self.unqueued.isEmpty()) {
                // Flush any queued SQEs and reuse the same syscall to wait for completions if required:
                try self.flushSubmissions();
                // We can now just peek for any CQEs without waiting and without another syscall:
                try self.flushCompletions();

                // The SQE array is empty from flushSubmissions(). Fill it up with unqueued completions.
                // This runs before `self.completed` is flushed below to prevent new IO from reserving SQE
                // slots and potentially starving those in `self.unqueued`.
                // Loop over a copy to avoid an infinite loop of `enqueue()` re-adding to `self.unqueued`.
                {
                    var copy = self.unqueued;
                    self.unqueued = .{};
                    while (copy.pop()) |c| self.enqueue(c);
                }

                while (self.completed.pop()) |c| {
                    c.callback(self, c);
                }
            }
        }

        fn flushCompletions(self: *Self) !void {
            var cqes: [512]io_uring_cqe = undefined;
            while (true) {
                const completed = self.ring.copy_cqes(&cqes, 1) catch |err| switch (err) {
                    error.SignalInterrupt => continue,
                    else => return err,
                };

                // decrement as much as completed events
                self.io_pending -= completed;

                for (cqes[0..completed]) |*cqe| {
                    const c: *Completion = @ptrFromInt(cqe.user_data);
                    // FIXME: Passing a copy might be better here
                    c.cqe = cqe;

                    // We do not run the completion here (instead appending to a linked list) to avoid:
                    // * recursion through `flush_submissions()` and `flush_completions()`,
                    // * unbounded stack usage, and
                    // * confusing stack traces.
                    self.completed.push(c);
                }

                break;
            }
        }

        fn flushSubmissions(self: *Self) !void {
            while (true) {
                // don't decrement the pending count here, we'll take care of it after calling copy_cqes.
                _ = self.ring.submit_and_wait(1) catch |err| switch (err) {
                    error.SignalInterrupt => continue,
                    error.CompletionQueueOvercommitted, error.SystemResources => {
                        // flush completions to create space
                        try self.flushCompletions();
                        continue;
                    },
                    else => return err,
                };

                break;
            }
        }

        // Registers a completion to the loop.
        fn enqueue(self: *Self, c: *Completion) void {
            // get a submission queue entry
            const sqe = self.ring.get_sqe() catch |err| switch (err) {
                //if SQE ring is full, the completion will be added to unqueued.
                error.SubmissionQueueFull => return self.unqueued.push(c),
            };

            // prepare the completion.
            switch (c.operation) {
                .none => unreachable,
                .read => |op| sqe.prep_rw(.READ, op.handle, @intFromPtr(op.buffer.ptr), op.buffer.len, op.offset),
                .write => |op| sqe.prep_rw(.WRITE, op.handle, @intFromPtr(op.buffer.ptr), op.buffer.len, op.offset),
                .connect => |*op| {
                    sqe.prep_connect(op.socket, &op.addr.any, op.addr.getOsSockLen());

                    if (comptime options.io_uring.direct_descriptors_mode) {
                        // we want to use direct descriptors
                        sqe.flags |= linux.IOSQE_FIXED_FILE;
                    }
                },
                .accept => |*op| switch (comptime options.io_uring.direct_descriptors_mode) {
                    true => {
                        // NOTE: Direct descriptors don't support SOCK_CLOEXEC flag at the moment.
                        // https://github.com/axboe/liburing/issues/1330
                        sqe.prep_multishot_accept_direct(op.socket, &op.addr, &op.addr_size, 0);
                        // we want to use direct descriptors
                        sqe.flags |= linux.IOSQE_FIXED_FILE;
                    },
                    // regular multishot accept
                    false => sqe.prep_multishot_accept(op.socket, &op.addr, &op.addr_size, posix.SOCK.CLOEXEC),
                },
                .recv => |op| {
                    if (comptime options.io_uring.buffer_pool_mode) {
                        // read multishot requires everything to be zero since the buffer will be selected automatically
                        sqe.prep_rw(.RECV, op.socket, 0, 0, 0);
                        // which buffer group to use, in liburing, field set is `buf_group` but it's actually defined as a union
                        sqe.buf_index = op.buf_pool.group_id;
                    } else {
                        // recv with userspace buffer
                        sqe.prep_rw(.RECV, op.socket, @intFromPtr(op.buffer.ptr), op.buffer.len, 0);
                    }

                    // we want to use direct descriptors
                    if (comptime options.io_uring.direct_descriptors_mode) {
                        sqe.flags |= linux.IOSQE_FIXED_FILE;
                    }

                    // we want to use buffer pools
                    if (comptime options.io_uring.buffer_pool_mode) {
                        // indicate that we've selected a buffer group
                        sqe.flags |= linux.IOSQE_BUFFER_SELECT;
                        // NOTE: Following flag only works on provided buffers.
                        // https://github.com/axboe/liburing/issues/1331#issuecomment-2599784984
                        sqe.ioprio |= linux.IORING_RECV_MULTISHOT;
                    }
                },
                .timeout => |*ts| sqe.prep_timeout(ts, 0, linux.IORING_TIMEOUT_ETIME_SUCCESS),
                // TODO: support cancelling timeouts
                .cancel => |op| switch (op) {
                    .all_io => |fd| io_uring_prep_cancel_fd(sqe, fd, linux.IORING_ASYNC_CANCEL_ALL),
                    .completion => |comp| sqe.prep_cancel(@intCast(@intFromPtr(comp)), 0),
                },
                .fds_update => |op| switch (comptime options.io_uring.direct_descriptors_mode) {
                    true => io_uring_prep_files_update(sqe, op.fds, op.offset),
                    false => unreachable,
                },
                .close => |fd| {
                    if (comptime options.io_uring.direct_descriptors_mode) {
                        sqe.prep_close_direct(@intCast(fd)); // works similar to fds_update
                    } else {
                        sqe.prep_close(fd);
                    }

                    // we'll allways set this to completion
                    sqe.user_data = @intFromPtr(c);

                    // FIXME: should I?
                    // don't create a CQE for this
                    sqe.flags |= linux.IOSQE_CQE_SKIP_SUCCESS;

                    // early return since we'll not increment the pending count
                    return;
                },
            }

            // userdata of an SQE is always set to a completion object.
            sqe.user_data = @intFromPtr(c);

            // we got a pending io
            self.io_pending += 1;
        }

        // Queues a read operation.
        pub fn read(
            self: *Self,
            comptime T: type,
            userdata: *T,
            completion: *Completion,
            handle: Handle,
            buffer: []u8,
            offset: u64,
            comptime callback: *const fn (
                userdata: *T,
                loop: *Self,
                completion: *Completion,
            ) void,
        ) void {
            completion.* = .{
                .next = null,
                .operation = .{
                    .read = .{
                        .handle = handle,
                        .buffer = buffer,
                        .offset = offset,
                    },
                },
                .userdata = userdata,
                // following is what happens when we receive a sqe for this completion.
                .callback = comptime struct {
                    fn wrap(loop: *Self, c: *Completion) void {
                        // invoke the user provided callback
                        @call(.always_inline, callback, .{ @as(*T, @ptrCast(@alignCast(c.userdata))), loop, c });
                    }
                }.wrap,
            };

            self.enqueue(completion);
        }

        // Queues a write operation.
        pub fn write(
            self: *Self,
            comptime T: type,
            userdata: *T,
            completion: *Completion,
            handle: Handle,
            buffer: []const u8,
            offset: u64,
            comptime callback: *const fn (
                userdata: *T,
                loop: *Self,
                completion: *Completion,
                // FIXME: use a well defined error union type instead of anyerror
                //result: anyerror!usize,
            ) void,
        ) void {
            completion.* = .{
                .next = null,
                .operation = .{
                    .write = .{
                        .handle = handle,
                        .buffer = buffer,
                        .offset = offset,
                    },
                },
                .userdata = userdata,
                // following is what happens when we receive a sqe for this completion.
                .callback = comptime struct {
                    fn wrap(loop: *Self, c: *Completion) void {
                        // invoke the user provided callback
                        @call(.always_inline, callback, .{ @as(*T, @ptrCast(@alignCast(c.userdata))), loop, c });
                    }
                }.wrap,
            };

            self.enqueue(completion);
        }

        // Queues a connect operation.
        pub fn connect(
            self: *Self,
            comptime T: type,
            userdata: *T,
            completion: *Completion,
            socket: Socket,
            addr: std.net.Address,
            comptime callback: *const fn (
                userdata: *T,
                loop: *Self,
                completion: *Completion,
            ) void,
        ) void {
            completion.* = .{
                .next = null,
                .operation = .{
                    .connect = .{
                        .socket = socket,
                        .addr = addr,
                    },
                },
                .userdata = userdata,
                .callback = comptime struct {
                    fn wrap(loop: *Self, c: *Completion) void {
                        @call(.always_inline, callback, .{ @as(*T, @ptrCast(@alignCast(c.userdata))), loop, c });
                    }
                }.wrap,
            };

            self.enqueue(completion);
        }

        // error that's not really expected
        pub const UnexpectedError = error{Unexpected};
        // error that's used on operations that can be cancelled
        pub const CancellationError = error{Cancelled};

        // possible errors accept can create
        pub const AcceptError = error{
            WouldBlock,
            ConnectionAborted,
            SocketNotListening,
            ProcessFdQuotaExceeded,
            SystemFdQuotaExceeded,
            SystemResources,
            OperationNotSupported,
            PermissionDenied,
            ProtocolFailure,
        } || CancellationError || UnexpectedError;

        // Starts accepting connections for a given socket.
        // This a multishot operation, meaning it'll keep on running until it's either cancelled or encountered with an error.
        // Completions given to multishot operations MUST NOT be reused until the multishot operation is either cancelled or encountered with an error.
        pub fn accept(
            self: *Self,
            comptime T: type,
            userdata: *T,
            completion: *Completion,
            socket: Socket,
            comptime callback: *const fn (
                userdata: *T,
                loop: *Self,
                completion: *Completion,
                result: AcceptError!Socket,
            ) void,
        ) void {
            completion.* = .{
                .next = null,
                .operation = .{
                    .accept = .{
                        .socket = socket,
                        .addr = undefined,
                        .addr_size = @sizeOf(posix.sockaddr),
                    },
                },
                .userdata = userdata,
                .callback = comptime struct {
                    fn wrap(loop: *Self, c: *Completion) void {
                        const cqe = c.cqe.?;
                        const res = cqe.res;

                        const result: AcceptError!Socket = if (res < 0) switch (@as(posix.E, @enumFromInt(-res))) {
                            .INTR => return loop.enqueue(c), // we're interrupted, try again
                            .AGAIN => error.WouldBlock,
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
                            .CANCELED => error.Cancelled,
                            else => error.Unexpected,
                        } else res; // valid socket

                        // we're accepting via `io_uring_prep_multishot_accept` so we have to increment pending for each successful accept request
                        // to keep the loop running.
                        // this must be done no matter what `cqe.res` is given.
                        if (cqe.flags & linux.IORING_CQE_F_MORE != 0) {
                            loop.io_pending += 1;
                        }

                        // invoke the user provided callback
                        @call(.always_inline, callback, .{ @as(*T, @ptrCast(@alignCast(c.userdata))), loop, c, result });
                    }
                }.wrap,
            };

            self.enqueue(completion);
        }

        pub const BufferPoolError = error{OutOfBuffers};

        // shared errors with buffer pool mode
        pub const RecvMutualError = error{
            EndOfStream,
            WouldBlock,
            SystemResources,
            OperationNotSupported,
            PermissionDenied,
            SocketNotConnected,
            ConnectionResetByPeer,
            ConnectionTimedOut,
            ConnectionRefused,
        } || CancellationError || UnexpectedError;

        pub const RecvError = switch (options.io_uring.buffer_pool_mode) {
            true => RecvMutualError || BufferPoolError,
            false => RecvMutualError,
        };

        /// Queues a recv operation.
        pub fn recv(
            self: *Self,
            completion: *Completion,
            comptime T: type,
            userdata: *T,
            socket: Socket,
            buf_pool_or_buffer: switch (options.io_uring.buffer_pool_mode) {
                true => *BufferPool,
                false => []u8,
            },
            comptime callback: switch (options.io_uring.buffer_pool_mode) {
                // buffer pool activated
                true => *const fn (
                    userdata: *T,
                    loop: *Self,
                    completion: *Completion,
                    socket: Socket,
                    buffer_pool: *BufferPool,
                    buffer_id: u16,
                    result: RecvError!u32, // actually u31
                ) void,
                // regular
                false => *const fn (
                    userdata: *T,
                    loop: *Self,
                    completion: *Completion,
                    socket: Socket,
                    buffer: []u8,
                    result: RecvError!u32, // actually u31
                ) void,
            },
        ) void {
            completion.* = .{
                .next = null,
                .operation = .{
                    .recv = switch (comptime options.io_uring.buffer_pool_mode) {
                        true => .{ .socket = socket, .buf_pool = buf_pool_or_buffer },
                        false => .{ .socket = socket, .buffer = buf_pool_or_buffer },
                    },
                },
                .userdata = userdata,
                .callback = struct {
                    fn wrap(loop: *Self, c: *Completion) void {
                        const cqe = c.cqe.?;
                        const res = cqe.res;
                        const op = c.operation.recv;

                        // error or success
                        const result: RecvError!u32 = if (res <= 0)
                            switch (@as(posix.E, @enumFromInt(-res))) {
                                .SUCCESS => error.EndOfStream, // 0 reads are interpretted as errors
                                .INTR => return loop.enqueue(c), // we're interrupted, try again
                                .AGAIN => error.WouldBlock,
                                .BADF => error.Unexpected,
                                .CONNREFUSED => error.ConnectionRefused,
                                .FAULT => unreachable,
                                .INVAL => unreachable,
                                .NOBUFS => if (comptime options.io_uring.buffer_pool_mode) error.OutOfBuffers else error.SystemResources,
                                .NOMEM => error.SystemResources,
                                .NOTCONN => error.SocketNotConnected,
                                .NOTSOCK => error.Unexpected,
                                .CONNRESET => error.ConnectionResetByPeer,
                                .TIMEDOUT => error.ConnectionTimedOut,
                                .OPNOTSUPP => error.OperationNotSupported,
                                .CANCELED => error.Cancelled,
                                else => error.Unexpected,
                            }
                        else
                            @intCast(res); // valid length

                        // what mode we're on
                        if (comptime options.io_uring.buffer_pool_mode) {
                            // which buffer did the worker pick from the pool?
                            const buffer_id: u16 = @intCast(cqe.flags >> linux.IORING_CQE_BUFFER_SHIFT);

                            // The application should check the flags of each CQE,
                            // regardless of its result. If a posted CQE does not have the
                            // IORING_CQE_F_MORE flag set then the multishot receive will be
                            // done and the application should issue a new request.
                            // https://man7.org/linux/man-pages/man3/io_uring_prep_recv_multishot.3.html
                            if (cqe.flags & linux.IORING_CQE_F_MORE != 0) {
                                loop.io_pending += 1;
                            }

                            // NOTE: Currently its up to caller to give back the buffer to kernel.
                            // This behaviour might change in the future though.
                            @call(
                                .always_inline,
                                callback,
                                .{ @as(*T, @ptrCast(@alignCast(c.userdata))), loop, c, op.socket, op.buf_pool, buffer_id, result },
                            );
                        } else {
                            // regular buffer version
                            @call(
                                .always_inline,
                                callback,
                                .{ @as(*T, @ptrCast(@alignCast(c.userdata))), loop, c, op.socket, op.buffer, result },
                            );

                            // NOTE: interesting behavior when received 0 length msg
                            //std.debug.print("{}\n", .{cqe.flags & linux.IORING_CQE_F_SOCK_NONEMPTY != 0});
                        }

                        // FIXME: IDK if we can benefit from this
                        // if (cqe.flags & linux.IORING_CQE_F_SOCK_NONEMPTY != 0) {
                        //     still has data...
                        // }
                    }
                }.wrap,
            };

            self.enqueue(completion);
        }

        pub const TimeoutError = CancellationError || UnexpectedError;

        /// TODO: we might want to prefer IORING_TIMEOUT_ABS here.
        /// Queues a timeout operation.
        /// nanoseconds are given as u63 for coercion to i64.
        pub fn timeout(
            self: *Self,
            completion: *Completion,
            comptime T: type,
            userdata: *T,
            ns: u63,
            comptime callback: *const fn (
                userdata: *T,
                loop: *Self,
                completion: *Completion,
                result: TimeoutError!void,
            ) void,
        ) void {
            completion.* = .{
                .next = null,
                .operation = .{
                    .timeout = .{ .sec = 0, .nsec = ns },
                },
                .userdata = userdata,
                .callback = comptime struct {
                    fn wrap(loop: *Self, c: *Completion) void {
                        const cqe = c.cqe.?;
                        const res = cqe.res;

                        const result: TimeoutError!void = if (res < 0) switch (@as(posix.E, @enumFromInt(-res))) {
                            .TIME => {}, // still a success
                            .INVAL => unreachable,
                            .FAULT => unreachable,
                            .CANCELED => error.Cancelled,
                            else => |err| posix.unexpectedErrno(err),
                        } else {};

                        @call(.always_inline, callback, .{ @as(*T, @ptrCast(@alignCast(c.userdata))), loop, c, result });
                    }
                }.wrap,
            };

            self.enqueue(completion);
        }

        // Queues a cancel operation.
        // If a handle is given as target, all I/O operations of the handle are cancelled.
        pub fn cancel(
            self: *Self,
            comptime cancel_type: Completion.Operation.CancelType,
            target: @FieldType(Completion.Operation.Cancel, @tagName(cancel_type)),
            completion: *Completion,
        ) void {
            completion.* = .{
                .next = null,
                .operation = .{
                    .cancel = @unionInit(Completion.Operation.Cancel, @tagName(cancel_type), target),
                },
                .userdata = null,
                .callback = comptime struct {
                    fn wrap(_: *Self, _: *Completion) void {
                        std.debug.print("cancellation run\n", .{});
                    }
                }.wrap,
            };

            self.enqueue(completion);
        }

        /// Queues a close operation.
        /// Supports both file descriptors and direct descriptors.
        /// This operation doesn't invoke a callback, so the given descriptor must be considered invalid after this call.
        pub fn close(self: *Self, completion: *Completion, fd: Handle) void {
            completion.* = .{
                .next = null,
                .operation = .{ .close = fd },
                .userdata = null,
                .callback = comptime struct {
                    fn wrap(_: *Self, c: *Completion) void {
                        const cqe = c.cqe.?;
                        const res = cqe.res;

                        switch (@as(posix.E, @enumFromInt(-res))) {
                            .BADF => unreachable, // Always a race condition.
                            .INTR => {}, // This is still a success. See https://github.com/ziglang/zig/issues/2425
                            else => {},
                        }
                    }
                }.wrap,
            };

            self.enqueue(completion);
        }

        pub const Registration = enum(u1) { regular, sparse };

        /// TODO: We might want to support resource tagging too, needs further investigations.
        ///
        /// io_uring only.
        ///
        /// Registers file descriptors that're going to be used within ring.
        /// * `regular` option requires a constant array of file descriptors.
        /// * `sparse` option allocates an invalid (-1) array of direct descriptors for the ring. Good if you want to register file descriptors later on.
        pub fn directDescriptors(
            self: *Self,
            comptime registration: Registration,
            array_or_u32: switch (registration) {
                .regular => []const linux.fd_t,
                .sparse => u32,
            },
        ) !void {
            if (!options.io_uring.direct_descriptors_mode) {
                @compileError("direct descriptors are not enabled");
            }

            return switch (registration) {
                .regular => self.ring.register_files(array_or_u32),
                .sparse => io_uring_register_files_sparse(&self.ring, array_or_u32),
            };
        }

        /// TODO: We might want to support resource tagging too, needs further investigations.
        /// io_uring only.
        ///
        /// Updates file descriptors. Starting from the `offset`.
        pub fn updateDescriptors(self: *Self, offset: u32, fds: []const linux.fd_t) !void {
            if (!options.io_uring.direct_descriptors_mode) {
                @compileError("direct descriptors are not enabled");
            }

            return self.ring.register_files_update(offset, fds);
        }

        pub const UpdateFdsError = error{
            OutOfMemory,
            InvalidArguments,
            UnableToCopyFileDescriptors,
            InvalidFileDescriptor,
            FileDescriptorOverflow,
        } || UnexpectedError;

        // Since this utility is used for both regular and alloc variants, it's better to have it write here once.
        fn getUpdateFdsResult(res: i32) UpdateFdsError!i32 {
            // FIXME: Is this operation cancellable?
            // FIXME: Are there any other errors this can produce?
            // https://www.man7.org/linux/man-pages/man3/io_uring_prep_files_update.3.html
            if (res < 0) {
                return switch (@as(posix.E, @enumFromInt(-res))) {
                    .NOMEM => error.OutOfMemory,
                    .INVAL => error.InvalidArguments,
                    .FAULT => error.UnableToCopyFileDescriptors,
                    .BADF => error.InvalidFileDescriptor,
                    .OVERFLOW => error.FileDescriptorOverflow,
                    else => |err| posix.unexpectedErrno(err),
                };
            }

            return res;
        }

        /// io_uring only.
        ///
        /// Queues an `io_uring_prep_files_update` operation, similar to `updateDescriptors` but happens in async manner.
        pub fn updateFds(
            self: *Self,
            completion: *Completion,
            comptime T: type,
            userdata: *T,
            fds: []const linux.fd_t,
            offset: u32, // NOTE: type of this might be a problem but I'm not totally sure
            comptime callback: *const fn (
                userdata: *T,
                loop: *Self,
                completion: *Completion,
                fds: []const linux.fd_t,
                offset: u32,
                result: UpdateFdsError!i32, // result can only be max. int32 either
            ) void,
        ) void {
            if (!options.io_uring.direct_descriptors_mode) {
                @compileError("direct descriptors are not enabled");
            }

            completion.* = .{
                .next = null,
                .operation = .{
                    .fds_update = .{
                        .fds = @constCast(fds),
                        .offset = offset,
                    },
                },
                .userdata = userdata,
                .callback = comptime struct {
                    fn wrap(loop: *Self, c: *Completion) void {
                        const cqe = c.cqe.?;
                        // get the result of this operation
                        const result = getUpdateFdsResult(cqe.res);

                        const op = c.operation.fds_update;

                        @call(.always_inline, callback, .{ @as(*T, @ptrCast(@alignCast(c.userdata))), loop, c, op.fds, op.offset, result });
                    }
                }.wrap,
            };

            self.enqueue(completion);
        }

        /// io_uring only.
        ///
        /// Same as `updateFds` but this variant doesn't take `offset` argument. Instead, it allocates fds
        /// to empty spots and fills the given `fds` slice with direct descriptors.
        pub fn updateFdsAlloc(
            self: *Self,
            completion: *Completion,
            comptime T: type,
            userdata: *T,
            fds: []linux.fd_t,
            comptime callback: *const fn (
                userdata: *T,
                loop: *Self,
                completion: *Completion,
                fds: []linux.fd_t,
                result: UpdateFdsError!i32, // result can only be max. int32 either
            ) void,
        ) void {
            if (!options.io_uring.direct_descriptors_mode) {
                @compileError("direct descriptors are not enabled");
            }

            completion.* = .{
                .next = null,
                .operation = .{
                    .fds_update = .{
                        .fds = fds,
                        .offset = linux.IORING_FILE_INDEX_ALLOC,
                    },
                },
                .userdata = userdata,
                .callback = comptime struct {
                    fn wrap(loop: *Self, c: *Completion) void {
                        const cqe = c.cqe.?;
                        // get the result of this operation
                        const result = getUpdateFdsResult(cqe.res);

                        const op = c.operation.fds_update;

                        @call(.always_inline, callback, .{ @as(*T, @ptrCast(@alignCast(c.userdata))), loop, c, op.fds, result });
                    }
                }.wrap,
            };

            self.enqueue(completion);
        }

        // callback used for an empty Completion.
        const emptyCallback = struct {
            fn callback(_: *Self, _: *Completion) void {}
        }.callback;

        // Represents operations.
        pub const Completion = struct {
            /// Pointer to CQE used for this completion, filled after a call to `copy_cqes`
            cqe: ?*io_uring_cqe = null,
            /// Intrusively linked
            next: ?*Completion = null,
            /// Operation intended by this completion
            operation: Operation = .none,
            /// Userdata and callback for when the completion is finished
            userdata: ?*anyopaque = null,
            /// NOTE: IDK if null or undefined suit here better
            callback: Callback = emptyCallback,

            // callback to be invoked when an operation is complete.
            pub const Callback = *const fn (_: *Self, _: *Completion) void;

            // operation kind
            pub const OperationType = enum {
                none,
                read,
                write,
                connect,
                accept,
                recv,
                timeout,
                cancel,
                fds_update,
                close,
            };

            // values needed by operations
            // TODO: hide the io_uring specific operation kinds behind a comptime switch, like `fds_update`.
            pub const Operation = union(OperationType) {
                // recv comptime magic
                pub const Recv = switch (options.io_uring.buffer_pool_mode) {
                    // when provided buffers are enabled, we only take `group_id` from caller
                    true => struct {
                        socket: Socket,
                        buf_pool: *BufferPool,
                    },
                    // regular recv operation, similar to read
                    false => struct {
                        socket: Socket,
                        buffer: []u8,
                    },
                };

                // fds_update comptime magic
                pub const FdsUpdate = switch (options.io_uring.direct_descriptors_mode) {
                    true => struct {
                        fds: []linux.fd_t,
                        offset: u32,
                    },
                    false => void,
                };

                // various kinds of cancel operations are supported
                // https://man7.org/linux/man-pages/man3/io_uring_prep_cancel.3.html
                pub const CancelType = enum(u1) { all_io, completion };

                // TODO: support for timeout cancels
                pub const Cancel = union(CancelType) {
                    all_io: Handle,
                    completion: *Completion,
                };

                // default
                none: void,
                read: struct {
                    handle: linux.fd_t,
                    buffer: []u8,
                    offset: u64,
                },
                write: struct {
                    handle: linux.fd_t,
                    buffer: []const u8,
                    offset: u64,
                },
                connect: struct {
                    socket: linux.fd_t,
                    addr: std.net.Address,
                },
                accept: struct {
                    socket: linux.fd_t, // listener socket
                    addr: posix.sockaddr = undefined,
                    addr_size: posix.socklen_t = @sizeOf(posix.sockaddr),
                },
                recv: Recv,
                timeout: linux.kernel_timespec,
                cancel: Cancel,
                fds_update: FdsUpdate,
                close: linux.fd_t,
            };
        };
    };
}

// on unix, there's a unified fd type that's used for both sockets and file, pipe etc. handles.
// since this library is intended for cross platform usage and windows separates file descriptors by different kinds, I've decided to adapt the same mindset to here.
pub const Handle = linux.fd_t;
pub const Socket = linux.fd_t;
