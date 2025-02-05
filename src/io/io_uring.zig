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
const ex = @import("linux/io_uring_ex.zig");

/// Event notifier implementation based on io_uring.
/// FIXME: Add error types
pub fn Loop(comptime options: Options) type {
    return struct {
        const Self = @This();
        /// io_uring instance
        ring: IO_Uring,
        /// count of CQEs that we're waiting to receive, includes;
        /// * I/O operations that're submitted successfully (not in unqueued),
        /// * I/O operations that create multiple CQEs (multishot operations, zero-copy send etc.).
        io_pending: u64 = 0,
        /// I/O operations that're not queued yet
        unqueued: Intrusive(Completion) = .{},

        /// TODO: Check io_uring capabilities of kernel.
        /// Initializes a new event loop backed by io_uring.
        pub fn init() !Self {
            var flags: u32 = 0;

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

        /// Deinitializes the event loop.
        pub fn deinit(self: *Self) void {
            self.ring.deinit();
        }

        /// NOTE: experimental
        pub inline fn hasIo(self: *const Self) bool {
            return self.io_pending > 0 and self.unqueued.isEmpty();
        }

        /// Runs the event loop until all operations are completed.
        pub fn run(self: *Self) !void {
            var cqes: [512]io_uring_cqe = undefined;

            while (self.hasIo()) {
                _ = self.ring.submit_and_wait(1) catch |err| switch (err) {
                    // interrupted, try again
                    error.SignalInterrupt => continue,
                    // we'll flush completions right after anyway
                    error.CompletionQueueOvercommitted => 0,
                    else => return err,
                };

                try self.flush(&cqes);
            }
        }

        /// This function does 3 things in a single call;
        /// * Flushes completions,
        /// * Retries submitting unqueued completions,
        /// * Runs the callbacks of completed completions.
        fn flush(self: *Self, slice: []io_uring_cqe) !void {
            // start by flushing completions that're queued
            const completed = try self.flushCompletions(slice);

            // Retry queueing completions that're unable to be queued before
            {
                var copy = self.unqueued;
                self.unqueued = .{};
                while (copy.pop()) |c| self.enqueue(c);
            }

            // run completions
            for (slice[0..completed]) |*cqe| {
                const c: *Completion = @ptrFromInt(cqe.user_data);
                // FIXME: I'm not sure passing a pointer here is okay
                c.cqe = cqe;
                // Execute the completion
                c.callback(self, c);
            }
        }

        fn flushCompletions(self: *Self, slice: []io_uring_cqe) !u32 {
            while (true) {
                const completed = self.ring.copy_cqes(slice, 1) catch |err| switch (err) {
                    error.SignalInterrupt => continue,
                    else => return err,
                };

                // Decrement as much as completed events.
                // Currently, `io_pending` is only decremented here.
                self.io_pending -= completed;

                return completed;
            }

            unreachable;
        }

        /// Registers a completion to the loop.
        fn enqueue(self: *Self, c: *Completion) void {
            // get a submission queue entry
            const sqe = self.ring.get_sqe() catch |err| switch (err) {
                //if SQE ring is full, the completion will be added to unqueued.
                error.SubmissionQueueFull => return self.unqueued.push(c),
            };

            // prepare the completion.
            switch (c.operation) {
                .none => unreachable,
                .read => |op| {
                    sqe.prep_rw(.READ, op.handle, @intFromPtr(op.buffer.ptr), op.buffer.len, op.offset);

                    // we want to use direct descriptors
                    if (comptime options.io_uring.direct_descriptors_mode) {
                        sqe.flags |= linux.IOSQE_FIXED_FILE;
                    }
                },
                .write => |op| {
                    sqe.prep_rw(.WRITE, op.handle, @intFromPtr(op.buffer.ptr), op.buffer.len, op.offset);

                    // we want to use direct descriptors
                    if (comptime options.io_uring.direct_descriptors_mode) {
                        sqe.flags |= linux.IOSQE_FIXED_FILE;
                    }
                },
                .connect => |*op| {
                    sqe.prep_connect(op.socket, &op.addr.any, op.addr.getOsSockLen());

                    // we want to use direct descriptors
                    if (comptime options.io_uring.direct_descriptors_mode) {
                        sqe.flags |= linux.IOSQE_FIXED_FILE;
                    }
                },
                .accept => |*op| switch (comptime options.io_uring.direct_descriptors_mode) {
                    true => {
                        // NOTE: Direct descriptors don't support SOCK_CLOEXEC flag at the moment.
                        // https://github.com/axboe/liburing/issues/1330
                        sqe.prep_multishot_accept_direct(op.socket, &op.addr, &op.addr_size, 0);
                        // op.socket is also a direct descriptor
                        sqe.flags |= linux.IOSQE_FIXED_FILE;
                    },
                    // regular multishot accept
                    false => sqe.prep_multishot_accept(op.socket, &op.addr, &op.addr_size, posix.SOCK.CLOEXEC),
                },
                .recv => |op| {
                    sqe.prep_rw(.RECV, op.socket, @intFromPtr(op.buffer.ptr), op.buffer.len, 0);

                    // we want to use direct descriptors
                    if (comptime options.io_uring.direct_descriptors_mode) {
                        sqe.flags |= linux.IOSQE_FIXED_FILE;
                    }
                },
                // recv buffer pool variant
                .recv_bp => |op| {
                    // recv multishot requires everything to be zero since the buffer will be selected automatically
                    sqe.prep_rw(.RECV, op.socket, 0, 0, 0);
                    // which buffer group to use, in liburing, field set is `buf_group` but it's actually defined as a union
                    sqe.buf_index = op.buf_pool.group_id;
                    // indicate that we've selected a buffer group
                    sqe.flags |= linux.IOSQE_BUFFER_SELECT;
                    // NOTE: Following flag only works on provided buffers.
                    // https://github.com/axboe/liburing/issues/1331#issuecomment-2599784984
                    sqe.ioprio |= linux.IORING_RECV_MULTISHOT;

                    // we want to use direct descriptors
                    if (comptime options.io_uring.direct_descriptors_mode) {
                        sqe.flags |= linux.IOSQE_FIXED_FILE;
                    }
                },
                .send => |op| {
                    sqe.prep_rw(.SEND, op.socket, @intFromPtr(op.buffer.ptr), op.buffer.len, 0);
                    // send flags
                    sqe.rw_flags = op.flags;
                    // FIXME: experimental SQE flags
                    sqe.flags |= @intCast(c.flags & 0xFF);

                    // we want to use direct descriptors
                    if (comptime options.io_uring.direct_descriptors_mode) {
                        sqe.flags |= linux.IOSQE_FIXED_FILE;
                    }
                },
                // FIXME: IORING_TIMEOUT_ETIME_SUCCESS seems to have no effect here
                .timeout => |*ts| sqe.prep_timeout(ts, 0, linux.IORING_TIMEOUT_ETIME_SUCCESS),
                .cancel => |op| switch (op) {
                    .all_io => |fd| ex.io_uring_prep_cancel_fd(sqe, fd, linux.IORING_ASYNC_CANCEL_ALL),
                    .completion => |comp| {
                        if (comp.operation == .timeout) {
                            // prepare a timeout removal instead
                            sqe.prep_timeout_remove(@intCast(@intFromPtr(comp)), 0);
                        } else {
                            sqe.prep_cancel(@intCast(@intFromPtr(comp)), 0);
                        }
                    },
                },
                .fds_update => |op| switch (comptime options.io_uring.direct_descriptors_mode) {
                    true => ex.io_uring_prep_files_update(sqe, op.fds, op.offset),
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

        pub const ReadError = anyerror;

        /// Queues a read operation.
        pub fn read(
            self: *Self,
            completion: *Completion,
            comptime T: type,
            userdata: *T,
            handle: Handle,
            buffer: []u8,
            comptime callback: *const fn (
                userdata: *T,
                loop: *Self,
                completion: *Completion,
                buffer: []u8,
                result: ReadError!u31,
            ) void,
        ) void {
            completion.* = .{
                .next = null,
                .operation = .{
                    .read = .{
                        .handle = handle,
                        .buffer = buffer,
                        // offset is a u64 but if the value is -1 then it uses the offset in the fd.
                        .offset = @bitCast(@as(i64, -1)),
                    },
                },
                .userdata = userdata,
                // following is what happens when we receive a sqe for this completion.
                .callback = comptime struct {
                    fn wrap(loop: *Self, c: *Completion) void {
                        const cqe = c.cqe.?;
                        const res = cqe.res;

                        const result: ReadError!u31 = if (res <= 0)
                            switch (@as(posix.E, @enumFromInt(-res))) {
                                .SUCCESS => error.EndOfStream,
                                .INTR, .AGAIN => {
                                    // Some file systems, like XFS, can return EAGAIN even when
                                    // reading from a blocking file without flags like RWF_NOWAIT.
                                    return loop.enqueue(c);
                                },
                                .BADF => error.NotOpenForReading,
                                .CONNRESET => error.ConnectionResetByPeer,
                                .FAULT => unreachable,
                                .INVAL => error.Alignment,
                                .IO => error.InputOutput,
                                .ISDIR => error.IsDir,
                                .NOBUFS => error.SystemResources,
                                .NOMEM => error.SystemResources,
                                .NXIO => error.Unseekable,
                                .OVERFLOW => error.Unseekable,
                                .SPIPE => error.Unseekable,
                                .TIMEDOUT => error.ConnectionTimedOut,
                                else => |err| posix.unexpectedErrno(err),
                            }
                        else
                            @intCast(res);

                        const buf = c.operation.read.buffer;

                        // invoke the user provided callback
                        @call(.always_inline, callback, .{ @as(*T, @ptrCast(@alignCast(c.userdata))), loop, c, buf, result });
                    }
                }.wrap,
            };

            self.enqueue(completion);
        }

        /// Queues a write operation.
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

        /// Queues a connect operation.
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

        /// Starts accepting connections for a given socket.
        /// This a multishot operation, meaning it'll keep on running until it's either cancelled or encountered with an error.
        /// Completions given to multishot operations MUST NOT be reused until the multishot operation is either cancelled or encountered with an error.
        pub fn accept(
            self: *Self,
            completion: *Completion,
            comptime T: type,
            userdata: *T,
            socket: Socket,
            comptime callback: *const fn (
                userdata: *T,
                loop: *Self,
                completion: *Completion,
                result: Completion.OperationType.returnType(.accept),
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

                        const result: Completion.OperationType.returnType(.accept) = if (res < 0) switch (@as(posix.E, @enumFromInt(-res))) {
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

        pub const RecvError = error{
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

        /// Queues a recv operation.
        pub fn recv(
            self: *Self,
            completion: *Completion,
            comptime T: type,
            userdata: *T,
            socket: Socket,
            buffer: []u8,
            comptime callback: *const fn (
                userdata: *T,
                loop: *Self,
                completion: *Completion,
                socket: Socket,
                buffer: []u8,
            ) void,
        ) void {
            completion.* = .{
                .next = null,
                .operation = .{
                    .recv = .{
                        .socket = socket,
                        .buffer = buffer,
                    },
                },
                .userdata = userdata,
                .callback = struct {
                    fn wrap(loop: *Self, c: *Completion) void {
                        const op = c.operation.recv;

                        // regular buffer version
                        @call(.always_inline, callback, .{ @as(*T, @ptrCast(@alignCast(c.userdata))), loop, c, op.socket, op.buffer });
                    }
                }.wrap,
            };

            self.enqueue(completion);
        }

        pub const RecvBufferPoolError = RecvError || error{OutOfBuffers};

        /// NOTE: Experimental.
        ///
        /// Queues a recv operation, this variant uses a buffer pool instead of a single buffer.
        ///
        /// io_uring supports a mechanism called provided buffers. The application sets
        /// aside a pool of buffers, and informs io_uring of those buffers. This allows the kernel to
        /// pick a suitable buffer when the given receive operation is ready to actually receive data,
        /// rather than upfront. The CQE posted will then additionally hold information about
        /// which buffer was picked.
        /// https://kernel.dk/io_uring%20and%20networking%20in%202023.pdf
        pub fn recvBufferPool(
            self: *Self,
            completion: *Completion,
            comptime T: type,
            userdata: *T,
            socket: Socket,
            buffer_pool: *BufferPool,
            comptime callback: *const fn (
                userdata: *T,
                loop: *Self,
                completion: *Completion,
                socket: Socket,
                buffer_pool: *BufferPool,
                buffer_id: u16,
            ) void,
        ) void {
            completion.* = .{
                .next = null,
                .operation = .{
                    .recv_bp = .{
                        .socket = socket,
                        .buffer_pool = buffer_pool,
                    },
                },
                .userdata = userdata,
                .callback = struct {
                    fn wrap(loop: *Self, c: *Completion) void {
                        const cqe = c.cqe.?;
                        const op = c.operation.recv;

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

                        // NOTE: Currently its up to caller to give the buffer back to kernel.
                        // This behaviour might change in the future though.
                        @call(.always_inline, callback, .{ @as(*T, @ptrCast(@alignCast(c.userdata))), loop, c, op.socket, op.buf_pool, buffer_id });
                    }
                }.wrap,
            };

            self.enqueue(completion);
        }

        pub const SendError = error{
            AccessDenied,
            WouldBlock,
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
        } || CancellationError || UnexpectedError;

        pub const Link = enum(u1) { linked, unlinked };

        /// Queues a send operation.
        pub fn send(
            self: *Self,
            // TODO: experimental linking
            comptime link: Link,
            completion: *Completion,
            comptime T: type,
            userdata: *T,
            socket: Socket,
            buffer: []const u8,
            comptime callback: *const fn (
                userdata: *T,
                loop: *Self,
                completion: *Completion,
                buffer: []const u8,
                /// u31 is preferred for coercion
                result: SendError!u31,
            ) void,
        ) void {
            completion.* = .{
                .next = null,
                // TODO: experimental linking
                .flags = if (comptime link == .linked) linux.IOSQE_IO_LINK else 0,
                .operation = .{
                    .send = .{
                        .socket = socket,
                        .buffer = buffer,
                        .flags = if (comptime link == .linked) linux.MSG.WAITALL else 0,
                    },
                },
                .userdata = userdata,
                .callback = comptime struct {
                    fn wrap(loop: *Self, c: *Completion) void {
                        const cqe = c.cqe.?;
                        const res = cqe.res;

                        const result: SendError!u31 = if (res < 0)
                            switch (@as(posix.E, @enumFromInt(-res))) {
                                .INTR => return loop.enqueue(c),
                                .ACCES => error.AccessDenied,
                                .AGAIN => error.WouldBlock,
                                .ALREADY => error.FastOpenAlreadyInProgress,
                                .AFNOSUPPORT => error.AddressFamilyNotSupported,
                                .BADF => error.FileDescriptorInvalid,
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
                                .CANCELED => error.Cancelled,
                                else => |err| posix.unexpectedErrno(err),
                            }
                        else
                            @intCast(res); // valid length

                        const buf = c.operation.send.buffer;
                        @call(.always_inline, callback, .{ @as(*T, @ptrCast(@alignCast(c.userdata))), loop, c, buf, result });
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

        /// FIXME: refactor
        /// Queues a cancel operation.
        /// If a handle is given as target, all I/O operations of the handle are cancelled.
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

                        // According to Zen, close operations should not fail.
                        // So we do not return any errors here.
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
            if (comptime !options.io_uring.direct_descriptors_mode) {
                @compileError("direct descriptors are not enabled");
            }

            return switch (registration) {
                .regular => self.ring.register_files(array_or_u32),
                .sparse => ex.io_uring_register_files_sparse(&self.ring, array_or_u32),
            };
        }

        /// TODO: We might want to support resource tagging too, needs further investigations.
        /// io_uring only.
        ///
        /// Updates file descriptors. Starting from the `offset`.
        /// This function works synchronously, which might suit better for some situations.
        /// Check `updateDescriptorsAsync` function if you need asynchronous update.
        pub fn updateDescriptorsSync(self: *Self, offset: u32, fds: []const linux.fd_t) !void {
            if (comptime !options.io_uring.direct_descriptors_mode) {
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
        pub fn updateDescriptorsAsync(
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
            if (comptime !options.io_uring.direct_descriptors_mode) {
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
        /// Same as `updateDescriptorsAsync` but this variant doesn't take `offset` argument. Instead, it allocates fds
        /// to empty spots and fills the given `fds` slice with direct descriptors. Order is preserved.
        pub fn updateDescriptorsAsyncAlloc(
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
            if (comptime !options.io_uring.direct_descriptors_mode) {
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
            /// NOTE: experimental SQE flags
            flags: u32 = 0,
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
                recv_bp,
                send,
                timeout,
                cancel,
                fds_update,
                close,

                /// Returns the return type for the given operation type.
                /// Can be run on comptime, but not necessarily.
                pub fn returnType(op: OperationType) type {
                    return switch (op) {
                        .none => unreachable,
                        .read => ReadError!usize, // TODO: implement ReadError
                        .write => anyerror!usize, // TODO: implement WriteError
                        .connect => anyerror!void, // TODO: implement ConnectError
                        .accept => AcceptError!Socket,
                        .recv => RecvError!usize,
                        .recv_bp => RecvBufferPoolError!usize,
                        .send => SendError!usize,
                        .timeout => TimeoutError!void,
                        .fds_update => UpdateFdsError!i32,
                        .close => void,
                        inline else => unreachable,
                    };
                }
            };

            // values needed by operations
            // TODO: hide the io_uring specific operation kinds behind a comptime switch, like `fds_update`.
            pub const Operation = union(OperationType) {
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
                // TODO: take `*posix.sockaddr` instead
                accept: struct {
                    socket: linux.fd_t, // listener socket
                    addr: posix.sockaddr = undefined,
                    addr_size: posix.socklen_t = @sizeOf(posix.sockaddr),
                },
                recv: struct {
                    socket: Socket,
                    buffer: []u8,
                    //flags: u32,
                },
                recv_bp: struct {
                    socket: Socket,
                    buf_pool: *BufferPool,
                },
                send: struct {
                    socket: linux.fd_t,
                    buffer: []const u8,
                    flags: u32,
                },
                timeout: linux.kernel_timespec,
                cancel: Cancel,
                fds_update: FdsUpdate,
                close: linux.fd_t,
            };

            // Returns the result of this completion.
            pub fn getResult(completion: *Completion, comptime op_type: OperationType) op_type.returnType() {
                const cqe = completion.cqe.?;
                const res = cqe.res;

                switch (comptime op_type) {
                    .none => unreachable,
                    .read => unreachable,
                    .write => unreachable,
                    .connect => unreachable,
                    .accept => {},
                    .send => {
                        if (res < 0) {
                            return switch (@as(posix.E, @enumFromInt(-res))) {
                                .INTR => unreachable, // TODO: this used to enqueue the completion again
                                .ACCES => error.AccessDenied,
                                .AGAIN => error.WouldBlock,
                                .ALREADY => error.FastOpenAlreadyInProgress,
                                .AFNOSUPPORT => error.AddressFamilyNotSupported,
                                .BADF => error.FileDescriptorInvalid,
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
                                .CANCELED => error.Cancelled,
                                else => |err| posix.unexpectedErrno(err),
                            };
                        } else {
                            // valid length
                            return @intCast(res);
                        }
                    },
                    .recv => {
                        // are you friend or foe
                        if (res <= 0) {
                            return switch (@as(posix.E, @enumFromInt(-res))) {
                                .SUCCESS => error.EndOfStream, // 0 reads are interpreted as errors
                                //.INTR => return loop.enqueue(c), // we're interrupted, try again
                                .INTR => unreachable,
                                .AGAIN => error.WouldBlock,
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
                                .CANCELED => error.Cancelled,
                                else => error.Unexpected,
                            };
                        } else {
                            // valid length
                            return @intCast(res);
                        }
                    },
                    inline else => unreachable,
                }

                unreachable;
            }
        };

        // on unix, there's a unified fd type that's used for both sockets and file, pipe etc. handles.
        // since this library is intended for cross platform usage and windows separates file descriptors by different kinds, I've decided to adapt the same mindset to here.
        pub const Handle = linux.fd_t;
        pub const Socket = linux.fd_t;
    };
}
