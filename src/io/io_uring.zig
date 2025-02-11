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
        io_pending: u32 = 0,
        /// I/O operations that're not queued yet
        unqueued: Intrusive(Completion) = .{},
        /// Completions that're ready for their callbacks to run
        completed: Intrusive(Completion) = .{},

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
                // Flush any queued SQEs and reuse the same syscall to wait for completions if required:
                try self.flushSubmissions(&cqes);
                // We can now just peek for any CQEs without waiting and without another syscall:
                try self.flushCompletions(&cqes, self.io_pending);

                // Retry queueing completions that were unable to be queued before
                {
                    var copy = self.unqueued;
                    self.unqueued = .{};
                    while (copy.pop()) |c| self.enqueue(c);
                }

                // Run completions
                while (self.completed.pop()) |c| {
                    c.callback(self, c);
                }
            }
        }

        fn flushCompletions(self: *Self, slice: []io_uring_cqe, wait_nr: u32) !void {
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
                    const c: *Completion = @ptrFromInt(cqe.user_data);
                    c.cqe = cqe;
                    // We do not run the completion here (instead appending to a linked list) to avoid:
                    // * recursion through `flush_submissions()` and `flush_completions()`,
                    // * unbounded stack usage, and
                    // * confusing stack traces.
                    self.completed.push(c);
                }

                if (completed < slice.len) break;
            }
        }

        fn flushSubmissions(self: *Self, slice: []io_uring_cqe) !void {
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
                    // which buffer group to use, in liburing, the actual field set is `buf_group` but it's defined as a union so
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
                    // check if we're doing zero-copy
                    const op_kind: linux.IORING_OP = blk: {
                        if (comptime options.io_uring.zero_copy_sends) {
                            break :blk .SEND_ZC;
                        } else {
                            break :blk .SEND;
                        }
                    };

                    sqe.prep_rw(op_kind, op.socket, @intFromPtr(op.buffer.ptr), op.buffer.len, 0);
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

        pub const ReadError = error{
            EndOfStream,
            NotOpenForReading,
            ConnectionResetByPeer,
            Alignment,
            InputOutput,
            IsDir,
            SystemResources,
            Unseekable,
            ConnectionTimedOut,
        } || CancellationError || UnexpectedError;

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
                result: ReadError!usize,
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

                        const result: ReadError!usize = if (res <= 0) switch (@as(posix.E, @enumFromInt(-res))) {
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
                            .CANCELED => error.Cancelled,
                            .NOBUFS => error.SystemResources,
                            .NOMEM => error.SystemResources,
                            .NXIO => error.Unseekable,
                            .OVERFLOW => error.Unseekable,
                            .SPIPE => error.Unseekable,
                            .TIMEDOUT => error.ConnectionTimedOut,
                            else => |err| posix.unexpectedErrno(err),
                        } else @intCast(res);

                        const buf = c.operation.read.buffer;
                        // invoke the user provided callback
                        @call(.always_inline, callback, .{ @as(*T, @ptrCast(@alignCast(c.userdata))), loop, c, buf, result });
                    }
                }.wrap,
            };

            self.enqueue(completion);
        }

        pub const WriteError = error{
            WouldBlock,
            NotOpenForWriting,
            NotConnected,
            DiskQuota,
            FileTooBig,
            Alignment,
            InputOutput,
            NoSpaceLeft,
            Unseekable,
            AccessDenied,
            BrokenPipe,
        } || CancellationError || UnexpectedError;

        /// Queues a write operation.
        pub fn write(
            self: *Self,
            completion: *Completion,
            comptime T: type,
            userdata: *T,
            handle: Handle,
            buffer: []const u8,
            comptime callback: *const fn (
                userdata: *T,
                loop: *Self,
                completion: *Completion,
                handle: Handle,
                buffer: []const u8,
                result: WriteError!usize,
            ) void,
        ) void {
            completion.* = .{
                .next = null,
                .operation = .{
                    .write = .{
                        .handle = handle,
                        .buffer = buffer,
                        // offset is a u64 but if the value is -1 then it uses the offset in the fd.
                        .offset = comptime @bitCast(@as(i64, -1)),
                    },
                },
                .userdata = userdata,
                // following is what happens when we receive a sqe for this completion.
                .callback = comptime struct {
                    fn wrap(loop: *Self, c: *Completion) void {
                        const cqe = c.cqe.?;
                        const res = cqe.res;

                        const result: Completion.OperationType.returnType(.write) = if (res <= 0) switch (@as(posix.E, @enumFromInt(-res))) {
                            .SUCCESS => error.EndOfStream, // 0 read
                            .INTR => return loop.enqueue(completion),
                            .AGAIN => error.WouldBlock,
                            .BADF => error.NotOpenForWriting,
                            .CANCELED => error.Cancelled,
                            .DESTADDRREQ => error.NotConnected,
                            .DQUOT => error.DiskQuota,
                            .FAULT => unreachable,
                            .FBIG => error.FileTooBig,
                            .INVAL => error.Alignment,
                            .IO => error.InputOutput,
                            .NOSPC => error.NoSpaceLeft,
                            .NXIO => error.Unseekable,
                            .OVERFLOW => error.Unseekable,
                            .PERM => error.AccessDenied,
                            .PIPE => error.BrokenPipe,
                            .SPIPE => error.Unseekable,
                            else => error.Unexpected,
                        } else @intCast(res);

                        const op = c.operation.write;
                        // invoke the user provided callback
                        @call(.always_inline, callback, .{ @as(*T, @ptrCast(@alignCast(c.userdata))), loop, c, op.handle, op.buffer, result });
                    }
                }.wrap,
            };

            self.enqueue(completion);
        }

        pub const ConnectError = error{
            AccessDenied,
            AddressInUse,
            AddressNotAvailable,
            AddressFamilyNotSupported,
            WouldBlock,
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
        } || CancellationError || UnexpectedError;

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
                socket: Socket,
                addr: std.net.Address,
                result: ConnectError!void,
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
                        const cqe = c.cqe.?;
                        const res = cqe.res;

                        const result: Completion.OperationType.returnType(.connect) = if (res < 0) switch (@as(posix.E, @enumFromInt(-res))) {
                            .INTR => return loop.enqueue(c),
                            .ACCES => error.AccessDenied,
                            .ADDRINUSE => error.AddressInUse,
                            .ADDRNOTAVAIL => error.AddressNotAvailable,
                            .AFNOSUPPORT => error.AddressFamilyNotSupported,
                            .AGAIN, .INPROGRESS => error.WouldBlock,
                            .ALREADY => error.OpenAlreadyInProgress,
                            .BADF => error.FileDescriptorInvalid,
                            .CANCELED => error.Cancelled,
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

                        const op = c.operation.connect;
                        @call(.always_inline, callback, .{ @as(*T, @ptrCast(@alignCast(c.userdata))), loop, c, op.socket, op.addr, result });
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
                socket: Socket,
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

                        const listener = c.operation.accept.socket;
                        // invoke the user provided callback
                        @call(.always_inline, callback, .{ @as(*T, @ptrCast(@alignCast(c.userdata))), loop, c, listener, result });
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
                result: RecvBufferPoolError!usize,
            ) void,
        ) void {
            completion.* = .{
                .next = null,
                .operation = .{
                    .recv_bp = .{
                        .socket = socket,
                        .buf_pool = buffer_pool,
                    },
                },
                .userdata = userdata,
                .callback = struct {
                    fn wrap(loop: *Self, c: *Completion) void {
                        const cqe = c.cqe.?;
                        const res = cqe.res;
                        const op = c.operation.recv_bp;

                        const result: Completion.OperationType.returnType(.recv_bp) = if (res <= 0)
                            switch (@as(posix.E, @enumFromInt(-res))) {
                                .SUCCESS => error.EndOfStream, // 0 reads are interpreted as errors
                                .INTR => return loop.enqueue(c), // we're interrupted, try again
                                .AGAIN => error.WouldBlock,
                                .BADF => error.Unexpected,
                                .CONNREFUSED => error.ConnectionRefused,
                                .FAULT => unreachable,
                                .INVAL => unreachable,
                                .NOBUFS => error.OutOfBuffers,
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
                            @intCast(res);

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
                        @call(.always_inline, callback, .{ @as(*T, @ptrCast(@alignCast(c.userdata))), loop, c, op.socket, op.buf_pool, buffer_id, result });
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
                socket: Socket,
                buffer: []const u8,
            ) void,
        ) void {
            completion.* = .{
                .next = null,
                // TODO: experimental linking
                .flags = if (comptime link == .linked) linux.IOSQE_IO_LINK else 0,
                .operation = .{
                    .send = switch (comptime options.io_uring.zero_copy_sends) {
                        true => .{
                            .socket = socket,
                            .buffer = buffer,
                            .flags = if (comptime link == .linked) linux.MSG.WAITALL else 0,
                            .result = 0,
                        },
                        false => .{
                            .socket = socket,
                            .buffer = buffer,
                            .flags = if (comptime link == .linked) linux.MSG.WAITALL else 0,
                        },
                    },
                },
                .userdata = userdata,
                .callback = comptime struct {
                    fn wrap(loop: *Self, c: *Completion) void {
                        const cqe = c.cqe.?;
                        const res = cqe.res;

                        // zero-copy sends are activated
                        // We likely receive 2 CQEs for this completion
                        if (comptime options.io_uring.zero_copy_sends) {
                            // we got the notification CQE
                            if (cqe.flags & linux.IORING_CQE_F_NOTIF != 0) {
                                // The notification's res field has to be 0
                                assert(res == 0);

                                // execute the user provided callback
                                const op = c.operation.send;
                                @call(.always_inline, callback, .{ @as(*T, @ptrCast(@alignCast(c.userdata))), loop, c, op.socket, op.buffer });

                                // operation done
                                return;
                            }

                            // copy the send result, we won't get a chance to do it again
                            c.operation.send.result = cqe.res;

                            // There will be another CQE if this is true
                            // To wait for it, we can increment the pending count by 1
                            if (cqe.flags & linux.IORING_CQE_F_MORE != 0) {
                                @branchHint(.likely);
                                loop.io_pending += 1;
                            } else {
                                // interestingly, we won't get a notification CQE
                                // execute the user provided callback
                                const op = c.operation.send;
                                @call(.always_inline, callback, .{ @as(*T, @ptrCast(@alignCast(c.userdata))), loop, c, op.socket, op.buffer });
                            }
                        } else {
                            // regular send operations
                            const op = c.operation.send;
                            @call(.always_inline, callback, .{ @as(*T, @ptrCast(@alignCast(c.userdata))), loop, c, op.socket, op.buffer });
                        }
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

        /// Creates a new TCP stream socket.
        pub fn tcpStream(_: Self) !Socket {
            return posix.socket(posix.AF.INET, posix.SOCK.STREAM, posix.IPPROTO.TCP);
        }

        /// Creates a new TCP listener socket, binds it to an address and starts listening.
        pub fn tcpListener(self: Self, addr: std.net.Address, backlog: u31) !Socket {
            const socket = try self.tcpStream();
            try posix.bind(socket, &addr.any, addr.getOsSockLen());
            try posix.listen(socket, backlog);

            return socket;
        }

        /// Sets REUSEADDR flag on a given socket.
        pub fn setReuseAddr(_: Self, socket: Socket) posix.SetSockOptError!void {
            return posix.setsockopt(socket, posix.SOL.SOCKET, posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));
        }

        /// Sets REUSEPORT flag on a given socket if possible.
        pub fn setReusePort(_: Self, socket: Socket) posix.SetSockOptError!void {
            if (@hasDecl(std.posix.SO, "REUSEPORT")) {
                return posix.setsockopt(socket, posix.SOL.SOCKET, posix.SO.REUSEPORT, &std.mem.toBytes(@as(c_int, 1)));
            }
        }

        /// Enables/disables TCP_NODELAY setting on a given socket.
        pub fn setTcpNoDelay(_: Self, socket: Socket, on: bool) posix.SetSockOptError!void {
            return posix.setsockopt(socket, posix.IPPROTO.TCP, posix.TCP.NODELAY, &std.mem.toBytes(@as(c_int, @intFromBool(on))));
        }

        /// Enables/disables SO_KEEPALIVE setting on a given socket.
        pub fn setTcpKeepAlive(_: Self, socket: Socket, on: bool) posix.SetSockOptError!void {
            return posix.setsockopt(socket, posix.SOL.SOCKET, posix.SO.KEEPALIVE, &std.mem.toBytes(@as(c_int, @intFromBool(on))));
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
                //cancel,
                fds_update,
                close,

                /// Returns the return type for the given operation type.
                /// Can be run on comptime, but not necessarily.
                pub fn returnType(op: OperationType) type {
                    return switch (op) {
                        .none => unreachable,
                        .read => ReadError!usize,
                        .write => WriteError!usize,
                        .connect => ConnectError!void,
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
                // send comptime magic
                pub const Send = switch (options.io_uring.zero_copy_sends) {
                    true => struct {
                        socket: linux.fd_t,
                        buffer: []const u8,
                        flags: u32,
                        result: i32, // holds the result of the first received CQE
                    },
                    false => struct {
                        socket: linux.fd_t,
                        buffer: []const u8,
                        flags: u32,
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
                //pub const CancelType = enum(u1) { all_io, completion };

                //pub const Cancel = union(CancelType) {
                //    all_io: Handle,
                //    completion: *Completion,
                //};

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
                    //TODO: flags: u32,
                },
                recv_bp: struct {
                    socket: Socket,
                    buf_pool: *BufferPool,
                },
                send: Send,
                timeout: linux.kernel_timespec,
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
                        // if zero-copy sends are activated, the result is held at `operation`
                        const result = blk: {
                            if (comptime options.io_uring.zero_copy_sends) {
                                break :blk completion.operation.send.result;
                            } else {
                                break :blk res;
                            }
                        };

                        if (result < 0) {
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
                            return @intCast(result);
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

test "io_uring direct descriptors" {
    const DefaultLoop = Loop(.{
        .io_uring = .{
            .direct_descriptors_mode = true,
        },
    });

    const Completion = DefaultLoop.Completion;

    // create a loop
    var loop = try DefaultLoop.init();
    defer loop.deinit();

    // get file descriptor limits
    const rlimit = try posix.getrlimit(.NOFILE);

    // create direct descriptors table
    try loop.directDescriptors(.sparse, @intCast(rlimit.max & std.math.maxInt(u32)));

    const server = try loop.tcpListener(std.net.Address.initIp4(.{ 127, 0, 0, 1 }, 8080), 128);
    try loop.setReuseAddr(server);
    try loop.setTcpNoDelay(server, true);

    var update_descriptors_c = Completion{};
    var fds = [_]linux.fd_t{server};

    loop.updateDescriptorsAsyncAlloc(&update_descriptors_c, Completion, &update_descriptors_c, &fds, struct {
        fn onUpdate(
            _: *Completion,
            _: *DefaultLoop,
            _: *Completion,
            _fds: []linux.fd_t,
            result: DefaultLoop.UpdateFdsError!i32,
        ) void {
            const res = result catch unreachable;
            std.testing.expect(res == 1) catch unreachable;
            std.testing.expect(_fds[@intCast(res & std.math.maxInt(u31) - 1)] == 0) catch unreachable;
        }
    }.onUpdate);

    try loop.run();
}
