/// Which backend to use for I/O related tasks.
pub const IoEngine = enum {
    /// Modern asynchronous I/O API of Linux.
    io_uring,
    /// Completion-based Overlapped I/O API of Windows.
    iocp,
};

/// Options shared across io models.
pub const Options = struct {
    /// TODO: zero-copy send
    /// TODO: investigate SQPOLL
    /// TODO: investigate `io_uring_prep_fixed_fd_install`.
    /// TODO: investigate `io_uring_register_ring_fd`.
    /// io_uring specific options.
    io_uring: struct {
        /// On by default.
        ///
        /// Indicates the ring that only a single thread will make submissions.
        ///
        /// It's not recommended to share a ring between threads.
        single_issuer: bool = true,

        /// On by Default.
        ///
        /// Cooperative task running. When requests complete, they often require
        /// forcing the submitter to transition to the kernel to complete. If this
        /// flag is set, work will be done when the task transitions anyway, rather
        /// than force an inter-processor interrupt reschedule. This avoids interrupting
        /// a task running in userspace, and saves an Inter-processor interrupt.
        cooperative_task_running: bool = true,

        /// Whether or not direct descriptors of io_uring be used.
        ///
        /// Direct descriptors exist only within the ring itself, but can be used for any request within that ring.
        /// https://kernel.dk/axboe-kr2022.pdf
        ///
        /// Beware that code that set this flag are likely to be non-portable. Since it activates Linux specific APIs.
        ///
        /// If this option is activated, file descriptors can only be used after they've been registered to loop via:
        /// `initDescriptors`, `updateDescriptors` or `updateFds`.
        ///
        /// TODO: Support `io_uring_prep_socket`, `io_uring_prep_open` etc. functions for direct descriptors.
        direct_descriptors_mode: bool = false,
    } = .{},
    // TODO:
    iocp: struct {} = .{},
};
