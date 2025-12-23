//! TODO

/// Which backend to use for I/O related tasks.
pub const IoEngine = enum {
    /// Modern asynchronous I/O API of Linux.
    io_uring,
    /// Completion-based Overlapped I/O API of Windows.
    iocp,
};

/// Options shared across I/O models.
pub const Options = struct {
    /// TODO: zero-copy send
    /// TODO: investigate SQPOLL
    /// TODO: investigate `io_uring_prep_fixed_fd_install`.
    /// TODO: investigate `io_uring_register_ring_fd`.
    /// io_uring specific options.
    io_uring: packed struct {} = .{},
    // TODO:
    iocp: struct {} = .{},
};
