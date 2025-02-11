const std = @import("std");
const builtin = @import("builtin");
const io_uring = @import("io/io_uring.zig");
const options = @import("io/options.zig");
const IoEngine = options.IoEngine;
const Options = options.Options;
const buffer_pool = @import("buffer_pool.zig");

/// Default loop for the given arch.
pub const Loop = switch (builtin.os.tag) {
    .linux => io_uring.Loop,
    inline else => @panic("OS not supported"),
};

/// Pick a loop implementation by I/O engine.
pub fn LoopByIoEngine(comptime io_engine: IoEngine, comptime optns: Options) type {
    return switch (io_engine) {
        .io_uring => io_uring.Loop(optns),
        inline else => @panic("not implemented yet"),
    };
}

/// Buffer pool.
pub const BufferPool = switch (builtin.os.tag) {
    .linux => buffer_pool.BufferPool(.io_uring),
    inline else => @panic("OS not supported"),
};

test {
    _ = switch (builtin.os.tag) {
        .linux => io_uring,
        inline else => @panic("OS not supported"),
    };
}
