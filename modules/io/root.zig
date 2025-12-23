const std = @import("std");
const builtin = @import("builtin");

/// Default loop for the given arch.
pub const Loop = switch (builtin.os.tag) {
    .linux => @import("io_uring.zig").IoUring,
    .macos => @import("Kqueue.zig"),
    inline else => @panic("OS not supported"),
};

pub const Handle = Loop.Handle;
pub const Completion = Loop.Completion;

pub const Queue = @import("queue.zig").Intrusive;

const testing = std.testing;
test {
    testing.refAllDecls(Loop);
}
