const std = @import("std");
const testing = std.testing;
const builtin = @import("builtin");

/// Default loop for the given arch.
pub const Loop = switch (builtin.os.tag) {
    .linux => @import("io/io_uring.zig").IoUring,
    inline else => @panic("OS not supported"),
};

pub const Queue = @import("queue.zig").Intrusive;

test {
    testing.refAllDecls(Loop);
}
