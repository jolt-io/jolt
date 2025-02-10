const std = @import("std");
const builtin = @import("builtin");

test {
    _ = switch (builtin.os.tag) {
        .linux => @import("io/io_uring.zig"),
        inline else => @panic("OS is not supported"),
    };
}
