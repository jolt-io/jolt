const std = @import("std");
const posix = std.posix;

const io = @import("jolt/io");

pub fn main() !void {
    var loop = try io.Loop.init();
    defer loop.deinit();

    var handle = io.Handle{};
    try loop.initSocket(&handle, posix.AF.INET, posix.SOCK.STREAM, posix.IPPROTO.TCP);
    defer posix.close(handle.fd);

    try handle.setReuseAddr(true);
    try handle.bind(.initIp4(.{ 127, 0, 0, 1 }, 8080));
    try handle.listen(128);

    var accept_c = io.Completion{};
    loop.acceptStart(&accept_c, io.Handle, &handle, &handle, onAccept);

    try loop.run(.until_done);
}

fn onAccept(
    _: *io.Handle,
    _: *io.Loop,
    _: *io.Completion,
    _: *io.Handle,
    result: posix.AcceptError!posix.socket_t,
) void {
    std.debug.print("{}\n", .{result catch unreachable});
}
