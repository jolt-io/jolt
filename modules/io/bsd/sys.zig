const std = @import("std");
const os = std.os;
const posix = std.posix;
const system = posix.system;
const darwin = system.darwin;

// This seems like what we need on MacOS, investigate for others.
pub const Kevent = system.kevent64_s;
pub fn kevent(
    kq: i32,
    changelist: []const Kevent,
    eventlist: []Kevent,
    timeout: ?*const posix.timespec,
) posix.KEventError!u31 {
    while (true) {
        const rc = posix.system.kevent64(
            kq,
            changelist.ptr,
            @intCast(changelist.len & std.math.maxInt(u31)),
            eventlist.ptr,
            @intCast(eventlist.len & std.math.maxInt(u31)),
            0,
            timeout,
        );
        switch (posix.errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .ACCES => return error.AccessDenied,
            .FAULT => unreachable,
            .BADF => unreachable, // Always a race condition.
            .INTR => continue,
            .INVAL => unreachable,
            .NOENT => return error.EventNotFound,
            .NOMEM => return error.SystemResources,
            .SRCH => return error.ProcessNotFound,
            else => unreachable,
        }
    }

    unreachable;
}

pub fn getsockoptError(sockfd: posix.fd_t) posix.E {
    var err_code: i32 = undefined;
    var size: u32 = @sizeOf(u32);
    const rc = system.getsockopt(sockfd, posix.SOL.SOCKET, posix.SO.ERROR, @ptrCast(&err_code), &size);
    std.debug.assert(size == 4);
    // If rc is 0 (.SUCCESS), `err_code` indicates an error.
    return switch (posix.errno(rc)) {
        .SUCCESS => @enumFromInt(err_code),
        else => unreachable,
    };
}

pub fn sendto(
    /// The file descriptor of the sending socket.
    sockfd: posix.socket_t,
    /// Message to send.
    buf: []const u8,
    flags: u32,
    dest_addr: ?*const posix.sockaddr,
    addrlen: posix.socklen_t,
) isize {
    return system.sendto(sockfd, buf.ptr, buf.len, flags, dest_addr, addrlen);
}
