const std = @import("std");
const mem = std.mem;
const linux = std.os.linux;
const io_uring_buf_ring = linux.io_uring_buf_ring;
const IO_Uring = linux.IoUring;
const IoEngine = @import("io/options.zig").IoEngine;

// Note to myself:
// a buffer pool implementation must satisfy these public functions:
// * init
// * deinit
// * put
// * get
//
// Keep these in mind when starting to iocp impl.

/// Buffer pool implementation for ring mapped provided buffers of io_uring.
/// Implementation for io_uring variant is based on what stdlib also offer.
pub fn BufferPool(comptime io_engine: IoEngine) type {
    return switch (io_engine) {
        .io_uring => struct {
            const Self = @This();
            /// Pointer to the memory shared by the kernel.
            /// `buffer_count` of `io_uring_buf` structures are shared by the kernel.
            /// First `io_uring_buf` is overlaid by `io_uring_buf_ring` struct.
            buffer_ring: *align(mem.page_size) io_uring_buf_ring,
            /// Contiguous block of memory of size (buffer_size * buffer_count).
            pool: [*]u8,
            /// Size of each buffer in buffers.
            buffer_size: u32,
            /// Number of buffers in `buffers`, number of `io_uring_buf structures` in buffer ring.
            buffer_count: u16,
            /// ID of this group, must be unique in ring.
            group_id: u16,

            /// Initializes a new buffer pool.
            ///
            /// This buffer pool is designed only to be used within a loop.
            /// Using outside of it requires careful programming.
            pub fn init(
                allocator: mem.Allocator,
                /// `anytype` is preferred since `Loop` is comptime-known type.
                /// Also it eliminates the dependency loop with `io/io_uring.zig`.
                loop: anytype,
                group_id: u16,
                buffer_size: u32,
                buffer_count: u16,
            ) !Self {
                // heap-allocated memory of this pool
                const pool = try allocator.alloc(u8, buffer_size * buffer_count);
                errdefer allocator.free(pool);

                // create a new buffer ring for the loop
                const buffer_ring = try IO_Uring.setup_buf_ring(loop.ring.fd, buffer_count, group_id);
                buffer_ring.tail = 0;

                const mask = IO_Uring.buf_ring_mask(buffer_count);

                var i: u16 = 0;
                while (i < buffer_count) : (i += 1) {
                    const start = buffer_size * i;
                    const buf = pool[start .. start + buffer_size];

                    IO_Uring.buf_ring_add(buffer_ring, buf, i, mask, i);
                }

                // advance as much as we've created
                IO_Uring.buf_ring_advance(buffer_ring, buffer_count);

                return .{
                    .buffer_ring = buffer_ring,
                    .pool = pool.ptr,
                    .buffer_size = buffer_size,
                    .buffer_count = buffer_count,
                    .group_id = group_id,
                };
            }

            /// Frees up resources needed by buffer pool.
            pub fn deinit(
                self: *Self,
                allocator: mem.Allocator,
                /// See init function for anytype here.
                loop: anytype,
            ) void {
                IO_Uring.free_buf_ring(loop.ring.fd, self.buffer_ring, self.buffer_count, self.group_id);
                allocator.free(self.pool[0 .. self.buffer_size * self.buffer_count]);
            }

            /// Get a buffer by id.
            pub inline fn get(self: *Self, buffer_id: u16) []u8 {
                const head = self.buffer_size * buffer_id;
                return (self.pool + head)[0..self.buffer_size];
            }

            /// Release a buffer back to the kernel.
            pub fn put(self: *Self, buffer_id: u16) void {
                const mask = IO_Uring.buf_ring_mask(self.buffer_count);
                const buffer = self.get(buffer_id);

                IO_Uring.buf_ring_add(self.buffer_ring, buffer, buffer_id, mask, 0);
                IO_Uring.buf_ring_advance(self.buffer_ring, 1);
            }
        },
        // TODO: IOCP implementation
        .iocp => @compileError("BufferPool is not implemented for IOCP yet"),
    };
}
