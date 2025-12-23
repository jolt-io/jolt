const std = @import("std");
const assert = std.debug.assert;

/// An intrusive queue implementation. The type T must have a field
/// "next" of type `?*T`.
///
/// For those unaware, an intrusive variant of a data structure is one in which
/// the data type in the list has the pointer to the next element, rather
/// than a higher level "node" or "container" type. The primary benefit
/// of this (and the reason we implement this) is that it defers all memory
/// management to the caller: the data structure implementation doesn't need
/// to allocate "nodes" to contain each element. Instead, the caller provides
/// the element and how its allocated is up to them.
pub fn Intrusive(comptime T: type) type {
    // check if `next` field exists
    comptime {
        if (@FieldType(T, "next") != ?*T) {
            @compileError("`next` field must be type of `?*T`");
        }
    }

    return struct {
        const Self = @This();

        /// Head is the front of the queue and tail is the back of the queue.
        head: ?*T = null,
        tail: ?*T = null,

        /// Enqueue a new element to the back of the queue.
        pub fn push(self: *Self, v: *T) void {
            if (self.tail) |tail| {
                // If we have elements in the queue, then we add a new tail.
                tail.next = v;
                self.tail = v;
            } else {
                // No elements in the queue we setup the initial state.
                self.head = v;
                self.tail = v;
            }
        }

        // Enqueue a new element to the front of the queue.
        // WARNING: This operation might reset the `next` of the given value `v`.
        pub fn unshift(self: *Self, v: *T) void {
            if (self.head) |head| {
                v.next = head;
                self.head = v;
            } else {
                // NOTE: we assume the next field is set to null, otherwise it can lead to bugs.
                v.next = null;
                self.head = v;
                self.tail = v;
            }
        }

        /// Dequeue the next element from the queue.
        pub fn pop(self: *Self) ?*T {
            // The next element is in "head".
            const next = self.head orelse return null;

            // If the head and tail are equal this is the last element
            // so we also set tail to null so we can now be empty.
            if (self.head == self.tail) self.tail = null;

            // Head is whatever is next (if we're the last element,
            // this will be null);
            self.head = next.next;

            // We set the "next" field to null so that this element
            // can be inserted again.
            next.next = null;
            return next;
        }

        /// Removes the top node. Assumes such node exist.
        pub inline fn removeAssumeHead(self: *Self) void {
            const top_node = self.head orelse unreachable;
            // Last node.
            if (top_node == self.tail) self.tail = null;
            // Update head.
            self.head = top_node.next;
            top_node.next = null;
        }

        /// Return the element on top but don't remove it.
        pub inline fn peek(self: *const Self) ?*T {
            return self.head;
        }

        /// Returns true if the queue is empty.
        pub fn isEmpty(self: *const Self) bool {
            return self.head == null;
        }
    };
}

/// An intrusive thread-unsafe doubly-linked queue of items.
/// `T` must have fields `next` and `prev` with type `?*T`.
pub fn DoublyLinked(comptime T: type) type {
    comptime {
        std.debug.assert(@FieldType(T, "next") == ?*T and @FieldType(T, "prev") == ?*T);
    }

    return extern struct {
        const Queue = @This();
        head: ?*T = null,
        tail: ?*T = null,

        /// Returns true if queue is empty.
        pub inline fn isEmpty(queue: *const Queue) bool {
            return queue.head == null;
        }

        /// Adds an element to the end of the queue.
        pub fn push(queue: *Queue, item: *T) void {
            if (queue.tail) |tail| {
                tail.next = item;
                item.prev = tail;
                // Reset item.
                item.next = null;
                queue.tail = item;
            } else {
                // Reset item.
                item.prev = null;
                item.next = null;
                // If queue don't have a tail, this is the first element.
                queue.head = item;
                queue.tail = item;
            }
        }

        /// Removes an element from the queue.
        pub fn remove(queue: *Queue, item: *T) void {
            if (item.prev) |prev_item| {
                prev_item.next = item.next;
            } else {
                // Item was the head.
                queue.head = item.next;
            }

            if (item.next) |next_item| {
                next_item.prev = item.prev;
            } else {
                // Item was the tail.
                queue.tail = item.prev;
            }

            // Reset removed element.
            item.next = null;
            item.prev = null;
        }

        /// Pops the first element from the queue.
        pub fn popFirst(queue: *Queue) ?*T {
            const head = queue.head orelse return null;

            // This is the only element in the queue.
            if (head == queue.tail.?) {
                queue.head = null;
                queue.tail = null;
            } else {
                const next = head.next.?;
                queue.head = next;
                next.prev = null;
            }

            // Reset popped element.
            head.next = null;
            head.prev = null;
            return head;
        }

        /// Pops the last element from the queue.
        pub fn popLast(queue: *Queue) ?*T {
            const tail = queue.tail orelse return null;

            // This is the only element in the queue.
            if (tail == queue.head.?) {
                queue.head = null;
                queue.tail = null;
            } else {
                const prev = tail.prev.?;
                queue.tail = prev;
                prev.next = null;
            }

            // Reset popped element.
            tail.next = null;
            tail.prev = null;
            return tail;
        }
    };
}
