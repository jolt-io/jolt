const std = @import("std");

// Although this function looks imperative, note that its job is to
// declaratively construct a build graph that will be executed by an external
// runner.
pub fn build(b: *std.Build) void {
    // Standard target options allows the person running `zig build` to choose
    // what target to build for. Here we do not override the defaults, which
    // means any target is allowed, and the default is native. Other options
    // for restricting supported target set are available.
    const target = b.standardTargetOptions(.{});

    // Standard optimization options allow the person running `zig build` to select
    // between Debug, ReleaseSafe, ReleaseFast, and ReleaseSmall. Here we do not
    // set a preferred release mode, allowing the user to decide how to optimize.
    const optimize = b.standardOptimizeOption(.{});

    const io_module = b.addModule("jolt/io", .{
        .root_source_file = b.path("modules/io/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    const http_module = b.addModule("jolt/http", .{
        .root_source_file = b.path("modules/http/root.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "jolt/io", .module = io_module },
        },
    });

    const executable = b.addExecutable(.{
        .name = "jolt-test-executable",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "jolt/io", .module = io_module },
                .{ .name = "jolt/http", .module = http_module },
            },
        }),
    });

    const run_artifact = b.addRunArtifact(executable);
    const run_step = b.step("run", "run executable");
    run_step.dependOn(&run_artifact.step);
}
