//go:build !linux

package bench

// describeFS is only meaningful on Linux, where the benchmark runs inside a container.
func describeFS(dir string) string { return "unknown filesystem" }
