package bench

import "syscall"

const tmpfsMagic = 0x01021994

// describeFS says what kind of filesystem holds dir, so a result file states how durable its commits were.
func describeFS(dir string) string {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return "unknown filesystem"
	}
	if int64(st.Type) == tmpfsMagic {
		return "tmpfs (RAM disk, fsync is nearly free)"
	}
	return "container disk (fsync goes to Docker Desktop's virtual disk)"
}
