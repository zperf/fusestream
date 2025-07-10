package cmd

func tryUmask() {
	syscall.Umask(0)
}
