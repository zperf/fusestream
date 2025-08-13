package cmd

import (
	"syscall"
)

func syscallUmask() {
	syscall.Umask(0)
}
