//go:build !unix

package rustsparklocal

import "os/exec"

func configureCLIProcess(_ *exec.Cmd) {}
func killCLIProcessGroup(_ *exec.Cmd) {}
