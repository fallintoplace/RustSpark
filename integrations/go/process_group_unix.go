//go:build unix

package rustsparklocal

import (
    "errors"
    "os"
    "os/exec"
    "syscall"
)

// Run the CLI in its own process group so cancellation also reaches
// subprocesses that inherited output pipes.
func configureCLIProcess(cmd *exec.Cmd) {
    cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
    cmd.Cancel = func() error {
        if cmd.Process == nil { return os.ErrProcessDone }
        if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil {
            if errors.Is(err, syscall.ESRCH) { return os.ErrProcessDone }
            return err
        }
        return nil
    }
}

func killCLIProcessGroup(cmd *exec.Cmd) {
    if cmd.Process != nil {
        _ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
    }
}
