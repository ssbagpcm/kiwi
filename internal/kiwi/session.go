package kiwi

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/creack/pty"
	"golang.org/x/term"
)

const sessionBufferLimit = 256 * 1024

func minimalSessionEnv() []string {
	return []string{
		"HOME=/root",
		"USER=root",
		"LOGNAME=root",
		"TERM=xterm-256color",
		"LANG=C.UTF-8",
		"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
	}
}

func shellSessionEnv(shell, hostname string) []string {
	env := append([]string{}, minimalSessionEnv()...)
	env = append(env, "SHELL="+shell)
	if hostname != "" {
		env = append(env, "HOSTNAME="+hostname)
	}
	return env
}

func sessionShellCandidates(configured string) []string {
	shells := []string{}
	if strings.TrimSpace(configured) != "" {
		shells = append(shells, strings.TrimSpace(configured))
	}
	defaults := []string{
		"/bin/bash", "/usr/bin/bash",
		"/bin/zsh", "/usr/bin/zsh",
		"/bin/fish", "/usr/bin/fish",
		"/bin/sh", "/usr/bin/sh",
		"/bin/ash", "/usr/bin/ash",
		"/bin/dash", "/usr/bin/dash",
	}
	for _, candidate := range defaults {
		found := false
		for _, shell := range shells {
			if shell == candidate {
				found = true
				break
			}
		}
		if !found {
			shells = append(shells, candidate)
		}
	}
	return shells
}

type sessionHandshake struct {
	Rows uint16 `json:"rows"`
	Cols uint16 `json:"cols"`
}

type sessionServer struct {
	store      Store
	container  string
	sessionID  string
	listener   net.Listener
	bufferPath string

	mu       sync.Mutex
	client   net.Conn
	ptyFile  *os.File
	shellCmd *exec.Cmd
	shellPID int
	stopping bool
}

func (s Store) AttachInteractiveSession(containerName, sessionID string) error {
	if os.Geteuid() != 0 {
		return fmt.Errorf("attach requires root; run with sudo")
	}
	if sessionID == "" {
		id, err := generateSessionID()
		if err != nil {
			return err
		}
		sessionID = id
	}
	if err := s.ensureSessionDaemon(containerName, sessionID); err != nil {
		return err
	}
	return s.attachSessionSocket(containerName, sessionID)
}

func (s Store) StopSessionProcesses(containerName, sessionID string) error {
	session, err := s.LoadSession(containerName, sessionID)
	if err != nil {
		return err
	}
	if session.ShellPID > 0 {
		return killProcessTree(session.ShellPID)
	}
	return nil
}

func (s Store) DeleteManagedSession(containerName, sessionID string) error {
	session, err := s.LoadSession(containerName, sessionID)
	if err != nil {
		return err
	}
	if session.DaemonPID > 0 && processAlive(session.DaemonPID) {
		_ = syscall.Kill(session.DaemonPID, syscall.SIGTERM)
		waitForExit(session.DaemonPID, 3)
		if processAlive(session.DaemonPID) {
			_ = syscall.Kill(session.DaemonPID, syscall.SIGKILL)
		}
	}
	return s.DeleteSession(containerName, sessionID)
}

func (s Store) DeleteAllManagedSessions(containerName string) error {
	sessions, err := s.ListSessions(containerName)
	if err != nil {
		return err
	}
	for _, session := range sessions {
		if err := s.DeleteManagedSession(containerName, session.ID); err != nil {
			return err
		}
	}
	return nil
}

func (s Store) ensureSessionDaemon(containerName, sessionID string) error {
	containerName = sanitizeName(containerName)
	sessionID = sanitizeName(sessionID)
	if containerName == "" || sessionID == "" {
		return fmt.Errorf("invalid session")
	}
	session, err := s.LoadSession(containerName, sessionID)
	if err == nil && session.DaemonPID > 0 && processAlive(session.DaemonPID) && pathExists(session.SocketPath) {
		return nil
	}
	if err := os.MkdirAll(s.SessionDir(containerName, sessionID), 0755); err != nil {
		return err
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	cmd := exec.Command(executable, "__sessiond", "--name", containerName, "--session", sessionID)
	cmd.Stdin = nil
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = minimalSessionEnv()
	if err := cmd.Start(); err != nil {
		return err
	}
	_ = cmd.Process.Release()
	for attempts := 0; attempts < 100; attempts++ {
		timeSleepMillis(50)
		session, err := s.LoadSession(containerName, sessionID)
		if err == nil && session.DaemonPID > 0 && processAlive(session.DaemonPID) && pathExists(session.SocketPath) {
			return nil
		}
	}
	return fmt.Errorf("session %q did not become ready", sessionID)
}

func (s Store) attachSessionSocket(containerName, sessionID string) error {
	session, err := s.LoadSession(containerName, sessionID)
	if err != nil {
		return err
	}
	conn, err := net.Dial("unix", session.SocketPath)
	if err != nil {
		return err
	}
	defer conn.Close()
	handshake := sessionHandshake{Rows: 24, Cols: 80}
	if term.IsTerminal(int(os.Stdin.Fd())) {
		if width, height, err := term.GetSize(int(os.Stdin.Fd())); err == nil {
			handshake.Rows = uint16(height)
			handshake.Cols = uint16(width)
		}
	}
	payload, err := json.Marshal(handshake)
	if err != nil {
		return err
	}
	if _, err := conn.Write(append(payload, '\n')); err != nil {
		return err
	}
	var restore func() error
	if term.IsTerminal(int(os.Stdin.Fd())) {
		state, err := term.MakeRaw(int(os.Stdin.Fd()))
		if err != nil {
			return err
		}
		restore = func() error { return term.Restore(int(os.Stdin.Fd()), state) }
		defer restore()
	}
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigChan)

	stdinDone := make(chan struct{}, 1)
	go func() {
		_, _ = io.Copy(conn, os.Stdin)
		close(stdinDone)
	}()

	outputDone := make(chan struct{}, 1)
	go func() {
		_, _ = io.Copy(os.Stdout, conn)
		close(outputDone)
	}()

	select {
	case <-stdinDone:
	case <-outputDone:
	case <-sigChan:
	}

	return nil
}

func RunSessionServer(containerName, sessionID string) error {
	if os.Geteuid() != 0 {
		return fmt.Errorf("session server requires root")
	}
	server := &sessionServer{
		store:      NewStore(),
		container:  sanitizeName(containerName),
		sessionID:  sanitizeName(sessionID),
		bufferPath: NewStore().SessionBufferPath(containerName, sessionID),
	}
	return server.run()
}

func RunChrootSession(rootPath, shell, hostname string) error {
	if os.Geteuid() != 0 {
		return fmt.Errorf("chroot session requires root")
	}
	if err := syscall.Unshare(syscall.CLONE_NEWNS | syscall.CLONE_NEWUTS); err != nil {
		return fmt.Errorf("unshare namespaces failed: %w", err)
	}
	if err := syscall.Mount("none", "/", "", syscall.MS_REC|syscall.MS_PRIVATE, ""); err != nil {
		return fmt.Errorf("make mounts private failed: %w", err)
	}
	if err := setupDev(rootPath); err != nil {
		return fmt.Errorf("setup /dev failed: %w", err)
	}
	if err := syscall.Chroot(rootPath); err != nil {
		return fmt.Errorf("chroot failed: %w", err)
	}
	if err := os.Chdir("/"); err != nil {
		return fmt.Errorf("chdir failed: %w", err)
	}
	if hostname = sanitizeName(hostname); hostname != "" {
		if err := syscall.Sethostname([]byte(hostname)); err != nil {
			return fmt.Errorf("set hostname failed: %w", err)
		}
	}
	cmd := exec.Command(shell, "-i")
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = shellSessionEnv(shell, hostname)
	return cmd.Run()
}

func orphanSessionSocketPaths(containerName string) []string {
	pattern := filepath.Join(os.TempDir(), fmt.Sprintf("kiwi-%s-*.sock", sanitizeName(containerName)))
	paths, err := filepath.Glob(pattern)
	if err != nil {
		return nil
	}
	return paths
}

func (s *sessionServer) run() error {
	if s.container == "" || s.sessionID == "" {
		return fmt.Errorf("invalid session")
	}
	if err := os.MkdirAll(s.store.SessionDir(s.container, s.sessionID), 0755); err != nil {
		return err
	}
	_ = os.Remove(s.store.SessionSocketPath(s.container, s.sessionID))
	listener, err := net.Listen("unix", s.store.SessionSocketPath(s.container, s.sessionID))
	if err != nil {
		return err
	}
	s.listener = listener
	if err := s.save(func(info *SessionInfo) {
		info.ID = s.sessionID
		info.Container = s.container
		info.SocketPath = s.store.SessionSocketPath(s.container, s.sessionID)
		info.BufferPath = s.bufferPath
		info.DaemonPID = os.Getpid()
		info.UpdatedAt = time.Now().UTC()
		if info.CreatedAt.IsZero() {
			info.CreatedAt = info.UpdatedAt
		}
	}); err != nil {
		return err
	}
	sigc := make(chan os.Signal, 4)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigc
		s.shutdown()
	}()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if s.isStopping() {
				return nil
			}
			return err
		}
		go s.handleConnection(conn)
	}
}


func (s *sessionServer) handleConnection(conn net.Conn) {
	reader := bufio.NewReader(conn)
	line, err := reader.ReadBytes('\n')
	if err != nil {
		_ = conn.Close()
		return
	}
	var handshake sessionHandshake
	if err := json.Unmarshal(bytesTrimSpace(line), &handshake); err != nil {
		_ = conn.Close()
		return
	}
	if handshake.Rows == 0 {
		handshake.Rows = 24
	}
	if handshake.Cols == 0 {
		handshake.Cols = 80
	}
	if err := s.ensureShell(handshake); err != nil {
		_, _ = conn.Write([]byte("\r\nkiwi: " + err.Error() + "\r\n"))
		_ = conn.Close()
		return
	}
	s.mu.Lock()
	if s.client != nil {
		s.mu.Unlock()
		_, _ = conn.Write([]byte("\r\nkiwi: session is already attached elsewhere\r\n"))
		_ = conn.Close()
		return
	}
	s.client = conn
	bufferPath := s.bufferPath
	_ = s.saveLocked(func(info *SessionInfo) {
		info.Attached = true
		info.UpdatedAt = time.Now().UTC()
	})
	s.mu.Unlock()
	// Reset any mouse-reporting / bracketed-paste modes the terminal may
	// have inherited from a previous TUI (opencode, htop, vim…) that
	// crashed or exited without cleaning up. Without this, stray mouse
	// events from the user's terminal emulator keep arriving at the shell
	// as literal escape sequences like `\e[<35;105;22M` and get echoed
	// back verbatim. These DECRST codes are the canonical way to turn off
	// mouse tracking variants (1000, 1002, 1003, 1005, 1006, 1015) and
	// bracketed paste (2004).
	const terminalReset = "\x1b[?1000l\x1b[?1002l\x1b[?1003l\x1b[?1005l\x1b[?1006l\x1b[?1015l\x1b[?2004l"
	_, _ = conn.Write([]byte(terminalReset))
	if tail, err := readTail(bufferPath, sessionBufferLimit); err == nil && len(tail) > 0 {
		_, _ = conn.Write(tail)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigChan)

	stdinClosed := make(chan struct{}, 1)
	go func() {
		_, _ = io.Copy(s.ptyFile, reader)
		close(stdinClosed)
	}()

	select {
	case <-stdinClosed:
	case <-sigChan:
	}

	s.mu.Lock()
	if s.client == conn {
		s.client = nil
	}
	_ = s.saveLocked(func(info *SessionInfo) {
		info.Attached = false
		info.UpdatedAt = time.Now().UTC()
	})
	s.mu.Unlock()
	// Send a final reset so the user's terminal emulator isn't left in
	// mouse-reporting / alt-screen mode if the shell exited from a TUI.
	const detachReset = "\x1b[?1000l\x1b[?1002l\x1b[?1003l\x1b[?1005l\x1b[?1006l\x1b[?1015l\x1b[?2004l\x1b[?25h\x1b[?1049l"
	_, _ = conn.Write([]byte(detachReset))
	_ = conn.Close()
}

func (s *sessionServer) ensureShell(handshake sessionHandshake) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ptyFile != nil && s.shellPID > 0 && processAlive(s.shellPID) {
		return pty.Setsize(s.ptyFile, &pty.Winsize{Rows: handshake.Rows, Cols: handshake.Cols})
	}
	config, err := s.store.LoadContainer(s.container)
	if err != nil {
		return err
	}
	state, err := s.store.LoadRuntimeState(config.Name)
	if err != nil || !state.Running || !processAlive(state.PID) {
		state, err = s.store.StartContainer(config.Name, StartOptions{})
		if err != nil {
			return err
		}
	}
	shells := sessionShellCandidates(config.Shell)
	var ptmx *os.File
	var cmd *exec.Cmd
	var shellErr error
	rootPath := runtimeRootfsPath(state)
	if rootPath == "" {
		return fmt.Errorf("container %q has no live rootfs", config.Name)
	}
	for _, shell := range shells {
		shellPath := shell
		if !strings.HasPrefix(shellPath, "/") {
			shellPath = "/" + shellPath
		}
		fullPath := filepath.Join(rootPath, strings.TrimPrefix(shellPath, "/"))
		if _, err := os.Stat(fullPath); err != nil {
			continue
		}
		args := []string{"-t", strconv.Itoa(state.PID), "-m", "-u", "-i", "-n", "-p", "-C", "--", shellPath, "-i"}
		cmd = exec.Command("nsenter", args...)
		cmd.Env = shellSessionEnv(shellPath, effectiveHostname(config))
		ptmx, shellErr = pty.StartWithSize(cmd, &pty.Winsize{Rows: handshake.Rows, Cols: handshake.Cols})
		if shellErr == nil {
			if err := attachToCgroup(state.CgroupPath, cmd.Process.Pid); err != nil {
				_ = cmd.Process.Kill()
				_ = ptmx.Close()
				shellErr = err
				continue
			}
			// nsenter forks the shell before we can attach; walk the
			// subtree after a short settle delay to pull any escapee
			// shell into the container cgroup. Without this,
			// cpuset.cpus and cpu.max only apply to nsenter itself
			// and `nproc` inside the container reports host totals.
			go func(pid int) {
				for i := 0; i < 20; i++ {
					timeSleepMillis(25)
					attachSubtreeToCgroup(state.CgroupPath, pid)
				}
			}(cmd.Process.Pid)
			break
		}
	}
	if shellErr != nil {
		return fmt.Errorf("no shell found in container (tried %v): %w", shells, shellErr)
	}
	s.ptyFile = ptmx
	s.shellCmd = cmd
	s.shellPID = cmd.Process.Pid
	if err := s.saveLocked(func(info *SessionInfo) {
		info.ShellPID = s.shellPID
		info.ShellRunning = true
		info.UpdatedAt = time.Now().UTC()
	}); err != nil {
		return err
	}
	go s.pipeShellOutput(ptmx)
	go s.waitShell(cmd, ptmx)
	return nil
}

func (s *sessionServer) pipeShellOutput(ptmx *os.File) {
	buffer := make([]byte, 4096)
	for {
		count, err := ptmx.Read(buffer)
		if count > 0 {
			chunk := append([]byte(nil), buffer[:count]...)
			_ = appendSessionBuffer(s.bufferPath, chunk)
			s.writeClient(chunk)
		}
		if err != nil {
			return
		}
	}
}

func (s *sessionServer) waitShell(cmd *exec.Cmd, ptmx *os.File) {
	_ = cmd.Wait()
	_ = ptmx.Close()
	s.mu.Lock()
	client := s.client
	if client != nil {
		s.client = nil
	}
	if s.ptyFile == ptmx {
		s.ptyFile = nil
	}
	if s.shellCmd == cmd {
		s.shellCmd = nil
	}
	s.shellPID = 0
	_ = s.saveLocked(func(info *SessionInfo) {
		info.ShellPID = 0
		info.ShellRunning = false
		info.Attached = false
		info.UpdatedAt = time.Now().UTC()
	})
	s.mu.Unlock()
	if client != nil {
		_ = client.Close()
	}
}

func (s *sessionServer) writeClient(data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.client == nil {
		return
	}
	if _, err := s.client.Write(data); err != nil {
		_ = s.client.Close()
		s.client = nil
		_ = s.saveLocked(func(info *SessionInfo) {
			info.Attached = false
			info.UpdatedAt = time.Now().UTC()
		})
	}
}

func (s *sessionServer) save(update func(*SessionInfo)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saveLocked(update)
}

func (s *sessionServer) saveLocked(update func(*SessionInfo)) error {
	info, _ := s.store.LoadSession(s.container, s.sessionID)
	if info.ID == "" {
		info = SessionInfo{
			ID:         s.sessionID,
			Container:  s.container,
			SocketPath: s.store.SessionSocketPath(s.container, s.sessionID),
			BufferPath: s.bufferPath,
			CreatedAt:  time.Now().UTC(),
		}
	}
	update(&info)
	return s.store.SaveSession(info)
}

func (s *sessionServer) shutdown() {
	s.mu.Lock()
	if s.stopping {
		s.mu.Unlock()
		return
	}
	s.stopping = true
	listener := s.listener
	client := s.client
	ptyFile := s.ptyFile
	shellPID := s.shellPID
	s.mu.Unlock()
	if listener != nil {
		_ = listener.Close()
	}
	if client != nil {
		_ = client.Close()
	}
	if shellPID > 0 {
		_ = killProcessTree(shellPID)
	}
	if ptyFile != nil {
		_ = ptyFile.Close()
	}
	_ = os.Remove(s.store.SessionSocketPath(s.container, s.sessionID))
}

func (s *sessionServer) isStopping() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stopping
}

func appendSessionBuffer(path string, chunk []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	if _, err := file.Write(chunk); err != nil {
		_ = file.Close()
		return err
	}
	info, err := file.Stat()
	_ = file.Close()
	if err != nil || info.Size() <= sessionBufferLimit*2 {
		return err
	}
	tail, err := readTail(path, sessionBufferLimit)
	if err != nil {
		return err
	}
	return os.WriteFile(path, tail, 0644)
}

func readTail(path string, limit int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	start := int64(0)
	if info.Size() > limit {
		start = info.Size() - limit
	}
	if _, err := file.Seek(start, io.SeekStart); err != nil {
		return nil, err
	}
	data, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}
	if len(data) > 0 && data[len(data)-1] != '\n' {
		if index := bytes.LastIndexByte(data, '\n'); index >= 0 {
			data = data[:index+1]
		} else {
			data = nil
		}
	}
	return data, nil
}

func killProcessTree(pid int) error {
	if pid <= 0 {
		return nil
	}
	if err := syscall.Kill(-pid, syscall.SIGTERM); err != nil {
		_ = syscall.Kill(pid, syscall.SIGTERM)
	}
	waitForExit(pid, 3)
	if processAlive(pid) {
		if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	}
	return nil
}

func isSessionDisconnect(err error) bool {
	if err == nil {
		return false
	}
	message := err.Error()
	return strings.Contains(message, "closed") || strings.Contains(message, "reset by peer")
}

func bytesTrimSpace(data []byte) []byte {
	return []byte(strings.TrimSpace(string(data)))
}
