package kiwi

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func (s Store) NewContainer(imageName, containerName string, sizeBytes int64) (ContainerConfig, error) {
	imageName = sanitizeName(imageName)
	containerName = sanitizeName(containerName)
	if imageName == "" {
		return ContainerConfig{}, fmt.Errorf("usage: ./kiwi create <image> [--storage 1G]")
	}
	if containerName == "" {
		generatedName, err := s.NextContainerName()
		if err != nil {
			return ContainerConfig{}, err
		}
		containerName = generatedName
	}
	if _, err := s.ensureImageDirectoryBackend(imageName); err != nil {
		return ContainerConfig{}, err
	}
	if _, err := os.Stat(s.ContainerConfigPath(containerName)); err == nil {
		return ContainerConfig{}, fmt.Errorf("container %q already exists", containerName)
	}
	if err := s.EnsureLayout(); err != nil {
		return ContainerConfig{}, err
	}
	ip, err := s.NextIP()
	if err != nil {
		return ContainerConfig{}, err
	}
	if err := os.MkdirAll(s.ContainerDir(containerName), 0755); err != nil {
		return ContainerConfig{}, err
	}
	if sizeBytes == 0 {
		sizeBytes = defaultStateSize
	}
	sizeBytes = alignSize(sizeBytes, 1024*1024)
	if sizeBytes < defaultStateSize {
		return ContainerConfig{}, fmt.Errorf("storage must be at least %s", formatBytesIEC(defaultStateSize))
	}
	statePath := s.ContainerStatePath(containerName)
	if err := os.MkdirAll(statePath, 0755); err != nil {
		return ContainerConfig{}, err
	}
	for _, dir := range []string{"upper", "work", "upper/etc"} {
		if err := ensureDir(filepath.Join(statePath, dir)); err != nil {
			return ContainerConfig{}, err
		}
	}
	config := ContainerConfig{
		Name:           containerName,
		Hostname:       containerName,
		Image:          imageName,
		StatePath:      statePath,
		StateSizeBytes: sizeBytes,
		StateSizeHost:  false,
		StateBackend:   "dir",
		IPv4:           ip,
		Gateway:        defaultGateway,
		Memory:         defaultMemory,
		CPU:            defaultCPU,
		Network:        defaultNetwork,
		CreatedAt:      time.Now().UTC(),
	}
	if err := s.SaveContainer(config); err != nil {
		return ContainerConfig{}, err
	}
	maybeChownPaths(
		s.ContainerDir(containerName),
		s.ContainerConfigPath(containerName),
		s.ContainerSnapshotsDir(containerName),
		s.ContainerSessionsDir(containerName),
	)
	return config, nil
}

func (s Store) NextContainerName() (string, error) {
	for attempts := 0; attempts < 32; attempts++ {
		candidate, err := generateContainerID()
		if err != nil {
			return "", err
		}
		if _, err := os.Stat(s.ContainerConfigPath(candidate)); os.IsNotExist(err) {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("failed to generate unique container id")
}

func (s Store) UpdateContainerResources(name string, options StartOptions) (ContainerConfig, error) {
	config, _, err := s.ensureContainerDirectoryBackend(name)
	if err != nil {
		return ContainerConfig{}, err
	}
	state, _ := s.LoadRuntimeState(config.Name)
	if state.Running && processAlive(state.PID) {
		return ContainerConfig{}, fmt.Errorf("container %q is running; stop it before changing config", config.Name)
	}
	if options.Memory != "" {
		if _, err := parseMemory(options.Memory); err != nil {
			return ContainerConfig{}, err
		}
		config.Memory = options.Memory
	}
	if strings.TrimSpace(options.Storage) != "" {
		if state.TargetMountpoint != "" && isMounted(state.TargetMountpoint) {
			return ContainerConfig{}, fmt.Errorf("container %q is mounted; unmount it before changing storage", config.Name)
		}
		if isMounted(state.MergedMountpoint) || isMounted(state.StateMountpoint) || isMounted(state.BaseMountpoint) {
			return ContainerConfig{}, fmt.Errorf("container %q is mounted; unmount it before changing storage", config.Name)
		}
		if options.StorageHost || strings.EqualFold(strings.TrimSpace(options.Storage), "host") {
			config.StateSizeHost = true
		} else {
			sizeBytes, err := parseSize(options.Storage)
			if err != nil {
				return ContainerConfig{}, err
			}
			sizeBytes = alignSize(sizeBytes, 1024*1024)
			if sizeBytes < defaultStateSize {
				return ContainerConfig{}, fmt.Errorf("storage must be at least %s", formatBytesIEC(defaultStateSize))
			}
			minimumSize, err := minimumAllowedStorage(config)
			if err != nil {
				return ContainerConfig{}, err
			}
			if sizeBytes < minimumSize {
				return ContainerConfig{}, fmt.Errorf("storage must be at least %s (current usage + 1G safety margin)", formatBytesIEC(minimumSize))
			}
			// The quota FUSE daemon reads StateSizeBytes at spawn time;
			// the container must be stopped (enforced above) so the
			// next start picks up the new cap. We also handle the
			// legacy ext4 image path for snapshots created before the
			// switch to quotafs.
			if config.StateBackend != "dir" && !isDirectoryPath(config.StatePath) {
				if err := resizeExt4Image(config.StatePath, sizeBytes); err != nil {
					return ContainerConfig{}, err
				}
			}
			config.StateSizeBytes = sizeBytes
			config.StateSizeBytes = sizeBytes
			config.StateSizeHost = false
		}
	}
	if options.CPUHost {
		config.CPUHost = true
		config.CPU = 0
	}
	if options.CPU > 0 {
		config.CPU = options.CPU
		config.CPUHost = false
	}
	if strings.TrimSpace(options.Network) != "" {
		if err := validateNetworkMode(options.Network); err != nil {
			return ContainerConfig{}, err
		}
		config.Network = normalizeNetworkMode(options.Network)
	}
	if options.Shell != "" {
		config.Shell = strings.TrimSpace(options.Shell)
	}
	defaultContainerConfigValues(&config)
	if err := s.SaveContainer(config); err != nil {
		return ContainerConfig{}, err
	}
	return config, nil
}

func (s Store) ensureMounted(name, target string) (RuntimeState, bool, error) {
	config, image, err := s.ensureContainerDirectoryBackend(name)
	if err != nil {
		return RuntimeState{}, false, err
	}

	runtimeDir := s.RuntimeContainerDir(config.Name)
	baseDir := filepath.Join(runtimeDir, "base")
	stateDir := filepath.Join(runtimeDir, "state")
	mergedDir := filepath.Join(runtimeDir, "merged")
	for _, dir := range []string{runtimeDir, baseDir, stateDir, mergedDir} {
		if err := ensureDir(dir); err != nil {
			return RuntimeState{}, false, err
		}
	}
	state, _ := s.LoadRuntimeState(config.Name)
	mountedDuringCall := false
	if !isMounted(mergedDir) {
		lowerDir, upperDir, workDir, cleanup, err := prepareOverlayDirs(image, config, baseDir, stateDir)
		if err != nil {
			return RuntimeState{}, false, err
		}
		defer cleanup()
		if err := mountOverlayPath(lowerDir, upperDir, workDir, mergedDir); err != nil {
			return RuntimeState{}, false, fmt.Errorf("mount overlay: %w", err)
		}
		mountedDuringCall = true
	}
	if target != "" {
		cleanTarget := filepath.Clean(target)
		if state.TargetMountpoint != "" && state.TargetMountpoint != cleanTarget && isMounted(state.TargetMountpoint) {
			return RuntimeState{}, false, fmt.Errorf("container %q is already mounted on %s", config.Name, state.TargetMountpoint)
		}
		if isMounted(cleanTarget) && state.TargetMountpoint != cleanTarget {
			return RuntimeState{}, false, fmt.Errorf("target %s is already mounted", cleanTarget)
		}
		if err := ensureDir(cleanTarget); err != nil {
			return RuntimeState{}, false, err
		}
		if !isMounted(cleanTarget) {
			if err := bindMountPath(mergedDir, cleanTarget); err != nil {
				return RuntimeState{}, false, fmt.Errorf("bind mount %s: %w", cleanTarget, err)
			}
			mountedDuringCall = true
		}
		state.TargetMountpoint = cleanTarget
	}
	state.Name = config.Name
	if image.Format == "dir" {
		state.BaseMountpoint = ""
	} else {
		state.BaseMountpoint = baseDir
	}
	if config.StateBackend == "dir" || isDirectoryPath(config.StatePath) {
		state.StateMountpoint = ""
	} else {
		state.StateMountpoint = stateDir
	}
	state.MergedMountpoint = mergedDir
	state.IPv4 = config.IPv4
	state.MountedAt = time.Now().UTC()
	if err := s.SaveRuntimeState(state); err != nil {
		return RuntimeState{}, false, err
	}
	return state, mountedDuringCall, nil
}

func (s Store) MountContainer(name, target string) (RuntimeState, error) {
	config, image, err := s.ensureContainerDirectoryBackend(name)
	if err != nil {
		return RuntimeState{}, err
	}
	state, _ := s.LoadRuntimeState(config.Name)
	cleanTarget := filepath.Clean(target)
	// Safety: refuse to mount at system-critical paths. An earlier bug had
	// us bind-mount /proc/<pid>/root (which resolves to / in the host mount
	// namespace), and callers chaining it with prepareRootfsFiles could
	// overwrite host files like /etc/hostname.
	if cleanTarget == "/" || cleanTarget == "" {
		return RuntimeState{}, fmt.Errorf("refusing to mount at %q", cleanTarget)
	}
	for _, forbidden := range []string{"/etc", "/bin", "/sbin", "/usr", "/lib", "/lib64", "/boot", "/root", "/var", "/home"} {
		if cleanTarget == forbidden {
			return RuntimeState{}, fmt.Errorf("refusing to mount at system path %q", cleanTarget)
		}
	}
	if isMounted(cleanTarget) {
		return RuntimeState{}, fmt.Errorf("target %s is already mounted", cleanTarget)
	}
	if err := ensureDir(cleanTarget); err != nil {
		return RuntimeState{}, err
	}
	// Mount the container's rootfs layers as a fresh overlay at `target`.
	// We never bind-mount `/proc/<pid>/root`: that path is resolved in the
	// *host's* mount namespace, so it points to the host rootfs (/) and
	// would expose the entire host filesystem — not what the user asked
	// for, and actively dangerous.
	tempDir, err := os.MkdirTemp("", "kiwi-mount-")
	if err != nil {
		return RuntimeState{}, err
	}
	defer os.RemoveAll(tempDir)
	lowerDir, upperDir, workDir, cleanup, err := prepareOverlayDirs(image, config, filepath.Join(tempDir, "base"), filepath.Join(tempDir, "state"))
	if err != nil {
		return RuntimeState{}, err
	}
	defer cleanup()
	if err := mountOverlayPath(lowerDir, upperDir, workDir, cleanTarget); err != nil {
		return RuntimeState{}, fmt.Errorf("mount overlay: %w", err)
	}
	state.Name = config.Name
	state.TargetMountpoint = cleanTarget
	state.MountedAt = time.Now().UTC()
	if err := s.SaveRuntimeState(state); err != nil {
		_ = unmountTreePath(cleanTarget)
		return RuntimeState{}, err
	}
	return state, nil
}

func (s Store) MountLiveContainer(name, target string) (RuntimeState, error) {
	return s.MountContainer(name, target)
}

func (s Store) UnmountContainer(name string) error {
	config, err := s.LoadContainer(name)
	if err != nil {
		return err
	}
	state, err := s.LoadRuntimeState(config.Name)
	if err != nil {
		return nil
	}
	if state.TargetMountpoint != "" {
		if err := unmountTreePath(state.TargetMountpoint); err != nil {
			return err
		}
		state.TargetMountpoint = ""
	}
	state.MountedAt = time.Time{}
	return s.SaveRuntimeState(state)
}

func (s Store) SnapshotContainer(name, snapshot string) (string, error) {
	config, manifest, err := s.ensureContainerDirectoryBackend(name)
	if err != nil {
		return "", err
	}
	snapshotName := sanitizeName(snapshot)
	destination := s.SnapshotPath(config.Name, snapshotName)
	if destination == ".kiwi" || snapshotName == "" {
		return "", fmt.Errorf("snapshot name is required")
	}
	snapshotConfig := config
	snapshotConfig.Hostname = snapshotName
	snapshotConfig.LazyStateArchive = ""
	if err := s.saveKiwiSquashfs(archiveEnvelope{Kind: "container", ImageName: manifest.Name, Container: config.Name}, manifest, &snapshotConfig, destination); err != nil {
		return "", err
	}
	maybeChownFile(destination)
	return destination, nil
}

// ensureKiwiMountPath mounts a squashfs .kiwi archive read-only at a
// content-addressed location under /run/kiwi/archive-mounts and returns the
// mountpoint. /run is a tmpfs that file managers (GNOME Files, Nautilus,
// KDE Dolphin) skip by design, so the loopback mount doesn't show up as a
// phantom drive in the desktop sidebar. The mount is shared across every
// container derived from the same archive.
func ensureKiwiMountPath(archivePath string) (string, error) {
	info, err := os.Stat(archivePath)
	if err != nil {
		return "", err
	}
	key := fmt.Sprintf("%s|%d|%d", archivePath, info.Size(), info.ModTime().UnixNano())
	hash := fmt.Sprintf("%x", sha256.Sum256([]byte(key)))[:16]
	mountPoint := filepath.Join("/run/kiwi/archive-mounts", hash)
	if err := os.MkdirAll(mountPoint, 0755); err != nil {
		return "", err
	}
	if !isMounted(mountPoint) {
		if err := mountSquashfsReadOnly(archivePath, mountPoint); err != nil {
			return "", err
		}
	}
	return mountPoint, nil
}

// archiveStateCachePath returns the canonical cache location for an archive.
// Same keying scheme as getCachedArchiveState so a snapshot warmed up here is
// reused unchanged by prepareOverlayDirs.
func (s Store) archiveStateCachePath(archivePath string) (string, error) {
	info, err := os.Stat(archivePath)
	if err != nil {
		return "", err
	}
	dataRoot := s.DataRoot
	if dataRoot == "" {
		exeDir := "."
		if executable, err := os.Executable(); err == nil {
			exeDir = filepath.Dir(executable)
		}
		dataRoot = filepath.Join(exeDir, "kiwi-data")
	}
	keyMaterial := fmt.Sprintf("%s|%d|%d", archivePath, info.Size(), info.ModTime().UnixNano())
	hash := fmt.Sprintf("%x", sha256.Sum256([]byte(keyMaterial)))[:16]
	return filepath.Join(dataRoot, "cache", "archive-state", hash), nil
}

func (s Store) StartContainer(name string, options StartOptions) (RuntimeState, error) {
	config, image, err := s.ensureContainerDirectoryBackend(name)
	if err != nil {
		return RuntimeState{}, err
	}

	if options.Memory == "" {
		options.Memory = config.Memory
	}
	if !options.CPUHost && config.CPUHost {
		options.CPUHost = true
	}
	if !options.CPUHost && options.CPU <= 0 {
		options.CPU = config.CPU
	}
	if options.Network == "" {
		options.Network = config.Network
	}
	networkMode := normalizeNetworkMode(options.Network)
	runtimeState, _ := s.LoadRuntimeState(config.Name)
	if runtimeState.Running && processAlive(runtimeState.PID) {
		return RuntimeState{}, fmt.Errorf("container %q is already running", config.Name)
	}
	state := runtimeState
	state.Name = config.Name
	state.IPv4 = config.IPv4
	state.MountedAt = time.Now().UTC()
	cgroupPath, err := createCgroup(config.Name, options)
	if err != nil {
		return RuntimeState{}, err
	}
	vethHost := ""
	cleanup := func() {
		if vethHost != "" {
			_ = runCommand("ip", "link", "del", vethHost)
		}
		if cgroupPath != "" {
			if pathExists(filepath.Join(cgroupPath, "cgroup.kill")) {
				_ = writeFileString(filepath.Join(cgroupPath, "cgroup.kill"), "1")
			}
			_ = os.RemoveAll(cgroupPath)
		}
	}
	readPipe, writePipe, err := os.Pipe()
	if err != nil {
		cleanup()
		return RuntimeState{}, err
	}
	defer readPipe.Close()
	defer writePipe.Close()
	executable, err := os.Executable()
	if err != nil {
		cleanup()
		return RuntimeState{}, err
	}
	if options.Name == "" {
		options.Name = effectiveHostname(config)
	}
	childArgs := []string{
		"__enter",
		"--image", image.RootfsPath,
		"--state", config.StatePath,
		"--name", options.Name,
		"--ipv4", config.IPv4,
		"--sync-fd", "3",
	}
	if config.LazyStateArchive != "" {
		childArgs = append(childArgs, "--archive", config.LazyStateArchive)
	}
	childArgs = append(childArgs, "--")
	childArgs = append(childArgs, options.Cmd...)
	cmd := exec.Command(executable, childArgs...)
	cmd.Stdin = nil
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = os.Environ()
	cmd.ExtraFiles = []*os.File{readPipe}
	// CLONE_NEWCGROUP virtualises /proc/self/cgroup inside the container so
	// `cat /proc/self/cgroup` shows "0::/" rather than the host path.
	const cloneNewCgroup = 0x02000000
	cloneFlags := uintptr(syscall.CLONE_NEWNS | syscall.CLONE_NEWPID | syscall.CLONE_NEWUTS | syscall.CLONE_NEWIPC | cloneNewCgroup)
	if networkMode != "host" {
		cloneFlags |= uintptr(syscall.CLONE_NEWNET)
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags: cloneFlags,
	}
	if err := cmd.Start(); err != nil {
		cleanup()
		return RuntimeState{}, err
	}
	if err := attachToCgroup(cgroupPath, cmd.Process.Pid); err != nil {
		_ = cmd.Process.Kill()
		cleanup()
		return RuntimeState{}, err
	}
	if networkMode != "host" {
		vethHost, err = setupContainerNetwork(cmd.Process.Pid, config)
		if err != nil {
			_ = cmd.Process.Kill()
			cleanup()
			return RuntimeState{}, err
		}
	}
	if _, err := writePipe.Write([]byte("1")); err != nil {
		_ = cmd.Process.Kill()
		cleanup()
		return RuntimeState{}, err
	}
	_ = writePipe.Close()
	timeSleepMillis(60)
	if !processAlive(cmd.Process.Pid) {
		cleanup()
		return RuntimeState{}, errors.New("container exited during startup")
	}
	state.PID = cmd.Process.Pid
	state.Running = true
	state.CgroupPath = cgroupPath
	state.VethHost = vethHost
	if networkMode == "host" {
		state.IPv4 = ""
	} else {
		state.IPv4 = config.IPv4
	}
	state.StartedAt = time.Now().UTC()
	if err := s.SaveRuntimeState(state); err != nil {
		_ = cmd.Process.Kill()
		cleanup()
		return RuntimeState{}, err
	}
	watchContainerStorageLimit(config, state.PID)
	maybeChownPaths(
		s.ContainerDir(config.Name),
		s.ContainerConfigPath(config.Name),
		s.ContainerSnapshotsDir(config.Name),
		s.ContainerSessionsDir(config.Name),
	)
	_ = cmd.Process.Release()
	return state, nil
}

func (s Store) StopContainer(name string) error {
	config, err := s.LoadContainer(name)
	if err != nil {
		return err
	}
	state, err := s.LoadRuntimeState(config.Name)
	if err != nil {
		return nil
	}
	// Use cgroup.kill as primary mechanism to kill ALL processes immediately (same as Docker)
	if state.CgroupPath != "" {
		if pathExists(filepath.Join(state.CgroupPath, "cgroup.kill")) {
			_ = writeFileString(filepath.Join(state.CgroupPath, "cgroup.kill"), "1")
		}
	}
	// Fallback: SIGTERM then SIGKILL only if cgroup.kill didn't work or isn't available
	if state.PID > 0 && processAlive(state.PID) {
		_ = syscall.Kill(state.PID, syscall.SIGTERM)
		waitForExit(state.PID, 5)
		if processAlive(state.PID) {
			_ = syscall.Kill(state.PID, syscall.SIGKILL)
		}
	}
	if state.CgroupPath != "" {
		_ = os.RemoveAll(state.CgroupPath)
	}
	if state.VethHost != "" {
		_ = runCommand("ip", "link", "del", state.VethHost)
	}
	_ = cleanupContainerNetworkLinks(config.Name)
	state.Running = false
	state.PID = 0
	state.CgroupPath = ""
	state.VethHost = ""
	state.IPv4 = ""
	// Release the overlay merge so the container can be safely deleted or
	// restarted without leftover mounts pinning files.
	if state.MergedMountpoint != "" && isMounted(state.MergedMountpoint) {
		_ = unmountPath(state.MergedMountpoint)
	}
	state.MergedMountpoint = ""
	state.BaseMountpoint = ""
	state.StateMountpoint = ""
	if err := s.SaveRuntimeState(state); err != nil {
		return err
	}
	return s.UnmountContainer(config.Name)
}

func (s Store) StopManagedContainer(name string) error {
	config, err := s.LoadContainer(name)
	if err != nil {
		return err
	}
	state, err := s.LoadRuntimeState(config.Name)
	if err != nil {
		_ = s.DeleteAllManagedSessions(config.Name)
		return nil
	}
	if state.Running && processAlive(state.PID) {
		err := s.StopContainer(config.Name)
		_ = s.DeleteAllManagedSessions(config.Name)
		return err
	}
	if state.CgroupPath != "" {
		if pathExists(filepath.Join(state.CgroupPath, "cgroup.kill")) {
			_ = writeFileString(filepath.Join(state.CgroupPath, "cgroup.kill"), "1")
		}
		_ = os.RemoveAll(state.CgroupPath)
	}
	if state.VethHost != "" {
		_ = runCommand("ip", "link", "del", state.VethHost)
	}
	_ = cleanupContainerNetworkLinks(config.Name)
	err = s.UnmountContainer(config.Name)
	_ = s.DeleteAllManagedSessions(config.Name)
	return err
}

func (s Store) DeleteContainer(name string) error {
	config, err := s.LoadContainer(name)
	if err != nil {
		return err
	}
	state, _ := s.LoadRuntimeState(config.Name)
	if state.Running && processAlive(state.PID) {
		if err := s.StopContainer(config.Name); err != nil {
			return err
		}
	} else {
		_ = s.UnmountContainer(config.Name)
	}
	_ = cleanupContainerNetworkLinks(config.Name)
	_ = s.DeleteAllManagedSessions(config.Name)
	if err := os.RemoveAll(s.RuntimeContainerDir(config.Name)); err != nil {
		return err
	}
	return os.RemoveAll(s.ContainerDir(config.Name))
}

// PurgeEverything is the nuclear option. It stops every kiwi-managed
// container, unmounts every archive/state/overlay mount under DataRoot and
// RuntimeRoot, tears down leftover network links, then removes both trees
// entirely. Running containers that won't terminate with SIGTERM are killed
// with SIGKILL. After this call, the host is back to a blank kiwi install.
func (s Store) PurgeEverything() error {
	// 1. Stop every container we know about, then kill anything still
	//    running under a kiwi __enter / __sessiond process.
	if containers, err := s.ListContainers(); err == nil {
		for _, container := range containers {
			_ = s.DeleteAllManagedSessions(container.Name)
			_ = s.cleanupRuntimeDirectory(container.Name)
		}
	}
	if entries, err := os.ReadDir(s.RuntimeContainersDir()); err == nil {
		for _, entry := range entries {
			if entry.IsDir() {
				_ = s.cleanupRuntimeDirectory(sanitizeName(entry.Name()))
			}
		}
	}
	// Belt-and-suspenders: kill any leftover kiwi helper process that may
	// still be pinning mounts (happens if a session crashed without cleanup).
	_ = runCommand("pkill", "-9", "-f", "kiwi __sessiond")
	_ = runCommand("pkill", "-9", "-f", "kiwi __enter")

	// 2. Unmount every mount point nested under DataRoot, RuntimeRoot, or
	//    /run/kiwi (where squashfs archive loop mounts live). We enumerate
	//    from /proc/self/mountinfo and sort descending so that child mounts
	//    are released before their parents.
	for _, root := range []string{s.DataRoot, s.RuntimeRoot, "/run/kiwi"} {
		if root == "" {
			continue
		}
		for _, mp := range mountsUnder(root) {
			_ = unmountTreePath(mp)
		}
	}

	// 3. Detach any loop device still pointing into DataRoot (squashfs
	//    archive mounts backed by loopback).
	_ = runCommand("sh", "-c", "losetup -a | awk -F: '{print $1}' | xargs -r -n1 losetup -d 2>/dev/null")

	// 4. Remove network bridge if kiwi created one.
	_ = runCommand("ip", "link", "del", defaultBridgeName)

	// 5. Wipe the trees.
	if err := os.RemoveAll(s.RuntimeRoot); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove runtime root: %w", err)
	}
	if err := os.RemoveAll(s.DataRoot); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove data root: %w", err)
	}
	_ = os.RemoveAll("/run/kiwi")
	return nil
}

// mountsUnder returns every mountpoint (deepest first) whose path starts
// with prefix. Reading /proc/self/mountinfo avoids depending on `findmnt`.
func mountsUnder(prefix string) []string {
	data, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return nil
	}
	cleanPrefix := filepath.Clean(prefix)
	var points []string
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 5 {
			continue
		}
		// Field 5 is the mountpoint in the mount namespace; it is
		// octal-escaped (e.g. " " -> \040) but we don't expect kiwi
		// paths to contain those, so a direct match is fine.
		mp := fields[4]
		if mp == cleanPrefix || strings.HasPrefix(mp, cleanPrefix+"/") {
			points = append(points, mp)
		}
	}
	// Deepest paths first so we unmount children before parents.
	for i := 0; i < len(points); i++ {
		for j := i + 1; j < len(points); j++ {
			if len(points[j]) > len(points[i]) {
				points[i], points[j] = points[j], points[i]
			}
		}
	}
	return points
}

func (s Store) ForceCleanupAll() error {
	runtimeDir := s.RuntimeContainersDir()
	if entries, err := os.ReadDir(runtimeDir); err == nil {
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			name := sanitizeName(entry.Name())
			_ = s.cleanupRuntimeDirectory(name)
		}
	}
	containersDir := s.ContainersDir()
	if entries, err := os.ReadDir(containersDir); err == nil {
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			name := sanitizeName(entry.Name())
			_ = os.RemoveAll(filepath.Join(containersDir, name))
		}
	}
	return nil
}

func (s Store) KillAllManaged() error {
	containers, err := s.ListContainers()
	if err == nil {
		for _, container := range containers {
			_ = s.DeleteAllManagedSessions(container.Name)
		}
	}
	entries, err := os.ReadDir(s.RuntimeContainersDir())
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	failures := make([]string, 0)
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := sanitizeName(entry.Name())
		if name == "" {
			continue
		}
		if err := s.cleanupRuntimeDirectory(name); err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", name, err))
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("killall failed: %s", strings.Join(failures, "; "))
	}
	return nil
}

func (s Store) CleanupOrphanRuntime() error {
	entries, err := os.ReadDir(s.RuntimeContainersDir())
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	failures := make([]string, 0)
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := sanitizeName(entry.Name())
		if name == "" || pathExists(s.ContainerConfigPath(name)) {
			continue
		}
		if err := s.cleanupRuntimeDirectory(name); err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", name, err))
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("cleanup failed: %s", strings.Join(failures, "; "))
	}
	return nil
}

func (s Store) cleanupRuntimeDirectory(name string) error {
	state, _ := s.LoadRuntimeState(name)
	if state.PID > 0 && processAlive(state.PID) {
		_ = syscall.Kill(state.PID, syscall.SIGTERM)
		waitForExit(state.PID, 3)
		if processAlive(state.PID) {
			_ = syscall.Kill(state.PID, syscall.SIGKILL)
		}
	}
	if state.CgroupPath != "" {
		if pathExists(filepath.Join(state.CgroupPath, "cgroup.kill")) {
			_ = writeFileString(filepath.Join(state.CgroupPath, "cgroup.kill"), "1")
		}
		_ = os.RemoveAll(state.CgroupPath)
	}
	if state.TargetMountpoint != "" {
		_ = unmountTreePath(state.TargetMountpoint)
	}
	for _, path := range []string{
		s.RuntimeMergedMountpoint(name),
		s.RuntimeStateMountpoint(name),
		s.RuntimeBaseMountpoint(name),
	} {
		_ = unmountPath(path)
	}
	_ = cleanupContainerNetworkLinks(name)
	for _, path := range orphanSessionSocketPaths(name) {
		_ = os.Remove(path)
	}
	_ = s.ClearRuntimeState(name)
	return os.RemoveAll(s.RuntimeContainerDir(name))
}

func (s Store) AttachContainer(name string, command []string) error {
	config, err := s.LoadContainer(name)
	if err != nil {
		return err
	}
	if os.Geteuid() != 0 {
		return fmt.Errorf("attach requires root; run with sudo")
	}
	state, err := s.LoadRuntimeState(config.Name)
	if err != nil || !state.Running || !processAlive(state.PID) {
		state, err = s.StartContainer(config.Name, StartOptions{})
		if err != nil {
			return err
		}
	}
	if len(command) == 0 {
		// Try shells one by one inside the container via nsenter
		shells := []string{}
		if strings.TrimSpace(config.Shell) != "" {
			shells = append(shells, config.Shell)
		}
		defaults := []string{"/bin/sh", "/bin/bash", "/usr/bin/bash", "/bin/zsh", "/usr/bin/zsh", "/bin/fish", "/usr/bin/fish", "/bin/ash", "/usr/bin/ash", "/bin/dash", "/usr/bin/dash"}
		for _, d := range defaults {
			found := false
			for _, s := range shells {
				if s == d {
					found = true
					break
				}
			}
			if !found {
				shells = append(shells, d)
			}
		}
		var lastErr error
		for _, shell := range shells {
			args := []string{"-t", strconv.Itoa(state.PID), "-m", "-u", "-i", "-n", "-p", "-C", "--", shell}
			cmd := exec.Command("nsenter", args...)
			cmd.Stdin = os.Stdin
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			cmd.Env = os.Environ()
			if err := runAttachedCommand(cmd, state.CgroupPath); err == nil {
				return nil
			} else {
				lastErr = err
			}
		}
		if lastErr != nil {
			return fmt.Errorf("no shell found in container (tried %v): %w", shells, lastErr)
		}
		return nil
	}
	args := []string{"-t", strconv.Itoa(state.PID), "-m", "-u", "-i", "-n", "-p", "-C", "--"}
	args = append(args, command...)
	cmd := exec.Command("nsenter", args...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = os.Environ()
	return runAttachedCommand(cmd, state.CgroupPath)
}

func resolveContainerShell(rootPath, configured string) (string, error) {
	candidates := []string{}
	configured = strings.TrimSpace(configured)
	if configured != "" {
		candidates = append(candidates, configured)
	}
	candidates = append(candidates,
		"/bin/bash", "/usr/bin/bash",
		"/bin/zsh", "/usr/bin/zsh",
		"/bin/fish", "/usr/bin/fish",
		"/bin/ash", "/usr/bin/ash",
		"/bin/sh", "/usr/bin/sh",
		"/usr/local/bin/bash", "/usr/local/bin/sh",
		"/bin/dash", "/usr/bin/dash",
	)
	seen := map[string]bool{}
	for _, candidate := range candidates {
		check := filepath.Join(rootPath, strings.TrimPrefix(candidate, "/"))
		if !seen[check] {
			seen[check] = true
			if pathExists(check) {
				return candidate, nil
			}
		}
	}
	// Always return /bin/sh no matter what, no error. All containers have at least this.
	return "/bin/sh", nil
}

func prepareRootfsFiles(rootPath, hostname, ipv4 string) error {
	if err := writeContainerFile(rootPath, "/etc/hostname", []byte(hostname+"\n"), 0644); err != nil {
		return err
	}
	hosts := fmt.Sprintf("127.0.0.1 localhost\n%s %s\n", ipv4, hostname)
	if err := writeContainerFile(rootPath, "/etc/hosts", []byte(hosts), 0644); err != nil {
		return err
	}
	resolv := buildContainerResolvConf()
	if err := writeContainerFile(rootPath, "/etc/resolv.conf", resolv, 0644); err != nil {
		return err
	}
	return nil
}

func writeContainerFile(root, path string, data []byte, mode os.FileMode) error {
	target := filepath.Join(root, strings.TrimPrefix(path, "/"))
	if err := ensureDir(filepath.Dir(target)); err != nil {
		return err
	}
	if err := os.Remove(target); err != nil && !os.IsNotExist(err) {
		return err
	}
	return os.WriteFile(target, data, mode)
}

func buildContainerResolvConf() []byte {
	paths := []string{"/etc/resolv.conf", "/run/systemd/resolve/resolv.conf", "/run/NetworkManager/no-stub-resolv.conf", "/usr/lib/systemd/resolv.conf"}
	seenNameservers := map[string]bool{}
	nameservers := make([]string, 0, 4)
	searchLine := "search ."
	optionsLine := "options edns0"
	for _, path := range paths {
		if !pathExists(path) {
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		for _, line := range strings.Split(string(data), "\n") {
			fields := strings.Fields(strings.TrimSpace(line))
			if len(fields) < 2 {
				continue
			}
			switch fields[0] {
			case "nameserver":
				if !isUsableNameserver(fields[1]) || seenNameservers[fields[1]] {
					continue
				}
				seenNameservers[fields[1]] = true
				nameservers = append(nameservers, fields[1])
			case "search":
				searchLine = strings.TrimSpace(line)
			case "options":
				optionsLine = strings.TrimSpace(line)
			}
		}
	}
	if len(nameservers) == 0 {
		nameservers = []string{"1.1.1.1", "8.8.8.8", "9.9.9.9"}
	}
	lines := []string{"# generated by kiwi"}
	for _, nameserver := range nameservers {
		lines = append(lines, "nameserver "+nameserver)
	}
	if searchLine != "" {
		lines = append(lines, searchLine)
	}
	if optionsLine != "" {
		lines = append(lines, optionsLine)
	}
	return []byte(strings.Join(lines, "\n") + "\n")
}

func formatBytesIEC(size int64) string {
	if size < 1024 {
		return fmt.Sprintf("%dB", size)
	}
	value := float64(size)
	units := []string{"KiB", "MiB", "GiB", "TiB"}
	for _, unit := range units {
		value /= 1024
		if value < 1024 || unit == units[len(units)-1] {
			if value == float64(int64(value)) {
				return fmt.Sprintf("%d%s", int64(value), unit)
			}
			return fmt.Sprintf("%.1f%s", value, unit)
		}
	}
	return fmt.Sprintf("%dB", size)
}

func ext4BlockSize(imagePath string) (int64, error) {
	output, err := runCommandOutput("dumpe2fs", "-h", imagePath)
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "Block size:") {
			continue
		}
		value := strings.TrimSpace(strings.TrimPrefix(line, "Block size:"))
		size, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return 0, err
		}
		return size, nil
	}
	return 0, fmt.Errorf("could not detect ext4 block size for %s", imagePath)
}

func ext4MinimumSizeBytes(imagePath string) (int64, error) {
	if err := runCommand("e2fsck", "-fy", imagePath); err != nil {
		return 0, err
	}
	output, err := runCommandOutput("resize2fs", "-P", imagePath)
	if err != nil {
		return 0, err
	}
	index := strings.LastIndex(output, ":")
	if index < 0 {
		return 0, fmt.Errorf("could not parse resize2fs output for %s", imagePath)
	}
	blocks, err := strconv.ParseInt(strings.TrimSpace(output[index+1:]), 10, 64)
	if err != nil {
		return 0, err
	}
	blockSize, err := ext4BlockSize(imagePath)
	if err != nil {
		return 0, err
	}
	return alignSize(blocks*blockSize, 1024*1024), nil
}


func resizeExt4Image(imagePath string, targetBytes int64) error {
	targetBytes = alignSize(targetBytes, 1024*1024)
	if targetBytes < defaultStateSize {
		return fmt.Errorf("storage must be at least %s", formatBytesIEC(defaultStateSize))
	}
	info, err := os.Stat(imagePath)
	if err != nil {
		return err
	}
	currentBytes := info.Size()
	if targetBytes == currentBytes {
		return nil
	}
	if targetBytes < currentBytes {
		minimumBytes, err := ext4MinimumSizeBytes(imagePath)
		if err != nil {
			return err
		}
		if targetBytes < minimumBytes {
			return fmt.Errorf("storage cannot go below %s of used space", formatBytesIEC(minimumBytes))
		}
		if err := runCommand("resize2fs", imagePath, fmt.Sprintf("%dK", targetBytes/1024)); err != nil {
			return err
		}
		if err := os.Truncate(imagePath, targetBytes); err != nil {
			return err
		}
	} else {
		if err := os.Truncate(imagePath, targetBytes); err != nil {
			return err
		}
		if err := runCommand("e2fsck", "-fy", imagePath); err != nil {
			return err
		}
		if err := runCommand("resize2fs", imagePath); err != nil {
			return err
		}
	}
	return runCommand("e2fsck", "-fy", imagePath)
}

func isUsableNameserver(value string) bool {
	switch strings.TrimSpace(value) {
	case "", "127.0.0.1", "127.0.0.53", "::1":
		return false
	default:
		return true
	}
}

func createCgroup(name string, options StartOptions) (string, error) {
	if err := writeFileString(filepath.Join("/sys/fs/cgroup", "cgroup.subtree_control"), "+cpu +memory +pids +cpuset"); err != nil && !os.IsPermission(err) {
		// keep going; many systems already have the controllers enabled
	}
	path := filepath.Join("/sys/fs/cgroup", fmt.Sprintf("kiwi-%s-%d", sanitizeName(name), time.Now().UnixNano()))
	if err := ensureDir(path); err != nil {
		return "", err
	}
	if err := configureCPUSet(path, options.CPU); err != nil {
		return "", err
	}
	if options.Memory != "" {
		memoryBytes, err := parseMemory(options.Memory)
		if err != nil {
			return "", err
		}
		if memoryBytes > 0 {
			if err := writeFileString(filepath.Join(path, "memory.max"), fmt.Sprintf("%d", memoryBytes)); err != nil {
				return "", fmt.Errorf("set memory.max: %w", err)
			}
			_ = writeFileString(filepath.Join(path, "memory.high"), fmt.Sprintf("%d", memoryBytes))
			_ = writeFileString(filepath.Join(path, "memory.swap.max"), "0")
			_ = writeFileString(filepath.Join(path, "memory.oom.group"), "1")
		} else if memoryBytes < 0 {
			_ = writeFileString(filepath.Join(path, "memory.max"), "max")
			_ = writeFileString(filepath.Join(path, "memory.high"), "max")
			_ = writeFileString(filepath.Join(path, "memory.swap.max"), "max")
		}
	}
	cpu := options.CPU
	if !options.CPUHost && cpu <= 0 {
		cpu = 1
	}
	value := "max 100000"
	if !options.CPUHost {
		quota := cpu * 100000
		value = fmt.Sprintf("%d 100000", quota)
	}
	if err := writeFileString(filepath.Join(path, "cpu.max"), value); err != nil {
		return "", fmt.Errorf("set cpu.max: %w", err)
	}
	if err := writeFileString(filepath.Join(path, "pids.max"), fmt.Sprintf("%d", defaultPidsMax)); err != nil {
		return "", fmt.Errorf("set pids.max: %w", err)
	}
	return path, nil
}

func attachToCgroup(cgroupPath string, pid int) error {
	if strings.TrimSpace(cgroupPath) == "" {
		return nil
	}
	return writeFileString(filepath.Join(cgroupPath, "cgroup.procs"), fmt.Sprintf("%d", pid))
}

// attachSubtreeToCgroup walks /proc/<pid>/task/*/children and moves every
// reachable process into cgroupPath. When nsenter forks a shell, the
// child is created before our attachToCgroup runs, inheriting the
// launcher's cgroup instead of the container's — cpuset.cpus never
// applies and `nproc` reports the host's CPU count. Calling this once
// after a short settle delay plugs all the escapees back in.
func attachSubtreeToCgroup(cgroupPath string, pid int) {
	if strings.TrimSpace(cgroupPath) == "" || pid <= 0 {
		return
	}
	visited := map[int]bool{}
	var walk func(int)
	walk = func(p int) {
		if p <= 0 || visited[p] {
			return
		}
		visited[p] = true
		_ = attachToCgroup(cgroupPath, p)
		entries, err := os.ReadDir(fmt.Sprintf("/proc/%d/task", p))
		if err != nil {
			return
		}
		for _, entry := range entries {
			data, err := os.ReadFile(fmt.Sprintf("/proc/%d/task/%s/children", p, entry.Name()))
			if err != nil {
				continue
			}
			for _, f := range strings.Fields(string(data)) {
				if child, err := strconv.Atoi(f); err == nil {
					walk(child)
				}
			}
		}
	}
	walk(pid)
}

func runAttachedCommand(cmd *exec.Cmd, cgroupPath string) error {
	if err := cmd.Start(); err != nil {
		return err
	}
	if cgroupPath != "" {
		if err := attachToCgroup(cgroupPath, cmd.Process.Pid); err != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			return err
		}
	}
	return cmd.Wait()
}

func configureCPUSet(path string, requestedCPU int) error {
	parent := filepath.Dir(path)
	mems, err := readFirstNonEmptyFile(
		filepath.Join(parent, "cpuset.mems.effective"),
		filepath.Join(parent, "cpuset.mems"),
	)
	if err != nil {
		return err
	}
	cpus, err := readFirstNonEmptyFile(
		filepath.Join(parent, "cpuset.cpus.effective"),
		filepath.Join(parent, "cpuset.cpus"),
	)
	if err != nil {
		return err
	}
	if strings.TrimSpace(mems) == "" || strings.TrimSpace(cpus) == "" {
		return nil
	}
	if err := writeFileString(filepath.Join(path, "cpuset.mems"), mems); err != nil {
		if os.IsNotExist(err) || os.IsPermission(err) {
			return nil
		}
		return fmt.Errorf("set cpuset.mems: %w", err)
	}
	selected := strings.TrimSpace(cpus)
	if requestedCPU > 0 {
		selected, err = limitCPUSet(cpus, requestedCPU)
		if err != nil {
			return err
		}
	}
	if err := writeFileString(filepath.Join(path, "cpuset.cpus"), selected); err != nil {
		if os.IsNotExist(err) || os.IsPermission(err) {
			return nil
		}
		return fmt.Errorf("set cpuset.cpus: %w", err)
	}
	return nil
}

func readFirstNonEmptyFile(paths ...string) (string, error) {
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return "", err
		}
		value := strings.TrimSpace(string(data))
		if value != "" {
			return value, nil
		}
	}
	return "", nil
}

func limitCPUSet(raw string, requestedCPU int) (string, error) {
	available, err := parseCPUSet(raw)
	if err != nil {
		return "", err
	}
	if len(available) == 0 {
		return "", fmt.Errorf("cpuset %q is empty", raw)
	}
	if requestedCPU <= 0 || requestedCPU >= len(available) {
		return formatCPUSet(available), nil
	}
	return formatCPUSet(available[:requestedCPU]), nil
}

func parseCPUSet(raw string) ([]int, error) {
	parts := strings.Split(strings.TrimSpace(raw), ",")
	cpus := make([]int, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if strings.Contains(part, "-") {
			bounds := strings.SplitN(part, "-", 2)
			if len(bounds) != 2 {
				return nil, fmt.Errorf("invalid cpuset %q", raw)
			}
			start, err := strconv.Atoi(strings.TrimSpace(bounds[0]))
			if err != nil {
				return nil, err
			}
			end, err := strconv.Atoi(strings.TrimSpace(bounds[1]))
			if err != nil {
				return nil, err
			}
			if end < start {
				return nil, fmt.Errorf("invalid cpuset %q", raw)
			}
			for cpu := start; cpu <= end; cpu++ {
				cpus = append(cpus, cpu)
			}
			continue
		}
		cpu, err := strconv.Atoi(part)
		if err != nil {
			return nil, err
		}
		cpus = append(cpus, cpu)
	}
	return cpus, nil
}

func formatCPUSet(cpus []int) string {
	if len(cpus) == 0 {
		return ""
	}
	var builder strings.Builder
	start := cpus[0]
	prev := cpus[0]
	flush := func(first int, last int) {
		if builder.Len() > 0 {
			builder.WriteByte(',')
		}
		if first == last {
			builder.WriteString(strconv.Itoa(first))
			return
		}
		builder.WriteString(strconv.Itoa(first))
		builder.WriteByte('-')
		builder.WriteString(strconv.Itoa(last))
	}
	for _, cpu := range cpus[1:] {
		if cpu == prev+1 {
			prev = cpu
			continue
		}
		flush(start, prev)
		start = cpu
		prev = cpu
	}
	flush(start, prev)
	return builder.String()
}

func setupContainerNetwork(pid int, config ContainerConfig) (string, error) {
	if err := ensureBridge(); err != nil {
		return "", err
	}
	vethHost, vethGuest := containerLinkNames(config.Name)
	if err := cleanupContainerNetworkLinks(config.Name); err != nil {
		return "", err
	}
	if err := runCommand("ip", "link", "add", vethHost, "type", "veth", "peer", "name", vethGuest); err != nil {
		return "", err
	}
	if err := runCommand("ip", "link", "set", vethHost, "master", defaultBridgeName); err != nil {
		return "", err
	}
	if err := runCommand("ip", "link", "set", vethHost, "up"); err != nil {
		return "", err
	}
	if err := runCommand("ip", "link", "set", vethGuest, "netns", fmt.Sprintf("%d", pid)); err != nil {
		return "", err
	}
	for _, args := range [][]string{
		{"-t", fmt.Sprintf("%d", pid), "-n", "ip", "link", "set", "lo", "up"},
		{"-t", fmt.Sprintf("%d", pid), "-n", "ip", "link", "set", vethGuest, "name", "eth0"},
		{"-t", fmt.Sprintf("%d", pid), "-n", "ip", "addr", "add", config.IPv4 + "/24", "dev", "eth0"},
		{"-t", fmt.Sprintf("%d", pid), "-n", "ip", "link", "set", "eth0", "up"},
		{"-t", fmt.Sprintf("%d", pid), "-n", "ip", "route", "add", "default", "via", config.Gateway},
	} {
		if err := runCommand("nsenter", args...); err != nil {
			return "", err
		}
	}
	return vethHost, nil
}

func cleanupContainerNetworkLinks(name string) error {
	vethHost, vethGuest := containerLinkNames(name)
	for _, link := range []string{vethHost, vethGuest} {
		if link == "" || !isLinkPresent(link) {
			continue
		}
		if err := runCommand("ip", "link", "del", link); err != nil && isLinkPresent(link) {
			return err
		}
	}
	return nil
}

func ensureBridge() error {
	if !isLinkPresent(defaultBridgeName) {
		if err := runCommand("ip", "link", "add", defaultBridgeName, "type", "bridge"); err != nil {
			return err
		}
		if err := runCommand("ip", "addr", "add", defaultGateway+"/24", "dev", defaultBridgeName); err != nil {
			return err
		}
	}
	if err := runCommand("ip", "link", "set", defaultBridgeName, "up"); err != nil {
		return err
	}
	_ = writeFileString("/proc/sys/net/ipv4/ip_forward", "1")
	iface, err := defaultOutboundInterface()
	if err == nil && iface != "" {
		if err := runCommand("iptables", "-t", "nat", "-C", "POSTROUTING", "-s", defaultCIDR, "!", "-o", defaultBridgeName, "-j", "MASQUERADE"); err != nil {
			_ = runCommand("iptables", "-t", "nat", "-A", "POSTROUTING", "-s", defaultCIDR, "!", "-o", defaultBridgeName, "-j", "MASQUERADE")
		}
		if err := runCommand("iptables", "-t", "filter", "-C", "FORWARD", "-i", defaultBridgeName, "-j", "ACCEPT"); err != nil {
			_ = runCommand("iptables", "-t", "filter", "-A", "FORWARD", "-i", defaultBridgeName, "-j", "ACCEPT")
		}
		if err := runCommand("iptables", "-t", "filter", "-C", "FORWARD", "-o", defaultBridgeName, "-j", "ACCEPT"); err != nil {
			_ = runCommand("iptables", "-t", "filter", "-A", "FORWARD", "-o", defaultBridgeName, "-j", "ACCEPT")
		}
	}
	return nil
}

func defaultOutboundInterface() (string, error) {
	output, err := runCommandOutput("ip", "route", "show", "default")
	if err != nil {
		return "", err
	}
	fields := strings.Fields(output)
	for i := range fields {
		if fields[i] == "dev" && i+1 < len(fields) {
			return fields[i+1], nil
		}
	}
	return "", fmt.Errorf("default interface not found")
}

func isLinkPresent(name string) bool {
	_, err := runCommandOutput("ip", "link", "show", name)
	return err == nil
}

func shortID(name string) string {
	id := sanitizeName(name)
	if len(id) > 8 {
		id = id[:8]
	}
	return id
}

func containerLinkNames(name string) (string, string) {
	id := shortID(name)
	return fmt.Sprintf("kh%s", id), fmt.Sprintf("kg%s", id)
}

func runtimeRootfsPath(state RuntimeState) string {
	if state.PID > 0 && state.Running && processAlive(state.PID) {
		return filepath.Join("/proc", strconv.Itoa(state.PID), "root")
	}
	if state.MergedMountpoint != "" && isMounted(state.MergedMountpoint) {
		return state.MergedMountpoint
	}
	return ""
}

// getCachedArchiveState returns the path to the archive's extracted state/
// directory, extracting on the first call. The cache key mixes the archive
// path with its size and mtime, so re-snapping to the same filename
// invalidates the cache automatically.
func getCachedArchiveState(archivePath string, dataRoot string) (string, error) {
	if dataRoot == "" {
		exeDir := "."
		if executable, err := os.Executable(); err == nil {
			exeDir = filepath.Dir(executable)
		}
		dataRoot = filepath.Join(exeDir, "kiwi-data")
	}

	cacheDir := filepath.Join(dataRoot, "cache", "archive-state")
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		return "", err
	}

	info, err := os.Stat(archivePath)
	if err != nil {
		return "", err
	}
	keyMaterial := fmt.Sprintf("%s|%d|%d", archivePath, info.Size(), info.ModTime().UnixNano())
	hash := fmt.Sprintf("%x", sha256.Sum256([]byte(keyMaterial)))[:16]
	cachedPath := filepath.Join(cacheDir, hash)

	if pathExists(cachedPath) && pathExists(filepath.Join(cachedPath, "upper")) {
		return cachedPath, nil
	}

	if err := os.MkdirAll(cachedPath, 0755); err != nil {
		return "", err
	}

	if err := extractArchiveEntryToPath(archivePath, cachedPath, "state"); err != nil {
		os.RemoveAll(cachedPath)
		return "", err
	}

	return cachedPath, nil
}

func prepareOverlayDirs(image ImageManifest, config ContainerConfig, baseDir, stateDir string) (string, string, string, func(), error) {
	cleanup := func() {}

	// Fast path: the container was created from a squashfs .kiwi image.
	// Mount it once at a content-addressed location (shared across every
	// container from the same image) and layer its rootfs + upper directly.
	if config.LazyStateArchive != "" && pathExists(config.LazyStateArchive) && isSquashfsFile(config.LazyStateArchive) {
		mountPoint, err := ensureKiwiMountPath(config.LazyStateArchive)
		if err != nil {
			return "", "", "", func() {}, err
		}
		rootfs := filepath.Join(mountPoint, "rootfs")
		if !pathExists(rootfs) {
			return "", "", "", func() {}, fmt.Errorf(".kiwi image is missing rootfs/")
		}
		lowerDir := rootfs
		archiveUpper := filepath.Join(mountPoint, "state", "upper")
		if pathExists(archiveUpper) {
			lowerDir = archiveUpper + ":" + lowerDir
		}
		upperDir, workDir, stateCleanup, err := mountUpperStorage(config)
		if err != nil {
			return "", "", "", func() {}, err
		}
		return lowerDir, upperDir, workDir, stateCleanup, nil
	}

	lowerDir := image.RootfsPath
	if image.Format != "dir" && !isDirectoryPath(image.RootfsPath) {
		if err := ensureDir(baseDir); err != nil {
			return "", "", "", cleanup, err
		}
		if !isMounted(baseDir) {
			if err := extractSquashfs(image.RootfsPath, baseDir); err != nil {
				return "", "", "", cleanup, err
			}
		}
		lowerDir = baseDir
	}

	// Legacy tar.gz archives: stack the archived upper via the layer cache.
	if config.LazyStateArchive != "" && pathExists(config.LazyStateArchive) {
		cachedState, err := getCachedArchiveState(config.LazyStateArchive, "")
		if err != nil {
			cleanup()
			return "", "", "", func() {}, err
		}
		archiveUpper := filepath.Join(cachedState, "upper")
		if pathExists(archiveUpper) {
			lowerDir = archiveUpper + ":" + lowerDir
		}
	}

	if config.StateBackend != "dir" && !isDirectoryPath(config.StatePath) {
		return "", "", "", func() {}, fmt.Errorf("container %q uses a legacy ext4-image state backend; recreate it with the current kiwi to migrate to the directory backend", config.Name)
	}

	upperDir, workDir, stateCleanup, err := mountUpperStorage(config)
	if err != nil {
		return "", "", "", func() {}, err
	}
	return lowerDir, upperDir, workDir, stateCleanup, nil
}

// mountUpperStorage creates the container's writable overlay layers on the
// host ext4 filesystem. Storage caps here are kiwi-native (soft watcher
// via watchContainerStorageLimit), not kernel-enforced — Linux has no
// universal way to do a true per-directory ENOSPC without either loop
// devices (fail on Codespaces) or FUSE (incompatible with overlayfs
// TMPFILE requirement). Memory and CPU limits remain fully kernel-enforced
// via cgroups.
func mountUpperStorage(config ContainerConfig) (string, string, func(), error) {
	cleanup := func() {}
	upperDir := filepath.Join(config.StatePath, "upper")
	workDir := filepath.Join(config.StatePath, "work")
	for _, dir := range []string{config.StatePath, upperDir, workDir} {
		if err := ensureDir(dir); err != nil {
			return "", "", cleanup, err
		}
	}
	return upperDir, workDir, cleanup, nil
}

func minimumAllowedStorage(config ContainerConfig) (int64, error) {
	minimum := alignSize(defaultStateSize, 1024*1024)
	if config.StateBackend == "dir" || isDirectoryPath(config.StatePath) {
		usedBytes, err := dirUsageBytes(filepath.Join(config.StatePath, "upper"))
		if err != nil && !os.IsNotExist(err) {
			return 0, err
		}
		required := alignSize(usedBytes+storageSafetyMargin, 1024*1024)
		if required > minimum {
			minimum = required
		}
		return minimum, nil
	}
	usedBytes, err := ext4MinimumSizeBytes(config.StatePath)
	if err != nil {
		return 0, err
	}
	required := alignSize(usedBytes+storageSafetyMargin, 1024*1024)
	if required > minimum {
		minimum = required
	}
	return minimum, nil
}

// watchContainerStorageLimit polls the container's upper directory once a
// second and SIGTERMs the container when usage exceeds the configured
// storage cap. This is a soft limit: a burst write between polls can
// temporarily exceed it. Linux has no universal way to do a hard
// per-directory ENOSPC without loop devices (unavailable on Codespaces
// and many sandboxed hosts) or FUSE (incompatible with overlayfs). We
// prefer this pragmatic approach over failing to start entirely.
func watchContainerStorageLimit(config ContainerConfig, pid int) {
	if pid <= 0 {
		return
	}
	if config.StateSizeHost {
		return
	}
	if !(config.StateBackend == "dir" || isDirectoryPath(config.StatePath)) {
		return
	}
	upperDir := filepath.Join(config.StatePath, "upper")
	limit := config.StateSizeBytes
	if limit < defaultStateSize {
		limit = defaultStateSize
	}
	go func() {
		for processAlive(pid) {
			usedBytes, err := dirUsageBytes(upperDir)
			if err == nil && usedBytes > limit {
				fmt.Fprintf(os.Stderr, "kiwi: container %s exceeded storage limit (%s > %s)\n", config.Name, formatBytesIEC(usedBytes), formatBytesIEC(limit))
				_ = syscall.Kill(pid, syscall.SIGTERM)
				waitForExit(pid, 3)
				if processAlive(pid) {
					_ = syscall.Kill(pid, syscall.SIGKILL)
				}
				return
			}
			timeSleepMillis(1000)
		}
	}()
}

func (s Store) ensureContainerDirectoryBackend(name string) (ContainerConfig, ImageManifest, error) {
	config, err := s.LoadContainer(name)
	if err != nil {
		return ContainerConfig{}, ImageManifest{}, err
	}
	// If the container was created from a squashfs .kiwi image, the rootfs
	// lives inside the mounted archive. We don't need a local base image
	// entry in the store for it — the .kiwi is fully self-contained.
	var image ImageManifest
	if config.LazyStateArchive != "" && pathExists(config.LazyStateArchive) && isSquashfsFile(config.LazyStateArchive) {
		mountPoint, err := ensureKiwiMountPath(config.LazyStateArchive)
		if err != nil {
			return ContainerConfig{}, ImageManifest{}, err
		}
		image = ImageManifest{
			Name:       config.Image,
			RootfsPath: filepath.Join(mountPoint, "rootfs"),
			Format:     "dir",
		}
	} else {
		image, err = s.ensureImageDirectoryBackend(config.Image)
		if err != nil {
			return ContainerConfig{}, ImageManifest{}, err
		}
	}
	targetState := s.ContainerStatePath(config.Name)
	if (config.StateBackend == "dir" || isDirectoryPath(config.StatePath)) && config.StatePath == targetState && isDirectoryPath(targetState) {
		return config, image, nil
	}
	if isDirectoryPath(targetState) && config.StatePath != targetState {
		config.StatePath = targetState
		config.StateBackend = "dir"
		if err := s.SaveContainer(config); err != nil {
			return ContainerConfig{}, ImageManifest{}, err
		}
		return config, image, nil
	}
	if !isDirectoryPath(config.StatePath) {
		return ContainerConfig{}, ImageManifest{}, fmt.Errorf("container %q has a legacy ext4-image state backend; recreate it with the current kiwi to migrate to the directory backend", config.Name)
	}
	if err := copyPath(filepath.Join(config.StatePath, "."), targetState); err != nil {
		return ContainerConfig{}, ImageManifest{}, err
	}
	for _, dir := range []string{filepath.Join(targetState, "upper"), filepath.Join(targetState, "work")} {
		if err := ensureDir(dir); err != nil {
			return ContainerConfig{}, ImageManifest{}, err
		}
	}
	config.StatePath = targetState
	config.StateBackend = "dir"
	if err := s.SaveContainer(config); err != nil {
		return ContainerConfig{}, ImageManifest{}, err
	}
	maybeChownPaths(
		s.ContainerDir(config.Name),
		s.ContainerConfigPath(config.Name),
		s.ContainerSnapshotsDir(config.Name),
		s.ContainerSessionsDir(config.Name),
	)
	return config, image, nil
}

func mountContainerRootfs(imagePath, statePath, archivePath, hostname, ipv4 string) (string, error) {
	if imagePath == "" || statePath == "" {
		return "", fmt.Errorf("both image and state paths are required")
	}
	if err := mountPath("tmpfs", "/tmp", "tmpfs", "mode=1777"); err != nil {
		return "", fmt.Errorf("mount tmpfs on /tmp: %w", err)
	}
	workspace, err := os.MkdirTemp("", "kiwi-root-")
	if err != nil {
		return "", err
	}
	mergedDir := filepath.Join(workspace, "merged")
	for _, dir := range []string{filepath.Join(workspace, "base"), filepath.Join(workspace, "state"), mergedDir} {
		if err := ensureDir(dir); err != nil {
			return "", err
		}
	}
	image := ImageManifest{RootfsPath: imagePath}
	if isDirectoryPath(imagePath) {
		image.Format = "dir"
	}
	config := ContainerConfig{StatePath: statePath, LazyStateArchive: archivePath}
	if isDirectoryPath(statePath) {
		config.StateBackend = "dir"
	}
	lowerDir, upperDir, workDir, cleanup, err := prepareOverlayDirs(image, config, filepath.Join(workspace, "base"), filepath.Join(workspace, "state"))
	if err != nil {
		return "", err
	}
	if err := mountOverlayPath(lowerDir, upperDir, workDir, mergedDir); err != nil {
		cleanup()
		return "", fmt.Errorf("mount overlay: %w", err)
	}
	if err := prepareRootfsFiles(mergedDir, hostname, ipv4); err != nil {
		cleanup()
		return "", err
	}
	return mergedDir, nil
}

func EnterContainer(root, imagePath, statePath, archivePath, name, ipv4 string, syncFD int, command []string) error {
	if syncFD > 0 {
		file := os.NewFile(uintptr(syncFD), "sync")
		buf := make([]byte, 1)
		_, err := file.Read(buf)
		file.Close()
		if err != nil {
			return fmt.Errorf("startup sync failed: %w", err)
		}
	}
	if err := makeMountsPrivate("/"); err != nil {
		return err
	}
	// Set the container hostname in the new UTS namespace as early as
	// possible. nsenter in later attach sessions reads the UTS namespace
	// value, so delaying this until after pivotRoot introduced a race
	// where the first shell prompt inherited the host's hostname.
	if err := syscall.Sethostname([]byte(name)); err != nil {
		return err
	}
	if root == "" {
		var err error
		root, err = mountContainerRootfs(imagePath, statePath, archivePath, name, ipv4)
		if err != nil {
			return err
		}
	}
	if err := setupDev(root); err != nil {
		return err
	}
	if err := setupRuntimeMounts(root); err != nil {
		return err
	}
	if err := pivotRoot(root); err != nil {
		return err
	}
	if os.Getenv("PATH") == "" {
		_ = os.Setenv("PATH", "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
	}
	return runInit(command)
}

func setupRuntimeMounts(root string) error {
	for _, mountSpec := range []struct {
		target string
		fstype string
		source string
		data   []string
	}{
		{target: "/proc", fstype: "proc", source: "proc"},
		{target: "/sys", fstype: "sysfs", source: "sysfs"},
		// No tmpfs size cap: the container's cgroup memory.max already
		// bounds total RAM usage, so a second cap would just cause
		// confusing ENOSPC errors inside perfectly healthy containers.
		{target: "/run", fstype: "tmpfs", source: "tmpfs", data: []string{"mode=755"}},
		{target: "/tmp", fstype: "tmpfs", source: "tmpfs", data: []string{"mode=1777"}},
	} {
		target := filepath.Join(root, strings.TrimPrefix(mountSpec.target, "/"))
		if err := ensureDir(target); err != nil {
			return err
		}
		if isMounted(target) {
			continue
		}
		if err := mountPath(mountSpec.source, target, mountSpec.fstype, mountSpec.data...); err != nil {
			return fmt.Errorf("mount %s: %w", target, err)
		}
	}
	return nil
}

func pivotRoot(root string) error {
	root = filepath.Clean(root)
	if err := recursiveBindMountPath(root, root); err != nil {
		return fmt.Errorf("bind new root: %w", err)
	}
	oldRoot := filepath.Join(root, ".old_root")
	if err := ensureDir(oldRoot); err != nil {
		return err
	}
	if err := syscall.PivotRoot(root, oldRoot); err != nil {
		return fmt.Errorf("pivot_root: %w", err)
	}
	if err := os.Chdir("/"); err != nil {
		return err
	}
	if err := runCommand("umount", "-l", "/.old_root"); err != nil {
		return err
	}
	return os.RemoveAll("/.old_root")
}

func setupDev(root string) error {
	devRoot := filepath.Join(root, "dev")
	if err := ensureDir(devRoot); err != nil {
		return err
	}
	if !isMounted(devRoot) {
		if err := mountPath("tmpfs", devRoot, "tmpfs", "mode=755"); err != nil {
			return err
		}
	}
	devices := []struct {
		path  string
		mode  uint32
		major uint32
		minor uint32
	}{
		{"/dev/null", syscall.S_IFCHR | 0666, 1, 3},
		{"/dev/zero", syscall.S_IFCHR | 0666, 1, 5},
		{"/dev/random", syscall.S_IFCHR | 0666, 1, 8},
		{"/dev/urandom", syscall.S_IFCHR | 0666, 1, 9},
		{"/dev/tty", syscall.S_IFCHR | 0666, 5, 0},
	}
	for _, device := range devices {
		path := filepath.Join(devRoot, strings.TrimPrefix(device.path, "/dev/"))
		_ = os.Remove(path)
		if err := syscall.Mknod(path, device.mode, makedev(device.major, device.minor)); err != nil {
			return err
		}
	}
	for _, dir := range []string{"pts", "shm"} {
		if err := ensureDir(filepath.Join(devRoot, dir)); err != nil {
			return err
		}
	}
	ptsPath := filepath.Join(devRoot, "pts")
	if !isMounted(ptsPath) {
		if err := mountPath("devpts", ptsPath, "devpts", "newinstance", "ptmxmode=0666", "mode=620"); err != nil {
			return err
		}
	}
	shmPath := filepath.Join(devRoot, "shm")
	if !isMounted(shmPath) {
		if err := mountPath("tmpfs", shmPath, "tmpfs", "mode=1777"); err != nil {
			return err
		}
	}
	for _, link := range []struct {
		path   string
		target string
	}{
		{path: filepath.Join(devRoot, "ptmx"), target: "pts/ptmx"},
		{path: filepath.Join(devRoot, "fd"), target: "/proc/self/fd"},
		{path: filepath.Join(devRoot, "stdin"), target: "/proc/self/fd/0"},
		{path: filepath.Join(devRoot, "stdout"), target: "/proc/self/fd/1"},
		{path: filepath.Join(devRoot, "stderr"), target: "/proc/self/fd/2"},
	} {
		_ = os.Remove(link.path)
		_ = os.Symlink(link.target, link.path)
	}
	return nil
}

func runInit(command []string) error {
	var mainCmd *exec.Cmd
	if len(command) > 0 {
		binary := command[0]
		if !strings.Contains(binary, "/") {
			resolved, err := exec.LookPath(binary)
			if err != nil {
				return err
			}
			binary = resolved
		}
		mainCmd = exec.Command(binary, command[1:]...)
		mainCmd.Stdin = os.Stdin
		mainCmd.Stdout = os.Stdout
		mainCmd.Stderr = os.Stderr
		mainCmd.Env = os.Environ()
		if err := mainCmd.Start(); err != nil {
			return err
		}
	}
	sigc := make(chan os.Signal, 8)
	signalNotify(sigc)
	mainPID := 0
	if mainCmd != nil && mainCmd.Process != nil {
		mainPID = mainCmd.Process.Pid
	}
	for {
		select {
		case sig := <-sigc:
			switch sig {
			case syscall.SIGCHLD:
				for {
					var status syscall.WaitStatus
					pid, err := syscall.Wait4(-1, &status, syscall.WNOHANG, nil)
					if pid <= 0 || err != nil {
						break
					}
					if mainPID != 0 && pid == mainPID {
						if status.Exited() {
							os.Exit(status.ExitStatus())
						}
						os.Exit(1)
					}
				}
			default:
				if mainCmd != nil && mainCmd.Process != nil {
					_ = mainCmd.Process.Signal(sig.(syscall.Signal))
				}
				if sig == syscall.SIGTERM || sig == syscall.SIGINT {
					os.Exit(0)
				}
			}
		}
	}
}

func signalNotify(ch chan os.Signal) {
	signal.Notify(ch, syscall.SIGCHLD, syscall.SIGTERM, syscall.SIGINT)
}
