package kiwi

import (
	"archive/tar"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const storageSafetyMargin = int64(1024 * 1024 * 1024)

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func expandHomeDir(path string) string {
	if path == "" {
		return path
	}
	if path == "~" || strings.HasPrefix(path, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return path
		}
		if path == "~" {
			return home
		}
		return filepath.Join(home, path[2:])
	}
	return path
}

func sanitizeName(name string) string {
	if name == "" {
		return ""
	}
	var builder strings.Builder
	for _, r := range strings.ToLower(name) {
		switch {
		case r >= 'a' && r <= 'z':
			builder.WriteRune(r)
		case r >= '0' && r <= '9':
			builder.WriteRune(r)
		case r == '-', r == '_', r == '.':
			builder.WriteRune(r)
		default:
			builder.WriteByte('-')
		}
	}
	return strings.Trim(builder.String(), "-._")
}

func defaultContainerConfigValues(config *ContainerConfig) {
	if strings.TrimSpace(config.Hostname) == "" {
		config.Hostname = sanitizeName(config.Name)
	}
	if config.Memory == "" {
		config.Memory = defaultMemory
	}
	if !config.CPUHost && config.CPU <= 0 {
		config.CPU = defaultCPU
	}
	config.Network = normalizeNetworkMode(config.Network)
	if config.Network == "" {
		config.Network = defaultNetwork
	}
	if config.StateBackend == "" {
		config.StateBackend = "image"
	}
}

func effectiveHostname(config ContainerConfig) string {
	return sanitizeName(config.Name)
}

func isDirectoryPath(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

func displayMemory(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return defaultMemory + "M"
	}
	if strings.EqualFold(trimmed, "host") {
		return "host"
	}
	if strings.IndexFunc(trimmed, func(r rune) bool { return r < '0' || r > '9' }) == -1 {
		return trimmed + "M"
	}
	return strings.ToUpper(trimmed)
}

func displayCPU(value int, host bool) string {
	if host {
		return "host"
	}
	if value <= 0 {
		return strconv.Itoa(defaultCPU)
	}
	return strconv.Itoa(value)
}

func displayShell(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "auto"
	}
	return trimmed
}

func displayNetwork(value string) string {
	mode := normalizeNetworkMode(value)
	if mode == "" {
		return defaultNetwork
	}
	return mode
}

func normalizeNetworkMode(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", defaultNetwork, "private", "isolated":
		return defaultNetwork
	case "host":
		return "host"
	default:
		return strings.ToLower(strings.TrimSpace(value))
	}
}

func validateNetworkMode(value string) error {
	switch normalizeNetworkMode(value) {
	case "host", defaultNetwork:
		return nil
	default:
		return fmt.Errorf("network must be host or %s", defaultNetwork)
	}
}

func generateContainerID() (string, error) {
	buf := make([]byte, 12)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

func generateSessionID() (string, error) {
	buf := make([]byte, 6)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

func extLabel(prefix, name string) string {
	label := sanitizeName(prefix + "-" + name)
	if len(label) <= 16 {
		return label
	}
	return label[:16]
}

func directoryUsage(root string) (int64, error) {
	var total int64
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		total += info.Size()
		if info.IsDir() {
			total += 4096
		}
		return nil
	})
	return total, err
}

func suggestedImageSize(usedBytes int64, minimum int64) int64 {
	size := usedBytes + usedBytes/3 + 128*1024*1024
	if size < minimum {
		size = minimum
	}
	return alignSize(size, 1024*1024)
}

func parseSize(value string) (int64, error) {
	trimmed := strings.TrimSpace(strings.ToLower(value))
	if trimmed == "" {
		return 0, fmt.Errorf("empty size")
	}
	var multiplier int64 = 1
	suffixes := map[string]int64{
		"k":   1024,
		"kb":  1024,
		"kib": 1024,
		"m":   1024 * 1024,
		"mb":  1024 * 1024,
		"mib": 1024 * 1024,
		"g":   1024 * 1024 * 1024,
		"gb":  1024 * 1024 * 1024,
		"gib": 1024 * 1024 * 1024,
		"t":   1024 * 1024 * 1024 * 1024,
		"tb":  1024 * 1024 * 1024 * 1024,
		"tib": 1024 * 1024 * 1024 * 1024,
	}
	for suffix, candidate := range suffixes {
		if strings.HasSuffix(trimmed, suffix) {
			multiplier = candidate
			trimmed = strings.TrimSuffix(trimmed, suffix)
			break
		}
	}
	base, err := strconv.ParseFloat(strings.TrimSpace(trimmed), 64)
	if err != nil {
		return 0, err
	}
	return int64(base * float64(multiplier)), nil
}

func parseMemory(value string) (int64, error) {
	trimmed := strings.TrimSpace(strings.ToLower(value))
	if trimmed == "" {
		return 0, nil
	}
	if trimmed == "host" || trimmed == "max" || trimmed == "unlimited" {
		return -1, nil
	}
	if strings.IndexFunc(trimmed, func(r rune) bool { return r < '0' || r > '9' }) == -1 {
		trimmed += "m"
	}
	return parseSize(trimmed)
}

func runCommand(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		trimmed := strings.TrimSpace(string(output))
		if trimmed == "" {
			return fmt.Errorf("%s %s failed: %w", name, strings.Join(args, " "), err)
		}
		return fmt.Errorf("%s %s failed: %w: %s", name, strings.Join(args, " "), err, trimmed)
	}
	return nil
}

func runCommandOutput(name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		trimmed := strings.TrimSpace(string(output))
		if trimmed == "" {
			return "", fmt.Errorf("%s %s failed: %w", name, strings.Join(args, " "), err)
		}
		return "", fmt.Errorf("%s %s failed: %w: %s", name, strings.Join(args, " "), err, trimmed)
	}
	return strings.TrimSpace(string(output)), nil
}

func mountPath(source, target, fstype string, options ...string) error {
	args := make([]string, 0, 6+len(options))
	if fstype != "" {
		args = append(args, "-t", fstype)
	}
	filtered := make([]string, 0, len(options))
	for _, option := range options {
		option = strings.TrimSpace(option)
		if option != "" {
			filtered = append(filtered, option)
		}
	}
	if len(filtered) > 0 {
		args = append(args, "-o", strings.Join(filtered, ","))
	}
	args = append(args, source, target)
	return runCommand("mount", args...)
}

func bindMountPath(source, target string) error {
	return runCommand("mount", "--bind", source, target)
}

func recursiveBindMountPath(source, target string) error {
	return runCommand("mount", "--rbind", source, target)
}

func makeMountsPrivate(target string) error {
	return runCommand("mount", "--make-rprivate", target)
}

func mountOverlayPath(lower, upper, work, target string) error {
	return mountPath("overlay", target, "overlay",
		"lowerdir="+lower,
		"upperdir="+upper,
		"workdir="+work,
	)
}

func writeFileString(path, value string) error {
	return os.WriteFile(path, []byte(value), 0644)
}

func createSizedFile(path string, size int64) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Truncate(size)
}

func copyPath(src, dst string) error {
	if isDirectoryPath(src) {
		if err := os.RemoveAll(dst); err != nil && !os.IsNotExist(err) {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
			return err
		}
		// Try reflink first (COW copy, instant on btrfs/xfs), fallback to regular copy
		if err := runCommand("cp", "--reflink=auto", "-a", src, dst); err == nil {
			return nil
		}
		return fmt.Errorf("copy directory %s -> %s failed", src, dst)
	}
	return copyFile(src, dst)
}

func copyFile(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}
	if err := runCommand("cp", "--sparse=always", "--reflink=auto", src, dst); err == nil {
		return nil
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Sync()
}

func maxInt64(values ...int64) int64 {
	var max int64
	for index, value := range values {
		if index == 0 || value > max {
			max = value
		}
	}
	return max
}

func alignSize(size int64, boundary int64) int64 {
	if size <= 0 || boundary <= 0 {
		return size
	}
	if rem := size % boundary; rem != 0 {
		size += boundary - rem
	}
	return size
}

func fileSHA256(path string) (string, error) {
	if isDirectoryPath(path) {
		return dirSHA256(path)
	}
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func dirUsageBytes(root string) (int64, error) {
	var total int64
	if err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			total += info.Size()
		}
		return nil
	}); err != nil {
		return 0, err
	}
	return total, nil
}

func dirSHA256(root string) (string, error) {
	hash := sha256.New()
	if err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if rel == "." {
			rel = ""
		}
		if _, err := io.WriteString(hash, rel+"\n"); err != nil {
			return err
		}
		if _, err := io.WriteString(hash, info.Mode().String()+"\n"); err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			file, err := os.Open(path)
			if err != nil {
				return err
			}
			if _, err := io.Copy(hash, file); err != nil {
				file.Close()
				return err
			}
			if err := file.Close(); err != nil {
				return err
			}
		} else if info.Mode()&os.ModeSymlink != 0 {
			target, err := os.Readlink(path)
			if err != nil {
				return err
			}
			if _, err := io.WriteString(hash, target); err != nil {
				return err
			}
		}
		if _, err := io.WriteString(hash, "\n"); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	if syscall.Kill(pid, 0) != nil {
		return false
	}
	data, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		return false
	}
	fields := strings.Fields(string(data))
	if len(fields) < 3 {
		return false
	}
	return fields[2] != "Z"
}

func makedev(major, minor uint32) int {
	return int(((major & 0xfff) << 8) | (minor & 0xff) | ((minor & 0xfffff00) << 12))
}

func downloadFile(url, dst string) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download %s: unexpected status %s", url, resp.Status)
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer out.Close()
	if _, err := io.Copy(out, resp.Body); err != nil {
		return err
	}
	return out.Sync()
}

func addFileToTar(tw *tar.Writer, archivePath, src string, mode int64) error {
	file, err := os.Open(src)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	header := &tar.Header{Name: archivePath, Mode: mode, Size: info.Size()}
	if err := tw.WriteHeader(header); err != nil {
		return err
	}
	_, err = io.Copy(tw, file)
	return err
}

func addBytesToTar(tw *tar.Writer, archivePath string, data []byte, mode int64) error {
	header := &tar.Header{Name: archivePath, Mode: mode, Size: int64(len(data))}
	if err := tw.WriteHeader(header); err != nil {
		return err
	}
	_, err := tw.Write(data)
	return err
}

func writeJSONBytes(value interface{}) ([]byte, error) {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func pathExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func isDirEmpty(path string) bool {
	entries, err := os.ReadDir(path)
	if err != nil {
		return true
	}
	return len(entries) == 0
}

func isLoopMountError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "loop device") || strings.Contains(message, "failed to setup loop device") || strings.Contains(message, "operation not permitted")
}

func maybeChownPath(path string) {
	uidValue := os.Getenv("SUDO_UID")
	gidValue := os.Getenv("SUDO_GID")
	if uidValue == "" || gidValue == "" {
		return
	}
	uid, err := strconv.Atoi(uidValue)
	if err != nil {
		return
	}
	gid, err := strconv.Atoi(gidValue)
	if err != nil {
		return
	}
	_ = os.Chown(path, uid, gid)
}

func maybeChownPaths(paths ...string) {
	for _, path := range paths {
		if strings.TrimSpace(path) == "" {
			continue
		}
		maybeChownPath(path)
	}
}

func maybeChownFile(path string) {
	maybeChownPath(path)
}

func waitForExit(pid int, timeoutSeconds int) {
	for i := 0; i < timeoutSeconds*10; i++ {
		if !processAlive(pid) {
			return
		}
		timeSleepMillis(100)
	}
}

func timeSleepMillis(ms int) {
	time.Sleep(time.Duration(ms) * time.Millisecond)
}

func isMounted(target string) bool {
	data, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return false
	}
	targets := map[string]bool{filepath.Clean(target): true}
	if resolved := resolvePath(target); resolved != "" {
		targets[resolved] = true
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) > 4 && targets[fields[4]] {
			return true
		}
	}
	return false
}

func ensureDir(path string) error {
	return os.MkdirAll(path, 0755)
}

func unmountPath(path string) error {
	if path == "" || !isMounted(path) {
		return nil
	}
	resolved := resolvePath(path)
	if resolved == "" {
		resolved = path
	}
	if err := runCommand("umount", resolved); err == nil {
		return nil
	}
	if !isMounted(resolved) {
		return nil
	}
	return runCommand("umount", "-l", resolved)
}

func unmountTreePath(path string) error {
	if path == "" || !isMounted(path) {
		return nil
	}
	resolved := resolvePath(path)
	if resolved == "" {
		resolved = path
	}
	if err := runCommand("umount", "-R", resolved); err == nil {
		return nil
	}
	if !isMounted(resolved) {
		return nil
	}
	if err := runCommand("umount", "-l", resolved); err == nil {
		return nil
	}
	if !isMounted(resolved) {
		return nil
	}
	return runCommand("umount", "-l", "-R", resolved)
}

func resolvePath(path string) string {
	if path == "" {
		return ""
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err == nil {
		return filepath.Clean(resolved)
	}
	parent := filepath.Dir(path)
	resolvedParent, parentErr := filepath.EvalSymlinks(parent)
	if parentErr != nil {
		return ""
	}
	return filepath.Join(filepath.Clean(resolvedParent), filepath.Base(path))
}

func splitArgsOnDoubleDash(args []string) ([]string, []string) {
	for index, arg := range args {
		if arg == "--" {
			return args[:index], args[index+1:]
		}
	}
	return args, nil
}
