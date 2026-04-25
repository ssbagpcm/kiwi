package kiwi

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)


type archiveData struct {
	Path      string
	Workspace string
	Meta      archiveEnvelope
	Manifest  ImageManifest
	Config    *ContainerConfig
}

// tarExtractXattrFlags keeps overlayfs whiteouts / opaque markers intact
// when we extract container state from a .kiwi archive. Without these flags
// `trusted.overlay.*` xattrs are dropped and deleted files resurface.
var tarExtractXattrFlags = []string{"--xattrs", "--xattrs-include=*"}

func extractArchiveEntries(archivePath, workspace string, entries ...string) error {
	args := append([]string{"-xzf", archivePath, "-C", workspace, "--occurrence=1"}, tarExtractXattrFlags...)
	args = append(args, entries...)
	return runCommand("tar", args...)
}

func extractArchiveEntryToPath(archivePath, destination, entry string) error {
	if strings.TrimSpace(destination) == "" {
		return fmt.Errorf("archive destination is required")
	}
	parent := filepath.Dir(destination)
	if err := os.MkdirAll(parent, 0755); err != nil {
		return err
	}
	base := filepath.Base(destination)
	tempFilePath := filepath.Join(parent, entry)
	_ = os.RemoveAll(destination)
	if tempFilePath != destination {
		_ = os.RemoveAll(tempFilePath)
	}
	args := append([]string{"-xzf", archivePath, "-C", parent, "--occurrence=1"}, tarExtractXattrFlags...)
	args = append(args, entry)
	if err := runCommand("tar", args...); err != nil {
		return err
	}
	if entry == base {
		return nil
	}
	if err := os.RemoveAll(destination); err != nil && !os.IsNotExist(err) {
		return err
	}
	return os.Rename(tempFilePath, destination)
}

// readArchiveSmallEntries reads a set of small-file entries (meta.json,
// manifest.json, config.json) from a .kiwi tarball in a single gzip pass,
// stopping as soon as every requested entry has been found. This replaces
// calling `tar -xzf` once per entry, which forced a full gzip decode of the
// whole (GB-scale) archive just to locate a few hundred bytes.
func readArchiveSmallEntries(archivePath string, wanted map[string]bool) (map[string][]byte, error) {
	f, err := os.Open(archivePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	remaining := len(wanted)
	result := make(map[string][]byte, remaining)
	for remaining > 0 {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		name := strings.TrimPrefix(hdr.Name, "./")
		if !wanted[name] {
			continue
		}
		buf, err := io.ReadAll(tr)
		if err != nil {
			return nil, err
		}
		result[name] = buf
		remaining--
	}
	return result, nil
}

// peekArchiveImageName reads only the meta.json header from a .kiwi archive to get
// the image name and kind, without extracting any files. Returns instantly.
func peekArchiveImageName(archivePath string) (string, string, error) {
	entries, err := readArchiveSmallEntries(archivePath, map[string]bool{"meta.json": true})
	if err != nil {
		return "", "", err
	}
	raw, ok := entries["meta.json"]
	if !ok {
		return "", "", fmt.Errorf("meta.json not found in archive")
	}
	var meta archiveEnvelope
	if err := json.Unmarshal(raw, &meta); err != nil {
		return "", "", err
	}
	imageName := meta.ImageName
	if imageName == "" {
		imageName = meta.Container
	}
	return imageName, meta.Kind, nil
}

func loadArchiveMetadata(archivePath string) (archiveData, func(), error) {
	workspace, err := os.MkdirTemp("", "kiwi-load-")
	if err != nil {
		return archiveData{}, nil, err
	}
	cleanup := func() {
		_ = os.RemoveAll(workspace)
	}
	// Single gzip pass: read meta, manifest, and (optionally) config in one
	// sweep. Stops as soon as all three headers have been seen, so we never
	// decompress the GB-sized rootfs / state payload just to parse JSON.
	entries, err := readArchiveSmallEntries(archivePath, map[string]bool{
		"meta.json":     true,
		"manifest.json": true,
		"config.json":   true,
	})
	if err != nil {
		cleanup()
		return archiveData{}, nil, err
	}
	metaData, ok := entries["meta.json"]
	if !ok {
		cleanup()
		return archiveData{}, nil, fmt.Errorf("meta.json not found in archive")
	}
	var meta archiveEnvelope
	if err := json.Unmarshal(metaData, &meta); err != nil {
		cleanup()
		return archiveData{}, nil, err
	}
	manifestData, ok := entries["manifest.json"]
	if !ok {
		cleanup()
		return archiveData{}, nil, fmt.Errorf("manifest.json not found in archive")
	}
	var manifest ImageManifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		cleanup()
		return archiveData{}, nil, err
	}
	data := archiveData{
		Path:      archivePath,
		Workspace: workspace,
		Meta:      meta,
		Manifest:  manifest,
	}
	if meta.Kind == "container" {
		configData, ok := entries["config.json"]
		if !ok {
			cleanup()
			return archiveData{}, nil, fmt.Errorf("config.json not found in archive")
		}
		var config ContainerConfig
		if err := json.Unmarshal(configData, &config); err != nil {
			cleanup()
			return archiveData{}, nil, err
		}
		data.Config = &config
	}
	return data, cleanup, nil
}

func (d *archiveData) extractRootfs() error {
	if d.Manifest.RootfsPath != "" && strings.HasPrefix(d.Manifest.RootfsPath, d.Workspace+string(os.PathSeparator)) {
		return nil
	}
	d.Manifest.RootfsPath = ""
	if err := extractArchiveEntries(d.Path, d.Workspace, "rootfs"); err == nil {
		d.Manifest.RootfsPath = filepath.Join(d.Workspace, "rootfs")
		d.Manifest.Format = "dir"
		return nil
	}
	if err := extractArchiveEntries(d.Path, d.Workspace, "rootfs.squashfs"); err != nil {
		return fmt.Errorf("archive image is missing rootfs")
	}
	d.Manifest.RootfsPath = filepath.Join(d.Workspace, "rootfs.squashfs")
	return nil
}

func (d *archiveData) extractState() error {
	if d.Config == nil {
		return nil
	}
	if strings.HasPrefix(d.Config.StatePath, d.Workspace+string(os.PathSeparator)) {
		return nil
	}
	d.Config.StatePath = ""
	if err := extractArchiveEntries(d.Path, d.Workspace, "state"); err == nil {
		d.Config.StatePath = filepath.Join(d.Workspace, "state")
		d.Config.StateBackend = "dir"
		return nil
	}
	if err := extractArchiveEntries(d.Path, d.Workspace, "state.img"); err != nil {
		return fmt.Errorf("container archive is missing state")
	}
	d.Config.StatePath = filepath.Join(d.Workspace, "state.img")
	return nil
}

func sameArchiveImageIdentity(a, b ImageManifest) bool {
	return sanitizeName(a.Name) == sanitizeName(b.Name) &&
		strings.TrimSpace(a.Format) == strings.TrimSpace(b.Format) &&
		strings.TrimSpace(a.Source) == strings.TrimSpace(b.Source) &&
		strings.TrimSpace(a.PreparedBy) == strings.TrimSpace(b.PreparedBy) &&
		strings.TrimSpace(a.Description) == strings.TrimSpace(b.Description) &&
		a.CreatedAt.Equal(b.CreatedAt)
}

// saveKiwiSquashfs builds a mountable, self-contained `.kiwi` file at
// destination. The output is a zstd-compressed squashfs image containing:
//
//	meta.json        — archive envelope (kind, image name, source container)
//	manifest.json    — base image manifest
//	config.json      — container config (for container snapshots)
//	rootfs/          — base image rootfs (hardlinked from the live store)
//	state/upper/     — writable layer with installed packages / config (for
//	                    container snapshots)
//
// The squashfs is mounted read-only at create/attach time, so no extraction
// ever runs: the kernel pages blocks in on demand. Copying this file to any
// host with kiwi installed yields instant container creation.
func (s Store) saveKiwiSquashfs(meta archiveEnvelope, manifest ImageManifest, config *ContainerConfig, destination string) error {
	if manifest.RootfsPath == "" {
		return fmt.Errorf("cannot snapshot image %q: rootfs path missing", manifest.Name)
	}
	if !isDirectoryPath(manifest.RootfsPath) {
		return fmt.Errorf("cannot snapshot image %q: rootfs backend is not a directory (run the container once to migrate)", manifest.Name)
	}
	staging, err := os.MkdirTemp(filepath.Dir(destination), "kiwi-stage-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(staging)

	metaBytes, err := writeJSONBytes(meta)
	if err != nil {
		return err
	}
	manifestBytes, err := writeJSONBytes(manifest)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(staging, "meta.json"), metaBytes, 0644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(staging, "manifest.json"), manifestBytes, 0644); err != nil {
		return err
	}
	if config != nil {
		configBytes, err := writeJSONBytes(config)
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(staging, "config.json"), configBytes, 0644); err != nil {
			return err
		}
	}
	// Hardlink-mirror both layers into the staging tree. On a same-filesystem
	// store this is O(inodes) — multi-GB layers are prepared in ~1 second.
	rootfsStage := filepath.Join(staging, "rootfs")
	if err := hardlinkTreePath(manifest.RootfsPath, rootfsStage); err != nil {
		return fmt.Errorf("stage rootfs: %w", err)
	}
	if config != nil && isDirectoryPath(config.StatePath) {
		upperSrc := filepath.Join(config.StatePath, "upper")
		if pathExists(upperSrc) {
			upperStage := filepath.Join(staging, "state", "upper")
			if err := hardlinkTreePath(upperSrc, upperStage); err != nil {
				return fmt.Errorf("stage upper layer: %w", err)
			}
		}
	}
	if err := packSquashfs(staging, destination); err != nil {
		return err
	}
	return nil
}

func (s Store) saveArchive(meta archiveEnvelope, manifest ImageManifest, config *ContainerConfig, destination string) error {
	workspace, err := os.MkdirTemp("", "kiwi-archive-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(workspace)
	metaBytes, err := writeJSONBytes(meta)
	if err != nil {
		return err
	}
	manifestBytes, err := writeJSONBytes(manifest)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(workspace, "meta.json"), metaBytes, 0644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(workspace, "manifest.json"), manifestBytes, 0644); err != nil {
		return err
	}
	if config != nil {
		configBytes, err := writeJSONBytes(config)
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(workspace, "config.json"), configBytes, 0644); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0755); err != nil {
		return err
	}
	command := []string{"--sparse", "--xattrs", "--xattrs-include=*", "-czf", destination, "-C", workspace, "meta.json", "manifest.json"}
	if config != nil {
		command = append(command, "config.json")
	}
	// Always include the base rootfs so a .kiwi file is fully self-contained:
	// moving it to another machine (with just kiwi installed) must suffice to
	// recreate a working container without a separate `kiwi pull`.
	if manifest.RootfsPath != "" {
		rootfsName := "rootfs"
		if !isDirectoryPath(manifest.RootfsPath) {
			rootfsName = "rootfs.squashfs"
		}
		command = append(command, "-C", filepath.Dir(manifest.RootfsPath), "--transform", "s|^"+filepath.Base(manifest.RootfsPath)+"$|"+rootfsName+"|", filepath.Base(manifest.RootfsPath))
	}
	if config != nil {
		stateName := "state"
		if !isDirectoryPath(config.StatePath) {
			stateName = "state.img"
		}
		command = append(command, "-C", filepath.Dir(config.StatePath), "--transform", "s|^"+filepath.Base(config.StatePath)+"$|"+stateName+"|", filepath.Base(config.StatePath))
	}
	err = runCommand("tar", command...)
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() == 1 {
			return nil
		}
		return err
	}
	return nil
}

func archiveMinimumCreateSize(archivePath string, sizes ...int64) (int64, error) {
	info, err := os.Stat(archivePath)
	if err != nil {
		return 0, err
	}
	values := append([]int64{defaultStateSize, info.Size()}, sizes...)
	return alignSize(maxInt64(values...), 1024*1024), nil
}

func (s Store) importArchiveImage(data archiveData, imageName string) (ImageManifest, error) {
	if err := s.EnsureLayout(); err != nil {
		return ImageManifest{}, err
	}
	manifest := data.Manifest
	manifest.Name = sanitizeName(imageName)
	if manifest.Name == "" {
		return ImageManifest{}, fmt.Errorf("archive image name is required")
	}
	manifest.RootfsPath = s.ImageRootfsPath(manifest.Name)
	if manifest.CreatedAt.IsZero() {
		manifest.CreatedAt = time.Now().UTC()
	}
	if err := os.MkdirAll(s.ImageDir(manifest.Name), 0755); err != nil {
		return ImageManifest{}, err
	}
	if err := copyPath(data.Manifest.RootfsPath, manifest.RootfsPath); err != nil {
		return ImageManifest{}, err
	}
	if err := s.SaveImage(manifest); err != nil {
		return ImageManifest{}, err
	}
	maybeChownPaths(s.ImageDir(manifest.Name), s.ImageManifestPath(manifest.Name))
	return manifest, nil
}

func (s Store) ensureImageFromArchive(data archiveData) (ImageManifest, error) {
	imageName := sanitizeName(data.Manifest.Name)
	if imageName == "" {
		return ImageManifest{}, fmt.Errorf("archive image name is required")
	}
	current, err := s.LoadImage(imageName)
	if err != nil {
		if err := data.extractRootfs(); err != nil {
			return ImageManifest{}, fmt.Errorf("base image %q not found and %w", imageName, err)
		}
		return s.importArchiveImage(data, imageName)
	}
	if sameArchiveImageIdentity(current, data.Manifest) {
		return current, nil
	}
	if err := data.extractRootfs(); err != nil {
		return ImageManifest{}, fmt.Errorf("base image %q differs and %w", imageName, err)
	}
	currentHash, err := fileSHA256(current.RootfsPath)
	if err != nil {
		return ImageManifest{}, err
	}
	archiveHash, err := fileSHA256(data.Manifest.RootfsPath)
	if err != nil {
		return ImageManifest{}, err
	}
	if currentHash == archiveHash {
		return current, nil
	}
	alternateName := sanitizeName(fmt.Sprintf("%s-%s", imageName, archiveHash[:8]))
	if alternateName == "" {
		return ImageManifest{}, fmt.Errorf("archive image name is required")
	}
	if alternate, err := s.LoadImage(alternateName); err == nil {
		alternateHash, err := fileSHA256(alternate.RootfsPath)
		if err != nil {
			return ImageManifest{}, err
		}
		if alternateHash == archiveHash {
			return alternate, nil
		}
		return ImageManifest{}, fmt.Errorf("image %q already exists with different contents", alternateName)
	}
	return s.importArchiveImage(data, alternateName)
}

func (s Store) CreateContainerFromArchive(archivePath string, sizeBytes int64, explicitSize bool) (ContainerConfig, error) {
	if isSquashfsFile(archivePath) {
		return s.createFromSquashfs(archivePath, sizeBytes, explicitSize)
	}

	// Legacy tar.gz path — kept for backward compatibility with snapshots
	// produced by earlier kiwi versions.
	if imageName, kind, err := peekArchiveImageName(archivePath); err == nil && kind == "image" {
		if existing, err := s.LoadImage(imageName); err == nil {
			if sameArchiveImageIdentity(existing, ImageManifest{Name: imageName}) {
				return s.NewContainer(existing.Name, "", sizeBytes)
			}
		}
	}

	data, cleanup, err := loadArchiveMetadata(archivePath)
	if err != nil {
		return ContainerConfig{}, err
	}
	defer cleanup()

	manifest, err := s.ensureImageFromArchive(data)
	if err != nil {
		return ContainerConfig{}, err
	}

	switch data.Meta.Kind {
	case "image":
		targetSize, err := archiveMinimumCreateSize(archivePath)
		if err != nil {
			return ContainerConfig{}, err
		}
		if explicitSize {
			targetSize = alignSize(sizeBytes, 1024*1024)
			minimum, err := archiveMinimumCreateSize(archivePath)
			if err != nil {
				return ContainerConfig{}, err
			}
			if targetSize < minimum {
				return ContainerConfig{}, fmt.Errorf("storage must be at least %s", formatBytesIEC(minimum))
			}
		}
		return s.NewContainer(manifest.Name, "", targetSize)
	case "container":
		if data.Config == nil {
			return ContainerConfig{}, fmt.Errorf("container archive is missing config.json")
		}
		archivedSize := data.Config.StateSizeBytes
		if archivedSize == 0 {
			archivedSize = defaultStateSize
		}
		targetSize := archivedSize
		if explicitSize {
			targetSize = alignSize(sizeBytes, 1024*1024)
		}
		validateDirStateMinimum := explicitSize && targetSize < archivedSize
		return s.restoreArchivedContainer(*data.Config, manifest.Name, data.Path, targetSize, validateDirStateMinimum)
	default:
		return ContainerConfig{}, fmt.Errorf("unsupported archive kind %q", data.Meta.Kind)
	}
}

// createFromSquashfs creates a container from a self-contained squashfs
// `.kiwi` image. It never extracts anything: the image is mounted read-only
// at a stable path inside kiwi-data, and the container's overlay stack
// references that mount directly. Create and attach are both instant.
func (s Store) createFromSquashfs(archivePath string, sizeBytes int64, explicitSize bool) (ContainerConfig, error) {
	if err := s.EnsureLayout(); err != nil {
		return ContainerConfig{}, err
	}
	mountPoint, meta, manifest, containerConfig, err := s.ensureKiwiImageMounted(archivePath)
	if err != nil {
		return ContainerConfig{}, err
	}
	containerName, err := s.NextContainerName()
	if err != nil {
		return ContainerConfig{}, err
	}
	ip, err := s.NextIP()
	if err != nil {
		return ContainerConfig{}, err
	}
	if err := os.MkdirAll(s.ContainerDir(containerName), 0755); err != nil {
		return ContainerConfig{}, err
	}
	statePath := s.ContainerStatePath(containerName)
	if err := os.MkdirAll(statePath, 0755); err != nil {
		return ContainerConfig{}, err
	}

	imageName := sanitizeName(manifest.Name)
	if imageName == "" {
		imageName = "kiwi-archive"
	}

	targetSize := sizeBytes
	if targetSize == 0 {
		if containerConfig != nil && containerConfig.StateSizeBytes > 0 {
			targetSize = containerConfig.StateSizeBytes
		} else {
			targetSize = defaultStateSize
		}
	}
	if explicitSize {
		targetSize = alignSize(sizeBytes, 1024*1024)
	}

	for _, dir := range []string{"upper", "work"} {
		if err := ensureDir(filepath.Join(statePath, dir)); err != nil {
			return ContainerConfig{}, err
		}
	}

	var config ContainerConfig
	if containerConfig != nil {
		config = *containerConfig
	}
	config.Name = containerName
	config.Hostname = containerName
	config.Image = imageName
	config.StatePath = statePath
	config.StateBackend = "dir"
	config.StateSizeBytes = targetSize
	config.LazyStateArchive = archivePath
	config.IPv4 = ip
	config.Gateway = defaultGateway
	config.CreatedAt = time.Now().UTC()
	defaultContainerConfigValues(&config)
	if err := s.SaveContainer(config); err != nil {
		return ContainerConfig{}, err
	}
	maybeChownPaths(
		s.ContainerDir(containerName),
		s.ContainerConfigPath(containerName),
		s.ContainerSnapshotsDir(containerName),
		s.ContainerSessionsDir(containerName),
	)
	_ = meta
	_ = mountPoint
	return config, nil
}

// ensureKiwiImageMounted mounts a .kiwi squashfs (if not already mounted)
// and returns its mountpoint plus decoded metadata. The mountpoint is
// content-addressed (keyed by the archive's path/size/mtime) so repeated
// calls share a single mount across all containers that use the same image.
func (s Store) ensureKiwiImageMounted(archivePath string) (string, archiveEnvelope, ImageManifest, *ContainerConfig, error) {
	mountPoint, err := ensureKiwiMountPath(archivePath)
	if err != nil {
		return "", archiveEnvelope{}, ImageManifest{}, nil, fmt.Errorf("mount .kiwi image: %w", err)
	}
	var meta archiveEnvelope
	metaBytes, err := os.ReadFile(filepath.Join(mountPoint, "meta.json"))
	if err != nil {
		return "", archiveEnvelope{}, ImageManifest{}, nil, fmt.Errorf("read meta.json: %w", err)
	}
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		return "", archiveEnvelope{}, ImageManifest{}, nil, err
	}
	var manifest ImageManifest
	manifestBytes, err := os.ReadFile(filepath.Join(mountPoint, "manifest.json"))
	if err != nil {
		return "", archiveEnvelope{}, ImageManifest{}, nil, fmt.Errorf("read manifest.json: %w", err)
	}
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return "", archiveEnvelope{}, ImageManifest{}, nil, err
	}
	var config *ContainerConfig
	if meta.Kind == "container" {
		cfgBytes, err := os.ReadFile(filepath.Join(mountPoint, "config.json"))
		if err != nil {
			return "", archiveEnvelope{}, ImageManifest{}, nil, fmt.Errorf("read config.json: %w", err)
		}
		var parsed ContainerConfig
		if err := json.Unmarshal(cfgBytes, &parsed); err != nil {
			return "", archiveEnvelope{}, ImageManifest{}, nil, err
		}
		config = &parsed
	}
	return mountPoint, meta, manifest, config, nil
}

func (s Store) restoreArchivedContainer(template ContainerConfig, imageName, archivePath string, sizeBytes int64, validateDirStateMinimum bool) (ContainerConfig, error) {
	if err := s.EnsureLayout(); err != nil {
		return ContainerConfig{}, err
	}
	containerName, err := s.NextContainerName()
	if err != nil {
		return ContainerConfig{}, err
	}
	ip, err := s.NextIP()
	if err != nil {
		return ContainerConfig{}, err
	}
	if err := os.MkdirAll(s.ContainerDir(containerName), 0755); err != nil {
		return ContainerConfig{}, err
	}
	statePath := s.ContainerStatePath(containerName)
	stateEntry := "state"
	if template.StateBackend != "dir" {
		stateEntry = "state.img"
	}

	if err := os.MkdirAll(statePath, 0755); err != nil {
		return ContainerConfig{}, err
	}
	for _, dir := range []string{"upper", "work"} {
		if err := ensureDir(filepath.Join(statePath, dir)); err != nil {
			return ContainerConfig{}, err
		}
	}

	resolvedPath := archivePath
	if !pathExists(resolvedPath) {
		absPath, _ := filepath.Abs(resolvedPath)
		if pathExists(absPath) {
			resolvedPath = absPath
		}
	}

	// Force the archive state into the persistent layer cache so subsequent
	// attaches are instant. On the host where the snapshot was created this
	// cache is already pre-warmed via hardlinks (see SnapshotContainer), so
	// this call returns immediately. On an imported .kiwi file we pay the
	// extraction cost exactly once, here at create time.
	if _, err := getCachedArchiveState(resolvedPath, s.DataRoot); err != nil {
		return ContainerConfig{}, fmt.Errorf("prepare snapshot layer: %w", err)
	}

	if stateEntry == "state.img" {
		archivedSize := template.StateSizeBytes
		if archivedSize == 0 {
			archivedSize = defaultStateSize
		}
		if sizeBytes < archivedSize {
			return ContainerConfig{}, fmt.Errorf("storage must be at least %s", formatBytesIEC(archivedSize))
		}
		if err := os.WriteFile(statePath+".img", nil, 0644); err != nil {
			return ContainerConfig{}, err
		}
		statePath = statePath + ".img"
		if err := resizeExt4Image(statePath, sizeBytes); err != nil {
			return ContainerConfig{}, err
		}
	}

	config := template
	config.Name = containerName
	config.Hostname = containerName
	config.Image = imageName
	config.StatePath = statePath
	config.StateSizeBytes = sizeBytes
	config.LazyStateArchive = resolvedPath
	if stateEntry == "state" {
		config.StateBackend = "dir"
		if validateDirStateMinimum {
			minimumSize := sizeBytes
			if minimumSize < defaultStateSize {
				minimumSize = defaultStateSize
			}
			if sizeBytes < minimumSize {
				return ContainerConfig{}, fmt.Errorf("storage must be at least %s", formatBytesIEC(minimumSize))
			}
		}
	}
	config.IPv4 = ip
	config.Gateway = defaultGateway
	config.CreatedAt = time.Now().UTC()
	defaultContainerConfigValues(&config)
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
