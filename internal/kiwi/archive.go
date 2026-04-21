package kiwi

import (
	"archive/tar"
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

func extractArchiveEntries(archivePath, workspace string, entries ...string) error {
	args := []string{"-xzf", archivePath, "-C", workspace}
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
	if err := runCommand("tar", "-xzf", archivePath, "-C", parent, entry); err != nil {
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

// peekArchiveImageName reads only the meta.json header from a .kiwi archive to get
// the image name and kind, without extracting any files. Returns instantly.
func peekArchiveImageName(archivePath string) (string, string, error) {
	f, err := os.Open(archivePath)
	if err != nil {
		return "", "", err
	}
	defer f.Close()

	tr := tar.NewReader(f)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", "", err
		}
		if strings.TrimPrefix(hdr.Name, "./") == "meta.json" {
			var meta archiveEnvelope
			if err := json.NewDecoder(tr).Decode(&meta); err != nil {
				return "", "", err
			}
			imageName := meta.ImageName
			if imageName == "" {
				imageName = meta.Container
			}
			return imageName, meta.Kind, nil
		}
	}
	return "", "", fmt.Errorf("meta.json not found in archive")
}

func loadArchiveMetadata(archivePath string) (archiveData, func(), error) {
	workspace, err := os.MkdirTemp("", "kiwi-load-")
	if err != nil {
		return archiveData{}, nil, err
	}
	cleanup := func() {
		_ = os.RemoveAll(workspace)
	}
	if err := extractArchiveEntries(archivePath, workspace, "meta.json", "manifest.json"); err != nil {
		cleanup()
		return archiveData{}, nil, err
	}
	metaData, err := os.ReadFile(filepath.Join(workspace, "meta.json"))
	if err != nil {
		cleanup()
		return archiveData{}, nil, err
	}
	var meta archiveEnvelope
	if err := json.Unmarshal(metaData, &meta); err != nil {
		cleanup()
		return archiveData{}, nil, err
	}
	manifestData, err := os.ReadFile(filepath.Join(workspace, "manifest.json"))
	if err != nil {
		cleanup()
		return archiveData{}, nil, err
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
		if err := extractArchiveEntries(archivePath, workspace, "config.json"); err != nil {
			cleanup()
			return archiveData{}, nil, err
		}
		configData, err := os.ReadFile(filepath.Join(workspace, "config.json"))
		if err != nil {
			cleanup()
			return archiveData{}, nil, err
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
	command := []string{"--sparse", "-czf", destination, "-C", workspace, "meta.json", "manifest.json"}
	if config != nil {
		command = append(command, "config.json")
	}
	if meta.Kind == "image" {
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
	// Fast path: peek at image name without full extraction
	if imageName, kind, err := peekArchiveImageName(archivePath); err == nil && kind == "image" {
		if existing, err := s.LoadImage(imageName); err == nil {
			if sameArchiveImageIdentity(existing, ImageManifest{Name: imageName}) {
				// Image already cached, skip tar extraction entirely
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

	// INSTANT CREATE: Just create empty state structure, no I/O!
	if err := os.MkdirAll(statePath, 0755); err != nil {
		return ContainerConfig{}, err
	}
	for _, dir := range []string{"upper", "work"} {
		if err := ensureDir(filepath.Join(statePath, dir)); err != nil {
			return ContainerConfig{}, err
		}
	}

	// PRE-CACHE: Extract archive state to cache NOW (during create)
	// This makes subsequent "attach" instant!
	resolvedPath := archivePath
	if !pathExists(resolvedPath) {
		absPath, _ := filepath.Abs(resolvedPath)
		if pathExists(absPath) {
			resolvedPath = absPath
		}
	}
	if pathExists(resolvedPath) {
		if _, cacheErr := getCachedArchiveState(resolvedPath, s.DataRoot); cacheErr != nil {
			fmt.Fprintf(os.Stderr, "kiwi: warning: failed to pre-cache archive: %v\n", cacheErr)
		}
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
