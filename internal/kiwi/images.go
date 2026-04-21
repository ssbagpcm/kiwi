package kiwi

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const imagePreparedBy = "kiwi-v7-dir"

var builtInImages = map[string]builtInImage{
	"alpine": {
		Name:        "alpine",
		URL:         "https://fra1lxdmirror01.do.letsbuildthe.cloud/images/alpine/3.23/amd64/cloud/20260420_13:00/rootfs.tar.xz",
		Description: "Alpine 3.23 cloud rootfs",
	},
	"debian": {
		Name:        "debian",
		URL:         "https://fra1lxdmirror01.do.letsbuildthe.cloud/images/debian/trixie/amd64/default/20260420_05:24/rootfs.tar.xz",
		Description: "Debian trixie cloud rootfs",
	},
}

func (s Store) PullBuiltInImage(name string) (ImageManifest, string, error) {
	image, ok := builtInImages[sanitizeName(name)]
	if !ok {
		return ImageManifest{}, "", fmt.Errorf("unknown built-in image %q", name)
	}
	if os.Geteuid() != 0 {
		return ImageManifest{}, "", fmt.Errorf("built-in images need sudo once to write local image files")
	}
	if err := s.EnsureLayout(); err != nil {
		return ImageManifest{}, "", err
	}
	if _, err := os.Stat(s.ImageManifestPath(image.Name)); err == nil {
		manifest, err := s.LoadImage(image.Name)
		if err != nil {
			return ImageManifest{}, "", err
		}
		if manifest.PreparedBy == imagePreparedBy {
			archivePath := s.ExportPath(image.Name)
			if !pathExists(archivePath) {
				if err := s.SaveImageArchive(manifest, archivePath); err != nil {
					return ImageManifest{}, "", err
				}
				maybeChownFile(archivePath)
			}
			return manifest, archivePath, nil
		}
	}
	if err := os.MkdirAll(s.ImageDir(image.Name), 0755); err != nil {
		return ImageManifest{}, "", err
	}
	downloadPath := filepath.Join(s.ImageDir(image.Name), "rootfs.tar.xz")
	if err := downloadFile(image.URL, downloadPath); err != nil {
		return ImageManifest{}, "", err
	}

	rootfsPath := s.ImageRootfsPath(image.Name)
	if err := os.RemoveAll(rootfsPath); err != nil && !os.IsNotExist(err) {
		return ImageManifest{}, "", err
	}
	if err := os.MkdirAll(rootfsPath, 0755); err != nil {
		return ImageManifest{}, "", err
	}

	// Extract immediately using native tar, removing the need for loop mounts later
	if err := runCommand("tar", "-xpf", downloadPath, "-C", rootfsPath); err != nil {
		return ImageManifest{}, "", err
	}
	_ = os.Remove(downloadPath)

	manifest := ImageManifest{
		Name:        image.Name,
		RootfsPath:  rootfsPath,
		Format:      "dir",
		Source:      image.URL,
		PreparedBy:  imagePreparedBy,
		Description: image.Description,
		CreatedAt:   time.Now().UTC(),
	}
	if err := s.SaveImage(manifest); err != nil {
		return ImageManifest{}, "", err
	}
	maybeChownPaths(s.ImageDir(image.Name), s.ImageManifestPath(manifest.Name))
	return manifest, "", nil
}

func (s Store) ImportImage(name, rootfsArchive string) (ImageManifest, error) {
	cleanName := sanitizeName(name)
	if cleanName == "" {
		return ImageManifest{}, fmt.Errorf("image name is required")
	}
	if rootfsArchive == "" {
		return ImageManifest{}, fmt.Errorf("rootfs archive is required")
	}
	if _, err := os.Stat(s.ImageManifestPath(cleanName)); err == nil {
		return ImageManifest{}, fmt.Errorf("image %q already exists", cleanName)
	}
	if err := s.EnsureLayout(); err != nil {
		return ImageManifest{}, err
	}
	workspace, err := os.MkdirTemp("", "kiwi-import-")
	if err != nil {
		return ImageManifest{}, err
	}
	defer os.RemoveAll(workspace)
	rootfsDir := filepath.Join(workspace, "rootfs")
	if err := ensureDir(rootfsDir); err != nil {
		return ImageManifest{}, err
	}
	if err := runCommand("tar", "-xpf", rootfsArchive, "-C", rootfsDir); err != nil {
		return ImageManifest{}, err
	}
	if err := os.MkdirAll(s.ImageDir(cleanName), 0755); err != nil {
		return ImageManifest{}, err
	}
	rootfsPath := s.ImageRootfsPath(cleanName)
	if err := copyPath(filepath.Join(rootfsDir, "."), rootfsPath); err != nil {
		return ImageManifest{}, err
	}
	manifest := ImageManifest{
		Name:        cleanName,
		RootfsPath:  rootfsPath,
		Format:      "dir",
		Source:      rootfsArchive,
		PreparedBy:  imagePreparedBy,
		Description: "Imported custom rootfs archive",
		CreatedAt:   time.Now().UTC(),
	}
	if err := s.SaveImage(manifest); err != nil {
		return ImageManifest{}, err
	}
	maybeChownPaths(s.ImageDir(cleanName), s.ImageManifestPath(cleanName))
	return manifest, nil
}

func (s Store) CommitContainer(containerName, newImageName string) (ImageManifest, string, error) {
	config, err := s.LoadContainer(containerName)
	if err != nil {
		return ImageManifest{}, "", err
	}
	if sanitizeName(newImageName) == "" {
		return ImageManifest{}, "", fmt.Errorf("new image name is required")
	}
	if _, err := os.Stat(s.ImageManifestPath(newImageName)); err == nil {
		return ImageManifest{}, "", fmt.Errorf("image %q already exists", newImageName)
	}
	state, _ := s.LoadRuntimeState(config.Name)
	rootPath := runtimeRootfsPath(state)
	if rootPath == "" {
		image, err := s.LoadImage(config.Image)
		if err != nil {
			return ImageManifest{}, "", err
		}
		tempDir, err := os.MkdirTemp("", "kiwi-commit-")
		if err != nil {
			return ImageManifest{}, "", err
		}
		defer os.RemoveAll(tempDir)
		mergedDir := filepath.Join(tempDir, "merged")
		if err := ensureDir(mergedDir); err != nil {
			return ImageManifest{}, "", err
		}
		lowerDir, upperDir, workDir, cleanup, err := prepareOverlayDirs(image, config, filepath.Join(tempDir, "base"), filepath.Join(tempDir, "state"))
		if err != nil {
			return ImageManifest{}, "", err
		}
		defer cleanup()
		if err := mountOverlayPath(lowerDir, upperDir, workDir, mergedDir); err != nil {
			return ImageManifest{}, "", fmt.Errorf("mount overlay: %w", err)
		}
		defer func() {
			_ = runCommand("umount", "-l", mergedDir)
		}()
		rootPath = mergedDir
	}
	if err := os.MkdirAll(s.ImageDir(newImageName), 0755); err != nil {
		return ImageManifest{}, "", err
	}
	rootfsPath := s.ImageRootfsPath(newImageName)
	if err := copyPath(filepath.Join(rootPath, "."), rootfsPath); err != nil {
		return ImageManifest{}, "", err
	}
	manifest := ImageManifest{
		Name:        sanitizeName(newImageName),
		RootfsPath:  rootfsPath,
		Format:      "dir",
		Source:      "commit:" + sanitizeName(containerName),
		PreparedBy:  imagePreparedBy,
		Description: fmt.Sprintf("Committed from container %s", config.Name),
		CreatedAt:   time.Now().UTC(),
	}
	if err := s.SaveImage(manifest); err != nil {
		return ImageManifest{}, "", err
	}
	archivePath := s.ExportPath(manifest.Name)
	if err := s.SaveImageArchive(manifest, archivePath); err != nil {
		return ImageManifest{}, "", err
	}
	maybeChownPaths(s.ImageDir(manifest.Name), s.ImageManifestPath(manifest.Name))
	maybeChownFile(archivePath)
	return manifest, archivePath, nil
}

func (s Store) SaveImageArchive(manifest ImageManifest, destination string) error {
	return s.saveArchive(archiveEnvelope{Kind: "image", ImageName: manifest.Name}, manifest, nil, destination)
}

func (s Store) ensureImageBackend(name string) (ImageManifest, error) {
	manifest, err := s.LoadImage(name)
	if err != nil {
		return ImageManifest{}, err
	}
	if manifest.Format == "dir" && isDirectoryPath(manifest.RootfsPath) {
		return manifest, nil
	}
	if manifest.Format == "squashfs" && pathExists(manifest.RootfsPath) {
		// Optimization: if we are on a host where loop is disabled, we MUST extract to dir format
		// to keep subsequent operations fast. We only do this ONCE.
		tempDir, err := os.MkdirTemp("", "kiwi-mount-test-")
		if err == nil {
			err = runCommand("mount", "-t", "squashfs", "-o", "loop,ro", manifest.RootfsPath, tempDir)
			if err == nil {
				_ = runCommand("umount", "-l", tempDir)
				_ = os.RemoveAll(tempDir)
				return manifest, nil
			}
			_ = os.RemoveAll(tempDir)
			if isLoopMountError(err) {
				// Loop not available, extract to directory for permanent backend
				targetRootfs := s.ImageRootfsPath(manifest.Name)
				_ = os.RemoveAll(targetRootfs)
				_ = os.MkdirAll(targetRootfs, 0755)
				// Try unsquashfs
				if err := runCommand("unsquashfs", "-f", "-d", targetRootfs, manifest.RootfsPath); err == nil {
					manifest.RootfsPath = targetRootfs
					manifest.Format = "dir"
					manifest.PreparedBy = imagePreparedBy
					_ = s.SaveImage(manifest)
					return manifest, nil
				}
				// If unsquashfs is not available, we must RE-PULL as tar.xz
				if _, ok := builtInImages[manifest.Name]; ok {
					newManifest, _, err := s.PullBuiltInImage(manifest.Name)
					if err == nil {
						return newManifest, nil
					}
				}
			}
		}
		return manifest, nil
	}
	// Migration/check for existing data
	targetRootfs := s.ImageRootfsPath(manifest.Name)
	if isDirectoryPath(targetRootfs) {
		manifest.RootfsPath = targetRootfs
		manifest.Format = "dir"
		manifest.PreparedBy = imagePreparedBy
		_ = s.SaveImage(manifest)
		return manifest, nil
	}
	squashPath := targetRootfs + ".squashfs"
	if pathExists(squashPath) {
		manifest.RootfsPath = squashPath
		manifest.Format = "squashfs"
		_ = s.SaveImage(manifest)
		return s.ensureImageBackend(manifest.Name)
	}
	return manifest, nil
}
