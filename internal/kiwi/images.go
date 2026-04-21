package kiwi

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const imagePreparedBy = "kiwi-v5-dir"

var builtInImages = map[string]builtInImage{
	"alpine": {
		Name:        "alpine",
		URL:         "https://fra1lxdmirror01.do.letsbuildthe.cloud/images/alpine/3.23/amd64/cloud/20260420_13:00/rootfs.squashfs",
		Description: "Alpine 3.23 cloud rootfs",
	},
	"debian": {
		Name:        "debian",
		URL:         "https://fra1lxdmirror01.do.letsbuildthe.cloud/images/debian/trixie/amd64/default/20260420_05:24/rootfs.squashfs",
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
	downloadPath := filepath.Join(s.ImageDir(image.Name), "rootfs.squashfs")
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
	mountDir, err := os.MkdirTemp("", "kiwi-image-")
	if err != nil {
		return ImageManifest{}, "", err
	}
	defer os.RemoveAll(mountDir)
	if err := runCommand("mount", "-t", "squashfs", "-o", "loop,ro", downloadPath, mountDir); err != nil {
		return ImageManifest{}, "", fmt.Errorf("%w\n\nTip: loop devices are not available on this host.\nRun: sudo modprobe loop", err)
	}
	defer func() {
		_ = runCommand("umount", "-l", mountDir)
	}()
	if err := copyPath(filepath.Join(mountDir, "."), rootfsPath); err != nil {
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
	// Don't create archive here - only create when explicitly exporting
	// This makes pull much faster by skipping re-reading all rootfs files
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

func (s Store) ensureImageDirectoryBackend(name string) (ImageManifest, error) {
	manifest, err := s.LoadImage(name)
	if err != nil {
		return ImageManifest{}, err
	}
	targetRootfs := s.ImageRootfsPath(manifest.Name)
	if manifest.Format == "dir" && manifest.RootfsPath == targetRootfs && isDirectoryPath(targetRootfs) {
		return manifest, nil
	}
	if isDirectoryPath(targetRootfs) && manifest.RootfsPath != targetRootfs {
		manifest.RootfsPath = targetRootfs
		manifest.Format = "dir"
		manifest.PreparedBy = imagePreparedBy
		if err := s.SaveImage(manifest); err != nil {
			return ImageManifest{}, err
		}
		return manifest, nil
	}
	if isDirectoryPath(manifest.RootfsPath) {
		if err := copyPath(filepath.Join(manifest.RootfsPath, "."), targetRootfs); err != nil {
			return ImageManifest{}, err
		}
	} else {
		mountDir, err := os.MkdirTemp("", "kiwi-image-migrate-")
		if err != nil {
			return ImageManifest{}, err
		}
		defer os.RemoveAll(mountDir)
		if err := runCommand("mount", "-t", "squashfs", "-o", "loop,ro", manifest.RootfsPath, mountDir); err != nil {
			return ImageManifest{}, fmt.Errorf("%w\n\nTip: loop devices are not available on this host.\nRun: sudo modprobe loop", err)
		}
		defer func() {
			_ = runCommand("umount", "-l", mountDir)
		}()
		if err := copyPath(filepath.Join(mountDir, "."), targetRootfs); err != nil {
			return ImageManifest{}, err
		}
	}
	manifest.RootfsPath = targetRootfs
	manifest.Format = "dir"
	manifest.PreparedBy = imagePreparedBy
	if err := s.SaveImage(manifest); err != nil {
		return ImageManifest{}, err
	}
	maybeChownPaths(s.ImageDir(manifest.Name), s.ImageManifestPath(manifest.Name))
	return manifest, nil
}
