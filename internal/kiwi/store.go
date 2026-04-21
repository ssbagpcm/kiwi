package kiwi

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type Store struct {
	DataRoot    string
	RuntimeRoot string
}

func NewStore() Store {
	exeDir := "."
	if executable, err := os.Executable(); err == nil {
		exeDir = filepath.Dir(executable)
	}
	defaultRoot := filepath.Join(exeDir, "kiwi-data")
	defaultRuntimeRoot := filepath.Join(defaultRoot, "mounts")
	legacyRuntimeRoot := filepath.Join(defaultRoot, "run")
	legacyRoot := filepath.Join(exeDir, ".kiwi")
	if _, err := os.Stat(defaultRoot); os.IsNotExist(err) {
		if _, legacyErr := os.Stat(legacyRoot); legacyErr == nil {
			_ = os.Rename(legacyRoot, defaultRoot)
		}
	}
	if _, err := os.Stat(defaultRuntimeRoot); os.IsNotExist(err) {
		if _, legacyErr := os.Stat(legacyRuntimeRoot); legacyErr == nil {
			_ = os.Rename(legacyRuntimeRoot, defaultRuntimeRoot)
		}
	}
	return Store{
		DataRoot:    envOrDefault("KIWI_ROOT", defaultRoot),
		RuntimeRoot: envOrDefault("KIWI_RUNTIME", defaultRuntimeRoot),
	}
}

func (s Store) EnsureLayout() error {
	for _, dir := range []string{s.ImagesDir(), s.ContainersDir(), s.RuntimeContainersDir(), s.ExportsDir()} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}
	return nil
}

func (s Store) ImagesDir() string {
	return filepath.Join(s.DataRoot, "images")
}

func (s Store) ImageDir(name string) string {
	return filepath.Join(s.ImagesDir(), sanitizeName(name))
}

func (s Store) ImageManifestPath(name string) string {
	return filepath.Join(s.ImageDir(name), "manifest.json")
}

func (s Store) ImageRootfsPath(name string) string {
	return filepath.Join(s.ImageDir(name), "rootfs")
}

func (s Store) ContainersDir() string {
	return filepath.Join(s.DataRoot, "containers")
}

func (s Store) ContainerDir(name string) string {
	return filepath.Join(s.ContainersDir(), sanitizeName(name))
}

func (s Store) ContainerConfigPath(name string) string {
	return filepath.Join(s.ContainerDir(name), "config.json")
}

func (s Store) ContainerStatePath(name string) string {
	return filepath.Join(s.ContainerDir(name), "state")
}

func (s Store) ContainerSnapshotsDir(name string) string {
	return filepath.Join(s.ContainerDir(name), "snapshots")
}

func (s Store) SnapshotPath(containerName, snapshotName string) string {
	return filepath.Join(s.DataRoot, sanitizeName(snapshotName)+".kiwi")
}

func (s Store) ContainerSessionsDir(name string) string {
	return filepath.Join(s.ContainerDir(name), "sessions")
}

func (s Store) SessionDir(containerName, sessionID string) string {
	return filepath.Join(s.ContainerSessionsDir(containerName), sanitizeName(sessionID))
}

func (s Store) SessionInfoPath(containerName, sessionID string) string {
	return filepath.Join(s.SessionDir(containerName, sessionID), "session.json")
}

func (s Store) SessionSocketPath(containerName, sessionID string) string {
	return filepath.Join(os.TempDir(), fmt.Sprintf("kiwi-%s-%s.sock", sanitizeName(containerName), sanitizeName(sessionID)))
}

func (s Store) SessionBufferPath(containerName, sessionID string) string {
	return filepath.Join(s.SessionDir(containerName, sessionID), "screen.log")
}

func (s Store) RuntimeContainersDir() string {
	return filepath.Join(s.RuntimeRoot, "containers")
}

func (s Store) RuntimeContainerDir(name string) string {
	return filepath.Join(s.RuntimeContainersDir(), sanitizeName(name))
}

func (s Store) RuntimeStatePath(name string) string {
	return filepath.Join(s.RuntimeContainerDir(name), "runtime.json")
}

func (s Store) RuntimeBaseMountpoint(name string) string {
	return filepath.Join(s.RuntimeContainerDir(name), "base")
}

func (s Store) RuntimeStateMountpoint(name string) string {
	return filepath.Join(s.RuntimeContainerDir(name), "state")
}

func (s Store) RuntimeMergedMountpoint(name string) string {
	return filepath.Join(s.RuntimeContainerDir(name), "merged")
}

func (s Store) ExportsDir() string {
	return filepath.Join(s.DataRoot, "images")
}

func (s Store) ExportPath(name string) string {
	return filepath.Join(s.ImagesDir(), sanitizeName(name)+".kiwi")
}

func (s Store) LoadImage(name string) (ImageManifest, error) {
	var manifest ImageManifest
	if err := readJSONFile(s.ImageManifestPath(name), &manifest); err != nil {
		return ImageManifest{}, err
	}
	return manifest, nil
}

func (s Store) LoadContainer(name string) (ContainerConfig, error) {
	var config ContainerConfig
	if err := readJSONFile(s.ContainerConfigPath(name), &config); err != nil {
		return ContainerConfig{}, err
	}
	defaultContainerConfigValues(&config)
	return config, nil
}

func (s Store) LoadRuntimeState(name string) (RuntimeState, error) {
	var state RuntimeState
	if err := readJSONFile(s.RuntimeStatePath(name), &state); err != nil {
		return RuntimeState{}, err
	}
	return state, nil
}

func (s Store) SaveImage(manifest ImageManifest) error {
	if err := os.MkdirAll(s.ImageDir(manifest.Name), 0755); err != nil {
		return err
	}
	return writeJSONFile(s.ImageManifestPath(manifest.Name), manifest)
}

func (s Store) SaveContainer(config ContainerConfig) error {
	defaultContainerConfigValues(&config)
	if err := os.MkdirAll(s.ContainerDir(config.Name), 0755); err != nil {
		return err
	}
	if err := os.MkdirAll(s.ContainerSnapshotsDir(config.Name), 0755); err != nil {
		return err
	}
	if err := os.MkdirAll(s.ContainerSessionsDir(config.Name), 0755); err != nil {
		return err
	}
	return writeJSONFile(s.ContainerConfigPath(config.Name), config)
}

func (s Store) SaveRuntimeState(state RuntimeState) error {
	if err := os.MkdirAll(s.RuntimeContainerDir(state.Name), 0755); err != nil {
		return err
	}
	return writeJSONFile(s.RuntimeStatePath(state.Name), state)
}

func (s Store) ClearRuntimeState(name string) error {
	path := s.RuntimeStatePath(name)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (s Store) LoadSession(containerName, sessionID string) (SessionInfo, error) {
	var session SessionInfo
	if err := readJSONFile(s.SessionInfoPath(containerName, sessionID), &session); err != nil {
		return SessionInfo{}, err
	}
	return session, nil
}

func (s Store) SaveSession(session SessionInfo) error {
	if err := os.MkdirAll(s.SessionDir(session.Container, session.ID), 0755); err != nil {
		return err
	}
	return writeJSONFile(s.SessionInfoPath(session.Container, session.ID), session)
}

func (s Store) DeleteSession(containerName, sessionID string) error {
	return os.RemoveAll(s.SessionDir(containerName, sessionID))
}

func readJSONFile(path string, out interface{}) error {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("%s does not exist", path)
		}
		return err
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	return nil
}

func writeJSONFile(path string, value interface{}) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0644)
}

func (s Store) NextIP() (string, error) {
	entries, err := os.ReadDir(s.ContainersDir())
	if err != nil && !os.IsNotExist(err) {
		return "", err
	}
	used := map[string]bool{defaultGateway: true}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		config, err := s.LoadContainer(entry.Name())
		if err == nil && config.IPv4 != "" {
			used[config.IPv4] = true
		}
	}
	for host := 10; host < 250; host++ {
		candidate := fmt.Sprintf("10.44.0.%d", host)
		if !used[candidate] {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("kiwi subnet %s is full", defaultCIDR)
}

func (s Store) ListImages() ([]ImageManifest, error) {
	entries, err := os.ReadDir(s.ImagesDir())
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	images := make([]ImageManifest, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		manifest, err := s.LoadImage(entry.Name())
		if err == nil {
			images = append(images, manifest)
		}
	}
	return images, nil
}

func (s Store) ListContainers() ([]ContainerConfig, error) {
	entries, err := os.ReadDir(s.ContainersDir())
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	containers := make([]ContainerConfig, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		config, err := s.LoadContainer(entry.Name())
		if err == nil {
			containers = append(containers, config)
		}
	}
	return containers, nil
}

func (s Store) ListSessions(containerName string) ([]SessionInfo, error) {
	entries, err := os.ReadDir(s.ContainerSessionsDir(containerName))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	sessions := make([]SessionInfo, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		session, err := s.LoadSession(containerName, entry.Name())
		if err == nil {
			sessions = append(sessions, session)
		}
	}
	return sessions, nil
}

func (s Store) FindSession(sessionID string) (SessionInfo, error) {
	containers, err := s.ListContainers()
	if err != nil {
		return SessionInfo{}, err
	}
	for _, container := range containers {
		session, err := s.LoadSession(container.Name, sessionID)
		if err == nil {
			return session, nil
		}
	}
	return SessionInfo{}, fmt.Errorf("session %q not found", sessionID)
}
