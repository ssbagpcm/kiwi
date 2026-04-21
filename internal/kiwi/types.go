package kiwi

import (
	"encoding/json"
	"time"
)

const (
	defaultBridgeName = "kiwi0"
	defaultCIDR       = "10.44.0.0/24"
	defaultGateway    = "10.44.0.1"
	defaultStateSize  = int64(1024 * 1024 * 1024)
	defaultPidsMax    = 512
	defaultMemory     = "256"
	defaultCPU        = 1
	defaultNetwork    = "separate"
)

type ImageManifest struct {
	Name        string    `json:"name"`
	RootfsPath  string    `json:"rootfs_path"`
	Format      string    `json:"format"`
	Source      string    `json:"source"`
	PreparedBy  string    `json:"prepared_by,omitempty"`
	Description string    `json:"description"`
	CreatedAt   time.Time `json:"created_at"`
}

type ContainerConfig struct {
	Name           string    `json:"name"`
	Hostname       string    `json:"hostname,omitempty"`
	Image          string    `json:"image"`
	StatePath      string    `json:"state_path"`
	StateSizeBytes int64     `json:"state_size_bytes"`
	StateSizeHost  bool      `json:"state_size_host,omitempty"`
	StateBackend   string    `json:"state_backend,omitempty"`
	// LazyStateArchive is the path to the archive for instant container restore
	LazyStateArchive string    `json:"lazy_state_archive,omitempty"`
	IPv4             string    `json:"ipv4"`
	Gateway          string    `json:"gateway"`
	Memory           string    `json:"-"`
	CPU              int       `json:"-"`
	CPUHost          bool      `json:"-"`
	Network          string    `json:"-"`
	Shell            string    `json:"-"`
	CreatedAt        time.Time `json:"-"`
}

type RuntimeState struct {
	Name             string    `json:"name"`
	PID              int       `json:"pid"`
	Running          bool      `json:"running"`
	CgroupPath       string    `json:"cgroup_path"`
	BaseMountpoint   string    `json:"base_mountpoint"`
	StateMountpoint  string    `json:"state_mountpoint"`
	MergedMountpoint string    `json:"merged_mountpoint"`
	TargetMountpoint string    `json:"target_mountpoint,omitempty"`
	VethHost         string    `json:"veth_host,omitempty"`
	IPv4             string    `json:"ipv4,omitempty"`
	MountedAt        time.Time `json:"mounted_at"`
	StartedAt        time.Time `json:"started_at,omitempty"`
}

type SessionInfo struct {
	ID           string    `json:"id"`
	Container    string    `json:"container"`
	SocketPath   string    `json:"socket_path"`
	BufferPath   string    `json:"buffer_path"`
	DaemonPID    int       `json:"daemon_pid,omitempty"`
	ShellPID     int       `json:"shell_pid,omitempty"`
	Attached     bool      `json:"attached,omitempty"`
	ShellRunning bool      `json:"shell_running,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

type StartOptions struct {
	Memory      string
	CPU         int
	CPUHost     bool
	Name        string
	Shell       string
	Network     string
	Storage     string
	StorageHost bool
	Cmd         []string
}

type builtInImage struct {
	Name        string
	URL         string
	Description string
}

type archiveEnvelope struct {
	Kind      string `json:"kind"`
	ImageName string `json:"image_name,omitempty"`
	Container string `json:"container,omitempty"`
}

type containerConfigJSON struct {
	Name              string    `json:"name"`
	Hostname          string    `json:"hostname,omitempty"`
	Image             string    `json:"image"`
	StatePath         string    `json:"state_path"`
	StateSizeBytes    int64     `json:"state_size_bytes"`
	StateSizeHost     bool      `json:"state_size_host,omitempty"`
	StateBackend      string    `json:"state_backend,omitempty"`
	LazyStateArchive  string    `json:"lazy_state_archive,omitempty"`
	IPv4              string    `json:"ipv4"`
	Gateway           string    `json:"gateway"`
	Memory            string    `json:"memory,omitempty"`
	CPU               int       `json:"cpu,omitempty"`
	CPUHost           bool      `json:"cpu_host,omitempty"`
	LegacySMP         int       `json:"smp,omitempty"`
	Network           string    `json:"network,omitempty"`
	Shell             string    `json:"shell,omitempty"`
	CreatedAt         time.Time `json:"created_at"`
}

func (c ContainerConfig) MarshalJSON() ([]byte, error) {
	config := c
	defaultContainerConfigValues(&config)
	return json.Marshal(containerConfigJSON{
		Name:              config.Name,
		Hostname:          config.Hostname,
		Image:             config.Image,
		StatePath:         config.StatePath,
		StateSizeBytes:    config.StateSizeBytes,
		StateSizeHost:     config.StateSizeHost,
		StateBackend:      config.StateBackend,
		LazyStateArchive:  config.LazyStateArchive,
		IPv4:              config.IPv4,
		Gateway:           config.Gateway,
		Memory:            config.Memory,
		CPU:               config.CPU,
		CPUHost:           config.CPUHost,
		Network:           config.Network,
		Shell:             config.Shell,
		CreatedAt:         config.CreatedAt,
	})
}

func (c *ContainerConfig) UnmarshalJSON(data []byte) error {
	var raw containerConfigJSON
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	*c = ContainerConfig{
		Name:              raw.Name,
		Hostname:          raw.Hostname,
		Image:             raw.Image,
		StatePath:         raw.StatePath,
		StateSizeBytes:    raw.StateSizeBytes,
		StateSizeHost:     raw.StateSizeHost,
		StateBackend:      raw.StateBackend,
		LazyStateArchive:  raw.LazyStateArchive,
		IPv4:              raw.IPv4,
		Gateway:           raw.Gateway,
		Memory:            raw.Memory,
		CPU:               raw.CPU,
		CPUHost:           raw.CPUHost,
		Network:           raw.Network,
		Shell:             raw.Shell,
		CreatedAt:         raw.CreatedAt,
	}
	if c.CPU <= 0 && raw.LegacySMP > 0 {
		c.CPU = raw.LegacySMP
	}
	defaultContainerConfigValues(c)
	return nil
}
