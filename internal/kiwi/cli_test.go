package kiwi

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

func TestGenerateContainerIDLength(t *testing.T) {
	id, err := generateContainerID()
	if err != nil {
		t.Fatalf("generateContainerID() error = %v", err)
	}
	if len(id) != 24 {
		t.Fatalf("generateContainerID() length = %d, want 24", len(id))
	}
}

func TestGenerateSessionIDLength(t *testing.T) {
	id, err := generateSessionID()
	if err != nil {
		t.Fatalf("generateSessionID() error = %v", err)
	}
	if len(id) != 12 {
		t.Fatalf("generateSessionID() length = %d, want 12", len(id))
	}
}

func TestParseCreateArgs(t *testing.T) {
	parsed, err := parseCreateArgs([]string{"alpine", "--size", "2G"})
	if err != nil {
		t.Fatalf("parseCreateArgs() error = %v", err)
	}
	if parsed.Source != "alpine" {
		t.Fatalf("source = %q, want alpine", parsed.Source)
	}
	if parsed.Size != "2G" {
		t.Fatalf("size = %q, want 2G", parsed.Size)
	}
	if !parsed.HasSize {
		t.Fatalf("hasSize = false, want true")
	}
	if _, err := parseCreateArgs([]string{"alpine", "custom-name"}); err == nil {
		t.Fatalf("parseCreateArgs() accepted a custom container name")
	}
	archiveParsed, err := parseCreateArgs([]string{"./arch-vnc-desktop.kiwi"})
	if err != nil {
		t.Fatalf("parseCreateArgs() archive error = %v", err)
	}
	if archiveParsed.Source != "./arch-vnc-desktop.kiwi" {
		t.Fatalf("archive source = %q, want ./arch-vnc-desktop.kiwi", archiveParsed.Source)
	}
	if archiveParsed.HasSize {
		t.Fatalf("archive hasSize = true, want false")
	}
}

func TestParseSetArgs(t *testing.T) {
	name, options, err := parseSetArgs([]string{"abc123", "--memory", "512M", "--cpu", "2", "--network", "host", "--storage", "3G"})
	if err != nil {
		t.Fatalf("parseSetArgs() error = %v", err)
	}
	if name != "abc123" {
		t.Fatalf("name = %q, want abc123", name)
	}
	if options.Memory != "512M" {
		t.Fatalf("memory = %q, want 512M", options.Memory)
	}
	if options.CPU != 2 {
		t.Fatalf("cpu = %d, want 2", options.CPU)
	}
	if options.Network != "host" {
		t.Fatalf("network = %q, want host", options.Network)
	}
	if options.Storage != "3G" {
		t.Fatalf("storage = %q, want 3G", options.Storage)
	}
}

func TestParseSetArgsHostValues(t *testing.T) {
	name, options, err := parseSetArgs([]string{"abc123", "--memory", "host", "--cpu", "host", "--storage", "host"})
	if err != nil {
		t.Fatalf("parseSetArgs() error = %v", err)
	}
	if name != "abc123" {
		t.Fatalf("name = %q, want abc123", name)
	}
	if options.Memory != "host" {
		t.Fatalf("memory = %q, want host", options.Memory)
	}
	if !options.CPUHost {
		t.Fatalf("CPUHost = false, want true")
	}
	if !options.StorageHost {
		t.Fatalf("StorageHost = false, want true")
	}
}

func TestParseDeleteArgs(t *testing.T) {
	parsed, err := parseDeleteArgs([]string{"deadbeef", "--yes"})
	if err != nil {
		t.Fatalf("parseDeleteArgs() error = %v", err)
	}
	if parsed.Name != "deadbeef" {
		t.Fatalf("name = %q, want deadbeef", parsed.Name)
	}
	if !parsed.Yes {
		t.Fatalf("yes = false, want true")
	}
}

func TestParseCommandLine(t *testing.T) {
	fields, err := parseCommandLine(`attach abc -- printf "hello world"`)
	if err != nil {
		t.Fatalf("parseCommandLine() error = %v", err)
	}
	want := []string{"attach", "abc", "--", "printf", "hello world"}
	if len(fields) != len(want) {
		t.Fatalf("field count = %d, want %d", len(fields), len(want))
	}
	for i := range want {
		if fields[i] != want[i] {
			t.Fatalf("field[%d] = %q, want %q", i, fields[i], want[i])
		}
	}
	if _, err := parseCommandLine(`attach "abc`); err == nil {
		t.Fatalf("parseCommandLine() accepted an unterminated quote")
	}
}

func TestContainerConfigLegacySMPCompatibility(t *testing.T) {
	raw := []byte(`{
  "name": "abc123",
  "image": "debian",
  "state_path": "/tmp/state.img",
  "state_size_bytes": 1024,
  "state_backend": "image",
  "ipv4": "10.44.0.10",
  "gateway": "10.44.0.1",
  "memory": "512M",
  "smp": 3,
  "created_at": "2026-04-16T19:00:00Z"
}`)
	var config ContainerConfig
	if err := json.Unmarshal(raw, &config); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if config.CPU != 3 {
		t.Fatalf("cpu = %d, want 3", config.CPU)
	}
	if config.Network != defaultNetwork {
		t.Fatalf("network = %q, want %q", config.Network, defaultNetwork)
	}
	config.CreatedAt = time.Date(2026, 4, 16, 19, 0, 0, 0, time.UTC)
	encoded, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if !bytes.Contains(encoded, []byte(`"cpu":3`)) {
		t.Fatalf("encoded config missing cpu field: %s", encoded)
	}
	if bytes.Contains(encoded, []byte(`"smp"`)) {
		t.Fatalf("encoded config still contains legacy smp field: %s", encoded)
	}
}

func TestParseCPUSet(t *testing.T) {
	cpus, err := parseCPUSet("0-2,4,6-7")
	if err != nil {
		t.Fatalf("parseCPUSet() error = %v", err)
	}
	want := []int{0, 1, 2, 4, 6, 7}
	if len(cpus) != len(want) {
		t.Fatalf("parseCPUSet() len = %d, want %d", len(cpus), len(want))
	}
	for index := range want {
		if cpus[index] != want[index] {
			t.Fatalf("cpu[%d] = %d, want %d", index, cpus[index], want[index])
		}
	}
}

func TestLimitCPUSet(t *testing.T) {
	value, err := limitCPUSet("0-5", 2)
	if err != nil {
		t.Fatalf("limitCPUSet() error = %v", err)
	}
	if value != "0-1" {
		t.Fatalf("limitCPUSet() = %q, want 0-1", value)
	}
	value, err = limitCPUSet("2,4,6", 2)
	if err != nil {
		t.Fatalf("limitCPUSet() sparse error = %v", err)
	}
	if value != "2,4" {
		t.Fatalf("limitCPUSet() sparse = %q, want 2,4", value)
	}
}
