package kiwi

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type kiwifileConfig struct {
	RAM     string
	Storage string
	CPU     int
	Network string
}

type kiwifileInstruction struct {
	Type string
	Args string
}

type kiwifile struct {
	config      kiwifileConfig
	instructions []kiwifileInstruction
	fromImage   string
}

func parseKiwifile(path string) (*kiwifile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}

	kf := &kiwifile{}
	inConfig := false
	inMultiline := false
	var currentArgs strings.Builder
	var multilineArgs strings.Builder

	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		if inMultiline {
			if strings.HasSuffix(line, "*/") {
				multilineArgs.WriteString(strings.TrimSuffix(line, "*/"))
				kf.instructions = append(kf.instructions, kiwifileInstruction{
					Type: "RUN",
					Args: strings.TrimSpace(multilineArgs.String()),
				})
				multilineArgs.Reset()
				inMultiline = false
			} else {
				multilineArgs.WriteString(line + "\n")
			}
			continue
		}

		if strings.HasPrefix(line, "/*") {
			if currentArgs.Len() > 0 {
				kf.instructions = append(kf.instructions, kiwifileInstruction{
					Type: "RUN",
					Args: strings.TrimSpace(currentArgs.String()),
				})
				currentArgs.Reset()
			}
			inMultiline = true
			multilineArgs.WriteString(strings.TrimPrefix(line, "/*") + "\n")
			continue
		}

		if strings.HasPrefix(line, "<config>") {
			inConfig = true
			continue
		}
		if strings.HasPrefix(line, "</config>") {
			inConfig = false
			continue
		}

		if inConfig {
			if strings.HasPrefix(line, "ram=") {
				kf.config.RAM = strings.TrimPrefix(line, "ram=")
			} else if strings.HasPrefix(line, "storage=") {
				kf.config.Storage = strings.TrimPrefix(line, "storage=")
			} else if strings.HasPrefix(line, "cpu=") {
				fmt.Sscanf(strings.TrimPrefix(line, "cpu="), "%d", &kf.config.CPU)
			} else if strings.HasPrefix(line, "network=") {
				kf.config.Network = strings.TrimPrefix(line, "network=")
			}
			continue
		}

		if strings.HasPrefix(line, "FROM ") {
			kf.fromImage = strings.TrimSpace(strings.TrimPrefix(line, "FROM "))
		} else if strings.HasPrefix(line, "RUN ") {
			if currentArgs.Len() > 0 {
				kf.instructions = append(kf.instructions, kiwifileInstruction{
					Type: "RUN",
					Args: strings.TrimSpace(currentArgs.String()),
				})
			}
			currentArgs.Reset()
			currentArgs.WriteString(strings.TrimPrefix(line, "RUN "))
		} else if strings.HasSuffix(line, "\\") {
			currentArgs.WriteString(strings.TrimSuffix(line, "\\"))
			currentArgs.WriteString(" ")
		} else if line != "" {
			if currentArgs.Len() > 0 {
				currentArgs.WriteString(line)
				kf.instructions = append(kf.instructions, kiwifileInstruction{
					Type: "RUN",
					Args: strings.TrimSpace(currentArgs.String()),
				})
				currentArgs.Reset()
			} else if !inConfig && !strings.HasPrefix(line, "#") {
				kf.instructions = append(kf.instructions, kiwifileInstruction{
					Type: "RUN",
					Args: line,
				})
			}
		}
	}

	if inMultiline {
		return nil, fmt.Errorf("unclosed multiline comment in %s", path)
	}

	if currentArgs.Len() > 0 {
		kf.instructions = append(kf.instructions, kiwifileInstruction{
			Type: "RUN",
			Args: strings.TrimSpace(currentArgs.String()),
		})
	}

	return kf, nil
}

func handleBuild(store Store, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: ./kiwi build <Kiwifile>")
	}

	kiwiFile := args[0]
	if _, err := os.Stat(kiwiFile); err != nil {
		return fmt.Errorf("Kiwifile not found: %s", kiwiFile)
	}

	kf, err := parseKiwifile(kiwiFile)
	if err != nil {
		return fmt.Errorf("parse Kiwifile: %w", err)
	}

	if kf.fromImage == "" {
		return fmt.Errorf("Kiwifile must have a FROM instruction")
	}

	fmt.Printf("Building from %s...\n", kf.fromImage)

	baseImage := kf.fromImage
	if strings.HasSuffix(strings.ToLower(baseImage), ".kiwi") {
		baseImage = strings.TrimSuffix(baseImage, ".kiwi")
	}

	manifest, err := store.LoadImage(baseImage)
	if err != nil {
		return fmt.Errorf("image %q not found, pull it first", baseImage)
	}

	workspace, err := os.MkdirTemp("", "kiwi-build-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(workspace)

	buildRoot := filepath.Join(workspace, "rootfs")
	if err := os.MkdirAll(buildRoot, 0755); err != nil {
		return err
	}

	if err := runCommand("cp", "-a", manifest.RootfsPath+"/.", buildRoot); err != nil {
		return fmt.Errorf("copy rootfs: %w", err)
	}

	// Copy DNS config from host so apt can resolve domains
	// Use static copy of actual nameservers instead of symlink
	hostResolv := "/etc/resolv.conf"
	if _, err := os.Stat("/run/systemd/resolve/stub-resolv.conf"); err == nil {
		hostResolv = "/run/systemd/resolve/stub-resolv.conf"
	}
	resolvContent, err := os.ReadFile(hostResolv)
	if err == nil {
		// Remove symlink if exists and write static resolv.conf
		buildResolv := filepath.Join(buildRoot, "etc/resolv.conf")
		os.Remove(buildResolv)
		if err := os.WriteFile(buildResolv, resolvContent, 0644); err != nil {
			// Ignore if fails
		}
	}

	// Copy host's network namespace to use host's network
	if err := os.MkdirAll(filepath.Join(buildRoot, "proc"), 0755); err != nil {
		return err
	}

	for i, inst := range kf.instructions {
		fmt.Printf("[%d/%d] RUN %s\n", i+1, len(kf.instructions), inst.Args)
		cmd := exec.Command("chroot", buildRoot, "/bin/sh", "-c", inst.Args)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.Env = []string{
			"HOME=/root",
			"TERM=xterm-256color",
			"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
			"DEBIAN_FRONTEND=noninteractive",
		}
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("RUN %q failed: %w", inst.Args, err)
		}
	}

	fmt.Println("Creating image archive...")

	name := filepath.Base(kiwiFile)
	if ext := filepath.Ext(name); ext != "" {
		name = name[:len(name)-len(ext)]
	}
	name = sanitizeName(name)

	if err := os.MkdirAll(store.ImageDir(name), 0755); err != nil {
		return err
	}
	rootfsPath := store.ImageRootfsPath(name)
	if err := runCommand("cp", "-a", buildRoot+"/.", rootfsPath); err != nil {
		return fmt.Errorf("copy build rootfs: %w", err)
	}

	imgManifest := ImageManifest{
		Name:        name,
		RootfsPath:  rootfsPath,
		Format:      "dir",
		PreparedBy:  "kiwi-build",
		Description: fmt.Sprintf("Built from %s", kf.fromImage),
		CreatedAt:   time.Now().UTC(),
	}
	if err := store.SaveImage(imgManifest); err != nil {
		return fmt.Errorf("save image manifest: %w", err)
	}

	archivePath := filepath.Join(store.DataRoot, "images", name+".kiwi")
	if err := store.SaveImageArchive(imgManifest, archivePath); err != nil {
		return fmt.Errorf("save archive: %w", err)
	}

	fmt.Printf("Built %s -> %s\n", name, archivePath)
	return nil
}
