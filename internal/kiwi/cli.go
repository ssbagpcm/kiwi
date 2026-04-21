package kiwi

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/term"
)

func Main(args []string) error {
	if len(args) == 0 {
		fmt.Println(banner())
		fmt.Println(usageText())
		return nil
	}

	store := NewStore()
	if os.Geteuid() == 0 {
		_ = store.CleanupOrphanRuntime()
	}
	switch args[0] {
	case "help", "--help", "-h":
		fmt.Println(banner())
		fmt.Println(usageText())
		return nil
	case "pull":
		return handlePull(store, args[1:])
	case "list", "ls":
		return handleList(store, args[1:])
	case "import":
		return handleImport(store, args[1:])
	case "create", "new":
		return handleCreate(store, args[1:])
	case "set", "config":
		return handleSet(store, args[1:])
	case "sessions":
		return handleSessions(store, args[1:])
	case "stop":
		return handleStop(store, args[1:])
	case "delete", "rm":
		return handleDelete(store, args[1:])
	case "mount":
		return handleMount(store, args[1:])
	case "umount", "unmount":
		return handleUnmount(store, args[1:])
	case "ip":
		return handleIP(store, args[1:])
	case "attach":
		return handleAttach(store, args[1:])
	case "terminal":
		return RunTerminal()
	case "cleanup":
		return handleCleanup(store, args[1:])
	case "killall":
		return handleKillAll(store, args[1:])
	case "force-cleanup":
		return handleForceCleanup(store, args[1:])
	case "snap":
		return handleSnapshot(store, args[1:])
	case "build":
		return handleBuild(store, args[1:])
	case "commit":
		return handleCommit(store, args[1:])
	case "__sessiond":
		return handleSessionDaemon(args[1:])
	case "__enter":
		return handleEnter(args[1:])
	case "__chroot":
		return handleChroot(args[1:])
	default:
		return fmt.Errorf("command %q does not exist. try: help", args[0])
	}
}

func handlePull(store Store, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: ./kiwi pull <image> [image ...]")
	}
	for _, imageName := range args {
		result, _, err := store.PullBuiltInImage(imageName)
		if err != nil {
			return err
		}
		fmt.Println(result.Name)
	}
	return nil
}

func handleImport(store Store, args []string) error {
	fs := flag.NewFlagSet("import", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	name := fs.String("name", "", "image name")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if len(fs.Args()) != 1 {
		return fmt.Errorf("usage: ./kiwi import --name <image> /path/rootfs.tar.xz")
	}
	manifest, err := withSpinner("importing "+sanitizeName(*name), func() (ImageManifest, error) {
		return store.ImportImage(*name, fs.Args()[0])
	})
	if err != nil {
		return err
	}
	fmt.Printf("imported %s -> %s\n", manifest.Name, manifest.RootfsPath)
	return nil
}

func handleCreate(store Store, args []string) error {
	parsed, err := parseCreateArgs(args)
	if err != nil {
		return err
	}
	sizeBytes := defaultStateSize
	if looksLikeArchiveSource(parsed.Source) {
		sizeBytes = 0
	}
	if parsed.HasSize {
		sizeBytes, err = parseSize(parsed.Size)
		if err != nil {
			return err
		}
	}
	var config ContainerConfig
	if looksLikeArchiveSource(parsed.Source) {
		config, err = store.CreateContainerFromArchive(parsed.Source, sizeBytes, parsed.HasSize)
	} else {
		config, err = store.NewContainer(parsed.Source, "", sizeBytes)
	}
	if err != nil {
		return err
	}
	fmt.Println(config.Name)
	return nil
}

func handleList(store Store, args []string) error {
	mode := "containers"
	if len(args) > 1 {
		return fmt.Errorf("usage: ./kiwi list [containers|images|all]")
	}
	if len(args) == 1 {
		mode = sanitizeName(args[0])
	}
	switch mode {
	case "all", "images", "containers":
	default:
		return fmt.Errorf("usage: ./kiwi list [containers|images|all]")
	}
	if mode == "all" || mode == "images" {
		images, err := store.ListImages()
		if err != nil {
			return err
		}
		for _, image := range images {
			fmt.Printf("image %s\n", image.Name)
		}
	}
	if mode == "all" && len(args) == 0 {
		mode = "containers"
	}
	if mode == "all" || mode == "containers" {
		containers, err := store.ListContainers()
		if err != nil {
			return err
		}
		for _, container := range containers {
			status := "stopped"
			mountTarget := ""
			state, err := store.LoadRuntimeState(container.Name)
			if err == nil {
				if state.Running && processAlive(state.PID) {
					status = "running"
				} else if state.TargetMountpoint != "" && isMounted(state.TargetMountpoint) {
					status = "mounted"
				}
				if state.TargetMountpoint != "" && isMounted(state.TargetMountpoint) {
					mountTarget = " mount=" + filepath.Clean(state.TargetMountpoint)
				}
			}
			fmt.Printf("%s %s image=%s memory=%s cpu=%s network=%s%s\n", container.Name, status, container.Image, displayMemory(container.Memory), displayCPU(container.CPU, container.CPUHost), displayNetwork(container.Network), mountTarget)
		}
	}
	return nil
}

func handleSet(store Store, args []string) error {
	name, options, err := parseSetArgs(args)
	if err != nil {
		return err
	}
	config, err := store.UpdateContainerResources(name, options)
	if err != nil {
		return err
	}
	fmt.Println(config.Name)
	return nil
}

func handleSessions(store Store, args []string) error {
	parsed, err := parseSessionsArgs(args)
	if err != nil {
		return err
	}
	if parsed.DeleteID != "" {
		if err := store.DeleteManagedSession(parsed.Name, parsed.DeleteID); err != nil {
			return err
		}
		fmt.Println(parsed.DeleteID)
		return nil
	}
	if parsed.KillID != "" {
		if err := store.StopSessionProcesses(parsed.Name, parsed.KillID); err != nil {
			return err
		}
		fmt.Println(parsed.KillID)
		return nil
	}
	sessions, err := store.ListSessions(parsed.Name)
	if err != nil {
		return err
	}
	if _, err := store.LoadContainer(parsed.Name); err != nil {
		return err
	}
	for _, session := range sessions {
		state := "idle"
		if session.Attached {
			state = "attached"
		} else if session.ShellRunning && processAlive(session.ShellPID) {
			state = "running"
		}
		fmt.Printf("%s %s\n", session.ID, state)
	}
	return nil
}

func handleStop(store Store, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: ./kiwi stop <id>")
	}
	if err := store.StopManagedContainer(args[0]); err != nil {
		return err
	}
	fmt.Println(sanitizeName(args[0]))
	return nil
}

func handleDelete(store Store, args []string) error {
	parsed, err := parseDeleteArgs(args)
	if err != nil {
		return err
	}
	if !parsed.Yes {
		confirmed, err := confirmDelete(parsed.Name)
		if err != nil {
			return err
		}
		if !confirmed {
			return fmt.Errorf("delete canceled")
		}
	}
	if err := store.DeleteContainer(parsed.Name); err != nil {
		return err
	}
	fmt.Println(parsed.Name)
	return nil
}

func handleMount(store Store, args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("usage: ./kiwi mount <id> <path>")
	}
	targetPath := expandHomeDir(args[1])
	state, err := store.MountLiveContainer(args[0], targetPath)
	if err != nil {
		return err
	}
	fmt.Println(state.Name)
	return nil
}

func handleUnmount(store Store, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: ./kiwi unmount <id>")
	}
	if err := store.UnmountContainer(args[0]); err != nil {
		return err
	}
	fmt.Println(sanitizeName(args[0]))
	return nil
}

func handleIP(store Store, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: ./kiwi ip <name>")
	}
	config, err := store.LoadContainer(args[0])
	if err != nil {
		return err
	}
	fmt.Println(config.IPv4)
	return nil
}

func handleAttach(store Store, args []string) error {
	parsed, err := parseAttachArgs(args)
	if err != nil {
		return err
	}
	if parsed.OldSession != "" {
		session, err := store.FindSession(parsed.OldSession)
		if err != nil {
			return err
		}
		if err := store.AttachInteractiveSession(session.Container, session.ID); err != nil {
			return err
		}
		printBlock("", "reattach: sudo ./kiwi attach --old "+parsed.OldSession)
		return nil
	}
	config, err := store.LoadContainer(parsed.Name)
	if err != nil {
		return err
	}
	if len(parsed.Command) == 0 && !parsed.Direct {
		sessionID, err := generateSessionID()
		if err != nil {
			return err
		}
		fmt.Println("session:  " + sessionID)
		if err := store.AttachInteractiveSession(config.Name, sessionID); err != nil {
			return err
		}
		printBlock("", "reattach: sudo ./kiwi attach --old "+sessionID)
		return nil
	}
	if len(parsed.Command) == 0 {
		return store.AttachContainer(config.Name, nil)
	}
	state, err := store.LoadRuntimeState(config.Name)
	if err != nil || !state.Running || !processAlive(state.PID) {
		if _, err := withSpinner("starting "+sanitizeName(config.Name), func() (RuntimeState, error) {
			return store.StartContainer(config.Name, StartOptions{})
		}); err != nil {
			return err
		}
	}
	return store.AttachContainer(parsed.Name, parsed.Command)
}

func handleSnapshot(store Store, args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("usage: ./kiwi snap <id> <name>")
	}
	state, _ := store.LoadRuntimeState(args[0])
	if state.Running && processAlive(state.PID) {
		return fmt.Errorf("container %q is running, stop it first", args[0])
	}
	path, err := withSpinner("snapshotting "+args[0], func() (string, error) {
		return store.SnapshotContainer(args[0], args[1])
	})
	if err != nil {
		return err
	}
	printBlock("", "snapshot: "+path)
	return nil
}

func handleCleanup(store Store, args []string) error {
	if len(args) != 0 {
		return fmt.Errorf("usage: ./kiwi cleanup")
	}
	if os.Geteuid() != 0 {
		return fmt.Errorf("cleanup requires root; run with sudo")
	}
	if err := store.CleanupOrphanRuntime(); err != nil {
		return err
	}
	fmt.Println("clean")
	return nil
}

func handleKillAll(store Store, args []string) error {
	if len(args) > 1 || (len(args) == 1 && args[0] != "--yes") {
		return fmt.Errorf("usage: ./kiwi killall [--yes]")
	}
	if os.Geteuid() != 0 {
		return fmt.Errorf("killall requires root; run with sudo")
	}
	if len(args) == 0 {
		confirmed, err := confirmDelete("all kiwi processes and mounts")
		if err != nil {
			return err
		}
		if !confirmed {
			return fmt.Errorf("killall canceled")
		}
	}
	if err := store.KillAllManaged(); err != nil {
		return err
	}
	fmt.Println("killed")
	return nil
}

func handleForceCleanup(store Store, args []string) error {
	if len(args) != 0 {
		return fmt.Errorf("usage: ./kiwi force-cleanup")
	}
	if os.Geteuid() != 0 {
		return fmt.Errorf("force-cleanup requires root; run with sudo")
	}
	if err := store.ForceCleanupAll(); err != nil {
		return err
	}
	fmt.Println("force-cleaned")
	return nil
}

func handleCommit(store Store, args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("usage: ./kiwi commit <container> <image>")
	}
	result, err := withSpinner("committing "+sanitizeName(args[0]), func() (struct {
		manifest ImageManifest
		archive  string
	}, error) {
		manifest, archive, err := store.CommitContainer(args[0], args[1])
		return struct {
			manifest ImageManifest
			archive  string
		}{manifest: manifest, archive: archive}, err
	})
	if err != nil {
		return err
	}
	printBlock("", "committed: "+sanitizeName(args[0])+" -> "+result.manifest.Name, "archive:   "+result.archive)
	return nil
}

func handleEnter(args []string) error {
	before, command := splitArgsOnDoubleDash(args)
	fs := flag.NewFlagSet("__enter", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	root := fs.String("root", "", "runtime root")
	image := fs.String("image", "", "base squashfs path")
	state := fs.String("state", "", "state image path")
	name := fs.String("name", "", "container hostname")
	ipv4 := fs.String("ipv4", "", "container ipv4")
	syncFD := fs.Int("sync-fd", 0, "startup sync fd")
	if err := fs.Parse(before); err != nil {
		return err
	}
	return EnterContainer(*root, *image, *state, *name, *ipv4, *syncFD, command)
}

func handleSessionDaemon(args []string) error {
	fs := flag.NewFlagSet("__sessiond", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	name := fs.String("name", "", "container name")
	sessionID := fs.String("session", "", "session id")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *name == "" || *sessionID == "" {
		return fmt.Errorf("missing session parameters")
	}
	return RunSessionServer(*name, *sessionID)
}

func handleChroot(args []string) error {
	fs := flag.NewFlagSet("__chroot", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	root := fs.String("root", "", "rootfs path")
	shell := fs.String("shell", "/bin/sh", "shell to run")
	hostname := fs.String("hostname", "", "session hostname")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *root == "" {
		return fmt.Errorf("--root is required")
	}
	return RunChrootSession(*root, *shell, *hostname)
}

func usageText() string {
	return strings.TrimSpace(`kiwi keeps containers simple.

  ./kiwi pull debian alpine
  ./kiwi create alpine
  sudo ./kiwi attach <id>

  ./kiwi list [containers|images|all]
  ./kiwi pull <image> [image ...]
  ./kiwi import --name <image> /path/rootfs.tar.xz
  ./kiwi create <image|file.kiwi> [--size 1G]
  ./kiwi set <id> [--memory 256M|host] [--cpu 1|host] [--storage 2G|host] [--network host|separate] [--shell /bin/bash]
  sudo ./kiwi attach <id> [-- command args]
  sudo ./kiwi attach --direct <id> [-- command args]
  sudo ./kiwi attach --session <id>
  sudo ./kiwi attach --old <session>
  ./kiwi sessions <id> [--kill <session>] [--delete <session>]
  ./kiwi stop <id>
  ./kiwi delete <id> [--yes]
  ./kiwi mount <id> <path>
  ./kiwi unmount <id>
  ./kiwi ip <id>
  ./kiwi cleanup
  ./kiwi killall [--yes]
  ./kiwi force-cleanup
  ./kiwi snap <id> <name>
  ./kiwi commit <container> <image>
  ./kiwi terminal`)
}

type createArgs struct {
	Source  string
	Size    string
	HasSize bool
}

func parseCreateArgs(args []string) (createArgs, error) {
	before, _ := splitArgsOnDoubleDash(args)
	parsed := createArgs{}
	for index := 0; index < len(before); index++ {
		arg := before[index]
		switch {
		case arg == "--size":
			index++
			if index >= len(before) {
				return createArgs{}, fmt.Errorf("usage: ./kiwi create <image|file.kiwi> [--size 1G]")
			}
			parsed.Size = before[index]
			parsed.HasSize = true
		case strings.HasPrefix(arg, "--size="):
			parsed.Size = strings.TrimPrefix(arg, "--size=")
			parsed.HasSize = true
		case strings.HasPrefix(arg, "-"):
			return createArgs{}, fmt.Errorf("unknown create flag %q", arg)
		default:
			if parsed.Source == "" {
				parsed.Source = arg
			} else {
				return createArgs{}, fmt.Errorf("usage: ./kiwi create <image|file.kiwi> [--size 1G]")
			}
		}
	}
	if parsed.Source == "" {
		return createArgs{}, fmt.Errorf("usage: ./kiwi create <image|file.kiwi> [--size 1G]")
	}
	return parsed, nil
}

func parseSetArgs(args []string) (string, StartOptions, error) {
	before, _ := splitArgsOnDoubleDash(args)
	options := StartOptions{}
	name := ""
	if err := parseResourceArgs(before, &name, &options, "usage: ./kiwi set <id> [--memory 256M|host] [--cpu 1|host] [--storage 2G|host] [--network host|separate] [--shell /bin/bash]"); err != nil {
		return "", StartOptions{}, err
	}
	if name == "" {
		return "", StartOptions{}, fmt.Errorf("usage: ./kiwi set <id> [--memory 256M|host] [--cpu 1|host] [--storage 2G|host] [--network host|separate] [--shell /bin/bash]")
	}
	if options.Memory == "" && options.CPU <= 0 && !options.CPUHost && strings.TrimSpace(options.Shell) == "" && strings.TrimSpace(options.Network) == "" && strings.TrimSpace(options.Storage) == "" && !options.StorageHost {
		return "", StartOptions{}, fmt.Errorf("usage: ./kiwi set <id> [--memory 256M|host] [--cpu 1|host] [--storage 2G|host] [--network host|separate] [--shell /bin/bash]")
	}
	return name, options, nil
}

type attachArgs struct {
	Name       string
	OldSession string
	Session    bool
	Direct     bool
	Command    []string
}

type sessionsArgs struct {
	Name     string
	KillID   string
	DeleteID string
}

type deleteArgs struct {
	Name string
	Yes  bool
}

func parseAttachArgs(args []string) (attachArgs, error) {
	before, command := splitArgsOnDoubleDash(args)
	if len(before) == 2 && before[0] == "--direct" {
		return attachArgs{Name: before[1], Direct: true, Command: command}, nil
	}
	if len(before) == 2 && before[0] == "--session" {
		return attachArgs{Name: before[1], Session: true, Command: command}, nil
	}
	if len(before) == 2 && before[0] == "--old" {
		return attachArgs{OldSession: before[1]}, nil
	}
	if len(before) != 1 {
		return attachArgs{}, fmt.Errorf("usage: ./kiwi attach <id> [-- command args]\n       ./kiwi attach --direct <id> [-- command args]\n       ./kiwi attach --session <id>\n       ./kiwi attach --old <session>")
	}
	return attachArgs{Name: before[0], Command: command}, nil
}

func parseSessionsArgs(args []string) (sessionsArgs, error) {
	parsed := sessionsArgs{}
	for index := 0; index < len(args); index++ {
		arg := args[index]
		switch arg {
		case "--kill":
			index++
			if index >= len(args) {
				return sessionsArgs{}, fmt.Errorf("usage: ./kiwi sessions <id> [--kill <session>] [--delete <session>]")
			}
			parsed.KillID = sanitizeName(args[index])
		case "--delete":
			index++
			if index >= len(args) {
				return sessionsArgs{}, fmt.Errorf("usage: ./kiwi sessions <id> [--kill <session>] [--delete <session>]")
			}
			parsed.DeleteID = sanitizeName(args[index])
		default:
			if strings.HasPrefix(arg, "-") {
				return sessionsArgs{}, fmt.Errorf("unknown flag %q", arg)
			}
			if parsed.Name != "" {
				return sessionsArgs{}, fmt.Errorf("usage: ./kiwi sessions <id> [--kill <session>] [--delete <session>]")
			}
			parsed.Name = sanitizeName(arg)
		}
	}
	if parsed.Name == "" {
		return sessionsArgs{}, fmt.Errorf("usage: ./kiwi sessions <id> [--kill <session>] [--delete <session>]")
	}
	if parsed.KillID != "" && parsed.DeleteID != "" {
		return sessionsArgs{}, fmt.Errorf("choose only one of --kill or --delete")
	}
	return parsed, nil
}

func parseDeleteArgs(args []string) (deleteArgs, error) {
	parsed := deleteArgs{}
	for _, arg := range args {
		switch arg {
		case "--yes":
			parsed.Yes = true
		default:
			if strings.HasPrefix(arg, "-") {
				return deleteArgs{}, fmt.Errorf("unknown flag %q", arg)
			}
			if parsed.Name != "" {
				return deleteArgs{}, fmt.Errorf("usage: ./kiwi delete <id> [--yes]")
			}
			parsed.Name = sanitizeName(arg)
		}
	}
	if parsed.Name == "" {
		return deleteArgs{}, fmt.Errorf("usage: ./kiwi delete <id> [--yes]")
	}
	return parsed, nil
}

func parseResourceArgs(args []string, name *string, options *StartOptions, usage string) error {
	for index := 0; index < len(args); index++ {
		arg := args[index]
		switch {
		case arg == "-m" || arg == "--memory":
			index++
			if index >= len(args) {
				return fmt.Errorf(usage)
			}
			options.Memory = args[index]
		case strings.HasPrefix(arg, "-m="):
			options.Memory = strings.TrimPrefix(arg, "-m=")
		case arg == "-c" || arg == "--cpu" || arg == "-smp" || arg == "--smp":
			index++
			if index >= len(args) {
				return fmt.Errorf(usage)
			}
			if strings.EqualFold(strings.TrimSpace(args[index]), "host") {
				options.CPU = 0
				options.CPUHost = true
				continue
			}
			value, err := strconv.Atoi(args[index])
			if err != nil {
				return err
			}
			options.CPU = value
			options.CPUHost = false
		case strings.HasPrefix(arg, "--cpu="):
			raw := strings.TrimPrefix(arg, "--cpu=")
			if strings.EqualFold(strings.TrimSpace(raw), "host") {
				options.CPU = 0
				options.CPUHost = true
				continue
			}
			value, err := strconv.Atoi(raw)
			if err != nil {
				return err
			}
			options.CPU = value
			options.CPUHost = false
		case strings.HasPrefix(arg, "-smp="):
			value, err := strconv.Atoi(strings.TrimPrefix(arg, "-smp="))
			if err != nil {
				return err
			}
			options.CPU = value
		case arg == "--shell":
			index++
			if index >= len(args) {
				return fmt.Errorf(usage)
			}
			options.Shell = args[index]
		case strings.HasPrefix(arg, "--shell="):
			options.Shell = strings.TrimPrefix(arg, "--shell=")
		case arg == "--network":
			index++
			if index >= len(args) {
				return fmt.Errorf(usage)
			}
			options.Network = args[index]
		case strings.HasPrefix(arg, "--network="):
			options.Network = strings.TrimPrefix(arg, "--network=")
		case arg == "--storage":
			index++
			if index >= len(args) {
				return fmt.Errorf(usage)
			}
			options.Storage = args[index]
			options.StorageHost = strings.EqualFold(strings.TrimSpace(args[index]), "host")
		case strings.HasPrefix(arg, "--storage="):
			options.Storage = strings.TrimPrefix(arg, "--storage=")
			options.StorageHost = strings.EqualFold(strings.TrimSpace(options.Storage), "host")
		case strings.HasPrefix(arg, "-"):
			return fmt.Errorf("unknown flag %q", arg)
		default:
			if *name == "" {
				*name = arg
			} else {
				return fmt.Errorf(usage)
			}
		}
	}
	return nil
}

func looksLikeArchiveSource(value string) bool {
	return strings.HasSuffix(strings.ToLower(strings.TrimSpace(value)), ".kiwi")
}

func confirmDelete(name string) (bool, error) {
	if !term.IsTerminal(int(os.Stdin.Fd())) {
		return false, fmt.Errorf("delete requires confirmation on a terminal; use --yes to force it")
	}
	fmt.Printf("delete %s? [y/N]: ", name)
	reader := bufio.NewReader(os.Stdin)
	line, err := reader.ReadString('\n')
	if err != nil {
		return false, err
	}
	switch strings.ToLower(strings.TrimSpace(line)) {
	case "y", "yes":
		return true, nil
	default:
		return false, nil
	}
}
