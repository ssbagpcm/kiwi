# Kiwi

Kiwi is a small Go container runtime built around simple commands, `.kiwi` archives, `overlayfs`, plain directories, Linux namespaces, and `cgroup v2`.

The goal is to stay easy enough to learn in two minutes.

## Build

```bash
./build.sh
```

`build.sh` is interactive and can build `linux-amd64`, `linux-arm64`, `linux-armv7`, or all three. Artifacts are written to `./dist/`, and when the selected target matches the current Linux host, `./kiwi` is updated too.

This stores Kiwi data next to the binary in the visible `./kiwi-data/` directory. Host-visible mounts only happen when you explicitly run `./kiwi mount <name> <target>`.

If you tested an older Kiwi build, stop any old container first and remove old local data before re-pulling:

```bash
sudo ./kiwi stop demo 2>/dev/null || true
rm -rf ./kiwi-data
sudo ./kiwi pull alpine
```

## Fast Start

```bash
sudo ./kiwi pull alpine
sudo ./kiwi create alpine
sudo ./kiwi set 8f3b9d2a1c44 --memory 512M --cpu 2 --storage 3G --shell /bin/bash
sudo ./kiwi attach 8f3b9d2a1c44
sudo ./kiwi stop 8f3b9d2a1c44
```

`kiwi create` generates a container id automatically, `kiwi set` changes CPU/RAM/storage/default shell without starting anything, and `kiwi attach` starts the container automatically if needed. `--memory host`, `--cpu host`, and `--storage host` remove the corresponding limits and share the host capacity.

`kiwi attach` accepts a one-shot command, and without a command it chooses a shell automatically (`bash`, `zsh`, `fish`, `ash`, `sh`) or uses the one saved with `kiwi set --shell`.

Default `attach` opens a reusable Kiwi-managed session on the host, so you can reattach later with `./kiwi attach --old <session>`. If you want a direct shell without the session daemon, use `./kiwi attach --direct <id>`.

Kiwi writes a fresh `/etc/resolv.conf` inside the container from the host DNS configuration and falls back to public resolvers when the host only exposes loopback DNS.

```bash
sudo ./kiwi attach 8f3b9d2a1c44
sudo ./kiwi attach --direct 8f3b9d2a1c44
sudo ./kiwi attach --session 8f3b9d2a1c44
sudo ./kiwi attach --old 5cf2a9d96b2e
sudo ./kiwi attach 8f3b9d2a1c44 -- uname -a
./kiwi create ./kiwi-data/8f3b9d2a1c44-base.kiwi
```

Legacy direct build also works:

```bash
go build -o kiwi .

sudo ./kiwi pull alpine
sudo ./kiwi create alpine
sudo ./kiwi attach 8f3b9d2a1c44
sudo ./kiwi stop 8f3b9d2a1c44
```

Built-in test images:

- `kiwi pull alpine` downloads the stock Alpine cloud rootfs and writes `alpine.kiwi`
- `kiwi pull debian` downloads the stock Debian cloud rootfs and writes `debian.kiwi`

## Commands

```text
./kiwi list [containers|images|all]
./kiwi pull <alpine|debian>
./kiwi import --name <image> /path/rootfs.tar.xz
./kiwi create <image|file.kiwi> [--storage 1G]
./kiwi set <name> [--memory 256M|host] [--cpu 1|host] [--storage 2G|host] [--network host|separate] [--shell /bin/bash]
./kiwi sessions <name> [--kill <session>] [--delete <session>]
./kiwi stop <name>
./kiwi delete <name> [--yes]
sudo ./kiwi attach <name> [-- command args]
sudo ./kiwi attach --direct <name> [-- command args]
sudo ./kiwi attach --session <name>
sudo ./kiwi attach --old <session>
./kiwi ip <name>
./kiwi cleanup
./kiwi mount <name> <target>
./kiwi unmount <name>
./kiwi snap <name> <snapshot>
./kiwi commit <container> <image>
./kiwi terminal
```

## Design

- base image: extracted `rootfs/`, read-only in practice
- container state: `state/upper` + `state/work`
- runtime view: `overlayfs`
- startup: `pivot_root`, minimal `/dev`, `proc`, `sys`, `tmpfs`
- limits: real `cgroup v2` `memory.max`, `memory.swap.max`, `cpu.max`, `pids.max`
- cpu visibility: Kiwi also applies a `cpuset`, so interactive shells inherit the container CPU set instead of the full host set
- network: one private IPv4 per container on `kiwi0`
- access: use `sudo ./kiwi attach <name>`; it starts the container if needed and opens a sensible shell automatically
- sessions: plain `attach` opens a reusable Kiwi-managed session; `attach --direct <name>` bypasses it, `attach --old <session>` reattaches to it, and `./kiwi sessions <name>` lists or removes sessions
- naming: if you omit the name, Kiwi creates a short docker-style hex id automatically
- archives: `./kiwi create file.kiwi` restores a container directly from a `.kiwi` archive
- mounts: `./kiwi mount <name> <target>` creates the single host-visible mount of the full container filesystem; when the container is already running Kiwi bind-mounts its live rootfs, and plain `attach` does not mount on the host

At runtime:

```text
rootfs/ (ro view)   -> lowerdir
state/upper (rw)    -> upperdir
state/work          -> workdir
overlayfs           -> merged /
```

## What `commit` Means

`kiwi commit demo demo-v1` takes the current merged view of the container and turns it into a new reusable base image.

Use it when:

- you installed packages in a container and want to reuse that state
- you configured a service once and want future containers to start from it

In short:

- `snap` = write a reusable container archive directly in `./kiwi-data/<id>-<name>.kiwi`
- `commit` = make a new base image from that container

## Requirements

- Linux
- root privileges for attach, stop, mount, namespaces, network, and cgroups
- `mount`
- `ip`
- `nsenter`

## Notes

- each container gets its own private IPv4, so port `6000` on the host and port `6000` in a container do not conflict
- by default Kiwi stores everything near the executable: `./kiwi-data/images`, `./kiwi-data/containers`, `./kiwi-data/mounts`, `./kiwi-data/exports`
- if an old `./.kiwi` directory exists, Kiwi renames it to the visible `./kiwi-data`
- if an old `./kiwi-data/run` exists, Kiwi renames it to `./kiwi-data/mounts`
- `set --memory host`, `set --cpu host`, and `set --storage host` disable the corresponding limits
- `set --storage` never goes below `1G`, and also refuses anything smaller than current usage plus a 1G safety margin
- `create file.kiwi` refuses a disk smaller than the archive it is restoring
- `./kiwi terminal` relaunches itself through `sudo` when needed so the shell keeps the required permissions
- `sudo ./kiwi cleanup` removes orphaned runtime mounts left behind after a partial manual delete or crash
test
