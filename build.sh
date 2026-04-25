#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)"
DIST_DIR="$ROOT_DIR/dist"
HOST_GOOS="$(go env GOHOSTOS)"
HOST_GOARCH="$(go env GOHOSTARCH)"

cat <<'EOF'
,--. ,--.,--.,--.   ,--.,--.
|  .'   /|  ||  |   |  ||  |
|  .   ' |  ||  |.'.|  ||  |
|  |\   \|  ||   ,'.   ||  |
`--' '--'`--''--'   '--'`--'

EOF

mkdir -p "$DIST_DIR"
cd "$ROOT_DIR"


build_target() {
  local goarch="$1"
  local goarm="${2:-}"
  local label="$3"
  local output="$DIST_DIR/kiwi-$label"

  printf '%s\n' "[..] building $label"
  if [[ -n "$goarm" ]]; then
    GOOS=linux GOARCH="$goarch" GOARM="$goarm" go build -ldflags='-s -w' -o "$output" .
  else
    GOOS=linux GOARCH="$goarch" go build -ldflags='-s -w' -o "$output" .
  fi
  chmod +x "$output"
  printf '%s\n' "[ok] built $output"

  if [[ "$HOST_GOOS" == "linux" && "$HOST_GOARCH" == "$goarch" && -z "$goarm" ]]; then
    cp "$output" "$ROOT_DIR/kiwi"
    chmod +x "$ROOT_DIR/kiwi"
    printf '%s\n' "binary: $ROOT_DIR/kiwi"
  fi
}

build_choice() {
  case "$1" in
    amd64)
      build_target amd64 "" linux-amd64
      ;;
    arm64)
      build_target arm64 "" linux-arm64
      ;;
    armv7)
      build_target arm 7 linux-armv7
      ;;
    all)
      build_target amd64 "" linux-amd64
      build_target arm64 "" linux-arm64
      build_target arm 7 linux-armv7
      ;;
    host)
      case "$HOST_GOARCH" in
        amd64)
          build_target amd64 "" linux-amd64
          ;;
        arm64)
          build_target arm64 "" linux-arm64
          ;;
        arm)
          build_target arm 7 linux-armv7
          ;;
        *)
          printf '%s\n' "unsupported host arch: $HOST_GOARCH" >&2
          exit 1
          ;;
      esac
      ;;
    *)
      printf '%s\n' "unknown build target: $1" >&2
      exit 1
      ;;
  esac
}

choose_target() {
  if [[ $# -ge 1 ]]; then
    printf '%s\n' "$1"
    return
  fi

  if [[ ! -t 0 ]]; then
    printf '%s\n' host
    return
  fi

  printf '%s\n' 'Choose a build target:'
  printf '%s\n' '  1) linux-amd64'
  printf '%s\n' '  2) linux-arm64'
  printf '%s\n' '  3) linux-armv7'
  printf '%s\n' '  4) all three'
  printf '%s\n' '  5) host default'
  printf 'Selection [5]: '

  local selection
  IFS= read -r selection
  case "$selection" in
    ""|5)
      printf '%s\n' host
      ;;
    1)
      printf '%s\n' amd64
      ;;
    2)
      printf '%s\n' arm64
      ;;
    3)
      printf '%s\n' armv7
      ;;
    4)
      printf '%s\n' all
      ;;
    amd64|arm64|armv7|all|host)
      printf '%s\n' "$selection"
      ;;
    *)
      printf '%s\n' 'invalid selection' >&2
      exit 1
      ;;
  esac
}

TARGET="$(choose_target "${1:-}")"
build_choice "$TARGET"

printf '%s\n' '[ok] build complete'
printf '%s\n' "artifacts: $DIST_DIR"
