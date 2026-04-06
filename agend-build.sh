#!/bin/bash
set -e

cd "$(dirname "$0")"

ACTION="${1:-build}"
RELEASE_FLAG="${2:-}"

# Resolve binary path: prefer release if exists, else debug
resolve_binary() {
  if [ -f ./target/release/zellij ]; then
    echo "./target/release/zellij"
  elif [ -f ./target/debug/zellij ]; then
    echo "./target/debug/zellij"
  else
    echo ""
  fi
}

case "$ACTION" in
  build)
    echo "🔨 Building agend-rs..."
    cargo build --no-default-features --features "vendored_curl,agend"
    echo "✅ Build complete: ./target/debug/zellij"
    ;;
  release)
    echo "🔨 Building agend-rs (release)..."
    cargo build --release --no-default-features --features "vendored_curl,agend"
    echo "✅ Release build: ./target/release/zellij"
    ;;
  test)
    echo "🧪 Running tests..."
    cargo test --features "agend" -p zellij-server -- agend
    ;;
  run)
    if [ "$RELEASE_FLAG" = "--release" ]; then
      # Release mode: build release + run
      echo "🔨 Building agend-rs (release)..."
      cargo build --release --no-default-features --features "vendored_curl,agend"
      BINARY="./target/release/zellij"
    else
      # Debug mode: auto-build if binary missing, else use existing
      BINARY="$(resolve_binary)"
      if [ -z "$BINARY" ]; then
        echo "🔨 No binary found, building..."
        cargo build --no-default-features --features "vendored_curl,agend"
        BINARY="./target/debug/zellij"
      fi
    fi
    echo "🚀 Starting agend (${BINARY})..."
    # Clean up dead session if exists
    "$BINARY" delete-session agend 2>/dev/null || true
    "$BINARY" agend
    ;;
  check)
    echo "🔍 Checking..."
    cargo check --no-default-features --features "vendored_curl,agend"
    echo "✅ Check passed"
    ;;
  stop)
    echo "🛑 Stopping agend..."
    BINARY="$(resolve_binary)"
    if [ -n "$BINARY" ]; then
      "$BINARY" kill-session agend 2>/dev/null || true
    fi
    pkill -f "zellij.*agend" 2>/dev/null || true
    # Delete dead session so next run doesn't conflict
    if [ -n "$BINARY" ]; then
      "$BINARY" delete-session agend 2>/dev/null || true
    fi
    echo "✅ Stopped"
    ;;
  clean)
    cargo clean
    echo "🧹 Cleaned"
    ;;
  *)
    echo "Usage: $0 {build|release|test|run [--release]|stop|check|clean}"
    exit 1
    ;;
esac
