#!/bin/bash
set -e

cd "$(dirname "$0")"

ACTION="${1:-build}"

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
    echo "🚀 Starting agend..."
    ./target/debug/zellij agend
    ;;
  check)
    echo "🔍 Checking..."
    cargo check --no-default-features --features "vendored_curl,agend"
    echo "✅ Check passed"
    ;;
  stop)
    echo "🛑 Stopping agend..."
    # Kill the zellij session named "agend" (kills server + all panes)
    ./target/debug/zellij kill-session agend 2>/dev/null || \
    ./target/release/zellij kill-session agend 2>/dev/null || \
    echo "No agend session found"
    # Also kill any orphaned zellij server processes running agend
    pkill -f "zellij.*agend" 2>/dev/null || true
    echo "✅ Stopped"
    ;;
  clean)
    cargo clean
    echo "🧹 Cleaned"
    ;;
  *)
    echo "Usage: $0 {build|release|test|run|stop|check|clean}"
    exit 1
    ;;
esac
