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
  clean)
    cargo clean
    echo "🧹 Cleaned"
    ;;
  *)
    echo "Usage: $0 {build|release|test|run|check|clean}"
    exit 1
    ;;
esac
