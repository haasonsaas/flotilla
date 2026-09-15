#!/bin/sh
# Install the latest flotilla release into ~/.local/bin and register the
# daemon as a user service. Usage:
#   curl -fsSL https://raw.githubusercontent.com/haasonsaas/flotilla/main/scripts/install.sh | sh
# Set FLOTILLA_VERSION=v0.2.0 to pin, FLOTILLA_BIN_DIR to change the target.
set -eu

repo="haasonsaas/flotilla"
bin_dir="${FLOTILLA_BIN_DIR:-$HOME/.local/bin}"
version="${FLOTILLA_VERSION:-}"

os=$(uname -s)
arch=$(uname -m)
case "$os-$arch" in
  Darwin-arm64)  target=aarch64-apple-darwin ;;
  Darwin-x86_64) target=x86_64-apple-darwin ;;
  Linux-x86_64)  target=x86_64-unknown-linux-gnu ;;
  Linux-aarch64|Linux-arm64) target=aarch64-unknown-linux-gnu ;;
  *) echo "unsupported platform: $os $arch" >&2; exit 1 ;;
esac

if [ -z "$version" ]; then
  version=$(curl -fsSL "https://api.github.com/repos/$repo/releases/latest" | sed -n 's/.*"tag_name": *"\([^"]*\)".*/\1/p' | head -1)
  [ -n "$version" ] || { echo "could not determine latest release" >&2; exit 1; }
fi

name="flotilla-$version-$target"
url="https://github.com/$repo/releases/download/$version/$name.tar.gz"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

echo "downloading $url"
curl -fsSL "$url" -o "$tmp/$name.tar.gz"
curl -fsSL "$url.sha256" -o "$tmp/$name.tar.gz.sha256"
(cd "$tmp" && shasum -a 256 -c "$name.tar.gz.sha256" >/dev/null) || { echo "checksum mismatch" >&2; exit 1; }
tar xzf "$tmp/$name.tar.gz" -C "$tmp"

mkdir -p "$bin_dir"
# Rename into place so a running daemon keeps its old inode until restart.
for b in flotilla flotillad; do
  cp "$tmp/$name/$b" "$bin_dir/$b.new"
  chmod +x "$bin_dir/$b.new"
  mv -f "$bin_dir/$b.new" "$bin_dir/$b"
done
echo "installed $version to $bin_dir"

if [ "${FLOTILLA_NO_SERVICE:-}" != "1" ]; then
  "$bin_dir/flotilla" install
fi
