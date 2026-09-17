#!/usr/bin/env bash
set -euo pipefail
# Native build-only tools. Keep the version and archive digest independent of
# the builder distribution (Ubuntu 20.04 packages lack the required features).
: "${ADX_EROFS_CACHE:?set the persistent build tools cache}"
version=1.8.10
revision=51b5939b5f783221310d25146e6a2019ba8129b6
sha256=b420c42eb27dd4834efb6bffc33656e4249ea3772b9b416d1515a41ed9de2d77
prefix="$ADX_EROFS_CACHE/erofs-utils-$version"
if [[ ! -x "$prefix/bin/mkfs.erofs" || ! -x "$prefix/bin/fsck.erofs" ]]; then
  mkdir -p "$ADX_EROFS_CACHE"
  archive="$ADX_EROFS_CACHE/erofs-utils-$version.tar.gz"
  if ! echo "$sha256  $archive" | sha256sum --check --status 2>/dev/null; then
    curl --fail --location --retry 3 --connect-timeout 20 --max-time 300 \
      "https://codeload.github.com/erofs/erofs-utils/tar.gz/$revision" -o "$archive.part"
    echo "$sha256  $archive.part" | sha256sum --check
    mv "$archive.part" "$archive"
  fi
  source_dir=$(mktemp -d "$ADX_EROFS_CACHE/erofs-build.XXXXXX")
  trap 'rm -rf "$source_dir"' EXIT
  tar -xzf "$archive" -C "$source_dir" --strip-components=1
  (
    cd "$source_dir"
    ./autogen.sh
    ./configure --prefix="$prefix" --disable-lz4 --disable-lzma \
      --without-uuid --without-zlib --without-libdeflate --without-libzstd --without-selinux
    make -j "${JOBS:-2}"
    make install
  )
fi
"$prefix/bin/mkfs.erofs" --version
"$prefix/bin/fsck.erofs" --version
