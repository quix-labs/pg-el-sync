#!/bin/sh

#Linux
for arch in "386" "amd64" "arm" "arm64"; do
  echo "Building arch: linux_${arch}"
  env GOOS=linux GOARCH="${arch}" go build -ldflags "-s -w" -o /tmp/pgsync_linux_"${arch}"
  rm /dist/pgsync_linux_"${arch}"
  upx -qq --best --lzma -o /dist/pgsync_linux_"${arch}" /tmp/pgsync_linux_"${arch}"
  chmod +x /dist/pgsync_linux_"${arch}"
  chmod 777 /dist/pgsync_linux_"${arch}"
done

#Windows
for arch in "amd64" "arm64"; do
  echo "Building arch: windows_${arch}"
  env GOOS=windows GOARCH="${arch}" go build -ldflags "-s -w" -o /dist/pgsync_windows_"${arch}".exe
#  rm dist/pgsync_windows_"${arch}".exe
#  upx -qq --best --lzma -o /dist/pgsync_windows_"${arch}".exe /tmp/pgsync_windows_"${arch}".exe
  chmod +x /dist/pgsync_windows_"${arch}".exe
  chmod 777 /dist/pgsync_windows_"${arch}".exe
done

#Darwin
for arch in "amd64" "arm64"; do
  echo "Building arch: darwin_${arch}"
  env GOOS=darwin GOARCH="${arch}" go build -ldflags "-s -w" -o /tmp/pgsync_darwin_"${arch}"
  rm /dist/pgsync_darwin_"${arch}"
  upx -qq --best --lzma -o /dist/pgsync_darwin_"${arch}" /tmp/pgsync_darwin_"${arch}"
  chmod +x /dist/pgsync_darwin_"${arch}"
  chmod 777 /dist/pgsync_darwin_"${arch}"
done