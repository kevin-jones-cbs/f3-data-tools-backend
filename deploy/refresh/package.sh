#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
build_dir="$(mktemp -d)"
trap 'rm -rf "$build_dir"' EXIT
output_zip="${1:-$PWD/refresh-publish.zip}"
dotnet publish tools/southfork-duckdb/Export/Export.csproj -c Release -r linux-x64 \
  --self-contained true -p:PublishReadyToRun=false -o "$build_dir/app/export"
rm -rf "$build_dir/app/export/Secrets"
cp F3Lambda/regions.json "$build_dir/app/export/regions.json"
cp tools/southfork-duckdb/refresh.py deploy/refresh/lambda_function.py "$build_dir/app/"
curl --fail --location --retry 3 \
  https://github.com/duckdb/duckdb/releases/download/v1.5.3/duckdb_cli-linux-amd64.zip \
  -o "$build_dir/duckdb.zip"
unzip -q "$build_dir/duckdb.zip" -d "$build_dir/duckdb"
cp "$build_dir/duckdb/duckdb" "$build_dir/app/duckdb"
chmod 755 "$build_dir/app/duckdb" "$build_dir/app/export/Export"
(cd "$build_dir/app" && zip -qr "$output_zip" .)
echo "Created $output_zip"
