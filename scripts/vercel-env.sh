#!/bin/bash
# Copy variables from a dotenv file into a Vercel project without printing their values.
#   scripts/vercel-env.sh <dotenv-file> <vercel-project> VAR [VAR...]
set -euo pipefail
file="$1"; project="$2"; shift 2
scope="gurudeepnaiks-projects"
stage="$(mktemp -d)"
trap 'rm -rf "$stage"' EXIT
cd "$stage"
npx -y vercel@64.0.2 link --yes --project "$project" --scope "$scope" >/dev/null
for name in "$@"; do
  value="$(grep -E "^${name}=" "$file" | head -1 | cut -d= -f2- | sed "s/^['\"]//; s/['\"]\$//")"
  if [ -z "$value" ]; then echo "$name: not found in $file"; continue; fi
  for env in production preview development; do
    if printf '%s' "$value" | npx -y vercel@64.0.2 env add "$name" "$env" --scope "$scope" --force >/dev/null 2>&1; then
      echo "$name → $env: added"
    else
      echo "$name → $env: FAILED"
    fi
  done
done
