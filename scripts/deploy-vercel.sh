#!/bin/bash
# Deploy the chaos playground to Vercel (FastAPI function + static dashboard, Neon Postgres).
#   scripts/deploy-vercel.sh          # preview
#   scripts/deploy-vercel.sh --prod   # production
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
STAGE="$(mktemp -d)/erp-sync-demo"   # the directory name becomes the Vercel project name
trap 'rm -rf "$(dirname "$STAGE")"' EXIT
mkdir -p "$STAGE"
# A copy without .git: deploys from the CLI are then not tied to a commit author.
rsync -a --exclude .git --exclude .venv --exclude __pycache__ --exclude tests "$ROOT/" "$STAGE/"
cd "$STAGE"
npx -y vercel@64.0.2 link --yes --scope gurudeepnaiks-projects >/dev/null
npx -y vercel@64.0.2 deploy --yes --scope gurudeepnaiks-projects "$@"
