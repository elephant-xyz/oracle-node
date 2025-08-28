#!/usr/bin/env bash
set -euo pipefail

# Backwards-compatible deploy script. Now delegates to simpler commands:
# - Sync DAGs
# - Update transforms

GREEN='\033[0;32m'; NC='\033[0m'
echo -e "${GREEN}🚀 MWAA Deploy (DAGs + Transforms)${NC}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

"$SCRIPT_DIR/sync-dags.sh"
"$SCRIPT_DIR/update-transforms.sh"

echo -e "${GREEN}✅ Done${NC}"
