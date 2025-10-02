#!/usr/bin/env bash
set -euo pipefail

# Script to update proxy rotation table without full deployment

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
info() { echo -e "${GREEN}[INFO]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
err()  { echo -e "${RED}[ERROR]${NC} $*"; }

STACK_NAME="${STACK_NAME:-elephant-oracle-node}"

show_usage() {
  cat <<EOF
${BLUE}Usage:${NC}
  $0 <proxy-file>           # Add/update proxies from file
  $0 --list                 # List all configured proxies
  $0 --clear                # Remove all proxies
  $0 --help                 # Show this help

${BLUE}Proxy File Format:${NC}
  One proxy per line: username:password@ip:port
  
${BLUE}Examples:${NC}
  # Add proxies from file
  $0 proxies.txt
  
  # List current proxies
  $0 --list
  
  # Clear all proxies
  $0 --clear
  
${BLUE}Environment Variables:${NC}
  STACK_NAME               CloudFormation stack name (default: elephant-oracle-node)
  AWS_PROFILE              AWS CLI profile to use
  AWS_REGION               AWS region
EOF
}

get_table_name() {
  local table_name
  table_name=$(aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --query 'Stacks[0].Outputs[?OutputKey==`ProxyRotationTableName`].OutputValue' \
    --output text 2>/dev/null || echo "")
  
  if [[ -z "$table_name" ]]; then
    err "ProxyRotationTable not found in stack '$STACK_NAME'"
    err "Make sure the stack is deployed with the proxy rotation feature"
    exit 1
  fi
  
  echo "$table_name"
}

list_proxies() {
  local table_name
  table_name=$(get_table_name)
  
  info "Listing proxies from table: $table_name"
  echo
  
  local result
  result=$(aws dynamodb query \
    --table-name "$table_name" \
    --index-name LastUsedIndex \
    --key-condition-expression "constantKey = :key" \
    --expression-attribute-values '{":key":{"S":"PROXY"}}' \
    --output json)
  
  local count
  count=$(echo "$result" | jq -r '.Count')
  
  if [[ "$count" -eq 0 ]]; then
    warn "No proxies configured"
    return 0
  fi
  
  info "Found $count proxy(ies):"
  echo
  
  echo "$result" | jq -r '.Items[] | 
    "Proxy ID: " + .proxyId.S + 
    "\n  URL: " + (.proxyUrl.S | split(":")[0] + ":***@" + (split("@")[1] // "unknown")) +
    "\n  Last Used: " + (.lastUsedTime.N | tonumber / 1000 | strftime("%Y-%m-%d %H:%M:%S UTC")) +
    "\n  Failed: " + (.failed.BOOL | tostring) + 
    "\n"'
}

clear_proxies() {
  local table_name
  table_name=$(get_table_name)
  
  info "Clearing all proxies from table: $table_name"
  
  # Get all proxy IDs
  local proxy_ids
  proxy_ids=$(aws dynamodb scan \
    --table-name "$table_name" \
    --projection-expression "proxyId" \
    --output json | jq -r '.Items[].proxyId.S')
  
  if [[ -z "$proxy_ids" ]]; then
    warn "No proxies to clear"
    return 0
  fi
  
  local count=0
  while IFS= read -r proxy_id; do
    [[ -z "$proxy_id" ]] && continue
    
    info "Deleting proxy: $proxy_id"
    aws dynamodb delete-item \
      --table-name "$table_name" \
      --key "{\"proxyId\": {\"S\": \"$proxy_id\"}}" >/dev/null
    
    ((count++))
  done <<< "$proxy_ids"
  
  info "Cleared $count proxy(ies)"
}

update_proxies() {
  local proxy_file="$1"
  
  if [[ ! -f "$proxy_file" ]]; then
    err "Proxy file not found: $proxy_file"
    exit 1
  fi
  
  local table_name
  table_name=$(get_table_name)
  
  info "Updating proxies in table: $table_name"
  info "Reading from file: $proxy_file"
  echo
  
  local proxy_count=0
  local initial_time=0  # Start with timestamp 0 for new proxies
  
  while IFS= read -r proxy_url || [[ -n "$proxy_url" ]]; do
    # Skip empty lines and comments
    [[ -z "$proxy_url" || "$proxy_url" =~ ^[[:space:]]*# ]] && continue
    
    # Trim whitespace
    proxy_url=$(echo "$proxy_url" | xargs)
    
    # Validate proxy format (username:password@ip:port)
    if [[ ! "$proxy_url" =~ ^[^:]+:[^@]+@[^:]+:[0-9]+$ ]]; then
      warn "Invalid proxy format (expected username:password@ip:port): $proxy_url"
      continue
    fi
    
    # Generate proxy ID (hash of the proxy URL for uniqueness)
    local proxy_id
    proxy_id=$(echo -n "$proxy_url" | sha256sum | cut -d' ' -f1)
    
    # Check if proxy already exists
    local existing
    existing=$(aws dynamodb get-item \
      --table-name "$table_name" \
      --key "{\"proxyId\": {\"S\": \"$proxy_id\"}}" \
      --output json 2>/dev/null | jq -r '.Item.proxyId.S // empty')
    
    if [[ -n "$existing" ]]; then
      info "Updating existing proxy: $proxy_id (${proxy_url%%:*}:***@...)"
      # Only update the URL, keep existing lastUsedTime and failed status
      aws dynamodb update-item \
        --table-name "$table_name" \
        --key "{\"proxyId\": {\"S\": \"$proxy_id\"}}" \
        --update-expression "SET proxyUrl = :url" \
        --expression-attribute-values "{\":url\": {\"S\": \"$proxy_url\"}}" >/dev/null
    else
      info "Adding new proxy: $proxy_id (${proxy_url%%:*}:***@...)"
      # Put new item with initial timestamp
      aws dynamodb put-item \
        --table-name "$table_name" \
        --item "{
          \"proxyId\": {\"S\": \"$proxy_id\"},
          \"proxyUrl\": {\"S\": \"$proxy_url\"},
          \"constantKey\": {\"S\": \"PROXY\"},
          \"lastUsedTime\": {\"N\": \"$initial_time\"},
          \"failed\": {\"BOOL\": false}
        }" >/dev/null
    fi
    
    ((proxy_count++))
  done < "$proxy_file"
  
  echo
  if [[ $proxy_count -eq 0 ]]; then
    warn "No valid proxies found in $proxy_file"
    exit 1
  else
    info "Successfully processed $proxy_count proxy(ies)"
    echo
    info "To verify, run: $0 --list"
  fi
}

main() {
  # Parse arguments first (for --help)
  if [[ $# -eq 0 ]]; then
    err "No arguments provided"
    echo
    show_usage
    exit 1
  fi
  
  case "${1:-}" in
    --help|-h)
      show_usage
      exit 0
      ;;
    --list|-l)
      # Check prerequisites
      command -v aws >/dev/null || { err "aws CLI not found"; exit 1; }
      command -v jq >/dev/null || { err "jq not found"; exit 1; }
      aws sts get-caller-identity >/dev/null || { err "AWS credentials not configured"; exit 1; }
      list_proxies
      ;;
    --clear|-c)
      # Check prerequisites
      command -v aws >/dev/null || { err "aws CLI not found"; exit 1; }
      command -v jq >/dev/null || { err "jq not found"; exit 1; }
      aws sts get-caller-identity >/dev/null || { err "AWS credentials not configured"; exit 1; }
      clear_proxies
      ;;
    *)
      if [[ "${1:-}" =~ ^- ]]; then
        err "Unknown option: $1"
        echo
        show_usage
        exit 1
      fi
      # Check prerequisites
      command -v aws >/dev/null || { err "aws CLI not found"; exit 1; }
      command -v jq >/dev/null || { err "jq not found"; exit 1; }
      aws sts get-caller-identity >/dev/null || { err "AWS credentials not configured"; exit 1; }
      update_proxies "$1"
      ;;
  esac
}

main "$@"

