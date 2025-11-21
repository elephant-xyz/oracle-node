#!/usr/bin/env bash
set -euo pipefail

# Set gas price configuration in AWS SSM Parameter Store for keystore mode oracles

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info() { echo -e "${GREEN}[INFO]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
err()  { echo -e "${RED}[ERROR]${NC} $*"; }

# Config with defaults
PARAMETER_NAME="/elephant-oracle-node/gas-price"

usage() {
    cat <<EOF
Usage: $0 [OPTIONS]

Set gas price configuration in AWS SSM Parameter Store for keystore mode oracles.

Options:
    --max-fee <gwei>            Set maxFeePerGas (EIP-1559 format)
    --priority-fee <gwei>       Set maxPriorityFeePerGas (EIP-1559 format, requires --max-fee)
    --gas-price <gwei>          Set legacy gas price (cannot be used with --max-fee)
    --auto                      Use automatic gas price detection
    --view                      View current gas price configuration
    --history                   View gas price configuration history
    --help                      Show this help message

Examples:
    # Set EIP-1559 gas price (recommended)
    $0 --max-fee 50 --priority-fee 2

    # Set legacy gas price
    $0 --gas-price 50

    # Use automatic gas price detection
    $0 --auto

    # View current configuration
    $0 --view

    # View configuration history
    $0 --history

Environment Variables:
    PARAMETER_NAME             SSM parameter name (default: /elephant-oracle-node/gas-price)

Notes:
    - Gas prices are specified in Gwei (1 Gwei = 0.000000001 ETH)
    - This configuration only affects keystore mode (self-custodial) oracles
    - API mode oracles use gas prices managed by Elephant
    - Changes take effect on the next Lambda execution (no redeployment needed)

Typical Gas Price Values:
    Normal conditions:  --max-fee 20-50  --priority-fee 1-3
    High congestion:    --max-fee 100-200 --priority-fee 5-10
    Low activity:       --max-fee 10-20  --priority-fee 1
EOF
}

# Check prerequisites
check_prereqs() {
    command -v aws >/dev/null || { err "aws CLI not found"; exit 1; }
    command -v jq >/dev/null || { err "jq not found"; exit 1; }
    aws sts get-caller-identity >/dev/null || { err "AWS credentials not configured"; exit 1; }
}

# View current gas price configuration
view_gas_price() {
    info "Fetching current gas price configuration from SSM Parameter Store"
    info "Parameter: $PARAMETER_NAME"
    echo ""

    if ! aws ssm get-parameter --name "$PARAMETER_NAME" --output json 2>/dev/null | jq -r '.Parameter | "Name: \(.Name)\nType: \(.Type)\nValue: \(.Value)\nVersion: \(.Version)\nLastModifiedDate: \(.LastModifiedDate)"'; then
        err "Parameter not found or access denied"
        err "Make sure the parameter exists and you have permission to read it"
        return 1
    fi
}

# View gas price configuration history
view_gas_price_history() {
    info "Fetching gas price configuration history from SSM Parameter Store"
    info "Parameter: $PARAMETER_NAME"
    echo ""

    if ! aws ssm get-parameter-history --name "$PARAMETER_NAME" --output json 2>/dev/null | jq -r '.Parameters[] | "Version: \(.Version)\nValue: \(.Value)\nLastModifiedDate: \(.LastModifiedDate)\n---"'; then
        err "Parameter not found or access denied"
        err "Make sure the parameter exists and you have permission to read its history"
        return 1
    fi
}

# Validate numeric value
validate_number() {
    local value="$1"
    local param_name="$2"

    if ! [[ "$value" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
        err "Invalid value for $param_name: $value"
        err "Must be a positive number (e.g., 50, 2.5, 100)"
        return 1
    fi
}

# Set EIP-1559 gas price
set_eip1559_gas_price() {
    local max_fee="$1"
    local priority_fee="$2"

    info "Setting EIP-1559 gas price configuration"
    info "maxFeePerGas: ${max_fee} Gwei"
    info "maxPriorityFeePerGas: ${priority_fee} Gwei"

    # Validate maxPriorityFeePerGas is not greater than maxFeePerGas
    if (( $(echo "$priority_fee > $max_fee" | bc -l) )); then
        err "maxPriorityFeePerGas ($priority_fee) cannot be greater than maxFeePerGas ($max_fee)"
        return 1
    fi

    local value
    value=$(jq -n \
        --arg maxFee "$max_fee" \
        --arg priorityFee "$priority_fee" \
        '{maxFeePerGas: $maxFee, maxPriorityFeePerGas: $priorityFee}' | jq -c .)

    if aws ssm put-parameter \
        --name "$PARAMETER_NAME" \
        --type "String" \
        --value "$value" \
        --overwrite \
        --output json >/dev/null; then

        info "✅ Gas price configuration updated successfully"
        info "Parameter: $PARAMETER_NAME"
        info "Value: $value"
        echo ""
        info "Changes will take effect on the next Lambda execution"
        return 0
    else
        err "❌ Failed to update gas price configuration"
        return 1
    fi
}

# Set legacy gas price
set_legacy_gas_price() {
    local gas_price="$1"

    info "Setting legacy gas price configuration"
    info "gasPrice: ${gas_price} Gwei"

    if aws ssm put-parameter \
        --name "$PARAMETER_NAME" \
        --type "String" \
        --value "$gas_price" \
        --overwrite \
        --output json >/dev/null; then

        info "✅ Gas price configuration updated successfully"
        info "Parameter: $PARAMETER_NAME"
        info "Value: $gas_price Gwei"
        echo ""
        info "Changes will take effect on the next Lambda execution"
        return 0
    else
        err "❌ Failed to update gas price configuration"
        return 1
    fi
}

# Set automatic gas price
set_auto_gas_price() {
    info "Setting automatic gas price detection"

    if aws ssm put-parameter \
        --name "$PARAMETER_NAME" \
        --type "String" \
        --value "auto" \
        --overwrite \
        --output json >/dev/null; then

        info "✅ Gas price configuration updated successfully"
        info "Parameter: $PARAMETER_NAME"
        info "Value: auto"
        echo ""
        info "The RPC provider will automatically determine optimal gas fees"
        info "Changes will take effect on the next Lambda execution"
        return 0
    else
        err "❌ Failed to update gas price configuration"
        return 1
    fi
}

main() {
    local max_fee=""
    local priority_fee=""
    local gas_price=""
    local auto_mode=false
    local view_mode=false
    local history_mode=false

    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --max-fee)
                if [[ -z "$2" || "$2" =~ ^-- ]]; then
                    err "--max-fee requires a value"
                    usage
                    exit 1
                fi
                max_fee="$2"
                shift 2
                ;;
            --priority-fee)
                if [[ -z "$2" || "$2" =~ ^-- ]]; then
                    err "--priority-fee requires a value"
                    usage
                    exit 1
                fi
                priority_fee="$2"
                shift 2
                ;;
            --gas-price)
                if [[ -z "$2" || "$2" =~ ^-- ]]; then
                    err "--gas-price requires a value"
                    usage
                    exit 1
                fi
                gas_price="$2"
                shift 2
                ;;
            --auto)
                auto_mode=true
                shift
                ;;
            --view)
                view_mode=true
                shift
                ;;
            --history)
                history_mode=true
                shift
                ;;
            --help)
                usage
                exit 0
                ;;
            -*)
                err "Unknown option: $1"
                usage
                exit 1
                ;;
            *)
                err "Unknown argument: $1"
                usage
                exit 1
                ;;
        esac
    done

    check_prereqs

    # Handle view mode
    if [[ "$view_mode" == "true" ]]; then
        view_gas_price
        exit $?
    fi

    # Handle history mode
    if [[ "$history_mode" == "true" ]]; then
        view_gas_price_history
        exit $?
    fi

    # Validate mutually exclusive options
    local mode_count=0
    [[ -n "$max_fee" ]] && ((mode_count++))
    [[ -n "$gas_price" ]] && ((mode_count++))
    [[ "$auto_mode" == "true" ]] && ((mode_count++))

    if [[ $mode_count -eq 0 ]]; then
        err "Must specify one of: --max-fee, --gas-price, --auto, --view, or --history"
        usage
        exit 1
    fi

    if [[ $mode_count -gt 1 ]]; then
        err "Cannot use --max-fee, --gas-price, and --auto together"
        err "Choose one gas price configuration method"
        usage
        exit 1
    fi

    # Handle auto mode
    if [[ "$auto_mode" == "true" ]]; then
        set_auto_gas_price
        exit $?
    fi

    # Handle EIP-1559 mode
    if [[ -n "$max_fee" ]]; then
        # Validate priority fee is provided
        if [[ -z "$priority_fee" ]]; then
            err "--priority-fee is required when using --max-fee"
            usage
            exit 1
        fi

        validate_number "$max_fee" "--max-fee" || exit 1
        validate_number "$priority_fee" "--priority-fee" || exit 1

        set_eip1559_gas_price "$max_fee" "$priority_fee"
        exit $?
    fi

    # Handle legacy mode
    if [[ -n "$gas_price" ]]; then
        if [[ -n "$priority_fee" ]]; then
            err "--priority-fee cannot be used with --gas-price"
            err "Use --max-fee instead for EIP-1559 format"
            usage
            exit 1
        fi

        validate_number "$gas_price" "--gas-price" || exit 1

        set_legacy_gas_price "$gas_price"
        exit $?
    fi
}

main "$@"