#!/usr/bin/env bash
set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info() { echo -e "${GREEN}[INFO]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
err()  { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

STACK_NAME="${STACK_NAME:-elephant-oracle-node}"
CODEBUILD_STACK_NAME="${CODEBUILD_STACK_NAME:-elephant-oracle-codebuild}"

usage() {
    cat <<EOF
Usage: $0 [OPTIONS]

Invoke the redrive auto repair CodeBuild project.

Options:
    --help             Show this help message

Environment Variables:
    STACK_NAME         CloudFormation stack name (default: elephant-oracle-node)
    CODEBUILD_STACK_NAME CodeBuild stack name (default: {STACK_NAME}-codebuild)

Examples:
    # Invoke redrive auto repair CodeBuild project
    $0

    # Invoke with a different stack name
    STACK_NAME=my-custom-stack $0
EOF
}

# Parse arguments
for arg in "$@"; do
    case $arg in
        --help)
            usage
            exit 0
            ;;
        *)
            # Unknown option
            err "Unknown option: $arg"
            ;;
    esac
done

check_prereqs() {
    command -v aws >/dev/null || err "aws CLI not found"
    command -v jq >/dev/null || err "jq not found"
    aws sts get-caller-identity >/dev/null || err "AWS credentials not configured"
}

get_codebuild_project_name() {
    aws cloudformation describe-stacks \
        --stack-name "$CODEBUILD_STACK_NAME" \
        --query 'Stacks[0].Outputs[?OutputKey==`RedriveAutoRepairCodeBuildProjectName`].OutputValue' \
        --output text 2>/dev/null
}

main() {
    check_prereqs

    info "Using CodeBuild stack: $CODEBUILD_STACK_NAME"

    local project_name
    project_name=$(get_codebuild_project_name)

    if [[ -z "$project_name" || "$project_name" == "None" ]]; then
        err "Could not find RedriveAutoRepairCodeBuildProjectName in stack outputs for stack '$CODEBUILD_STACK_NAME'."
        err "Make sure the stack is deployed and contains the RedriveAutoRepairCodeBuildProjectName output."
    fi

    info "Starting CodeBuild project: $project_name"

    local build_id
    build_id=$(aws codebuild start-build \
        --project-name "$project_name" \
        --query 'build.id' \
        --output text 2>&1)

    if [[ $? -eq 0 ]]; then
        info "✅ CodeBuild project started successfully"
        info "Build ID: $build_id"
        info "Monitor progress with:"
        info "  aws codebuild batch-get-builds --ids $build_id"
        info "Or view in AWS Console:"
        info "  https://console.aws.amazon.com/codesuite/codebuild/projects/$project_name/build/$build_id"
    else
        err "❌ Failed to start CodeBuild project."
        echo "$build_id" >&2
    fi
}

main "$@"
