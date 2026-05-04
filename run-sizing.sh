#!/usr/bin/env bash
# ════════════════════════════════════════════════════════════════════════════════
# run-sizing.sh  —  Cortex Cloud Azure Workload Sizing  (Production Orchestrator)
# ════════════════════════════════════════════════════════════════════════════════
#
# Handles end-to-end orchestration:
#   • Virtualenv creation and dependency installation (idempotent)
#   • Authentication via Azure CLI (az login)
#   • Token verification and refresh before every scan pass
#   • Multi-pass batched scanning for tenants with 100s of subscriptions
#   • Automatic failure retry with configurable passes
#   • Tenant scan (Entra ID user count + tenant-level diagnostic settings)
#   • Summary report and Excel workbook generation
#   • Timestamped log file for every run
#
# ── What the scan does ────────────────────────────────────────────────────────
#   Resource counts use a hybrid API strategy:
#     Azure Resource Graph  — VMs (running state), Storage Accounts, Cosmos DB,
#                             Azure SQL databases.  Single query per type; fast.
#                             Standard Reader role sufficient — no extra role.
#                             ARM fallback retained for each resource type.
#     ARM SDK / REST        — AKS node counts, ARO nodes, ACI, Container Apps,
#                             Function App function counts (not a Resource Graph
#                             type), ACR management plane, Event Hub,
#                             Network Watcher flow logs.
#     ACR data plane        — Image tag and manifest counts.
#                             If the client IP is blocked by ACR network firewall,
#                             the output flags ⚠ FIREWALL BLOCKED rather than
#                             showing a silent 0.
#     Blob storage          — Audit log + flow log volume measurement (EH path).
#                             Flow log blobs are listed per flow-log resource using
#                             the exact prefix written by Azure Network Watcher
#                             (flowLogResourceID=/ for VNet, resourceId=/ for NSG).
#     Log Analytics API     — Audit + flow log volume when customer routes to LAW.
#     Microsoft Graph       — Entra ID user counts only (not resource inventory).
#                             If User.Read.All is not granted (403), the scan
#                             completes but SaaS workloads = 0 and the Grand Total
#                             is flagged ⚠ understated in the summary report.
#
# ── Usage ────────────────────────────────────────────────────────────────────
#   ./run-sizing.sh              # full run: preflight → scan → tenant → report
#   ./run-sizing.sh preflight    # permission checks only
#   ./run-sizing.sh scan         # discovery + scan (skips preflight + report)
#   ./run-sizing.sh tenant       # Entra ID user count + tenant diag settings
#   ./run-sizing.sh retry        # retry currently-failed subscriptions only
#   ./run-sizing.sh summary      # regenerate report from existing results
#   ./run-sizing.sh help         # show this help
#
# ── Environment variable overrides (all optional) ────────────────────────────
#   CC_BATCH_SIZE        Subscriptions per scan pass             [default: 25]
#   CC_MAX_RETRY         Retry passes after initial scan         [default: 2]
#   CC_SUB_TIMEOUT       Per-subscription timeout in minutes     [default: 20]
#   CC_HEARTBEAT         Heartbeat interval in seconds           [default: 10]
#   CC_NO_VERIFY_SSL     Set to 1 for --no-verify-ssl
#                        (needed when behind Zscaler / SSL-inspecting proxies)
#   CC_SKIP_TENANT_SCAN  Set to 1 to skip Entra ID user count
#   CC_WORK_DIR          Directory for all output files
#                        [default: same directory as this script]
#   CLIENT_IP            Override the public IP used in firewall fix strings.
#                        Set this if api.ipify.org is blocked by your proxy.
#
# ── Token refresh strategy ───────────────────────────────────────────────────
#   Azure access tokens expire after 1 hour. For tenants with hundreds of
#   subscriptions, a full scan can run for many hours. This script:
#     1. Verifies token validity (with a 5-minute buffer) before every pass.
#     2. Attempts a silent refresh via 'az account get-access-token'.
#     3. Falls back to full re-authentication if silent refresh fails.
#   The Python SDK (DefaultAzureCredential) also refreshes tokens internally
#   during execution, so mid-pass expiry is handled transparently by the SDK.
#
# ── Large-tenant design ──────────────────────────────────────────────────────
#   Subscriptions are processed in batches of CC_BATCH_SIZE. The outer loop
#   continues until no subscriptions remain in "pending" state. Between passes
#   the token is verified and the state file is checked for remaining work.
#   The script is safely re-entrant: interrupting with Ctrl+C and re-running
#   will resume from where it left off.
# ════════════════════════════════════════════════════════════════════════════════

set -euo pipefail

# ── Resolve paths ─────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK_DIR="${CC_WORK_DIR:-${SCRIPT_DIR}}"
VENV_DIR="${SCRIPT_DIR}/.venv"
REQUIREMENTS="${SCRIPT_DIR}/requirements-azure.txt"
SIZING_PY="${SCRIPT_DIR}/az-sizing.py"
SUMMARY_PY="${SCRIPT_DIR}/az-summary.py"
STATE_FILE="${WORK_DIR}/azure_state.jsonl"
RESULTS_FILE="${WORK_DIR}/azure_results.json"
TENANT_FILE="${WORK_DIR}/azure_tenant.json"
XLSX_FILE="${WORK_DIR}/cortex_azure_sizing.xlsx"

# ── Configuration (override via environment) ──────────────────────────────────
BATCH_SIZE="${CC_BATCH_SIZE:-25}"
MAX_RETRY="${CC_MAX_RETRY:-2}"
SUB_TIMEOUT="${CC_SUB_TIMEOUT:-20}"
HEARTBEAT="${CC_HEARTBEAT:-10}"
NO_VERIFY_SSL="${CC_NO_VERIFY_SSL:-}"
SKIP_TENANT_SCAN="${CC_SKIP_TENANT_SCAN:-}"

# ── Logging setup ─────────────────────────────────────────────────────────────
mkdir -p "${WORK_DIR}"
LOG_FILE="${WORK_DIR}/sizing-$(date +%Y%m%d-%H%M%S).log"

RED='\033[0;31m'; YELLOW='\033[1;33m'; GREEN='\033[0;32m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

_ts() { date '+%H:%M:%S'; }
log()  { printf '[%s] %b\n'  "$(_ts)" "$*"                     | tee -a "${LOG_FILE}"; }
info() { printf '[%s] %b\n'  "$(_ts)" "${CYAN}${*}${RESET}"    | tee -a "${LOG_FILE}"; }
ok()   { printf '[%s] %b\n'  "$(_ts)" "${GREEN}✔  ${*}${RESET}" | tee -a "${LOG_FILE}"; }
warn() { printf '[%s] %b\n'  "$(_ts)" "${YELLOW}⚠  ${*}${RESET}" | tee -a "${LOG_FILE}"; }
die()  { printf '[%s] %b\n'  "$(_ts)" "${RED}✘  ${*}${RESET}"  | tee -a "${LOG_FILE}"; exit 1; }

SEP="════════════════════════════════════════════════════════════════════════════════"
THIN="────────────────────────────────────────────────────────────────────────────────"

# ── Interrupt handling ────────────────────────────────────────────────────────
_interrupted=0
_cleanup() {
    _interrupted=1
    echo ""
    warn "Interrupted — progress is saved to ${STATE_FILE}."
    warn "Re-run the same command to resume from where you left off."
    warn "Full log: ${LOG_FILE}"
}
trap '_cleanup' INT TERM

# ════════════════════════════════════════════════════════════════════════════════
# Environment setup
# ════════════════════════════════════════════════════════════════════════════════

setup_venv() {
    info "Checking Python environment..."

    command -v python3 &>/dev/null \
        || die "python3 not found. Install Python 3.9+ and re-run."

    local py_ver
    py_ver=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
    log "  Python ${py_ver}"

    if [[ ! -f "${VENV_DIR}/bin/python3" ]]; then
        log "  Creating virtualenv at ${VENV_DIR} ..."
        python3 -m venv "${VENV_DIR}" \
            || die "Failed to create virtualenv. Check Python installation."
    fi

    # shellcheck source=/dev/null
    source "${VENV_DIR}/bin/activate"

    # Only re-install when requirements.txt is newer than the last install stamp
    local stamp="${VENV_DIR}/.install-stamp"
    if [[ ! -f "${stamp}" || "${REQUIREMENTS}" -nt "${stamp}" ]]; then
        log "  Installing / updating dependencies from ${REQUIREMENTS} ..."
        pip install --quiet --upgrade pip
        pip install --quiet -r "${REQUIREMENTS}" \
            || die "Dependency installation failed. Check ${LOG_FILE}."
        touch "${stamp}"
        ok "Dependencies installed."
    else
        ok "Dependencies up to date (requirements.txt unchanged)."
    fi
}

# ════════════════════════════════════════════════════════════════════════════════
# Authentication
# ════════════════════════════════════════════════════════════════════════════════

authenticate() {
    info "Authenticating to Azure..."

    log "  Mode: Azure CLI  (az login)"
    # 'az account show' is a fast no-op when already authenticated
    if ! az account show --output none 2>/dev/null; then
        log "  No active CLI session — launching interactive login..."
        az login --output none \
            || die "az login failed. Ensure az CLI is installed and you have network access."
    fi

    local tenant account
    tenant=$(az account show --query tenantId -o tsv 2>/dev/null || echo "unknown")
    account=$(az account show --query name    -o tsv 2>/dev/null || echo "unknown")
    ok "Authenticated  |  Tenant: ${tenant}  |  Account: ${account}"
}

# ════════════════════════════════════════════════════════════════════════════════
# Token helpers
# ════════════════════════════════════════════════════════════════════════════════

verify_token() {
    info "  Verifying Azure token..."

    local token_json expiry_epoch now_epoch remaining_min
    token_json=$(az account get-access-token --output json 2>/dev/null) || {
        warn "Token refresh failed — attempting re-authentication..."
        authenticate
        token_json=$(az account get-access-token --output json 2>/dev/null) \
            || die "Cannot acquire Azure token after re-authentication."
    }

    expiry_epoch=$(echo "${token_json}" \
        | python3 -c "import sys,json,email.utils; d=json.load(sys.stdin)['expiresOn']; \
                      import datetime; \
                      print(int(datetime.datetime.fromisoformat(d).timestamp()))" 2>/dev/null \
        || echo 0)

    now_epoch=$(date +%s)
    remaining_min=$(( (expiry_epoch - now_epoch - 300) / 60 ))

    if [[ "${remaining_min}" -gt 0 ]]; then
        log "  Token valid for ~${remaining_min} min."
    else
        warn "Token near expiry — refreshing..."
        az account get-access-token --output none 2>/dev/null || authenticate
    fi
}

# ════════════════════════════════════════════════════════════════════════════════
# Sizing helper — passes common flags to az-sizing.py
# ════════════════════════════════════════════════════════════════════════════════

run_sizing() {
    local args=("$@")
    [[ -n "${NO_VERIFY_SSL}" ]] && args+=("--no-verify-ssl")
    python3 "${SIZING_PY}" "${args[@]}"
}

# ════════════════════════════════════════════════════════════════════════════════
# State helpers
# ════════════════════════════════════════════════════════════════════════════════

count_status() {
    local status="$1"
    if [[ ! -f "${STATE_FILE}" ]]; then echo 0; return; fi
    python3 -c "
import json
rows=[json.loads(l) for l in open('${STATE_FILE}') if l.strip()]
print(sum(1 for r in rows if r.get('status')=='${status}'))
"
}

# ════════════════════════════════════════════════════════════════════════════════
# Stages
# ════════════════════════════════════════════════════════════════════════════════

stage_preflight() {
    printf '\n%s\n' "${SEP}" | tee -a "${LOG_FILE}"
    info "STAGE: Preflight permission check"
    printf '%s\n' "${THIN}" | tee -a "${LOG_FILE}"

    # Preflight uses role-assignment API — takes ~1s per subscription,
    # not 5-15s like the old storage-account sampling approach.
    run_sizing --preflight 2>&1 | tee -a "${LOG_FILE}"
}

stage_scan() {
    printf '\n%s\n' "${SEP}" | tee -a "${LOG_FILE}"
    info "STAGE: Subscription discovery"
    printf '%s\n' "${THIN}" | tee -a "${LOG_FILE}"

    run_sizing \
        --init-state \
        --state-file "${STATE_FILE}" \
        2>&1 | tee -a "${LOG_FILE}"

    local pending_n
    pending_n=$(count_status "pending")

    if [[ "${pending_n}" -eq 0 ]]; then
        warn "No subscriptions found in state file. Check az login permissions."
        return
    fi

    ok "${pending_n} subscription(s) queued.  Batch size: ${BATCH_SIZE} per pass."
    log "Automated scan starting — no manual steps required from here."
    log "  ${pending_n} subscriptions will be processed in passes of ${BATCH_SIZE}."
    log "  Token will be verified before each pass."
    log "  Failed subscriptions will be retried up to ${MAX_RETRY} time(s) automatically."

    local pass=1
    local total_passes=$(( (pending_n + BATCH_SIZE - 1) / BATCH_SIZE ))

    while true; do
        pending_n=$(count_status "pending")
        done_n=$(count_status "done")
        failed_n=$(count_status "failed")

        [[ "${pending_n}" -eq 0 ]] && break

        printf '\n%s\n' "${SEP}" | tee -a "${LOG_FILE}"
        info "SCAN PASS ${pass} of ~${total_passes}  —  pending=${pending_n}  done=${done_n}  failed=${failed_n}  [automated]"
        printf '%s\n' "${THIN}" | tee -a "${LOG_FILE}"

        verify_token

        run_sizing \
            --resume \
            --batch-size      "${BATCH_SIZE}" \
            --state-file      "${STATE_FILE}" \
            --results-file    "${RESULTS_FILE}" \
            --heartbeat-sec   "${HEARTBEAT}" \
            --sub-timeout-min "${SUB_TIMEOUT}" \
            2>&1 | tee -a "${LOG_FILE}"

        pass=$(( pass + 1 ))
        [[ "${_interrupted}" -eq 1 ]] && break
    done

    # Retry passes
    local retry=1
    while [[ "${retry}" -le "${MAX_RETRY}" ]]; do
        failed_n=$(count_status "failed")
        [[ "${failed_n}" -eq 0 ]] && break

        printf '\n%s\n' "${SEP}" | tee -a "${LOG_FILE}"
        warn "RETRY PASS ${retry} of ${MAX_RETRY}  —  ${failed_n} failed subscription(s)"
        printf '%s\n' "${THIN}" | tee -a "${LOG_FILE}"

        verify_token

        run_sizing \
            --resume \
            --retry-failed \
            --batch-size      "${BATCH_SIZE}" \
            --state-file      "${STATE_FILE}" \
            --results-file    "${RESULTS_FILE}" \
            --heartbeat-sec   "${HEARTBEAT}" \
            --sub-timeout-min "${SUB_TIMEOUT}" \
            2>&1 | tee -a "${LOG_FILE}"

        retry=$(( retry + 1 ))
        [[ "${_interrupted}" -eq 1 ]] && break
    done

    done_n=$(count_status "done")
    failed_n=$(count_status "failed")
    ok "Scan complete  —  done=${done_n}  failed=${failed_n}"

    if [[ "${failed_n}" -gt 0 ]]; then
        warn "${failed_n} subscription(s) could not be scanned after ${MAX_RETRY} retry pass(es)."
        warn "To retry now:  ./run-sizing.sh retry"
        warn "Full log:      ${LOG_FILE}"
    fi
}

stage_tenant_scan() {
    if [[ -n "${SKIP_TENANT_SCAN}" ]]; then
        info "Skipping tenant scan  (CC_SKIP_TENANT_SCAN is set)."
        return
    fi

    printf '\n%s\n' "${SEP}" | tee -a "${LOG_FILE}"
    info "STAGE: Tenant scan  (Entra ID user count + tenant-level diagnostic settings)"
    printf '%s\n' "${THIN}" | tee -a "${LOG_FILE}"

    verify_token

    # Tenant scan does two things:
    #   1. Counts Entra ID users (Member + Guest, enabled) via Microsoft Graph.
    #      Requires User.Read.All Application permission with admin consent.
    #      If User.Read.All is not granted, the scan completes without error but
    #      SaaS workloads = 0 and the Grand Total is flagged ⚠ in the summary.
    #   2. Discovers tenant-level diagnostic settings (which LAW workspaces,
    #      Event Hubs, or storage accounts receive Entra ID audit logs).
    #      These are configured at the tenant root and are invisible to
    #      per-subscription diagnostic setting scans.
    # Failure is non-fatal — the sizing report is still produced without SaaS users.
    run_sizing \
        --tenant-scan \
        --tenant-file "${TENANT_FILE}" \
        2>&1 | tee -a "${LOG_FILE}" \
        || warn "Tenant scan exited with an error. Check ${LOG_FILE} for details."
}

stage_summary() {
    printf '\n%s\n' "${SEP}" | tee -a "${LOG_FILE}"
    info "STAGE: Generating sizing report and Excel workbook"
    printf '%s\n' "${THIN}" | tee -a "${LOG_FILE}"

    if [[ ! -f "${RESULTS_FILE}" ]]; then
        warn "Results file not found: ${RESULTS_FILE}"
        warn "Run the scan stage first:  ./run-sizing.sh scan"
        return
    fi

    local summary_args=(
        --results "${RESULTS_FILE}"
        --state   "${STATE_FILE}"
        --xlsx    "${XLSX_FILE}"
    )
    [[ -f "${TENANT_FILE}" ]] && summary_args+=(--tenant "${TENANT_FILE}")

    python3 "${SUMMARY_PY}" "${summary_args[@]}" 2>&1 | tee -a "${LOG_FILE}"

    ok "Excel workbook : ${XLSX_FILE}"
    ok "Run log        : ${LOG_FILE}"
}

# ════════════════════════════════════════════════════════════════════════════════
# Entry point
# ════════════════════════════════════════════════════════════════════════════════

ACTION="${1:-all}"

printf '\n%s\n' "${SEP}" | tee -a "${LOG_FILE}"
printf '%b\n'  "${BOLD}  Cortex Cloud — Azure Workload Sizing${RESET}" | tee -a "${LOG_FILE}"
log "  Action      : ${ACTION}"
log "  Work dir    : ${WORK_DIR}"
log "  Log file    : ${LOG_FILE}"
log "  Batch size  : ${BATCH_SIZE} subscriptions per pass"
log "  Max retries : ${MAX_RETRY} retry pass(es)  |  Sub timeout: ${SUB_TIMEOUT} min"
[[ -n "${NO_VERIFY_SSL}" ]] && log "  SSL verify  : DISABLED (CC_NO_VERIFY_SSL=1)"
log "  Auth mode   : Azure CLI (az login)"
printf '%s\n' "${SEP}" | tee -a "${LOG_FILE}"

# Validate required files exist before doing anything
[[ -f "${SIZING_PY}"    ]] || die "az-sizing.py not found at ${SIZING_PY}"
[[ -f "${SUMMARY_PY}"   ]] || die "az-summary.py not found at ${SUMMARY_PY}"
[[ -f "${REQUIREMENTS}" ]] || die "requirements-azure.txt not found at ${REQUIREMENTS}"

setup_venv
authenticate

case "${ACTION}" in

  preflight)
    stage_preflight
    ;;

  scan)
    stage_preflight
    stage_scan
    ;;

  tenant)
    stage_tenant_scan
    ;;

  retry)
    verify_token
    failed_n=$(count_status "failed")
    if [[ "${failed_n}" -eq 0 ]]; then
        ok "No failed subscriptions — nothing to retry."
    else
        warn "Retrying ${failed_n} failed subscription(s)  (pass 1 of up to ${MAX_RETRY})..."
        run_sizing \
            --resume \
            --retry-failed \
            --batch-size       "${BATCH_SIZE}" \
            --state-file       "${STATE_FILE}" \
            --results-file     "${RESULTS_FILE}" \
            --heartbeat-sec    "${HEARTBEAT}" \
            --sub-timeout-min  "${SUB_TIMEOUT}" \
            2>&1 | tee -a "${LOG_FILE}" \
            || true
    fi
    ;;

  summary)
    stage_summary
    ;;

  all)
    stage_preflight
    stage_scan
    stage_tenant_scan
    stage_summary
    printf '\n%s\n' "${SEP}" | tee -a "${LOG_FILE}"
    ok "=== All stages complete  |  Log: ${LOG_FILE} ==="
    printf '%s\n' "${SEP}" | tee -a "${LOG_FILE}"
    ;;

  help|--help|-h)
    cat <<USAGE

Usage: $(basename "$0") [ACTION]

Actions:
  all         Full run: preflight → scan → tenant → summary  [default]
  preflight   Permission checks only
  scan        Discover + scan subscriptions  (includes preflight)
  tenant      Entra ID user count + tenant-level diagnostic settings discovery
  retry       Retry currently-failed subscriptions
  summary     Regenerate report from existing results
  help        Show this help

Environment variables  (all optional):
  CC_BATCH_SIZE        Subscriptions per scan pass             [${BATCH_SIZE}]
  CC_MAX_RETRY         Retry passes after initial scan         [${MAX_RETRY}]
  CC_SUB_TIMEOUT       Per-subscription timeout (minutes)      [${SUB_TIMEOUT}]
  CC_HEARTBEAT         Heartbeat interval (seconds)            [${HEARTBEAT}]
  CC_NO_VERIFY_SSL     Set to 1 for --no-verify-ssl
                       (use when behind Zscaler / NGFW SSL proxy)
  CC_SKIP_TENANT_SCAN  Set to 1 to skip Entra ID user count
  CC_WORK_DIR          Output directory                        [${WORK_DIR}]
  CLIENT_IP            Override the public IP used in firewall fix strings.
                       Required if api.ipify.org is blocked by your proxy.

Examples:
  # Standard interactive run
  ./run-sizing.sh

  # Behind an SSL-inspecting proxy  (Zscaler / Palo Alto NGFW)
  CC_NO_VERIFY_SSL=1 ./run-sizing.sh

  # Large tenant — increase batch size and retry budget
  CC_BATCH_SIZE=50 CC_MAX_RETRY=3 ./run-sizing.sh

  # Write outputs to a separate directory
  CC_WORK_DIR=/mnt/sizing-output ./run-sizing.sh

  # Resume after interruption  (already-done subs are skipped automatically)
  ./run-sizing.sh scan

  # Regenerate Excel from an existing results file
  ./run-sizing.sh summary

USAGE
    exit 0
    ;;

  *)
    die "Unknown action '${ACTION}'.  Run '$(basename "$0") help' for usage."
    ;;

esac

deactivate 2>/dev/null || true
