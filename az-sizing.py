#!/usr/bin/env python3
"""
az-sizing.py  —  Cortex Cloud Azure Workload Sizing
=====================================================
Scans every Azure subscription and writes per-subscription workload counts
to azure_results.json, which az-summary.py reads to produce the final table.

API STRATEGY — why different resources use different APIs
─────────────────────────────────────────────────────────
This script uses a hybrid approach, choosing the right API for each resource:

  Azure Resource Graph  — fast single-query discovery for resources where
    existence = billable unit and no sub-resource detail is needed.
    Used for: Running VMs (power state via properties.extended.instanceView),
    Storage Accounts (count only), Cosmos DB (publicNetworkAccess filter),
    Azure SQL databases (sub-resource type, master excluded).
    ARM fallback is retained for each in case Resource Graph is blocked.

  ARM SDK / REST  — required when the billable unit lives in a sub-resource
    or when state/configuration not indexed by Resource Graph is needed.
    Used for: AKS agent-pool node counts, ARO worker profiles, ACI container
    counts, Container App revision replicas, Function App function enumeration
    (microsoft.web/sites/functions is not a Resource Graph resource type),
    ACR management plane (registry discovery), Event Hub namespaces.

  ACR data plane (api.azurecr.io)  — tag and manifest counts live behind
    the registry data plane; the ARM management plane only knows the registry
    exists.  Used for: ACR image tag and manifest counts.

  Azure Blob Storage data plane  — actual ingestion volume is in the blobs
    themselves; no ARM or Graph API surface exposes GB/day.
    Used for: Event Hub capture blob measurement (audit logs), VNet/NSG
    flow-log blob measurement (flow logs).

  Log Analytics Query API (api.loganalytics.io)  — customers who route
    logs to a Log Analytics Workspace instead of (or in addition to) Event Hub
    or blob storage.  Queries the Usage table over a 7-day window.
    Requires a separate OAuth2 token scope from ARM.
    Used for: Audit log volume only — all Cortex Cloud audit DataTypes
    (AzureActivity, AuditLogs, MicrosoftGraphActivityLogs, SigninLogs + 4 variants
    including ADFSSignInLogs, ProvisioningLogs, AKSAudit + AKSAuditAdmin + AKSControlPlane).
    NOT used for flow log measurement — see flow log note below.
  Microsoft Graph API (graph.microsoft.com)  — directory objects only.
    Used for: Entra ID user counts (Member + Guest, enabled accounts).
    Not used for any resource inventory — that is ARM/Resource Graph territory.

Resources counted per subscription
───────────────────────────────────────────────────────────────────────────────
COMPUTE  (1 workload per VM / node)
  • Running VMs        — Resource Graph (properties.extended.instanceView.powerState)
  • AKS nodes          — ARM agent_pools.list() per cluster (sub-resource, not in RG)
  • ARO nodes          — ARM open_shift_clusters.list(); 3 masters fixed + worker profiles

CaaS  (10 containers = 1 workload)
  • ACI containers     — ARM container_groups.list(); containers[] per succeeded group
  • Container Apps     — ARM container_apps.list_by_subscription(); template.containers per app
                         (already on the list response — no second API call)

SERVERLESS  (25 function apps = 1 workload)
  • Azure Functions    — ARM web_apps.list() filtered to kind=functionapp, state=Running
                         Each running function app = 1 billing unit.  Logic Apps Standard
                         (kind=workflowapp) and stopped apps are excluded.

DATABASES  (2 DBs = 1 workload)
  • Azure SQL databases — Resource Graph (microsoft.sql/servers/databases, master excluded)
  • Cosmos DB accounts  — Resource Graph (publicNetworkAccess == Enabled filter)

STORAGE  (10 accounts = 1 workload)
  • Storage Accounts   — Resource Graph (microsoft.storage/storageaccounts)

CONTAINER IMAGES  (10 images = 1 workload, after free allowance)
  • ACR tags across all repositories / registries in the subscription
    (manifest count is also collected for reference only)
  • Free allowance formula:
      (vm_running + aks_nodes + aro_nodes) × 10      ← 10 per VM / node
    + floor(total_caas_containers / 10)  × 10        ← 10 per group of 10 CaaS containers

LOG INGESTION SIZING  (exact measured values only — no estimates or assumptions)
  ─────────────────────────────────────────────────────────────────────────────
  Included ingestion = total_workloads / 50  GB/day

  Audit Logs  (Posture + Runtime SKU)  — two independent measurement paths:
    Path EH:  Event Hub Capture — discovers EH namespaces, identifies hubs with
              Capture enabled, measures blob sizes on capture storage account.
              7-day average.  Requires Storage Blob Data Reader on capture storage.
              Covers ALL log types routed to the hub (Activity, AD, AKS resource logs).
    Path LAW: Log Analytics Workspace — queries the Usage table on any LAW
              workspace found as a diagnostic sink for audit logs.
              Covers: AzureActivity, AuditLogs, MicrosoftGraphActivityLogs,
              SigninLogs (+ NonInteractive, ServicePrincipal, ManagedIdentity, ADFS),
              ProvisioningLogs, AKSAudit, AKSAuditAdmin, AKSControlPlane.
              Requires Log Analytics Reader.  Uses api.loganalytics.io.
    Both paths run independently. If both are measured, dual-sink is flagged.

  Flow Logs  (Runtime SKU only)  — blob listing only:
    Cortex ingests flow logs via a dedicated Azure Function that reads raw blobs
    directly from Network Watcher storage (Cortex Azure NSG/VNet Flow Logs Collector).
    Containers measured:
      insights-logs-flowlogflowevent                    (VNet, current)
      insights-logs-networksecuritygroupflowevent       (NSG, retired June 2025)
    7-day average.  Requires Storage Blob Data Reader on flow-log storage accounts.
    LAW Traffic Analytics tables (NTANetAnalytics, AzureNetworkAnalytics_CL) are
    NOT measured — they reflect a separate aggregation pipeline, not Cortex ingestion.

  Entra ID audit logs  — tenant-level diagnostic settings scanned once during
    --tenant-scan.  API: /providers/microsoft.aadiam/diagnosticSettings.
    These settings are invisible to per-subscription diagnostic setting scans.

  DNS Logs  (Runtime SKU only):
    NOT AVAILABLE — cannot be sized automatically.  Collect manually from the
    customer via Azure Monitor / DNS resolver diagnostic settings export.

SKU split
  C1  = Storage + Database workloads
  C3  = VM/Node + Serverless + CaaS workloads
  IMG = Net Container Image workloads (after free allowance)
  TOTAL = C1 + C3 + IMG

Ingestion summary
  Included GB/day            = TOTAL / 50
  Posture additional GB/day  = max(0,  audit_gb_day  − included_gb_day)
  Runtime additional GB/day  = max(0,  audit+flow_gb_day  − included_gb_day)
                               DNS always excluded (NOT AVAILABLE)

Workflow
  python3 az-sizing.py --init-state                      # discover subs
  python3 az-sizing.py --resume --batch-size 25          # process in batches
  python3 az-sizing.py --resume --retry-failed           # retry failures
  python3 az-summary.py --results azure_results.json     # print summary

Permissions required (beyond basic ARM Reader):
  Storage Blob Data Reader   — on each subscription (flow logs + audit EH capture)
  AcrPull / AcrMetadataRead  — on each subscription (ACR image count)
  Log Analytics Reader       — on each subscription (LAW log volume queries)
                               Optional: only needed if customer routes logs to LAW
  EventHub namespace listing — covered by standard ARM Reader role
  Resource Graph queries     — covered by standard ARM Reader role (no extra role needed)
"""

import json
import math
import os
import argparse
import time
import base64
import requests as _requests
from datetime import datetime, timedelta, timezone

SEPARATOR = "─" * 155

# ──────────────────────────────────────────────────────────────────────────────
# Cortex Cloud metering ratios
# ──────────────────────────────────────────────────────────────────────────────
CC_METERING = {
    "vm":         1,
    "caas":       10,
    "serverless": 25,
    "buckets":    10,
    "db":         2,
    "images":     10,
    "saas_users": 10,
}

CC_METERING_TABLE = [
    ("VMs / Kubernetes Nodes",               "1 per VM / node"),
    ("CaaS – ACI + Container Apps",          "10 containers  → 1 workload"),
    ("Serverless – Azure Functions",         "25 function apps → 1 workload"),
    ("Storage Accounts",                     "10 accounts    → 1 workload"),
    ("Managed Databases  (SQL + Cosmos)",    "2 DBs          → 1 workload"),
    ("Container Images  (net of allowance)", "10 images      → 1 workload"),
    ("  Free allowance per VM / node",       "10 free image scans"),
    ("  Free allowance per 10 CaaS ctrs",    "10 free image scans"),
    ("Included log ingestion",               "1 GB/day per 50 total workloads"),
    ("  Posture SKU  — log types",           "Audit Logs  (Event Hub capture blobs OR Log Analytics Workspace)"),
    ("  Runtime SKU  — log types",           "Audit Logs (EH blob or LAW)  +  Flow Logs (blob only)"),
    ("Additional ingestion (if any)",        "Shown when exact measurements available; dual-sink flagged separately"),
]

# Blob containers written by flow logs
# NSG flow logs (retired June 2025, existing deployments still write here):
_FLOW_LOG_CONTAINER_NSG  = "insights-logs-networksecuritygroupflowevent"
# VNet flow logs (replacement for NSG flow logs, new deployments use this):
_FLOW_LOG_CONTAINER_VNET = "insights-logs-flowlogflowevent"
# Both are checked during blob measurement
_FLOW_LOG_CONTAINERS = [_FLOW_LOG_CONTAINER_NSG, _FLOW_LOG_CONTAINER_VNET]
# Blob listing cap per storage account
_MAX_BLOBS_PER_ACCOUNT = 10_000

# Sentinel for "could not be determined"
_NOT_AVAILABLE = None

# ──────────────────────────────────────────────────────────────────────────────
# Diagnostic schema
# ──────────────────────────────────────────────────────────────────────────────
# Every issue the scan encounters becomes one structured DiagnosticRecord, used
# by both console summary and the Excel Diagnostics sheet.  Categories are
# closed-set strings so the renderer can group/filter without string parsing.
#
# IMPACT codes describe what the operator loses if the issue is not resolved.
# FIX strings are copy-pasteable Azure CLI commands wherever possible.

_DIAG_CATEGORIES = {
    "rbac_missing_sbdr",          # missing Storage Blob Data Reader on a storage account
    "rbac_missing_law_reader",    # missing Log Analytics Reader on a workspace
    "rbac_missing_eh_reader",     # missing Reader on the EH namespace
    "rbac_missing_acr_pull",      # missing AcrPull on a registry
    "firewall_blocked",           # SBDR (or equivalent) is granted but storage firewall blocks the request
    "private_endpoint_only",      # storage account has no public DNS — only reachable from inside the VNet
    "ssl_intercept",              # corporate TLS interception (self-signed cert in chain)
    "container_not_present",      # well-known container does not exist on this account (informational)
    "tenant_mismatch",            # subscription is in a different tenant than the auth context
    "not_configured",             # feature exists but is disabled or has no destination configured
}

_IMPACT_CODES = {
    "flow_log_unmeasured":        "Flow log GB/day for this source not counted",
    "audit_log_unmeasured":       "Audit log GB/day for this source not counted",
    "law_volume_unmeasured":      "LAW workspace ingestion volume not counted",
    "acr_image_count_unmeasured": "ACR image/tag count not collected",
    "eh_capture_unmeasured":      "Event Hub capture blob volume not counted",
}

# Well-known Azure built-in role definition GUIDs.
# Source: Azure docs (these IDs are the same across every tenant).
_ROLE_GUIDS = {
    "acdd72a7-3385-48ef-bd42-f606fba81ae7": "Reader",
    "b24988ac-6180-42a0-ab88-20f7382dd24c": "Contributor",
    "8e3af657-a8ff-443c-a75c-2fe8c4bcb635": "Owner",
    "2a2b9908-6ea1-4ae2-8e65-a410df84e7d1": "Storage Blob Data Reader",
    "ba92f5b4-2d11-453d-a403-e96b0029c9fe": "Storage Blob Data Contributor",
    "b7e6dc6d-f1e8-4753-8033-0f276bb0955b": "Storage Blob Data Owner",
    "73c42c96-874c-492b-b04d-ab87d138a893": "Log Analytics Reader",
    "92aaf0da-9dab-42b6-94a3-d43ce8d16293": "Log Analytics Contributor",
    "43d0d8ad-25c7-4714-9337-8ba259a9fe05": "Monitoring Reader",
    "7f951dda-4ed3-49bb-93ca-95158a3bd461": "AcrPull",
    "8311e382-0749-4cb8-b61a-304f252e45ec": "AcrPush",
    "a638d3c7-ab3a-418d-83e6-5f17a39d4fde": "Azure Event Hubs Data Receiver",
    "f526a384-b230-433a-b45c-95f59c4a2dec": "Azure Event Hubs Data Owner",
    "4d97b98b-1d4f-4787-a291-c67834d212e7": "Network Contributor",
}

# Roles that grant Storage Blob *data* plane reads (any one is sufficient for our needs).
_SBDR_EQUIVALENT_ROLES = {
    "Storage Blob Data Reader",
    "Storage Blob Data Contributor",
    "Storage Blob Data Owner",
}


def _decode_oid_from_token(cred, verify_ssl: bool = True) -> str | None:
    """
    Extract the signed-in object ID from an ARM access token's JWT 'oid' claim.
    Avoids the `az ad signed-in-user show` call that Conditional Access can block.
    Returns None on any failure — caller handles gracefully.
    """
    try:
        token = cred.get_token("https://management.azure.com/.default")
        payload = token.token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        claims  = json.loads(base64.urlsafe_b64decode(payload))
        return claims.get("oid")
    except Exception:
        return None


def fetch_sub_roles(sub_id: str, oid: str, cred, verify_ssl: bool = True) -> dict:
    """
    Return a dict mapping role-name → list of scopes where the OID has that role
    in this subscription.  One ARM call per subscription, ~1s.  Cached implicitly
    by being called once per subscription scan.

    Custom roles are skipped (only well-known built-in role GUIDs are resolved).
    Returns {} if the call fails — callers must treat empty as "unknown" not "no roles".
    """
    import requests as _r
    if not oid:
        return {}
    try:
        token = cred.get_token("https://management.azure.com/.default")
        url = (
            f"https://management.azure.com/subscriptions/{sub_id}"
            f"/providers/Microsoft.Authorization/roleAssignments"
            f"?$filter=principalId%20eq%20'{oid}'"
            f"&api-version=2022-04-01"
        )
        resp = _r.get(
            url,
            headers={"Authorization": f"Bearer {token.token}"},
            verify=verify_ssl, timeout=15,
        )
        resp.raise_for_status()
        out: dict = {}
        for ra in resp.json().get("value", []):
            p = ra.get("properties", {})
            rd_id = (p.get("roleDefinitionId") or "").rsplit("/", 1)[-1].lower()
            scope = p.get("scope") or ""
            role_name = _ROLE_GUIDS.get(rd_id)
            if role_name and scope:
                out.setdefault(role_name, []).append(scope)
        return out
    except Exception:
        return {}


def has_role_for(role_name: str, resource_id: str, sub_roles: dict) -> bool:
    """
    True if the OID has `role_name` at any scope that contains `resource_id`.
    Works for sub > RG > resource hierarchy via case-insensitive prefix match.

    Special case: any one of _SBDR_EQUIVALENT_ROLES satisfies a check for
    "Storage Blob Data Reader".
    """
    if not resource_id:
        return False
    rid_lower = resource_id.lower()

    if role_name == "Storage Blob Data Reader":
        candidate_roles = _SBDR_EQUIVALENT_ROLES
    else:
        candidate_roles = {role_name}

    for r in candidate_roles:
        for scope in sub_roles.get(r, []):
            if rid_lower.startswith(scope.lower()):
                return True
    return False


def make_diagnostic(
    sub_id: str, sub_name: str,
    resource_type: str, resource_id: str, resource_name: str,
    category: str, impact: str,
    sub_path: str | None = None,
    fix: str | None = None,
    raw_error: str | None = None,
) -> dict:
    """
    Build a single DiagnosticRecord dict.  Categories and impacts must be from
    the closed sets above; unknown values raise so we catch typos in CI/dev.
    """
    if category not in _DIAG_CATEGORIES:
        raise ValueError(f"unknown diagnostic category: {category}")
    if impact not in _IMPACT_CODES:
        raise ValueError(f"unknown impact code: {impact}")
    return {
        "subscription_id":   sub_id,
        "subscription_name": sub_name,
        "resource_type":     resource_type,
        "resource_id":       resource_id,
        "resource_name":     resource_name,
        "resource_sub_path": sub_path or "",
        "issue_category":    category,
        "impact":            impact,
        "fix":               fix or "",
        "detected_at":       datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "raw_error":         (raw_error or "")[:500],
    }


def build_fix_string(category: str, oid: str, scope: str,
                     extra: dict | None = None) -> str:
    """
    Compose a copy-pasteable remediation command for a given category.
    Returns a placeholder note for categories that have no script-based fix.
    """
    extra = extra or {}
    if category == "rbac_missing_sbdr":
        return (
            f"az role assignment create "
            f"--role 'Storage Blob Data Reader' "
            f"--assignee {oid or '<your-oid>'} "
            f"--scope {scope}"
        )
    if category == "rbac_missing_law_reader":
        return (
            f"az role assignment create "
            f"--role 'Log Analytics Reader' "
            f"--assignee {oid or '<your-oid>'} "
            f"--scope {scope}"
        )
    if category == "rbac_missing_eh_reader":
        return (
            f"az role assignment create "
            f"--role 'Reader' "
            f"--assignee {oid or '<your-oid>'} "
            f"--scope {scope}"
        )
    if category == "rbac_missing_acr_pull":
        return (
            f"az role assignment create "
            f"--role 'AcrPull' "
            f"--assignee {oid or '<your-oid>'} "
            f"--scope {scope}"
        )
    if category == "firewall_blocked":
        client_ip = (extra.get("client_ip") or "").strip() or "<your-public-ip>"
        return (
            f"Add IP {client_ip} to storage account network allowlist:  "
            f"Portal → Storage Account → Networking → Firewalls and virtual networks → Add your client IP"
        )
    if category == "private_endpoint_only":
        return "Run the script from inside the customer's VNet (storage account is private-endpoint-only, no public DNS)."
    if category == "ssl_intercept":
        return "Corporate TLS interception detected; re-run with --no-verify-ssl flag, or whitelist *.azurecr.io / *.blob.core.windows.net in your proxy."
    if category == "container_not_present":
        return "No fix required — well-known container does not exist on this account (informational)."
    if category == "tenant_mismatch":
        return "Re-run from a session authenticated to the subscription's home tenant (az login --tenant <tenant-id>)."
    if category == "not_configured":
        return "Customer must enable the relevant log routing (Diagnostic Settings → Event Hub or Log Analytics) before measurement is possible."
    return ""


# ──────────────────────────────────────────────────────────────────────────────

# ── Log Analytics Workspace (LAW) constants ──────────────────────────────────
# Query API endpoint (different OAuth2 audience than ARM)
_LAW_QUERY_ENDPOINT   = "https://api.loganalytics.io"
_LAW_API_AUDIENCE     = "https://api.loganalytics.io/.default"
_LAW_ARM_API_VERSION  = "2023-09-01"  # OperationalInsights workspaces
_LAW_QUERY_API_VER    = "v1"

# ── Audit log DataTypes queried from LAW Usage table ─────────────────────────
#
# Cortex Cloud ingests Azure audit logs via Event Hub (primary path).
# When customers also route logs to a Log Analytics Workspace, the Usage table
# measures what is stored there — used as a proxy for ingestion volume.
#
# Sources confirmed from:
#   (a) Cortex XQL: dataset = cloud_audit_logs | filter cloud_provider = ENUM.Azure
#   (b) Cortex XSIAM Event Hub integration doc (log category configuration table)
#
# Cortex cloud_audit_logs log_name → LAW DataType:
#   microsoft_graph_activity_logs  → MicrosoftGraphActivityLogs  (typically largest)
#   azure_ad_signin_logs           → SigninLogs + four variants below
#   azure_ad_audit_logs            → AuditLogs
#   azure_ad_provisioning_logs     → ProvisioningLogs
#   (subscription activity)        → AzureActivity
#
# AKS / resource audit logs:
#   Cortex ingests resource logs (including AKS audit logs) via Event Hub.
#   If the customer also routes to LAW, AKS logs land in dedicated tables
#   (newer clusters) or AzureDiagnostics with Category=kube-audit (older).
#   Both paths are included below so no AKS volume is missed in LAW.
#   Note: the EH capture blob path already covers AKS logs automatically
#   when AKS diagnostic settings point to the same Event Hub.
#
# All types must be queried — omitting any understates GB/day to Cortex Cloud.
_LAW_AUDIT_DATA_TYPES = {
    # Subscription-level
    "AzureActivity",                 # subscription activity / control plane (all 8 categories)
    # Entra ID / Azure AD
    "AuditLogs",                     # Entra ID audit events
    "MicrosoftGraphActivityLogs",    # Microsoft Graph API calls — typically the largest source
    "SigninLogs",                    # interactive user sign-ins
    "NonInteractiveUserSignInLogs",  # non-interactive (token refresh, cached credentials)
    "ServicePrincipalSignInLogs",    # service principal / app authentication
    "ManagedIdentitySignInLogs",     # managed identity authentication
    "ADFSSignInLogs",                # ADFS federation sign-ins (on-prem to cloud)
    "ProvisioningLogs",              # Entra ID provisioning events
    # AKS / resource audit logs (data-plane, ingested via Event Hub resource diag settings)
    "AKSAudit",                      # AKS kube-audit (dedicated table — newer clusters)
    "AKSAuditAdmin",                 # AKS kube-audit-admin (dedicated table — newer clusters)
    "AKSControlPlane",               # AKS control plane logs (dedicated table — newer clusters)
}

# ── Flow log DataTypes — LAW is NOT the Cortex ingestion path ────────────────
#
# Cortex ingests flow logs via a dedicated Azure Function that reads raw blobs
# directly from storage (container = insights-logs-flowlogflowevent for VNet,
# insights-logs-networksecuritygroupflowevent for NSG).
#
# LAW tables such as NTANetAnalytics, AzureNetworkAnalytics_CL, and
# AzureNetworkWatcherFlowLog are written by Traffic Analytics — a Microsoft
# product that aggregates flow data separately.  These tables reflect a
# different pipeline and a different (aggregated, lower) volume than what
# Cortex actually ingests.  Measuring them would produce a number unrelated
# to actual Cortex flow log ingestion volume.
#
# Flow log sizing = blob listing only.  LAW flow measurement is not performed.
# _LAW_FLOW_DATA_TYPES is intentionally empty — kept as a named constant so
# the query builder does not need conditional logic.
_LAW_FLOW_DATA_TYPES: set = set()

# AzureDiagnostics legacy path:
# NSG flow events (Category=NetworkSecurityGroupFlowEvent) land here for older
# deployments.  However, since LAW is not the Cortex flow log ingestion path,
# this legacy query is also removed.  Blob listing covers both NSG and VNet.
_LAW_LEGACY_DIAG_TABLE = "AzureDiagnostics"
_LAW_LEGACY_FLOW_CATEGORY = "NetworkSecurityGroupFlowEvent"

# Entra ID tenant-level diagnostic settings API
_ENTRA_DIAG_API = (
    "https://management.azure.com/providers/microsoft.aadiam"
    "/diagnosticSettings?api-version=2017-04-01-preview"
)


# ──────────────────────────────────────────────────────────────────────────────
# Timestamp
# ──────────────────────────────────────────────────────────────────────────────
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


# ──────────────────────────────────────────────────────────────────────────────
# State file helpers  (JSONL – one subscription per line)
# ──────────────────────────────────────────────────────────────────────────────
def load_state(path: str) -> list:
    rows = []
    if not os.path.exists(path):
        return rows
    with open(path, "r") as fh:
        for line in fh:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def write_state(path: str, rows: list) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w") as fh:
        for r in rows:
            fh.write(json.dumps(r) + "\n")
    os.replace(tmp, path)


# ──────────────────────────────────────────────────────────────────────────────
# Results file helper  (JSON – upsert per subscription, no duplicates)
# ──────────────────────────────────────────────────────────────────────────────
def upsert_results(results_file: str, sub_id: str, payload: dict) -> None:
    data = {}
    if os.path.exists(results_file):
        with open(results_file, "r") as fh:
            data = json.load(fh) or {}
    data[sub_id] = payload
    tmp = results_file + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(data, fh, indent=2)
    os.replace(tmp, results_file)


# ──────────────────────────────────────────────────────────────────────────────
# Subscription selection
# ──────────────────────────────────────────────────────────────────────────────
def select_pending(rows: list, retry_failed: bool = False, batch_size: int = 0) -> list:
    allowed = {"pending"}
    if retry_failed:
        allowed.add("failed")
    selected = [r for r in rows if r.get("status") in allowed]
    if batch_size > 0:
        selected = selected[:batch_size]
    return selected


# ──────────────────────────────────────────────────────────────────────────────
# Heartbeat
# ──────────────────────────────────────────────────────────────────────────────
def heartbeat(msg: str, last_ts: float, interval_sec: int) -> float:
    now = time.time()
    if now - last_ts >= interval_sec:
        print(msg, flush=True)
        return now
    return last_ts


# ──────────────────────────────────────────────────────────────────────────────
# Auth-error detection
# ──────────────────────────────────────────────────────────────────────────────
def is_auth_error(exc: Exception) -> bool:
    """
    Returns True only for genuine authentication failures (expired token,
    missing credential, AADSTS errors).  HTTP 403 / AuthorizationPermissionMismatch
    is a PERMISSION error — not an auth error — and must NOT stop the run.
    """
    msg = (str(exc) or "").lower()
    keywords = [
        "authentication", "expired", "aadsts", "invalid_grant",
        "token", "unauthorized", "credential",
    ]
    if "authorizationpermissionmismatch" in msg or "permission" in msg:
        return False
    return any(k in msg for k in keywords)


# ──────────────────────────────────────────────────────────────────────────────
# Blob size measurement helpers
# ──────────────────────────────────────────────────────────────────────────────
def measure_blob_container_gb_day(
    blob_svc_client,
    container_name: str,
    days: int = 7,
    max_blobs: int = _MAX_BLOBS_PER_ACCOUNT,
    warn_fn=None,
    label: str = "",
) -> tuple:
    """
    General-purpose blob measurement for audit EH capture containers.
    Lists all blobs, filters by last_modified, sums sizes.
    Returns (gb_per_day, capped, accessible).

    EH capture containers are small (one Avro file per partition per hour),
    so a full listing is fast. The cap remains as a safety guard.
    """
    cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=days)
    container_cli = blob_svc_client.get_container_client(container_name)

    period_bytes = 0
    n_blobs = 0

    try:
        for blob in container_cli.list_blobs():
            if n_blobs >= max_blobs:
                if warn_fn:
                    warn_fn(
                        label or container_name,
                        Exception(
                            f"Blob listing exceeded safe cap of {max_blobs:,} — "
                            f"exact daily average not available for this container"
                        ),
                    )
                return _NOT_AVAILABLE, True, True

            n_blobs += 1
            lm = blob.last_modified
            if lm:
                lm_naive = lm.replace(tzinfo=None) if lm.tzinfo else lm
                if lm_naive >= cutoff:
                    period_bytes += blob.size or 0
    except Exception as exc:
        msg = str(exc).lower()
        if "containernotfound" in msg or "does not exist" in msg:
            return _NOT_AVAILABLE, False, False
        raise

    if period_bytes == 0:
        return 0.0, False, True

    gb_per_day = round(period_bytes / days / (1024 ** 3), 4)
    return gb_per_day, False, True


def _build_flow_log_prefix_base(spec: dict) -> str | None:
    """
    Build the per-resource blob prefix root for a single flow log.

    NSG flow logs  (container: insights-logs-networksecuritygroupflowevent):
      resourceId=/SUBSCRIPTIONS/{SUB}/RESOURCEGROUPS/{NSG_RG}/PROVIDERS/
      MICROSOFT.NETWORK/NETWORKSECURITYGROUPS/{NSG}

    VNet flow logs (container: insights-logs-flowlogflowevent):
      flowLogResourceID=/{SUB}_{FLOWLOG_RG}/{FLOWLOG_NAME}

    Note the asymmetry — NSG paths key on the *target* resource, VNet paths
    key on the *flow log resource itself*.  All segments are uppercased to
    match what Azure writes.  Returns None if the spec is missing a field
    needed for its container type.
    """
    container = spec.get("container_name", "")

    if container == _FLOW_LOG_CONTAINER_VNET:
        sub_id      = (spec.get("flow_log_sub_id") or "").upper()
        fl_rg       = (spec.get("flow_log_rg") or "").upper()
        watcher     = (spec.get("flow_log_watcher_name") or "").upper()
        fl_name     = (spec.get("flow_log_name") or "").upper()
        if not (sub_id and fl_rg and watcher and fl_name):
            return None
        # Azure flattens the flow log's parent (network watcher) and leaf names
        # into a single segment joined by underscore.
        return f"flowLogResourceID=/{sub_id}_{fl_rg}/{watcher}_{fl_name}"

    if container == _FLOW_LOG_CONTAINER_NSG:
        # Target is /subscriptions/{S}/resourceGroups/{RG}/providers/Microsoft.Network/networkSecurityGroups/{N}
        target = spec.get("target_id") or ""
        parts  = target.split("/")
        try:
            sub_idx  = next(i for i, p in enumerate(parts) if p.lower() == "subscriptions")
            sub      = parts[sub_idx + 1].upper()
            rg_idx   = next(i for i, p in enumerate(parts) if p.lower() == "resourcegroups")
            rg_name  = parts[rg_idx + 1].upper()
            pr_idx   = next(i for i, p in enumerate(parts) if p.lower() == "providers")
            res_type = parts[pr_idx + 2].upper()
            res_name = parts[pr_idx + 3].upper()
        except (StopIteration, IndexError):
            return None
        return (
            f"resourceId=/SUBSCRIPTIONS/{sub}"
            f"/RESOURCEGROUPS/{rg_name}"
            f"/PROVIDERS/MICROSOFT.NETWORK"
            f"/{res_type}/{res_name}"
        )

    return None


def measure_flow_blobs_by_prefix(
    blob_svc_client,
    container_name: str,
    flow_log_specs: list,
    days: int = 7,
) -> tuple:
    """
    Measure flow log blob sizes using date-based prefix listing.

    Flow log containers accumulate blobs indefinitely — a busy subscription
    can have 100,000+ blobs going back years.  Iterating all of them to filter
    by last_modified is slow and unnecessary because the date is encoded
    directly in the blob path.

    flow_log_specs: list of dicts, each describing one enabled flow log:
        {
          "flow_log_id":    full ARM ID of the flow log resource itself,
          "flow_log_sub_id":subscription containing the flow log resource,
          "flow_log_rg":    resource group of the flow log resource (usually NetworkWatcherRG),
          "flow_log_name":  name of the flow log resource,
          "target_id":      full ARM ID of the NSG or VNet being monitored,
          "container_name": which blob container this flow log writes to,
        }

    Container_name is also passed explicitly so the caller can scope each call
    to one container.  Specs whose container_name differs from container_name
    are skipped silently.

    For each of the last `days` days, constructs a prefix and calls
    list_blobs(name_starts_with).  Each prefix matches ~24 blobs per resource
    per day (one per hour, possibly multiplied by NIC count for VNet logs).

    Returns (gb_per_day, accessible):
      gb_per_day  — float (may be 0.0 if no blobs in window)
      accessible  — False if container does not exist or is unreachable
    """
    from datetime import date as _date, timedelta as _td

    container_cli = blob_svc_client.get_container_client(container_name)
    total_bytes   = 0
    today         = _date.today()

    # Verify container is accessible with a cheap probe before iterating
    try:
        container_cli.get_container_properties()
    except Exception as exc:
        msg = str(exc).lower()
        if "containernotfound" in msg or "does not exist" in msg:
            return 0.0, False   # container absent — not an error for this target type
        raise                   # auth / network error — caller handles

    for spec in flow_log_specs:
        if spec.get("container_name") != container_name:
            continue

        base = _build_flow_log_prefix_base(spec)
        if not base:
            continue

        for offset in range(days):
            day    = today - _td(days=offset)
            prefix = f"{base}/y={day.year}/m={day.month:02d}/d={day.day:02d}/"
            try:
                for blob in container_cli.list_blobs(name_starts_with=prefix):
                    total_bytes += blob.size or 0
            except Exception:
                pass   # individual day/prefix failures are non-fatal

    gb_per_day = round(total_bytes / days / (1024 ** 3), 4)
    return gb_per_day, True


# ──────────────────────────────────────────────────────────────────────────────
# SKU + ingestion calculation
# ──────────────────────────────────────────────────────────────────────────────
def compute_sku(counts: dict) -> dict:
    """
    Convert raw resource counts into Cortex Cloud workload SKUs and build the
    log-ingestion summary.

    Log values of None mean 'not available / not measured'.
    DNS logs are always None — no estimation is performed.
    Additional ingestion is only computed when exact measured values exist.
    """
    def wl(raw: int, rate: int) -> int:
        return math.ceil(raw / rate) if raw > 0 else 0

    vm_total   = counts["vm_running"] + counts["aks_nodes"] + counts["aro_nodes"]
    caas_total = counts["aci_containers"] + counts["container_app_containers"]
    db_total   = counts["azure_sql_dbs"] + counts["cosmos_db"]
    net_images = max(0, counts["acr_images"] - counts["free_image_allowance"])

    c1    = wl(counts["storage_accounts"], CC_METERING["buckets"]) + wl(db_total, CC_METERING["db"])
    c3    = (wl(vm_total, CC_METERING["vm"])
             + wl(counts["azure_functions"], CC_METERING["serverless"])
             + wl(caas_total, CC_METERING["caas"]))
    c_img = wl(net_images, CC_METERING["images"])
    total = c1 + c3 + c_img

    included_gb_day = round(total / 50, 4) if total > 0 else 0.0

    audit_gb = counts.get("audit_log_gb_day")   # float or None
    flow_gb  = counts.get("flow_log_gb_day")    # float or None
    # DNS: always None — never estimated

    def _additional(estimated):
        """Returns additional GB/day only when we have an exact measured value."""
        if estimated is None:
            return None
        return round(max(0.0, estimated - included_gb_day), 4)

    # Posture: audit only
    posture_additional = _additional(audit_gb)

    # Runtime: audit + flow only (DNS excluded — always NOT AVAILABLE)
    if audit_gb is not None and flow_gb is not None:
        runtime_total      = round(audit_gb + flow_gb, 4)
        runtime_additional = _additional(runtime_total)
    else:
        runtime_total      = None
        runtime_additional = None

    return {
        "c1":                            c1,
        "c3":                            c3,
        "c_images":                      c_img,
        "total":                         total,
        "included_ingestion_gb_day":     included_gb_day,
        "measured_audit_gb_day":         audit_gb,
        "measured_flow_gb_day":          flow_gb,
        "dns_log_gb_day":                None,   # always NOT AVAILABLE
        "measured_runtime_total_gb_day": runtime_total,
        "posture_additional_gb_day":     posture_additional,
        "runtime_additional_gb_day":     runtime_additional,
    }


# ──────────────────────────────────────────────────────────────────────────────
# Console output helpers
# ──────────────────────────────────────────────────────────────────────────────
def print_metering_reference() -> None:
    print(f"\n{SEPARATOR}\nCortex Cloud Workload Metering Reference\n{SEPARATOR}")
    print(f"  {'Resource type':<50} {'Rate'}")
    print(f"  {'─'*50} {'─'*55}")
    for svc, rate in CC_METERING_TABLE:
        print(f"  {svc:<50} {rate}")
    print(SEPARATOR)


def print_resource_table(sub_name: str, sub_id: str, rows: list) -> None:
    header = f"{sub_id}  ({sub_name})"
    print(f"\n  {'Subscription':<74} {'Resource':<44} {'Count':>8}")
    print(f"  {SEPARATOR[:-4]}")
    first = True
    for label, value in rows:
        acct = header if first else ""
        first = False
        print(f"  {acct:<74} {label:<44} {str(value):>8}")
    print(f"  {SEPARATOR[:-4]}")


def _fmt_gb(val) -> str:
    if val is None:
        return "NOT AVAILABLE"
    return f"{val:.4f} GB/day"


def _fmt_additional(val) -> str:
    if val is None:
        return "cannot determine  (measured value not available)"
    if val == 0.0:
        return "✓  0.0000  — within included allowance"
    return f"⚠   {val:.4f} GB/day  — additional purchase needed"


def print_compact_status(sub_name: str, sub_id: str,
                          counts: dict, sku: dict,
                          diagnostics: list,
                          flow_log_sources_total: int) -> None:
    """
    Compact three-to-four-line per-subscription status block.

    Replaces the verbose SKU breakdown + log ingestion tables.  Full detail
    lives in the Excel sheets (Workload Summary, Log Ingestion, Log Source
    Inventory, Diagnostics).  This is what prints during a normal scan;
    --verbose restores the detailed tables for troubleshooting.
    """
    from collections import Counter as _Counter

    total    = sku["total"]
    audit_gb = sku.get("measured_audit_gb_day")
    flow_gb  = sku.get("measured_flow_gb_day")

    def _fmt(v):
        return "NOT MEASURED" if v is None else f"{v:.4f} GB/day"

    # Counts line: show only non-zero categories for compactness
    vm_total   = counts["vm_running"] + counts["aks_nodes"] + counts["aro_nodes"]
    caas_total = counts["aci_containers"] + counts["container_app_containers"]
    db_total   = counts["azure_sql_dbs"] + counts["cosmos_db"]
    parts = []
    if vm_total:                   parts.append(f"VMs/Nodes={vm_total}")
    if caas_total:                 parts.append(f"CaaS={caas_total}")
    if counts['azure_functions']:  parts.append(f"Func={counts['azure_functions']}")
    if db_total:                   parts.append(f"DB={db_total}")
    if counts['storage_accounts']: parts.append(f"Stor={counts['storage_accounts']}")
    if counts.get('acr_tag_count'):parts.append(f"ACR={counts['acr_tag_count']}")
    counts_line = "  ".join(parts) if parts else "(no billable resources)"

    # Flow-source measurement ratio
    flow_configured = flow_log_sources_total or 0
    flow_measured_accts = counts.get("flow_accts_measured", 0)
    flow_total_accts    = counts.get("flow_accts_total", 0)
    flow_ratio = (
        f"{flow_measured_accts}/{flow_total_accts} storage accts measured"
        if flow_total_accts else
        ("no flow logs configured" if flow_configured == 0 else "not measured")
    )

    # Diagnostic count & category rollup
    cats = _Counter(d["issue_category"] for d in diagnostics)
    if cats:
        _pretty = {
            "rbac_missing_sbdr":       "SBDR missing",
            "rbac_missing_law_reader": "LAW reader missing",
            "rbac_missing_eh_reader":  "EH reader missing",
            "rbac_missing_acr_pull":   "AcrPull missing",
            "firewall_blocked":        "firewall blocked",
            "private_endpoint_only":   "private endpoint",
            "ssl_intercept":           "SSL intercept",
            "tenant_mismatch":         "tenant mismatch",
            "container_not_present":   "container absent",
            "not_configured":          "not configured",
        }
        diag_detail = ", ".join(f"{n} {_pretty.get(c, c)}" for c, n in cats.most_common())
        diag_line = f"⚠ {len(diagnostics)} issues  ({diag_detail})"
    else:
        diag_line = "✅ no issues"

    # Render
    print()
    print(f"  Workloads:  {counts_line}  →  Total WL: {total}")
    print(f"  Logs:       Audit={_fmt(audit_gb)}  |  Flow={_fmt(flow_gb)}  →  {flow_ratio}")
    print(f"  Status:     {diag_line}")


def print_sku_breakdown(sub_name: str, counts: dict, sku: dict) -> None:
    """Print full workload SKU + exact log ingestion analysis."""

    def wl(raw: int, rate: int) -> int:
        return math.ceil(raw / rate) if raw > 0 else 0

    vm_total   = counts["vm_running"] + counts["aks_nodes"] + counts["aro_nodes"]
    caas_total = counts["aci_containers"] + counts["container_app_containers"]
    db_total   = counts["azure_sql_dbs"] + counts["cosmos_db"]
    net_images = max(0, counts["acr_images"] - counts["free_image_allowance"])
    total      = sku["total"]
    incl       = sku["included_ingestion_gb_day"]

    # ── Workload breakdown ────────────────────────────────────────────────────
    print(f"\n  Workload SKU breakdown  —  {sub_name}")
    print(f"  {'─'*92}")
    print(f"  {'Category':<52} {'Raw count':>12}   {'Workloads':>10}")
    print(f"  {'─'*92}")
    print(f"  {'VMs + AKS nodes + ARO nodes':<52} {vm_total:>12}   {wl(vm_total, CC_METERING['vm']):>10}")
    print(f"  {'ACI + Container App containers  (CaaS)':<52} {caas_total:>12}   {wl(caas_total, CC_METERING['caas']):>10}")
    print(f"  {'Azure Functions  (Serverless)':<52} {counts['azure_functions']:>12}   {wl(counts['azure_functions'], CC_METERING['serverless']):>10}")
    print(f"  {'Storage Accounts':<52} {counts['storage_accounts']:>12}   {wl(counts['storage_accounts'], CC_METERING['buckets']):>10}")
    print(f"  {'Azure SQL + Cosmos DB':<52} {db_total:>12}   {wl(db_total, CC_METERING['db']):>10}")
    _acr_sku_label = (
        "ACR images  (tags — ⚠ FIREWALL BLOCKED)"
        if counts.get("acr_all_blocked") else
        "ACR images  (tags — ⚠ PARTIAL — some blocked)"
        if counts.get("acr_partially_blocked") else
        "ACR images  (tags — All scan mode)"
    )
    print(f"  {_acr_sku_label:<52} {counts['acr_tag_count']:>12}")
    print(f"  {'  └─ ACR manifests  (unique digests — info only)':<52} {counts.get('acr_manifest_count', 0):>12}")
    print(f"  {'  └─ Free allowance  (VMs×10 + CaaS÷10×10)':<52} {counts['free_image_allowance']:>12}")
    print(f"  {'  └─ Net billable images':<52} {net_images:>12}   {wl(net_images, CC_METERING['images']):>10}")

    # ACR firewall warning — block count being 0 is meaningless if registries were blocked
    if counts.get("acr_all_blocked"):
        print(f"  ⚠  ACR count = 0 because all {counts['acr_registries_found']} registry(ies) blocked "
              f"the client IP on the data-plane.")
        print(f"     This is NOT a genuine zero — actual image count is UNKNOWN.")
        print(f"     Fix: Portal → Container Registry → Networking → Add client IP,")
        print(f"          OR run the tool from a VM inside the registry's VNet / Private Endpoint.")
    elif counts.get("acr_partially_blocked"):
        print(f"  ⚠  {counts['acr_registries_blocked']} of {counts['acr_registries_found']} "
              f"registry(ies) blocked the client IP — tag count is understated.")
        print(f"     Fix: Portal → Container Registry → Networking → Add client IP.")
    print(f"  {'─'*92}")

    parts = []
    if wl(vm_total,                    CC_METERING["vm"]):         parts.append(f"VMs/Nodes={wl(vm_total, CC_METERING['vm'])}")
    if wl(caas_total,                  CC_METERING["caas"]):       parts.append(f"CaaS={wl(caas_total, CC_METERING['caas'])}")
    if wl(counts["azure_functions"],   CC_METERING["serverless"]): parts.append(f"Functions={wl(counts['azure_functions'], CC_METERING['serverless'])}")
    if wl(counts["storage_accounts"],  CC_METERING["buckets"]):    parts.append(f"Storage={wl(counts['storage_accounts'], CC_METERING['buckets'])}")
    if wl(db_total,                    CC_METERING["db"]):         parts.append(f"Databases={wl(db_total, CC_METERING['db'])}")
    if wl(net_images,                  CC_METERING["images"]):     parts.append(f"Images={wl(net_images, CC_METERING['images'])}")
    formula = " + ".join(parts) if parts else "0"
    print(f"  Total Required Workload Licenses:  {formula} = {total}")
    print(f"  Included daily log ingestion:      {total} / 50 = {incl:.4f} GB/day")

    # ── Log ingestion — exact measured values only ────────────────────────────
    audit_gb      = sku["measured_audit_gb_day"]
    flow_gb       = sku["measured_flow_gb_day"]
    runtime_total = sku["measured_runtime_total_gb_day"]

    audit_method = counts.get("audit_log_method", "—")
    flow_method  = counts.get("flow_log_method",  "—")

    # Per-source breakdown for transparency
    eh_audit_gb  = counts.get("eh_audit_gb_day")
    blob_flow_gb = counts.get("blob_flow_gb_day")
    law_audit_gb = counts.get("law_audit_gb_day")
    law_flow_gb  = counts.get("law_flow_gb_day")
    audit_dual   = counts.get("audit_dual_sink", False)
    flow_dual    = counts.get("flow_dual_sink",  False)
    law_ws_found = counts.get("law_workspaces_found", 0)

    print(f"\n  Log Ingestion  (exact measurements — no estimates)")
    print(f"  {'─'*92}")
    print(f"  {'Log Type':<44} {'Measured GB/day':>16}   Notes")
    print(f"  {'─'*92}")

    # Audit — EH is primary; LAW shown as reference when both measured
    if law_ws_found > 0 and (eh_audit_gb is not None or law_audit_gb is not None):
        print(f"  {'Audit Logs  — EH / blob capture  [USED FOR SIZING]':<44} {_fmt_gb(eh_audit_gb):>16}")
        ref_note = "  (reference — same events; not added to EH figure)" if audit_dual else ""
        print(f"  {'Audit Logs  — Log Analytics (LAW)  [reference only]':<44} {_fmt_gb(law_audit_gb):>16}{ref_note}")
    else:
        print(f"  {'Audit Logs':<44} {_fmt_gb(audit_gb):>16}   {audit_method}")

    # SKU note: audit logs count toward BOTH Posture and Runtime SKUs
    # (they are not double-counted — the same log volume applies to both licenses)
    print(f"  {'  └─ Applies to: Posture SKU + Runtime SKU  (same volume, both licenses)':<44}")

    # Flow — blob only (LAW path removed — Traffic Analytics ≠ Cortex ingestion)
    print(f"  {'Flow Logs  [Runtime SKU only]':<44} {_fmt_gb(flow_gb):>16}   {flow_method}")

    print(f"  {'─'*92}")

    if runtime_total is not None:
        print(f"  {'Runtime subtotal  (Audit + Flow)':<44} {_fmt_gb(runtime_total):>16}")
    else:
        print(f"  {'Runtime subtotal  (Audit + Flow)':<44} {'NOT AVAILABLE':>16}   (one or both components not measured)")
    print(f"  {'Included ingestion allowance':<44} {incl:>12.4f} GB/day")
    print(f"  {'─'*92}")

    p_add = sku["posture_additional_gb_day"]
    r_add = sku["runtime_additional_gb_day"]
    print(f"  Posture SKU  —  measures Audit Logs only:")
    print(f"    Included {incl:.4f} GB/day  |  Measured {_fmt_gb(audit_gb)}  →  {_fmt_additional(p_add)}")
    print(f"  Runtime SKU  —  measures Audit + Flow Logs  (superset of Posture):")
    print(f"    Included {incl:.4f} GB/day  |  Measured {_fmt_gb(runtime_total)}  →  {_fmt_additional(r_add)}")
    print(f"  ⓘ  These SKUs are not additive — Runtime includes everything Posture measures")
    print(f"     plus Flow Logs.  A customer buying Runtime does not also need Posture separately.")
    print(SEPARATOR)


# ──────────────────────────────────────────────────────────────────────────────
# Log Analytics Workspace helpers
# ──────────────────────────────────────────────────────────────────────────────

def _get_law_customer_id(workspace_resource_id: str, arm_token: str,
                          verify_ssl: bool) -> str | None:
    """
    Retrieve the Log Analytics workspace GUID (customerId) from ARM.

    The Log Analytics Query API requires the workspace GUID, NOT the ARM
    resource ID.  They are different identifiers.

    Returns the customerId string or None on any failure.
    """
    try:
        url = f"https://management.azure.com{workspace_resource_id}?api-version={_LAW_ARM_API_VERSION}"
        r = _requests.get(
            url,
            headers={"Authorization": f"Bearer {arm_token}"},
            verify=verify_ssl,
            timeout=15,
        )
        if r.status_code != 200:
            return None
        return r.json().get("properties", {}).get("customerId")
    except Exception:
        return None


def _query_law_usage(customer_id: str, law_token: str,
                      verify_ssl: bool) -> dict | None:
    """
    Query the Log Analytics Usage table for the last 7 days.

    Uses the Usage table (always present, always populated) rather than
    _BilledSize on individual tables, which requires table-level access and
    is absent on free-tier workspaces.

    Usage.Quantity is in MEGABYTES.  This function converts to GB/day.

    Returns a dict:
        {
          "audit_gb_day": float | None,   # all Cortex Cloud audit DataTypes combined
          "flow_gb_day":  float | None,   # flow-related DataTypes
          "legacy_flow_gb_day": float | None,  # AzureDiagnostics NSG events
          "data_types":   dict[str, float],    # raw per-DataType MB totals
          "dual_source_note": str | None,
        }
    or None on total failure (HTTP error, no access, workspace not found).

    Two queries are issued:
      Q1 — Usage table:  all known audit + flow DataTypes (7-day window).
      Q2 — AzureDiagnostics:  Category==NetworkSecurityGroupFlowEvent
           (legacy NSG flow logs landing in the catch-all table).
           This query is only issued if Q1 succeeds.
    """
    headers = {
        "Authorization": f"Bearer {law_token}",
        "Content-Type":  "application/json",
    }
    base_url = f"{_LAW_QUERY_ENDPOINT}/{_LAW_QUERY_API_VER}/workspaces/{customer_id}/query"

    # ── Q1: Usage table — all targeted DataTypes ──────────────────────────────
    all_types = (
        list(_LAW_AUDIT_DATA_TYPES) + list(_LAW_FLOW_DATA_TYPES)
        + [_LAW_LEGACY_DIAG_TABLE]
    )
    types_kql = ", ".join(f'"{t}"' for t in all_types)
    q1 = (
        "Usage"
        "| where TimeGenerated > ago(7d)"
        f"| where DataType in ({types_kql})"
        "| summarize TotalMB = sum(Quantity) by DataType"
    )
    try:
        resp = _requests.post(
            base_url,
            headers=headers,
            json={"query": q1},
            verify=verify_ssl,
            timeout=60,
        )
        if resp.status_code in (401, 403):
            return None    # No LAW Reader — caller handles NOT AVAILABLE
        if resp.status_code != 200:
            return None
        rows_q1 = resp.json().get("tables", [{}])[0].get("rows", [])
    except Exception:
        return None

    # Parse Q1: {DataType → total MB over 7 days}
    dt_mb: dict[str, float] = {}
    for row in rows_q1:
        # columns: DataType (str), TotalMB (float)
        if len(row) >= 2:
            dt_mb[row[0]] = float(row[1] or 0)

    # ── Q2: AzureDiagnostics legacy NSG flow events ───────────────────────────
    legacy_mb = 0.0
    if _LAW_LEGACY_DIAG_TABLE in dt_mb:
        # The Usage table gives us total AzureDiagnostics bytes but can't
        # split by Category.  Run a targeted query on the actual table.
        # Use _BilledSize which is available in all non-free workspaces;
        # fall back gracefully if the column is absent (free tier).
        q2 = (
            "AzureDiagnostics"
            "| where TimeGenerated > ago(7d)"
            f'| where Category == "{_LAW_LEGACY_FLOW_CATEGORY}"'
            "| summarize TotalMB = sum(_BilledSize) / (1024.0 * 1024.0)"
        )
        try:
            resp2 = _requests.post(
                base_url,
                headers=headers,
                json={"query": q2},
                verify=verify_ssl,
                timeout=60,
            )
            if resp2.status_code == 200:
                rows_q2 = resp2.json().get("tables", [{}])[0].get("rows", [])
                if rows_q2 and rows_q2[0]:
                    legacy_mb = float(rows_q2[0][0] or 0)
        except Exception:
            pass   # legacy query failure is non-fatal

    # ── Aggregate into audit / flow buckets ───────────────────────────────────
    def mb7_to_gb_day(mb_total: float) -> float:
        """Convert 7-day cumulative MB to GB/day."""
        return round(mb_total / 1024.0 / 7.0, 4)

    audit_mb = sum(dt_mb.get(t, 0.0) for t in _LAW_AUDIT_DATA_TYPES)
    flow_mb  = sum(dt_mb.get(t, 0.0) for t in _LAW_FLOW_DATA_TYPES)

    audit_gb_day = mb7_to_gb_day(audit_mb) if audit_mb > 0 else None
    flow_gb_day  = mb7_to_gb_day(flow_mb)  if flow_mb  > 0 else None
    legacy_flow_gb_day = mb7_to_gb_day(legacy_mb) if legacy_mb > 0 else None

    return {
        "audit_gb_day":       audit_gb_day,
        "flow_gb_day":        flow_gb_day,
        "legacy_flow_gb_day": legacy_flow_gb_day,
        "data_types":         {k: round(v, 2) for k, v in dt_mb.items()},
    }


# ──────────────────────────────────────────────────────────────────────────────
# Blob warning categorisation helper
# ──────────────────────────────────────────────────────────────────────────────
def _categorise_blob_warn(warn_fn, acct_name: str, container_name: str,
                           msg: str, impact: str, sub_id: str,
                           emit_fn=None, sub_name: str = "",
                           storage_id: str = "", sub_roles: dict | None = None,
                           oid: str = "", client_ip: str = "") -> None:
    """
    Emit a categorised warning for blob access failures.

    Console behavior is unchanged from the original: a single line per failure
    via warn_fn (preserves backwards-compat with existing console layout).

    Additionally, when emit_fn is supplied, this function builds a structured
    DiagnosticRecord and invokes emit_fn(record).  When sub_roles is provided,
    the AuthorizationFailure branch is disambiguated:
      • SBDR not granted → category 'rbac_missing_sbdr', fix names exact scope
      • SBDR granted     → category 'firewall_blocked', fix names client IP
    Without sub_roles we fall back to the legacy ambiguous message.
    """
    msg_lower = msg.lower()
    label = f"{acct_name}/{container_name}"

    is_dns_fail  = "nodename nor servname" in msg_lower or "failed to resolve" in msg_lower
    is_auth_fail = "authorizationfailure" in msg_lower or "not authorized" in msg_lower

    # ── Console line (backwards-compatible) ──
    if is_dns_fail:
        warn_fn(label, Exception(
            f"Private endpoint / DNS failure — storage account has no public DNS. "
            f"Run from inside the customer's VNet to reach this account.  ({impact})"
        ))
    elif is_auth_fail:
        warn_fn(label, Exception(
            f"Storage access blocked (AuthorizationFailure) — either Storage Blob Data "
            f"Reader role is missing OR the storage account network firewall is blocking "
            f"this client IP.  Check IAM first; if role is assigned, whitelist this IP "
            f"in the storage account's Networking settings.  ({impact})"
        ))
    else:
        warn_fn(label, Exception(f"{msg[:200]}  ({impact})"))

    # ── Structured DiagnosticRecord ──
    if emit_fn is None:
        return

    if is_dns_fail:
        category = "private_endpoint_only"
        fix      = build_fix_string(category, oid, storage_id)
    elif is_auth_fail:
        if sub_roles is not None and storage_id and \
           has_role_for("Storage Blob Data Reader", storage_id, sub_roles):
            category = "firewall_blocked"
            fix      = build_fix_string(category, oid, storage_id,
                                        extra={"client_ip": client_ip})
        else:
            category = "rbac_missing_sbdr"
            fix      = build_fix_string(category, oid, storage_id)
    else:
        # Unclassified — record raw error but no automated fix
        category = "not_configured"
        fix      = ""

    try:
        emit_fn(make_diagnostic(
            sub_id=sub_id, sub_name=sub_name,
            resource_type="storage_account",
            resource_id=storage_id,
            resource_name=acct_name,
            category=category,
            impact=impact,
            sub_path=container_name,
            fix=fix,
            raw_error=msg,
        ))
    except ValueError:
        # Unknown impact code — drop silently rather than crash the scan
        pass


# ──────────────────────────────────────────────────────────────────────────────
# Per-subscription scan
# ──────────────────────────────────────────────────────────────────────────────
def scan_subscription(
    cred,
    sub_id:          str,
    sub_name:        str,
    transport,
    heartbeat_sec:   int  = 10,
    sub_timeout_min: int  = 20,
    verify_ssl:      bool = True,
    verbose:         bool = False,
) -> tuple:
    """
    Scan one subscription.  Returns (counts: dict, sku: dict, diagnostics: list).

    Non-auth errors inside each section are caught, surfaced to the console as
    warnings, AND appended as structured DiagnosticRecord dicts to the
    diagnostics list.  Auth errors are re-raised immediately.

    The diagnostics list is written into the per-subscription payload of
    azure_results.json under the key 'diagnostics'.  az-summary.py renders it
    as the Diagnostics sheet (one row per issue, sortable/filterable).
    """
    from azure.mgmt.compute import ComputeManagementClient
    from azure.mgmt.containerservice import ContainerServiceClient
    from azure.mgmt.web import WebSiteManagementClient
    from azure.mgmt.sql import SqlManagementClient
    from azure.mgmt.cosmosdb import CosmosDBManagementClient
    from azure.mgmt.storage import StorageManagementClient
    from azure.mgmt.containerinstance import ContainerInstanceManagementClient
    from azure.mgmt.appcontainers import ContainerAppsAPIClient
    from azure.mgmt.redhatopenshift import AzureRedHatOpenShiftClient
    from azure.mgmt.containerregistry import ContainerRegistryManagementClient
    from azure.mgmt.monitor import MonitorManagementClient
    from azure.mgmt.eventhub import EventHubManagementClient
    from azure.mgmt.resourcegraph import ResourceGraphClient
    from azure.mgmt.resourcegraph.models import QueryRequest as RGQueryRequest
    from azure.storage.blob import BlobServiceClient as BlobSvcClient
    from azure.containerregistry import ContainerRegistryClient
    from azure.mgmt.core.tools import parse_resource_id
    from azure.core.pipeline.transport import RequestsTransport

    kw = {"transport": transport}

    # Dedicated transport for blob and ACR data-plane calls:
    #   - Short read_timeout (15 s) so unreachable/DNS-failing storage accounts
    #     fail fast instead of blocking for 120 s each.
    #   - Inherits verify_ssl from the caller so --no-verify-ssl is respected
    #     for ContainerRegistryClient token exchange (which ignores the main
    #     transport's SSL setting).
    #   - max_retries=0 on this transport; retry logic is handled per-call.
    blob_transport = RequestsTransport(
        connection_timeout=5,
        read_timeout=15,
        connection_verify=verify_ssl,
    )

    compute_client  = ComputeManagementClient(cred, sub_id, **kw)
    aks_client      = ContainerServiceClient(cred, sub_id, **kw)
    web_client      = WebSiteManagementClient(cred, sub_id, **kw)
    sql_client      = SqlManagementClient(cred, sub_id, **kw)
    cosmos_client   = CosmosDBManagementClient(cred, sub_id, **kw)
    storage_client  = StorageManagementClient(cred, sub_id, **kw)
    aci_client      = ContainerInstanceManagementClient(cred, sub_id, **kw)
    cap_client      = ContainerAppsAPIClient(cred, sub_id, **kw)
    aro_client      = AzureRedHatOpenShiftClient(cred, sub_id, **kw)
    acr_mgmt_client = ContainerRegistryManagementClient(cred, sub_id, **kw)
    monitor_client  = MonitorManagementClient(cred, sub_id, **kw)
    eh_client       = EventHubManagementClient(cred, sub_id, **kw)
    # Resource Graph client is subscription-agnostic; subscriptions are passed
    # per query.  Standard ARM Reader covers Resource Graph queries — no extra role.
    rg_client       = ResourceGraphClient(cred, **kw)

    sub_start = time.time()
    last_hb   = [0.0]

    def hb(phase: str, extra: str = "") -> None:
        elapsed = int(time.time() - sub_start)
        last_hb[0] = heartbeat(
            f"    ...phase={phase}{' ' + extra if extra else ''}  elapsed={elapsed}s",
            last_hb[0], heartbeat_sec,
        )
        if time.time() - sub_start > sub_timeout_min * 60:
            raise TimeoutError(
                f"Per-subscription budget of {sub_timeout_min} min exceeded (phase={phase})"
            )

    def warn(service: str, exc: Exception) -> None:
        # Warnings are always captured as structured DiagnosticRecords further
        # down the call chain.  Console output is suppressed by default so the
        # scan is scannable on a single screen; --verbose restores the legacy
        # per-warning console lines for debugging.
        if verbose:
            print(f"    [warn] {service}: {type(exc).__name__}: {exc}", flush=True)

    def guard(exc: Exception) -> None:
        if is_auth_error(exc):
            raise exc

    # ── Diagnostic schema collection ─────────────────────────────────────────
    # Every issue encountered during this scan is appended as a structured
    # DiagnosticRecord.  Console output is unchanged; the records are returned
    # alongside counts/sku and persisted to azure_results.json for the Excel
    # Diagnostics sheet to consume.
    _diagnostics: list = []

    def emit_diagnostic(record: dict) -> None:
        _diagnostics.append(record)

    # Resolve OID + role assignments once per subscription.  Both are best-effort:
    # if either fails (e.g. Conditional Access, custom roles only), the scan
    # continues with the legacy ambiguous diagnostic ("RBAC OR firewall").
    _oid       = _decode_oid_from_token(cred, verify_ssl=verify_ssl) or ""
    _sub_roles = fetch_sub_roles(sub_id, _oid, cred, verify_ssl=verify_ssl)
    _client_ip = ""
    # Fallback chain for detecting this host's public IP for firewall fix strings.
    # 1. CLIENT_IP env var (manual override — required when ipify is firewalled)
    # 2. ipify HTTPS with normal cert verification
    # 3. ipify HTTPS with cert verification disabled (corporate TLS interception)
    # 4. ipify HTTP (proxy / interception ate the cert entirely)
    _ip_override = os.environ.get("CLIENT_IP", "").strip()
    if _ip_override:
        _client_ip = _ip_override
    else:
        import warnings as _warnings
        for _ip_url, _ip_verify in [
            ("https://api.ipify.org", verify_ssl),
            ("https://api.ipify.org", False),
            ("http://api.ipify.org",  True),
        ]:
            try:
                with _warnings.catch_warnings():
                    _warnings.simplefilter("ignore")
                    _resp = _requests.get(_ip_url, timeout=4, verify=_ip_verify)
                _txt  = (_resp.text or "").strip()
                if _txt.count(".") == 3 and all(p.isdigit() for p in _txt.split(".")):
                    _client_ip = _txt
                    break
            except Exception:
                continue

    def _arm_get(url: str):
        """Simple ARM GET helper that always uses a fresh bearer token."""
        token = cred.get_token("https://management.azure.com/.default")
        return _requests.get(
            url,
            headers={"Authorization": f"Bearer {token.token}"},
            verify=verify_ssl,
            timeout=15,
        )

    def _arm_list(url: str, label: str, not_found_ok: bool = True) -> list:
        """
        Follow ARM nextLink pagination until exhausted.

        Many Azure list APIs paginate on larger subscriptions. Keeping this as a
        tiny helper avoids repeating pagination logic in every scan section.
        """
        items = []
        next_url = url
        seen_urls = set()

        while next_url:
            if next_url in seen_urls:
                warn(label, Exception("nextLink loop detected — stopping pagination"))
                break
            seen_urls.add(next_url)

            resp = _arm_get(next_url)
            if resp.status_code == 404 and not_found_ok:
                break
            if resp.status_code in (401, 403):
                # Raise so the caller's guard() can detect auth failures and
                # trigger the auth-error bail-out path — a plain warn+break here
                # would silently swallow token expiry / RBAC revocation.
                raise PermissionError(
                    f"HTTP {resp.status_code} on {label}: {resp.text[:200]}"
                )
            if resp.status_code != 200:
                warn(label, Exception(f"HTTP {resp.status_code}: {resp.text[:200]}"))
                break

            body = resp.json() or {}
            items.extend(body.get("value", []))
            next_url = body.get("nextLink")

        return items

    def _rg_count(kql: str, label: str) -> int | None:
        """
        Run a Resource Graph KQL query scoped to this subscription and return
        the integer from a '| summarize count()' terminal clause.

        Returns the count as int, or None on any failure.
        None signals the caller to fall back to the ARM SDK path.

        Resource Graph is eventually consistent (typically < 60 s lag).
        For sizing purposes this is acceptable.  The ARM fallback covers
        environments where Resource Graph is blocked by deny assignments
        or where very recently provisioned resources are not yet indexed.

        Required permission: standard ARM Reader — no additional role needed.
        """
        try:
            req    = RGQueryRequest(subscriptions=[sub_id], query=kql)
            result = rg_client.resources(req)
            rows   = result.data or []
            if rows:
                row = rows[0]
                # 'summarize count()' produces a column named 'count_'
                for key in ("count_", "Count_", "count", "Count"):
                    if key in row:
                        return int(row[key])
            return 0
        except Exception as exc:
            guard(exc)
            warn(f"Resource Graph ({label})", exc)
            return None   # caller falls back to ARM SDK

    # ══════════════════════════════════════════════════════════════════════════
    # 1. Running VMs
    #
    # Primary:  Azure Resource Graph
    #   properties.extended.instanceView.powerState.code is available in
    #   Resource Graph as of 2023, replacing the O(N) per-VM instance_view()
    #   call pattern.  One query returns the running count instantly regardless
    #   of subscription size.
    #
    # Fallback: ARM instance_view() per VM
    #   Used when Resource Graph is unavailable (deny assignments, throttling).
    #   Slower on large subscriptions — each VM requires one extra ARM call.
    # ══════════════════════════════════════════════════════════════════════════
    vm_running = _rg_count(
        "Resources"
        " | where type == 'microsoft.compute/virtualmachines'"
        f" | where subscriptionId == '{sub_id}'"
        " | where properties.extended.instanceView.powerState.code"
        "       =~ 'PowerState/running'"
        " | summarize count()",
        "VMs running",
    )
    if vm_running is None:
        # ARM fallback: one instance_view() call per VM
        vm_running = 0
        vm_seen    = 0
        try:
            for vm in compute_client.virtual_machines.list_all():
                vm_seen += 1
                hb("VMs", f"seen={vm_seen} running={vm_running} [ARM fallback]")
                try:
                    rid = parse_resource_id(vm.id)
                    iv  = compute_client.virtual_machines.instance_view(
                        rid["resource_group"], vm.name
                    )
                    if any(
                        (s.code or "").lower() == "powerstate/running"
                        for s in (iv.statuses or [])
                    ):
                        vm_running += 1
                except Exception:
                    pass
        except Exception as exc:
            guard(exc); warn("VMs (ARM fallback)", exc)
    else:
        hb("VMs", f"running={vm_running} [Resource Graph]")

    # ══════════════════════════════════════════════════════════════════════════
    # 2. AKS agent-pool nodes
    # ══════════════════════════════════════════════════════════════════════════
    aks_nodes     = 0
    clusters_seen = 0
    try:
        for cluster in aks_client.managed_clusters.list():
            clusters_seen += 1
            hb("AKS", f"clusters={clusters_seen} nodes={aks_nodes}")
            rid = parse_resource_id(cluster.id)
            try:
                for ap in aks_client.agent_pools.list(rid["resource_group"], cluster.name):
                    aks_nodes += ap.count or 0
            except Exception as exc2:
                guard(exc2); warn(f"AKS agent-pools ({cluster.name})", exc2)
    except Exception as exc:
        guard(exc); warn("AKS", exc)

    # ══════════════════════════════════════════════════════════════════════════
    # 3. ARO nodes
    #
    #  Masters: ARO always provisions exactly 3 master nodes — this is a fixed
    #  architectural requirement documented by Microsoft, not configurable by
    #  the user. The Azure Red Hat OpenShift API does not expose a separate
    #  master node count field; 3 is the only valid value for all ARO clusters.
    #  Ref: https://learn.microsoft.com/azure/openshift/openshift-faq
    #
    #  Workers: read exactly from each workerProfile.count via the API.
    #  Multiple worker profiles are summed (heterogeneous node pools).
    # ══════════════════════════════════════════════════════════════════════════
    ARO_MASTERS_PER_CLUSTER = 3  # Fixed by ARO architecture — not an assumption
    aro_nodes         = 0
    aro_clusters_seen = 0
    try:
        for cluster in aro_client.open_shift_clusters.list():
            hb("ARO")
            aro_clusters_seen += 1
            aro_nodes += ARO_MASTERS_PER_CLUSTER
            for wp in (cluster.worker_profiles or []):
                aro_nodes += wp.count or 0
    except Exception as exc:
        guard(exc); warn("ARO", exc)

    # ══════════════════════════════════════════════════════════════════════════
    # 4. ACI – containers inside running Container Groups
    # ══════════════════════════════════════════════════════════════════════════
    aci_containers = 0
    try:
        for cg in aci_client.container_groups.list():
            hb("ACI")
            if (cg.provisioning_state or "").lower() == "succeeded":
                aci_containers += len(cg.containers or [])
    except Exception as exc:
        guard(exc); warn("ACI", exc)

    # ══════════════════════════════════════════════════════════════════════════
    # 5. Azure Container Apps  (containers defined in app template)
    #
    # Billing unit: containers (10 containers = 1 CaaS workload).
    #
    # app.template.containers is returned on every app object from
    # list_by_subscription() — no second API call needed.  Attempting to count
    # replicas × containers via the revisions API adds complexity, requires a
    # separate SDK call that has historically had inconsistent method names
    # across SDK versions, and is not necessary for licensing sizing — Cortex
    # Cloud bills on container definitions, not running replicas.
    #
    # Floor: 1 container per app when template is absent (e.g. system-managed
    # apps) so that every discovered app contributes to the count.
    # ══════════════════════════════════════════════════════════════════════════
    container_app_containers = 0
    try:
        for app in cap_client.container_apps.list_by_subscription():
            hb("ContainerApps")
            container_app_containers += len(
                (app.template.containers if app.template else None) or []
            ) or 1          # floor=1 when template missing
    except Exception as exc:
        guard(exc); warn("ContainerApps", exc)

    # ══════════════════════════════════════════════════════════════════════════
    # 6. Azure Functions  (running function apps)
    #
    # Billing unit: function apps (25 function apps = 1 serverless workload).
    #
    # The previous approach tried to enumerate individual functions inside each
    # app via GET /sites/{name}/functions.  That endpoint is unreliable:
    #   • Returns 404 or empty list for containerized / Flex Consumption apps
    #   • Times out frequently on corporate networks with strict egress controls
    #   • Is unnecessary — Cortex Cloud sizing uses app count, not trigger count
    #
    # Simple rule: count running function apps directly from web_apps.list().
    # One pass, no sub-resource calls, nothing to time out.
    #
    # Exclusions:
    #   • Logic Apps Standard (kind contains "workflowapp") — different product,
    #     not metered as Azure Functions under Cortex Cloud
    #   • Non-functionapp kinds (App Service plans, web apps, etc.)
    #   • Stopped / disabled apps (state != "Running") — consistent with
    #     running-only policy applied to VMs
    # ══════════════════════════════════════════════════════════════════════════
    azure_functions      = 0   # running function apps
    function_apps_stopped    = 0   # excluded: state != Running
    function_apps_logic_apps = 0   # excluded: Logic Apps Standard (workflowapp)

    try:
        for app in web_client.web_apps.list():
            kind_lower = (app.kind or "").lower()

            # Exclude Logic Apps Standard — not metered as serverless
            if "workflowapp" in kind_lower:
                function_apps_logic_apps += 1
                continue

            # Only process Function App kinds
            if not kind_lower.startswith("functionapp"):
                continue

            # Exclude stopped / disabled apps
            if (app.state or "").lower() != "running":
                function_apps_stopped += 1
                continue

            # Running function app — count it
            azure_functions += 1
            hb("Functions", f"apps={azure_functions}")

    except Exception as exc:
        guard(exc); warn("Functions", exc)

    # ══════════════════════════════════════════════════════════════════════════
    # 7. Azure SQL databases  (master excluded)
    #
    # Primary:  Resource Graph  (microsoft.sql/servers/databases is indexed)
    #   Single query across all servers in the subscription; master excluded
    #   via name filter.  Eliminates the nested server → database SDK loop.
    #
    # Fallback: ARM SDK — iterate servers then databases per server.
    # ══════════════════════════════════════════════════════════════════════════
    azure_sql_dbs = _rg_count(
        "Resources"
        " | where type == 'microsoft.sql/servers/databases'"
        f" | where subscriptionId == '{sub_id}'"
        " | where name != 'master'"
        " | summarize count()",
        "Azure SQL databases",
    )
    if azure_sql_dbs is None:
        azure_sql_dbs = 0
        try:
            for srv in sql_client.servers.list():
                hb("SQL", f"dbs={azure_sql_dbs} [ARM fallback]")
                rid = parse_resource_id(srv.id)
                try:
                    for db in sql_client.databases.list_by_server(
                        rid["resource_group"], srv.name
                    ):
                        if (db.name or "").lower() != "master":
                            azure_sql_dbs += 1
                except Exception as exc2:
                    guard(exc2); warn(f"SQL DBs ({srv.name})", exc2)
        except Exception as exc:
            guard(exc); warn("SQL (ARM fallback)", exc)
    else:
        hb("SQL", f"dbs={azure_sql_dbs} [Resource Graph]")

    # ══════════════════════════════════════════════════════════════════════════
    # 8. Cosmos DB  (public network access = Enabled)
    #
    # Primary:  Resource Graph  — publicNetworkAccess filter applied in KQL,
    #   matching the ARM SDK filter.  Private-only accounts excluded because
    #   Cortex Cloud only meters publicly accessible Cosmos DB accounts.
    #
    # Fallback: ARM SDK — iterate accounts and check publicNetworkAccess field.
    # ══════════════════════════════════════════════════════════════════════════
    cosmos_db = _rg_count(
        "Resources"
        " | where type == 'microsoft.documentdb/databaseaccounts'"
        f" | where subscriptionId == '{sub_id}'"
        " | where properties.publicNetworkAccess =~ 'Enabled'"
        " | summarize count()",
        "Cosmos DB",
    )
    if cosmos_db is None:
        cosmos_db = 0
        try:
            for acc in cosmos_client.database_accounts.list():
                hb("CosmosDB [ARM fallback]")
                if str(getattr(acc, "public_network_access", "") or "").lower() == "enabled":
                    cosmos_db += 1
        except Exception as exc:
            guard(exc); warn("CosmosDB (ARM fallback)", exc)
    else:
        hb("CosmosDB", f"accounts={cosmos_db} [Resource Graph]")

    # ══════════════════════════════════════════════════════════════════════════
    # 9. Storage Accounts
    #
    # Primary:  Resource Graph  — existence = billable unit; no state filter
    #   or sub-resource detail needed, making this the ideal Resource Graph use.
    #
    # Fallback: ARM SDK — iterate storage accounts via StorageManagementClient.
    # ══════════════════════════════════════════════════════════════════════════
    storage_accounts = _rg_count(
        "Resources"
        " | where type == 'microsoft.storage/storageaccounts'"
        f" | where subscriptionId == '{sub_id}'"
        " | summarize count()",
        "Storage Accounts",
    )
    if storage_accounts is None:
        storage_accounts = 0
        try:
            for _ in storage_client.storage_accounts.list():
                hb("Storage [ARM fallback]")
                storage_accounts += 1
        except Exception as exc:
            guard(exc); warn("Storage (ARM fallback)", exc)
    else:
        hb("Storage", f"accounts={storage_accounts} [Resource Graph]")

    # ══════════════════════════════════════════════════════════════════════════
    # 10. ACR container image tags + manifests
    #
    #  Official Cortex Cloud sizing methodology (per briefing doc):
    #    "Total Images: 100 ECR + 50 ACR + 1000 JFrog = 1,150 Images"
    #  This count is tag-based, matching what customers see in their registry UI
    #  and what Cortex Cloud enumerates in "All" scan mode.
    #
    #  Assumption: sizing assumes "All" scan mode (all tags scanned).
    #  Caveat to customer: if they use "Latest tag" or "Days Modified" mode,
    #  the billable image count will be lower.
    #
    #  Manifest count is also collected as an informational field — it shows
    #  how many unique image binaries exist (deduplication opportunity).
    #  In normal CI/CD environments tags ≈ manifests.
    # ══════════════════════════════════════════════════════════════════════════
    acr_tag_count      = 0   # total tags across all repos — PRIMARY billable unit
    acr_manifest_count = 0   # unique manifests (digests) — informational
    acr_registries_found   = 0   # total registries discovered via management plane
    acr_registries_blocked = 0   # blocked by ACR network firewall (data-plane DENIED)
    acr_registries_scanned = 0   # successfully accessed and enumerated

    def _is_acr_firewall(exc) -> bool:
        """True when ACR data-plane rejects the client IP (firewall rule, not a permission issue)."""
        msg = str(exc).lower()
        return (
            "not allowed access" in msg          # explicit firewall message from ACR
            or ("denied" in msg and "firewall" in msg)
            or ("denied" in msg and "acr" in msg)
            or ("denied" in msg and "azurecr" in msg)
        )

    try:
        for registry in acr_mgmt_client.registries.list():
            acr_registries_found += 1
            hb("ACR", f"tags={acr_tag_count} manifests={acr_manifest_count}")
            endpoint = f"https://{registry.login_server}"
            try:
                acr_data = ContainerRegistryClient(endpoint, cred, transport=blob_transport)
                for repo_name in acr_data.list_repository_names():
                    try:
                        for manifest in acr_data.list_manifest_properties(repo_name):
                            acr_manifest_count += 1
                            acr_tag_count += len(getattr(manifest, "tags", None) or [])
                    except Exception as exc3:
                        guard(exc3); warn(f"ACR manifests ({registry.name}/{repo_name})", exc3)
                acr_registries_scanned += 1
            except Exception as exc2:
                guard(exc2)
                if _is_acr_firewall(exc2):
                    acr_registries_blocked += 1
                warn(f"ACR data-plane ({registry.name})", exc2)
    except Exception as exc:
        guard(exc); warn("ACR management", exc)

    # acr_images = tag count — used for free allowance and workload calculation
    acr_images = acr_tag_count

    # True when every registry was firewall-blocked — count=0 is NOT a genuine zero
    acr_all_blocked = (
        acr_registries_found > 0
        and acr_registries_blocked == acr_registries_found
        and acr_registries_scanned == 0
    )
    # True when at least one registry was blocked (partial data — counts are understated)
    acr_partially_blocked = (
        acr_registries_blocked > 0 and not acr_all_blocked
    )

    caas_total_for_allowance = aci_containers + container_app_containers
    free_image_allowance = (
        (vm_running + aks_nodes + aro_nodes) * 10
        + (caas_total_for_allowance // 10) * 10
    )

    # ══════════════════════════════════════════════════════════════════════════
    # 11. Flow log sources — discover via REST API
    #
    #  Uses REST directly to list all Network Watchers across the subscription,
    #  including those in NetworkWatcherRG (Azure-managed). SDK network_watchers
    #  .list_all() misses watchers in RGs where the identity lacks Reader — REST
    #  with subscription-scope Reader sees everything.
    #
    #  GET /subscriptions/{id}/providers/Microsoft.Network/networkWatchers
    #  → for each watcher, GET /networkWatchers/{name}/flowLogs
    #  → enabled flow logs → storage_id for blob measurement
    #
    #  LAW workspace IDs collected here (Traffic Analytics + VNet flow log diag)
    #  are consumed in section 15.  Both sets must be initialized before this
    #  section executes — _law_audit_workspace_ids is also populated in section
    #  13 Path A3, so both are declared together here for clarity.
    # ══════════════════════════════════════════════════════════════════════════
    # LAW workspace IDs discovered as audit/flow log sinks (deduplicated sets).
    # Initialized here — before section 11 uses _law_flow_workspace_ids and
    # before section 13 Path A3 uses _law_audit_workspace_ids.
    _law_audit_workspace_ids: set = set()
    _law_flow_workspace_ids:  set = set()

    flow_log_sources  = 0
    # Maps storage_account_id → list of flow log "specs" (one per enabled flow log).
    # Each spec carries everything needed to build the per-resource blob prefix:
    # the flow log's own sub/RG/name (used for VNet container) and the target
    # resource ID (used for NSG container).  Container_name is also recorded
    # so the measurement function can scope each call.
    _flow_log_specs: dict = {}   # storage_id → list[spec]

    try:
        hb("FlowLogs-discover")
        watchers = _arm_list(
            f"https://management.azure.com/subscriptions/{sub_id}"
            f"/providers/Microsoft.Network/networkWatchers"
            f"?api-version=2023-05-01",
            "FlowLogs Network Watcher list",
        )
        for watcher in watchers:
            w_id     = watcher["id"]
            w_parsed = parse_resource_id(w_id)
            w_name   = w_parsed["resource_name"]
            hb("FlowLogs-scan", w_name)
            try:
                flow_logs = _arm_list(
                    f"https://management.azure.com{w_id}/flowLogs"
                    f"?api-version=2023-05-01",
                    f"FlowLogs list ({w_name})",
                )
                for fl in flow_logs:
                    p = fl.get("properties", {})
                    if not p.get("enabled"):
                        continue

                    flow_log_sources += 1
                    sid       = p.get("storageId")
                    target_id = p.get("targetResourceId", "")
                    if not sid:
                        continue

                    # Parse the flow log's own ARM ID — needed for VNet prefix.
                    # Format:
                    #  /subscriptions/{S}/resourceGroups/{RG}/providers/
                    #  Microsoft.Network/networkWatchers/{WATCHER}/flowLogs/{NAME}
                    fl_id = fl.get("id", "")
                    fl_sub_id = sub_id
                    fl_rg     = ""
                    fl_watcher = ""
                    fl_name    = ""
                    fl_parts = fl_id.split("/")
                    try:
                        sub_idx = next(i for i, p in enumerate(fl_parts)
                                       if p.lower() == "subscriptions")
                        fl_sub_id = fl_parts[sub_idx + 1]
                        rg_idx  = next(i for i, p in enumerate(fl_parts)
                                       if p.lower() == "resourcegroups")
                        fl_rg   = fl_parts[rg_idx + 1]
                        nw_idx  = next(i for i, p in enumerate(fl_parts)
                                       if p.lower() == "networkwatchers")
                        fl_watcher = fl_parts[nw_idx + 1]
                        flg_idx = next(i for i, p in enumerate(fl_parts)
                                       if p.lower() == "flowlogs")
                        fl_name = fl_parts[flg_idx + 1]
                    except (StopIteration, IndexError):
                        pass   # spec will be incomplete; prefix builder will skip it

                    # Container is determined by the *target* resource type:
                    # NSG → networksecuritygroupflowevent (legacy)
                    # VNet/Subnet → flowlogflowevent (current)
                    target_lower = target_id.lower()
                    if "/networksecuritygroups/" in target_lower:
                        container_name = _FLOW_LOG_CONTAINER_NSG
                    else:
                        container_name = _FLOW_LOG_CONTAINER_VNET

                    spec = {
                        "flow_log_id":           fl_id,
                        "flow_log_sub_id":       fl_sub_id,
                        "flow_log_rg":           fl_rg,
                        "flow_log_watcher_name": fl_watcher,
                        "flow_log_name":         fl_name,
                        "target_id":             target_id,
                        "container_name":        container_name,
                    }
                    _flow_log_specs.setdefault(sid, []).append(spec)

                    # Traffic Analytics LAW workspace (aggregated — not Cortex path)
                    fa = p.get("flowAnalyticsConfiguration", {})
                    fa_cfg = fa.get("networkWatcherFlowAnalyticsConfiguration", {})
                    if fa_cfg.get("enabled") and fa_cfg.get("workspaceResourceId"):
                        _law_flow_workspace_ids.add(fa_cfg["workspaceResourceId"])

            except Exception as exc2:
                guard(exc2); warn(f"FlowLogs list ({w_name})", exc2)
    except Exception as exc:
        guard(exc); warn("FlowLogs discovery", exc)

    # ══════════════════════════════════════════════════════════════════════════
    # 12. AUDIT LOG SIZING — two independent parallel paths
    #
    #  Path A — Subscription Diagnostic Settings (Activity Log routing):
    #    GET /subscriptions/{id}/providers/microsoft.insights/diagnosticSettings
    #    → finds Event Hub namespaces + specific hub that receives Activity Logs
    #    → for each such hub, reads capture destination via REST and measures blobs
    #    → also handles direct-to-storage routing (well-known container names)
    #
    #  Path B — Direct Event Hub Capture scan (independent of diagnostic settings):
    #    GET /subscriptions/{id}/providers/Microsoft.EventHub/namespaces
    #    → for each namespace, GET /namespaces/{ns}/eventhubs
    #    → for each hub with captureDescription.enabled == true, read exact
    #      storageAccountResourceId + blobContainer from REST response
    #    → measure blobs on that exact container
    #
    #  Both paths run independently and deduplicate by storageId::container key.
    #  This handles:
    #    - Standard Cortex Cloud setup (Activity Log → EH via diagnostic setting)
    #    - Direct EH capture (events pushed to EH, captured to storage)
    #    - Cases where diagnostic settings are absent but capture is active
    #
    #  REST API used for EH hub details — SDK (azure-mgmt-eventhub) frequently
    #  drops captureDescription.destination fields during deserialization.
    #  The raw REST response always returns the complete object.
    #
    #  Required: Storage Blob Data Reader on the capture storage account.
    # ══════════════════════════════════════════════════════════════════════════
    audit_log_gb_day      = _NOT_AVAILABLE
    audit_log_method      = "NOT AVAILABLE — no Event Hub namespaces or subscription diagnostic settings found"
    eh_namespaces_found   = 0
    eh_with_capture_found = 0
    audit_bytes_total       = 0.0
    audit_accts_measured    = 0
    audit_accts_capped      = 0
    seen_capture_storage: set  = set()
    _eh_capture_inventory: list = []   # one entry per successfully-measured EH capture path

    # ── helper: fetch EH capture destination via REST (SDK drops fields) ──────
    def _get_eh_capture_dest(sub_id, rg, ns, hub_name):
        """Returns (storage_account_resource_id, blob_container) or (None, None)."""
        try:
            token = cred.get_token("https://management.azure.com/.default")
            url   = (
                f"https://management.azure.com/subscriptions/{sub_id}"
                f"/resourceGroups/{rg}/providers/Microsoft.EventHub"
                f"/namespaces/{ns}/eventhubs/{hub_name}"
                f"?api-version=2021-11-01"
            )
            r = _requests.get(
                url,
                headers={"Authorization": f"Bearer {token.token}"},
                verify=verify_ssl,
                timeout=15,
            )
            if r.status_code != 200:
                return None, None
            cap  = r.json().get("properties", {}).get("captureDescription", {})
            if not cap.get("enabled"):
                return None, None
            props = cap.get("destination", {}).get("properties", {})
            return props.get("storageAccountResourceId"), props.get("blobContainer")
        except Exception as exc:
            guard(exc)
            return None, None

    # ── helper: measure a known storage account + container ───────────────────
    def _measure_capture_container(storage_id, container_name, label,
                                    ns_name: str = "", hub_name: str = ""):
        nonlocal audit_bytes_total, audit_accts_measured, audit_accts_capped
        skey = f"{storage_id}::{container_name}"
        if skey in seen_capture_storage:
            return
        seen_capture_storage.add(skey)
        try:
            s_parsed = parse_resource_id(storage_id)
            acct_url = f"https://{s_parsed['resource_name']}.blob.core.windows.net"
            blob_svc = BlobSvcClient(
                account_url=acct_url,
                credential=cred,
                transport=blob_transport,
            )
            gb_day, capped, accessible = measure_blob_container_gb_day(
                blob_svc,
                container_name,
                days=7,
                max_blobs=_MAX_BLOBS_PER_ACCOUNT,
                warn_fn=warn,
                label=label,
            )
            if capped:
                audit_accts_capped += 1
                return
            if accessible and gb_day is not None:
                audit_bytes_total += gb_day
                audit_accts_measured += 1
                _eh_capture_inventory.append({
                    "ns_name":        ns_name,
                    "hub_name":       hub_name,
                    "storage_account": s_parsed["resource_name"],
                    "storage_id":     storage_id,
                    "container":      container_name,
                    "gb_day":         round(gb_day, 4),
                    "status":         "measured",
                })
        except Exception as exc2:
            guard(exc2); warn(f"Capture blob listing ({label})", exc2)
            # Structured diagnostic — disambiguate RBAC vs firewall using role cache
            try:
                acct_name_local = parse_resource_id(storage_id)["resource_name"]
            except Exception:
                acct_name_local = storage_id.rsplit("/", 1)[-1] if storage_id else ""
            msg_l = str(exc2).lower()
            if "nodename nor servname" in msg_l or "failed to resolve" in msg_l:
                _cat = "private_endpoint_only"
                _fix = build_fix_string(_cat, _oid, storage_id)
            elif "authorizationfailure" in msg_l or "not authorized" in msg_l:
                if storage_id and has_role_for("Storage Blob Data Reader",
                                               storage_id, _sub_roles):
                    _cat = "firewall_blocked"
                    _fix = build_fix_string(_cat, _oid, storage_id,
                                            extra={"client_ip": _client_ip})
                else:
                    _cat = "rbac_missing_sbdr"
                    _fix = build_fix_string(_cat, _oid, storage_id)
            else:
                _cat, _fix = "not_configured", ""
            try:
                emit_diagnostic(make_diagnostic(
                    sub_id=sub_id, sub_name=sub_name,
                    resource_type="event_hub_capture",
                    resource_id=storage_id,
                    resource_name=acct_name_local,
                    category=_cat,
                    impact="audit_log_unmeasured",
                    sub_path=container_name,
                    fix=_fix,
                    raw_error=str(exc2),
                ))
            except ValueError:
                pass

    # ═══════════════════════════════════════════════════════════════════════════
    # PATH A — Subscription Diagnostic Settings
    # ═══════════════════════════════════════════════════════════════════════════
    diag_settings_raw = []
    try:
        hb("DiagSettings-audit")
        diag_settings_raw = _arm_list(
            f"https://management.azure.com/subscriptions/{sub_id}"
            f"/providers/microsoft.insights/diagnosticSettings"
            f"?api-version=2021-05-01-preview",
            "Diagnostic settings REST",
        )
    except Exception as exc:
        guard(exc); warn("Diagnostic settings (subscription)", exc)

    for ds in diag_settings_raw:
        p = ds.get("properties", {})

        # A1 — Activity Log routed to Event Hub
        eh_rule = p.get("eventHubAuthorizationRuleId")
        eh_name = p.get("eventHubName")
        if eh_rule:
            try:
                ns_parts  = eh_rule.split("/")
                ns_id     = "/".join(ns_parts[:-2])
                ns_parsed = parse_resource_id(ns_id)
                ns_rg, ns_name = ns_parsed["resource_group"], ns_parsed["resource_name"]
                eh_namespaces_found = max(eh_namespaces_found, 1)

                # List hubs in this namespace, filter to named hub if specified
                try:
                    hubs = list(eh_client.event_hubs.list_by_namespace(ns_rg, ns_name))
                except Exception as exc2:
                    guard(exc2); warn(f"EH list (diag path, {ns_name})", exc2)
                    hubs = []

                for hub in hubs:
                    if eh_name and hub.name != eh_name:
                        continue
                    storage_id, container_name = _get_eh_capture_dest(
                        sub_id, ns_rg, ns_name, hub.name
                    )
                    if storage_id and container_name:
                        eh_with_capture_found += 1
                        _measure_capture_container(
                            storage_id, container_name,
                            f"DiagSetting→EH capture ({ns_name}/{hub.name}/{container_name})",
                            ns_name=ns_name, hub_name=hub.name,
                        )
            except Exception as exc:
                guard(exc); warn(f"Diag setting EH path ({eh_rule})", exc)

        # A2 — Activity Log routed directly to Storage Account
        storage_id_diag = p.get("storageAccountId")
        if storage_id_diag:
            try:
                s_parsed = parse_resource_id(storage_id_diag)
                acct_url = f"https://{s_parsed['resource_name']}.blob.core.windows.net"
                blob_svc = BlobSvcClient(account_url=acct_url, credential=cred,
                                          transport=blob_transport)
                # Azure uses well-known container names for direct storage routing
                for cname in ["insights-operational-logs", "insights-activity-logs",
                               "insights-logs-administrative", "insights-logs-auditevent"]:
                    skey = f"{storage_id_diag}::{cname}"
                    if skey in seen_capture_storage:
                        continue
                    seen_capture_storage.add(skey)
                    gb_day, capped, accessible = measure_blob_container_gb_day(
                        blob_svc,
                        cname,
                        days=7,
                        max_blobs=_MAX_BLOBS_PER_ACCOUNT,
                        warn_fn=lambda *a, **k: None,   # suppress 404s for non-existent containers
                        label=f"DiagSetting→Storage ({s_parsed['resource_name']}/{cname})",
                    )
                    if capped:
                        audit_accts_capped += 1
                    elif accessible and gb_day is not None:
                        audit_bytes_total += gb_day
                        audit_accts_measured += 1
            except Exception as exc2:
                guard(exc2); warn(f"Diag setting storage path ({storage_id_diag})", exc2)

        # A3 — Activity Log routed to Log Analytics Workspace
        law_id = p.get("workspaceId")
        if law_id:
            _law_audit_workspace_ids.add(law_id)

    # ═══════════════════════════════════════════════════════════════════════════
    # PATH B — Direct Event Hub Capture scan (independent of diagnostic settings)
    #
    # Walks every EH namespace → every hub → REST call for exact capture
    # destination. O(namespaces × hubs) — typically single digits per sub.
    # Deduplicates against seen_capture_storage so Path A results aren't doubled.
    # ═══════════════════════════════════════════════════════════════════════════
    try:
        hb("EventHub-capture-scan")
        namespaces_raw = _arm_list(
            f"https://management.azure.com/subscriptions/{sub_id}"
            f"/providers/Microsoft.EventHub/namespaces"
            f"?api-version=2021-11-01",
            "EventHub namespace list",
        )
        eh_namespaces_found = max(eh_namespaces_found, len(namespaces_raw))

        for ns in namespaces_raw:
            ns_id     = ns["id"]
            ns_parsed = parse_resource_id(ns_id)
            ns_rg     = ns_parsed["resource_group"]
            ns_name   = ns_parsed["resource_name"]
            hb("EventHub-capture-scan", ns_name)

            hubs = _arm_list(
                f"https://management.azure.com{ns_id}/eventhubs"
                f"?api-version=2021-11-01",
                f"EventHub list ({ns_name})",
            )
            for hub in hubs:
                hub_name = hub["name"]
                cap      = hub.get("properties", {}).get("captureDescription", {})
                if not cap.get("enabled"):
                    continue

                # Read exact destination from the hub REST response directly.
                # Some SDK paths drop destination fields during deserialization.
                props          = cap.get("destination", {}).get("properties", {})
                storage_id     = props.get("storageAccountResourceId")
                container_name = props.get("blobContainer")

                # Fallback: re-fetch this specific hub if destination missing
                if not (storage_id and container_name):
                    storage_id, container_name = _get_eh_capture_dest(
                        sub_id, ns_rg, ns_name, hub_name
                    )

                if storage_id and container_name:
                    eh_with_capture_found += 1
                    _measure_capture_container(
                        storage_id,
                        container_name,
                        f"EH capture ({ns_name}/{hub_name}/{container_name})",
                        ns_name=ns_name, hub_name=hub_name,
                    )
    except Exception as exc:
        guard(exc); warn("EventHub capture scan (Path B)", exc)

    # ── Compose final status string ───────────────────────────────────────────
    if audit_accts_capped > 0:
        audit_log_method = (
            f"NOT AVAILABLE — {audit_accts_capped} audit capture container(s) exceeded "
            f"the safe listing cap of {_MAX_BLOBS_PER_ACCOUNT:,} blobs in the 7-day window"
        )
    elif audit_accts_measured > 0:
        audit_log_gb_day = round(audit_bytes_total, 4)
        audit_log_method = (
            f"Audit blob listing  "
            f"({audit_accts_measured} capture container(s),  7-day avg)"
        )
    elif eh_with_capture_found > 0:
        audit_log_method = (
            f"NOT AVAILABLE — {eh_with_capture_found} Event Hub(s) with capture found "
            f"but capture storage inaccessible  "
            f"(add Storage Blob Data Reader on capture storage account)"
        )
    elif eh_namespaces_found > 0:
        audit_log_method = (
            f"NOT AVAILABLE — {eh_namespaces_found} Event Hub namespace(s) found "
            f"but no Event Hubs have capture enabled"
        )
    elif diag_settings_raw:
        audit_log_method = (
            f"NOT AVAILABLE — subscription diagnostic setting found "
            f"but no capture storage accessible"
        )

    # ══════════════════════════════════════════════════════════════════════════
    # 14. FLOW LOG SIZING — prefix-based blob measurement
    #
    #  Cortex Cloud ingests flow logs via a dedicated Azure Function that reads
    #  raw blobs directly from Network Watcher storage accounts.
    #
    #  Two container names are checked per storage account:
    #    NSG flow logs (retired June 2025, legacy deployments):
    #      insights-logs-networksecuritygroupflowevent
    #    VNet flow logs (current, new deployments since June 2025):
    #      insights-logs-flowlogflowevent
    #
    #  Flow log containers accumulate blobs indefinitely — a busy subscription
    #  can have hundreds of thousands of blobs spanning years.  Iterating all
    #  of them to filter by last_modified is slow and unnecessary because the
    #  date is encoded directly in the blob path hierarchy:
    #
    #    NSG: resourceId=/SUBSCRIPTIONS/{SUB}/RESOURCEGROUPS/{RG}/PROVIDERS/
    #         MICROSOFT.NETWORK/NETWORKSECURITYGROUPS/{NSG}/y={Y}/m={MM}/d={DD}/...
    #         (prefix is keyed on the TARGET NSG resource ID)
    #    VNet: flowLogResourceID=/{SUB}_{FLOWLOG_RG}/{FLOWLOG_NAME}/
    #          y={Y}/m={MM}/d={DD}/h={HH}/...
    #          (prefix is keyed on the FLOW LOG RESOURCE itself, not the target)
    #
    #  For each enabled flow log target (NSG or VNet), we construct date-specific
    #  prefixes for the last 7 days and call list_blobs(name_starts_with=prefix).
    #  This fetches only ~24 blobs per resource per day regardless of how many
    #  years of history exist in the container.
    #
    #  Required: Storage Blob Data Reader on the flow-log storage account(s).
    #  Network firewalls on storage accounts block access — whitelist the
    #  script runner's IP or run from inside the customer's VNet.
    # ══════════════════════════════════════════════════════════════════════════
    flow_log_gb_day = _NOT_AVAILABLE
    flow_log_method = "NOT AVAILABLE — no flow logs configured in this subscription"
    _flow_log_inventory: list = []    # one entry per spec (flow log resource) that was successfully measured

    if flow_log_sources > 0 and _flow_log_specs:
        flow_bytes_total    = 0.0
        flow_accts_measured = 0
        flow_accts_blocked  = 0
        containers_found    = []

        # Bounded retry: storage account firewall rules can take 5-15 minutes to
        # propagate after an IP allowlist edit.  We allow ONE 15s retry per scan
        # (not per account) — sufficient to recover from a freshly-added rule
        # without ballooning scan time when many accounts genuinely lack RBAC.
        _firewall_retry_used = [False]   # list for closure-mutable flag

        def _is_auth_failure(exc) -> bool:
            m = str(exc).lower()
            return "authorizationfailure" in m or "not authorized" in m

        for storage_id, specs in _flow_log_specs.items():
            try:
                s_parsed  = parse_resource_id(storage_id)
                acct_name = s_parsed["resource_name"]
                acct_url  = f"https://{acct_name}.blob.core.windows.net"

                blob_svc = BlobSvcClient(
                    account_url=acct_url,
                    credential=cred,
                    transport=blob_transport,
                )

                acct_gb       = 0.0
                acct_measured = False

                # Distinct containers actually used by flow logs on this account
                containers_for_acct = sorted({s["container_name"] for s in specs})

                for container_name in containers_for_acct:
                    attempt = 0
                    while True:
                        attempt += 1
                        try:
                            gb_day, accessible = measure_flow_blobs_by_prefix(
                                blob_svc,
                                container_name,
                                specs,
                                days=7,
                            )
                            if accessible:
                                acct_gb      += gb_day
                                acct_measured = True
                                if container_name not in containers_found:
                                    containers_found.append(container_name)
                            break
                        except Exception as exc_c:
                            if (attempt == 1
                                    and _is_auth_failure(exc_c)
                                    and not _firewall_retry_used[0]):
                                _firewall_retry_used[0] = True
                                import time as _t
                                _t.sleep(15)
                                continue
                            _categorise_blob_warn(
                                warn, acct_name, container_name, str(exc_c),
                                "flow_log_unmeasured", sub_id,
                                emit_fn=emit_diagnostic,
                                sub_name=sub_name,
                                storage_id=storage_id,
                                sub_roles=_sub_roles,
                                oid=_oid,
                                client_ip=_client_ip,
                            )
                            break

                if acct_measured:
                    flow_bytes_total    += acct_gb
                    flow_accts_measured += 1
                    # Record one inventory entry per flow log spec writing to this account.
                    # gb_day is the total for the whole storage account (shared across specs).
                    for _spec in specs:
                        _flow_log_inventory.append({
                            "flow_log_name":   (
                                f"{_spec.get('flow_log_watcher_name', '')}/"
                                f"{_spec.get('flow_log_name', '')}"
                            ),
                            "target_id":       _spec.get("target_id", ""),
                            "storage_account": acct_name,
                            "storage_id":      storage_id,
                            "container":       _spec.get("container_name", ""),
                            "gb_day":          round(acct_gb, 4),
                            "shared_account":  len(specs) > 1,
                        })
                else:
                    flow_accts_blocked  += 1

            except Exception as exc2:
                guard(exc2)
                warn(f"Flow log storage account ({storage_id.split('/')[-1]})", exc2)

        if flow_accts_measured > 0:
            flow_log_gb_day = round(flow_bytes_total, 4)
            container_note  = ", ".join(containers_found) if containers_found else "unknown"
            flow_log_method = (
                f"Flow-log blob prefix listing  "
                f"({flow_accts_measured} storage acct(s),  "
                f"containers={container_note},  7-day avg)"
            )
            if flow_accts_blocked > 0:
                flow_log_method += (
                    f"  ⚠ {flow_accts_blocked} acct(s) inaccessible "
                    f"(firewall or RBAC — measurement is partial)"
                )
        else:
            flow_log_method = (
                f"NOT AVAILABLE — {flow_log_sources} flow log source(s) configured "
                f"but storage inaccessible  "
                f"(Storage Blob Data Reader role required; storage account network "
                f"firewall may also block access — whitelist this client IP or run "
                f"from inside the customer's VNet)"
            )

    # ══════════════════════════════════════════════════════════════════════════
    # 15. LOG ANALYTICS WORKSPACE (LAW) LOG SIZING
    #
    #  Customers may route Audit Logs and/or Flow Logs to a Log Analytics
    #  Workspace instead of (or in addition to) Event Hub / blob storage.
    #
    #  Discovery sources (collected in earlier sections):
    #    _law_audit_workspace_ids — from subscription diagnostic settings (A3)
    #    _law_flow_workspace_ids  — from Traffic Analytics + VNet flow log diag
    #
    #  For each unique workspace found, this section:
    #    1. Acquires a Log Analytics API token (different scope from ARM).
    #       Token is acquired once and cached for all workspace queries.
    #    2. Fetches the workspace GUID (customerId) from ARM — the Query API
    #       requires the GUID, not the ARM resource ID.
    #    3. Queries the Usage table (7-day window) for known audit + flow
    #       DataTypes.  Usage.Quantity is in MB — converted to GB/day here.
    #    4. For AzureDiagnostics (legacy NSG flow logs) runs a secondary
    #       Category-filtered query on the actual table.
    #
    #  Dual-sink detection:
    #    If BOTH Event Hub/blob AND LAW measurements are available for the
    #    same log type, a dual_sink flag is set.  The combined value is the
    #    sum of both measured streams (the customer routes the same events to
    #    both sinks).  The output table clearly flags dual-sink cases so the
    #    SE can choose which single-sink number to use for Cortex ingestion
    #    sizing if Cortex will connect to only one pipeline.
    #
    #  Cross-subscription workspaces:
    #    Workspaces in a different subscription are queried with the same
    #    credential (works if the identity has cross-subscription Reader).
    #    On 403, the workspace is reported as NOT AVAILABLE with an explicit
    #    cross-subscription note rather than a generic failure.
    #
    #  Required permissions:
    #    Log Analytics Reader — on the workspace or its subscription.
    #    ARM Reader — already required for all other sections.
    # ══════════════════════════════════════════════════════════════════════════
    law_audit_gb_day    = _NOT_AVAILABLE
    law_flow_gb_day     = _NOT_AVAILABLE   # always NOT_AVAILABLE — LAW is not Cortex's flow path
    law_audit_method    = "NOT AVAILABLE — no Log Analytics Workspace found as audit log sink"
    law_flow_method     = (
        "NOT MEASURED via LAW — Cortex ingests flow logs directly from blob storage "
        "(Azure Function reads insights-logs-flowlogflowevent / "
        "insights-logs-networksecuritygroupflowevent containers). "
        "LAW Traffic Analytics tables (NTANetAnalytics, AzureNetworkAnalytics_CL) "
        "reflect a separate aggregation pipeline, not Cortex ingestion volume."
    )
    law_workspaces_found      = 0
    law_workspaces_no_access  = 0
    law_workspaces_cross_sub  = 0
    law_audit_bytes_total     = 0.0
    law_audit_ws_measured     = 0

    # Only query workspaces that are audit sinks — flow log LAW measurement
    # is not performed (see _LAW_FLOW_DATA_TYPES comment above).
    all_law_ids = _law_audit_workspace_ids
    law_workspaces_found = len(all_law_ids)

    if all_law_ids:
        # ── Acquire Log Analytics API token (one per subscription scan) ───────
        # Scope is completely different from ARM — must be acquired separately.
        law_token_str = None
        try:
            law_tok = cred.get_token(_LAW_API_AUDIENCE)
            law_token_str = law_tok.token
        except Exception as exc_lt:
            warn("LAW token acquisition", exc_lt)

        # ── Acquire ARM token for workspace GUID lookups ──────────────────────
        arm_token_str = None
        try:
            arm_tok = cred.get_token("https://management.azure.com/.default")
            arm_token_str = arm_tok.token
        except Exception as exc_at:
            warn("ARM token for LAW GUID lookup", exc_at)

        for ws_id in all_law_ids:
            hb("LAW-sizing", ws_id.split("/")[-1] if "/" in ws_id else ws_id)

            is_audit_sink = ws_id in _law_audit_workspace_ids

            # Detect cross-subscription references
            try:
                ws_parsed = parse_resource_id(ws_id)
                ws_sub    = ws_parsed.get("subscription")
                is_cross_sub = (ws_sub and ws_sub.lower() != sub_id.lower())
            except Exception:
                ws_sub       = None
                is_cross_sub = False

            if is_cross_sub:
                law_workspaces_cross_sub += 1
                cross_note = f"cross-subscription workspace ({ws_sub})"
                if is_audit_sink:
                    law_audit_method = f"NOT AVAILABLE — {cross_note} — verify Reader access to that subscription"
                # Attempt anyway — credential may have cross-sub access

            if not law_token_str or not arm_token_str:
                law_workspaces_no_access += 1
                if is_audit_sink:
                    law_audit_method = "NOT AVAILABLE — LAW or ARM token could not be acquired"
                continue

            # Fetch workspace GUID — required by the Query API
            customer_id = _get_law_customer_id(ws_id, arm_token_str, verify_ssl)
            if not customer_id:
                law_workspaces_no_access += 1
                if is_audit_sink:
                    law_audit_method = (
                        f"NOT AVAILABLE — workspace found ({ws_id.split('/')[-1]}) "
                        f"but GUID lookup failed (check ARM Reader on workspace)"
                    )
                try:
                    emit_diagnostic(make_diagnostic(
                        sub_id=sub_id, sub_name=sub_name,
                        resource_type="law",
                        resource_id=ws_id,
                        resource_name=ws_id.split("/")[-1],
                        category="rbac_missing_law_reader",
                        impact="law_volume_unmeasured",
                        sub_path="",
                        fix=build_fix_string("rbac_missing_law_reader",
                                             _oid, ws_id),
                        raw_error="LAW GUID lookup failed (likely missing Reader on workspace)",
                    ))
                except ValueError:
                    pass
                continue

            # Query usage volumes
            result = _query_law_usage(customer_id, law_token_str, verify_ssl)

            if result is None:
                # HTTP 403 → no Log Analytics Reader
                law_workspaces_no_access += 1
                no_access_note = (
                    f"workspace {ws_id.split('/')[-1]} — "
                    f"add Log Analytics Reader role"
                )
                if is_audit_sink:
                    law_audit_method = f"NOT AVAILABLE — LAW found, no reader: {no_access_note}"
                try:
                    emit_diagnostic(make_diagnostic(
                        sub_id=sub_id, sub_name=sub_name,
                        resource_type="law",
                        resource_id=ws_id,
                        resource_name=ws_id.split("/")[-1],
                        category="rbac_missing_law_reader",
                        impact="law_volume_unmeasured",
                        sub_path="",
                        fix=build_fix_string("rbac_missing_law_reader",
                                             _oid, ws_id),
                        raw_error="HTTP 403 from Log Analytics Query API",
                    ))
                except ValueError:
                    pass
                continue

            # Accumulate audit measurements
            if is_audit_sink and result.get("audit_gb_day") is not None:
                law_audit_bytes_total += result["audit_gb_day"]
                law_audit_ws_measured += 1

        # ── Compose LAW method strings ────────────────────────────────────────
        if law_audit_ws_measured > 0:
            law_audit_gb_day = round(law_audit_bytes_total, 4)
            law_audit_method = (
                f"LAW Usage query  ({law_audit_ws_measured} workspace(s), 7-day avg, "
                f"tables: AzureActivity + AuditLogs + MicrosoftGraphActivityLogs "
                f"+ SigninLogs (5 variants incl ADFS) + ProvisioningLogs "
                f"+ AKSAudit + AKSAuditAdmin + AKSControlPlane)"
            )
        elif law_workspaces_found > 0 and law_workspaces_no_access == 0 and law_workspaces_cross_sub == 0:
            law_audit_method = (
                f"NOT AVAILABLE — {law_workspaces_found} LAW workspace(s) found "
                f"but no audit log data in last 7 days"
            )

        # law_flow_gb_day and law_flow_method are not set here —
        # LAW is not the Cortex flow log ingestion path (see constant comments).

    # ── Combine EH/blob measurements with LAW measurements ───────────────────
    # EH/blob is the PRIMARY path — it measures what Cortex actually ingests.
    # LAW is a SECONDARY proxy — it measures the same events from a different
    # vantage point (what the customer stored in their workspace).
    #
    # These are NOT additive. Both paths observe the same underlying events;
    # summing them double-counts every event.
    #
    # Priority rule:
    #   EH measured              → use EH for all sizing math
    #   LAW measured, no EH      → use LAW as proxy
    #   Both measured (dual-sink)→ use EH for sizing; show LAW as reference
    #   Neither measured         → NOT AVAILABLE

    eh_audit_gb_day  = audit_log_gb_day   # preserve pre-combination EH/blob value
    blob_flow_gb_day = flow_log_gb_day

    audit_dual_sink = (eh_audit_gb_day  is not None and law_audit_gb_day is not None)
    flow_dual_sink  = (blob_flow_gb_day is not None and law_flow_gb_day  is not None)

    def _resolve(primary, secondary, dual, label):
        """Return the value to use for SKU math (primary preferred)."""
        if primary is not None:
            return primary          # EH/blob always wins when available
        if secondary is not None:
            return secondary        # LAW only when EH not measured
        return None                 # neither measured

    def _build_method(primary_method, secondary_method, primary_val, dual):
        if not dual:
            # Single source — return whichever has data
            if primary_val is not None:
                return primary_method
            return secondary_method
        # Dual-sink: EH used for sizing, LAW shown as reference
        return (
            f"EH/blob measurement used for sizing  ({primary_method}).  "
            f"LAW also measured ({secondary_method}) — same events routed to both sinks; "
            f"LAW value is for reference only and is NOT added to the EH figure."
        )

    combined_audit_gb_day = _resolve(eh_audit_gb_day, law_audit_gb_day, audit_dual_sink, "audit")
    combined_flow_gb_day  = _resolve(blob_flow_gb_day, law_flow_gb_day, flow_dual_sink,  "flow")
    combined_audit_method = _build_method(audit_log_method, law_audit_method,
                                           eh_audit_gb_day, audit_dual_sink)
    combined_flow_method  = _build_method(flow_log_method,  law_flow_method,
                                           blob_flow_gb_day, flow_dual_sink)

    # Overwrite with resolved values for downstream SKU calculation
    audit_log_gb_day = combined_audit_gb_day
    flow_log_gb_day  = combined_flow_gb_day
    audit_log_method = combined_audit_method
    flow_log_method  = combined_flow_method

    # ══════════════════════════════════════════════════════════════════════════
    # Assemble, print, return
    # ══════════════════════════════════════════════════════════════════════════
    counts = {
        # Resource counts
        "vm_running":               vm_running,
        "aks_nodes":                aks_nodes,
        "aro_nodes":                aro_nodes,
        "aro_clusters":             aro_clusters_seen,
        "aci_containers":           aci_containers,
        "container_app_containers": container_app_containers,
        "azure_functions":          azure_functions,
        # Function App breakdown (informational — not used in metering formula)
        "function_apps_stopped":        function_apps_stopped,         # excluded: state != Running
        "function_apps_logic_apps":     function_apps_logic_apps,      # excluded: Logic Apps Standard
        "azure_sql_dbs":            azure_sql_dbs,
        "cosmos_db":                cosmos_db,
        "storage_accounts":         storage_accounts,
        "acr_images":               acr_images,            # tag count — primary billable unit (All scan mode)
        "acr_tag_count":            acr_tag_count,         # same as acr_images — explicit tag count
        "acr_manifest_count":       acr_manifest_count,    # unique manifests — informational
        "acr_registries_found":     acr_registries_found,
        "acr_registries_blocked":   acr_registries_blocked,  # firewall-blocked (data-plane DENIED)
        "acr_registries_scanned":   acr_registries_scanned,
        "acr_all_blocked":          acr_all_blocked,       # True = count=0 is NOT a genuine zero
        "acr_partially_blocked":    acr_partially_blocked, # True = count is understated
        "free_image_allowance":     free_image_allowance,
        # Log source inventory
        "flow_log_sources":         flow_log_sources,
        "flow_log_inventory":       _flow_log_inventory,   # one entry per measured flow log spec
        "eh_capture_inventory":     _eh_capture_inventory,  # one entry per measured EH capture path
        "flow_accts_measured":      locals().get("flow_accts_measured", 0),
        "flow_accts_blocked":       locals().get("flow_accts_blocked", 0),
        "flow_accts_total":         (locals().get("flow_accts_measured", 0)
                                     + locals().get("flow_accts_blocked", 0)),
        "eh_namespaces_found":      eh_namespaces_found,
        "eh_with_capture_found":    eh_with_capture_found,
        # LAW source inventory
        "law_workspaces_found":     law_workspaces_found,
        "law_workspaces_no_access": law_workspaces_no_access,
        "law_workspaces_cross_sub": law_workspaces_cross_sub,
        "law_audit_workspace_ids":  sorted(_law_audit_workspace_ids),  # for debug/audit
        # Log ingestion — pre-combination individual source values (None = not measured)
        "eh_audit_gb_day":          eh_audit_gb_day,
        "blob_flow_gb_day":         blob_flow_gb_day,
        "law_audit_gb_day":         law_audit_gb_day,
        "law_flow_gb_day":          law_flow_gb_day,
        "law_audit_method":         law_audit_method,
        "law_flow_method":          law_flow_method,
        # Log ingestion — combined final values (what SKU calc and summary use)
        "audit_log_gb_day":         audit_log_gb_day,
        "audit_log_method":         audit_log_method,
        "flow_log_gb_day":          flow_log_gb_day,
        "flow_log_method":          flow_log_method,
        # Dual-sink flags — set when BOTH EH/blob AND LAW are measured
        "audit_dual_sink":          audit_dual_sink,
        "flow_dual_sink":           flow_dual_sink,
    }

    net_images = max(0, acr_images - free_image_allowance)
    aro_label  = (
        f"ARO nodes  ({aro_clusters_seen} cluster(s): {ARO_MASTERS_PER_CLUSTER} masters + workers each)"
        if aro_clusters_seen > 0 else "ARO nodes"
    )

    # Function App detail rows — only show non-zero categories
    fa_detail = []
    if function_apps_stopped > 0:
        fa_detail.append(("  └─ Stopped / disabled  (excluded)",    function_apps_stopped))
    if function_apps_logic_apps > 0:
        fa_detail.append(("  └─ Logic Apps Standard  (excluded)",   function_apps_logic_apps))

    # LAW detail rows — only show when workspaces were found
    law_detail = []
    if law_workspaces_found > 0:
        law_detail.append(("  └─ LAW audit sinks",  len(_law_audit_workspace_ids)))
        law_detail.append(("  └─ LAW flow sinks",   len(_law_flow_workspace_ids)))
        if law_workspaces_no_access > 0:
            law_detail.append(("  └─ LAW workspaces — no reader access ⚠",  law_workspaces_no_access))
        if law_workspaces_cross_sub > 0:
            law_detail.append(("  └─ LAW workspaces — cross-subscription ⚠", law_workspaces_cross_sub))

    acr_tag_label = (
        "ACR images  (tags — ⚠ FIREWALL BLOCKED — count not reliable)"
        if acr_all_blocked else
        "ACR images  (tags — ⚠ PARTIAL — some registries blocked)"
        if acr_partially_blocked else
        "ACR images  (tags — All scan mode)"
    )

    sku = compute_sku(counts)

    if verbose:
        print_resource_table(sub_name, sub_id, [
            ("VMs (running)",                                    vm_running),
            ("AKS agent-pool nodes",                             aks_nodes),
            (aro_label,                                          aro_nodes),
            ("ACI containers",                                   aci_containers),
            ("Container App containers  (running)",              container_app_containers),
            ("Azure Functions  (running apps)",                 azure_functions),
            *fa_detail,
            ("Azure SQL databases",                              azure_sql_dbs),
            ("Cosmos DB accounts  (public)",                     cosmos_db),
            ("Storage Accounts",                                 storage_accounts),
            (acr_tag_label,                                      acr_tag_count),
            ("  └─ ACR manifests  (unique digests — info only)", acr_manifest_count),
            ("  └─ Free allowance  (VMs×10 + CaaS÷10×10)",      free_image_allowance),
            ("  └─ Net billable images",                         net_images),
            ("Flow Log sources      [Runtime SKU]",              flow_log_sources),
            ("Event Hub namespaces found",                       eh_namespaces_found),
            ("  └─ Event Hubs with capture enabled",             eh_with_capture_found),
            ("Log Analytics Workspaces  (log sinks)",            law_workspaces_found),
            *law_detail,
        ])
        print_sku_breakdown(sub_name, counts, sku)
    else:
        print_compact_status(sub_name, sub_id, counts, sku, _diagnostics,
                             flow_log_sources_total=flow_log_sources)

    return counts, sku, _diagnostics


# ──────────────────────────────────────────────────────────────────────────────
# Preflight permission check
# ──────────────────────────────────────────────────────────────────────────────
_PF_PASS = "PASS"
_PF_FAIL = "FAIL"
_PF_SSL  = "SSL"
_PF_SKIP = "SKIP"


def _preflight_check_sub(sub_id: str, cred: object,
                         transport: object, verify_ssl: bool) -> dict:
    """
    Lightweight per-subscription permission check.

    Checks:
      reader       — list storage accounts  (basic ARM Reader)
      blob_reader  — list blob containers on a small sample of storage accounts
                     (covers flow-log storage and Event Hub capture storage)
      eh_reader    — list Event Hub namespaces (ARM Reader sufficient)
      acr_dp       — test a small sample of registries with a lightweight
                     repository-name read
      law_reader   — list Log Analytics workspaces + attempt a Usage query on
                     the first workspace found (Log Analytics Reader role)
    """
    from datetime import timedelta

    r: dict = {
        "reader":        _PF_SKIP,
        "blob_reader":   _PF_SKIP,
        "eh_reader":     _PF_SKIP,
        "acr_dp":        _PF_SKIP,
        "law_reader":    _PF_SKIP,
        "storage_count": 0,
        "eh_count":      0,
        "acr_count":     0,
        "law_count":     0,
        "detail":        [],
    }

    def _is_ssl(msg: str) -> bool:
        return any(k in msg.lower() for k in ("ssl", "certificate", "verify"))

    def _is_perm(msg: str) -> bool:
        return any(k in msg.lower() for k in
                   ("authorization", "permission", "forbidden", "403",
                    "authorizationfailure", "authorizationpermissionmismatch"))

    # ── Reader: list storage accounts ──────────────────────────────────────
    storage_accounts: list = []
    try:
        from azure.mgmt.storage import StorageManagementClient
        stc = StorageManagementClient(cred, sub_id, transport=transport)
        storage_accounts = list(stc.storage_accounts.list())
        r["reader"] = _PF_PASS
        r["storage_count"] = len(storage_accounts)
    except Exception as exc:
        msg = str(exc)
        r["reader"] = _PF_FAIL if _is_perm(msg) else _PF_SSL if _is_ssl(msg) else _PF_FAIL
        r["detail"].append(f"Reader: {msg[:100]}")
        return r

    # ── Storage Blob Data Reader ────────────────────────────────────────────
    # Samples up to 5 storage accounts and attempts list_containers() on each.
    # We try ALL samples — not just the first — because many subscriptions have
    # a mix of open and firewall-restricted accounts.  Reporting ❌ on the first
    # firewall block while ignoring the others would misrepresent the actual role
    # assignment.
    #
    # IMPORTANT: Azure returns the same AuthorizationFailure (HTTP 403) for both
    # missing RBAC roles AND storage account network firewall blocks.  The two
    # cannot be reliably distinguished from the error alone.  If the role IS
    # assigned at subscription scope but all sampled accounts have firewalls,
    # the check will still report ❌ — this is a known limitation.  The
    # preflight output explicitly flags this ambiguity.
    if storage_accounts:
        sample_accounts = storage_accounts[:5]
        saw_pass = False
        saw_firewall_block = False
        saw_dns_fail = False
        n_tried = 0

        for sa in sample_accounts:
            n_tried += 1
            try:
                from azure.storage.blob import BlobServiceClient
                bsc = BlobServiceClient(
                    account_url=f"https://{sa.name}.blob.core.windows.net",
                    credential=cred,
                    transport=transport,
                    max_retries=1,
                )
                next(iter(bsc.list_containers(results_per_page=1)), None)
                saw_pass = True
            except Exception as exc:
                msg = str(exc)
                msg_lower = msg.lower()
                if _is_ssl(msg):
                    continue
                if "nodename nor servname" in msg_lower or "failed to resolve" in msg_lower:
                    saw_dns_fail = True
                    continue
                if _is_perm(msg):
                    # Could be missing role OR firewall — note it but keep trying others
                    saw_firewall_block = True
                    r["detail"].append(f"Blob access blocked on {sa.name} (RBAC or firewall)")
                    continue
                # Other error — note and continue
                r["detail"].append(f"Blob-Reader: {msg[:80]}")

        if saw_pass:
            r["blob_reader"] = _PF_PASS
        elif saw_dns_fail and not saw_firewall_block:
            r["blob_reader"] = _PF_FAIL
            r["detail"].append("All sampled storage accounts unreachable (private endpoints)")
        elif saw_firewall_block or saw_dns_fail:
            # At least one access block — role may or may not be assigned
            r["blob_reader"] = _PF_FAIL
            r["detail"].append(
                "Blob access blocked on all sampled accounts — "
                "Storage Blob Data Reader may be MISSING, OR storage account "
                "network firewalls are blocking this client IP.  "
                "Check IAM; if role is assigned, the firewall is the blocker."
            )

    # ── Event Hub namespace listing (ARM Reader sufficient) ─────────────────
    try:
        from azure.mgmt.eventhub import EventHubManagementClient
        ehc = EventHubManagementClient(cred, sub_id, transport=transport)
        namespaces = list(ehc.namespaces.list())
        r["eh_count"] = len(namespaces)
        r["eh_reader"] = _PF_PASS
    except Exception as exc:
        msg = str(exc)
        if _is_ssl(msg):
            r["eh_reader"] = _PF_SSL
        elif _is_perm(msg):
            r["eh_reader"] = _PF_FAIL
            r["detail"].append("EventHub Reader MISSING — audit log sizing disabled")
        else:
            r["eh_reader"] = _PF_FAIL
            r["detail"].append(f"EventHub: {msg[:80]}")

    # ── ACR data-plane ─────────────────────────────────────────────────────
    registries: list = []
    try:
        from azure.mgmt.containerregistry import ContainerRegistryManagementClient
        rcm = ContainerRegistryManagementClient(cred, sub_id, transport=transport)
        registries = list(rcm.registries.list())
        r["acr_count"] = len(registries)
    except Exception as exc:
        r["detail"].append(f"ACR-list: {str(exc)[:80]}")

    if registries:
        sample_registries = registries[:3]
        saw_ssl = False
        saw_pass = False

        for reg in sample_registries:
            try:
                from azure.containerregistry import ContainerRegistryClient
                rcd = ContainerRegistryClient(
                    f"https://{reg.login_server}",
                    cred,
                    transport=transport,
                )
                next(iter(rcd.list_repository_names()), None)
                saw_pass = True
            except Exception as exc:
                msg = str(exc)
                if _is_ssl(msg):
                    saw_ssl = True
                    continue
                if "denied" in msg.lower() or "not allowed access" in msg.lower():
                    r["acr_dp"] = _PF_FAIL
                    r["detail"].append(
                        f"ACR-dp network firewall blocking IP on {reg.name} — "
                        f"not a permission gap, add your IP to ACR allowlist"
                    )
                    break
                if any(k in msg.lower() for k in ("unauthorized", "forbidden", "403")):
                    r["acr_dp"] = _PF_FAIL
                    r["detail"].append(
                        f"ACR-dp perm denied on {reg.name} — "
                        f"assign AcrPull or AcrMetadataRead"
                    )
                    break
                r["acr_dp"] = _PF_FAIL
                r["detail"].append(f"ACR-dp: {msg[:80]}")
                break

        if r["acr_dp"] == _PF_SKIP:
            if saw_pass:
                r["acr_dp"] = _PF_PASS
            elif saw_ssl:
                r["acr_dp"] = _PF_SSL

    # ── Log Analytics Reader (LAW) ──────────────────────────────────────────
    # Two-step check:
    #   Step 1 — list LAW workspaces in the subscription (ARM Reader sufficient).
    #   Step 2 — attempt a trivial Usage query on the first workspace found
    #            (requires Log Analytics Reader role on the workspace).
    # If no workspaces exist, the check is skipped (SKIP is not a failure).
    try:
        law_ws_list_url = (
            f"https://management.azure.com/subscriptions/{sub_id}"
            f"/providers/Microsoft.OperationalInsights/workspaces"
            f"?api-version={_LAW_ARM_API_VERSION}"
        )
        arm_tok = cred.get_token("https://management.azure.com/.default")
        law_list_resp = _requests.get(
            law_ws_list_url,
            headers={"Authorization": f"Bearer {arm_tok.token}"},
            verify=verify_ssl,
            timeout=15,
        )
        if law_list_resp.status_code == 200:
            law_workspaces = law_list_resp.json().get("value", [])
            r["law_count"] = len(law_workspaces)
            if law_workspaces:
                # Attempt a lightweight Usage query on the first workspace
                first_ws = law_workspaces[0]
                customer_id = first_ws.get("properties", {}).get("customerId")
                if customer_id:
                    try:
                        law_tok = cred.get_token(_LAW_API_AUDIENCE)
                        probe_resp = _requests.post(
                            f"{_LAW_QUERY_ENDPOINT}/{_LAW_QUERY_API_VER}/workspaces/{customer_id}/query",
                            headers={
                                "Authorization": f"Bearer {law_tok.token}",
                                "Content-Type": "application/json",
                            },
                            json={"query": "Usage | take 1 | project DataType"},
                            verify=verify_ssl,
                            timeout=20,
                        )
                        if probe_resp.status_code == 200:
                            r["law_reader"] = _PF_PASS
                        elif probe_resp.status_code in (401, 403):
                            r["law_reader"] = _PF_FAIL
                            r["detail"].append(
                                f"LAW Reader MISSING on {first_ws.get('name', customer_id)} — "
                                f"assign Log Analytics Reader role"
                            )
                        else:
                            r["law_reader"] = _PF_FAIL
                            r["detail"].append(f"LAW query HTTP {probe_resp.status_code}")
                    except Exception as exc_lq:
                        r["law_reader"] = _PF_FAIL
                        r["detail"].append(f"LAW query error: {str(exc_lq)[:80]}")
                else:
                    r["law_reader"] = _PF_FAIL
                    r["detail"].append("LAW workspace found but customerId missing from ARM response")
            # else: no workspaces → SKIP (not a failure)
        elif law_list_resp.status_code in (401, 403):
            r["law_reader"] = _PF_FAIL
            r["detail"].append("ARM Reader cannot list LAW workspaces")
    except Exception as exc_lw:
        msg = str(exc_lw)
        if _is_ssl(msg):
            r["law_reader"] = _PF_SSL
        else:
            r["law_reader"] = _PF_FAIL
            r["detail"].append(f"LAW check error: {msg[:80]}")

    return r


def run_preflight(verify_ssl: bool = True,
                  target_sub_id: str | None = None) -> None:
    """
    Preflight permission check — runs lightweight checks across every visible
    subscription so gaps in RBAC or SSL issues are surfaced before a full scan.

    Per-subscription checks:
      Reader           — list storage accounts (basic ARM access)
      Blob Data Reader — list blob containers on a small storage-account sample
                         (required for both flow-log and Event Hub capture sizing)
      EventHub Reader  — list Event Hub namespaces (ARM Reader is sufficient)
      ACR data-plane   — lightweight repo-name read on a small ACR sample
      LAW Reader       — list LAW workspaces + probe Usage query on first workspace

    Tenant-level check (once):
      Graph User.Read.All — count Entra ID users  (needed for --tenant-scan)
    """
    from azure.identity import DefaultAzureCredential
    from azure.mgmt.subscription import SubscriptionClient
    from azure.core.pipeline.transport import RequestsTransport

    if not verify_ssl:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    SEP = "─" * 96

    def sym(code: str) -> str:
        return {"PASS": "✅", "FAIL": "❌", "SSL": "⚠SSL", "SKIP": "–"}[code]

    print(f"\n{SEP}")
    print("Cortex Cloud — Azure Sizing Preflight Check")
    print(f"{SEP}\n")

    transport = RequestsTransport(connection_timeout=10, read_timeout=30,
                                  connection_verify=verify_ssl)
    cred = DefaultAzureCredential()

    print("Checking authentication...", flush=True)
    subs: list = []
    try:
        sub_client = SubscriptionClient(cred, transport=transport)
        _INACCESSIBLE = {"disabled", "deleted"}
        all_subs = list(sub_client.subscriptions.list())
        subs = [s for s in all_subs
                if (s.state or "").lower() not in _INACCESSIBLE]
    except Exception as exc:
        print(f"  ❌  Cannot list subscriptions: {exc}")
        print(f"\n  Fix authentication first:  az login\n")
        return

    if not subs:
        print("  ⚠  No accessible subscriptions found — check permissions.\n")
        return

    by_state: dict = {}
    for s in subs:
        st = (s.state or "unknown").lower()
        by_state[st] = by_state.get(st, 0) + 1
    state_note = ", ".join(f"{v} {k}" for k, v in sorted(by_state.items()))
    print(f"  ✅  {len(subs)} subscription(s) visible  ({state_note})\n")

    # ── Tenant-level: Graph API ─────────────────────────────────────────────
    print("Tenant-level check (Microsoft Graph — once per organisation):")
    graph_status = ""
    try:
        token = cred.get_token("https://graph.microsoft.com/.default")
        resp  = _requests.get(
            "https://graph.microsoft.com/v1.0/users/$count",
            headers={"Authorization": f"Bearer {token.token}",
                     "ConsistencyLevel": "eventual"},
            verify=verify_ssl,
            timeout=15,
        )
        if resp.status_code == 200:
            graph_status = f"✅  PASS  ({resp.text} users — User.Read.All is working)"
        elif resp.status_code == 403:
            graph_status = ("❌  FAIL  — Grant 'User.Read.All' Application permission "
                            "+ admin consent in Entra ID")
        else:
            graph_status = f"⚠   HTTP {resp.status_code}: {resp.text[:60]}"
    except Exception as exc:
        graph_status = f"⚠   {str(exc)[:100]}"
    print(f"  Graph /v1.0/users/$count  →  {graph_status}\n")

    # ── Per-subscription checks ─────────────────────────────────────────────
    subs_to_check = (
        [s for s in subs
         if s.subscription_id == target_sub_id or s.display_name == target_sub_id]
        if target_sub_id else subs
    )
    if target_sub_id and not subs_to_check:
        print(f"  ⚠  '{target_sub_id}' not found in visible subscriptions — checking all.\n")
        subs_to_check = subs

    n = len(subs_to_check)
    scope = (f"subscription: {subs_to_check[0].display_name}"
             if target_sub_id else f"all {n} subscription(s)")
    print(f"Per-subscription checks  ({scope}):")
    print(f"  Role-assignment lookup — ~1 s per subscription  "
          f"(no storage-account sampling).\n")

    # Resolve OID once — every sub's role cache is filtered to this principal.
    _oid = _decode_oid_from_token(cred, verify_ssl=verify_ssl) or ""
    if not _oid:
        print(f"  ⚠  Could not decode signed-in OID from token — role checks will be skipped.\n")

    sub_results: list[dict] = []
    for i, sub in enumerate(subs_to_check, 1):
        sub_id = sub.subscription_id
        name   = sub.display_name
        print(f"  [{i}/{n}]  {name:<45}  checking...", end="", flush=True)

        # Role cache — one ARM call per sub
        sub_roles = fetch_sub_roles(sub_id, _oid, cred, verify_ssl=verify_ssl) if _oid else {}

        # Reader check: any of Reader / Contributor / Owner at sub scope
        sub_scope = f"/subscriptions/{sub_id}"
        has_reader = (
            has_role_for("Reader",       sub_scope, sub_roles) or
            has_role_for("Contributor",  sub_scope, sub_roles) or
            has_role_for("Owner",        sub_scope, sub_roles)
        )
        has_sbdr    = has_role_for("Storage Blob Data Reader", sub_scope, sub_roles)
        has_law_rdr = has_role_for("Log Analytics Reader",     sub_scope, sub_roles)
        has_acrpull = has_role_for("AcrPull",                  sub_scope, sub_roles)

        # Still do an ACR data-plane SSL check (only thing we can't infer from RBAC)
        acr_dp_status = _PF_SKIP
        acr_count     = 0
        try:
            from azure.mgmt.containerregistry import ContainerRegistryManagementClient
            from azure.containerregistry import ContainerRegistryClient
            rcm = ContainerRegistryManagementClient(cred, sub_id, transport=transport)
            regs = list(rcm.registries.list())
            acr_count = len(regs)
            if regs:
                endpoint = f"https://{regs[0].login_server}"
                try:
                    rc = ContainerRegistryClient(endpoint, cred, transport=transport)
                    # Cheap probe — just list repositories (returns empty on new registries)
                    list(rc.list_repository_names())[:1]
                    acr_dp_status = _PF_PASS
                except Exception as dp_exc:
                    acr_dp_status = _PF_SSL if "ssl" in str(dp_exc).lower() else _PF_FAIL
        except Exception:
            acr_dp_status = _PF_SKIP

        r = {
            "name":        name,
            "sub_id":      sub_id,
            "reader":      _PF_PASS if has_reader   else _PF_FAIL,
            "blob_reader": _PF_PASS if has_sbdr     else _PF_FAIL,
            "eh_reader":   _PF_PASS if has_reader   else _PF_FAIL,    # EH list needs only Reader
            "law_reader":  _PF_PASS if has_law_rdr  else _PF_FAIL,
            "acr_pull":    _PF_PASS if has_acrpull  else _PF_FAIL,
            "acr_dp":      acr_dp_status,
            "acr_count":   acr_count,
            "detail":      [],
        }
        sub_results.append(r)
        cells = [sym(r["reader"]), sym(r["blob_reader"]),
                 sym(r["eh_reader"]), sym(r["acr_dp"]), sym(r["law_reader"])]
        print(f"\r  [{i}/{n}]  {name:<45}  "
              f"Reader:{cells[0]}  SBDR:{cells[1]}  EH-Reader:{cells[2]}  "
              f"ACR-dp:{cells[3]}  LAW:{cells[4]}",
              flush=True)

    # ── Summary table ───────────────────────────────────────────────────────
    col_name = max(len(r["name"]) for r in sub_results)
    col_name = max(col_name, 30)
    hdr  = f"  {'Subscription':<{col_name}}  Reader   SBDR   EH-Reader  ACR-dp   LAW-Reader"
    div  = "  " + "─" * (len(hdr) - 2)
    print(f"\n{div}")
    print(hdr)
    print(div)
    for r in sub_results:
        print(f"  {r['name']:<{col_name}}  "
              f"{sym(r['reader']):<6}   "
              f"{sym(r['blob_reader']):<4}   "
              f"{sym(r['eh_reader']):<8}   "
              f"{sym(r['acr_dp']):<6}   "
              f"{sym(r['law_reader']):<10}")
    print(div)
    print(f"  RBAC checks derived from role-assignments API (principalId = {_oid or 'UNKNOWN'}).")
    print(f"  Inherited roles at sub/RG/resource scope are respected.")

    # ── Consolidated action items ───────────────────────────────────────────
    blob_fails  = [r for r in sub_results if r["blob_reader"] == _PF_FAIL]
    eh_fail     = [r for r in sub_results if r["eh_reader"]   == _PF_FAIL]
    acr_ssl     = [r for r in sub_results if r["acr_dp"]      == _PF_SSL]
    acr_fail    = [r for r in sub_results if r["acr_dp"]      == _PF_FAIL]
    reader_fail = [r for r in sub_results if r["reader"]      == _PF_FAIL]
    law_fail    = [r for r in sub_results if r["law_reader"]  == _PF_FAIL]

    any_issue = blob_fails or eh_fail or acr_ssl or acr_fail or reader_fail or law_fail
    print()

    if reader_fail:
        print("  ❌  Reader role MISSING on:")
        for r in reader_fail:
            print(f"       • {r['name']}  ({r['sub_id']})")
        print(f"     Fix: az role assignment create --role Reader \\")
        print(f"            --assignee <your-object-id> \\")
        print(f"            --scope /subscriptions/<id>")
        print()

    if blob_fails:
        print("  ❌  Storage Blob Data Reader MISSING at subscription scope on:")
        print(f"     (Required for: flow-log blob sizing + Event Hub capture blob sizing)")
        print(f"     Note: sub-scope SBDR is the cleanest option; an RG/resource-level")
        print(f"           assignment will work too but won't show as a pass here.")
        for r in blob_fails:
            print(f"       • {r['name']}  ({r['sub_id']})")
        print(f"     Fix: az role assignment create \\")
        print(f"            --role \"Storage Blob Data Reader\" \\")
        print(f"            --assignee <your-object-id> \\")
        print(f"            --scope /subscriptions/<id>")
        print(f"     Per-account firewall blocks will be reported separately during scan.")
        print()

    if eh_fail:
        print("  ❌  EventHub namespace listing FAILED (audit log sizing disabled) on:")
        for r in eh_fail:
            print(f"       • {r['name']}  ({r['sub_id']})")
        print(f"     Note: ARM Reader on the subscription should cover EventHub listing.")
        print(f"           Verify Reader role is assigned at subscription scope.")
        print()

    if acr_fail:
        firewall_acr = [r for r in acr_fail if any("firewall" in d for d in r["detail"])]
        perm_acr     = [r for r in acr_fail if not any("firewall" in d for d in r["detail"])]
        if perm_acr:
            print("  ❌  AcrPull / AcrMetadataRead MISSING (image count will be 0) on:")
            for r in perm_acr:
                print(f"       • {r['name']}  ({r['sub_id']})")
            print()
        if firewall_acr:
            print("  ⚠   ACR network firewall blocking access (RBAC OK, image count may be 0) on:")
            for r in firewall_acr:
                print(f"       • {r['name']}  ({r['sub_id']})")
            print(f"     Fix: Portal → Container Registry → Networking → Add client IP")
            print()

    if acr_ssl:
        print(f"  ⚠   SSL certificate errors on ACR data-plane:")
        for r in acr_ssl:
            print(f"       • {r['name']}")
        print(f"     Add --no-verify-ssl to all commands.")
        print()

    if law_fail:
        print("  ❌  Log Analytics Reader MISSING (LAW log sizing will be NOT AVAILABLE) on:")
        print(f"     (Required for: Log Analytics Workspace audit + flow log volume queries)")
        for r in law_fail:
            print(f"       • {r['name']}  ({r['sub_id']})")
        print(f"     Fix: az role assignment create \\")
        print(f"            --role \"Log Analytics Reader\" \\")
        print(f"            --assignee <your-object-id> \\")
        print(f"            --scope /subscriptions/<id>")
        print(f"     Note: LAW Reader is only required if the customer routes logs to a")
        print(f"           Log Analytics Workspace.  It is safe to proceed without it if")
        print(f"           the customer uses Event Hub / blob storage exclusively.")
        print()

    if not any_issue and "FAIL" not in graph_status:
        print("  ✅  All checks passed — ready to scan.")
        print(f"  Next:  python3 az-sizing.py --init-state")
    elif not (reader_fail or blob_fails or eh_fail or acr_fail or law_fail) and acr_ssl:
        print("  ✅  No permission gaps.  Add --no-verify-ssl for SSL issues above.")
        print(f"  Next:  python3 az-sizing.py --init-state --no-verify-ssl")
    elif not (reader_fail or blob_fails or eh_fail or acr_fail) and law_fail:
        print("  ⚠   Log Analytics Reader missing — LAW log sizing will be NOT AVAILABLE.")
        print(f"     All other checks passed.  Scan can proceed; add LAW Reader if needed.")
        print(f"  Next:  python3 az-sizing.py --init-state")

    print(f"\n{SEP}\n")


# ──────────────────────────────────────────────────────────────────────────────
# Tenant scan  (SaaS Users via Microsoft Graph API)
# ──────────────────────────────────────────────────────────────────────────────
def run_tenant_scan(tenant_file: str = "azure_tenant.json",
                    verify_ssl: bool = True) -> None:
    """
    Query Microsoft Graph API to count active Entra ID users.
    10 SaaS users = 1 Cortex Cloud workload.
    """
    import base64
    from azure.identity import DefaultAzureCredential

    print(f"\n{SEPARATOR}")
    print("Cortex Cloud  —  Tenant Scan  (SaaS Users / Entra ID)")
    print(SEPARATOR)
    print("  Querying Microsoft Graph API for Entra ID user counts.")
    print("  Required permission : User.Read.All  (Application permission in Entra ID)\n")

    cred = DefaultAzureCredential()

    try:
        token = cred.get_token("https://graph.microsoft.com/.default")
    except Exception as exc:
        print(f"  ERROR: Cannot acquire Graph API token.\n  {exc}")
        print(
            "\n  Troubleshooting:\n"
            "    1. Ensure the identity has User.Read.All or Directory.Read.All\n"
            "       as an APPLICATION permission in Entra ID (not delegated).\n"
            "    2. An admin must have granted tenant-wide consent for the permission.\n"
            "    3. Re-authenticate:  az login  or  az login --service-principal ..."
        )
        return

    tenant_id = None
    try:
        payload = token.token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        claims    = json.loads(base64.urlsafe_b64decode(payload))
        tenant_id = claims.get("tid")
        print(f"  Tenant ID : {tenant_id}")
    except Exception:
        pass

    headers = {
        "Authorization": f"Bearer {token.token}",
        "ConsistencyLevel": "eventual",
    }

    graph_api_failed = False   # set True if any Graph call returns non-200

    def graph_count(filter_str: str, label: str) -> int:
        nonlocal graph_api_failed
        try:
            resp = _requests.get(
                "https://graph.microsoft.com/v1.0/users/$count",
                headers=headers,
                params={"$filter": filter_str},
                verify=verify_ssl,
                timeout=30,
            )
            resp.raise_for_status()
            count = int((resp.text or "0").strip())
            print(f"  {label:<48}: {count:,}")
            return count
        except Exception as exc:
            graph_api_failed = True
            print(f"  [warn] {label}: {exc}")
            return 0

    member_users = graph_count(
        "accountEnabled eq true and userType eq 'Member'",
        "Entra ID Member users (enabled)",
    )
    guest_users = graph_count(
        "accountEnabled eq true and userType eq 'Guest'",
        "Entra ID Guest users (enabled)",
    )

    total_saas = member_users + guest_users
    saas_wl    = math.ceil(total_saas / CC_METERING["saas_users"]) if total_saas > 0 else 0

    print(f"\n  {'Total SaaS users':<48}: {total_saas:,}")
    print(f"  {'SaaS workloads  ({:,} / {:})'.format(total_saas, CC_METERING['saas_users']):<48}: {saas_wl}")

    if graph_api_failed:
        print(f"\n  ⚠  GRAPH API FAILED — SaaS user count is 0 due to a permission error,")
        print(f"     NOT because there are no users.  The Grand Total will be understated.")
        print(f"     Fix: Grant 'User.Read.All' Application permission in Entra ID")
        print(f"          (Portal → Entra ID → App registrations → your app → API permissions)")
        print(f"          and have an admin grant tenant-wide consent.")

    # ── Entra ID tenant-level diagnostic settings ─────────────────────────────
    # Entra ID audit logs (sign-ins, directory changes) are configured at the
    # TENANT level — not per subscription.  The API is:
    #   GET /providers/microsoft.aadiam/diagnosticSettings
    # This is invisible to subscription-level diagnostic setting scans.
    #
    # If these settings route to a Log Analytics Workspace, we record the
    # workspace IDs so the SE knows LAW query sizing applies at tenant scope.
    # We do NOT measure volume here (requires LAW Reader on each workspace);
    # this discovery result is saved to the tenant file for reference.
    #
    # Required: ARM Reader on the tenant root (same credential used above).
    print(f"\n  Scanning Entra ID tenant-level diagnostic settings...")
    entra_diag_workspaces: list[str] = []
    entra_diag_eh:         list[str] = []
    entra_diag_storage:    list[str] = []
    entra_diag_status = "NOT AVAILABLE"

    try:
        arm_tok = cred.get_token("https://management.azure.com/.default")
        diag_resp = _requests.get(
            _ENTRA_DIAG_API,
            headers={"Authorization": f"Bearer {arm_tok.token}"},
            timeout=20,
        )
        if diag_resp.status_code == 200:
            entra_diags = diag_resp.json().get("value", [])
            for ds in entra_diags:
                p = ds.get("properties", {})
                ws = p.get("workspaceId")
                eh = p.get("eventHubAuthorizationRuleId")
                sa = p.get("storageAccountId")
                if ws:
                    entra_diag_workspaces.append(ws)
                if eh:
                    entra_diag_eh.append(eh)
                if sa:
                    entra_diag_storage.append(sa)
            if entra_diags:
                sinks = []
                if entra_diag_workspaces: sinks.append(f"LAW×{len(entra_diag_workspaces)}")
                if entra_diag_eh:         sinks.append(f"EventHub×{len(entra_diag_eh)}")
                if entra_diag_storage:    sinks.append(f"Storage×{len(entra_diag_storage)}")
                entra_diag_status = f"{len(entra_diags)} setting(s) found → {', '.join(sinks)}"
            else:
                entra_diag_status = "No tenant-level diagnostic settings configured"
        elif diag_resp.status_code in (401, 403):
            entra_diag_status = "NOT AVAILABLE — insufficient permissions (need ARM Reader at tenant scope)"
        else:
            entra_diag_status = f"NOT AVAILABLE — HTTP {diag_resp.status_code}"
    except Exception as exc_ed:
        entra_diag_status = f"NOT AVAILABLE — {str(exc_ed)[:100]}"

    print(f"  {'Entra ID diagnostic settings':<48}: {entra_diag_status}")
    if entra_diag_workspaces:
        print(f"  {'  → Audit logs routed to LAW workspace(s)':<48}: {len(entra_diag_workspaces)}")
        print(f"  ⓘ  AuditLogs table in these workspaces holds Entra ID audit data.")
        print(f"     Run LAW volume queries on these workspaces for audit log sizing.")
        for ws_id in entra_diag_workspaces[:5]:   # show first 5 to avoid screen flood
            print(f"       {ws_id}")
        if len(entra_diag_workspaces) > 5:
            print(f"       ... and {len(entra_diag_workspaces) - 5} more (see tenant JSON)")

    data = {
        "tenant_id":                    tenant_id,
        "timestamp":                    now_iso(),
        "graph_api_failed":             graph_api_failed,   # True = SaaS count is 0 due to 403, not genuine
        "entra_member_users":           member_users,
        "entra_guest_users":            guest_users,
        "total_saas_users":             total_saas,
        "saas_user_workloads":          saas_wl,
        # Entra ID diagnostic settings (tenant-level)
        "entra_diag_status":            entra_diag_status,
        "entra_diag_law_workspaces":    entra_diag_workspaces,
        "entra_diag_eventhub_rules":    entra_diag_eh,
        "entra_diag_storage_accounts":  entra_diag_storage,
    }
    tmp = tenant_file + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(data, fh, indent=2)
    os.replace(tmp, tenant_file)

    print(f"\n  Saved to : {tenant_file}")
    print(f"  Summary  : python3 az-summary.py --tenant {tenant_file}")
    print(SEPARATOR)


# ──────────────────────────────────────────────────────────────────────────────
# Main Azure orchestration  (init-state / resume / batch)
# ──────────────────────────────────────────────────────────────────────────────
def pcs_sizing_az(
    init_state:      bool = False,
    resume:          bool = False,
    batch_size:      int  = 0,
    retry_failed:    bool = False,
    state_file:      str  = "azure_state.jsonl",
    results_file:    str  = "azure_results.json",
    heartbeat_sec:   int  = 10,
    sub_timeout_min: int  = 20,
    verify_ssl:      bool = True,
    verbose:         bool = False,
) -> None:

    from azure.identity import DefaultAzureCredential
    from azure.mgmt.subscription import SubscriptionClient
    from azure.core.pipeline.transport import RequestsTransport

    if not verify_ssl:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    transport  = RequestsTransport(connection_timeout=10, read_timeout=120,
                                   connection_verify=verify_ssl)
    cred       = DefaultAzureCredential()
    sub_client = SubscriptionClient(cred, transport=transport)
    if not verify_ssl:
        print("  ⚠  SSL verification disabled (--no-verify-ssl).  "
              "Use only in trusted corporate network environments.", flush=True)

    print(f"\n{SEPARATOR}\nCortex Cloud  —  Azure Workload Sizing\n{SEPARATOR}")

    if init_state:
        _INACCESSIBLE = {"disabled", "deleted"}
        rows = []
        for sub in sub_client.subscriptions.list():
            if (sub.state or "").lower() in _INACCESSIBLE:
                continue
            rows.append({
                "sub_id":      sub.subscription_id,
                "name":        sub.display_name,
                "status":      "pending",
                "attempts":    0,
                "last_error":  None,
                "started_at":  None,
                "finished_at": None,
                "updated_at":  now_iso(),
            })
        write_state(state_file, rows)
        print(f"\n  Discovered {len(rows)} subscription(s)  →  {state_file}")
        print(f"\n  Next:  python3 az-sizing.py --resume --batch-size 25\n")
        return

    state_rows = load_state(state_file)
    if not state_rows:
        print(f"\n  State file '{state_file}' not found.  Run --init-state first.\n")
        return
    if not resume:
        print("\n  Requires --resume (or --init-state to start fresh).")
        print("    python3 az-sizing.py --resume --batch-size 25\n")
        return

    recovered = 0
    for r in state_rows:
        if r.get("status") == "running":
            r["status"]     = "pending"
            r["last_error"] = "Recovered from previous crash."
            r["updated_at"] = now_iso()
            recovered += 1
    if recovered:
        write_state(state_file, state_rows)
        print(f"  Recovered {recovered} subscription(s) from crashed run.", flush=True)

    selected = select_pending(state_rows, retry_failed=retry_failed, batch_size=batch_size)
    if not selected:
        done    = sum(1 for r in state_rows if r["status"] == "done")
        failed  = sum(1 for r in state_rows if r["status"] == "failed")
        pending = sum(1 for r in state_rows if r["status"] == "pending")
        print(f"\n  Nothing to process  (done={done}  failed={failed}  pending={pending})")
        if failed:
            print(f"  Retry:  python3 az-sizing.py --resume --retry-failed")
        return

    batch_total = len(selected)
    print(f"\n  Processing {batch_total} subscription(s)"
          f"  [batch_size={batch_size or 'all'}  retry_failed={retry_failed}]\n")

    try:
        for idx, row in enumerate(selected, start=1):
            sub_id   = row["sub_id"]
            sub_name = row["name"]

            for r in state_rows:
                if r["sub_id"] == sub_id:
                    r.update(status="running", attempts=int(r.get("attempts", 0)) + 1,
                             started_at=now_iso(), finished_at=None,
                             last_error=None, updated_at=now_iso())
            write_state(state_file, state_rows)
            print(f"\n[{idx}/{batch_total}]  {sub_name}  ({sub_id})", flush=True)

            try:
                counts, sku, diagnostics = scan_subscription(
                    cred, sub_id, sub_name, transport,
                    heartbeat_sec=heartbeat_sec, sub_timeout_min=sub_timeout_min,
                    verify_ssl=verify_ssl, verbose=verbose,
                )
                upsert_results(results_file, sub_id, {
                    "name":        sub_name,
                    "timestamp":   now_iso(),
                    "raw_counts":  counts,
                    "sku":         sku,
                    "diagnostics": diagnostics,
                })
                for r in state_rows:
                    if r["sub_id"] == sub_id:
                        r.update(status="done", finished_at=now_iso(),
                                 last_error=None, updated_at=now_iso())
                write_state(state_file, state_rows)

            except Exception as exc:
                for r in state_rows:
                    if r["sub_id"] == sub_id:
                        r.update(status="failed", finished_at=now_iso(),
                                 last_error=f"{type(exc).__name__}: {exc}", updated_at=now_iso())
                write_state(state_file, state_rows)
                print(f"\n  ERROR  [{sub_name}]: {type(exc).__name__}: {exc}", flush=True)
                if is_auth_error(exc):
                    print("\n  Auth failure — re-authenticate then resume:\n"
                          "    python3 az-sizing.py --resume\n", flush=True)
                    break
                continue

    except KeyboardInterrupt:
        for r in state_rows:
            if r.get("status") == "running":
                r.update(status="failed", finished_at=now_iso(),
                         last_error="Interrupted by user (Ctrl+C)", updated_at=now_iso())
        write_state(state_file, state_rows)
        print(f"\n\n  ⚠  Interrupted by user — progress saved to {state_file}.", flush=True)
        print(f"  Resume where you left off:\n"
              f"    python3 az-sizing.py --resume --batch-size {batch_size or 25}\n",
              flush=True)

    state_rows = load_state(state_file)
    done    = sum(1 for r in state_rows if r["status"] == "done")
    failed  = sum(1 for r in state_rows if r["status"] == "failed")
    pending = sum(1 for r in state_rows if r["status"] == "pending")

    print(f"\n{SEPARATOR}")
    print(f"  Batch complete")
    print(f"  {'Done':<16}: {done}")
    print(f"  {'Failed':<16}: {failed}")
    print(f"  {'Still pending':<16}: {pending}")

    # ── Diagnostic rollup — aggregate all structured issues across the batch ─
    try:
        from collections import Counter as _Counter
        _data = {}
        if os.path.exists(results_file):
            with open(results_file) as _fh:
                _data = json.load(_fh)
        _all_diags = []
        for _payload in (_data.values() if isinstance(_data, dict) else _data):
            _all_diags.extend(_payload.get("diagnostics", []) or [])
        if _all_diags:
            _cats = _Counter(d["issue_category"] for d in _all_diags)
            _pretty = {
                "rbac_missing_sbdr":       "SBDR missing",
                "rbac_missing_law_reader": "LAW Reader missing",
                "rbac_missing_eh_reader":  "EH Reader missing",
                "rbac_missing_acr_pull":   "AcrPull missing",
                "firewall_blocked":        "firewall blocked",
                "private_endpoint_only":   "private endpoint only",
                "ssl_intercept":           "SSL intercept",
                "tenant_mismatch":         "tenant mismatch",
                "container_not_present":   "container absent",
                "not_configured":          "not configured",
            }
            print(f"\n  Issues        : {len(_all_diags)} across {len(_data)} subscription(s)")
            for _cat, _n in _cats.most_common():
                print(f"    • {_n:>3} {_pretty.get(_cat, _cat)}")
            print(f"  Full detail   :  Diagnostics sheet of the Excel workbook")
    except Exception:
        # Rollup is nice-to-have; never fail the scan over it
        pass

    if failed:
        print(f"\n  Retry failed  :  python3 az-sizing.py --resume --retry-failed")
    if pending:
        bs = batch_size or 25
        print(f"  Continue      :  python3 az-sizing.py --resume --batch-size {bs}")
    print(f"\n  Results file  :  {results_file}")
    print(f"  Summary       :  python3 az-summary.py --results {results_file}")
    print(SEPARATOR)


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(
        prog="az-sizing.py",
        description="Cortex Cloud — Azure workload sizing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Subscription workload sizing
  python3 az-sizing.py --init-state
  python3 az-sizing.py --resume --batch-size 25 --no-verify-ssl
  python3 az-sizing.py --resume --retry-failed --no-verify-ssl

  # Tenant-level SaaS user count
  python3 az-sizing.py --tenant-scan
  python3 az-sizing.py --tenant-scan --tenant-file my_tenant.json

  # Reference / summary
  python3 az-sizing.py --show-metering
  python3 az-summary.py --results azure_results.json --tenant azure_tenant.json

Permissions required beyond ARM Reader:
  Storage Blob Data Reader  — flow-log storage accounts + Event Hub capture storage
  AcrPull / AcrMetadataRead — ACR image count (data-plane)
  EventHub namespace listing — covered by ARM Reader
""",
    )
    parser.add_argument("--init-state",      action="store_true",
                        help="Discover all subscriptions and write the state file")
    parser.add_argument("--resume",          action="store_true",
                        help="Process pending subscriptions from the state file")
    parser.add_argument("--batch-size",      type=int, default=0, metavar="N",
                        help="Max subscriptions per run  (0 = all pending)")
    parser.add_argument("--retry-failed",    action="store_true",
                        help="Include failed subscriptions in the current batch")
    parser.add_argument("--state-file",      default="azure_state.jsonl", metavar="PATH",
                        help="State file  (JSONL)   [default: azure_state.jsonl]")
    parser.add_argument("--results-file",    default="azure_results.json", metavar="PATH",
                        help="Results file  (JSON)  [default: azure_results.json]")
    parser.add_argument("--heartbeat-sec",   type=int, default=10, metavar="SEC",
                        help="Progress heartbeat interval  [default: 10]")
    parser.add_argument("--sub-timeout-min", type=int, default=20, metavar="MIN",
                        help="Per-subscription time budget  [default: 20]")
    parser.add_argument("--show-metering",   action="store_true",
                        help="Print the Cortex Cloud metering reference table and exit")
    parser.add_argument("--tenant-scan",     action="store_true",
                        help="Count Entra ID users via Graph API  (requires User.Read.All)")
    parser.add_argument("--tenant-file",     default="azure_tenant.json", metavar="PATH",
                        help="Output file for tenant scan results  [default: azure_tenant.json]")
    parser.add_argument("--no-verify-ssl",   action="store_true",
                        help="Disable SSL certificate verification  "
                             "(use when a corporate SSL proxy intercepts HTTPS)")
    parser.add_argument("--preflight",       action="store_true",
                        help="Run permission checks and exit  (run before --init-state)")
    parser.add_argument("--preflight-sub",   default=None, metavar="SUB_ID",
                        help="Limit preflight to a single subscription ID or name")
    parser.add_argument("--verbose",         action="store_true",
                        help="Print every [warn] line during scan.  Default: warnings are "
                             "collected as structured diagnostics and written to "
                             "azure_results.json + the Diagnostics Excel sheet only.")

    args = parser.parse_args()

    if args.preflight:
        run_preflight(verify_ssl=not args.no_verify_ssl,
                      target_sub_id=args.preflight_sub)
        return

    if args.tenant_scan:
        run_tenant_scan(tenant_file=args.tenant_file,
                        verify_ssl=not args.no_verify_ssl)
        return

    if args.show_metering:
        print_metering_reference()
        return

    pcs_sizing_az(
        init_state      = args.init_state,
        resume          = args.resume,
        batch_size      = args.batch_size,
        retry_failed    = args.retry_failed,
        state_file      = args.state_file,
        results_file    = args.results_file,
        heartbeat_sec   = args.heartbeat_sec,
        sub_timeout_min = args.sub_timeout_min,
        verify_ssl      = not args.no_verify_ssl,
        verbose         = args.verbose,
    )


if __name__ == "__main__":
    main()