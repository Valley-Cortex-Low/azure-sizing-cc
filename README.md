# Cortex Cloud — Azure Workload Sizing Toolkit

Pre-sales sizing toolkit for Microsoft Azure. Scans all subscriptions in a tenant and produces per-subscription workload SKU breakdowns, log ingestion measurements, a structured diagnostic report, and an Excel workbook ready for sizing conversations.

---

## Quick Start

```bash
chmod +x run-sizing.sh
./run-sizing.sh
```

`run-sizing.sh` handles everything end-to-end: virtualenv setup, authentication, preflight, multi-pass scanning, tenant scan, and workbook generation. See [Environment Variables](#environment-variables) for optional overrides.

If you're behind a corporate SSL proxy (Zscaler, Palo Alto NGFW):

```bash
CC_NO_VERIFY_SSL=1 ./run-sizing.sh
```

---

## Files

| File | Purpose |
|---|---|
| `run-sizing.sh` | Shell orchestrator — runs all stages, handles token refresh and retry |
| `az-sizing.py` | Subscription scanner — produces `azure_results.json` |
| `az-summary.py` | Report generator — produces console output + `cortex_azure_sizing.xlsx` |
| `requirements-azure.txt` | Python dependencies |
| `azure_state.jsonl` | Scan progress (created at runtime) |
| `azure_results.json` | Raw scan results (created at runtime) |
| `azure_tenant.json` | Entra ID user count (created at runtime) |
| `cortex_azure_sizing.xlsx` | Final Excel workbook (created at runtime) |

---

## Environment Variables

All are optional. Set before calling `run-sizing.sh`.

| Variable | Default | Description |
|---|---|---|
| `CC_BATCH_SIZE` | `25` | Subscriptions processed per scan pass |
| `CC_MAX_RETRY` | `2` | Retry passes after the initial scan |
| `CC_SUB_TIMEOUT` | `20` | Per-subscription timeout in minutes |
| `CC_HEARTBEAT` | `10` | Heartbeat log interval in seconds |
| `CC_NO_VERIFY_SSL` | *(unset)* | Set to `1` to disable SSL verification (Zscaler / NGFW proxy) |
| `CC_SKIP_TENANT_SCAN` | *(unset)* | Set to `1` to skip the Entra ID user count |
| `CC_WORK_DIR` | Script directory | Directory for all output files |
| `CLIENT_IP` | *(auto-detected)* | Override the public IP used in firewall fix strings. Set this if `api.ipify.org` is blocked by your proxy. |

**Token refresh**: Azure tokens expire after 1 hour. For tenants with hundreds of subscriptions, a scan can run for many hours. `run-sizing.sh` verifies the token before every pass and silently refreshes it. The Python SDK (`DefaultAzureCredential`) also refreshes tokens internally mid-pass.

---

## Step-by-Step Workflow

### Step 1 — Set up the environment

`run-sizing.sh` creates the virtualenv and installs dependencies automatically on first run. To do it manually:

```bash
python3 -m venv .venv
source .venv/bin/activate       # macOS / Linux
# .venv\Scripts\activate        # Windows PowerShell
pip install -r requirements-azure.txt
```

### Step 2 — Authenticate to Azure

```bash
az login
az account list --output table   # verify the right tenant and subscriptions
```

To target a specific tenant: `az login --tenant <tenant-id>`

### Step 3 — Preflight permission check

The preflight uses the Azure role-assignment API — one ARM call per subscription, about 1 second each. It checks exactly which roles your signed-in identity holds and shows a definitive pass/fail per column without any storage-account sampling.

```bash
python3 az-sizing.py --preflight
# add --no-verify-ssl if on a corporate SSL proxy
```

**Expected output:**

```
  Subscription                     Reader   SBDR   EH-Reader  ACR-dp   LAW-Reader
  ─────────────────────────────────────────────────────────────────────────────────
  PC-Commercial-East               ✅        ✅      ✅          ⚠SSL     ❌
  NAM_Systems_Engineering          ✅        ❌      ✅          ⚠SSL     ❌
  RBAC checks derived from role-assignments API (principalId = <your-oid>).
```

Column meanings:

| Column | Role required | Notes |
|---|---|---|
| Reader | `Reader`, `Contributor`, or `Owner` | Required for all ARM enumeration and Resource Graph queries |
| SBDR | `Storage Blob Data Reader` (or Contributor/Owner with data-plane) | Required for EH capture blob + flow log blob volume measurement |
| EH-Reader | Covered by Reader | Event Hub namespace listing uses ARM Reader |
| ACR-dp | `AcrPull` or data-plane equivalent | Required for image tag/manifest counts from ACR data plane |
| LAW-Reader | `Log Analytics Reader` | Required for LAW Usage table queries. Non-blocking — scan proceeds without it; LAW sizing shows `NOT AVAILABLE` |

Fix any `❌` before scanning:

```bash
# Get your object ID (decoded from the ARM token — works even when Conditional Access blocks az ad commands)
OID=$(az account get-access-token --query accessToken -o tsv | \
  python3 -c "
import sys, json, base64
t = sys.stdin.read().strip().split('.')[1]
t += '=' * (-len(t) % 4)
print(json.loads(base64.urlsafe_b64decode(t))['oid'])
")

# Storage Blob Data Reader — covers flow logs + Event Hub capture blobs
az role assignment create --role "Storage Blob Data Reader" \
  --assignee "$OID" --scope "/subscriptions/<subscription-id>"

# AcrPull — covers ACR image tag and manifest enumeration
az role assignment create --role "AcrPull" \
  --assignee "$OID" --scope "/subscriptions/<subscription-id>"

# Log Analytics Reader — only needed if customer routes logs to LAW
az role assignment create --role "Log Analytics Reader" \
  --assignee "$OID" --scope "/subscriptions/<subscription-id>"
```

Allow 2–5 minutes for RBAC to propagate, then re-run `--preflight`.

> **Note on `Storage Blob Data Reader`**: the preflight checks for this role at subscription scope. If you have it at a narrower scope (resource group or individual storage account), the preflight will show `❌` but individual storage accounts will still be accessible during the scan. Per-account access failures are diagnosed precisely during the scan and reported in the Diagnostics sheet.

### Step 4 — Run the full scan

```bash
./run-sizing.sh
```

Or run individual stages:

```bash
./run-sizing.sh scan      # preflight + discovery + scan
./run-sizing.sh tenant    # Entra ID user count only
./run-sizing.sh summary   # regenerate Excel from existing results
./run-sizing.sh retry     # retry failed subscriptions
```

### Step 5 — Review outputs

**Console:** Three lines per subscription during the scan:

```
[1/3]  PC-Commercial-East  (0ed8c6b0-...)
    ...phase=VMs running=7 [Resource Graph]  elapsed=2s
    ...phase=FlowLogs-discover  elapsed=24s
    ...phase=LAW-sizing commercials-east-aks-logs  elapsed=48s

  Workloads:  VMs/Nodes=14  DB=1  Stor=22  →  Total WL: 18
  Logs:       Audit=NOT MEASURED  |  Flow=0.2052 GB/day  →  2/2 storage accts measured
  Status:     ⚠ 2 issues  (1 firewall blocked, 1 LAW reader missing)
```

Add `--verbose` to restore the full per-resource table output for debugging:

```bash
python3 az-sizing.py --resume --verbose
```

**End-of-scan rollup** (bottom of scan output):

```
  Issues        : 7 across 3 subscription(s)
    •   3 firewall blocked
    •   2 SBDR missing
    •   1 private endpoint only
    •   1 LAW Reader missing
  Full detail   :  Diagnostics sheet of the Excel workbook
```

**Excel workbook** — five sheets:

| Sheet | Content |
|---|---|
| Workload Summary | One row per subscription — all resource counts and workload SKU totals |
| Log Ingestion | Per-subscription audit and flow log GB/day measurements with provenance |
| Grand Total | Tenant-wide totals, Posture vs Runtime SKU sizing summary |
| Log Source Inventory | **One row per log source resource** — each flow log, EH capture path, and LAW workspace, with ✅/❌ status, category, and copy-pasteable fix command |
| Diagnostics | **One row per issue (DIAG-NNN)** — sortable/filterable by subscription, resource, category, and impact |

---

## Diagnostic System

Every access failure or measurement gap encountered during the scan becomes a structured `DiagnosticRecord`, collected per subscription and persisted in `azure_results.json`. These records drive both the Diagnostics Excel sheet and the per-subscription status lines.

### Issue categories

| Category | Meaning | Fix string |
|---|---|---|
| `SBDR missing` | `Storage Blob Data Reader` not granted to the signed-in OID at any scope covering this account | `az role assignment create --role 'Storage Blob Data Reader' ...` |
| `Firewall blocked` | SBDR is granted, but the storage account network firewall is blocking the client IP | `Add IP <detected-ip> to storage account network allowlist` |
| `Private endpoint` | Storage account has no public DNS — unreachable from outside the customer's VNet | Run the script from inside the customer's VNet |
| `LAW Reader missing` | `Log Analytics Reader` not granted on the workspace | `az role assignment create --role 'Log Analytics Reader' ...` |
| `SSL intercept` | Corporate TLS proxy intercepts the ACR data-plane connection | `--no-verify-ssl` flag, or proxy bypass for `*.azurecr.io` |
| `Not configured` | Feature exists but log routing is not set up | Customer must configure Diagnostic Settings |

### How RBAC vs firewall is distinguished

The tool calls the Azure role-assignment API once per subscription at scan start, building a local cache of every role the signed-in OID holds. When an `AuthorizationFailure` occurs on a blob call:

- If `Storage Blob Data Reader` (or equivalent) **is not** in the cache for that account's scope → `SBDR missing`
- If it **is** in the cache → the role is granted, so the only remaining cause is a firewall rule → `Firewall blocked`

This eliminates the old "could be RBAC OR firewall — check both" ambiguity.

### Log Source Inventory sheet

The Log Source Inventory shows every individual log source with its measurement result:

- **`✅ Measured N.NNNN GB/day`** — the blob container was accessible and blobs were listed. Zero bytes means the flow log is configured but produced no data in the last 7 days (inactive VNet, short retention policy, or newly enabled).
- **`❌ Not measured`** — a diagnostic record exists for this source. See the Category and Fix columns, or cross-reference the DIAG-NNN ID in the Diagnostics sheet.
- **`(shared acct)`** — multiple flow logs write to the same storage account. The GB/day shown is the whole-account total shared across all flow log specs for that account.

---

## Cortex Cloud Ingestion Architecture

Understanding how Cortex Cloud ingests Azure logs explains why the script measures what it does.

### Audit Logs

**Cortex Cloud ingests Azure audit logs via Event Hub.**

The customer routes Azure Activity Logs (and optionally AKS audit logs, Entra ID audit logs, and other control-plane events) to an Event Hub using Azure Diagnostic Settings. Cortex then reads from the Event Hub in real time.

For log sizing purposes:
- If the customer uses **Event Hub capture** (Azure stores EH messages as Avro blobs in a storage account), the script measures the blob sizes directly — the Avro files are what Cortex will process, so their size is the ground truth for ingestion volume.
- If the customer routes to a **Log Analytics Workspace** (LAW), the script queries the `Usage` table over 7 days as a proxy measurement.
- The EH capture path is primary. LAW is a secondary reference.

**Cortex XSIAM documentation — Audit Logs (Azure Event Hub):**
[Ingest Logs from Microsoft Azure Event Hub](https://docs-cortex.paloaltonetworks.com/r/Cortex-XSIAM/Cortex-XSIAM-Documentation/Ingest-Logs-from-Microsoft-Azure-Event-Hub)

Audit log DataTypes measured from LAW (when LAW path is available):
`AzureActivity`, `AuditLogs`, `MicrosoftGraphActivityLogs`, `SigninLogs`, `ADFSSignInLogs`, `ProvisioningLogs`, `AKSAudit`, `AKSAuditAdmin`, `AKSControlPlane`

### Flow Logs

**Cortex Cloud ingests Azure flow logs via a dedicated Azure Function that reads blobs directly from storage.**

Azure Network Watcher writes flow log blobs to a storage account. Cortex deploys an Azure Function that periodically reads new blobs from the two well-known containers:

| Container | Applies to |
|---|---|
| `insights-logs-flowlogflowevent` | VNet flow logs (current, replaces NSG flow logs from June 2025) |
| `insights-logs-networksecuritygroupflowevent` | NSG flow logs (legacy, retired June 30 2025 — existing deployments may still write here) |

The script discovers all enabled flow logs in Network Watcher, then lists blobs using the exact path prefix that Azure writes:

```
VNet flow logs:
  flowLogResourceID=/{SUB_ID}_{NETWORK-WATCHER-RG}/{WATCHER-NAME}_{FLOWLOG-NAME}/y=YYYY/m=MM/d=DD/...

NSG flow logs:
  resourceId=/SUBSCRIPTIONS/{SUB}/RESOURCEGROUPS/{RG}/PROVIDERS/MICROSOFT.NETWORK/NETWORKSECURITYGROUPS/{NSG}/y=YYYY/m=MM/d=DD/...
```

**Important**: `Traffic Analytics` (LAW tables `NTANetAnalytics`, `AzureNetworkAnalytics_CL`) is a separate Microsoft aggregation pipeline — it does not reflect what Cortex ingests. The script measures raw blob sizes, not Traffic Analytics volumes.

**Cortex XSIAM documentation — Flow Logs (Azure Network Watcher):**
[Ingest network flow logs from Microsoft Azure Network Watcher](https://docs-cortex.paloaltonetworks.com/r/Cortex-XSIAM/Cortex-XSIAM-Documentation/Ingest-network-flow-logs-from-Microsoft-Azure-Network-Watcher)

---

## API Strategy

The script uses a hybrid API approach. Each API is chosen because it is the only one that exposes the required data for that resource type.

| API | Resources | Why |
|---|---|---|
| **Azure Resource Graph** | Running VMs, Storage Accounts, Cosmos DB, Azure SQL | Single query per type; power state available in RG since 2023. Reader role sufficient. ARM fallback retained. |
| **ARM SDK / REST** | AKS nodes, ARO nodes, ACI, Container Apps, Function Apps, ACR management plane, Event Hub, Network Watcher flow logs | Sub-resource counts and service configuration not indexed by Resource Graph. Function App `/functions` is not an RG resource type. |
| **ACR data plane** | Image tags and manifests | ARM management plane only knows the registry exists. Tag/manifest counts require `api.azurecr.io`. |
| **Azure Blob Storage data plane** | Audit log volume (EH capture blobs), flow log volume (Network Watcher blobs) | Actual GB/day lives in the blobs. No ARM or Resource Graph surface exposes blob byte counts. |
| **Log Analytics Query API** | Audit log volume (LAW path) | Customers routing logs to LAW instead of EH. Queries `Usage` table over 7 days. Separate OAuth2 token scope (`api.loganalytics.io`). |
| **Azure Role Assignments API** | Preflight RBAC check, RBAC-vs-firewall attribution | `principalId` filter returns all roles for the signed-in OID across a subscription. One ARM call per sub. Used to distinguish missing SBDR (RBAC failure) from AuthorizationFailure caused by storage firewall. |
| **Microsoft Graph** | Entra ID user counts | Directory objects only. Microsoft Graph ≠ Azure Resource Graph — separate APIs, separate token scopes. |

---

## Cortex Cloud Sizing Fundamentals

### How workloads are measured

| Workload category | Rate | What is counted | Excluded | API |
|---|---|---|---|---|
| **Virtual Machines** | 1 = 1 WL | Running VMs (`PowerState/running`) | Stopped, deallocated, deleted | Resource Graph `powerState` (ARM fallback) |
| **AKS nodes** | 1 = 1 WL | Agent-pool node count | Control-plane nodes | ARM `agent_pools.list()` |
| **ARO nodes** | 1 = 1 WL | 3 masters (fixed) + worker profile counts | — | ARM `open_shift_clusters.list()` |
| **ACI containers** | 10 = 1 WL | Containers in succeeded groups | Failed groups | ARM `container_groups.list()` |
| **Container Apps** | 10 = 1 WL | Active revision replicas × containers | Inactive revisions | ARM `container_apps_revisions.list()` |
| **Azure Functions** | 25 = 1 WL | Actual function count per app. Containerized/Flex apps floor-counted at 1. | Logic Apps Standard, stopped apps | ARM REST `/sites/{app}/functions` |
| **Azure SQL** | 2 DBs = 1 WL | All databases per server | `master` | Resource Graph (ARM fallback) |
| **Cosmos DB** | 2 = 1 WL | Accounts with `publicNetworkAccess = Enabled` | Private-only | Resource Graph (ARM fallback) |
| **Storage Accounts** | 10 = 1 WL | All storage accounts | — | Resource Graph (ARM fallback) |
| **ACR images** | 10 = 1 WL *(after free allowance)* | Image tags (primary) + manifests (informational) | Within free allowance | ACR data plane |
| **SaaS Users** | 10 = 1 WL *(tenant-level)* | Enabled Member + Guest users | Disabled, service principals | Microsoft Graph `/v1.0/users/$count` |

### Container image free allowance

```
free_allowance  = (vm_running + aks_nodes + aro_nodes) × 10
                + floor((aci_containers + container_app_containers) / 10) × 10
net_billable    = max(0, acr_tags − free_allowance)
image_workloads = ceil(net_billable / 10)
```

### Workload and log ingestion formula

| Resource | Contribution |
|---|---|
| VMs + AKS nodes + ARO nodes | `count × 1` |
| ACI + Container App containers | `⌈count / 10⌉` |
| Azure Functions | `⌈count / 25⌉` |
| Azure SQL + Cosmos DB | `⌈count / 2⌉` |
| Storage Accounts | `⌈count / 10⌉` |
| ACR images (net) | `⌈net / 10⌉` |
| Entra ID users | `⌈count / 10⌉` |
| **Total Required Workload Licenses** | **Sum of all rows** |
| **Included daily log ingestion** | **Total ÷ 50 GB/day** |

### Log ingestion sizing

| Log type | Posture SKU | Runtime SKU |
|---|---|---|
| Audit Logs | ✅ Included | ✅ Included |
| Flow Logs | ❌ Not supported | ✅ Included |

**Measurement methods:**

| Log type | Path | Method |
|---|---|---|
| **Audit Logs** | Event Hub capture (primary) | EH namespaces → capture-enabled hubs → Avro blob listing → 7-day avg |
| **Audit Logs** | Log Analytics Workspace | Diag settings → LAW workspace → `Usage` table KQL query → 7-day avg |
| **Flow Logs** | Blob containers (primary) | Network Watcher flow log configs → blob listing by exact path prefix → 7-day avg |
| **Flow Logs** | Log Analytics Workspace | Traffic Analytics config → LAW — **NOTE: not the Cortex ingestion path, reference only** |

**Posture vs Runtime:** these SKUs are not additive. Runtime includes everything Posture measures plus Flow Logs. A customer buying Runtime does not also need Posture separately.

**Dual-sink:** if both EH/blob and LAW are measured for the same log type, output flags `⚠ DUAL-SINK`. Use the individual source value for single-pipeline sizing.

### Required permissions

| Role | Scope | What it covers | If missing |
|---|---|---|---|
| `Reader` (or Contributor/Owner) | Each subscription | ARM enumeration + Resource Graph queries | Subscription scan fails entirely |
| `Storage Blob Data Reader` | Each subscription (or narrower) | EH capture blob + flow-log blob volume measurement | `NOT AVAILABLE` on all blob-based log sizing; per-account access failures diagnosed in Diagnostics sheet |
| `AcrPull` | Each subscription | ACR image tag and manifest counts | ACR image count = 0; if blocked by ACR network firewall, output flags `⚠ FIREWALL BLOCKED` |
| `Log Analytics Reader` | Each subscription | LAW `Usage` table queries. **Optional** — only if customer routes logs to LAW | `NOT AVAILABLE` on LAW log sizing only; scan proceeds |
| `User.Read.All` | Entra ID tenant | Entra ID user count for SaaS workloads | Tenant scan runs, Graph returns 403, SaaS workloads = 0, Grand Total flagged `⚠ understated` |

> **Resource Graph** requires no additional role beyond standard `Reader`. The same credential works for both ARM and Resource Graph queries.

---

## Interpreting ACR image counts

ACR tag count = 0 can mean three different things. The toolkit distinguishes all three:

| Output | Meaning | Action |
|---|---|---|
| `ACR images (tags — All scan mode) 0` with no warning | Genuine zero — no images exist | None |
| `ACR images (tags — ⚠ FIREWALL BLOCKED — count not reliable)` | All registries blocked the client IP at the data plane — count is unknown, not zero | Run from a VM inside the registry's VNet, or add the client IP in Portal → Container Registry → Networking |
| `ACR images (tags — ⚠ PARTIAL — some registries blocked)` | Some registries blocked; count is understated | Same fix for blocked registries |

---

## Enterprise Blockers and Remediation

| Scenario | Preflight catches it | Error / symptom | Remediation |
|---|---|---|---|
| `Reader` not assigned | ✅ Yes — `Reader: ❌` | Subscription scan fails entirely | Assign `Reader` at subscription scope |
| `Storage Blob Data Reader` not assigned | ✅ Yes — `SBDR: ❌` | Diagnostics sheet: `SBDR missing` with exact `az role assignment create` fix | Assign `Storage Blob Data Reader` at subscription or narrower scope |
| Storage account network firewall blocks runner IP | ❌ No — but diagnosed exactly during scan | Diagnostics sheet: `Firewall blocked` with `Add IP <ip> to allowlist` | Add the detected IP in Portal → Storage Account → Networking, or set `CLIENT_IP` env var |
| `AcrPull` not assigned | ✅ Yes — `ACR-dp: ❌` | ACR image count = 0 | Assign `AcrPull` at subscription scope |
| ACR network firewall | ✅ Partially — SSL or connection refused | Resource table: `⚠ FIREWALL BLOCKED` | Add client IP in Portal → Container Registry → Networking, or run from inside the VNet |
| `Log Analytics Reader` not assigned | ✅ Yes — `LAW-Reader: ❌` | LAW log sizing = `NOT AVAILABLE`. Non-blocking. | Assign `Log Analytics Reader`. Not required if customer uses EH/blob exclusively. |
| `User.Read.All` not granted | ✅ Yes — Graph check `❌ FAIL` | SaaS workloads = 0; Grand Total flagged `⚠ understated` | Grant `User.Read.All` app permission with admin consent |
| Private endpoint storage (no public DNS) | ❌ No — but diagnosed exactly | Diagnostics sheet: `Private endpoint` with VNet note | Run script from inside the customer's VNet |
| SSL inspection proxy | ✅ Yes — `ACR-dp: ⚠SSL` | `CERTIFICATE_VERIFY_FAILED` on ACR calls | Set `CC_NO_VERIFY_SSL=1` |
| Conditional Access blocks identity lookups | ❌ No | `AADSTS53003` at authentication | Exclude sizing identity from CA policy; OID is decoded from JWT token (no `az ad` call needed) |
| Cross-subscription LAW workspace | ❌ No | `NOT AVAILABLE — cross-subscription workspace` | Verify identity has `Log Analytics Reader` on the workspace's home subscription |
| Resource Graph blocked by deny assignment | ❌ No | `[warn] Resource Graph` — ARM fallback activates | ARM fallback runs automatically; result is correct, just slower |
| Stale Event Hub in diagnostic settings | ❌ No | `ResourceGroupNotFound` or `ParentResourceNotFound` on EH namespace | Non-blocking — stale customer config; customer should delete the orphaned diagnostic setting |
| Subscription timeout | ❌ No | `TimeoutError`, subscription marked `failed` | Increase `CC_SUB_TIMEOUT` to 45–60 min |
| VNet flow log blob prefix mismatch (legacy tools) | N/A — fixed in v5 | Would have silently returned 0.0000 GB/day | Use v5+ of this toolkit which constructs the correct `flowLogResourceID=/` prefix |
| NSG flow logs retired June 2025 | ❌ No — informational | Script checks both `insights-logs-flowlogflowevent` and `insights-logs-networksecuritygroupflowevent` | No action needed — both containers are scanned automatically |

---

## Output Files Reference

| File | Created by | Contents |
|---|---|---|
| `azure_state.jsonl` | `az-sizing.py --init-state` | One JSON line per subscription: sub ID, name, status (`pending`/`done`/`failed`), timestamps |
| `azure_results.json` | `az-sizing.py --resume` | One key per subscription ID: raw resource counts, workload SKUs, log ingestion measurements, `diagnostics` array (one `DiagnosticRecord` per issue), `flow_log_inventory` and `eh_capture_inventory` lists |
| `azure_tenant.json` | `az-sizing.py --tenant-scan` | Tenant ID, Entra ID user counts (member + guest), tenant-level diagnostic settings |
| `cortex_azure_sizing.xlsx` | `az-summary.py` | Five-sheet workbook — see Excel Workbook section above |
| `sizing-YYYYMMDD-HHMMSS.log` | `run-sizing.sh` | Full timestamped output of every stage |
