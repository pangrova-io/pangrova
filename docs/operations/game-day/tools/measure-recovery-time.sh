#!/usr/bin/env bash
#
# measure-recovery-time.sh — Recovery time measurement tool for Pangrova game days.
#
# Times HTTP responses to asynchronous SQL submissions and records the interval
# from a failed submission probe to the next HTTP 200. Does not consume results.
#
# Usage:
#   ./measure-recovery-time.sh baseline --endpoint URL [--token TOKEN] --query SQL --interval DURATION --output FILE
#   ./measure-recovery-time.sh measure  --endpoint URL [--token TOKEN] --probe-query SQL --timeout DURATION --output FILE
#   ./measure-recovery-time.sh report   --baseline FILE --recovery FILE [--rto-coordinator N] [--rto-worker N] [--rto-metadata N]
#   ./measure-recovery-time.sh watch    --endpoint URL [--token TOKEN] --interval DURATION
#
# Commands:
#   baseline    Record HTTP submission latency and HTTP 200 rate
#   measure     Watch for HTTP 200 after a failed submission probe
#   report      Compare one observed submission-recovery interval with thresholds
#   watch       Live tail of endpoint health (no output file; Ctrl-C to stop)
#
# Environment variables (override CLI flags):
#   PANGROVA_ENDPOINT    Coordinator base URL (e.g., https://coordinator:8080)
#   PANGROVA_TOKEN       Bearer token for authentication
#
# Requires: curl, python3 (for JSON parsing in report command)
#

set -euo pipefail

readonly SCRIPT_NAME="$(basename "$0")"
readonly VERSION="1.1.0"

# Colors for terminal output
readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[1;33m'
readonly CYAN='\033[0;36m'
readonly BOLD='\033[1m'
readonly NC='\033[0m' # No Color

usage() {
    cat <<EOF
${BOLD}${SCRIPT_NAME} v${VERSION}${NC} — Pangrova game day SQL submission measurement

${BOLD}USAGE${NC}
  ${SCRIPT_NAME} <command> [options]

${BOLD}COMMANDS${NC}
  baseline    Record baseline HTTP submission metrics (run before injecting fault)
  measure     Measure failed-probe-to-HTTP-200 interval (run immediately after fault)
  report      Report HTTP submission metrics; does not certify query RTO
  watch       Live health tail (Ctrl-C to stop; no output file)

${BOLD}GLOBAL OPTIONS${NC}
  --endpoint URL      Pangrova coordinator base URL (required; or set \$PANGROVA_ENDPOINT)
  --token TOKEN       Bearer token for auth (or set \$PANGROVA_TOKEN)
  --interval DURATION Probe interval: Ns/Nm/Nh (default: 1s)

${BOLD}OPTIONS: baseline${NC}
  --query SQL         SQL to submit for HTTP response timing (required)
  --duration DURATION How long to collect baseline data (default: 60s)
  --output FILE       Output JSON file path (required)

${BOLD}OPTIONS: measure${NC}
  --probe-query SQL   SQL submission probe (default: "SELECT 1")
  --timeout DURATION  Max wait for an observed submission outage to clear (default: 120s)
  --output FILE       Output JSON file path (required)

${BOLD}OPTIONS: report${NC}
  --baseline FILE         Baseline JSON file (required)
  --recovery FILE         Recovery JSON file (required)
  --rto-coordinator N     Submission-recovery comparator for coordinator failover (seconds) (default: 30)
  --rto-worker N          Submission-recovery comparator for worker recovery (seconds) (default: 60)
  --rto-metadata N        Submission-recovery comparator for metadata partition (seconds) (default: 120)

${BOLD}EXAMPLES${NC}
  # Step 1: Record 60s of healthy baseline
  ${SCRIPT_NAME} baseline \\
    --endpoint https://coordinator:8080 \\
    --token "\${PANGROVA_TOKEN}" \\
    --query "SELECT count(*) FROM benchmark.lineitem" \\
    --duration 60s \\
    --output /tmp/gd-001-baseline.json

  # Step 2: Inject fault (in another terminal), then immediately run:
  ${SCRIPT_NAME} measure \\
    --endpoint https://coordinator:8080 \\
    --token "\${PANGROVA_TOKEN}" \\
    --timeout 120s \\
    --output /tmp/gd-001-recovery.json

  # Step 3: Generate report
  ${SCRIPT_NAME} report \\
    --baseline /tmp/gd-001-baseline.json \\
    --recovery /tmp/gd-001-recovery.json \\
    --rto-coordinator 30

  # Live health monitoring during a game day
  ${SCRIPT_NAME} watch \\
    --endpoint https://coordinator:8080 \\
    --token "\${PANGROVA_TOKEN}" \\
    --interval 2s

EOF
}

log_info()  { echo -e "${GREEN}[INFO]${NC}  $(date '+%H:%M:%S.%3N') $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $(date '+%H:%M:%S.%3N') $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $(date '+%H:%M:%S.%3N') $*"; }
log_event() { echo -e "${CYAN}[EVENT]${NC} $(date '+%H:%M:%S.%3N') $*"; }

# Parse duration string (e.g., "30s", "2m", "1h") to integer seconds
parse_duration() {
    local input="$1"
    if [[ "$input" =~ ^[0-9]+$ ]]; then
        echo "$input"
        return
    fi
    local value="${input%[smhSMH]}"
    local unit="${input: -1}"
    case "${unit,,}" in
        s) echo "$value" ;;
        m) echo $((value * 60)) ;;
        h) echo $((value * 3600)) ;;
        *) log_error "Cannot parse duration: '$input' (use Ns, Nm, or Nh)"; exit 1 ;;
    esac
}

# Current time in milliseconds (portable: date then python3 fallback)
now_ms() {
    date +%s%3N 2>/dev/null || python3 -c 'import time; print(int(time.time()*1000))'
}

# Submit SQL asynchronously and discard the response body; do not fetch results.
# Prints "<http_code>:<latency_ms>".
probe_endpoint() {
    local endpoint="$1"
    local query="$2"
    local token="${3:-}"

    local start_ms
    start_ms=$(now_ms)

    local auth_header=""
    if [[ -n "$token" ]]; then
        auth_header="-H \"Authorization: Bearer ${token}\""
    fi

    local http_code
    # Use /v1/query/submit — the Pangrova REST query submission path
    http_code=$(eval curl -s -o /dev/null -w '"%{http_code}"' \
        --max-time 5 \
        -X POST "'${endpoint}/v1/query/submit'" \
        -H "'Content-Type: application/json'" \
        ${auth_header} \
        -d "'{\"query\":\"${query}\"}'" 2>/dev/null || echo '"000"')

    # Strip surrounding quotes that some curl versions add
    http_code="${http_code//\"/}"

    local end_ms
    end_ms=$(now_ms)
    local latency_ms=$((end_ms - start_ms))

    echo "${http_code}:${latency_ms}"
}

# Probe the health endpoint (no auth needed, no SQL required)
probe_health() {
    local endpoint="$1"
    local http_code
    http_code=$(curl -s -o /dev/null -w "%{http_code}" \
        --max-time 5 \
        "${endpoint}/v1/health" 2>/dev/null || echo "000")
    echo "$http_code"
}

# ─── Command: baseline ─────────────────────────────────────────────────────────

cmd_baseline() {
    local endpoint="${PANGROVA_ENDPOINT:-}"
    local token="${PANGROVA_TOKEN:-}"
    local query=""
    local interval="1s"
    local duration="60s"
    local output=""

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --endpoint)  endpoint="$2";  shift 2 ;;
            --token)     token="$2";     shift 2 ;;
            --query)     query="$2";     shift 2 ;;
            --interval)  interval="$2";  shift 2 ;;
            --duration)  duration="$2";  shift 2 ;;
            --output)    output="$2";    shift 2 ;;
            *) log_error "Unknown option: $1"; exit 1 ;;
        esac
    done

    [[ -z "$endpoint" ]] && { log_error "--endpoint required (or set \$PANGROVA_ENDPOINT)"; exit 1; }
    [[ -z "$query"    ]] && { log_error "--query required"; exit 1; }
    [[ -z "$output"   ]] && { log_error "--output required"; exit 1; }

    local interval_sec duration_sec
    interval_sec=$(parse_duration "$interval")
    duration_sec=$(parse_duration "$duration")

    log_info "Recording baseline: ${duration_sec}s at ${interval_sec}s intervals"
    log_info "Endpoint : ${endpoint}"
    log_info "Query    : ${query}"
    [[ -n "$token" ]] && log_info "Auth     : Bearer token provided"

    local start_time success_count total_count total_latency
    start_time=$(date +%s)
    success_count=0; total_count=0; total_latency=0
    local -a latencies=()
    local -a samples=()

    while true; do
        local now; now=$(date +%s)
        local elapsed=$(( now - start_time ))
        [[ $elapsed -ge $duration_sec ]] && break

        local result; result=$(probe_endpoint "$endpoint" "$query" "$token")
        local http_code="${result%%:*}"
        local latency="${result##*:}"

        total_count=$((total_count + 1))

        if [[ "$http_code" == "200" ]]; then
            success_count=$((success_count + 1))
            total_latency=$((total_latency + latency))
            latencies+=("$latency")
            echo -ne "\r  [${elapsed}s/${duration_sec}s] Probe ${total_count}: ${GREEN}OK${NC} ${latency}ms    "
        else
            echo -ne "\r  [${elapsed}s/${duration_sec}s] Probe ${total_count}: ${RED}FAIL${NC} HTTP ${http_code}    "
        fi

        samples+=("{\"t\":${now},\"s\":${http_code},\"ms\":${latency}}")
        sleep "$interval_sec"
    done

    echo ""

    # Compute stats
    local avg_latency=0
    local p50=0 p95=0 p99=0
    if [[ $success_count -gt 0 ]]; then
        avg_latency=$((total_latency / success_count))

        # Sort latencies and compute percentiles
        if [[ ${#latencies[@]} -gt 0 ]]; then
            local sorted
            sorted=$(printf '%s\n' "${latencies[@]}" | sort -n)
            local count=${#latencies[@]}

            p50=$(echo "$sorted" | awk -v n="$count" 'NR==int(n*0.50)+1{print}')
            p95=$(echo "$sorted" | awk -v n="$count" 'NR==int(n*0.95)+1{print}')
            p99=$(echo "$sorted" | awk -v n="$count" 'NR==int(n*0.99)+1{print}')
        fi
    fi

    local success_rate=0
    [[ $total_count -gt 0 ]] && success_rate=$((success_count * 100 / total_count))

    log_info "Baseline complete:"
    log_info "  Submission probes : ${success_count}/${total_count} HTTP 200 (${success_rate}%)"
    log_info "  Submission latency : avg=${avg_latency}ms  p50=${p50}ms  p95=${p95}ms  p99=${p99}ms"

    local samples_json
    samples_json=$(printf '%s,' "${samples[@]}" | sed 's/,$//')

    cat > "$output" <<JSONEOF
{
  "type": "baseline",
  "version": "${VERSION}",
  "endpoint": "${endpoint}",
  "query": "${query}",
  "start_time": ${start_time},
  "duration_sec": ${duration_sec},
  "interval_sec": ${interval_sec},
  "total_probes": ${total_count},
  "successful_probes": ${success_count},
  "success_rate_pct": ${success_rate},
  "latency_ms": {
    "avg": ${avg_latency},
    "p50": ${p50},
    "p95": ${p95},
    "p99": ${p99}
  },
  "samples": [${samples_json}]
}
JSONEOF

    log_info "Baseline written to: ${output}"
}

# ─── Command: measure ──────────────────────────────────────────────────────────

cmd_measure() {
    local endpoint="${PANGROVA_ENDPOINT:-}"
    local token="${PANGROVA_TOKEN:-}"
    local probe_query="SELECT 1"
    local timeout="120s"
    local interval="1s"
    local output=""

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --endpoint)    endpoint="$2";    shift 2 ;;
            --token)       token="$2";       shift 2 ;;
            --probe-query) probe_query="$2"; shift 2 ;;
            --timeout)     timeout="$2";     shift 2 ;;
            --interval)    interval="$2";    shift 2 ;;
            --output)      output="$2";      shift 2 ;;
            *) log_error "Unknown option: $1"; exit 1 ;;
        esac
    done

    [[ -z "$endpoint" ]] && { log_error "--endpoint required (or set \$PANGROVA_ENDPOINT)"; exit 1; }
    [[ -z "$output"   ]] && { log_error "--output required"; exit 1; }

    local interval_sec timeout_sec
    interval_sec=$(parse_duration "$interval")
    timeout_sec=$(parse_duration "$timeout")

    log_event "Starting submission recovery measurement (timeout: ${timeout_sec}s)"
    log_info "Endpoint    : ${endpoint}"
    log_info "Probe query : ${probe_query}"
    log_info "Interval    : ${interval_sec}s"
    [[ -n "$token" ]] && log_info "Auth        : Bearer token provided"

    local start_time; start_time=$(date +%s)
    local first_failure_time="" first_success_time=""
    local failure_count=0 total_count=0
    local -a samples=()
    local timed_out=false

    while true; do
        local now; now=$(date +%s)
        local elapsed=$(( now - start_time ))

        if [[ $elapsed -ge $timeout_sec ]]; then
            echo ""
            log_warn "Timeout reached (${timeout_sec}s) — endpoint did not recover"
            timed_out=true
            break
        fi

        local result; result=$(probe_endpoint "$endpoint" "$probe_query" "$token")
        local http_code="${result%%:*}"
        local latency="${result##*:}"

        total_count=$((total_count + 1))
        samples+=("{\"t\":${now},\"s\":${http_code},\"ms\":${latency}}")

        if [[ "$http_code" != "200" ]]; then
            failure_count=$((failure_count + 1))
            [[ -z "$first_failure_time" ]] && first_failure_time=$now
            echo -ne "\r  [${elapsed}s] ${RED}DOWN${NC}  HTTP ${http_code} — ${failure_count} failed probes    "
        else
            if [[ $failure_count -gt 0 && -z "$first_success_time" ]]; then
                first_success_time=$now
                local recovery_sec=$(( first_success_time - first_failure_time ))
                echo ""
                log_event "${GREEN}FIRST HTTP 200${NC} after ${recovery_sec}s down (${failure_count} failed probes)"
                log_info "Checking submission-probe stability (10s window)..."

                # Stability confirmation: probe for 10 more seconds
                local confirm_start; confirm_start=$(date +%s)
                local confirm_failures=0 confirm_total=0
                while [[ $(( $(date +%s) - confirm_start )) -lt 10 ]]; do
                    sleep "$interval_sec"
                    local cr; cr=$(probe_endpoint "$endpoint" "$probe_query" "$token")
                    local cc="${cr%%:*}"
                    confirm_total=$((confirm_total + 1))
                    [[ "$cc" != "200" ]] && confirm_failures=$((confirm_failures + 1))
                done

                if [[ $confirm_failures -eq 0 ]]; then
                    log_info "Submission-probe stability confirmed: ${confirm_total} probes clean in 10s window"
                else
                    log_warn "Submission probes UNSTABLE: ${confirm_failures}/${confirm_total} failures in 10s window"
                fi
                break
            elif [[ $failure_count -eq 0 ]]; then
                echo -ne "\r  [${elapsed}s] ${GREEN}UP${NC}   ${latency}ms — waiting for a failed submission probe...    "
            fi
        fi

        sleep "$interval_sec"
    done

    echo ""

    local recovery_time_sec="null"
    if [[ -n "$first_success_time" && -n "$first_failure_time" ]]; then
        recovery_time_sec=$(( first_success_time - first_failure_time ))
    elif [[ -n "$first_success_time" ]]; then
        recovery_time_sec=$(( first_success_time - start_time ))
    fi

    local ft="${first_failure_time:-null}"
    local st="${first_success_time:-null}"
    local samples_json
    samples_json=$(printf '%s,' "${samples[@]}" | sed 's/,$//')

    cat > "$output" <<JSONEOF
{
  "type": "recovery",
  "version": "${VERSION}",
  "endpoint": "${endpoint}",
  "probe_query": "${probe_query}",
  "start_time": ${start_time},
  "first_failure_time": ${ft},
  "first_success_time": ${st},
  "recovery_time_sec": ${recovery_time_sec},
  "total_probes": ${total_count},
  "failed_probes": ${failure_count},
  "timeout_sec": ${timeout_sec},
  "timed_out": ${timed_out},
  "samples": [${samples_json}]
}
JSONEOF

    log_info "Results written to: ${output}"
    if [[ "$recovery_time_sec" != "null" ]]; then
        log_event "Submission recovery interval: ${BOLD}${recovery_time_sec}s${NC}"
    else
        log_warn "No failed-probe-to-HTTP-200 interval observed within ${timeout_sec}s"
    fi
}

# ─── Command: report ───────────────────────────────────────────────────────────

cmd_report() {
    local baseline_file="" recovery_file=""
    local rto_coordinator=30 rto_worker=60 rto_metadata=120

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --baseline)         baseline_file="$2"; shift 2 ;;
            --recovery)         recovery_file="$2"; shift 2 ;;
            --rto-coordinator)  rto_coordinator="$2"; shift 2 ;;
            --rto-worker)       rto_worker="$2"; shift 2 ;;
            --rto-metadata)     rto_metadata="$2"; shift 2 ;;
            *) log_error "Unknown option: $1"; exit 1 ;;
        esac
    done

    [[ -z "$baseline_file" ]] && { log_error "--baseline required"; exit 1; }
    [[ -z "$recovery_file" ]] && { log_error "--recovery required"; exit 1; }
    [[ -f "$baseline_file" ]] || { log_error "Baseline file not found: ${baseline_file}"; exit 1; }
    [[ -f "$recovery_file" ]] || { log_error "Recovery file not found: ${recovery_file}"; exit 1; }

    python3 - "$baseline_file" "$recovery_file" \
              "$rto_coordinator" "$rto_worker" "$rto_metadata" <<'PYEOF'
import json, sys, datetime

baseline_file, recovery_file = sys.argv[1], sys.argv[2]
rto_coord  = int(sys.argv[3])
rto_worker = int(sys.argv[4])
rto_meta   = int(sys.argv[5])

b = json.load(open(baseline_file))
r = json.load(open(recovery_file))

GREEN, RED, YELLOW, BOLD, NC = '\033[0;32m', '\033[0;31m', '\033[1;33m', '\033[1m', '\033[0m'

def rto_line(label, target, actual):
    if actual is None:
        return f"  {label:<35} target={target}s  {RED}FAIL{NC} (no submission recovery interval observed)"
    delta = actual - target
    status = f"{GREEN}PASS{NC}" if actual <= target else f"{RED}FAIL{NC}"
    sign   = f"+{delta}" if delta > 0 else str(delta)
    return f"  {label:<35} target={target}s  {status} actual={actual}s ({sign}s)"

rt = r.get("recovery_time_sec")
if rt == "null" or rt is None:
    rt = None

print()
print(f"{'═'*60}")
print(f"  GAME DAY — SQL SUBMISSION REPORT")
print(f"{'═'*60}")
print()

# Baseline
lat = b.get("latency_ms", {})
print(f"{BOLD}HTTP SUBMISSION BASELINE{NC}")
print(f"  Endpoint   : {b['endpoint']}")
print(f"  Query      : {b['query']}")
print(f"  Duration   : {b['duration_sec']}s  ({b['total_probes']} probes)")
print(f"  HTTP 200   : {b['successful_probes']}/{b['total_probes']} ({b['success_rate_pct']}%)")
print(f"  Submit latency: avg={lat.get('avg','?')}ms  p50={lat.get('p50','?')}ms  p95={lat.get('p95','?')}ms  p99={lat.get('p99','?')}ms")
print()

# Recovery
print(f"{BOLD}SUBMISSION RECOVERY MEASUREMENT{NC}")
print(f"  Probe query  : {r['probe_query']}")
print(f"  Total probes : {r['total_probes']}")
print(f"  Failed probes: {r['failed_probes']}")
print(f"  Timed out    : {r['timed_out']}")
if rt is not None:
    print(f"  Submission recovery interval: {BOLD}{rt}s{NC}")
    # Down window = first_failure to first_success
    if r.get('first_failure_time') and r.get('first_success_time'):
        ft = datetime.datetime.fromtimestamp(r['first_failure_time']).strftime('%H:%M:%S')
        st = datetime.datetime.fromtimestamp(r['first_success_time']).strftime('%H:%M:%S')
        print(f"  Failed probe to HTTP 200: {ft} → {st}")
else:
    print(f"  Submission recovery interval: {RED}NO INTERVAL OBSERVED within {r['timeout_sec']}s{NC}")
print()

# Submission recovery threshold comparison
print(f"{BOLD}SUBMISSION RECOVERY THRESHOLD COMPARISON{NC}")
print("  One interval is compared with each threshold; this is not query RTO.")
print(rto_line("Coordinator failover",  rto_coord,  rt))
print(rto_line("Worker crash recovery", rto_worker, rt))
print(rto_line("Metadata partition",    rto_meta,   rt))
print()

# Verdict
if rt is not None and rt <= min(rto_coord, rto_worker, rto_meta):
    print(f"  Verdict: {GREEN}{BOLD}PASS — Observed interval met all comparison thresholds{NC}")
elif rt is not None:
    print(f"  Verdict: {YELLOW}{BOLD}PARTIAL — Observed interval exceeded one or more thresholds{NC}")
else:
    print(f"  Verdict: {RED}{BOLD}FAIL — No submission recovery interval observed{NC}")
print()
print(f"{'═'*60}")
PYEOF
}

# ─── Command: watch ────────────────────────────────────────────────────────────

cmd_watch() {
    local endpoint="${PANGROVA_ENDPOINT:-}"
    local token="${PANGROVA_TOKEN:-}"
    local interval="2s"

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --endpoint) endpoint="$2"; shift 2 ;;
            --token)    token="$2";    shift 2 ;;
            --interval) interval="$2"; shift 2 ;;
            *) log_error "Unknown option: $1"; exit 1 ;;
        esac
    done

    [[ -z "$endpoint" ]] && { log_error "--endpoint required (or set \$PANGROVA_ENDPOINT)"; exit 1; }

    local interval_sec
    interval_sec=$(parse_duration "$interval")

    log_info "Live health watch: ${endpoint}  (Ctrl-C to stop)"
    echo ""

    local prev_state="unknown"
    local down_since=""

    while true; do
        local health_code; health_code=$(probe_health "$endpoint")

        local ts; ts=$(date '+%H:%M:%S')
        if [[ "$health_code" == "200" ]]; then
            local result; result=$(probe_endpoint "$endpoint" "SELECT 1" "$token")
            local http_code="${result%%:*}"
            local latency="${result##*:}"

            if [[ "$prev_state" != "up" ]]; then
                echo ""
                if [[ -n "$down_since" ]]; then
                    local now_s; now_s=$(date +%s)
                    local outage=$(( now_s - down_since ))
                    log_event "${GREEN}RECOVERED${NC} after ${outage}s"
                    down_since=""
                fi
            fi
            echo -ne "\r  ${ts}  ${GREEN}UP${NC}  health=200  submit=${http_code}  submit_latency=${latency}ms    "
            prev_state="up"
        else
            if [[ "$prev_state" != "down" ]]; then
                echo ""
                log_event "${RED}HEALTH FAILED${NC} — HTTP ${health_code}"
                down_since=$(date +%s)
            fi
            local elapsed=""
            if [[ -n "$down_since" ]]; then
                local now_s; now_s=$(date +%s)
                elapsed=" (down $(( now_s - down_since ))s)"
            fi
            echo -ne "\r  ${ts}  ${RED}DOWN${NC} health=${health_code}${elapsed}    "
            prev_state="down"
        fi

        sleep "$interval_sec"
    done
}

# ─── Main ──────────────────────────────────────────────────────────────────────

main() {
    if [[ $# -lt 1 ]]; then
        usage
        exit 1
    fi

    local command="$1"
    shift

    case "$command" in
        baseline) cmd_baseline "$@" ;;
        measure)  cmd_measure  "$@" ;;
        report)   cmd_report   "$@" ;;
        watch)    cmd_watch    "$@" ;;
        --help|-h) usage ;;
        --version) echo "${SCRIPT_NAME} v${VERSION}" ;;
        *) log_error "Unknown command: ${command}"; usage; exit 1 ;;
    esac
}

main "$@"
