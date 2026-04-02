#!/usr/bin/env bash
# Local integration tests for pfc extension — requires PFC binary + DuckDB build
# Usage: bash test/test_local.sh [path/to/duckdb]
#
# Run from repo root after: GEN=ninja make release

set -euo pipefail

DUCKDB="${1:-build/release/duckdb}"
EXT="build/release/extension/pfc/pfc.duckdb_extension"
FIXTURE="test/fixtures/fixture.pfc"
PASS=0
FAIL=0

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

# run DESCRIPTION SQL EXPECTED_SUBSTRING
# Strips whitespace for reliable substring comparison.
run() {
    local desc="$1"
    local sql="$2"
    local expected="$3"
    local result
    result=$(timeout 15 ${DUCKDB} -c "LOAD '${EXT}'; ${sql}" 2>&1 | tr -d ' \t\n')
    if [[ "$result" == *"$expected"* ]]; then
        echo -e "${GREEN}PASS${NC}  $desc"
        PASS=$((PASS+1))
    else
        echo -e "${RED}FAIL${NC}  $desc"
        echo "      expected substring: $expected"
        echo "      got:                $result"
        FAIL=$((FAIL+1))
    fi
}

# run_error DESCRIPTION SQL EXPECTED_ERROR_SUBSTRING
run_error() {
    local desc="$1"
    local sql="$2"
    local expected_err="$3"
    local result
    result=$(timeout 15 ${DUCKDB} -c "LOAD '${EXT}'; ${sql}" 2>&1 || true)
    if [[ "$result" == *"$expected_err"* ]]; then
        echo -e "${GREEN}PASS${NC}  $desc (expected error)"
        PASS=$((PASS+1))
    else
        echo -e "${RED}FAIL${NC}  $desc"
        echo "      expected error containing: $expected_err"
        echo "      got: $result"
        FAIL=$((FAIL+1))
    fi
}

echo "═══════════════════════════════════════════"
echo " pfc extension — local integration tests"
echo "═══════════════════════════════════════════"

# 1. Extension loads
run "extension loads" "SELECT 42 AS ok;" "42"

# 2. Read all 10 rows from fixture
run "read_pfc_jsonl returns 10 rows" \
    "SELECT count(*) AS n FROM read_pfc_jsonl('${FIXTURE}');" \
    "10"

# 3. line column is VARCHAR containing JSON object
run "line column contains JSON object" \
    "SELECT line LIKE '{%}' AS is_json FROM read_pfc_jsonl('${FIXTURE}') LIMIT 1;" \
    "true"

# 4. ts_from=0, ts_to=0 → no filter, all rows returned
run "ts_from=0 ts_to=0 returns all rows" \
    "SELECT count(*) FROM read_pfc_jsonl('${FIXTURE}', ts_from=0, ts_to=0);" \
    "10"

# 5. ts_to in the distant past → entire block skipped, 0 rows
run "ts_to before block ts_start → 0 rows" \
    "SELECT count(*) FROM read_pfc_jsonl('${FIXTURE}', ts_to=1000000000);" \
    "0"

# 6. ts_from in the distant future → entire block skipped, 0 rows
run "ts_from after block ts_end → 0 rows" \
    "SELECT count(*) FROM read_pfc_jsonl('${FIXTURE}', ts_from=9999999999);" \
    "0"

# 7. ts_from within block range → block included, all 10 rows
run "ts_from=1735689600 (exact block start) → 10 rows" \
    "SELECT count(*) FROM read_pfc_jsonl('${FIXTURE}', ts_from=1735689600);" \
    "10"

# 8. Both ts_from and ts_to within block range → block included, all 10 rows
run "ts_from + ts_to both spanning block → 10 rows" \
    "SELECT count(*) FROM read_pfc_jsonl('${FIXTURE}', ts_from=1735689600, ts_to=1735775999);" \
    "10"

# 9. ts_from > ts_to → BinderException at query time
run_error "ts_from > ts_to gives clear error" \
    "SELECT count(*) FROM read_pfc_jsonl('${FIXTURE}', ts_from=9999999999, ts_to=1);" \
    "ts_from"

# 10. Missing .pfc file → clear error message
run_error "missing .pfc gives actionable error" \
    "SELECT * FROM read_pfc_jsonl('/nonexistent.pfc');" \
    "Cannot open PFC file"

# 11. Missing .bidx file (create a dummy .pfc without a .bidx)
TMPDIR_PFC=$(mktemp -d)
touch "${TMPDIR_PFC}/orphan.pfc"
run_error "missing .bidx gives actionable error with hint" \
    "SELECT * FROM read_pfc_jsonl('${TMPDIR_PFC}/orphan.pfc');" \
    "Cannot open index file"
rm -rf "${TMPDIR_PFC}"

# 12. All lines are valid JSON objects (none start outside '{...}')
run "all lines start with '{' (JSON object format)" \
    "SELECT count(*) FROM read_pfc_jsonl('${FIXTURE}') WHERE line NOT LIKE '{%}';" \
    "0"

# ── Multi-block tests (requires separate fixture) ─────────────────────────────
if [[ -f "test/fixtures/multiblock.pfc" ]]; then
    run "multi-block: ts_to filters to block 0 only" \
        "SELECT count(*) FROM read_pfc_jsonl('test/fixtures/multiblock.pfc', ts_to=1735800000);" \
        "7831"
    run "multi-block: ts_from filters to block 1 only" \
        "SELECT count(*) FROM read_pfc_jsonl('test/fixtures/multiblock.pfc', ts_from=1740787200);" \
        "2170"
else
    echo "  SKIP  multi-block tests (fixture not present)"
fi

echo "═══════════════════════════════════════════"
echo " Results: ${PASS} passed, ${FAIL} failed"
echo "═══════════════════════════════════════════"
[[ $FAIL -eq 0 ]]
