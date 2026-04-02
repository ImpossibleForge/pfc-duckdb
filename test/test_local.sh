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

run() {
    local desc="$1"
    local sql="$2"
    local expected="$3"
    local result
    result=$(${DUCKDB} -c "LOAD '${EXT}'; ${sql}" 2>&1 | tr -d ' \t\n')
    if [[ "$result" == *"$expected"* ]]; then
        echo -e "${GREEN}PASS${NC}  $desc"
        PASS=$((PASS+1))
    else
        echo -e "${RED}FAIL${NC}  $desc"
        echo "      expected: $expected"
        echo "      got:      $result"
        FAIL=$((FAIL+1))
    fi
}

run_error() {
    local desc="$1"
    local sql="$2"
    local expected_err="$3"
    local result
    result=$(${DUCKDB} -c "LOAD '${EXT}'; ${sql}" 2>&1 || true)
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

# 3. line column is VARCHAR containing JSON
run "line column contains JSON object" \
    "SELECT line LIKE '{%}' AS is_json FROM read_pfc_jsonl('${FIXTURE}') LIMIT 1;" \
    "true"

# 4. ts_from=0, ts_to=0 → all blocks (no filter)
run "ts_from=0 ts_to=0 returns all rows" \
    "SELECT count(*) FROM read_pfc_jsonl('${FIXTURE}', ts_from=0, ts_to=0);" \
    "10"

# 5. ts_to far in the past → 0 rows (block skipped)
run "ts_to before block ts_start → 0 rows" \
    "SELECT count(*) FROM read_pfc_jsonl('${FIXTURE}', ts_to=1000000000);" \
    "0"

# 6. ts_from far in the future → 0 rows (block skipped)
run "ts_from after block ts_end → 0 rows" \
    "SELECT count(*) FROM read_pfc_jsonl('${FIXTURE}', ts_from=9999999999);" \
    "0"

# 7. ts_from within block range → 10 rows (block included)
run "ts_from=1735689600 (exact block start) → 10 rows" \
    "SELECT count(*) FROM read_pfc_jsonl('${FIXTURE}', ts_from=1735689600);" \
    "10"

# 8. Missing .bidx → clear error
run_error "missing .bidx gives actionable error" \
    "SELECT * FROM read_pfc_jsonl('/nonexistent.pfc');" \
    "Cannot open index file"

# 9. line contains valid JSON (string check, no json extension required)
run "all lines start with '{' (valid JSON objects)" \
    "SELECT count(*) FROM read_pfc_jsonl('${FIXTURE}') WHERE line NOT LIKE '{%}';" \
    "0"

# 10. Multi-block timestamp filter (needs multiblock fixture)
if [[ -f "test/fixtures/multiblock.pfc" ]]; then
    run "multi-block: ts_to filters to block 0 only" \
        "SELECT count(*) FROM read_pfc_jsonl('test/fixtures/multiblock.pfc', ts_to=1735800000);" \
        "7831"
    run "multi-block: ts_from filters to block 1 only" \
        "SELECT count(*) FROM read_pfc_jsonl('test/fixtures/multiblock.pfc', ts_from=1740787200);" \
        "2170"
else
    echo "  SKIP  multi-block tests (run generate_multiblock_fixture.sh first)"
fi

echo "═══════════════════════════════════════════"
echo " Results: ${PASS} passed, ${FAIL} failed"
echo "═══════════════════════════════════════════"
[[ $FAIL -eq 0 ]]
