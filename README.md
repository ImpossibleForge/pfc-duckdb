# Tests

## SQL logic tests (CI-safe, no binary required)

Located in `test/sql/pfc.test`. Run with:

```bash
build/release/test/unittest "test/sql/pfc.test"
```

These tests only cover error handling (missing file, wrong types) and do not require the `pfc_jsonl` binary.

## Local integration tests (requires pfc_jsonl binary)

```bash
bash test/test_local.sh
# Expected: 9 passed, 0 failed
```

Tests cover:
- Row count from fixture (10 rows)
- JSON format of output lines
- `ts_from`/`ts_to` block filtering (0 rows when block excluded, 10 rows when included)
- Missing `.bidx` error message

## Test fixtures

`test/fixtures/fixture.pfc` — 10 JSON lines, 1 block, timestamps from 2026-01-01.
`test/fixtures/fixture.pfc.bidx` — corresponding binary block index.

Generated with: `pfc_jsonl compress fixture.jsonl fixture.pfc`
