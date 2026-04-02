# pfc Extension — Developer Docs

## Building

```bash
git clone --recurse-submodules https://github.com/ImpossibleForge/pfc-duckdb
cd pfc-duckdb
GEN=ninja make release
```

Requires: cmake 3.21+, ninja, g++ 11+

## Testing

**CI tests (no binary required):**
```bash
build/release/test/unittest "test/sql/pfc.test"
```

**Full local integration tests (requires pfc_jsonl binary):**
```bash
bash test/test_local.sh
```

## Updating DuckDB version

See [UPDATING.md](UPDATING.md).

## Architecture

The extension is a thin C++ wrapper around the `pfc_jsonl` binary:

- `src/pfc_index.cpp` — reads `.pfc.bidx` binary block index
- `src/pfc_extension.cpp` — DuckDB table function (Bind/Init/Scan)

The decompression logic is entirely in the closed-source `pfc_jsonl` binary.
No compression algorithm is exposed in this repository.
