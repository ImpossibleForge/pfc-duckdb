# pfc — DuckDB Extension for PFC-JSONL

Query compressed PFC-JSONL log files directly in DuckDB — no decompression step, no intermediate files.

```sql
INSTALL pfc FROM community;
LOAD pfc;

SELECT line->>'$.level' AS level, count(*)
FROM read_pfc_jsonl('/var/log/events.pfc')
GROUP BY level;
```

## What is PFC-JSONL?

[PFC-JSONL](https://github.com/pureforge/pfc-jsonl) is a high-performance compressed log format built for structured (JSONL) data. It achieves **better compression than gzip, zstd, and bzip2** on real log data while supporting **random block access** — meaning you can decompress only the time range you need.

Key properties:
- Each file is split into independently compressible blocks
- A `.pfc.bidx` binary index stores the byte offset and timestamp range of every block
- The PFC binary can decompress any subset of blocks in a single call
- Community Mode: **5 GB/day free**, no license key required

## Installation

### From DuckDB Community Extensions (recommended)

```sql
INSTALL pfc FROM community;
LOAD pfc;
```

### From source

Requires: cmake, ninja, g++ 11+, git

```bash
git clone --recurse-submodules https://github.com/pureforge/pfc-duckdb
cd pfc-duckdb
GEN=ninja make release
```

```sql
LOAD 'build/release/extension/pfc/pfc.duckdb_extension';
```

### PFC Binary

The extension calls the PFC binary for decompression. Install it once:

```bash
pip install pfc-jsonl          # or download from github.com/pureforge/pfc-jsonl
```

The extension looks for the binary at `/usr/local/bin/pfc_jsonl` by default.
Override with the environment variable:

```bash
PFC_JSONL_BINARY=/path/to/pfc_jsonl duckdb
```

## Usage

### Basic query

```sql
LOAD pfc;

-- Read all lines from a local file
SELECT line FROM read_pfc_jsonl('/path/to/file.pfc');
```

Each row contains one raw JSON string in the `line` column.
Use the DuckDB `json` extension to parse fields:

```sql
LOAD json;

SELECT
    line->>'$.timestamp'   AS ts,
    line->>'$.level'       AS level,
    line->>'$.message'     AS message,
    line->>'$.service'     AS service
FROM read_pfc_jsonl('/path/to/file.pfc');
```

### Timestamp-based block filtering

PFC files include a `.pfc.bidx` index with the timestamp range of each block.
Pass `ts_from` and/or `ts_to` (Unix seconds) to skip entire blocks before decompression:

```sql
-- Only read blocks that overlap the given time window
-- Blocks outside the range are never decompressed
SELECT line
FROM read_pfc_jsonl(
    '/path/to/file.pfc',
    ts_from = 1735689600,   -- 2026-01-01 00:00:00 UTC
    ts_to   = 1735775999    -- 2026-01-01 23:59:59 UTC
);
```

Convert a timestamp string to Unix seconds with `epoch()`:

```sql
SELECT line
FROM read_pfc_jsonl(
    '/path/to/file.pfc',
    ts_from = epoch(TIMESTAMPTZ '2026-01-01 00:00:00+00'),
    ts_to   = epoch(TIMESTAMPTZ '2026-01-02 00:00:00+00')
);
```

### Combining block filter and row filter

The `ts_from`/`ts_to` parameters operate at the **block level** (coarse skip).
Add a `WHERE` clause for **row-level** precision:

```sql
LOAD json;

SELECT line->>'$.message' AS msg
FROM read_pfc_jsonl(
    '/var/log/api.pfc',
    ts_from = epoch(TIMESTAMPTZ '2026-03-15 08:00:00+00'),
    ts_to   = epoch(TIMESTAMPTZ '2026-03-15 10:00:00+00')
)
WHERE (line->>'$.level') = 'ERROR';
```

### Aggregations and analytics

```sql
LOAD json;

-- Error rate per hour
SELECT
    strftime(to_timestamp((line->>'$.ts')::BIGINT), '%Y-%m-%d %H:00') AS hour,
    count(*) FILTER (WHERE line->>'$.level' = 'ERROR') AS errors,
    count(*)                                            AS total,
    round(100.0 * count(*) FILTER (WHERE line->>'$.level' = 'ERROR') / count(*), 2) AS error_pct
FROM read_pfc_jsonl('/var/log/api.pfc')
GROUP BY hour
ORDER BY hour;
```

```sql
-- Top 10 slowest endpoints
SELECT
    line->>'$.path'      AS endpoint,
    avg((line->>'$.duration_ms')::DOUBLE) AS avg_ms,
    max((line->>'$.duration_ms')::DOUBLE) AS max_ms,
    count(*) AS requests
FROM read_pfc_jsonl('/var/log/api.pfc')
GROUP BY endpoint
ORDER BY avg_ms DESC
LIMIT 10;
```

## API Reference

### `read_pfc_jsonl(path [, ts_from, ts_to])`

| Parameter | Type   | Default | Description |
|-----------|--------|---------|-------------|
| `path`    | VARCHAR | — | Path to the `.pfc` file. A `.pfc.bidx` index must exist at `path + ".bidx"`. |
| `ts_from` | BIGINT | 0 | Lower bound for block selection (Unix seconds). `0` = no lower bound. |
| `ts_to`   | BIGINT | 0 | Upper bound for block selection (Unix seconds). `0` = no upper bound. |

**Returns:** table with one column `line VARCHAR` — one row per decompressed JSON line.

**Block filtering semantics:**
A block is included if its timestamp range `[ts_start, ts_end]` overlaps `[ts_from, ts_to]`.
Blocks with unknown timestamps (`ts_start == 0 && ts_end == 0`) are always included.
If both `ts_from` and `ts_to` are `0`, all blocks are read (no filter).

## File Requirements

| File | Required | Description |
|------|----------|-------------|
| `file.pfc` | yes | Compressed PFC-JSONL file |
| `file.pfc.bidx` | yes | Binary block index (written automatically by PFC-JSONL v3.4+) |

Generate both files with the PFC binary:

```bash
pfc_jsonl compress input.jsonl output.pfc
# Produces: output.pfc + output.pfc.bidx
```

## Performance

Block-level filtering can skip the majority of a file without reading it at all.
Example: a 30-day log file with 720 hourly blocks — a 1-hour query reads **1 block** instead of 720.

| Query range | Blocks read | Speedup (720-block file) |
|-------------|-------------|--------------------------|
| 30 days (all) | 720/720 | 1× |
| 1 day | ~24/720 | ~30× |
| 1 hour | ~1/720 | ~720× |

Actual speedup depends on block size and data distribution.

## Community Mode vs. Licensed

The PFC binary includes a built-in Community Mode:

| | Community | Licensed |
|--|-----------|----------|
| Daily limit | 5 GB/day | Unlimited |
| License key | not required | required |
| Phone home | never | never |
| Commercial use | allowed | allowed |

Usage is tracked locally in `~/.pfc/usage.json`. No data leaves your machine.

## Troubleshooting

**`Cannot open index file`**
The `.pfc.bidx` file is missing. Recompress with PFC-JSONL v3.4+:
```bash
pfc_jsonl compress input.jsonl output.pfc
```

**`PFC binary exited with code N`**
- Code 1: Community Mode daily limit exceeded (5 GB/day). Wait until midnight UTC or add a license key.
- Code 2: File not found or corrupted.
- Code 32512: Binary not found. Set `PFC_JSONL_BINARY=/path/to/pfc_jsonl`.

**`popen failed`**
The extension uses `popen()` to call the PFC binary. On Windows, use WSL or a Linux/macOS machine.

## License

MIT — see [LICENSE](LICENSE).

The PFC-JSONL binary (decoder) is proprietary software. Community Mode is free for up to 5 GB/day.
