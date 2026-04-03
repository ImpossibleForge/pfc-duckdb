# pfc — DuckDB Extension for PFC-JSONL

Query compressed PFC-JSONL log files directly in DuckDB — no decompression step, no intermediate files.

> **Platform:** Linux x86_64 available now. macOS coming soon. Windows users: use WSL2.

```sql
INSTALL pfc FROM community;
LOAD pfc;
LOAD json;

SELECT
    line->>'$.level'   AS level,
    line->>'$.message' AS message
FROM read_pfc_jsonl('/var/log/events.pfc')
WHERE line->>'$.level' = 'ERROR';
```

## What is PFC-JSONL?

[PFC-JSONL](https://github.com/ImpossibleForge/pfc-jsonl) is a high-performance compressed log format built for structured (JSONL) data. It achieves **better compression than gzip, zstd, and bzip2** on real log data while supporting **random block access** — meaning you can decompress only the time range you need.

Key properties:
- Each file is split into independently compressible blocks
- A `.pfc.bidx` binary index stores the byte offset and timestamp range of every block
- The PFC binary can decompress any subset of blocks in a single call
- **Community Mode: 5 GB/day free, no license key required**

## How It Works (Architecture)

```
┌──────────────────────────────────────────────────────────────┐
│ DuckDB                                                       │
│                                                              │
│  SELECT * FROM read_pfc_jsonl('events.pfc', ts_from=...)     │
│           │                                                  │
│  ┌────────▼──────────┐    reads     ┌─────────────────────┐  │
│  │  pfc extension    │─────────────▶│  events.pfc.bidx    │  │
│  │  (MIT, open src)  │  block index │  (block timestamps) │  │
│  └────────┬──────────┘              └─────────────────────┘  │
│           │ popen() / subprocess                              │
└───────────┼──────────────────────────────────────────────────┘
            │
            ▼
  ┌─────────────────────┐
  │  pfc_jsonl binary   │  ← proprietary, closed source
  │  (v3.4+, local)     │    contains BWT+rANS compression
  └─────────────────────┘
            │
            ▼
  decompressed JSON lines → back to DuckDB
```

The extension is a **thin open-source wrapper** — it reads the `.bidx` index in C++ to select which blocks are needed, then calls the PFC binary once to decompress only those blocks. The compression algorithm stays closed.

## Installation

### Step 1 — Install the PFC binary (once per machine)

The extension calls the `pfc_jsonl` binary for decompression.
Download the latest release for your platform:

**Linux x64:**
```bash
curl -L https://github.com/ImpossibleForge/pfc-jsonl/releases/latest/download/pfc_jsonl-linux-x64 \
     -o /usr/local/bin/pfc_jsonl
chmod +x /usr/local/bin/pfc_jsonl
pfc_jsonl --version   # verify
```

> **macOS:** Binary coming soon — contact **impossibleforge@gmail.com** for early access.

> **Custom path:** Set `PFC_JSONL_BINARY=/path/to/pfc_jsonl` in your environment to override the default `/usr/local/bin/pfc_jsonl`.

### Step 2 — Install the DuckDB extension

```sql
INSTALL pfc FROM community;
LOAD pfc;
```

### Build from source (developers)

```bash
git clone --recurse-submodules https://github.com/ImpossibleForge/pfc-duckdb
cd pfc-duckdb
GEN=ninja make release
# Extension at: build/release/extension/pfc/pfc.duckdb_extension
```

## Usage

### Basic query

```sql
LOAD pfc;

SELECT line FROM read_pfc_jsonl('/path/to/file.pfc');
```

Each row contains one raw JSON string in the `line` column.
Use the DuckDB `json` extension to parse fields:

```sql
LOAD json;

SELECT
    line->>'$.timestamp' AS ts,
    line->>'$.level'     AS level,
    line->>'$.message'   AS message,
    line->>'$.service'   AS service
FROM read_pfc_jsonl('/path/to/file.pfc');
```

### Timestamp-based block filtering

PFC files include a `.pfc.bidx` index with the timestamp range of each block.
Pass `ts_from` and/or `ts_to` (Unix seconds) to skip entire blocks before decompression:

```sql
-- Only decompress blocks that overlap the given time window
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

`ts_from`/`ts_to` skip entire **blocks** (coarse, fast).
Add a `WHERE` clause for **row-level** precision:

```sql
LOAD json;

SELECT line->>'$.message' AS msg
FROM read_pfc_jsonl(
    '/var/log/api.pfc',
    ts_from = epoch(TIMESTAMPTZ '2026-03-15 08:00:00+00'),
    ts_to   = epoch(TIMESTAMPTZ '2026-03-15 10:00:00+00')
)
WHERE line->>'$.level' = 'ERROR';
```

### Analytics examples

```sql
LOAD json;

-- Error rate per hour
SELECT
    strftime(to_timestamp((line->>'$.ts')::BIGINT), '%Y-%m-%d %H:00') AS hour,
    count(*) FILTER (WHERE line->>'$.level' = 'ERROR') AS errors,
    count(*)                                            AS total
FROM read_pfc_jsonl('/var/log/api.pfc')
GROUP BY hour ORDER BY hour;

-- Top 10 slowest endpoints
SELECT
    line->>'$.path'                              AS endpoint,
    avg((line->>'$.duration_ms')::DOUBLE)        AS avg_ms,
    count(*)                                     AS requests
FROM read_pfc_jsonl('/var/log/api.pfc')
GROUP BY endpoint ORDER BY avg_ms DESC LIMIT 10;
```

## API Reference

### `read_pfc_jsonl(path [, ts_from, ts_to])`

| Parameter | Type    | Default | Description |
|-----------|---------|---------|-------------|
| `path`    | VARCHAR | —       | Path to the `.pfc` file. A `.pfc.bidx` index must exist at `path + ".bidx"`. |
| `ts_from` | BIGINT  | 0       | Lower bound for block selection (Unix seconds). `0` = no lower bound. |
| `ts_to`   | BIGINT  | 0       | Upper bound for block selection (Unix seconds). `0` = no upper bound. |

**Returns:** table with one column `line VARCHAR` — one row per decompressed JSON line.

**Block filtering semantics:**
A block is included if its timestamp range `[ts_start, ts_end]` overlaps `[ts_from, ts_to]`.
Blocks with unknown timestamps are always included.
If both `ts_from` and `ts_to` are `0`, all blocks are read.

## File Requirements

| File | Required | Description |
|------|----------|-------------|
| `file.pfc` | yes | Compressed PFC-JSONL file |
| `file.pfc.bidx` | yes | Binary block index (requires PFC-JSONL v3.4+) |

Generate both with the PFC binary:

```bash
pfc_jsonl compress input.jsonl output.pfc
# Produces: output.pfc  +  output.pfc.bidx
```

> **Note:** The Docker image on Docker Hub (`impossibleforge/pfc-jsonl`) is a server-side compression tool. It is **not** required for using the DuckDB extension — you only need the standalone `pfc_jsonl` binary from GitHub Releases.

## Performance

Block-level filtering can skip the majority of a file.
Example: 30-day log file, 720 hourly blocks — a 1-hour query reads **1 block** instead of 720.

| Query range | Blocks read | Speedup (720-block file) |
|-------------|-------------|--------------------------|
| 30 days     | 720/720     | 1×                       |
| 1 day       | ~24/720     | ~30×                     |
| 1 hour      | ~1/720      | ~720×                    |

## Community Mode vs. Licensed

The PFC binary includes a built-in Community Mode — **no account, no signup required**:

|                    | Community                        | Licensed        |
|--------------------|----------------------------------|-----------------|
| Daily limit        | 5 GB decompressed output / day   | Unlimited       |
| License key        | not required                     | required        |
| Phone home         | never                            | never           |
| Commercial use     | allowed                          | allowed         |
| Counter reset      | midnight UTC                     | —               |

Usage is tracked locally in `~/.pfc/usage.json`. Nothing leaves your machine.

> **Note:** The 5 GB limit applies to **decompressed output bytes** (not the compressed file size).
> For most log-analysis workloads this is more than sufficient per query session.

For a license key: [impossibleforge@gmail.com](mailto:impossibleforge@gmail.com)

## Troubleshooting

**`Cannot open index file: /path/to/file.pfc.bidx`**
The `.pfc.bidx` index is missing. Compress with PFC-JSONL v3.4+:
```bash
pfc_jsonl compress input.jsonl output.pfc
```

**`PFC Community Mode daily limit reached`**
Community Mode daily limit exceeded (5 GB decompressed / UTC day). Wait until midnight UTC, or [get a license](mailto:impossibleforge@gmail.com).

**`PFC binary not found at '/usr/local/bin/pfc_jsonl'`**
Binary is missing or not executable. Re-run the curl install command, or set `PFC_JSONL_BINARY=/path/to/pfc_jsonl`.

**`popen() failed — could not start PFC binary subprocess`**
The extension uses `popen()` to call the PFC binary. Windows is not supported — use WSL2 or a Linux machine (macOS binary coming soon).

**`ts_from (...) must be <= ts_to (...)`**
You passed an inverted time range. Swap the values so `ts_from` comes before `ts_to`.

## License

The **pfc DuckDB extension** (this repository) is released under the **MIT License** — see [LICENSE](LICENSE).

The **PFC-JSONL binary** (`pfc_jsonl`) is **proprietary software** — it contains the closed-source compression/decompression engine. Community Mode provides 5 GB/day free. Commercial licenses available at [impossibleforge@gmail.com](mailto:impossibleforge@gmail.com).
