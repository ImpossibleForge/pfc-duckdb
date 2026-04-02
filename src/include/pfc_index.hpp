#pragma once

#include <cstdint>
#include <vector>
#include <string>

// ─── PFC Binary Index (.pfc.bidx) Format ─────────────────────────────────────
//
// Written by PFC-JSONL v3.4+ alongside every .pfc file.
// Fixed-size, little-endian, readable directly in C++ via struct cast.
//
// Header (16 bytes):
//   [magic:       4B]  b'PFCI'
//   [version:     2B]  uint16 = 1
//   [block_count: 4B]  uint32
//   [reserved:    6B]  0x00 * 6
//
// Record (32 bytes per block):
//   [block_id:    4B]  uint32  — 0-based block index
//   [byte_offset: 8B]  uint64  — absolute byte offset in .pfc file
//   [block_size:  4B]  uint32  — compressed byte count
//   [ts_start:    8B]  int64   — earliest Unix timestamp (seconds), 0 = unknown
//   [ts_end:      8B]  int64   — latest  Unix timestamp (seconds), 0 = unknown

#pragma pack(push, 1)
struct PFCBidxHeader {
    uint8_t  magic[4];       // "PFCI"
    uint16_t version;        // 1
    uint32_t block_count;
    uint8_t  reserved[6];
};

struct PFCBidxRecord {
    uint32_t block_id;
    uint64_t byte_offset;
    uint32_t block_size;
    int64_t  ts_start;       // Unix seconds (0 = unknown)
    int64_t  ts_end;         // Unix seconds (0 = unknown)
};
#pragma pack(pop)

// Compile-time size checks
static_assert(sizeof(PFCBidxHeader) == 16, "PFCBidxHeader must be 16 bytes");
static_assert(sizeof(PFCBidxRecord) == 32, "PFCBidxRecord must be 32 bytes");

struct PFCIndex {
    std::vector<PFCBidxRecord> blocks;

    // Read .pfc.bidx from disk. Throws std::runtime_error on failure.
    static PFCIndex ReadFromFile(const std::string &bidx_path);

    // Return all block IDs in order.
    std::vector<uint32_t> GetAllBlockIds() const;

    // Return block IDs whose timestamp range overlaps [ts_from, ts_to].
    // If ts_from == 0 && ts_to == 0: returns all blocks (no filter).
    // Blocks with unknown timestamps (ts_start==0 && ts_end==0) are always included.
    std::vector<uint32_t> GetBlocksInRange(int64_t ts_from, int64_t ts_to) const;
};
