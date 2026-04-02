#include "pfc_index.hpp"

#include <fstream>
#include <stdexcept>
#include <cstring>
#include <climits>

// Size checks are in the header via static_assert

PFCIndex PFCIndex::ReadFromFile(const std::string &bidx_path) {
    std::ifstream f(bidx_path, std::ios::binary);
    if (!f.is_open()) {
        throw std::runtime_error(
            "Cannot open index file: " + bidx_path + "\n"
            "Hint: compress your file with PFC-JSONL v3.4+ to generate the .bidx index:\n"
            "  pfc_jsonl compress input.jsonl output.pfc");
    }

    PFCBidxHeader hdr;
    f.read(reinterpret_cast<char *>(&hdr), sizeof(hdr));
    if (!f) {
        throw std::runtime_error("Failed to read index header: " + bidx_path);
    }

    if (std::memcmp(hdr.magic, "PFCI", 4) != 0) {
        throw std::runtime_error("Invalid magic bytes in index file: " + bidx_path);
    }
    if (hdr.version != 1) {
        throw std::runtime_error("Unsupported index version " +
                                 std::to_string(hdr.version) + ": " + bidx_path);
    }

    PFCIndex idx;
    idx.blocks.resize(hdr.block_count);
    f.read(reinterpret_cast<char *>(idx.blocks.data()),
           hdr.block_count * sizeof(PFCBidxRecord));
    if (!f) {
        throw std::runtime_error("Failed to read index records: " + bidx_path);
    }

    return idx;
}

std::vector<uint32_t> PFCIndex::GetAllBlockIds() const {
    std::vector<uint32_t> ids;
    ids.reserve(blocks.size());
    for (const auto &rec : blocks) {
        ids.push_back(rec.block_id);
    }
    return ids;
}

std::vector<uint32_t> PFCIndex::GetBlocksInRange(int64_t ts_from, int64_t ts_to) const {
    // No filter requested → return all
    if (ts_from == 0 && ts_to == 0) {
        return GetAllBlockIds();
    }

    // 0 means "open bound": ts_from=0 → no lower limit, ts_to=0 → no upper limit
    const int64_t effective_from = (ts_from == 0) ? INT64_MIN : ts_from;
    const int64_t effective_to   = (ts_to   == 0) ? INT64_MAX : ts_to;

    std::vector<uint32_t> ids;
    for (const auto &rec : blocks) {
        // Unknown timestamps → always include
        if (rec.ts_start == 0 && rec.ts_end == 0) {
            ids.push_back(rec.block_id);
            continue;
        }
        // Overlap check: block [ts_start, ts_end] overlaps query [effective_from, effective_to] if
        //   block.ts_start <= effective_to && block.ts_end >= effective_from
        if (rec.ts_start <= effective_to && rec.ts_end >= effective_from) {
            ids.push_back(rec.block_id);
        }
    }
    return ids;
}
