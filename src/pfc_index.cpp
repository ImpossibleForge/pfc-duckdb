#include "pfc_index.hpp"

#include <fstream>
#include <stdexcept>
#include <cstring>
#include <climits>

// Compile-time size checks are in the header via static_assert.

PFCIndex PFCIndex::ReadFromFile(const std::string &bidx_path) {
	std::ifstream f(bidx_path, std::ios::binary);
	if (!f.is_open()) {
		throw std::runtime_error("Cannot open index file: " + bidx_path +
		                         "\n"
		                         "Hint: compress your file with PFC-JSONL v3.4+ to generate the .bidx index:\n"
		                         "  pfc_jsonl compress input.jsonl output.pfc");
	}

	// ── Read and validate header ────────────────────────────────────────────

	PFCBidxHeader hdr;
	f.read(reinterpret_cast<char *>(&hdr), sizeof(hdr));
	if (!f) {
		throw std::runtime_error("Failed to read index header (file too small?): " + bidx_path);
	}

	if (std::memcmp(hdr.magic, "PFCI", 4) != 0) {
		throw std::runtime_error("Invalid magic bytes in index file — not a PFC-JSONL v3.4+ index: " + bidx_path);
	}
	if (hdr.version != 1) {
		throw std::runtime_error("Unsupported index version " + std::to_string(hdr.version) +
		                         " (expected 1): " + bidx_path);
	}

	// ── Sanity-check declared block count against actual file size ──────────
	// Guards against corrupt or adversarially crafted files that declare an
	// unrealistically large block_count, which would cause a huge allocation.

	f.seekg(0, std::ios::end);
	const auto file_size = static_cast<uint64_t>(f.tellg());
	const uint64_t expected_size =
	    static_cast<uint64_t>(sizeof(PFCBidxHeader)) + static_cast<uint64_t>(hdr.block_count) * sizeof(PFCBidxRecord);

	if (file_size < expected_size) {
		throw std::runtime_error("Index file is truncated — declared " + std::to_string(hdr.block_count) +
		                         " blocks but file is too small: " + bidx_path);
	}
	f.seekg(sizeof(hdr), std::ios::beg); // rewind to start of record section

	// ── Read all block records ──────────────────────────────────────────────

	PFCIndex idx;
	idx.blocks.resize(hdr.block_count);
	f.read(reinterpret_cast<char *>(idx.blocks.data()),
	       static_cast<std::streamsize>(hdr.block_count) * sizeof(PFCBidxRecord));
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
	// Both zero → no filter requested, return every block.
	if (ts_from == 0 && ts_to == 0) {
		return GetAllBlockIds();
	}

	// A single zero means "open bound": ts_from=0 → −∞, ts_to=0 → +∞.
	const int64_t effective_from = (ts_from == 0) ? INT64_MIN : ts_from;
	const int64_t effective_to = (ts_to == 0) ? INT64_MAX : ts_to;

	std::vector<uint32_t> ids;
	for (const auto &rec : blocks) {
		// Blocks whose both timestamps are 0 have unknown ranges (written by older tooling
		// without timestamp tracking). Always include them to avoid silently dropping data.
		if (rec.ts_start == 0 && rec.ts_end == 0) {
			ids.push_back(rec.block_id);
			continue;
		}
		// Standard interval-overlap check:
		//   block [ts_start, ts_end] overlaps query [effective_from, effective_to]
		//   if and only if: ts_start <= effective_to  AND  ts_end >= effective_from
		if (rec.ts_start <= effective_to && rec.ts_end >= effective_from) {
			ids.push_back(rec.block_id);
		}
	}
	return ids;
}
