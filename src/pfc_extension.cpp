#include "pfc_extension.hpp"
#include "pfc_index.hpp"
#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"

#include <string>
#include <vector>
#include <stdexcept>
#include <cstdio>
#include <cstdlib>

namespace duckdb {

// ─── Helpers ─────────────────────────────────────────────────────────────────

static std::string FindPFCBinary() {
    const char *env = std::getenv("PFC_JSONL_BINARY");
    if (env && env[0] != '\0') {
        return std::string(env);
    }
    return "/usr/local/bin/pfc_jsonl";
}

// Escape a string for safe shell argument use (single-quote wrap)
static std::string ShellEscape(const std::string &s) {
    std::string out = "'";
    for (char c : s) {
        if (c == '\'') {
            out += "'\\''";
        } else {
            out += c;
        }
    }
    out += "'";
    return out;
}

// ─── Table function state ─────────────────────────────────────────────────────

struct PFCBindData : public TableFunctionData {
    std::string pfc_path;
    std::string bidx_path;
    PFCIndex    index;
    int64_t     ts_from = 0;   // Unix seconds, 0 = no lower bound
    int64_t     ts_to   = 0;   // Unix seconds, 0 = no upper bound
};

struct PFCGlobalState : public GlobalTableFunctionState {
    std::vector<std::string> lines;
    idx_t                    line_idx = 0;
    bool                     done     = false;
};

// ─── Block decompression via subprocess ──────────────────────────────────────

static std::vector<std::string> CallPFCSeekBlocks(
        const std::string &binary,
        const std::string &pfc_path,
        const std::vector<uint32_t> &block_ids) {

    if (block_ids.empty()) {
        return {};
    }

    // Build: pfc_jsonl seek-blocks --blocks N [N...] -- 'path/to/file.pfc.jsonl'
    // The '--' separator is required so argparse does not consume the path as a block id.
    std::string cmd = ShellEscape(binary) + " seek-blocks --blocks";
    for (uint32_t id : block_ids) {
        cmd += " " + std::to_string(id);
    }
    cmd += " -- " + ShellEscape(pfc_path) + " 2>/dev/null";

    FILE *pipe = popen(cmd.c_str(), "r");
    if (!pipe) {
        throw std::runtime_error("popen failed for PFC binary: " + cmd);
    }

    std::vector<std::string> result;
    char buf[4096];
    std::string current_line;

    while (fgets(buf, sizeof(buf), pipe)) {
        current_line += buf;
        // fgets includes '\n' at end of line (unless truncated)
        if (!current_line.empty() && current_line.back() == '\n') {
            current_line.pop_back();
            result.push_back(std::move(current_line));
            current_line.clear();
        }
    }
    if (!current_line.empty()) {
        result.push_back(std::move(current_line));
    }

    int rc = pclose(pipe);
    if (rc != 0) {
        throw std::runtime_error("PFC binary exited with code " +
                                 std::to_string(rc) + " — check license or file path");
    }

    return result;
}

// ─── Bind ─────────────────────────────────────────────────────────────────────

static unique_ptr<FunctionData> PFCBind(
        ClientContext &context,
        TableFunctionBindInput &input,
        vector<LogicalType> &return_types,
        vector<string> &names) {

    if (input.inputs.empty()) {
        throw BinderException("read_pfc_jsonl requires a file path argument");
    }

    auto bind_data = make_uniq<PFCBindData>();
    bind_data->pfc_path  = input.inputs[0].GetValue<std::string>();
    bind_data->bidx_path = bind_data->pfc_path + ".bidx";

    // Optional timestamp range parameters for block-level filtering
    auto it_from = input.named_parameters.find("ts_from");
    if (it_from != input.named_parameters.end()) {
        bind_data->ts_from = it_from->second.GetValue<int64_t>();
    }
    auto it_to = input.named_parameters.find("ts_to");
    if (it_to != input.named_parameters.end()) {
        bind_data->ts_to = it_to->second.GetValue<int64_t>();
    }

    // Read the binary index — throws if missing or malformed
    bind_data->index = PFCIndex::ReadFromFile(bind_data->bidx_path);

    // Output schema: single VARCHAR column "line"
    return_types.push_back(LogicalType::VARCHAR);
    names.push_back("line");

    return std::move(bind_data);
}

// ─── Init ─────────────────────────────────────────────────────────────────────

static unique_ptr<GlobalTableFunctionState> PFCInit(
        ClientContext &context,
        TableFunctionInitInput &input) {

    auto &bind_data = input.bind_data->Cast<PFCBindData>();
    auto state      = make_uniq<PFCGlobalState>();

    std::string binary = FindPFCBinary();

    // Block-level filter: skip blocks outside the requested timestamp range
    auto block_ids = bind_data.index.GetBlocksInRange(bind_data.ts_from, bind_data.ts_to);

    state->lines    = CallPFCSeekBlocks(binary, bind_data.pfc_path, block_ids);
    state->line_idx = 0;
    state->done     = state->lines.empty();

    return std::move(state);
}

// ─── Scan ─────────────────────────────────────────────────────────────────────

static void PFCScan(
        ClientContext &context,
        TableFunctionInput &data,
        DataChunk &output) {

    auto &state = data.global_state->Cast<PFCGlobalState>();

    if (state.done) {
        output.SetCardinality(0);
        return;
    }

    idx_t count = 0;
    auto &col   = output.data[0];

    while (count < STANDARD_VECTOR_SIZE && state.line_idx < state.lines.size()) {
        col.SetValue(count, Value(state.lines[state.line_idx]));
        ++state.line_idx;
        ++count;
    }

    if (state.line_idx >= state.lines.size()) {
        state.done = true;
    }

    output.SetCardinality(count);
}

// ─── Register ─────────────────────────────────────────────────────────────────

static void LoadInternal(ExtensionLoader &loader) {
    TableFunction tf("read_pfc_jsonl",
                     {LogicalType::VARCHAR},
                     PFCScan,
                     PFCBind,
                     PFCInit);

    // Named parameters for block-level timestamp filtering
    // ts_from / ts_to: Unix seconds (int64). Blocks outside the range are skipped entirely.
    // Both default to 0, which means "no filter" (all blocks are read).
    tf.named_parameters["ts_from"] = LogicalType::BIGINT;
    tf.named_parameters["ts_to"]   = LogicalType::BIGINT;

    loader.RegisterFunction(tf);
}

// ─── Extension entry points ───────────────────────────────────────────────────

void PfcExtension::Load(ExtensionLoader &loader) {
    LoadInternal(loader);
}

std::string PfcExtension::Name() {
    return "pfc";
}

std::string PfcExtension::Version() const {
#ifdef EXT_VERSION_PFC
    return EXT_VERSION_PFC;
#else
    return "0.1.0";
#endif
}

} // namespace duckdb

DUCKDB_CPP_EXTENSION_ENTRY(pfc, loader) {
    duckdb::LoadInternal(loader);
}
