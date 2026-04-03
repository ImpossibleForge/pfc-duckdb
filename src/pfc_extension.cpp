#include "pfc_extension.hpp"
#include "pfc_index.hpp"
#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"

#include <string>
#include <vector>
#include <stdexcept>
#include <cstdio>
#include <cstdlib>
#if !defined(_WIN32) && !defined(__EMSCRIPTEN__)
#include <sys/wait.h> // WIFEXITED / WEXITSTATUS (Linux + macOS)
#endif

namespace duckdb {

// ─── Helpers ─────────────────────────────────────────────────────────────────

static std::string FindPFCBinary() {
	const char *env = std::getenv("PFC_JSONL_BINARY");
	if (env && env[0] != '\0') {
		return std::string(env);
	}
	return "/usr/local/bin/pfc_jsonl";
}

// Escape a string for safe use as a single shell argument.
// Wraps in single quotes and replaces any embedded ' with '\''.
// This is safe for /bin/sh on Linux and macOS.
// Only compiled on POSIX platforms where popen() is available.
#if !defined(_WIN32) && !defined(__EMSCRIPTEN__)
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

// RAII wrapper for FILE* returned by popen().
// Ensures pclose() is called even if an exception is thrown inside the read loop.
struct PipeGuard {
	FILE *pipe = nullptr;
	explicit PipeGuard(FILE *p) : pipe(p) {
	}
	~PipeGuard() {
		if (pipe) {
			pclose(pipe);
		}
	}
	PipeGuard(const PipeGuard &) = delete;
	PipeGuard &operator=(const PipeGuard &) = delete;
	// Call once to retrieve the exit status; marks pipe as closed.
	int close() {
		int rc = pclose(pipe);
		pipe = nullptr;
		return rc;
	}
};
#endif // !_WIN32 && !__EMSCRIPTEN__

// ─── Table function state ─────────────────────────────────────────────────────

struct PFCBindData : public TableFunctionData {
	std::string pfc_path;
	std::string bidx_path;
	PFCIndex index;
	int64_t ts_from = 0; // Unix seconds, 0 = no lower bound
	int64_t ts_to = 0;   // Unix seconds, 0 = no upper bound
};

struct PFCGlobalState : public GlobalTableFunctionState {
	std::vector<std::string> lines;
	idx_t line_idx = 0;
	bool done = false;
};

// ─── Block decompression via subprocess ──────────────────────────────────────

// popen/pclose + WIFEXITED/WEXITSTATUS are POSIX-only and not available on
// Windows or WebAssembly.  We compile a stub on those platforms so that the
// extension links; at runtime the stub throws an informative error before any
// subprocess call is made.

#if defined(_WIN32) || defined(__EMSCRIPTEN__)

static std::vector<std::string> CallPFCSeekBlocks(const std::string & /*binary*/, const std::string & /*pfc_path*/,
                                                  const std::vector<uint32_t> & /*block_ids*/) {
	throw std::runtime_error("pfc extension: subprocess decompression is not supported on this platform. "
	                         "Windows users: please use WSL2.");
}

#else // Linux + macOS

static std::vector<std::string> CallPFCSeekBlocks(const std::string &binary, const std::string &pfc_path,
                                                  const std::vector<uint32_t> &block_ids) {
	if (block_ids.empty()) {
		return {};
	}

	// Build: pfc_jsonl seek-blocks --blocks N [N...] -- 'path/to/file.pfc'
	// The '--' separator is required so argparse does not treat the path as a block id.
	std::string cmd = ShellEscape(binary) + " seek-blocks --blocks";
	for (uint32_t id : block_ids) {
		cmd += " " + std::to_string(id);
	}
	cmd += " -- " + ShellEscape(pfc_path) + " 2>/dev/null";

	FILE *pipe = popen(cmd.c_str(), "r");
	if (!pipe) {
		throw std::runtime_error("popen() failed — could not start PFC binary subprocess");
	}
	PipeGuard guard(pipe);

	std::vector<std::string> result;
	char buf[4096];
	std::string current_line;

	while (fgets(buf, sizeof(buf), pipe)) {
		current_line += buf;
		// fgets retains the trailing '\n'; accumulate partial reads until we have a full line.
		if (!current_line.empty() && current_line.back() == '\n') {
			current_line.pop_back();
			result.push_back(std::move(current_line));
			current_line.clear();
		}
	}
	// Handle a final line if the subprocess did not end with '\n'.
	if (!current_line.empty()) {
		result.push_back(std::move(current_line));
	}

	int rc = guard.close();
	if (rc != 0) {
		// Decode the raw waitpid status to the actual process exit code.
		const int exit_code = WIFEXITED(rc) ? WEXITSTATUS(rc) : -1;

		if (exit_code == 127) {
			// POSIX: 127 = shell "command not found"
			throw std::runtime_error("PFC binary not found at '" + binary +
			                         "'.\n"
			                         "Install with:\n"
			                         "  curl -L https://github.com/ImpossibleForge/pfc-jsonl/releases/latest"
			                         "/download/pfc_jsonl-linux-x64 -o /usr/local/bin/pfc_jsonl"
			                         " && chmod +x /usr/local/bin/pfc_jsonl\n"
			                         "Or set: export PFC_JSONL_BINARY=/path/to/pfc_jsonl");
		}
		if (exit_code == 1) {
			// PFC Community Mode daily limit exceeded (5 GB decompressed / UTC day)
			throw std::runtime_error("PFC Community Mode daily limit reached (5 GB decompressed / day).\n"
			                         "Wait until midnight UTC, or get an unlimited license: "
			                         "https://github.com/ImpossibleForge/pfc-jsonl");
		}
		throw std::runtime_error("PFC binary exited with code " + std::to_string(exit_code) +
		                         " — verify PFC-JSONL v3.4+ is installed and the .pfc file is not corrupt");
	}

	return result;
}

#endif // !_WIN32 && !__EMSCRIPTEN__

// ─── Bind ─────────────────────────────────────────────────────────────────────

static unique_ptr<FunctionData> PFCBind(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.empty()) {
		throw BinderException("read_pfc_jsonl requires a file path argument");
	}

	auto bind_data = make_uniq<PFCBindData>();
	bind_data->pfc_path = input.inputs[0].GetValue<std::string>();
	bind_data->bidx_path = bind_data->pfc_path + ".bidx";

	if (bind_data->pfc_path.empty()) {
		throw BinderException("read_pfc_jsonl: path must not be empty");
	}

	// Verify the .pfc file is accessible before attempting anything else.
	// This produces a clearer error than a failed popen() or a missing .bidx message
	// when the user simply has a typo in the path.
	{
		FILE *probe = std::fopen(bind_data->pfc_path.c_str(), "rb");
		if (!probe) {
			throw BinderException("Cannot open PFC file: " + bind_data->pfc_path);
		}
		std::fclose(probe);
	}

	// Optional timestamp range parameters for block-level filtering.
	auto it_from = input.named_parameters.find("ts_from");
	if (it_from != input.named_parameters.end()) {
		bind_data->ts_from = it_from->second.GetValue<int64_t>();
	}
	auto it_to = input.named_parameters.find("ts_to");
	if (it_to != input.named_parameters.end()) {
		bind_data->ts_to = it_to->second.GetValue<int64_t>();
	}

	// Validate range: when both non-zero bounds are provided, ts_from must not exceed ts_to.
	if (bind_data->ts_from != 0 && bind_data->ts_to != 0 && bind_data->ts_from > bind_data->ts_to) {
		throw BinderException("read_pfc_jsonl: ts_from (" + std::to_string(bind_data->ts_from) +
		                      ") must be <= ts_to (" + std::to_string(bind_data->ts_to) + ")");
	}

	// Read the binary index — throws with an actionable message if missing or malformed.
	bind_data->index = PFCIndex::ReadFromFile(bind_data->bidx_path);

	// Output schema: single VARCHAR column "line"
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("line");

	return std::move(bind_data);
}

// ─── Init ─────────────────────────────────────────────────────────────────────

static unique_ptr<GlobalTableFunctionState> PFCInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<PFCBindData>();
	auto state = make_uniq<PFCGlobalState>();

	std::string binary = FindPFCBinary();

	// Block-level filter: skip blocks outside the requested timestamp range.
	// Returns all block IDs when ts_from == ts_to == 0 (no filter requested).
	auto block_ids = bind_data.index.GetBlocksInRange(bind_data.ts_from, bind_data.ts_to);

	state->lines = CallPFCSeekBlocks(binary, bind_data.pfc_path, block_ids);
	state->line_idx = 0;
	state->done = state->lines.empty();

	return std::move(state);
}

// ─── Scan ─────────────────────────────────────────────────────────────────────

static void PFCScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<PFCGlobalState>();

	if (state.done) {
		output.SetCardinality(0);
		return;
	}

	idx_t count = 0;
	auto &col = output.data[0];

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
	TableFunction tf("read_pfc_jsonl", {LogicalType::VARCHAR}, PFCScan, PFCBind, PFCInit);

	// Named parameters for block-level timestamp filtering.
	// ts_from / ts_to: Unix epoch seconds (BIGINT). 0 means "no bound" (open interval).
	// Blocks whose timestamp range does not overlap [ts_from, ts_to] are skipped entirely.
	tf.named_parameters["ts_from"] = LogicalType::BIGINT;
	tf.named_parameters["ts_to"] = LogicalType::BIGINT;

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
