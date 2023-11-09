#pragma once

#include "duckdb.hpp"
#include "duckdb/execution/operator/scan/csv/buffered_csv_reader.hpp"
#include "duckdb/execution/operator/scan/csv/csv_buffer.hpp"
#include "duckdb/execution/operator/scan/csv/csv_buffer_manager.hpp"
#include "duckdb/execution/operator/scan/csv/csv_file_handle.hpp"
#include "duckdb/execution/operator/scan/csv/csv_reader_options.hpp"
#include "duckdb/execution/operator/scan/csv/parallel_csv_reader.hpp"
#include "duckdb/function/built_in_functions.hpp"
#include "duckdb/function/scalar/strftime_format.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/execution/operator/scan/csv/csv_state_machine_cache.hpp"

namespace duckdb {

class OmlParserExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;
};


struct ReadOMLTableFunction {
	static TableFunction GetFunction();
	static void RegisterFunction(BuiltinFunctions &set);
};





} // namespace duckdb
