#define DUCKDB_EXTENSION_MAIN

#include "oml_parser_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/table/read_csv.hpp"

#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include <iostream>
#include <fstream>
#include <sstream>
#include <string>


// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {








// This version attempts to copy ReadCSVBind and hardcode the names and types
static unique_ptr<FunctionData> ReadOMLBindStatic (
  ClientContext &context,TableFunctionBindInput &input, 
  vector<LogicalType> &return_types, vector<string> &names) {

      // We want to only provide read_oml with filename so set this to true and then overwrite the auto detection
      // input.named_parameters["auto_detect"] = Value::BOOLEAN(true);
      
      // 

      // until we know if we need to edit this struct we will copy the ReadCSVData
      // ReadCSVData inherits from BaseCSVData which inherits from TableFunctionData 
    	auto result = make_uniq<ReadCSVData>();
      // creates a reference to result.options so any changes to options is changes to result.options
	    auto &options = result->options;

      // Setup options that we need
      // Not sure if needed as it appears to mainly be used in ReadCVSBind which this function is replacing
      // TODO should probably be false as we aren't using FromNamedParameters
      options.auto_detect = true;
      // oml file starts on line 10 so skip lines to reach that point
      options.SetSkipRows(9);
      string decimal_separator = ".";
      options.decimal_separator = decimal_separator;
      string delimiter = "\t";
      options.SetDelimiter(delimiter);

      // Don't need to add any hive_partitioning as it defaults to false duckdb/src/include/duckdb/common/multi_file_reader_options.hpp:21
      // Same for union_by_name


      // TODO might need a new line option as well not sure.


      // Hard code the following types and names:
      // experiment_id VARCHAR,
      return_types.push_back(LogicalType::VARCHAR);
      names.push_back("experiment_id");
      // node_id VARCHAR,
      return_types.push_back(LogicalType::VARCHAR);
      names.push_back("node_id");
      // node_id_seq VARCHAR,
      return_types.push_back(LogicalType::VARCHAR);
      names.push_back("node_id_seq");
      // time_sec VARCHAR NOT NULL,
      return_types.push_back(LogicalType::VARCHAR);
      names.push_back("time_sec");
      // time_usec VARCHAR NOT NULL,
      return_types.push_back(LogicalType::VARCHAR);
      names.push_back("time_usec");
      // power REAL NOT NULL,
      return_types.push_back(LogicalType::FLOAT);
      names.push_back("power");
      // current REAL NOT NULL,
      return_types.push_back(LogicalType::FLOAT);
      names.push_back("current");
      // voltage REAL NOT NULL
      return_types.push_back(LogicalType::FLOAT);
      names.push_back("voltage");


      // updates the result.files attribute to include files given to read_oml
	    result->files = MultiFileReader::GetFileList(context, input.inputs[0], "CSV");
      options.file_path = result->files[0];
      // OpenCSV expects options to have file_path and compression attributes. compression should be automatically handled
      auto file_handle = BaseCSVReader::OpenCSV(context, options);
      // initialize buffer manager
      result->buffer_manager = make_shared<CSVBufferManager>(context, std::move(file_handle), options);
      
      // TODO Do I need all 4 ????
      result->csv_types = return_types;
      result->csv_names = names;
      result->return_types = return_types;
      result->return_names = names;

      // Not sure if needed. read_csv also has option union_by_name and this is the other option. 
      // This checks if filename is a column name and adds filename to return_types and names
      // This allows MultiFileReader to handle multiple files
      // Also does HivePartitioning stuff, so options should hopefully not have hivepartitioning
      result->reader_bind = MultiFileReader::BindOptions(options.file_options, result->files, return_types, names);

      // FinalizeRead appears to mainly be a way to throw exceptions. Including it here to hopefully have some error handling in case any mistakes were made
      result->FinalizeRead(context);


      return std::move(result);

 }



static void LoadInternal(DatabaseInstance &instance) {
    string name = "read_oml_static";

    // Register a table function
    // auto oml_parser_table_function = TableFunction(name, {LogicalType::VARCHAR}, OMLFunction, ReadOMLBindStatic, OMLInit, OMLInitLocal);

    auto read_oml_static = ReadCSVTableFunction::GetFunction();
    read_oml_static.name = "read_oml_static";
    read_oml_static.bind = ReadOMLBindStatic;
    
    ExtensionUtil::RegisterFunction(instance, read_oml_static);

}




void OmlParserExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string OmlParserExtension::Name() {
	return "oml_parser";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void oml_parser_init(duckdb::DatabaseInstance &db) {
	LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *oml_parser_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
