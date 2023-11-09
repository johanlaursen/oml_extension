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

// // Copied from SingleThreadedCSVFunction
// static void ReadOMLFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {

//   auto &func_data = (FunctionData &)*data_p.bind_data;

  
//   string filename = data_p.inputs[0].ToString();

//   std:: ifstream oml_file(filename);

//   string line;

//     // Logic to read and parse the OML file
//     // For simplicity, let's assume we're reading a file line by line
//     std::ifstream oml_file("your_file.oml");
//     string line;
//     idx_t current_row = 0;

//     if (oml_file.is_open()) {
//         while (getline(oml_file, line) && current_row < STANDARD_VECTOR_SIZE) {
//             // Parse the line and store it in the output
//             output.SetValue(0, current_row, Value(line));
//             current_row++;
//         }
//         oml_file.close();
//     }
//     output.SetCardinality(current_row);
// }

// placeholder until actual function is implemented
static void dummyReadOMLFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {}

static unique_ptr<FunctionData> ReadOMLBind(ClientContext &context,
 TableFunctionBindInput &input,
 vector<LogicalType> &return_types, vector<string> &names) {
  
  string filename = input.inputs[0].ToString();

  std:: ifstream oml_file(filename);

  string line;

  if (oml_file.is_open()){
    while (getline(oml_file, line)){
      if (line.empty()) {
            // Stop processing if a blank line is encountered
            break;
        }

      string word;
      std::stringstream ss(line);

      ss >> word;
      if (word == "schema:"){
        // process schema line
        string schema_definition;
        while (ss >> schema_definition){
          size_t colon_pos = schema_definition.find(':');
          if (colon_pos == string::npos) {
          // If no colon found, skip to the next word
          continue;
          }
          if (colon_pos != string::npos) {
              string var_name = schema_definition.substr(0, colon_pos);
              string var_type = schema_definition.substr(colon_pos + 1);
              
              // Save the variable name
              names.push_back(var_name);
              
              // Map the OML type to DuckDB LogicalType
              if (var_type == "string") {
                  return_types.push_back(LogicalType::VARCHAR);
              } else if (var_type == "uint32") {
                  return_types.push_back(LogicalType::UINTEGER);
              } else if (var_type == "double") {
                  return_types.push_back(LogicalType::DOUBLE);
              }   
            }
      

        }
      }
                                 
    }
      // Is this line needed?
    }
  return make_unique<FunctionData>(); 
  }

// This version attempts to copy ReadCSVBind and hardcode the names and types
static unique_ptr<FunctionData> ReadOMLBindStatic (
  ClientContext &context,TableFunctionBindInput &input, 
  vector<LogicalType> &return_types, vector<string> &names) {

      // We want to only provide read_oml with filename so set this to true and then overwrite the auto detection
      input.named_parameters["auto_detect"] = Value::BOOLEAN(true);

      // until we know if we need to edit this struct we will copy the ReadCSVData
      // ReadCSVData inherits from BaseCSVData which inherits from TableFunctionData 
    	auto result = make_uniq<ReadCSVData>();
      // creates a reference to result.options so any changes to options is changes to result.options
	    auto &options = result->options;
      
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
      result->reader_bind = MultiFileReader::BindOptions(options.file_options, result->files, return_types, names);

      return std::move(result);

 }

// TableFunction ReadOMLTableFunction::GetFunction() {
//   TableFunction Power_Consumption_load("Power_Consumption_load",
//     {LogicalType::VARCHAR},
//     ReadOMLFunction,
//     ReadOMLBind,
//     ReadOMLInitGlobal,
//     ReadOMLInitLocal);

//   return Power_Consumption_load;
// }

// void ReadCSVTableFunction::RegisterFunction(BuiltinFunctions &set) {
// 	set.AddFunction(MultiFileReader::CreateFunctionSet(ReadOMLTableFunction::GetAutoFunction()));
// }

static void LoadInternal(DatabaseInstance &instance) {
    vector<LogicalType> argument_types;
    string name = "Power_Consumption_load";

    // Register a table function
    auto oml_parser_table_function = TableFunction(name, argument_types, dummyReadOMLFunction, ReadOMLBind);

    ExtensionUtil::RegisterFunction(instance, oml_parser_table_function);

}

// ScalarFunction
// inline void OmlParserScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
//     auto &name_vector = args.data[0];
//     UnaryExecutor::Execute<string_t, string_t>(
// 	    name_vector, result, args.size(),
// 	    [&](string_t name) {
// 			return StringVector::AddString(result, "OmlParser "+name.GetString()+" 🐥");;
//         });
// }

// inline void OmlParserOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
//     auto &name_vector = args.data[0];
//     UnaryExecutor::Execute<string_t, string_t>(
// 	    name_vector, result, args.size(),
// 	    [&](string_t name) {
// 			return StringVector::AddString(result, "OmlParser " + name.GetString() +
//                                                      ", my linked OpenSSL version is " +
//                                                      OPENSSL_VERSION_TEXT );;
//         });
// }



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
