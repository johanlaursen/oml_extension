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




  

// Not sure if GlobalTableFunctionState is needed as it appears to only be used for multiple files
// struct OMLState : public GlobalTableFunctionState {
// 	explicit OMLState(idx_t total_files) : total_files(total_files), next_file(0), progress_in_files(0) {
// 	}

// 	mutex csv_lock;
// 	unique_ptr<BufferedCSVReader> initial_reader;
// 	//! The total number of files to read from
// 	idx_t total_files;
// 	//! The index of the next file to read (i.e. current file + 1)
// 	atomic<idx_t> next_file;
// 	//! How far along we are in reading the current set of open files
// 	//! This goes from [0...next_file] * 100
// 	atomic<idx_t> progress_in_files;
// 	//! The set of SQL types
// 	vector<LogicalType> csv_types;
// 	//! The set of SQL names to be read from the file
// 	vector<string> csv_names;
// 	//! The column ids to read
// 	vector<column_t> column_ids;

// 	idx_t MaxThreads() const override {
// 		return total_files;
// 	}

// 	double GetProgress(const ReadCSVData &bind_data) const {
// 		D_ASSERT(total_files == bind_data.files.size());
// 		D_ASSERT(progress_in_files <= total_files * 100);
// 		return (double(progress_in_files) / double(total_files));
// 	}

// 	unique_ptr<BufferedCSVReader> GetCSVReader(ClientContext &context, ReadCSVData &bind_data, idx_t &file_index,
// 	                                           idx_t &total_size) {
// 		return GetCSVReaderInternal(context, bind_data, file_index, total_size);
// 	}

// private:
// 	unique_ptr<BufferedCSVReader> GetCSVReaderInternal(ClientContext &context, ReadCSVData &bind_data,
// 	                                                   idx_t &file_index, idx_t &total_size) {
// 		CSVReaderOptions options;
// 		{
// 			lock_guard<mutex> l(csv_lock);
// 			if (initial_reader) {
// 				total_size = initial_reader->file_handle ? initial_reader->file_handle->FileSize() : 0;
// 				return std::move(initial_reader);
// 			}
// 			if (next_file >= total_files) {
// 				return nullptr;
// 			}
// 			options = bind_data.options;
// 			file_index = next_file;
// 			next_file++;
// 		}

//     // Can potentially delete some of the code below as we aren't using union_readers
// 		// reuse csv_readers was created during binding (comment from original code)
// 		unique_ptr<BufferedCSVReader> result;
// 		if (file_index < bind_data.union_readers.size() && bind_data.union_readers[file_index]) {
// 			result = std::move(bind_data.union_readers[file_index]);
// 		} else {
// 			auto union_by_name = options.file_options.union_by_name;
// 			options.file_path = bind_data.files[file_index];
// 			result = make_uniq<BufferedCSVReader>(context, std::move(options), csv_types);
// 			if (!union_by_name) {
// 				result->names = csv_names;
// 			}
// 			MultiFileReader::InitializeReader(*result, bind_data.options.file_options, bind_data.reader_bind,
// 			                                  bind_data.return_types, bind_data.return_names, column_ids, nullptr,
// 			                                  bind_data.files.front(), context);
// 		}
// 		total_size = result->file_handle->FileSize();
// 		return result;
// 	}
// };


// from duckdb/src/function/table/read_csv.cpp:719~734
// struct OMLLocalState : public LocalTableFunctionState {
// public:
// 	explicit OMLLocalState() : bytes_read(0), total_size(0), current_progress(0), file_index(0) {
// 	}

// 	//! The CSV reader
// 	unique_ptr<BufferedCSVReader> csv_reader;
// 	//! The current amount of bytes read by this reader
// 	idx_t bytes_read;
// 	//! The total amount of bytes in the file
// 	idx_t total_size;
// 	//! The current progress from 0..100
// 	idx_t current_progress;
// 	//! The file index of this reader
// 	idx_t file_index;
// };

// // from SingleThreadedCSVInit duckdb/src/function/table/read_csv.cpp:736~776
// static unique_ptr<GlobalTableFunctionState> OMLInit(ClientContext &context,
//                                                                   TableFunctionInitInput &input) {
// 	auto &bind_data = input.bind_data->CastNoConst<ReadCSVData>();
// 	auto result = make_uniq<OMLState>(bind_data.files.size());

//   // Potentially remove this
// 	if (bind_data.files.empty()) {
// 		// This can happen when a filename based filter pushdown has eliminated all possible files for this scan.
// 		return std::move(result);
// 	} else {
//     // Isn't this already covered in bind function???
// 		bind_data.options.file_path = bind_data.files[0];
// 		result->initial_reader = make_uniq<BufferedCSVReader>(context, bind_data.options, bind_data.csv_types);
		
//     // Commented out if as not using union_by_name
//     // if (!bind_data.options.file_options.union_by_name) {
// 			result->initial_reader->names = bind_data.csv_names;
// 		// }

//     // TODO can probably comment out if statement as auto_detect should always be true for all oml_read functions
// 		if (bind_data.options.auto_detect) {
//       // TODO find out where initial_reader is intialized
// 			bind_data.options = result->initial_reader->options;
// 		}
// 	}
// 	MultiFileReader::InitializeReader(*result->initial_reader, bind_data.options.file_options, bind_data.reader_bind,
// 	                                  bind_data.return_types, bind_data.return_names, input.column_ids, input.filters,
// 	                                  bind_data.files.front(), context);
	
//   // Shouldn't have union_readers as we arent doing any union bind stuff so commenting out
//   // for (auto &reader : bind_data.union_readers) {
// 	// 	if (!reader) {
// 	// 		continue;
// 	// 	}
// 	// 	MultiFileReader::InitializeReader(*reader, bind_data.options.file_options, bind_data.reader_bind,
// 	// 	                                  bind_data.return_types, bind_data.return_names, input.column_ids,
// 	// 	                                  input.filters, bind_data.files.front(), context);
// 	// }

//   // TODO Is this created by bind function?
// 	result->column_ids = input.column_ids;

//   // Not using union_by_name so commenting out
// 	// if (!bind_data.options.file_options.union_by_name) {
// 	// 	// if we are reading multiple files - run auto-detect only on the first file
// 	// 	// UNLESS union by name is turned on - in that case we assume that different files have different schemas
// 	// 	// as such, we need to re-run the auto detection on each file
// 	// 	bind_data.options.auto_detect = false;
// 	// }
// 	result->csv_types = bind_data.csv_types;
// 	result->csv_names = bind_data.csv_names;
// 	result->next_file = 1;
// 	return std::move(result);
// }

// unique_ptr<LocalTableFunctionState> OMLInitLocal(ExecutionContext &context,
//                                                                    TableFunctionInitInput &input,
//                                                                    GlobalTableFunctionState *global_state_p) {
// 	auto &bind_data = input.bind_data->CastNoConst<ReadCSVData>();
// 	auto &data = global_state_p->Cast<OMLState>();
// 	auto result = make_uniq<OMLLocalState>();
// 	result->csv_reader = data.GetCSVReader(context.client, bind_data, result->file_index, result->total_size);
// 	return std::move(result);
// }

// static void OMLFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
// 	auto &bind_data = data_p.bind_data->CastNoConst<ReadCSVData>();
// 	auto &data = data_p.global_state->Cast<OMLState>();
// 	auto &lstate = data_p.local_state->Cast<OMLLocalState>();
// 	if (!lstate.csv_reader) {
// 		// no csv_reader was set, this can happen when a filename-based filter has filtered out all possible files
// 		return;
// 	}

// 	do {
// 		lstate.csv_reader->ParseCSV(output);
// 		// update the number of bytes read
// 		D_ASSERT(lstate.bytes_read <= lstate.csv_reader->bytes_in_chunk);
// 		auto bytes_read = MinValue<idx_t>(lstate.total_size, lstate.csv_reader->bytes_in_chunk);
// 		auto current_progress = lstate.total_size == 0 ? 100 : 100 * bytes_read / lstate.total_size;
// 		if (current_progress > lstate.current_progress) {
// 			if (current_progress > 100) {
// 				throw InternalException("Progress should never exceed 100");
// 			}
// 			data.progress_in_files += current_progress - lstate.current_progress;
// 			lstate.current_progress = current_progress;
// 		}
// 		if (output.size() == 0) {
// 			// exhausted this file, but we might have more files we can read
// 			auto csv_reader = data.GetCSVReader(context, bind_data, lstate.file_index, lstate.total_size);
// 			// add any left-over progress for this file to the progress bar
// 			if (lstate.current_progress < 100) {
// 				data.progress_in_files += 100 - lstate.current_progress;
// 			}
// 			// reset the current progress
// 			lstate.current_progress = 0;
// 			lstate.bytes_read = 0;
// 			lstate.csv_reader = std::move(csv_reader);
// 			if (!lstate.csv_reader) {
// 				// no more files - we are done
// 				return;
// 			}
// 			lstate.bytes_read = 0;
// 		} else {
// 			MultiFileReader::FinalizeChunk(bind_data.reader_bind, lstate.csv_reader->reader_data, output);
// 			break;
// 		}
// 	} while (true);
// }


// placeholder until actual function is implemented
// static void dummyReadOMLFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {}

// static unique_ptr<FunctionData> ReadOMLBindDynamic(ClientContext &context,
//  TableFunctionBindInput &input,
//  vector<LogicalType> &return_types, vector<string> &names) {
  
//   string filename = input.inputs[0].ToString();

//   std:: ifstream oml_file(filename);

//   string line;

//   if (oml_file.is_open()){
//     while (getline(oml_file, line)){
//       if (line.empty()) {
//             // Stop processing if a blank line is encountered
//             break;
//         }

//       string word;
//       std::stringstream ss(line);

//       ss >> word;
//       if (word == "schema:"){
//         // process schema line
//         string schema_definition;
//         while (ss >> schema_definition){
//           size_t colon_pos = schema_definition.find(':');
//           if (colon_pos == string::npos) {
//           // If no colon found, skip to the next word
//           continue;
//           }
//           if (colon_pos != string::npos) {
//               string var_name = schema_definition.substr(0, colon_pos);
//               string var_type = schema_definition.substr(colon_pos + 1);
              
//               // Save the variable name
//               names.push_back(var_name);
              
//               // Map the OML type to DuckDB LogicalType
//               if (var_type == "string") {
//                   return_types.push_back(LogicalType::VARCHAR);
//               } else if (var_type == "uint32") {
//                   return_types.push_back(LogicalType::UINTEGER);
//               } else if (var_type == "double") {
//                   return_types.push_back(LogicalType::DOUBLE);
//               }   
//             }
      

//         }
//       }
                                 
//     }
//       // Is this line needed?
//     }
//   return make_unique<FunctionData>(); 
//   }

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
