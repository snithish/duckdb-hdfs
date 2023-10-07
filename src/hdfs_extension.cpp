#define DUCKDB_EXTENSION_MAIN

#include "hdfs_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

inline void HdfsScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name) {
			return StringVector::AddString(result, "Hdfs "+name.GetString()+" 🐥");;
        });
}

inline void HdfsOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name) {
			return StringVector::AddString(result, "Hdfs " + name.GetString() +
                                                     ", my linked OpenSSL version is " +
                                                     OPENSSL_VERSION_TEXT );;
        });
}

static void LoadInternal(DatabaseInstance &instance) {
    // Register a scalar function
    auto hdfs_scalar_function = ScalarFunction("hdfs", {LogicalType::VARCHAR}, LogicalType::VARCHAR, HdfsScalarFun);
    ExtensionUtil::RegisterFunction(instance, hdfs_scalar_function);

    // Register another scalar function
    auto hdfs_openssl_version_scalar_function = ScalarFunction("hdfs_openssl_version", {LogicalType::VARCHAR},
                                                LogicalType::VARCHAR, HdfsOpenSSLVersionScalarFun);
    ExtensionUtil::RegisterFunction(instance, hdfs_openssl_version_scalar_function);
}

void HdfsExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string HdfsExtension::Name() {
	return "hdfs";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void hdfs_init(duckdb::DatabaseInstance &db) {
	LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *hdfs_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
