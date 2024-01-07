#define DUCKDB_EXTENSION_MAIN

#include "hdfs_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include <curl/curl.h>

namespace duckdb {

inline void HdfsScalarFun(DataChunk &args, ExpressionState &state,
                          Vector &result) {
  auto &name_vector = args.data[0];
  UnaryExecutor::Execute<string_t, string_t>(
      name_vector, result, args.size(), [&](string_t name) {
        return StringVector::AddString(result,
                                       "Hdfs " + name.GetString() + " 🐥");
        ;
      });
}

inline void HdfsCurlScalarFun(DataChunk &args, ExpressionState &state,
                              Vector &result) {
  auto &name_vector = args.data[0];
  CURL *curl;
  CURLcode res;

  /* get a curl handle */
  curl = curl_easy_init();
  if (curl) {
    /* First set the URL that is about to receive our POST. This URL can
       just as well be an https:// URL if that is what should receive the
       data. */

    //    select hdfs_curl('quakc') as result;
    curl_easy_setopt(curl, CURLOPT_URL,
                     "http://localhost:14000/"
                     "webhdfs/v1/user/uname?op=GETFILESTATUS");

    curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, "false");
    curl_easy_setopt(curl, CURLOPT_PROXY, "socks5h://127.0.0.1:28081");
    curl_easy_setopt(curl, CURLOPT_HTTPAUTH, CURLAUTH_NEGOTIATE);

    /* Perform the request, res will get the return code */
    res = curl_easy_perform(curl);
    /* Check for errors */
    if (res != CURLE_OK)
      fprintf(stderr, "curl_easy_perform() failed: %s\n",
              curl_easy_strerror(res));
    /* always cleanup */
    curl_easy_cleanup(curl);
  }
  curl_global_cleanup();
  UnaryExecutor::Execute<string_t, string_t>(
      name_vector, result, args.size(), [&](string_t name) {
        return StringVector::AddString(result, "Hdfs " + name.GetString() +
                                                   " 🐥" + " curl " + " ");
        ;
      });
}

static void LoadInternal(DatabaseInstance &instance) {
  // Register a scalar function
  auto hdfs_scalar_function = ScalarFunction(
      "hdfs", {LogicalType::VARCHAR}, LogicalType::VARCHAR, HdfsScalarFun);
  ExtensionUtil::RegisterFunction(instance, hdfs_scalar_function);

  // Register curl
  auto hdfs_curl_scalar_function =
      ScalarFunction("hdfs_curl", {LogicalType::VARCHAR}, LogicalType::VARCHAR,
                     HdfsCurlScalarFun);
  ExtensionUtil::RegisterFunction(instance, hdfs_curl_scalar_function);
}

void HdfsExtension::Load(DuckDB &db) { LoadInternal(*db.instance); }
std::string HdfsExtension::Name() { return "hdfs"; }

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
