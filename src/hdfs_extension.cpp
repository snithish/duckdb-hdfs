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

struct memory {
  char *response;
  size_t size;
};

static size_t copy_response(void *data, size_t size, size_t nmemb,
                            void *buffer_out) {
  size_t realsize = size * nmemb;
  auto *mem = (struct memory *)buffer_out;

  void *ptr = realloc(mem->response, mem->size + realsize + 1);
  if (!ptr)
    return 0; /* out of memory! */

  mem->response = (char *)ptr;
  memcpy(&(mem->response[mem->size]), data, realsize);
  mem->size += realsize;
  mem->response[mem->size] = 0;

  return realsize;
}

string invoke_curl(int length) {
  CURL *curl;
  CURLcode res;

  /* get a curl handle */
  curl = curl_easy_init();
  struct memory chunk = {nullptr};
  if (curl) {
    /* First set the URL that is about to receive our POST. This URL can
       just as well be an https:// URL if that is what should receive the
       data. */

    //    select hdfs_curl('quakc') as result;
    std::string host = "http://localhost:14000";
    std::string url = host +
                      "/webhdfs/v1/user/meh/"
                      "some.csv?op=OPEN&offset=0&length=" +
                      to_string(length);
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, "false");
    curl_easy_setopt(curl, CURLOPT_PROXY, "socks5h://127.0.0.1:28081");
    curl_easy_setopt(curl, CURLOPT_HTTPAUTH, CURLAUTH_NEGOTIATE);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, copy_response);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)&chunk);

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
  std::string response_string;
  if (chunk.response != nullptr) {
    response_string.assign(chunk.response, chunk.size);
  }
  return response_string;
}
inline void HdfsCurlScalarFun(DataChunk &args, ExpressionState &state,
                              Vector &result) {
  auto &name_vector = args.data[0];
  auto &len_vector = args.data[1];
  BinaryExecutor::Execute<string_t, int, string_t>(
      name_vector, len_vector, result, args.size(),
      [&](string_t name, int length) {
        std::string response_string = invoke_curl(length);
        return StringVector::AddString(
            result, "Hdfs " + name.GetString() + " 🐥" + " curl " +
                        " : Web Response : " + response_string);
      });
}

static void LoadInternal(DatabaseInstance &instance) {
  // Register a scalar function
  auto hdfs_scalar_function = ScalarFunction(
      "hdfs", {LogicalType::VARCHAR}, LogicalType::VARCHAR, HdfsScalarFun);
  ExtensionUtil::RegisterFunction(instance, hdfs_scalar_function);

  // Register curl
  auto hdfs_curl_scalar_function =
      ScalarFunction("hdfs_curl", {LogicalType::VARCHAR, LogicalType::INTEGER},
                     LogicalType::VARCHAR, HdfsCurlScalarFun);
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
