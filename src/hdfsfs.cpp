#include "hdfsfs.hpp"

using namespace std;

namespace duckdb {
bool HDFSFileSystem::CanHandleFile(const string &fpath) {
  return fpath.rfind("webhdfs://", 0) == 0 || fpath.rfind("hdfs://", 0) == 0;
}

duckdb::unique_ptr<duckdb::FileHandle>
HDFSFileSystem::OpenFile(const string &path, uint8_t flags, FileLockType lock,
                         FileCompressionType compression, FileOpener *opener) {
  D_ASSERT(compression == FileCompressionType::UNCOMPRESSED);
  return duckdb::make_uniq<HDFSFileHandle>(*this, path, flags, opener);
}

::HDFSFileHandle(FileSystem &fs, string path, uint8_t flags, FileOpener *opener)
    : FileHandle(fs, path), flags(flags) {
  if (flags & FileFlags::FILE_FLAGS_WRITE) {
    throw IOException("WebHDFS Extension: Does not support writes");
  }
  Value value;
  proxy_url = "No Proxy";
  if (FileOpener::TryGetCurrentSetting(opener, "hdfs_namenode_url", value)) {
    namenode_url = value.GetValue<std::string>();
  }
  if (FileOpener::TryGetCurrentSetting(opener, "hdfs_proxy", value)) {
    proxy_url = value.GetValue<std::string>();
  }

  D_ASSERT(fs.CanHandleFile(path));

  http_path = path;
}
} // namespace duckdb
