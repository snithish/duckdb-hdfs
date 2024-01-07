
#include <hdfsfs.hpp>

namespace duckdb {
duckdb::unique_ptr<HDFSFileHandle>
HDFSFileSystem::CreateHandle(const string &path, uint8_t flags,
                             FileLockType lock, FileCompressionType compression,
                             FileOpener *opener) {
  return nullptr;
}

int64_t HDFSFileSystem::Write(FileHandle &handle, void *buffer,
                              int64_t nr_bytes) {
  return FileSystem::Write(handle, buffer, nr_bytes);
}
bool HDFSFileSystem::CanHandleFile(const string &fpath) {
  return fpath.rfind("webhdfs://", 0) == 0;
}
} // namespace duckdb
