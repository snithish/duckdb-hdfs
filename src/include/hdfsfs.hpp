#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/http_state.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/main/client_data.hpp"
#include <curl/curl.h>

namespace duckdb {

class HDFSFileHandle : public FileHandle {
public:
  HDFSFileHandle(FileSystem &fs, string path, uint8_t flags);
  ~HDFSFileHandle() override;
  // This two-phase construction allows subclasses more flexible setup.
  virtual void Initialize(FileOpener *opener);

  // File handle info
  uint8_t flags;
  idx_t length;
  time_t last_modified;

  // When using full file download, the full file will be written to a cached
  // file handle
  unique_ptr<CachedFileHandle> cached_file_handle;

  // Read info
  idx_t buffer_available;
  idx_t buffer_idx;
  idx_t file_offset;
  idx_t buffer_start;
  idx_t buffer_end;

  // Read buffer
  duckdb::unique_ptr<data_t[]> read_buffer;
  constexpr static idx_t READ_BUFFER_LEN = 1000000;

  shared_ptr<HTTPState> state;

public:
  void Close() override {}

protected:
  virtual void InitializeClient();
};

class HDFSFileSystem : public FileSystem {
public:
  static duckdb::unique_ptr<CURL> GetClient(const char *proto_host_port);
  static void ParseUrl(string &url, string &path_out,
                       string &proto_host_port_out);
  duckdb::unique_ptr<FileHandle>
  OpenFile(const string &path, uint8_t flags, FileLockType lock = DEFAULT_LOCK,
           FileCompressionType compression = DEFAULT_COMPRESSION,
           FileOpener *opener = nullptr) final;

  vector<string> Glob(const string &path,
                      FileOpener *opener = nullptr) override {
    return {path}; // FIXME
  }

  // FS methods
  void Read(FileHandle &handle, void *buffer, int64_t nr_bytes,
            idx_t location) override;
  int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
  void Write(FileHandle &handle, void *buffer, int64_t nr_bytes,
             idx_t location) override;
  int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
  void FileSync(FileHandle &handle) override;
  int64_t GetFileSize(FileHandle &handle) override;
  time_t GetLastModifiedTime(FileHandle &handle) override;
  bool FileExists(const string &filename) override;
  void Seek(FileHandle &handle, idx_t location) override;
  idx_t SeekPosition(FileHandle &handle) override;
  bool CanHandleFile(const string &fpath) override;
  bool CanSeek() override { return true; }
  bool OnDiskFile(FileHandle &handle) override { return false; }
  bool IsPipe(const string &filename) override { return false; }
  string GetName() const override { return "HDFSFileSystem"; }
  string PathSeparator(const string &path) override { return "/"; }
  static void Verify();

protected:
  virtual duckdb::unique_ptr<HDFSFileHandle>
  CreateHandle(const string &path, uint8_t flags, FileLockType lock,
               FileCompressionType compression, FileOpener *opener);
};

} // namespace duckdb
