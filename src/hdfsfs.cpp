#include "hdfsfs.hpp"

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/http_state.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/function/scalar/strftime_format.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

#include <chrono>

namespace duckdb {

HDFSFileHandle::HDFSFileHandle(FileSystem &fs, string path, uint8_t flags)
    : FileHandle(fs, path), flags(flags), length(0), buffer_available(0),
      buffer_idx(0), file_offset(0), buffer_start(0), buffer_end(0) {}

unique_ptr<FileHandle>
HDFSFileSystem::OpenFile(const string &path, uint8_t flags, FileLockType lock,
                         FileCompressionType compression, FileOpener *opener) {
  D_ASSERT(compression == FileCompressionType::UNCOMPRESSED);

  auto handle = CreateHandle(path, flags, lock, compression, opener);
  handle->Initialize(opener);
  return std::move(handle);
}

// Buffered read from http file.
// Note that buffering is disabled when FileFlags::FILE_FLAGS_DIRECT_IO is set
void HDFSFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes,
                          idx_t location) {
  auto &hfh = (HDFSFileHandle &)handle;

  idx_t to_read = nr_bytes;
  idx_t buffer_offset = 0;

  // Don't buffer when DirectIO is set.
  if (hfh.flags & FileFlags::FILE_FLAGS_DIRECT_IO && to_read > 0) {
    hfh.buffer_available = 0;
    hfh.buffer_idx = 0;
    hfh.file_offset = location + nr_bytes;
    return;
  }

  if (location >= hfh.buffer_start && location < hfh.buffer_end) {
    hfh.file_offset = location;
    hfh.buffer_idx = location - hfh.buffer_start;
    hfh.buffer_available = (hfh.buffer_end - hfh.buffer_start) - hfh.buffer_idx;
  } else {
    // reset buffer
    hfh.buffer_available = 0;
    hfh.buffer_idx = 0;
    hfh.file_offset = location;
  }
  while (to_read > 0) {
    auto buffer_read_len = MinValue<idx_t>(hfh.buffer_available, to_read);
    if (buffer_read_len > 0) {
      D_ASSERT(hfh.buffer_start + hfh.buffer_idx + buffer_read_len <=
               hfh.buffer_end);
      memcpy((char *)buffer + buffer_offset,
             hfh.read_buffer.get() + hfh.buffer_idx, buffer_read_len);

      buffer_offset += buffer_read_len;
      to_read -= buffer_read_len;

      hfh.buffer_idx += buffer_read_len;
      hfh.buffer_available -= buffer_read_len;
      hfh.file_offset += buffer_read_len;
    }

    if (to_read > 0 && hfh.buffer_available == 0) {
      auto new_buffer_available =
          MinValue<idx_t>(hfh.READ_BUFFER_LEN, hfh.length - hfh.file_offset);
    }
  }
}

int64_t HDFSFileSystem::Read(FileHandle &handle, void *buffer,
                             int64_t nr_bytes) {
  auto &hfh = (HDFSFileHandle &)handle;
  idx_t max_read = hfh.length - hfh.file_offset;
  nr_bytes = MinValue<idx_t>(max_read, nr_bytes);
  Read(handle, buffer, nr_bytes, hfh.file_offset);
  return nr_bytes;
}

void HDFSFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes,
                           idx_t location) {
  throw NotImplementedException("Writing to HTTP files not implemented");
}

int64_t HDFSFileSystem::Write(FileHandle &handle, void *buffer,
                              int64_t nr_bytes) {
  auto &hfh = (HDFSFileHandle &)handle;
  Write(handle, buffer, nr_bytes, hfh.file_offset);
  return nr_bytes;
}

void HDFSFileSystem::FileSync(FileHandle &handle) {
  throw NotImplementedException("FileSync for HTTP files not implemented");
}

int64_t HDFSFileSystem::GetFileSize(FileHandle &handle) {
  auto &sfh = (HDFSFileHandle &)handle;
  return sfh.length;
}

time_t HDFSFileSystem::GetLastModifiedTime(FileHandle &handle) {
  auto &sfh = (HDFSFileHandle &)handle;
  return sfh.last_modified;
}

bool HDFSFileSystem::FileExists(const string &filename) {
  try {
    auto handle = OpenFile(filename.c_str(), FileFlags::FILE_FLAGS_READ);
    auto &sfh = (HDFSFileHandle &)*handle;
    if (sfh.length == 0) {
      return false;
    }
    return true;
  } catch (...) {
    return false;
  };
}

bool HDFSFileSystem::CanHandleFile(const string &fpath) {
  return fpath.rfind("hdfs://", 0) == 0;
}

void HDFSFileSystem::Seek(FileHandle &handle, idx_t location) {
  auto &sfh = (HDFSFileHandle &)handle;
  sfh.file_offset = location;
}

idx_t HDFSFileSystem::SeekPosition(FileHandle &handle) {
  auto &sfh = (HDFSFileHandle &)handle;
  return sfh.file_offset;
}
duckdb::unique_ptr<HDFSFileHandle>
HDFSFileSystem::CreateHandle(const string &path, uint8_t flags,
                             FileLockType lock, FileCompressionType compression,
                             FileOpener *opener) {
  D_ASSERT(compression == FileCompressionType::UNCOMPRESSED);
  return duckdb::make_uniq<HDFSFileHandle>(*this, path, flags);
}

void HDFSFileHandle::Initialize(FileOpener *opener) {
  InitializeClient();
  auto &hfs = (HDFSFileHandle &)file_system;
}

void HDFSFileHandle::InitializeClient() {
  string path_out, proto_host_port;
  //  setup hdfs client
}

HDFSFileHandle::~HDFSFileHandle() = default;
} // namespace duckdb
