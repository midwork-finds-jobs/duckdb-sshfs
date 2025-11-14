#pragma once

#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"
#include "ssh_client.hpp"
#include <memory>
#include <unordered_map>

namespace duckdb {

class SSHFSFileSystem : public FileSystem {
public:
  SSHFSFileSystem();
  ~SSHFSFileSystem() override = default;

  // FileSystem interface implementation
  unique_ptr<FileHandle>
  OpenFile(const string &path, FileOpenFlags flags,
           optional_ptr<FileOpener> opener = nullptr) override;

  void Write(FileHandle &handle, void *buffer, int64_t nr_bytes,
             idx_t location) override;

  int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override;

  int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;

  void Read(FileHandle &handle, void *buffer, int64_t nr_bytes,
            idx_t location) override;

  void Seek(FileHandle &handle, idx_t location) override;

  void Reset(FileHandle &handle) override;

  idx_t SeekPosition(FileHandle &handle) override;

  void Truncate(FileHandle &handle, int64_t new_size) override;

  void FileSync(FileHandle &handle) override;

  bool FileExists(const string &filename,
                  optional_ptr<FileOpener> opener = nullptr) override;

  void RemoveFile(const string &filename,
                  optional_ptr<FileOpener> opener = nullptr) override;

  void MoveFile(const string &source, const string &target,
                optional_ptr<FileOpener> opener = nullptr) override;

  void CreateDirectory(const string &directory,
                       optional_ptr<FileOpener> opener = nullptr) override;

  bool DirectoryExists(const string &directory,
                       optional_ptr<FileOpener> opener = nullptr) override;

  void RemoveDirectory(const string &directory,
                       optional_ptr<FileOpener> opener = nullptr) override;

  vector<OpenFileInfo> Glob(const string &path,
                            FileOpener *opener = nullptr) override;

  bool CanHandleFile(const string &fpath) override;

  timestamp_t GetLastModifiedTime(FileHandle &handle) override;

  int64_t GetFileSize(FileHandle &handle) override;

  bool CanSeek() override;

  bool OnDiskFile(FileHandle &handle) override;

  string GetName() const override { return "SSHFSFileSystem"; }

  // SSHFS specific methods
  std::shared_ptr<SSHClient> GetOrCreateClient(const string &path,
                                               FileOpener *opener);
  SSHConnectionParams ParseURL(const string &path, FileOpener *opener);

private:
  // Connection pool
  std::unordered_map<string, std::shared_ptr<SSHClient>> client_pool;
  std::mutex pool_mutex;

  string ExtractConnectionKey(const SSHConnectionParams &params);
};

} // namespace duckdb
