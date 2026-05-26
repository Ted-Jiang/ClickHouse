#pragma once

#include "config.h"

#if USE_HDFS

#include "arrow/io/hdfs_internal.h"

#include <base/defines.h>
#include <Common/logger_useful.h>

#include <string>
#include <utility>


namespace DB
{

/// RAII wrapper for HDFS file handle
/// Automatically closes the file when destroyed, preventing resource leaks
class HDFSFileHandle
{
public:
    /// Constructor - takes ownership of file handle
    HDFSFileHandle(
        hdfsFS fs,
        hdfsFile file,
        arrow::io::internal::LibHdfsShim* driver,
        std::string path)
        : fs_(fs)
        , file_(file)
        , driver_(driver)
        , path_(std::move(path))
    {
        chassert(fs_ != nullptr);
        chassert(file_ != nullptr);
        chassert(driver_ != nullptr);
    }

    /// Destructor - automatically closes file
    ~HDFSFileHandle() noexcept
    {
        if (file_ && fs_ && driver_)
        {
            int result = driver_->hdfsCloseFile(fs_, file_);
            if (result != 0)
            {
                LOG_ERROR(getLogger("HDFSFileHandle"), "Failed to close HDFS file: {}", path_);
            }
            else
            {
                LOG_TRACE(getLogger("HDFSFileHandle"), "HDFS file closed: {}", path_);
            }
        }else
        {
            LOG_WARNING(
                getLogger("HDFSFileHandle"),
                "HDFS file closed skip some thing is null {} ,{}, {}",
                (file_ ? "file not null" : "file null"),
                (fs_ ? "fs not null" : "fs null"),
                (driver_ ? "driver not null" : "driver null"));
        }
    }

    /// Non-copyable
    HDFSFileHandle(const HDFSFileHandle&) = delete;
    HDFSFileHandle& operator=(const HDFSFileHandle&) = delete;

    /// Movable
    HDFSFileHandle(HDFSFileHandle&& other) noexcept
        : fs_(other.fs_)
        , file_(other.file_)
        , driver_(other.driver_)
        , path_(std::move(other.path_))
    {
        other.file_ = nullptr;
        other.fs_ = nullptr;
        other.driver_ = nullptr;
    }

    HDFSFileHandle& operator=(HDFSFileHandle&& other) noexcept
    {
        if (this != &other)
        {
            /// Close current file if any
            if (file_ && fs_ && driver_)
            {
                driver_->hdfsCloseFile(fs_, file_);
            }

            /// Move from other
            fs_ = other.fs_;
            file_ = other.file_;
            driver_ = other.driver_;
            path_ = std::move(other.path_);

            other.file_ = nullptr;
            other.fs_ = nullptr;
            other.driver_ = nullptr;
        }
        return *this;
    }

    /// Get raw file handle
    hdfsFile get() const
    {
        return file_;
    }

    /// Check if handle is valid
    bool isValid() const
    {
        return file_ != nullptr;
    }

    /// Get file path
    const std::string& path() const
    {
        return path_;
    }

private:
    hdfsFS fs_;
    hdfsFile file_;
    arrow::io::internal::LibHdfsShim* driver_;
    std::string path_;
};

}

#endif
