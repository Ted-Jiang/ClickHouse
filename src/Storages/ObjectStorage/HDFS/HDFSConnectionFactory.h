#pragma once

#include "config.h"

#if USE_HDFS

#include "arrow/io/hdfs_internal.h"
#include "arrow/io/hdfs.h"

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <memory>
#include <string>


namespace DB
{

namespace ErrorCodes
{
    extern const int HDFS_ERROR;
}

/// RAII wrapper for HDFS filesystem handle
namespace detail
{

struct HDFSFsDeleter
{
    arrow::io::internal::LibHdfsShim* driver_;

    /// Default constructor for uninitialized state
    HDFSFsDeleter() : driver_(nullptr) {}

    explicit HDFSFsDeleter(arrow::io::internal::LibHdfsShim* driver)
        : driver_(driver)
    {
    }

    void operator()(hdfsFS fs_ptr)
    {
        if (fs_ptr && driver_)
        {
            driver_->hdfsDisconnect(fs_ptr);
            LOG_TRACE(getLogger("HDFSClient"), "HDFS connection closed");
        }
    }
};

}

using HDFSFSPtr = std::unique_ptr<std::remove_pointer_t<hdfsFS>, detail::HDFSFsDeleter>;

/// Helper function to get HDFS error message
inline std::string getHDFSError(arrow::io::internal::LibHdfsShim* driver, hdfsFS fs)
{
    if (!driver || !fs)
        return "Invalid HDFS driver or filesystem";

    /// Try to get error from driver if available, for now the libhdfs API doesn't provide a standard way to retrieve the last error message,
    /// so this is a placeholder for future implementation if such functionality is added.
    // if (driver->hdfsGetLastError())
    // {
    //     const char* error = driver->hdfsGetLastError();
    //     if (error && *error)
    //         return std::string(error);
    // }

    /// Fallback to errno if available
    if (errno != 0)
        return std::strerror(errno);

    return "Unknown HDFS error";
}

/// Singleton factory class for creating HDFS connections
class HDFSConnectionFactory
{
public:
    struct Connection
    {
        arrow::io::internal::LibHdfsShim* driver;
        HDFSFSPtr fs;
    };

    /// Get singleton instance
    static HDFSConnectionFactory & instance()
    {
        static HDFSConnectionFactory factory;
        return factory;
    }

    /// Create HDFS connection with given configuration
    Connection createConnection() const
    {
        chassert(driver_ != nullptr);
        hdfsBuilder* builder = driver_->NewBuilder();
        if (!builder)
        {
            throw Exception(ErrorCodes::HDFS_ERROR, "Failed to create HDFS builder");
        }

        /// Configure NameNode
        /// See https://github.com/facebookincubator/velox/blob/main/velox/external/hdfs/hdfs.h#L289
        // If the string given is 'default', the default NameNode
        // configuration will be used (from the XML configuration files)
        driver_->BuilderSetNameNode(builder, "default");
        driver_->BuilderSetForceNewInstance(builder);

        /// Connect to HDFS
        hdfsFS hdfsClient = driver_->BuilderConnect(builder);

        if (hdfsClient == nullptr)
        {
            throw Exception(ErrorCodes::HDFS_ERROR, "Unable to connect to HDFS cluster with NameNode!");
        }

        LOG_INFO(getLogger("HDFSClient"), "Successfully connected to HDFS NameNode.");
        Connection conn {driver_, HDFSFSPtr(hdfsClient, detail::HDFSFsDeleter(driver_))};

        return conn;
    }

    /// Delete copy constructor and assignment operator
    HDFSConnectionFactory(const HDFSConnectionFactory &) = delete;
    HDFSConnectionFactory & operator=(const HDFSConnectionFactory &) = delete;

private:
    arrow::io::internal::LibHdfsShim* driver_;

    /// Private constructor for singleton
    HDFSConnectionFactory()
    {
        arrow::Status status = ::arrow::io::internal::ConnectLibHdfs(&driver_);

        if (!status.ok())
        {
            throw Exception(
                ErrorCodes::HDFS_ERROR,
                "Unable to connect to HDFS library: {}",
                status.ToString());
        }
        LOG_INFO(getLogger("[HDFSConnectionFactory]"), "HDFS library driver initialized successfully");
    }

    /// Destructor
    ~HDFSConnectionFactory() = default;
};

}

#endif
