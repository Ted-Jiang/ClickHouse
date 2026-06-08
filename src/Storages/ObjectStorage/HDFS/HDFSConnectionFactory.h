#pragma once

#include "config.h"

#if USE_HDFS

#include "arrow/io/hdfs_internal.h"
#include "arrow/io/hdfs.h"

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <cerrno>
#include <cstring>
#include <memory>
#include <string>


namespace ProfileEvents
{
    extern const Event HDFSConnectionsCreated;
    extern const Event HDFSConnectionsReused;
    extern const Event HDFSConnectionErrors;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int HDFS_ERROR;
}


using HDFSFSPtr = std::shared_ptr<std::remove_pointer_t<hdfsFS>>;

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

    /// Returns the singleton HDFS connection initialized once at startup.
    /// Thread-safe: shared_ptr copy is safe without a lock after construction.
    Connection createConnection()
    {
        ProfileEvents::increment(ProfileEvents::HDFSConnectionsReused);
        return Connection{driver_, shared_connection_};
    }

    void handleError(const String & underlying_err_str)
    {
        const int error_code = errno;
        const char * error_message = std::strerror(error_code);
        ProfileEvents::increment(ProfileEvents::HDFSConnectionErrors);
        LOG_ERROR(getLogger("HDFSClient"), "HDFS error in {}: {}.", underlying_err_str, error_message);
    }

    /// Delete copy constructor and assignment operator
    HDFSConnectionFactory(const HDFSConnectionFactory &) = delete;
    HDFSConnectionFactory & operator=(const HDFSConnectionFactory &) = delete;

private:
    // Pointer used as a singleton handle to dynamically loaded HDFS symbols.
    arrow::io::internal::LibHdfsShim* driver_;
    // From https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/filesystem/filesystem.html
    // the static FileSystem.get() may return a pre-existing instance shared across threads.
    // We rely on that: one connection is created at startup and reused for the process lifetime.
    HDFSFSPtr shared_connection_;

    /// Private constructor for Singleton - initializes both the driver and the HDFS connection eagerly.
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
        LOG_INFO(getLogger("HDFSConnectionFactory"), "HDFS library driver initialized successfully");

        shared_connection_ = createNewConnection();
        ProfileEvents::increment(ProfileEvents::HDFSConnectionsCreated);
        LOG_INFO(getLogger("HDFSConnectionFactory"), "HDFS connection initialized for process lifetime");
    }

    /// Destructor
    ~HDFSConnectionFactory()
    {
        shared_connection_.reset();
        LOG_DEBUG(getLogger("HDFSConnectionFactory"), "HDFS connection factory destroyed");
    }

    /// Create a new HDFS connection
    HDFSFSPtr createNewConnection()
    {
        chassert(driver_ != nullptr);
        hdfsBuilder* builder = driver_->NewBuilder();
        if (!builder)
        {
            throw Exception(ErrorCodes::HDFS_ERROR, "Failed to create HDFS builder");
        }

        /// Configure NameNode
        /// See https://github.com/facebookincubator/velox/blob/main/velox/external/hdfs/hdfs.h#L289
        /// If the string given is 'default', the default NameNode
        /// configuration will be used (from the XML configuration files)
        driver_->BuilderSetNameNode(builder, "default");
        driver_->BuilderSetForceNewInstance(builder);

        /// Connect to HDFS
        hdfsFS hdfsClient = driver_->BuilderConnect(builder);

        if (hdfsClient == nullptr)
        {
            throw Exception(ErrorCodes::HDFS_ERROR, "Unable to connect to HDFS cluster with NameNode!");
        }

        LOG_INFO(getLogger("HDFSClient"), "Successfully created new HDFS connection");

        /// Use shared_ptr with custom deleter for connection sharing
        HDFSFSPtr fs_ptr(hdfsClient, [driver = driver_](hdfsFS fs)
        {
            if (fs && driver)
            {
                driver->hdfsDisconnect(fs);
                LOG_WARNING(getLogger("HDFSClient"), "HDFS connection closed");
            }
        });

        return fs_ptr;
    }

};

}

#endif
