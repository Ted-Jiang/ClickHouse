#pragma once

#include "config.h"

#if USE_HDFS

#include "arrow/io/hdfs_internal.h"
#include "arrow/io/hdfs.h"

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>

#include <Poco/URI.h>

#include <cerrno>
#include <cstring>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>


namespace ProfileEvents
{
    extern const Event HDFSConnectionsCreated;
    extern const Event HDFSConnectionsReused;
    extern const Event HDFSConnectionErrors;
    extern const Event HDFSConnectionFailures;
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

/// Singleton factory that manages one hdfsFS connection per NameNode.
///
/// Design mirrors fs-hdfs (https://github.com/datafusion-contrib/fs-hdfs):
///   - Key  : scheme+authority extracted from the URI (e.g. "hdfs://nn:8020",
///             "viewfs://cluster", or "default")
///   - Value: shared_ptr<hdfsFS> so multiple readers can hold a reference
///             without preventing cleanup when the factory is destroyed.
///   - Lock : std::shared_mutex – concurrent reads share, writes are exclusive.
class HDFSConnectionFactory
{
public:
    struct Connection
    {
        arrow::io::internal::LibHdfsShim* driver;
        HDFSFSPtr fs;
    };

    static HDFSConnectionFactory & instance()
    {
        static HDFSConnectionFactory factory;
        return factory;
    }

    void validNameNodeSchema(const std::string key)
    {
        Poco::URI uri;
        try
        {
            uri = Poco::URI(key);
        }
        catch (const Poco::SyntaxException &)
        {
            throw Exception(
                ErrorCodes::HDFS_ERROR,
                "Invalid HDFS namenode '{}': not a valid URI, expected 'hdfs://host:port' or 'viewfs://cluster'",
                key);
        }

        const auto & scheme = uri.getScheme();
        if (scheme != "hdfs" && scheme != "viewfs")
            throw Exception(
                ErrorCodes::HDFS_ERROR, "Invalid HDFS namenode '{}': unsupported scheme '{}', expected 'hdfs' or 'viewfs'", key, scheme);

        if (uri.getHost().empty())
            throw Exception(ErrorCodes::HDFS_ERROR, "Invalid HDFS namenode '{}': missing host/authority after '{}'", key, scheme + "://");
    }

    /// Return a cached connection for the given namenode key.
    ///
    /// The namenode must be either:
    ///   - "default"             – reads NameNode from Hadoop XML configuration files
    ///   - "hdfs://host:port"   – connect directly to this HDFS NameNode
    ///   - "viewfs://cluster"   – connect via ViewFS federation
    ///
    /// Any other value (wrong scheme, missing authority, bare path, etc.) throws HDFS_ERROR.
    ///
    /// If no connection for this namenode exists yet, one is created and cached.
    /// Concurrent callers for the same namenode will not create duplicate connections
    /// (double-checked locking under a unique_lock).
    Connection getConnection(const std::string & namenode = "default")
    {
        const std::string key = namenode.empty() ? "default" : namenode;

        /// Validate that the key is "default" or a well-formed URI with a supported scheme.
        if (key != "default")
        {
            validNameNodeSchema(key);
        }

        /// Fast path: shared_lock allows concurrent reads.
        {
            std::shared_lock lock(cache_mutex_);
            auto it = connection_cache_.find(key);
            if (it != connection_cache_.end())
            {
                ProfileEvents::increment(ProfileEvents::HDFSConnectionsReused);
                LOG_TRACE(getLogger("HDFSConnectionFactory"), "Reusing connection for namenode: {}", key);
                return Connection{driver_, it->second};
            }
        }

        /// Slow path: exclusive lock to insert a new connection.
        std::unique_lock lock(cache_mutex_);

        /// Double-check: another thread may have inserted while we waited.
        auto it = connection_cache_.find(key);
        if (it != connection_cache_.end())
        {
            ProfileEvents::increment(ProfileEvents::HDFSConnectionsReused);
            return Connection{driver_, it->second};
        }

        LOG_INFO(getLogger("HDFSConnectionFactory"), "Creating new connection for namenode: {}", key);
        HDFSFSPtr fs = createNewConnection(key);
        connection_cache_.emplace(key, fs);
        ProfileEvents::increment(ProfileEvents::HDFSConnectionsCreated);
        return Connection{driver_, fs};
    }

    void handleError(const String & caller)
    {
        const int error_code = errno;
        ProfileEvents::increment(ProfileEvents::HDFSConnectionErrors);
        LOG_ERROR(getLogger("HDFSConnectionFactory"), "HDFS error in {}: {}.", caller, std::strerror(error_code));
    }

    HDFSConnectionFactory(const HDFSConnectionFactory &) = delete;
    HDFSConnectionFactory & operator=(const HDFSConnectionFactory &) = delete;

private:
    /// Pointer to dynamically loaded HDFS symbols (Arrow LibHdfsShim).
    arrow::io::internal::LibHdfsShim* driver_;

    mutable std::shared_mutex cache_mutex_;
    /// One shared hdfsFS per namenode key, reused for the process lifetime.
    std::unordered_map<std::string, HDFSFSPtr> connection_cache_;

    /// Constructor – loads libhdfs symbols only; connections are created on demand
    /// so that the JVM is not started before any fork() that ClickHouse may perform.
    HDFSConnectionFactory()
    {
        arrow::Status status = ::arrow::io::internal::ConnectLibHdfs(&driver_);
        if (!status.ok())
            throw Exception(ErrorCodes::HDFS_ERROR, "Unable to load HDFS library: {}", status.ToString());

        LOG_INFO(getLogger("HDFSConnectionFactory"), "HDFS library driver initialized successfully");
    }

    ~HDFSConnectionFactory()
    {
        std::unique_lock lock(cache_mutex_);
        connection_cache_.clear(); /// shared_ptr deleter calls hdfsDisconnect for each entry
        LOG_DEBUG(getLogger("HDFSConnectionFactory"), "HDFS connection factory destroyed");
    }

    /// Open a new hdfsFS for the given namenode key.
    /// Must be called with cache_mutex_ held (unique_lock).
    HDFSFSPtr createNewConnection(const std::string & namenode)
    {
        chassert(driver_ != nullptr);

        hdfsBuilder * builder = driver_->NewBuilder();
        if (!builder)
            throw Exception(ErrorCodes::HDFS_ERROR, "Failed to create HDFS builder");

        /// "default" tells libhdfs to read fs.defaultFS from Hadoop XML config files,
        /// enabling ViewFS, NameNode HA, etc.  Any other value is used verbatim as the
        /// NameNode URI, e.g. "hdfs://namenode:8020" or "viewfs://cluster".
        driver_->BuilderSetNameNode(builder, namenode.c_str());

        hdfsFS client = driver_->BuilderConnect(builder);
        if (client == nullptr)
            throw Exception(ErrorCodes::HDFS_ERROR, "Unable to connect to HDFS namenode: {}", namenode);

        LOG_INFO(getLogger("HDFSConnectionFactory"), "Connected to HDFS namenode: {}", namenode);

        return HDFSFSPtr(client, [driver = driver_, namenode](hdfsFS fs)
        {
            if (fs && driver)
            {
                driver->hdfsDisconnect(fs);
                LOG_INFO(getLogger("HDFSConnectionFactory"), "Disconnected from HDFS namenode: {}", namenode);
            }
        });
    }
};

}

#endif
