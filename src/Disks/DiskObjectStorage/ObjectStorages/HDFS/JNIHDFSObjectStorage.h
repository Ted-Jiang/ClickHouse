//
// Created by Jiang, Yang on 2025/5/7.
//
#pragma once

#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Common/getRandomASCIIString.h>
#include <Common/logger_useful.h>
#include <Storages/ObjectStorage/HDFS/HDFSConnectionFactory.h>

#include <Poco/DateTimeFormatter.h>
#include <Poco/Timestamp.h>

#include "arrow/io/hdfs_internal.h"
#include "arrow/io/hdfs.h"

namespace DB
{

struct HDFSObjectStorageSettings
{
    HDFSObjectStorageSettings(int min_bytes_for_seek_, int replication_)
        : min_bytes_for_seek(min_bytes_for_seek_)
        , replication(replication_)
    {}

    size_t min_bytes_for_seek;
    int replication;
};


class JNIHDFSObjectStorage : public IObjectStorage
{
public:

    using SettingsPtr = std::unique_ptr<HDFSObjectStorageSettings>;

    JNIHDFSObjectStorage(
        const String & hdfs_root_path_,
        SettingsPtr settings_,
        const Poco::Util::AbstractConfiguration & config_,
        bool lazy_initialize)
        : config(config_)
        , settings(std::move(settings_))
        , log(getLogger("JNIHDFSObjectStorage(" + hdfs_root_path_ + ")"))
    {
        const size_t begin_of_path = hdfs_root_path_.find('/', hdfs_root_path_.find("//") + 2);
        url = hdfs_root_path_;
        url_without_path = url.substr(0, begin_of_path);
        if (begin_of_path < url.size())
            data_directory = url.substr(begin_of_path);
        else
            data_directory = "/";

        if (!lazy_initialize)
            initializeHDFSFS();
    }

    std::string getName() const override { return "JNIHDFSObjectStorage"; }

    std::string getCommonKeyPrefix() const override { return url; }

    std::string getDescription() const override { return url; }

    ObjectStorageType getType() const override { return ObjectStorageType::JNIHDFS; }

    std::string getDiskName() const override { return "JNIHDFS"; }

    bool exists(const StoredObject & object) const override;

    std::unique_ptr<ReadBufferFromFileBase> readObject( /// NOLINT
        const StoredObject & object,
        const ReadSettings & read_settings,
        std::optional<size_t> read_hint = {}
        ) const override;

    /// Open the file for write and return WriteBufferFromFileBase object.
    std::unique_ptr<WriteBufferFromFileBase> writeObject( /// NOLINT
        const StoredObject & object,
        WriteMode mode,
        std::optional<ObjectAttributes> attributes = {},
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        const WriteSettings & write_settings = {}) override;

    void removeObjectIfExists(const StoredObject & object) override;

    void removeObjectsIfExist(const StoredObjects & objects) override;

    std::string construct_the_etag(const std::string & path, tTime time) const
    {
        return path + Poco::DateTimeFormatter::format(Poco::Timestamp::fromEpochTime(time), "%Y-%m-%d %H:%M:%S");
    }

    ObjectMetadata getObjectMetadata(const std::string & path, bool with_tags) const override;

    std::optional<ObjectMetadata> tryGetObjectMetadata(const std::string & path, bool with_tags) const override;

    virtual ObjectStorageKeyGeneratorPtr createKeyGenerator() const override;

    void copyObject( /// NOLINT
        const StoredObject & object_from,
        const StoredObject & object_to,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        std::optional<ObjectAttributes> object_to_attributes = {}) override;

    void listObjects(const std::string & path, RelativePathsWithMetadata & children, size_t max_keys) const override;

    String getObjectsNamespace() const override { return ""; }

    std::unique_ptr<IObjectStorage> cloneObjectStorage(
        const std::string & new_namespace,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        ContextPtr context);

    ObjectStorageKey generateObjectKeyForPath(const std::string & path, const std::optional<std::string> & key_prefix) const ;

    bool areObjectKeysRandom() const  { return true; }

    bool isRemote() const override { return true; }

    void startup() override { }

    void shutdown() override { }

private:
    void initializeHDFSFS() const;
    void createDriver() const;
    std::string extractObjectKeyFromURL(const StoredObject & object) const;

    /// Remove file. Throws exception if file doesn't exists or it's a directory.
    void removeObject(const StoredObject & object);

    void removeObjects(const StoredObjects & objects);

    const Poco::Util::AbstractConfiguration & config;

    mutable arrow::io::internal::LibHdfsShim* driver_;
    /// Notice hdfs_fs should deconstruct before driver_
    mutable HDFSFSPtr hdfs_fs;

    mutable std::once_flag init_flag;

    SettingsPtr settings;
    std::string url;
    std::string url_without_path;
    std::string data_directory;

    LoggerPtr log;
};

}

