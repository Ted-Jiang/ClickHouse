//
// Created by Jiang, Yang on 2025/5/7.
//

#include <Common/re2.h>
#include "Poco/DateTimeFormatter.h"
#include <Disks/DiskObjectStorage/ObjectStorages/HDFS/JNIHDFSObjectStorage.h>

#include <IO/copyData.h>

#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Storages/ObjectStorage/HDFS/JNIReadBufferFromHDFS.h>
#include <Common/getRandomASCIIString.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNSUPPORTED_METHOD;
extern const int HDFS_ERROR;
extern const int ACCESS_DENIED;
extern const int LOGICAL_ERROR;
}

struct HDFSFileInfo
{
    hdfsFileInfo * file_info;
    int length;

    HDFSFileInfo() : file_info(nullptr) , length(0) {}

    HDFSFileInfo(const HDFSFileInfo & other) = delete;
    HDFSFileInfo(HDFSFileInfo && other) = default;
    HDFSFileInfo & operator=(const HDFSFileInfo & other) = delete;
    HDFSFileInfo & operator=(HDFSFileInfo && other) = default;
    ~HDFSFileInfo();
};

HDFSFileInfo::~HDFSFileInfo()
{
    hdfsFreeFileInfo(file_info, length);
}

void JNIHDFSObjectStorage::initializeHDFSFS() const
{
    if (initialized)
        return;

    std::lock_guard lock(init_mutex);
    if (initialized)
        return;

    createDriver();
    initialized = true;
}

void JNIHDFSObjectStorage::createDriver() const
{
    LOG_DEBUG(getLogger("JNIHDFSObjectStorage"), "url {}.", url);
    LOG_DEBUG(getLogger("JNIHDFSObjectStorage"), "url_without_path {}.", url_without_path );
    LOG_DEBUG(getLogger("JNIHDFSObjectStorage"), "data_directory {}.", data_directory);

    arrow::io::internal::LibHdfsShim* libhdfs_shim;
    arrow::Status status = ::arrow::io::internal::ConnectLibHdfs(&libhdfs_shim);
    if (!status.ok()) {
        throw Exception(ErrorCodes::HDFS_ERROR, "Unable to create builder to connect to HDFS. {}", status.ToString());
    }else {
        LOG_DEBUG(getLogger("HDFSClient"), "Connect to HDFS successfully. {}", status.ToString());
    }

    hdfsBuilder* builder2 = libhdfs_shim->NewBuilder();
    if (!builder2) {
        throw Exception(ErrorCodes::HDFS_ERROR, "Failed to create HDFS builder.");
    }

    // See https://github.com/facebookincubator/velox/blob/main/velox/external/hdfs/hdfs.h#L289
    // If the string given is 'default', the default NameNode
    // configuration will be used (from the XML configuration files)
    libhdfs_shim->BuilderSetNameNode(builder2, "default");
    libhdfs_shim->BuilderSetForceNewInstance(builder2);

    hdfsFS hdfsClient = libhdfs_shim->BuilderConnect(builder2);
    if (hdfsClient == nullptr) {
        throw Exception(ErrorCodes::HDFS_ERROR, "Unable to create builder to connect to HDFS. {}", status.ToString());
    }
    driver_ = libhdfs_shim;
    hdfs_fs = HDFSFSPtr(hdfsClient, detail::HDFSFsDeleter(libhdfs_shim));
}

static constexpr std::string_view BAD_HDFS_URL_REGEXP = "^hdfs:/[^/]+/.*";
static constexpr std::string_view BAD_VIEW_URL_REGEXP = "^viewfs:/[^/]+/.*";
static constexpr std::string_view HDFS_URL_REGEXP = "^hdfs://[^/]*/.*";
static constexpr std::string_view VIEWFS_URL_REGEXP = "^viewfs://[^/]*/.*";


std::string JNIHDFSObjectStorage::extractObjectKeyFromURL(const StoredObject & object) const
{
    /// This is very unfortunate, but for disk HDFS we made a mistake
    /// and now its behaviour is inconsistent with S3 and Azure disks.
    /// The mistake is that for HDFS we write into metadata files whole URL + data directory + key,
    /// while for S3 and Azure we write there only data_directory + key.
    /// This leads us into ambiguity that for StorageHDFS we have just key in object.remote_path,
    /// but for DiskHDFS we have there URL as well.
    auto path = object.remote_path;
    if (path.starts_with(url))
        path = path.substr(url.size());
    if (path.starts_with("/"))
        path = path.substr(1);
    /// If path start with BAD_HDFS_URL_REGEXP change to HDFS_HOST_REGEXP
    if (re2::RE2::FullMatch(path, std::string((BAD_HDFS_URL_REGEXP))))
    {
        re2::RE2::Replace(&path, "hdfs:/", "hdfs://");
    }

    if (re2::RE2::FullMatch(path, std::string((BAD_VIEW_URL_REGEXP))))
    {
        re2::RE2::Replace(&path, "viewfs:/", "viewfs://");
    }

    if (!re2::RE2::FullMatch(path, std::string((HDFS_URL_REGEXP))) && !re2::RE2::FullMatch(path, std::string((VIEWFS_URL_REGEXP))))
    {
        path = url_without_path + "/" + path;
    }
    return path;
}


std::string fixObjectKeyFromURL(const String & path)
{
    /// This is very unfortunate, but for disk HDFS we made a mistake
    /// and now its behaviour is inconsistent with S3 and Azure disks.
    /// The mistake is that for HDFS we write into metadata files whole URL + data directory + key,
    /// while for S3 and Azure we write there only data_directory + key.
    /// This leads us into ambiguity that for StorageHDFS we have just key in object.remote_path,
    /// but for DiskHDFS we have there URL as well.
    String new_path = path;
    /// If path start with BAD_HDFS_URL_REGEXP change to HDFS_HOST_REGEXP
    if (re2::RE2::FullMatch(path, std::string((BAD_HDFS_URL_REGEXP))))
    {
        re2::RE2::Replace(&new_path, "hdfs:/", "hdfs://");
    }
    if (re2::RE2::FullMatch(path, std::string((BAD_VIEW_URL_REGEXP))))
    {
        re2::RE2::Replace(&new_path, "viewfs:/", "viewfs://");
    }
    return new_path;
}

ObjectStorageKey
JNIHDFSObjectStorage::generateObjectKeyForPath(const std::string & /* path */, const std::optional<std::string> & /* key_prefix */) const
{
    initializeHDFSFS();
    /// what ever data_source_description.description value is, consider that key as relative key
    chassert(data_directory.starts_with("/"));
    return ObjectStorageKey::createAsRelative(
        std::filesystem::path(url_without_path) / data_directory.substr(1), getRandomASCIIString(32));
}

bool JNIHDFSObjectStorage::exists(const StoredObject & object) const
{
    initializeHDFSFS();
    std::string path = object.remote_path;
    if (path.starts_with(url_without_path))
        path = path.substr(url_without_path.size());

    int res = driver_->Exists(hdfs_fs.get(), path.c_str());
    return (0 == res);
}

std::unique_ptr<ReadBufferFromFileBase> JNIHDFSObjectStorage::readObject( /// NOLINT
    const StoredObject & object,
    const ReadSettings & read_settings,
    std::optional<size_t>) const
{
    initializeHDFSFS();
    auto path = extractObjectKeyFromURL(object);
    LOG_INFO(log, " HDFSObjectStorage::readObject PATH is  {}", path);
    LOG_INFO(log, " HDFSObjectStorage::readObject url_without_path is  {}", url_without_path);
    LOG_INFO(log, " HDFSObjectStorage::readObject data_directory is  {}", data_directory);
    return std::make_unique<JNIReadBufferFromHDFS>(
        std::filesystem::path(url_without_path) / "",
        path,
        config,
        patchSettings(read_settings),
        /* read_until_position */0,
        read_settings.remote_read_buffer_use_external_buffer);
}

std::unique_ptr<WriteBufferFromFileBase> JNIHDFSObjectStorage::writeObject( /// NOLINT
    const StoredObject & ,
    WriteMode ,
    std::optional<ObjectAttributes> ,
    size_t ,
    const WriteSettings & )
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "HDFS API doesn't support writeObject");
}


void JNIHDFSObjectStorage::removeObjectIfExists(const StoredObject & )
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "HDFS API doesn't support removeObjectIfExists");
}

void JNIHDFSObjectStorage::removeObjectsIfExist(const StoredObjects & )
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "HDFS API doesn't support removeObjectsIfExist");
}

ObjectMetadata JNIHDFSObjectStorage::getObjectMetadata(const std::string & path,  bool) const
{
    initializeHDFSFS();
    if (!path.data())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "HDFS path is empty");
    }

    auto fix_path = fixObjectKeyFromURL(path);
    auto *file_info = driver_->GetPathInfo(hdfs_fs.get(), fix_path.data());

    if (!file_info)
    {
        throw Exception(
            ErrorCodes::HDFS_ERROR,
            "[HDFS] Cannot get file info for: {}. fix path: {}. Error details: {}",
            path,
            fix_path,
            std::strerror(errno));
    }

    ObjectMetadata metadata;
    metadata.size_bytes = static_cast<size_t>(file_info->mSize);
    metadata.last_modified = Poco::Timestamp::fromEpochTime(file_info->mLastMod);
    // `etag` (entity tag) is typically used to identify a specific version of an object. It is commonly the MD5 hash of the object's content.
    // Here change to file path + last modify time to make it unique.
    metadata.etag = construct_the_etag(path, file_info->mLastMod);

    hdfsFreeFileInfo(file_info, 1);
    return metadata;
}


ObjectStorageKeyGeneratorPtr JNIHDFSObjectStorage::createKeyGenerator() const
{
    // todo check this usage
    initializeHDFSFS();
    /// what ever data_source_description.description value is, consider that key as relative key
    chassert(data_directory.starts_with("/"));
    return createObjectStorageKeyGeneratorByPrefix(std::filesystem::path(url_without_path) / data_directory.substr(1));
}

std::optional<ObjectMetadata> JNIHDFSObjectStorage::tryGetObjectMetadata(const std::string & path, bool) const
{
    initializeHDFSFS();
    if (!path.data())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "HDFS path is empty");
    }

    auto fix_path = fixObjectKeyFromURL(path);
    auto *file_info = driver_->GetPathInfo(hdfs_fs.get(), fix_path.data());

    if (!file_info)
    {
        return std::nullopt;
    }

    ObjectMetadata metadata;
    metadata.size_bytes = static_cast<size_t>(file_info->mSize);
    metadata.last_modified = Poco::Timestamp::fromEpochTime(file_info->mLastMod);
    // `etag` (entity tag) is typically used to identify a specific version of an object. It is commonly the MD5 hash of the object's content.
    // Here change to file path + last modify time to make it unique.
    metadata.etag = construct_the_etag(path, file_info->mLastMod);

    hdfsFreeFileInfo(file_info, 1);
    return metadata;
}



void JNIHDFSObjectStorage::listObjects(const std::string & path, RelativePathsWithMetadata & children, size_t max_keys) const
{
    initializeHDFSFS();
    LOG_TEST(log, "Trying to list files for {}", path);

    HDFSFileInfo ls;
    ls.file_info = driver_->ListDirectory(hdfs_fs.get(), path.data(), &ls.length);
    if (ls.file_info == nullptr && errno != ENOENT) // NOLINT
    {
        // ignore file not found exception, keep throw other exception,
        // libhdfs3 doesn't have function to get exception type, so use errno.
        throw Exception(ErrorCodes::ACCESS_DENIED, "Cannot list directory {}: {}", path, std::strerror(errno));
    }

    if (!ls.file_info && ls.length > 0)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "file_info shouldn't be null");
    }

    LOG_TEST(log, "Listed {} files for {}", ls.length, path);

    for (int i = 0; i < ls.length; ++i)
    {
        const String file_path = std::filesystem::path(ls.file_info[i].mName).lexically_normal();
        const bool is_directory = ls.file_info[i].mKind == 'D';
        if (is_directory)
        {
            listObjects(std::filesystem::path(file_path) / "", children, max_keys);
        }
        else
        {
            children.emplace_back(std::make_shared<RelativePathWithMetadata>(
                String(file_path),
                ObjectMetadata{
                    static_cast<uint64_t>(ls.file_info[i].mSize),
                    true,
                    Poco::Timestamp::fromEpochTime(ls.file_info[i].mLastMod),
                    construct_the_etag(file_path, ls.file_info[i].mLastMod),
                    {},
                    {}}));
        }

        if (max_keys && children.size() >= max_keys)
            break;
    }
}

void JNIHDFSObjectStorage::copyObject( /// NOLINT
    const StoredObject & ,
    const StoredObject & ,
    const ReadSettings & ,
    const WriteSettings & ,
    std::optional<ObjectAttributes> )
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "HDFS API doesn't support copyObject");
}


std::unique_ptr<IObjectStorage> JNIHDFSObjectStorage::cloneObjectStorage(
    const std::string &,
    const Poco::Util::AbstractConfiguration &,
    const std::string &, ContextPtr)
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "HDFS object storage doesn't support cloning");
}

}
