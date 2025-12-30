#include <Storages/ObjectStorage/DataLakes/Common.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Common/logger_useful.h>

#include <fmt/ranges.h>

namespace DB
{

std::vector<String> listFiles(
    const IObjectStorage & object_storage,
    const StorageObjectStorageConfiguration & configuration,
    const String & prefix, const String & suffix)
{
    auto key = std::filesystem::path(configuration.getPathForRead().path) / prefix;
    RelativePathsWithMetadata files_with_metadata;
    object_storage.listObjects(key, files_with_metadata, 0);
    Strings res;
    for (const auto & file_with_metadata : files_with_metadata)
    {
        const auto & filename = file_with_metadata->relative_path;
        if (filename.ends_with(suffix))
            res.push_back(filename);
    }
    LOG_TRACE(getLogger("DataLakeCommon"), "Listed {} files ({})", res.size(), fmt::join(res, ", "));
    return res;
}

std::vector<std::pair<String, UInt64>> listFilesWithLastModifiedTime(
    const IObjectStorage & object_storage,
    const StorageObjectStorageConfiguration & configuration,
    const String & prefix, const String & suffix)
{
    auto key = std::filesystem::path(configuration.getPathForRead().path) / prefix;
    RelativePathsWithMetadata files_with_metadata;
    object_storage.listObjects(key, files_with_metadata, 0);
    std::vector<std::pair<String, UInt64>> res;
    for (const auto & file_with_metadata : files_with_metadata)
    {
        const auto & filename = file_with_metadata->relative_path;

        if (filename.ends_with(suffix))
        {
            const auto last_modified_time = file_with_metadata->metadata.has_value()
                ? static_cast<UInt64>(file_with_metadata->metadata->last_modified.epochTime())
                : 0;
            res.emplace_back(filename, last_modified_time);
        }
    }
    LOG_TRACE(getLogger("DataLakeCommon"), "Listed {} files ({})", res.size(), fmt::join(res, ", "));
    return res;
}


}
