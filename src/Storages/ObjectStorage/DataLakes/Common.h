#pragma once
#include <Core/Types.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>

namespace DB
{

class IObjectStorage;

std::vector<String> listFiles(
    const IObjectStorage & object_storage,
    const StorageObjectStorageConfiguration & configuration,
    const String & prefix, const String & suffix);

/// Lists files with their last modified timestamp information.
/// @param object_storage Object storage instance to use for listing
/// @param configuration Storage configuration containing the base path
/// @param prefix Directory prefix to search within
/// @param suffix File extension or suffix to filter by
/// @return Vector of pairs containing (filename, last_modified_timestamp_in_epoch_time)
/// @note If metadata is unavailable for a file, the timestamp will be 0
std::vector<std::pair<String, UInt64>> listFilesWithLastModifiedTime(
    const IObjectStorage & object_storage,
    const StorageObjectStorageConfiguration & configuration,
    const String & prefix, const String & suffix);

}
