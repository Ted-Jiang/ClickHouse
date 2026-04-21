#pragma once
#include "config.h"

#if USE_AVRO

#include <IO/CompressionMethod.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>

namespace DB::Iceberg
{

// All fields in this struct should be either thread-safe or immutable, because it can be used by several queries
struct PersistentTableComponents
{
    IcebergSchemaProcessorPtr schema_processor;
    IcebergMetadataFilesCachePtr metadata_cache;
    const Int32 format_version;
    const String table_location;
    const CompressionMethod metadata_compression_method;
    const String table_path;
    const std::optional<String> table_uuid;

    /// Current metadata file path (absolute to table directory with the protocol header like `hdfs://`)
    /// Used to pass the path to worker nodes to avoid re-listing metadata directory
    const String current_metadata_file_path;
};

}

#endif
