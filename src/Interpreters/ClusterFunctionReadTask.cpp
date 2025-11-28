#include <Interpreters/ClusterFunctionReadTask.h>
#include <Interpreters/SetSerialization.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <Core/ProtocolDefines.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/ActionsDAG.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergDataObjectInfo.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_PROTOCOL;
    extern const int LOGICAL_ERROR;
}
namespace Setting
{
    extern const SettingsBool cluster_function_process_archive_on_multiple_nodes;
}

ClusterFunctionReadTaskResponse::ClusterFunctionReadTaskResponse(ObjectInfoPtr object, const ContextPtr & context)
{
    if (!object)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "`object` cannot be null");

    if (object->data_lake_metadata.has_value())
        data_lake_metadata = object->data_lake_metadata.value();

    auto iceberg_object_info = std::dynamic_pointer_cast<IcebergDataObjectInfo>(object);
    //here notice should pass these args
    if (iceberg_object_info) {
        position_deletes_objects = iceberg_object_info->position_deletes_objects;
        data_object_file_path_key = iceberg_object_info->data_object_file_path_key;
        underlying_format_read_schema_id = iceberg_object_info->underlying_format_read_schema_id;
        sequence_number = iceberg_object_info->sequence_number;
    }


    const bool send_over_whole_archive = !context->getSettingsRef()[Setting::cluster_function_process_archive_on_multiple_nodes];
    path = send_over_whole_archive ? object->getPathOrPathToArchiveIfArchive() : object->getPath();
}

ClusterFunctionReadTaskResponse::ClusterFunctionReadTaskResponse(const std::string & path_)
    : path(path_)
{
}

ObjectInfoPtr ClusterFunctionReadTaskResponse::getObjectInfo() const
{
    if (isEmpty())
        return {};

    if (position_deletes_objects.empty()) {
        auto object = std::make_shared<ObjectInfo>(path);
        object->data_lake_metadata = data_lake_metadata;
        return object;
    } else {
        auto object = std::make_shared<IcebergDataObjectInfo>(path);
        object->data_object_file_path_key = data_object_file_path_key;
        object->data_lake_metadata = data_lake_metadata;
        object->position_deletes_objects = position_deletes_objects;
        object->underlying_format_read_schema_id = underlying_format_read_schema_id;
        object->sequence_number = sequence_number;
        return object;
    }
}

void ClusterFunctionReadTaskResponse::serialize(WriteBuffer & out, size_t protocol_version) const
{
    writeVarUInt(protocol_version, out);
    writeStringBinary(path, out);

    if (protocol_version >= DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_DATA_LAKE_METADATA)
    {
        SerializedSetsRegistry registry;
        if (data_lake_metadata.transform)
            data_lake_metadata.transform->serialize(out, registry);
        else
            ActionsDAG().serialize(out, registry);
    }

    if (protocol_version >= DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_ICEBERG_POS_DELETE)
    {
        writeVarUInt(position_deletes_objects.size(), out);
        for (const auto & pos_delete_obj : position_deletes_objects)
        {
            writeStringBinary(pos_delete_obj.file_path, out);
            writeStringBinary(pos_delete_obj.file_format, out);
            if (pos_delete_obj.reference_data_file_path.has_value())
            {
                writeVarUInt(1, out);
                writeStringBinary(pos_delete_obj.reference_data_file_path.value(), out);
            }
            else
            {
                writeVarUInt(0, out);
            }
        }
        writeStringBinary(data_object_file_path_key, out);
        writeVarInt(underlying_format_read_schema_id, out);
        writeVarInt(sequence_number, out);
    }
}

void ClusterFunctionReadTaskResponse::deserialize(ReadBuffer & in)
{
    size_t protocol_version = 0;
    readVarUInt(protocol_version, in);
    if (protocol_version < DBMS_CLUSTER_INITIAL_PROCESSING_PROTOCOL_VERSION
        || protocol_version > DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION)
    {
        throw Exception(
            ErrorCodes::UNKNOWN_PROTOCOL, "Supported protocol versions are in range [{}, {}], got: {}",
            DBMS_CLUSTER_INITIAL_PROCESSING_PROTOCOL_VERSION, DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION,
            protocol_version);
    }

    readStringBinary(path, in);
    if (protocol_version >= DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_DATA_LAKE_METADATA)
    {
        DeserializedSetsRegistry registry;
        auto transform = std::make_shared<ActionsDAG>(ActionsDAG::deserialize(in, registry, Context::getGlobalContextInstance()));

        if (!path.empty() && !transform->getInputs().empty())
        {
            data_lake_metadata.transform = std::move(transform);
        }
    }

    if (protocol_version >= DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_ICEBERG_POS_DELETE)
    {
        size_t pos_delete_obj_size = 0;
        readVarUInt(pos_delete_obj_size, in);
        position_deletes_objects.resize(pos_delete_obj_size);
        for (size_t i = 0; i < pos_delete_obj_size; ++i)
        {
            Iceberg::PositionDeleteObject & pos_delete_obj = position_deletes_objects[i];
            readStringBinary(pos_delete_obj.file_path, in);
            readStringBinary(pos_delete_obj.file_format, in);
            size_t has_reference_path = 0;
            readVarUInt(has_reference_path, in);
            if (has_reference_path == 1)
            {
                String reference_path;
                readStringBinary(reference_path, in);
                pos_delete_obj.reference_data_file_path = reference_path;
            }
        }
        readStringBinary(data_object_file_path_key, in);
        readVarInt(underlying_format_read_schema_id, in);
        readVarInt(sequence_number, in);
    }
}

}
