#include <Storages/ObjectStorage/StorageObjectStorageStableTaskDistributorV2.h>
#include <Common/SipHash.h>
#include <consistent_hashing.h>
#include <optional>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

StorageObjectStorageStableTaskDistributorV2::StorageObjectStorageStableTaskDistributorV2(
    std::shared_ptr<IObjectIterator> iterator_,
    std::vector<std::string> && ids_of_nodes_,
    bool send_over_whole_archive_)
    : iterator(std::move(iterator_))
    , number_of_shards_(ids_of_nodes_.size())
    , send_over_whole_archive(send_over_whole_archive_)
    , connection_to_files(ids_of_nodes_.size())
    , ids_of_nodes(std::move(ids_of_nodes_))
    , replica_mutexes(ids_of_nodes.size())
    , iterator_exhausted(false)
{
    init();
}




void StorageObjectStorageStableTaskDistributorV2::init()
{
    if (!iterator)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Iterator cannot be null");
    if (number_of_shards_ == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Number of shards must be greater than zero");


    // todo
    bool enable_dynamic_task_stealing = false;
    float stealer_triggering_threshold_load = 1.2f;

    LOG_DEBUG(log, "[CHash] dynamic task stealing is {}", enable_dynamic_task_stealing ? "Enable" : "Disable");

    LOG_DEBUG(
        log,
        "[CHash] Stealer triggering threshold load: {}.",
        stealer_triggering_threshold_load);

    // use hash of file path to put each file into bucket shard_to_files
    int file_count = 0;
    std::vector<ObjectInfoPtr> exceed_files{number_of_shards_};

    // first round, according to consistent hash mapping as much as possible

    while (true)
    {
        auto it = iterator->next(0);
        if (!it) {
            break;
        }
        file_count++;
        String file_path;
        if (send_over_whole_archive && it->isArchive())
        {
            file_path = it->getPathOrPathToArchiveIfArchive();
        }
        else
        {
            file_path = it->getPath();
        }
        auto position = getReplicaForFile(file_path);
        connection_to_files[position].emplace_back(it);
    }
    // todo second round, assign exceed files to shards which are under limit

    if (file_count == 0)
        iterator_exhausted.store(true);

}


ObjectInfoPtr StorageObjectStorageStableTaskDistributorV2::getNextTask(size_t number_of_current_replica)
{
    LOG_TRACE(log, "[v2]Received request from replica {} looking for a file", number_of_current_replica);
    {
        std::lock_guard lock(replica_mutexes[number_of_current_replica]);
        if (!connection_to_files[number_of_current_replica].empty())
        {
            auto next_file = connection_to_files[number_of_current_replica].back();
            connection_to_files[number_of_current_replica].pop_back();
            LOG_TRACE(log, "[v2]Assigning pre-queued file {} to replica {}", next_file->getPath(), number_of_current_replica);
            return next_file;
        }
        else
        {
            LOG_TRACE(log, "[v2]No pre-queued files for replica {}", number_of_current_replica);
            return {};
        }
    }
}

size_t StorageObjectStorageStableTaskDistributorV2::getReplicaForFile(const String & file_path)
{
    size_t nodes_count = ids_of_nodes.size();

    /// Trivial case
    if (nodes_count < 2)
        return 0;

    /// Rendezvous hashing
    size_t best_id = 0;
    UInt64 best_weight = sipHash64(ids_of_nodes[0] + file_path);
    for (size_t id = 1; id < nodes_count; ++id)
    {
        UInt64 weight = sipHash64(ids_of_nodes[id] + file_path);
        if (weight > best_weight)
        {
            best_weight = weight;
            best_id = id;
        }
    }
    return best_id;
}
}
