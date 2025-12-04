#pragma once

#include <Client/Connection.h>
#include <Common/Logger.h>
#include <Interpreters/Cluster.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <unordered_set>
#include <vector>
#include <mutex>
#include <memory>

namespace DB
{

class StorageObjectStorageStableTaskDistributorV2
{
public:
    StorageObjectStorageStableTaskDistributorV2(
        std::shared_ptr<IObjectIterator> iterator_,
        std::vector<std::string> && ids_of_nodes_,
        bool send_over_whole_archive_);

    ObjectInfoPtr getNextTask(size_t number_of_current_replica);

private:
    void init();
    size_t getReplicaForFile(const String & file_path);

    const std::shared_ptr<IObjectIterator> iterator;
    const size_t number_of_shards_;
    const bool send_over_whole_archive;

    std::vector<std::vector<ObjectInfoPtr>> connection_to_files;

    std::vector<std::string> ids_of_nodes;

    /// Global mutex for shared state (iterator, unprocessed_files, iterator_exhausted)
    std::mutex global_mutex;
    /// Per-replica mutexes for connection_to_files vectors, assume is has same size as connection_to_files
    std::vector<std::mutex> replica_mutexes;
    std::unordered_map<size_t , UInt64> shard_work_load;
    std::atomic_bool iterator_exhausted = false;


    LoggerPtr log = getLogger("StorageClusterTaskDistributorV2");
};

}
