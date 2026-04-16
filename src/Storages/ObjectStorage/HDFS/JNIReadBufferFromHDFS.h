//
// Created by Jiang, Yang on 2025/5/8.
//

#pragma once

#include "config.h"
#include "Disks/DiskObjectStorage/ObjectStorages/HDFS/JNIHDFSObjectStorage.h"

#include <IO/ReadBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <string>
#include <memory>
#include <base/types.h>
#include <Interpreters/Context.h>
#include <IO/ReadBufferFromFileBase.h>

#include "arrow/io/hdfs_internal.h"
#include "arrow/io/hdfs.h"

namespace DB
{
/** Accepts HDFS path to file and opens it.
 * Closes file by himself (thus "owns" a file descriptor).
 */
class JNIReadBufferFromHDFS : public ReadBufferFromFileBase
{
    struct JNIReadBufferFromHDFSImpl;

public:
    JNIReadBufferFromHDFS(
        const String & hdfs_uri_,
        const String & hdfs_file_path_,
        const Poco::Util::AbstractConfiguration & config_,
        const ReadSettings & read_settings_,
        size_t read_until_position_ = 0,
        bool use_external_buffer = false,
        std::optional<size_t> file_size = std::nullopt);



    ~JNIReadBufferFromHDFS() override;

    bool nextImpl() override;

    off_t seek(off_t offset_, int whence) override;

    off_t getPosition() override;

    std::optional<size_t> tryGetFileSize() override;

    size_t getFileOffsetOfBufferEnd() const override;

    String getFileName() const override;

    size_t readBigAt(char * buffer, size_t size, size_t offset, const std::function<bool(size_t)> & function) const override;

    bool supportsReadAt() override;

private:
    std::unique_ptr<JNIReadBufferFromHDFSImpl> impl;
    bool use_external_buffer;
};
}

