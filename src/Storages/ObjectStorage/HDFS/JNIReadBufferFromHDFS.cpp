//
// Created by Jiang, Yang on 2025/5/8.
//
#include "JNIReadBufferFromHDFS.h"
#include "HDFSConnectionFactory.h"
#include "HDFSFileHandle.h"

#include <Common/ProfileEvents.h>
#include <Common/Scheduler/ResourceGuard.h>
#include <Common/Throttler.h>
#include <Common/safe_cast.h>



namespace ProfileEvents
{
extern const Event RemoteReadThrottlerBytes;
extern const Event RemoteReadThrottlerSleepMicroseconds;
extern const Event HDFSOpenFile;
extern const Event HDFSOpenFileErrors;
extern const Event HDFSGetPathInfo;
extern const Event HDFSGetPathInfoErrors;
extern const Event HDFSPread;
extern const Event HDFSPreadErrors;
extern const Event HDFSPreadBytes;
extern const Event HDFSSeek;
extern const Event HDFSSeekErrors;
}

namespace DB
{

namespace ErrorCodes
{
extern const int HDFS_ERROR;
extern const int CANNOT_OPEN_FILE;
extern const int CANNOT_SEEK_THROUGH_FILE;
extern const int SEEK_POSITION_OUT_OF_BOUND;
extern const int LOGICAL_ERROR;
extern const int UNKNOWN_FILE_SIZE;
}


struct JNIReadBufferFromHDFS::JNIReadBufferFromHDFSImpl : public BufferWithOwnMemory<SeekableReadBuffer>, public WithFileSize
{
    String hdfs_uri;
    String hdfs_file_path;

    /// IMPORTANT: Order matters for destruction!
    /// These must be declared BEFORE file_handle so they are destroyed AFTER it
    arrow::io::internal::LibHdfsShim* driver_;
    HDFSFSPtr fs;

    /// RAII wrapper for HDFS file handle - must be destroyed before HDFSFSPtr fs
    std::unique_ptr<HDFSFileHandle> file_handle;

    ReadSettings read_settings;
    Poco::Util::AbstractConfiguration const & config;

    off_t file_offset = 0;
    off_t read_until_position = 0;
    off_t file_size;
    bool enable_pread = true;

    explicit JNIReadBufferFromHDFSImpl(
        const std::string & hdfs_uri_,
        const std::string & hdfs_file_path_,
        const Poco::Util::AbstractConfiguration & config_,
        const ReadSettings & read_settings_,
        size_t read_until_position_,
        bool use_external_buffer_,
        std::optional<size_t> file_size_)
        : BufferWithOwnMemory<SeekableReadBuffer>(use_external_buffer_ ? 0 : read_settings_.remote_fs_buffer_size)
        , hdfs_uri(hdfs_uri_)
        , hdfs_file_path(hdfs_file_path_)
        , read_settings(read_settings_)
        , config(config_)
        , read_until_position(read_until_position_)
        , enable_pread(read_settings_.enable_hdfs_pread)
    {
        /// Create HDFS connection using factory - one connection per namenode.
        /// Extract the scheme+authority from hdfs_uri (e.g. "viewfs://cluster" or "hdfs://nn:8020").
        LOG_TRACE(getLogger("JNIReadBufferFromHDFSImpl"), "Creating HDFS connection for file: {}, {}", hdfs_file_path, hdfs_uri);
        auto connection = HDFSConnectionFactory::instance().getConnection(hdfs_uri);

        driver_ = connection.driver;
        fs = std::move(connection.fs);

        /// Open file with RAII wrapper
        LOG_TRACE(getLogger("JNIReadBufferFromHDFSImpl"), "Opening HDFS file: {}", hdfs_file_path);
        ProfileEvents::increment(ProfileEvents::HDFSOpenFile);
        hdfsFile fin = driver_->OpenFile(fs.get(), hdfs_file_path.c_str(), O_RDONLY, 0, 0, 0);

        if (fin == nullptr)
        {
            ProfileEvents::increment(ProfileEvents::HDFSOpenFileErrors);
            HDFSConnectionFactory::instance().handleError(__PRETTY_FUNCTION__);
            throw Exception(
                ErrorCodes::CANNOT_OPEN_FILE,
                "Unable to open HDFS file: {}. Error: {}",
                hdfs_file_path,
                getHDFSError(driver_, fs.get()));
        }

        /// Create RAII wrapper for file handle - will be automatically closed on destruction
        file_handle = std::make_unique<HDFSFileHandle>(fs.get(), fin, driver_, hdfs_file_path);

        /// Get file size
        if (file_size_.has_value())
        {
            file_size = file_size_.value();
        }
        else
        {
            ProfileEvents::increment(ProfileEvents::HDFSGetPathInfo);
            auto * file_info = driver_->GetPathInfo(fs.get(), hdfs_file_path.c_str());
            if (!file_info)
            {
                ProfileEvents::increment(ProfileEvents::HDFSGetPathInfoErrors);
                HDFSConnectionFactory::instance().handleError(__PRETTY_FUNCTION__);
                throw Exception(ErrorCodes::UNKNOWN_FILE_SIZE,
                    "Cannot find out file size for: {}. Error: {}",
                    hdfs_file_path,
                    getHDFSError(driver_, fs.get()));
            }
            file_size = static_cast<size_t>(file_info->mSize);
            driver_->hdfsFreeFileInfo(file_info, 1);
            LOG_TRACE(getLogger("JNIReadBufferFromHDFSImpl"), "HDFS file size: {} bytes for {}", file_size, hdfs_file_path);
        }
    }

    ~JNIReadBufferFromHDFSImpl() override = default;


    std::optional<size_t> tryGetFileSize() override
    {
        return file_size;
    }

    off_t getReadUntilPosition() const { return read_until_position; }
    off_t getFileOffset() const { return file_offset; }

    bool nextImpl() override
    {
        size_t num_bytes_to_read;
        if (read_until_position)
        {
            if (read_until_position == file_offset)
            {
                return false;
            }

            if (read_until_position < file_offset)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to read beyond right offset ({} > {})", file_offset, read_until_position - 1);

            num_bytes_to_read = std::min<size_t>(read_until_position - file_offset, internal_buffer.size());
        }
        else
        {
            num_bytes_to_read = internal_buffer.size();
        }
        if (file_size != 0 && file_offset >= file_size)
        {
            return false;
        }

        /// Loop to fill the buffer as much as possible, similar to ReadBufferFromIStream.
        /// This reduces the number of JNI calls and improves performance.
        size_t total_bytes_read = 0;
        char * read_to = internal_buffer.begin();

        while (total_bytes_read < num_bytes_to_read)
        {
            /// Check boundaries on each iteration to respect read_until_position and file_size
            size_t bytes_remaining = num_bytes_to_read - total_bytes_read;

            /// Calculate current position for pread
            off_t current_offset = file_offset + total_bytes_read;

            /// Re-check read_until_position limit on each iteration
            if (read_until_position)
            {
                if (current_offset >= read_until_position)
                {
                    LOG_TRACE(getLogger("HDFSClient"), "[DEBUG] Reached read_until_position {} (current offset: {})", read_until_position, current_offset);
                    break;
                }
                bytes_remaining = std::min<size_t>(bytes_remaining, read_until_position - current_offset);
            }

            /// Re-check file_size limit on each iteration
            if (file_size != 0)
            {
                if (current_offset >= file_size)
                {
                    LOG_TRACE(getLogger("HDFSClient"), "[DEBUG] Reached file_size {} (current offset: {})", file_size, current_offset);
                    break;
                }
                bytes_remaining = std::min<size_t>(bytes_remaining, file_size - current_offset);
            }

            size_t bytes_read = pread(read_to, bytes_remaining, current_offset);

            if (bytes_read == 0)
            {
                LOG_TRACE(getLogger("HDFSClient"), "[DEBUG] Reached EOF for HDFS file: {} (URI: {})", hdfs_file_path, hdfs_uri);
                break; /// EOF reached
            }

            total_bytes_read += bytes_read;
            read_to += bytes_read;
        }

        LOG_TRACE(
            getLogger("HDFSClient"),
            "[DEBUG] Success {} from HDFS total_bytes_read {} / {}",
            "Pread",
            total_bytes_read,
            num_bytes_to_read);

        if (total_bytes_read)
        {
            working_buffer = internal_buffer;
            working_buffer.resize(total_bytes_read);
            file_offset += total_bytes_read;
            if (read_settings.remote_throttler)
                read_settings.remote_throttler->throttle(total_bytes_read);

            return true;
        }

        return false;
    }

    off_t seek(off_t file_offset_, int whence) override
    {
        if (whence != SEEK_SET)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Only SEEK_SET is supported");

        ProfileEvents::increment(ProfileEvents::HDFSSeek);
        int seek_status = driver_->hdfsSeek(fs.get(), file_handle->get(), file_offset_);
        if (seek_status != 0)
        {
            ProfileEvents::increment(ProfileEvents::HDFSSeekErrors);
            HDFSConnectionFactory::instance().handleError(__PRETTY_FUNCTION__);
            throw Exception(
                ErrorCodes::CANNOT_SEEK_THROUGH_FILE,
                "Failed to seek HDFS file: {} to offset: {}. Error: {}",
                hdfs_file_path,
                file_offset_,
                getHDFSError(driver_, fs.get()));
        }
        file_offset = file_offset_;
        resetWorkingBuffer();
        return file_offset;
    }

    off_t getPosition() override
    {
        return file_offset;
    }

    size_t pread(char * buffer, size_t size, size_t offset)
    {
        ResourceGuard rlock(ResourceGuard::Metrics::getIORead(), read_settings.io_scheduling.read_resource_link, size);
        ProfileEvents::increment(ProfileEvents::HDFSPread);
        auto bytes_read = driver_->Pread(fs.get(), file_handle->get(), offset, buffer, safe_cast<int>(size));
        rlock.unlock(std::max(0, bytes_read));

        if (bytes_read < 0)
        {
            ProfileEvents::increment(ProfileEvents::HDFSPreadErrors);
            HDFSConnectionFactory::instance().handleError(__PRETTY_FUNCTION__);
            throw Exception(
                ErrorCodes::HDFS_ERROR,
                "Failed to pread from HDFS file: {} at offset: {} (URI: {}). Error: {}",
                hdfs_file_path,
                offset,
                hdfs_uri,
                getHDFSError(driver_, fs.get()));
        }
        if (bytes_read && read_settings.remote_throttler)
        {
            read_settings.remote_throttler->throttle(bytes_read);
        }
        ProfileEvents::increment(ProfileEvents::HDFSPreadBytes, static_cast<ProfileEvents::Count>(bytes_read));
        return bytes_read;
    }
};


JNIReadBufferFromHDFS::JNIReadBufferFromHDFS(
    const String & hdfs_uri_,
    const String & hdfs_file_path_,
    const Poco::Util::AbstractConfiguration & config_,
    const ReadSettings & read_settings_,
    size_t read_until_position_,
    bool use_external_buffer_,
    std::optional<size_t> file_size_)
    : ReadBufferFromFileBase()
    , impl(std::make_unique<JNIReadBufferFromHDFSImpl>(
          hdfs_uri_, hdfs_file_path_, config_, read_settings_, read_until_position_, use_external_buffer_, file_size_))
    , use_external_buffer(use_external_buffer_)
{
}

JNIReadBufferFromHDFS::~JNIReadBufferFromHDFS() = default;

std::optional<size_t> JNIReadBufferFromHDFS::tryGetFileSize()
{
    return impl->tryGetFileSize();
}

bool JNIReadBufferFromHDFS::nextImpl()
{
    if (use_external_buffer)
    {
           /**
            * use_external_buffer -- means we read into the buffer which
            * was passed to us from somewhere else. We do not check whether
            * previously returned buffer was read or not (no hasPendingData() check is needed),
            * because this branch means we are prefetching data,
            * each nextImpl() call we can fill a different buffer.
            */
        size_t buffer_size = internal_buffer.size();

        /// Limit buffer size by read_until_position to avoid reading beyond the requested range
        auto read_until_position = impl->getReadUntilPosition();
        if (read_until_position)
        {
            auto file_offset = impl->getFileOffset();
            if (read_until_position > file_offset)
            {
                size_t max_bytes_to_read = read_until_position - file_offset;
                buffer_size = std::min(buffer_size, max_bytes_to_read);
            }
            else
            {
                buffer_size = 0;
            }
        }

        impl->set(internal_buffer.begin(), buffer_size);
        assert(working_buffer.begin() != nullptr);
        assert(!internal_buffer.empty());
    }
    else
    {
        impl->position() = impl->buffer().begin() + offset();
        assert(!impl->hasPendingData());
    }

    auto result = impl->next();

    if (result)
        BufferBase::set(impl->buffer().begin(), impl->buffer().size(), impl->offset()); /// use the buffer returned by `impl`

    return result;
}


off_t JNIReadBufferFromHDFS::seek(off_t offset_, int whence)
{
    if (whence != SEEK_SET)
        throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Only SEEK_SET mode is allowed.");

    if (offset_ < 0)
        throw Exception(ErrorCodes::SEEK_POSITION_OUT_OF_BOUND, "Seek position is out of bounds. Offset: {}", offset_);

    if (!working_buffer.empty()
        && size_t(offset_) >= impl->getPosition() - working_buffer.size()
        && offset_ < impl->getPosition())
    {
        pos = working_buffer.end() - (impl->getPosition() - offset_);
        assert(pos >= working_buffer.begin());
        assert(pos <= working_buffer.end());

        return getPosition();
    }

    resetWorkingBuffer();
    impl->seek(offset_, whence);
    return impl->getPosition();
}


off_t JNIReadBufferFromHDFS::getPosition()
{
    return impl->getPosition() - available();
}

size_t JNIReadBufferFromHDFS::getFileOffsetOfBufferEnd() const
{
    return impl->getPosition();
}

String JNIReadBufferFromHDFS::getFileName() const
{
    return impl->hdfs_file_path;
}

size_t JNIReadBufferFromHDFS::readBigAt(char * buffer, size_t size, size_t offset, const std::function<bool(size_t)> &) const
{
    return impl->pread(buffer, size, offset);
}

bool JNIReadBufferFromHDFS::supportsReadAt()
{
    return impl->enable_pread;
}

void JNIReadBufferFromHDFS::setReadUntilPosition(size_t position)
{
    if (impl)
    {
        if (position != static_cast<size_t>(impl->read_until_position))
        {
            impl->read_until_position = position;
        }
    }
}

}
