#include <filesystem>
#include <fstream>
#include <iostream>

#include "common/exception/storage.h"
#include "common/file_system/file_system.h"
#include "common/file_system/local_file_system.h"
#include "common/serializer/buffered_file.h"
#include "common/serializer/deserializer.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/storage_utils.h"
#include "storage/wal/checksum_reader.h"
#include "storage/wal/wal_record.h"

using namespace lbug::common;
using namespace lbug::storage;

static constexpr std::string_view checksumMismatchMessage =
    "Checksum verification failed, the WAL file is corrupted.";

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <database_path>\n";
        return 1;
    }

    std::string databasePath = argv[1];
    std::string walPath = StorageUtils::getWALFilePath(databasePath);

    std::cout << "WAL File: " << walPath << "\n\n";

    if (!std::filesystem::exists(walPath)) {
        std::cout << "WAL file does not exist. Database was cleanly shutdown or no modifications "
                     "were made.\n";
        return 0;
    }

    try {
        LocalFileSystem lfs("");
        auto fileInfo = lfs.openFile(walPath, FileOpenFlags(FileFlags::READ_ONLY));
        auto fileSize = fileInfo->getFileSize();
        if (fileSize == 0) {
            std::cout << "WAL file is empty. Database was cleanly shutdown.\n";
            return 0;
        }

        Deserializer headerDeserializer(std::make_unique<BufferedFileReader>(*fileInfo));

        headerDeserializer.getReader()->onObjectBegin();
        WALHeader walHeader{};
        headerDeserializer.deserializeValue(walHeader.databaseID);
        uint8_t enableChecksumsBytes = 0;
        headerDeserializer.deserializeValue(enableChecksumsBytes);
        walHeader.enableChecksums = enableChecksumsBytes != 0;
        headerDeserializer.getReader()->onObjectEnd();

        std::cout << "WAL Header:\n";
        std::cout << "  Database ID: " << UUID::toString(walHeader.databaseID) << "\n";
        std::cout << "  Checksums Enabled: " << (walHeader.enableChecksums ? "true" : "false")
                  << "\n";
        std::cout << "  File Size: " << fileSize << " bytes\n\n";

        std::cout << "Record offsets:\n";

        uint64_t recordCount = 0;
        uint64_t lastOffset = 0;
        uint64_t errorCount = 0;

        if (walHeader.enableChecksums) {
            auto bm = std::make_unique<BufferManager>(databasePath, "", 32 * 1024 * 1024, 8388608,
                nullptr, true);
            auto mm = std::make_unique<MemoryManager>(bm.get(), nullptr);
            fileInfo->seek(17, SEEK_SET);
            auto checksumReader =
                std::make_unique<ChecksumReader>(*fileInfo, *mm, checksumMismatchMessage);
            Deserializer deserializer(std::move(checksumReader));

            while (!deserializer.finished()) {
                lastOffset = deserializer.getReader()->cast<ChecksumReader>()->getReadOffset();
                std::cout << "  " << lastOffset << "\n";
                recordCount++;

                try {
                    deserializer.getReader()->onObjectBegin();
                    deserializer.getReader()->onObjectEnd();
                } catch (const StorageException& e) {
                    std::cerr << "\nError: WAL file is corrupted - checksum verification failed.\n";
                    std::cerr << "This WAL file cannot be read.\n";
                    return 1;
                } catch (const std::exception& e) {
                    std::cerr << "\nError at offset " << lastOffset << ": " << e.what() << "\n";
                    errorCount++;
                    if (errorCount > 10) {
                        std::cerr << "\nToo many errors, stopping.\n";
                        break;
                    }
                }
            }
        } else {
            Deserializer deserializer(std::make_unique<BufferedFileReader>(*fileInfo));

            while (!deserializer.finished()) {
                lastOffset = deserializer.getReader()->cast<BufferedFileReader>()->getReadOffset();
                std::cout << "  " << lastOffset << "\n";
                recordCount++;

                try {
                    deserializer.getReader()->onObjectBegin();
                    deserializer.getReader()->onObjectEnd();
                } catch (const std::exception& e) {
                    std::cerr << "\nError at offset " << lastOffset << ": " << e.what() << "\n";
                    errorCount++;
                    if (errorCount > 10) {
                        std::cerr << "\nToo many errors, stopping.\n";
                        break;
                    }
                }
            }
        }

        std::cout << "\nTotal records found: " << recordCount << "\n";
        if (errorCount > 0) {
            std::cout << "Records with errors: " << errorCount << "\n";
        }
        std::cout << "Last offset: " << lastOffset << "\n";

    } catch (const std::exception& e) {
        std::cerr << "Error reading WAL file: " << e.what() << "\n";
        return 1;
    }

    return 0;
}
