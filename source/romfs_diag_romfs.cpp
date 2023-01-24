/*
 * Copyright (c) Atmosphère-NX
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms and conditions of the GNU General Public License,
 * version 2, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
#include <stratosphere.hpp>
#include "romfs_diag_romfs.hpp"

namespace ams::romfs_diag {

    namespace {

        constinit size_t g_peak_size[AllocationType_Count];
        constinit size_t g_allocated_size[AllocationType_Count];
        constinit size_t g_alloc_count[AllocationType_Count];
        constinit size_t g_peak_count[AllocationType_Count];

        constinit size_t g_total_allocated;
        constinit size_t g_peak_allocated;

        constinit size_t g_total_combined_count;
        constinit size_t g_peak_combined_count;

        const char *GetAllocationTypeName(AllocationType type) {
            switch (type) {
                case AllocationType_FileName: return "AllocationType_FileName:";
                case AllocationType_DirName: return "AllocationType_DirName:";
                case AllocationType_FullPath: return "AllocationType_FullPath:";
                case AllocationType_SourceInfo: return "AllocationType_SourceInfo:";
                case AllocationType_BuildFileContext: return "AllocationType_BuildFileContext:";
                case AllocationType_BuildDirContext: return "AllocationType_BuildDirContext:";
                case AllocationType_TableCache: return "AllocationType_TableCache:";
                case AllocationType_DirPointerArray: return "AllocationType_DirPointerArray:";
                case AllocationType_DirContextSet: return "AllocationType_DirContextSet:";
                case AllocationType_FileContextSet: return "AllocationType_FileContextSet:";
                case AllocationType_Header: return "AllocationType_Header:";
                AMS_UNREACHABLE_DEFAULT_CASE();
            }
        }

    }

    void *AllocateTracked(AllocationType type, size_t size) {
        g_total_allocated += size;
        g_allocated_size[type] += size;
        g_alloc_count[type]++;
        g_total_combined_count++;
        g_peak_size[type] = std::max(g_allocated_size[type], g_peak_size[type]);
        g_peak_count[type] = std::max(g_alloc_count[type], g_peak_count[type]);
        g_peak_allocated = std::max(g_total_allocated, g_peak_allocated);
        g_peak_combined_count = std::max(g_total_combined_count, g_peak_combined_count);
        return std::malloc(size);
    }

    void FreeTracked(AllocationType type, void *p, size_t size) {
        g_total_allocated -= size;
        g_allocated_size[type] -= size;
        g_alloc_count[type]--;
        g_total_combined_count--;
        std::free(p);
    }

    namespace {

        constexpr u32 EmptyEntry = 0xFFFFFFFF;
        constexpr size_t FilePartitionOffset = 0x200;

        struct Header {
            s64 header_size;
            s64 dir_hash_table_ofs;
            s64 dir_hash_table_size;
            s64 dir_table_ofs;
            s64 dir_table_size;
            s64 file_hash_table_ofs;
            s64 file_hash_table_size;
            s64 file_table_ofs;
            s64 file_table_size;
            s64 file_partition_ofs;
        };
        static_assert(util::is_pod<Header>::value && sizeof(Header) == 0x50);

        struct DirectoryEntry {
            u32 parent;
            u32 sibling;
            u32 child;
            u32 file;
            u32 hash;
            u32 name_size;
            char name[];
        };
        static_assert(util::is_pod<DirectoryEntry>::value && sizeof(DirectoryEntry) == 0x18);

        struct FileEntry {
            u32 parent;
            u32 sibling;
            s64 offset;
            s64 size;
            u32 hash;
            u32 name_size;
            char name[];
        };
        static_assert(util::is_pod<FileEntry>::value && sizeof(FileEntry) == 0x20);

        class DynamicTableCache {
            NON_COPYABLE(DynamicTableCache);
            NON_MOVEABLE(DynamicTableCache);
            private:
                static constexpr size_t MaxCachedSize = 16_KB;
            private:
                size_t m_cache_bitsize;
                size_t m_cache_size;
            protected:
                void *m_cache;
            protected:
                DynamicTableCache(size_t sz) {
                    m_cache_size = util::CeilingPowerOfTwo(std::min(sz, MaxCachedSize));
                    m_cache = AllocateTracked(AllocationType_TableCache, m_cache_size);
                    while (m_cache == nullptr) {
                        m_cache_size >>= 1;
                        AMS_ABORT_UNLESS(m_cache_size >= 16_KB);
                        m_cache = AllocateTracked(AllocationType_TableCache, m_cache_size);
                    }
                    m_cache_bitsize = util::CountTrailingZeros(m_cache_size);
                }

                ~DynamicTableCache() {
                    FreeTracked(AllocationType_TableCache, m_cache, m_cache_size);
                }

                ALWAYS_INLINE size_t GetCacheSize() const { return static_cast<size_t>(1) << m_cache_bitsize; }
        };

        class HashTableStorage : public DynamicTableCache {
            public:
                HashTableStorage(size_t sz) : DynamicTableCache(sz) { /* ... */ }

                ALWAYS_INLINE u32 *GetBuffer() { return reinterpret_cast<u32 *>(m_cache); }
                ALWAYS_INLINE size_t GetBufferSize() const { return DynamicTableCache::GetCacheSize(); }
        };

        template<typename Entry>
        class TableReader : public DynamicTableCache {
            NON_COPYABLE(TableReader);
            NON_MOVEABLE(TableReader);
            private:
                static constexpr size_t FallbackCacheSize = 1_KB;
            private:
                ams::fs::IStorage *m_storage;
                size_t m_offset;
                size_t m_size;
                size_t m_cache_idx;
                u8 m_fallback_cache[FallbackCacheSize];
            private:
                ALWAYS_INLINE void Read(size_t ofs, void *dst, size_t size) {
                    R_ABORT_UNLESS(m_storage->Read(m_offset + ofs, dst, size));
                }
                ALWAYS_INLINE void ReloadCacheImpl(size_t idx) {
                    const size_t rel_ofs = idx * this->GetCacheSize();
                    AMS_ABORT_UNLESS(rel_ofs < m_size);
                    const size_t new_cache_size = std::min(m_size - rel_ofs, this->GetCacheSize());
                    this->Read(rel_ofs, m_cache, new_cache_size);
                    m_cache_idx = idx;
                }

                ALWAYS_INLINE void ReloadCache(size_t idx) {
                    if (m_cache_idx != idx) {
                        this->ReloadCacheImpl(idx);
                    }
                }

                ALWAYS_INLINE size_t GetCacheIndex(u32 ofs) {
                    return ofs / this->GetCacheSize();
                }
            public:
                TableReader(ams::fs::IStorage *s, size_t ofs, size_t sz) : DynamicTableCache(sz), m_storage(s), m_offset(ofs), m_size(sz), m_cache_idx(0) {
                    AMS_ABORT_UNLESS(m_cache != nullptr);
                    this->ReloadCacheImpl(0);
                }

                const Entry *GetEntry(u32 entry_offset) {
                    this->ReloadCache(this->GetCacheIndex(entry_offset));

                    const size_t ofs = entry_offset % this->GetCacheSize();

                    const Entry *entry = reinterpret_cast<const Entry *>(reinterpret_cast<uintptr_t>(m_cache) + ofs);
                    if (AMS_UNLIKELY(this->GetCacheIndex(entry_offset) != this->GetCacheIndex(entry_offset + sizeof(Entry) + entry->name_size + sizeof(u32)))) {
                        this->Read(entry_offset, m_fallback_cache, std::min(m_size - entry_offset, FallbackCacheSize));
                        entry = reinterpret_cast<const Entry *>(m_fallback_cache);
                    }
                    return entry;
                }
        };

        template<typename Entry>
        class TableWriter : public DynamicTableCache {
            NON_COPYABLE(TableWriter);
            NON_MOVEABLE(TableWriter);
            private:
                static constexpr size_t FallbackCacheSize = 1_KB;
            private:
                fs::FileHandle m_file;
                size_t m_offset;
                size_t m_size;
                size_t m_cache_idx;
                u8 m_fallback_cache[FallbackCacheSize];
                size_t m_fallback_cache_entry_offset;
                size_t m_fallback_cache_entry_size;
                bool m_cache_dirty;
                bool m_fallback_cache_dirty;
            private:
                ALWAYS_INLINE void Read(size_t ofs, void *dst, size_t sz) {
                    u64 read_size;
                    R_ABORT_UNLESS(fs::ReadFile(m_file, static_cast<s64>(m_offset + ofs), dst, sz))
                }

                ALWAYS_INLINE void Write(size_t ofs, const void *src, size_t sz) {
                    R_ABORT_UNLESS(fs::WriteFile(m_file, static_cast<s64>(m_offset + ofs), src, sz, fs::WriteOption::None));
                }

                ALWAYS_INLINE void Flush() {
                    AMS_ABORT_UNLESS(!(m_cache_dirty && m_fallback_cache_dirty));

                    if (m_cache_dirty) {
                        const size_t ofs = m_cache_idx * this->GetCacheSize();
                        this->Write(ofs, m_cache, std::min(m_size - ofs, this->GetCacheSize()));
                        m_cache_dirty = false;
                    }
                    if (m_fallback_cache_dirty) {
                        this->Write(m_fallback_cache_entry_offset, m_fallback_cache, m_fallback_cache_entry_size);
                        m_fallback_cache_dirty = false;
                    }
                }

                ALWAYS_INLINE size_t GetCacheIndex(u32 ofs) {
                    return ofs / this->GetCacheSize();
                }

                ALWAYS_INLINE void RefreshCacheImpl() {
                    const size_t cur_cache = m_cache_idx * this->GetCacheSize();
                    this->Read(cur_cache, m_cache, std::min(m_size - cur_cache, this->GetCacheSize()));
                }

                ALWAYS_INLINE void RefreshCache(u32 entry_offset) {
                    if (size_t idx = this->GetCacheIndex(entry_offset); idx != m_cache_idx || m_fallback_cache_dirty) {
                        this->Flush();
                        m_cache_idx = idx;
                        this->RefreshCacheImpl();
                    }
                }
            public:
                TableWriter(fs::FileHandle f, size_t ofs, size_t sz) : DynamicTableCache(sz), m_file(f), m_offset(ofs), m_size(sz), m_cache_idx(0), m_fallback_cache_entry_offset(), m_fallback_cache_entry_size(), m_cache_dirty(), m_fallback_cache_dirty() {
                    AMS_ABORT_UNLESS(m_cache != nullptr);

                    std::memset(m_cache, 0, this->GetCacheSize());
                    std::memset(m_fallback_cache, 0, sizeof(m_fallback_cache));
                    for (size_t cur = 0; cur < m_size; cur += this->GetCacheSize()) {
                        this->Write(cur, m_cache, std::min(m_size - cur, this->GetCacheSize()));
                    }
                }

                ~TableWriter() {
                    this->Flush();
                }

                Entry *GetEntry(u32 entry_offset, u32 name_len) {
                    this->RefreshCache(entry_offset);

                    const size_t ofs = entry_offset % this->GetCacheSize();

                    Entry *entry = reinterpret_cast<Entry *>(reinterpret_cast<uintptr_t>(m_cache) + ofs);
                    if (ofs + sizeof(Entry) + util::AlignUp(name_len, sizeof(u32)) > this->GetCacheSize()) {
                        this->Flush();

                        m_fallback_cache_entry_offset = entry_offset;
                        m_fallback_cache_entry_size   = sizeof(Entry) + util::AlignUp(name_len, sizeof(u32));
                        this->Read(m_fallback_cache_entry_offset, m_fallback_cache, m_fallback_cache_entry_size);

                        entry = reinterpret_cast<Entry *>(m_fallback_cache);
                        m_fallback_cache_dirty = true;
                    } else {
                        m_cache_dirty = true;
                    }

                    return entry;
                }
        };

        using DirectoryTableWriter = TableWriter<DirectoryEntry>;
        using FileTableWriter      = TableWriter<FileEntry>;

        constexpr inline u32 CalculatePathHash(u32 parent, const char *_path, u32 start, size_t path_len) {
            const unsigned char *path = reinterpret_cast<const unsigned char *>(_path);
            u32 hash = parent ^ 123456789;
            for (size_t i = 0; i < path_len; i++) {
                hash = (hash >> 5) | (hash << 27);
                hash ^= path[start + i];
            }
            return hash;
        }

        constexpr inline size_t GetHashTableSize(size_t num_entries) {
            if (num_entries < 3) {
                return 3;
            } else if (num_entries < 19) {
                return num_entries | 1;
            } else {
                size_t count = num_entries;
                while ((count %  2 == 0) ||
                       (count %  3 == 0) ||
                       (count %  5 == 0) ||
                       (count %  7 == 0) ||
                       (count % 11 == 0) ||
                       (count % 13 == 0) ||
                       (count % 17 == 0))
               {
                   count++;
               }
               return count;
            }
        }

        constinit os::SdkMutex g_fs_romfs_path_lock;
        constinit char g_fs_romfs_path_buffer[fs::EntryNameLengthMax + 1];

        NOINLINE void OpenFileSystemRomfsDirectory(std::unique_ptr<fs::fsa::IDirectory> *out, BuildDirectoryContext *parent, fs::OpenDirectoryMode mode, fs::fsa::IFileSystem *fs) {
            std::scoped_lock lk(g_fs_romfs_path_lock);
            g_fs_romfs_path_buffer[0] = '/';
            parent->GetPath(g_fs_romfs_path_buffer + 1);

            fs::Path fs_path;
            R_ABORT_UNLESS(fs_path.SetShallowBuffer(g_fs_romfs_path_buffer));
            R_ABORT_UNLESS(fs->OpenDirectory(out, fs_path, mode));
        }

    }

    Builder::Builder() : m_num_dirs(0), m_num_files(0), m_dir_table_size(0), m_file_table_size(0), m_dir_hash_table_size(0), m_file_hash_table_size(0), m_file_partition_size(0) {
        auto res = m_directories.emplace(std::unique_ptr<BuildDirectoryContext>(AllocateTyped<BuildDirectoryContext>(AllocationType_BuildDirContext, BuildDirectoryContext::RootTag{})));
        AMS_ABORT_UNLESS(res.second);
        m_root = res.first->get();
        m_num_dirs = 1;
        m_dir_table_size = 0x18;
    }

    void Builder::AddDirectory(BuildDirectoryContext **out, BuildDirectoryContext *parent_ctx, std::unique_ptr<BuildDirectoryContext> child_ctx) {
        /* Set parent context member. */
        child_ctx->parent = parent_ctx;

        /* Check if the directory already exists. */
        auto existing = m_directories.find(child_ctx);
        if (existing != m_directories.end()) {
            *out = existing->get();
            return;
        }

        /* Add a new directory. */
        m_num_dirs++;
        m_dir_table_size += sizeof(DirectoryEntry) + util::AlignUp(child_ctx->path_len, 4);

        *out = child_ctx.get();
        m_directories.emplace(std::move(child_ctx));
    }

    void Builder::AddFile(BuildDirectoryContext *parent_ctx, std::unique_ptr<BuildFileContext> file_ctx) {
        /* Set parent context member. */
        file_ctx->parent = parent_ctx;

        /* Check if the file already exists. */
        if (m_files.find(file_ctx) != m_files.end()) {
            return;
        }

        /* Add a new file. */
        m_num_files++;
        m_file_table_size += sizeof(FileEntry) + util::AlignUp(file_ctx->path_len, 4);
        m_files.emplace(std::move(file_ctx));
    }

    void Builder::VisitDirectory(fs::fsa::IFileSystem *fs, BuildDirectoryContext *parent) {
        std::unique_ptr<fs::fsa::IDirectory> dir;

        /* Get number of child directories. */
        s64 num_child_dirs = 0;
        {
            OpenFileSystemRomfsDirectory(std::addressof(dir), parent, fs::OpenDirectoryMode_Directory, fs);
            ON_SCOPE_EXIT { dir = nullptr; };
            R_ABORT_UNLESS(dir->GetEntryCount(std::addressof(num_child_dirs)));
        }
        AMS_ABORT_UNLESS(num_child_dirs >= 0);

        {
            BuildDirectoryContext **child_dirs = num_child_dirs != 0 ? reinterpret_cast<BuildDirectoryContext **>(AllocateTracked(AllocationType_DirPointerArray, sizeof(BuildDirectoryContext *) * num_child_dirs)) : nullptr;
            AMS_ABORT_UNLESS(num_child_dirs == 0 || child_dirs != nullptr);
            ON_SCOPE_EXIT { if (child_dirs != nullptr) { FreeTracked(AllocationType_DirPointerArray, child_dirs, sizeof(BuildDirectoryContext *) * num_child_dirs); } };

            s64 cur_child_dir_ind = 0;
            {
                OpenFileSystemRomfsDirectory(std::addressof(dir), parent, fs::OpenDirectoryMode_All, fs);
                ON_SCOPE_EXIT { dir = nullptr; };

                s64 read_entries = 0;
                while (true) {
                    R_ABORT_UNLESS(dir->Read(std::addressof(read_entries), std::addressof(m_dir_entry), 1));
                    if (read_entries != 1) {
                        break;
                    }

                    AMS_ABORT_UNLESS(m_dir_entry.type == fs::DirectoryEntryType_File || m_dir_entry.type == fs::DirectoryEntryType_Directory);
                    if (m_dir_entry.type == fs::DirectoryEntryType_Directory) {
                        AMS_ABORT_UNLESS(child_dirs != nullptr);

                        BuildDirectoryContext *real_child = nullptr;

                        this->AddDirectory(std::addressof(real_child), parent, std::unique_ptr<BuildDirectoryContext>(AllocateTyped<BuildDirectoryContext>(AllocationType_BuildDirContext, m_dir_entry.name, strlen(m_dir_entry.name))));
                        AMS_ABORT_UNLESS(real_child != nullptr);
                        child_dirs[cur_child_dir_ind++] = real_child;
                        AMS_ABORT_UNLESS(cur_child_dir_ind <= num_child_dirs);
                    } else /* if (m_dir_entry.type == fs::DirectoryEntryType_File) */ {
                        this->AddFile(parent, std::unique_ptr<BuildFileContext>(AllocateTyped<BuildFileContext>(AllocationType_BuildFileContext, m_dir_entry.name, strlen(m_dir_entry.name), m_dir_entry.file_size, 0, m_cur_source_type)));
                    }
                }
            }

            AMS_ABORT_UNLESS(num_child_dirs == cur_child_dir_ind);
            for (s64 i = 0; i < num_child_dirs; i++) {
                this->VisitDirectory(fs, child_dirs[i]);
            }
        }

    }

    class DirectoryTableReader : public TableReader<DirectoryEntry> {
        public:
            DirectoryTableReader(ams::fs::IStorage *s, size_t ofs, size_t sz) : TableReader(s, ofs, sz) { /* ... */ }
    };

    class FileTableReader : public TableReader<FileEntry> {
        public:
            FileTableReader(ams::fs::IStorage *s, size_t ofs, size_t sz) : TableReader(s, ofs, sz) { /* ... */ }
    };

    void Builder::AddSdFiles(const char *path) {
        /* Create local file system for host root. */
        fssrv::fscreator::LocalFileSystemCreator local_fs_creator(true);
        fs::Path normalized_path;
        R_ABORT_UNLESS(normalized_path.InitializeAsEmpty());
        std::shared_ptr<fs::fsa::IFileSystem> local_fs;
        R_ABORT_UNLESS(static_cast<fssrv::fscreator::ILocalFileSystemCreator &>(local_fs_creator).Create(std::addressof(local_fs), normalized_path, false));

        /* Get the fs path. */
        ams::fs::Path fs_path;
        R_ABORT_UNLESS(fs_path.SetShallowBuffer(path));

        fssystem::SubDirectoryFileSystem subdir_fs(local_fs);
        R_ABORT_UNLESS(subdir_fs.Initialize(fs_path));

        m_cur_source_type = DataSourceType::LooseSdFile;
        this->VisitDirectory(std::addressof(subdir_fs), m_root);
    }

    void Builder::Build(SourceInfoVector *out_infos) {
        printf("Building\n");

        /* Clear output. */
        out_infos->clear();

        /* Calculate hash table sizes. */
        const size_t num_dir_hash_table_entries  = GetHashTableSize(m_num_dirs);
        const size_t num_file_hash_table_entries = GetHashTableSize(m_num_files);
        m_dir_hash_table_size  = sizeof(u32) * num_dir_hash_table_entries;
        m_file_hash_table_size = sizeof(u32) * num_file_hash_table_entries;

        /* Allocate metadata, make pointers. */
        Header *header = reinterpret_cast<Header *>(std::malloc(sizeof(Header)));
        std::memset(header, 0x00, sizeof(*header));

        /* Ensure later hash tables will have correct defaults. */
        static_assert(EmptyEntry == 0xFFFFFFFF);

        /* Emplace metadata source info. */
        out_infos->emplace_back(0, sizeof(*header), DataSourceType::Memory, header);

        /* Process Files. */
        {
            u32 entry_offset = 0;
            BuildFileContext *cur_file = nullptr;
            BuildFileContext *prev_file = nullptr;
            for (const auto &it : m_files) {
                cur_file = it.get();

                /* By default, pad to 0x10 alignment. */
                m_file_partition_size = util::AlignUp(m_file_partition_size, 0x10);

                /* Check if extra padding is present in original source, preserve it to make our life easier. */
                const bool is_storage_or_file = cur_file->source_type == DataSourceType::Storage || cur_file->source_type == DataSourceType::File;
                if (prev_file != nullptr && prev_file->source_type == cur_file->source_type && is_storage_or_file) {
                    const s64 expected = m_file_partition_size - prev_file->offset + prev_file->orig_offset;
                    if (expected != cur_file->orig_offset) {
                        AMS_ABORT_UNLESS(expected <= cur_file->orig_offset);
                        m_file_partition_size += cur_file->orig_offset - expected;
                    }
                }

                /* Calculate offsets. */
                cur_file->offset = m_file_partition_size;
                m_file_partition_size += cur_file->size;
                cur_file->entry_offset = entry_offset;
                entry_offset += sizeof(FileEntry) + util::AlignUp(cur_file->path_len, 4);

                /* Save current file as prev for next iteration. */
                prev_file = cur_file;
            }
            /* Assign deferred parent/sibling ownership. */
            for (auto it = m_files.rbegin(); it != m_files.rend(); it++) {
                cur_file = it->get();
                cur_file->sibling = cur_file->parent->file;
                cur_file->parent->file = cur_file;
            }
        }

        /* Process Directories. */
        {
            u32 entry_offset = 0;
            BuildDirectoryContext *cur_dir = nullptr;
            for (const auto &it : m_directories) {
                cur_dir = it.get();
                cur_dir->entry_offset = entry_offset;
                entry_offset += sizeof(DirectoryEntry) + util::AlignUp(cur_dir->path_len, 4);
            }
            /* Assign deferred parent/sibling ownership. */
            for (auto it = m_directories.rbegin(); it != m_directories.rend(); it++) {
                cur_dir = it->get();
                if (cur_dir == m_root) {
                    continue;
                }
                cur_dir->sibling = cur_dir->parent->child;
                cur_dir->parent->child = cur_dir;
            }
        }

        /* Set all files' hash value = hash index. */
        for (const auto &it : m_files) {
            BuildFileContext *cur_file = it.get();
            cur_file->hash_value = CalculatePathHash(cur_file->parent->entry_offset, cur_file->path.get(), 0, cur_file->path_len) % num_file_hash_table_entries;
        }

        /* Set all directories' hash value = hash index. */
        for (const auto &it : m_directories) {
            BuildDirectoryContext *cur_dir = it.get();
            cur_dir->hash_value = CalculatePathHash(cur_dir == m_root ? 0 : cur_dir->parent->entry_offset, cur_dir->path.get(), 0, cur_dir->path_len) % num_dir_hash_table_entries;
        }

        /* Write hash tables. */
        {
            HashTableStorage hash_table_storage(std::max(m_dir_hash_table_size, m_file_hash_table_size));

            u32 *hash_table = hash_table_storage.GetBuffer();
            size_t hash_table_size = hash_table_storage.GetBufferSize();

            /* Write the file hash table. */
            for (size_t ofs = 0; ofs < m_file_hash_table_size; ofs += hash_table_size) {
                std::memset(hash_table, 0xFF, hash_table_size);

                const u32 ofs_ind = ofs / sizeof(u32);
                const u32 end_ind = (ofs + hash_table_size) / sizeof(u32);

                for (const auto &it : m_files) {
                    BuildFileContext *cur_file = it.get();
                    if (cur_file->HasHashMark()) {
                        continue;
                    }

                    if (const auto hash_ind = cur_file->hash_value; ofs_ind <= hash_ind && hash_ind < end_ind) {
                        cur_file->hash_value = hash_table[hash_ind - ofs_ind];
                        hash_table[hash_ind - ofs_ind] = cur_file->entry_offset;

                        cur_file->SetHashMark();
                    }
                }
            }

            /* Write the directory hash table. */
            for (size_t ofs = 0; ofs < m_dir_hash_table_size; ofs += hash_table_size) {
                std::memset(hash_table, 0xFF, hash_table_size);

                const u32 ofs_ind = ofs / sizeof(u32);
                const u32 end_ind = (ofs + hash_table_size) / sizeof(u32);

                for (const auto &it : m_directories) {
                    BuildDirectoryContext *cur_dir = it.get();
                    if (cur_dir->HasHashMark()) {
                        continue;
                    }

                    if (const auto hash_ind = cur_dir->hash_value; ofs_ind <= hash_ind && hash_ind < end_ind) {
                        cur_dir->hash_value = hash_table[hash_ind - ofs_ind];
                        hash_table[hash_ind - ofs_ind] = cur_dir->entry_offset;

                        cur_dir->SetHashMark();
                    }
                }
            }
        }

        /* Replace sibling pointers with sibling entry_offsets, so that we can de-allocate as we go. */
        {
            /* Set all directories sibling and file pointers. */
            for (const auto &it : m_directories) {
                BuildDirectoryContext *cur_dir = it.get();

                cur_dir->ClearHashMark();

                cur_dir->sibling_offset = (cur_dir->sibling == nullptr) ? EmptyEntry : cur_dir->sibling->entry_offset;
                cur_dir->child_offset   = (cur_dir->child   == nullptr) ? EmptyEntry : cur_dir->child->entry_offset;
                cur_dir->file_offset    = (cur_dir->file    == nullptr) ? EmptyEntry : cur_dir->file->entry_offset;

                cur_dir->parent_offset  = cur_dir == m_root ? 0 : cur_dir->parent->entry_offset;
            }

            /* Replace all files' sibling pointers. */
            for (const auto &it : m_files) {
                BuildFileContext *cur_file = it.get();

                cur_file->ClearHashMark();

                cur_file->sibling_offset = (cur_file->sibling == nullptr) ? EmptyEntry : cur_file->sibling->entry_offset;
            }
        }

        /* Write the file table. */
        {
            //FileTableWriter file_table(std::addressof(metadata_file), m_dir_hash_table_size + m_dir_table_size + m_file_hash_table_size, m_file_table_size);

            for (auto it = m_files.begin(); it != m_files.end(); it = m_files.erase(it)) {
                BuildFileContext *cur_file = it->get();
                //FileEntry *cur_entry = file_table.GetEntry(cur_file->entry_offset, cur_file->path_len);
                //
                ///* Set entry fields. */
                //cur_entry->parent  = cur_file->parent->entry_offset;
                //cur_entry->sibling = cur_file->sibling_offset;
                //cur_entry->offset  = cur_file->offset;
                //cur_entry->size    = cur_file->size;
                //cur_entry->hash    = cur_file->hash_value;
                //
                ///* Set name. */
                //const u32 name_size = cur_file->path_len;
                //cur_entry->name_size = name_size;
                //if (name_size) {
                //    std::memcpy(cur_entry->name, cur_file->path.get(), name_size);
                //    for (size_t i = name_size; i < util::AlignUp(name_size, 4); i++) {
                //        cur_entry->name[i] = 0;
                //    }
                //}

                /* Emplace a source. */
                switch (cur_file->source_type) {
                    case DataSourceType::Storage:
                    case DataSourceType::File:
                        {
                            /* Try to compact if possible. */
                            auto &back = out_infos->back();
                            if (back.source_type == cur_file->source_type) {
                                back.size = cur_file->offset + FilePartitionOffset + cur_file->size - back.virtual_offset;
                            } else {
                                out_infos->emplace_back(cur_file->offset + FilePartitionOffset, cur_file->size, cur_file->source_type, cur_file->orig_offset + FilePartitionOffset);
                            }
                        }
                        break;
                    case DataSourceType::LooseSdFile:
                        {
                            char full_path[fs::EntryNameLengthMax + 1];
                            const size_t path_needed_size = cur_file->GetPathLength() + 1;
                            AMS_ABORT_UNLESS(path_needed_size <= sizeof(full_path));
                            cur_file->GetPath(full_path);

                            FreeTracked(AllocationType_FileName, cur_file->path.release(), cur_file->path_len + 1);

                            char *new_path = static_cast<char *>(AllocateTracked(AllocationType_FullPath, path_needed_size));
                            std::memcpy(new_path, full_path, path_needed_size);
                            out_infos->emplace_back(cur_file->offset + FilePartitionOffset, cur_file->size, cur_file->source_type, new_path);
                        }
                        break;
                    AMS_UNREACHABLE_DEFAULT_CASE();
                }
            }
        }
        printf("FileTable\n");

        /* Write the directory table. */
        {
            //DirectoryTableWriter dir_table(std::addressof(metadata_file), m_dir_hash_table_size, m_dir_table_size);

            for (auto it = m_directories.begin(); it != m_directories.end(); it = m_directories.erase(it)) {
                //BuildDirectoryContext *cur_dir = it->get();
                //DirectoryEntry *cur_entry = dir_table.GetEntry(cur_dir->entry_offset, cur_dir->path_len);
                //
                ///* Set entry fields. */
                //cur_entry->parent  = cur_dir->parent_offset;
                //cur_entry->sibling = cur_dir->sibling_offset;
                //cur_entry->child   = cur_dir->child_offset;
                //cur_entry->file    = cur_dir->file_offset;
                //cur_entry->hash    = cur_dir->hash_value;
                //
                ///* Set name. */
                //const u32 name_size = cur_dir->path_len;
                //cur_entry->name_size = name_size;
                //if (name_size) {
                //    std::memcpy(cur_entry->name, cur_dir->path.get(), name_size);
                //    for (size_t i = name_size; i < util::AlignUp(name_size, 4); i++) {
                //        cur_entry->name[i] = 0;
                //    }
                //}
            }
        }
        printf("DirTable\n");

        /* Delete maps. */
        m_root = nullptr;
        m_directories.clear();
        m_files.clear();

        /* Set header fields. */
        header->header_size          = sizeof(*header);
        header->file_hash_table_size = m_file_hash_table_size;
        header->file_table_size      = m_file_table_size;
        header->dir_hash_table_size  = m_dir_hash_table_size;
        header->dir_table_size       = m_dir_table_size;
        header->file_partition_ofs   = FilePartitionOffset;
        header->dir_hash_table_ofs   = util::AlignUp(FilePartitionOffset + m_file_partition_size, 4);
        header->dir_table_ofs        = header->dir_hash_table_ofs + header->dir_hash_table_size;
        header->file_hash_table_ofs  = header->dir_table_ofs + header->dir_table_size;
        header->file_table_ofs       = header->file_hash_table_ofs + header->file_hash_table_size;

        ///* Save metadata to the SD card, to save on memory space. */
        //{
        //    R_ABORT_UNLESS(fsFileFlush(std::addressof(metadata_file)));
        //    out_infos->emplace_back(header->dir_hash_table_ofs, metadata_size, DataSourceType::Metadata, new RemoteFile(metadata_file));
        //}

        printf("=== Count by Type ===\n");
        for (AllocationType i = static_cast<AllocationType>(0); i < AllocationType_Count; i = static_cast<AllocationType>(util::ToUnderlying(i) + 1)) {
            printf("%-36s %zu\n", GetAllocationTypeName(i), g_alloc_count[i]);
        }
        printf("===\n\n");

        printf("=== Peak Count by Type ===\n");
        for (AllocationType i = static_cast<AllocationType>(0); i < AllocationType_Count; i = static_cast<AllocationType>(util::ToUnderlying(i) + 1)) {
            printf("%-36s %zu\n", GetAllocationTypeName(i), g_peak_count[i]);
        }
        printf("===\n\n");

        printf("=== Allocation by Type ===\n");
        for (AllocationType i = static_cast<AllocationType>(0); i < AllocationType_Count; i = static_cast<AllocationType>(util::ToUnderlying(i) + 1)) {
            printf("%-36s %zu (%zu KiB)\n", GetAllocationTypeName(i), g_allocated_size[i], g_allocated_size[i] / 1_KB);
        }
        printf("===\n\n");

        printf("=== Peak by Type ===\n");
        for (AllocationType i = static_cast<AllocationType>(0); i < AllocationType_Count; i = static_cast<AllocationType>(util::ToUnderlying(i) + 1)) {
            printf("%-36s %zu (%zu KiB)\n", GetAllocationTypeName(i), g_peak_size[i], g_peak_size[i] / 1_KB);
        }
        printf("===\n\n");

        printf("=== Stats ===\n");
        printf("Total Allocated: %zu (%zu KiB)\n", g_total_allocated, g_total_allocated / 1_KB);
        printf("Peak Allocated:  %zu (%zu KiB)\n", g_peak_allocated, g_peak_allocated / 1_KB);
        printf("Total Count: %zu\n", g_total_combined_count);
        printf("Peak Count:  %zu\n", g_peak_combined_count);
        printf("===\n\n");
    }

}
