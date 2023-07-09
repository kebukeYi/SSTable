#include "table.h"

namespace leveldb {

    struct Table::Rep {
        Block *index_block;
        RandomAccessFile *file;
        Options options;
    };

    Status Table::Open(const Options &options, RandomAccessFile *file, uint64_t file_size, Table **table) {
        // 文件 结尾 数据块
        Slice footer_input;
        // 存放结尾块的字节空间 固定28字节空间
        char footer_space[Footer::kEncodedLength];
        // 读取文件末尾 28字节  到内存中
        Status s = file->Read(file_size - Footer::kEncodedLength, Footer::kEncodedLength, &footer_input, footer_space);

        if (!s.ok()) return s;
        // 解析尾巴信息
        Footer footer{};
        // index_handle_ 解析完毕 其中保存了 index block offset
        s = footer.DecodeFrom(&footer_input);
        if (!s.ok()) return s;

        // 索引数据块
        BlockContents index_block_contents;
        ReadOptions opt;
        // 是否打开 检测
        opt.verify_checksums = true;
        // 读取到了数据  data + restarts_
        s = ReadBlock(file, opt, footer.index_handle(), &index_block_contents);

        if (s.ok()) {
            // 计算出 data | restart offset | NumRestarts
            auto *index_block = new Block(index_block_contents);
            Rep *rep = new Table::Rep;
            rep->index_block = index_block;
            rep->file = file;
            rep->options = options;
            *table = new Table(rep);
        }
        return Status::OK();
    }

    Iterator *Table::BlockReader(void *arg, const ReadOptions &options, const Slice &index_value) {
        auto *table = reinterpret_cast<Table *>(arg);
        BlockHandle handle{};
        Slice input = index_value;
        BlockContents contents;
        // 解析出来 offset_  size_
        handle.DecodeFrom(&input);
        // 从文件中 读取 这个 data block 内容
        Status s = ReadBlock(table->rep_->file, options, handle, &contents);
        // 解析 data block 中的 data + restarts_offset_
        Block *block = new Block(contents);
        // data block 迭代器
        return block->NewIterator(table->rep_->options.comparator);
    }

    // 读取数据,回调函数
    Status Table::InternalGet(const ReadOptions &options, const Slice &key,
                              void (*handle_result)(const Slice &, const Slice &)) {
        Status s;

        // 给 index block 建立迭代器
        Iterator *iterator = rep_->index_block->NewIterator(rep_->options.comparator);

        // 在 index block 中定位到 key
        iterator->Seek(key);
        //
        if (iterator->Valid()) {
            // 获得 data block 的 handle
            Slice handle_value = iterator->value();
            // handle 中有 data block 的 offset 和 size
            // 用 block reader 根据 data block handle 建立 data block 的迭代器
            Iterator *block_iter = BlockReader(this, options, handle_value);
            // 定位到 key
            block_iter->Seek(key);
            //
            if (block_iter->Valid()) {
                // 用 handle_result 函数处理 kv 对
                (*handle_result)(block_iter->key(), block_iter->value());
            }
            s = block_iter->status();
            delete block_iter;
        }

        if (s.ok()) {
            s = iterator->status();
        }

        delete iterator;
        return s;
    }
}