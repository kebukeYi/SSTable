#include "table_builder.h"

namespace leveldb {

    // 这里之所以要特意用一个结构体来存储变量而不直接在类中定义变量
    // 是因为table_builder.cc是供用户使用的
    // leveldb不希望底层的参数被用户访问或者修改
    // 此外也是为了把参数和接口解耦，便于升级
    // https://stackoverflow.com/questions/33427916/why-table-and-tablebuilder-in-leveldb-use-struct-rep
    struct TableBuilder::Rep {
        Options options;
        Options index_block_options;

        BlockBuilder data_block;
        BlockBuilder index_block;

        BlockHandle pending_handle;// pre_pending_handle
        WritableFile *file;
        bool pending_index_entry;
        Status status;

        std::string compressed_output;
        uint64_t offset;

        std::string last_key;

        Rep(const Options &opt, WritableFile *f)
                : options(opt),
                  index_block_options(opt),
                  index_block(&index_block_options, std::string("index block")),
                  data_block(&opt, std::string("data block")),
                  file(f),
                  offset(0),
                  pending_index_entry(false)// 刚刚开始时，不向index block写入数据
        {}
    };

    TableBuilder::TableBuilder(const Options &options, WritableFile *file) : rep_(new Rep(options, file)) {
    }

    // TableBuilder
    void TableBuilder::Add(const Slice &key, const Slice &value) {
        Rep *r = rep_;// db 实例

        // 如果之前 持久化了一个 data block，则准备向 index block 插入一条指向它的 kv 对
        if (r->pending_index_entry) {
            // 找一个介于全局 last_key 和 key 之间的，最短的 mid_key, 目的是什么？为的是 缩短key占用空间
            // 比如说 zzzzb + zzc = zzd
            // 没有任何交集的话 last_key 不变，否则只会变得更短
            r->options.comparator->FindShortestSeparator(&r->last_key, key);

            // 把上一次刷盘的 data block 的 handle 位移信息编码为字符串
            std::string pre_data_handle_encoding;
            // pending_handle 存放的是 上一次 data block 刷盘的信息
            r->pending_handle.EncodeTo(&pre_data_handle_encoding);

            // BlockBuilder 类型: 写入 index block
            // 此时的 r->last_key 可能会发生变化，然后将其所在的位移信息 加入到 index 块中
            // 上一波全局 last_key 与 现在的 key 进行 匹配 && 结果再跟 index block 中的 上一个 last_key_ 进行匹配 && 存放在 index block 中追加在
            // 上一个 data block 块之后
            r->index_block.Add(r->last_key, Slice(pre_data_handle_encoding));
            r->pending_index_entry = false;
        }

        // 上一步 没有持久化
        // 更新全局 last_key ，由于不用前缀压缩，所以直接把key复制进来
        r->last_key.assign(key.data(), key.size());
        // BlockBuilder 类型 写入 data block
        r->data_block.Add(key, value);

        // 估计 data block 的大小
        const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
        //如果 Block 的大小达到了阈值，则此 Block 已满
        if (estimated_block_size >= r->options.block_size) {
            // 合并Entry 和 restart point，并持久化到磁盘
            Flush();
        }
    }

    void TableBuilder::Flush() {
        Rep *r = rep_;

        // 持久化到磁盘，并生成 BlockHandle 到 pending_handle,以供下次 写时,将这次的信息 组装成 index 来保存
        // 先用 snappy 压缩，后进行 crc 编码，最终持久化到磁盘,更新全局 offset
        WriteBlock(&r->data_block, &r->pending_handle);

        if (ok()) {
            // 此时 block 已经 刷盘了，并且 刷盘的位置 大小 赋值给了 &r->pending_handle 变量
            // 那么接下开怎么做？让下一次 key 参与这次刷盘后的 index block 的建设
            r->pending_index_entry = true;
            // 调用文件系统的刷新接口，实际上并没有真正地持久化到磁盘，还是有可能存储在文件系统的buffer pool
            // 甚至是FTL的cache里
            r->status = r->file->Flush();
        }
    }

    // 持久化一个Block
    // 本函数的工作是对block中的数据进行压缩（如果需要的话）
    // 压缩之后调用WriteRawBlock真正进行持久化
    void TableBuilder::WriteBlock(BlockBuilder *block, BlockHandle *pending_handle) {
        Rep *r = rep_;
        // 将Block的各个部分合并 restarts_ 起来  还是在内存中
        Slice raw = block->Finish();
        // 获取压缩类型，默认是采用 snappy 压缩
        CompressionType type = r->options.compression;
        Slice block_contents;

        switch (type) {
            // 如果不压缩，则直接持久化原数据
            case kNoCompression:
                block_contents = raw;
                break;
                // 如果采用snappy压缩，则要先检查压缩率是否达标
            case kSnappyCompression: {
                std::string *compressed = &r->compressed_output;
                // 当 CMakeLists 中的 HAVE_SNAPPY 没有被开启，Snappy_Compress 就不运行实际的压缩程序
                // 压缩就会失败，Snappy_Compress() 就会返回false
                // 如果压缩成功，且压缩率大于 1/8，说明压缩了之后不会增加解析时间
                if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
                    compressed->size() < raw.size() - (raw.size() / 8u)) {
                    // 把压缩后的数据持久化
                    block_contents = *compressed;
                } else { // 如果未启用Snappy，或者压缩率不够，则还是只持久化原数据
                    block_contents = raw;
                    // 压缩类型改为不压缩，这样读取的时候就不会解压缩
                    type = kNoCompression;
                }
                break;
            }
        }

        // 将处理好的数据block_contents和压缩类型type持久化到磁盘
        // 并且赋值 数据开头位置偏移量 和 长度 到 pending_handle 指针中
        WriteRawBlock(block_contents, type, pending_handle);

        // 清除保存压缩数据的变量
        r->compressed_output.clear();
        // 重置 block builder 的缓存数据
        block->Reset();
    }

    // 真正持久化经过压缩处理的block数据
    // 持久化前进行crc编码，方便校验
    void TableBuilder::WriteRawBlock(const Slice &block_contents, CompressionType type, BlockHandle *pending_handle) {
        Rep *r = rep_;

        // 更新 pending_handle ,赋值 此组 block 在文件中的开始偏移量和长度 包含(data + type + crc)
        // 当前文件的 尾偏移量 就是要持久化block的开头偏移量
        pending_handle->set_offset(r->offset);
        // size 不包括 type 和 cr c值, 但是包括 restarts_ 数据
        // 这样也可以读取到,也type和crc的长度都是固定的
        pending_handle->set_size(block_contents.size());

        // 向文件末尾追加这个block数据
        r->status = r->file->Append(block_contents);

        // 如果写入成功，就进行追加 crc 编码
        if (r->status.ok()) {
            // kBlockTrailerSize = type + crc 值
            // type(8bit, 1Byte) + crc(uint32_t, 32bit, 4Byte) = 5Byte
            char trailer[kBlockTrailerSize];
            // 赋值入压缩的类型 1B
            trailer[0] = type;

            // 计算 block 数据的 crc 值
            // 底层调用了下面的 crc32c::Extend
            // Extend(0, data, n)
            // 计算 data[0, n - 1] 的 crc 值
            uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());

            // 计算 trailer[0, 0] 的 crc 值
            // trailer[0, 0] 就是 type
            // 使用block的crc值作为种子
            crc = crc32c::Extend(crc, trailer, 1);

            // 计算crc的掩码，写入到trailer的后4个Byte
            // 注释说只计算crc值会有问题
            // 这里有更详细的讨论: https://stackoverflow.com/questions/61639618/why-leveldb-and-rocksdb-need-a-masked-crc32
            EncodeFixed32(trailer + 1, crc32c::Mask(crc));

            // 将type和crc校验码写入文件
            r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));

            if (r->status.ok()) {
                // 更新全局文件偏移量
                r->offset += block_contents.size() + kBlockTrailerSize;
            }
        }
    }

    Status TableBuilder::Finish() {
        Rep *r = rep_;
        // data_block 强制刷盘一波 , 因为 之前的写 data block 可能没有达到刷盘阈值 还在内存中
        Flush();
        // index 索引所写的位置位移处
        BlockHandle index_block_handle;

        if (ok()) {
            // 还有没达到阈值的 data block, 需要额外封装成一个 data block
            if (r->pending_index_entry) {
                // 找到一个比last_key大的短key
                // 因为最后一个 data block 已经没有下一个 data block 了
                // zzzzb -> zzzzc
                r->options.comparator->FindShortSuccessor(&r->last_key);

                // 编码成字符串
                std::string handle_encoding;
                // pending_handle 存放的是 pre data block 的刷盘信息
                r->pending_handle.EncodeTo(&handle_encoding);

                // 写入 index block
                r->index_block.Add(r->last_key, Slice(handle_encoding));
                r->pending_index_entry = false;
            }
            // 所有 data block 的 handler 都已经被写入到了 index block 了，持久化 index block
            // 获得 index block 的 index block handle
            WriteBlock(&r->index_block, &index_block_handle);
        }

        if (ok()) {
            // 位移信息
            Footer footer{};
            // 将 index_block_handle : offset size 写入到 footer
            footer.set_index_handle(index_block_handle);

            // 给 footer 加入 padding 和 magic number
            // 把它编码为字符串
            std::string footer_encoding;
            footer.EncodeTo(&footer_encoding);
            // footer_encoding 是 28 字节
            // 写入 footer 数据
            r->status = r->file->Append(Slice(footer_encoding));

            // 更新 全局 偏移量
            if (r->status.ok()) {
                r->offset += footer_encoding.size();
            }
        }
        return r->status;
    }

    // 把内存的数据都写入到磁盘
    Status TableBuilder::Sync() {
        return rep_->file->Sync();
    }

    Status TableBuilder::status() const {
        return rep_->status;
    }

    // 临时加的，方便测试用，实际是不可能把rep_的内部数据返回的
    // 完整的sstable中，Block的BlockHandle是写入index block的
    BlockHandle TableBuilder::ReturnBlockHandle() {
        return rep_->pending_handle;
    }

    uint64_t TableBuilder::FileSize() const {
        return rep_->offset;
    }
}
