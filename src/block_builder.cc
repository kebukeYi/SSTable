#include "block_builder.h"

#include <utility>

namespace leveldb {


    void BlockBuilder::Add(const Slice &key, const Slice &value) {
        // data block  : last_key_
        // index block : last_key_
        Slice last_key_piece(last_key_);

        assert(!finished_);

        assert(counter_ <= options_->block_restart_interval);
        // assert(buffer_.empty());
        // assert(options_->comparator->Compare(key, last_key_piece) > 0);

        // 初始化共享长度，第一个Entry为0，因为保存的是完整的key
        size_t shared = 0;

        // 还未到 组合 阈值 16
        if (counter_ < options_->block_restart_interval) {
            // 如果不是第一个Entry，就需要特别计算共享长度
            // 从左向右遍历的长度，取两个key的最小值即可，因为我们要比较同一位置的字符，比较的前提是两个key都要有这个字符
            const size_t min_length = std::min(last_key_piece.size(), key.size());

            // 如果遍历未到头，后面还有字符可以遍历，同时当前位置的字符相同，则继续遍历，看看下一个字符是否相同
            while ((shared < min_length) && (last_key_piece[shared] == key[shared])) {
                shared++; //同一位置字符相同，共享长度加一
            }
        } else {
            // counter_ 到达阈值，保存住 buffer 的大小
            // 16个 entry 为一组，然后记录每个组的大小
            // 在持久化 block 时也需要计算这个
            restarts_.push_back(buffer_.size());
            counter_ = 0;
        }

        // 非共享长度等于总长度减去共享长度
        const size_t non_shared = key.size() - shared;

        // 将共享长度、非共享长度、value 的长度放入  buffer_  12字节
        PutVarint32(&buffer_, shared);
        PutVarint32(&buffer_, non_shared);
        PutVarint32(&buffer_, value.size());

        // keya keyb => b
        // 将 非共享key字段 写入buffer_，共享的key就不用了 1字节
        buffer_.append(key.data() + shared, non_shared);

        // 将 value 写入 buffer_ 6字节
        buffer_.append(value.data(), value.size());

        // 把上一个完整的 key 缩减到共享 key 的长度
        last_key_.resize(shared);
        // 加上非共享长度就是当前的 key，这样节省共享key的拷贝
        last_key_.append(key.data() + shared, non_shared);
        // 保证拼接的key和key相等
        assert(Slice(last_key_) == key);

        //Entry个数相加
        counter_++;
    }

    Slice BlockBuilder::Finish() {
        // 这里面存放的是 16个entry为一组的 大小值
        for (unsigned int restart: restarts_) {
            // 因为要进行二分查找，所以使用固定大小的空间来存储 restart point
            PutFixed32(&buffer_, restart);
        }
        // 4 字节
        PutFixed32(&buffer_, restarts_.size());
        finished_ = true;
        return Slice(buffer_);
    }

    BlockBuilder::BlockBuilder(const Options *options, std::string name)
            : options_(options), restarts_(), counter_(0), finished_(false), name_(std::move(name)) {
        restarts_.push_back(0);
    }

    size_t BlockBuilder::CurrentSizeEstimate() const {
        size_t buffers = buffer_.size();
        size_t res = restarts_.size() * sizeof(uint32_t);
        return (buffers + res + sizeof(uint32_t));
    }

    void BlockBuilder::Reset() {
        buffer_.clear();

        restarts_.clear();
        restarts_.push_back(0);

        counter_ = 0;
        finished_ = false;

        last_key_.clear();
    }
}
