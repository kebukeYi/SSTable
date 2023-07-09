#ifndef SSTABLE_BLOCK_H
#define SSTABLE_BLOCK_H

#include <status.h>
#include "slice.h"
#include "../include/comparator.h"
#include "../include/iterator.h"
#include "format.h"

namespace leveldb {
    struct BlockContents;

    class Block {
    public:
        // 传入一个构建好的Block内容初始化Block对象
        // 禁止Block block = "contents";这种调用方法
        // 只能Block block = Block::Block("contents");
        explicit Block(BlockContents contents);

        ~Block();

        Iterator *NewIterator(const Comparator *comparator);

    private:
        class Iter;
        // data 区域 起始地址
        const char *data_;
        // 大小与  unsigned int  或  unsigned long  相同
        size_t size_; // size_ 要参与和 sizeof() 的计算，同时它并不会为了持久化被编码，所以声明为 size_t，其它的变量都是uint32_t
        uint32_t restarts_offset_; //

        bool owned;

        uint32_t NumRestarts() const;
    };
}
#endif //SSTABLE_BLOCK_H
