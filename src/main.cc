#include <iostream>
#include <random>
#include "block_builder.h"
#include "block.h"
#include "snappy.h"
#include "table.h"
#include "string"

#define OS "Linux"
#define KV_NUM 160 * 160
// #define KV_NUM 40

std::string path;
std::vector<std::string> test_case;

leveldb::Options options;
leveldb::ReadOptions readOptions;

leveldb::Env *env;
leveldb::Status s;

leveldb::WritableFile *file = nullptr;
leveldb::RandomAccessFile *randomAccessFile = nullptr;

std::string key = "key";
std::string value = "value";

int get_queue[KV_NUM];

// 如果有报错，就显示出来
void check_status(const leveldb::Status &s) {
    if (!s.ok()) {
        std::cout << s.ToString() << std::endl;
        exit(0);
    }
}

// 配置一些参数
void init_config() {
    // 包含了 比较器 环境器
    options = leveldb::Options();
    // 读操作？
    readOptions = leveldb::ReadOptions();
    readOptions.verify_checksums = true;
    env = leveldb::Env::Default();
}

// 根据系统选取地址
leveldb::Status init_path() {
    leveldb::Status s;
    if (OS == "WIN") {
        path = "./data/test_case.sst";
        s = leveldb::Status::OK();
    } else if (OS == "Linux") {
        path = "/home/wangtingzheng/source/test_case.sst";
        s = leveldb::Status::OK();
    } else {
        s = leveldb::Status::Corruption("Can not find OS", OS);
    }
    return s;
}

// 生成num对 符合comparator的KV对
std::vector<std::string> generate(int num) {
    std::vector<std::string> dst;

    for (int i = 1; i <= num; i++) {
        std::string temp;
        for (int j = 0; j < i / 26; ++j) {
            temp.append("z");
        }

        if (i % 26 != 0) {
            temp += char(96 + i % 26);
        }
        dst.push_back(temp);
    }
    return dst;
}

// 打乱[0, KV_NUM - 1]
void init_get_queue() {
    for (int i = 0; i < KV_NUM; i++) {
        get_queue[i] = i;
    }
    std::shuffle(get_queue, get_queue + KV_NUM, std::mt19937(std::random_device()()));
}

// 生成KV_NUM对KV对
void init_test_case() {
    assert(KV_NUM > 0);
    test_case = generate(KV_NUM);
}

// 初始化
void init() {
    // 初始化 数据文件所在地
    init_path();
    // 初始化 数组值
    init_get_queue();
    // 创造出 Env
    init_config();
    // 生成一维数组
    init_test_case();
}

// 根据index取test_case中取数，追加到head后面
leveldb::Slice add_number_to_slice(std::string head, int index) {
    return leveldb::Slice(head.append(test_case[index]));
}

// 使用comparator对test_case进行检验，不然无法Add进Block
void test_test_case() {
    assert(!test_case.empty());

    for (int i = 0; i < KV_NUM - 1; i++) {
        assert(options.comparator->Compare(add_number_to_slice("key", i + 1), add_number_to_slice("key", i)) > 0);
    }
}

// 写操作
void test_block_write() {
    // 清空文件 只写文件 创造文件
    s = env->NewWritableFile(path, &file);
    check_status(s);
    // 初始化 ssTable 构造器
    leveldb::TableBuilder tableBuilder = leveldb::TableBuilder(options, file);

    // 把test_case的所有KV写入SSTable，并且达到阈值后，会进行刷盘的
    for (int i = 0; i < KV_NUM; ++i) {
        leveldb::Slice key_ = add_number_to_slice(key, i);
        leveldb::Slice value_ = add_number_to_slice(value, i);
        // 向 data 向 index 中 写入数据
        tableBuilder.Add(key_, value_);
    }

    // 构建 SSTable，写入 footer 信息
    s = tableBuilder.Finish();
    check_status(s);

    // 持久化到磁盘中，光调用Flush()不行
    // Flush()之后，数据可能会留在文件系统、SSD的内存里
    s = tableBuilder.Sync();
    check_status(s);
}

// 查询到KV对,之后的处理函数
void kv_handler(const leveldb::Slice &key, const leveldb::Slice &value) {
    // 去除前缀
    std::string key_number = key.ToString().replace(0, 3, "");
    std::string value_number = value.ToString().replace(0, 5, "");
    // 比较序号，对应的kv对的序号应该是相同的
    assert(key_number == value_number);
}

// 之所以分开两个函数，是因为读写同一个文件似乎不能写在一个函数中
void test_block_read() {
    // 打开文件 仅仅可读
    s = env->NewRandomAccessFile(path, &randomAccessFile);
    check_status(s);

    leveldb::Table *table = nullptr;

    uint64_t size;
    // 浅浅试探一下 获得文件大小
    env->GetFileSize(path, &size);
    // 读取 SSTable 中的 Restarts point 组数据到 table 内存中
    s = leveldb::Table::Open(options, randomAccessFile, size, &table);
    check_status(s);

    for (int i = 0; i < KV_NUM; i++) {
        // 从随机序列中取得一个序号，取得字符串与key合并，查询这个key，使用kv_handler检查kv对是否对应
        table->InternalGet(readOptions, add_number_to_slice("key", get_queue[i]), kv_handler);
    }
    printf("All test passed\n");
}

int main(int argc, const char *argv[]) {
    init();
    test_test_case();
    // 普通写流程
    test_block_write();
    // 普通读流程
    test_block_read();
    return 0;
}