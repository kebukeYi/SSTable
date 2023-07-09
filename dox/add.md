--------------------------------------------------------------
# 写流程
第一次
key1 value1
last_key  = key1
last_key_ = "" 
r->data_block.Add(key, value);// 数据块中写入数据
last_key_ = "" // 首次 是 空的
shared = 0
non_shared = key1.size()
buffer_ = shared、non_shared、value1.size()、non_shared_key1_data、value1.data()
          "\000\004\006 keya valuea" 13 字节
buffers = 13 + restarts_.size()*4 + 4 = 21
last_key_ = key1

第二次
key2 value2
last_key  = key2
last_key_ = key1
shared = key = 3
non_shared = key1 - key = 1
buffer_ = buffer_ + shared、non_shared、value2.size()、non_shared_key2_data、value2.data()
          "\000\004\006 keya valuea \003\001\006 b valueb"
buffers = 23 + restarts_.size()*4 + 4 = 31
last_key_ = key2 last_key = key2

触发 block.Flush();
table_builder.Flush();
    table_builder.WriteBlock(&r->data_block, &r->pending_handle);
        BlockBuilder.Finish();
        buffer_ = buffer_+ shared、non_shared、value2.size()、non_shared_key2_data、value2.data()+
        restarts_[i] + restarts_.size();
        "\000\004\006keyavaluea\003\001\006bvalueb\000\000\000\000\001\000\000"  31B
        31 + 1 type + 4 crc =  offset = 36 B
    WriteRawBlock(block_contents, type, handle);
        r->file->Append(block_contents);  WriteUnbuffered(buf_, pos_); ::write(fd_, data, size);
        r->file->Append(Slice(trailer, kBlockTrailerSize));
        r->offset += block_contents.size() + kBlockTrailerSize; // 更新全局 offset  
        BlockBuilder::Reset(); buffer_.clear(); restarts_.push_back(0);  last_key_.clear();
    r->pending_index_entry = true;// 打开索引开关？
    r->file->Flush(); ::write(fd_, data, size);// 再次调用系统刷盘

清空一切
第三次 要进索引 
key3 value3
pending_index_entry = true
r->last_key = key2  key = key3  => last_key = key2
pending_handle : offset_=0 size_=31 // 上一次 block data 的刷盘位置信息
handle_encoding: offset_=0 size_=31 没有加5字节
r->index_block.Add(r->last_key, value3:Slice(handle_encoding));
    last_key_ = ""
    buffer_ = shared、non_shared、handle_encoding.size()、non_shared_key2_data、handle_encoding.data()
                "\000\004\002 keyb\000\037"
pending_index_entry = false; // 索引信息 存放完毕

last_key  = key3
last_key_ = ""
shared = key = 3
non_shared = key1 - key = 1
last_key = key3
    buffer_ = shared、non_shared、value3.size()、non_shared_key3_data、value3.data()
            "\000\004\006keycvaluec" = 13 B
buffers = 13 + restarts_.size()*4 + 4 = 21 B < 30 B
last_key_ = key3 last_key = key3

第四次
key4 value4
last_key  = key4
last_key_ = key3
shared = key = 3
non_shared = key1 - key = 1
buffer_ = buffer_3 + shared、non_shared、value4.size()、non_shared_key4_data、value4.data()
         "\000\004\006keycvaluec \003\001\006dvalued" = 23 B 
buffers = 23 + restarts_.size()*4 + 4 = 31 B > 30B
buffers + restart 
"\000\004\006keycvaluec\003\001\006dvalued\000\000\000\000\001\000\000"

handle->set_offset(r->offset); 36
handle->set_size(block_contents.size()); 31
r->file->Append(block_contents);
r->offset += block_contents.size() + kBlockTrailerSize; 36+31+5= 72B
block->Reset();
r->pending_index_entry = true;

last_key_ = key4  last_key = key4

清空一切
第五次 要进索引
key5 value5
pending_index_entry = true
r->last_key = key4  key = key4  => last_key = key4
pending_handle : offset_=0 size_=36 // 上一次 block data 的刷盘位置信息
handle_encoding: offset_=0 size_=31 
r->index_block.Add(r->last_key, value3:Slice(handle_encoding));
last_key_ = "key2"
buffer_ = shared、non_shared、handle_encoding.size()、non_shared_key2_data
"\000\004\002keyb\000\037\003\001\002d$\037"
last_key_ = "key4"
pending_index_entry = false; // 索引信息 存放完毕

r->last_key = key5
buffer_ = shared、non_shared、handle_encoding.size()、non_shared_key2_data()、restart.size()
"\000\004\006keyevaluee" = 13B



last_key  = key3
last_key_ = ""
shared = key = 3
non_shared = key1 - key = 1
last_key = key3
buffer_ = shared、non_shared、value3.size()、non_shared_key3_data、value3.data()
"\000\004\006keycvaluec" = 13 B
buffers = 13 + restarts_.size()*4 + 4 = 21 B < 30 B
last_key_ = key3 last_key = key3



--------------------------------------------------------------
# 读流程




































