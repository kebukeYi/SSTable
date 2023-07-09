// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "comparator.h"

#include <algorithm>
#include <cstdint>
#include <string>
#include <type_traits>

#include "slice.h"
#include "no_destructor.h"

namespace leveldb {

    Comparator::~Comparator() = default;

    namespace {
        //
        class BytewiseComparatorImpl : public Comparator {
        public:
            BytewiseComparatorImpl() = default;

            const char *Name() const override { return "leveldb.BytewiseComparator"; }

            int Compare(const Slice &a, const Slice &b) const override {
                return a.compare(b);
            }

            // Find length of common prefix
            // 比如说 zz zzb + zz c = zz d
            void FindShortestSeparator(std::string *start, const Slice &limit) const override {
                size_t min_length = std::min(start->size(), limit.size());
                size_t diff_index = 0;
                //  abddddd  abcccccc  具有共同前缀 diff_index = 2
                //  ksdfd    ddrer     没有共同前缀 diff_index = 0
                //  abckkkk  abck      具有完整共同前缀 diff_index =
                // 找到两者 共同前缀
                while ((diff_index < min_length) && ((*start)[diff_index] == limit[diff_index])) {
                    diff_index++;
                }
                // 具有完整共同前缀
                if (diff_index >= min_length) {
                    // Do not shorten if one string is a prefix of the other
                    // 如果一个字符串是另一个字符串的前缀，则不要缩短
                } else {
                    // diff_index < min_length
                    // start_byte
                    uint8_t diff_byte = static_cast<uint8_t>((*start)[diff_index]);

                    // st   li      st
                    // abca abcb => abca
                    // abca abcc => abcb
                    // abca abc  => abca
                    // abc  abca => abc
                    // start_byte < 1111 1111
                    if (diff_byte < static_cast<uint8_t>(0xff) &&
                        // start_byte + 1 < limit_byte
                        (diff_byte + 1 < static_cast<uint8_t>(limit[diff_index]))) {
                        // 元素值 +1
                        (*start)[diff_index]++;
                        start->resize(diff_index + 1);
                        assert(Compare(*start, limit) < 0);
                    }
                }
            }

            void FindShortSuccessor(std::string *key) const override {
                // Find first character that can be incremented
                size_t n = key->size();
                for (size_t i = 0; i < n; i++) {
                    const uint8_t byte = (*key)[i];
                    if (byte != static_cast<uint8_t>(0xff)) {
                        (*key)[i] = byte + 1;
                        key->resize(i + 1);
                        return;
                    }
                }
                // *key is a run of 0xffs.  Leave it alone.
            }
        };
    }  // namespace

    const Comparator *BytewiseComparator() {
        // 静态局部变量在函数内部声明，但在函数调用结束后仍然保持存在，并且在下一次调用该函数时保持其值不变。
        static NoDestructor<BytewiseComparatorImpl> singleton;
        return singleton.get();
    }

}  // namespace leveldb
