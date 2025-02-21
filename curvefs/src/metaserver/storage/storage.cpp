/*
 *  Copyright (c) 2022 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: Curve
 * Date: 2022-02-14
 * Author: Jingli Chen (Wine93)
 */

#include <glog/logging.h>

#include <memory>

#include "curvefs/src/metaserver/storage/storage.h"
#include "curvefs/src/metaserver/storage/memory_storage.h"
#include "curvefs/src/metaserver/storage/rocksdb_storage.h"

namespace curvefs {
namespace metaserver {
namespace storage {

using ::curvefs::metaserver::storage::KVStorage;
using ::curvefs::metaserver::storage::MemoryStorage;
using ::curvefs::metaserver::storage::RocksDBStorage;

static std::shared_ptr<KVStorage> kKVStorage;

bool InitStorage(StorageOptions options) {
    if (options.type == "memory") {
        kKVStorage = std::make_shared<MemoryStorage>(options);
        LOG(INFO) << "using memory storage";
    } else if (options.type == "rocksdb") {
        kKVStorage = std::make_shared<RocksDBStorage>(options);
        LOG(INFO) << "using rocksdb storage";
    } else {
        LOG(ERROR) << "unsupport storage type: " << options.type;
        return false;
    }
    return kKVStorage->Open();
}

std::shared_ptr<KVStorage> GetStorageInstance() {
    return kKVStorage;
}

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs
