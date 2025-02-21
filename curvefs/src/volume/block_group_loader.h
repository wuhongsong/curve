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
 * Project: curve
 * Date: Friday Mar 04 22:59:29 CST 2022
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_VOLUME_BLOCK_GROUP_LOADER_H_
#define CURVEFS_SRC_VOLUME_BLOCK_GROUP_LOADER_H_

#include <memory>

#include "curvefs/proto/space.pb.h"
#include "curvefs/src/volume/allocator.h"
#include "curvefs/src/volume/block_group_updater.h"
#include "curvefs/src/volume/common.h"
#include "curvefs/src/volume/option.h"

namespace curvefs {
namespace volume {

using ::curvefs::common::BitmapLocation;

class BlockDeviceClient;

struct AllocatorAndBitmapUpdater {
    uint64_t blockGroupOffset;
    std::unique_ptr<Allocator> allocator;
    std::unique_ptr<BlockGroupBitmapUpdater> bitmapUpdater;
};

// load bitmap for each block group
class BlockGroupBitmapLoader {
 public:
    BlockGroupBitmapLoader(BlockDeviceClient* client,
                           uint32_t blockSize,
                           uint64_t offset,
                           uint64_t blockGroupSize,
                           BitmapLocation location,
                           const AllocatorOption& option)
        : blockDev_(client),
          offset_(offset),
          blockGroupSize_(blockGroupSize),
          blockSize_(blockSize),
          bitmapLocation_(location),
          allocatorOption_(option) {}

    BlockGroupBitmapLoader(const BlockGroupBitmapLoader&) = delete;
    BlockGroupBitmapLoader& operator=(const BlockGroupBitmapLoader&) = delete;

    /**
     * @brief Create a allocator and bitmap updater that corresponding to
     *        current block group
     * @return return true if success, otherwise, return false
     */
    bool Load(AllocatorAndBitmapUpdater* out);

 private:
    BitmapRange CalcBitmapRange() const;

 private:
    BlockDeviceClient* blockDev_;
    uint64_t offset_;
    uint64_t blockGroupSize_;
    uint32_t blockSize_;
    BitmapLocation bitmapLocation_;
    const AllocatorOption& allocatorOption_;
};

}  // namespace volume
}  // namespace curvefs

#endif  // CURVEFS_SRC_VOLUME_BLOCK_GROUP_LOADER_H_
