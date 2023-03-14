/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Created Date: 21-5-31
 * Author: huyao
 */

#ifndef CURVEFS_SRC_CLIENT_S3_CLIENT_S3_ADAPTOR_H_
#define CURVEFS_SRC_CLIENT_S3_CLIENT_S3_ADAPTOR_H_

#include <bthread/execution_queue.h>

#include <memory>
#include <string>
#include <vector>

#include "curvefs/proto/common.pb.h"
#include "curvefs/proto/mds.pb.h"
#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/client/common/common.h"
#include "curvefs/src/client/common/config.h"
#include "curvefs/src/client/client_storage_adaptor.h"
#include "curvefs/src/client/kvclient/kvclient_manager.h"
#include "curvefs/src/client/kvclient/kvclient.h"
#include "curvefs/src/client/error_code.h"
#include "curvefs/src/client/inode_wrapper.h"
#include "curvefs/src/client/inode_cache_manager.h"
#include "curvefs/src/client/rpcclient/mds_client.h"
#include "curvefs/src/client/s3/client_s3.h"
#include "curvefs/src/client/s3/client_s3_cache_manager.h"
#include "curvefs/src/client/s3/disk_cache_manager_impl.h"
#include "src/common/wait_interval.h"

namespace curvefs {
namespace client {


using curve::common::GetObjectAsyncCallBack;
using curve::common::PutObjectAsyncCallBack;
using curve::common::S3Adapter;
using curvefs::client::common::S3ClientAdaptorOption;
using curvefs::client::metric::S3Metric;


// client use s3 internal interface
class S3ClientAdaptorImpl : public StorageAdaptor {
 public:
    S3ClientAdaptorImpl() : StorageAdaptor() {}
    virtual ~S3ClientAdaptorImpl() {
        LOG(INFO) << "delete S3ClientAdaptorImpl";
    }
    /**
     * @brief Initailize s3 client
     * @param[in] options the options for s3 client
     */
 //   CURVEFS_ERROR Init(const FuseClientOption &option) override;

    CURVEFS_ERROR
    Init(const FuseClientOption &option,
         std::shared_ptr<InodeCacheManager> inodeManager,
         std::shared_ptr<MdsClient> mdsClient,
         std::shared_ptr<FsCacheManager> fsCacheManager,
         std::shared_ptr<DiskCacheManagerImpl> diskCacheManagerImpl,
         std::shared_ptr<KVClientManager> kvClientManager,
         bool startBackGround,
         std::shared_ptr<FsInfo> fsInfo) override;

    int Stop() override;

    CURVEFS_ERROR FlushDataCache(const UperFlushRequest& req, uint64_t* writeOffset);

    CURVEFS_ERROR ReadFromLowlevel(UperReadRequest request) override;  // whs need todo

    CURVEFS_ERROR Truncate(InodeWrapper *inodeWrapper, uint64_t size);

    std::shared_ptr<S3Client> GetS3Client() { return client_; }
/*
    std::shared_ptr<UnderStorage> GetUnderStorage() {
        return s3Storage_;
    }
*/

    CURVEFS_ERROR FuseOpInit(void *userdata, struct fuse_conn_info *conn) override
    {
        StorageAdaptor::FuseOpInit(userdata, conn);
      // whs : need to do dosomething
        return CURVEFS_ERROR::OK;
    }

 private:
   int ReadKVRequest(const std::vector<S3ReadRequest> &kvRequests,
      char *dataBuf, uint64_t fileLen);
   CURVEFS_ERROR PrepareFlushTasks(const UperFlushRequest& req,
     std::vector<std::shared_ptr<PutObjectAsyncContext>> *s3Tasks,
     std::vector<std::shared_ptr<SetKVCacheTask>> *kvCacheTasks,
     uint64_t* writeOffset);
   void FlushTaskExecute(CachePoily cachePoily,
    const std::vector<std::shared_ptr<PutObjectAsyncContext>> &s3Tasks,
    const std::vector<std::shared_ptr<SetKVCacheTask>> &kvCacheTasks);

   void HandleReadRequest(
    const ReadRequest &request, const S3ChunkInfo &s3ChunkInfo,
    std::vector<ReadRequest> *addReadRequests,
    std::vector<uint64_t> *deletingReq, std::vector<S3ReadRequest> *requests,
    char *dataBuf, uint64_t fsId, uint64_t inodeId);

   void GenerateS3Request(ReadRequest request,
    const S3ChunkInfoList &s3ChunkInfoList,
    char *dataBuf,
    std::vector<S3ReadRequest> *requests,
    uint64_t fsId,
    uint64_t inodeId);

   int GenerateKVReuqest(
    const std::shared_ptr<InodeWrapper> &inodeWrapper,
    const std::vector<ReadRequest> &readRequest, char *dataBuf,
    std::vector<S3ReadRequest> *kvRequest);
   int HandleReadS3NotExist(int ret, uint32_t retry,
    const std::shared_ptr<InodeWrapper> &inodeWrapper);
   bool ReadKVRequestFromS3(const std::string &name,
    char *databuf, uint64_t offset,uint64_t length, int *ret);
   bool ReadKVRequestFromRemoteCache(const std::string &name,
    char *databuf,uint64_t offset,uint64_t length);
   bool ReadKVRequestFromLocalCache(const std::string &name,char *databuf,
     uint64_t offset,uint64_t len);

   void PrefetchForBlock(const S3ReadRequest &req, uint64_t fileLen,
     uint64_t blockSize,uint64_t chunkSize,uint64_t startBlockIndex);

   void GetChunkLoc(uint64_t offset, uint64_t *index,
     uint64_t *chunkPos, uint64_t *chunkSize);
   void GetBlockLoc(uint64_t offset, uint64_t *chunkIndex,uint64_t *chunkPos,
     uint64_t *blockIndex,uint64_t *blockPos);

 public:
    std::shared_ptr<S3Metric> s3Metric_;


 private:
    std::shared_ptr<S3Client> client_;
  //  std::shared_ptr<UnderStorage> s3Storage_;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_S3_CLIENT_S3_ADAPTOR_H_
