#include "DirectoryConnection.h"

#include "Connection.h"

DirectoryConnection::DirectoryConnection(uint16_t dirID, void *dsmPool,
                                         uint64_t dsmSize, uint32_t machineNR,
                                         RemoteConnection *remoteInfo)
    : dirID(dirID), remoteInfo(remoteInfo) {

  createContext(&ctx);
  cq = ibv_create_cq(ctx.ctx, RAW_RECV_CQ_COUNT, NULL, NULL, 0);
  // RawMessage connection is a unreliable connection.
  message = new RawMessageConnection(ctx, cq, DIR_MESSAGE_NR); //DirTH一端是128的大小容量，可能和自身要承接更多消息数据有关

  message->initRecv();
  message->initSend();

  // dsm memory
  this->dsmPool = dsmPool;
  this->dsmSize = dsmSize;
  this->dsmMR = createMemoryRegion((uint64_t)dsmPool, dsmSize, &ctx);
  this->dsmLKey = dsmMR->lkey;

  // on-chip lock memory
  if (dirID == 0) {
//    this->lockPool = (void *)define::kLockStartAddr;
      this->lockPool = (void *)hugePageAlloc(define::kLockChipMemSize);
    this->lockSize = define::kLockChipMemSize;
      memset(lockPool, 0,     this->lockSize = define::kLockChipMemSize);
//    this->lockMR = createMemoryRegionOnChip((uint64_t)this->lockPool,
//                                            this->lockSize, &ctx);
    this->lockMR = createMemoryRegion((uint64_t)this->lockPool,
                                              this->lockSize, &ctx);  
    this->lockLKey = lockMR->lkey;
  }

  // app, RC
  for (int i = 0; i < MAX_APP_THREAD; ++i) {
    data2app[i] = new ibv_qp *[machineNR];   //单一dirTH构建了到每个computeNode的qp连接或者是是针对每个CN end_point的qp
    for (size_t k = 0; k < machineNR; ++k) {  //这里的machineNR数量是CN的数量
      createQueuePair(&data2app[i][k], IBV_QPT_RC, cq, &ctx);
    }
  }
}

void DirectoryConnection::sendMessage2App(RawMessage *m, uint16_t node_id,
                                          uint16_t th_id) {
  message->sendRawMessage(m, remoteInfo[node_id].appMessageQPN[th_id],
                          remoteInfo[node_id].dirToAppAh[dirID][th_id]);  //这里的node_id确认是MN端的node_id
  ;
}
