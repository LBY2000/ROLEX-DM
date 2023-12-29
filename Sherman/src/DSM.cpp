
#include "DSM.h"
#include "Directory.h"
#include "HugePageAlloc.h"

#include "DSMKeeper.h"

#include <algorithm>

thread_local int DSM::thread_id = -1;
thread_local ThreadConnection *DSM::iCon = nullptr;
thread_local char *DSM::rdma_buffer = nullptr;
thread_local LocalAllocator DSM::local_allocator;
thread_local RdmaBuffer DSM::rbuf[define::kMaxCoro];  //按照Coro来划分8个RdmaBuffer
thread_local uint64_t DSM::thread_tag = 0;
//这些都是thread_local的，本地线程独有的
DSM *DSM::getInstance(const DSMConfig &conf) {
  static DSM *dsm = nullptr;
  static WRLock lock;

  lock.wLock();
  if (!dsm) {
    dsm = new DSM(conf);
  } else {
  }
  lock.wUnlock();

  return dsm;
}

DSM::DSM(const DSMConfig &conf)
    : conf(conf), appID(0), cache(conf.cacheConfig) {

//  baseAddr = (uint64_t)hugePageAlloc(conf.dsmSize * define::GB);

//  Debug::notifyInfo("shared memory size: %dGB, 0x%lx", conf.dsmSize, baseAddr);
  Debug::notifyInfo("cache size: %dGB", conf.cacheConfig.cacheSize);

  // warmup
  // memset((char *)baseAddr, 0, conf.dsmSize * define::GB);
//  for (uint64_t i = baseAddr; i < baseAddr + conf.dsmSize * define::GB;
//       i += 2 * define::MB) {
//    *(char *)i = 0;
//  }
//
//  // clear up first chunk
//  memset((char *)baseAddr, 0, define::kChunkSize);

    initRDMAConnection_Compute();

//  for (int i = 0; i < NR_DIRECTORY; ++i) {
//    dirAgent[i] =
//        new Directory(dirCon_[i], remoteInfo, conf.MemoryNodeNum, i, myNodeID);
//  }

  keeper->barrier("DSM-init");  //这段代码的含义是，编号为0的CN写入barrierDSM-init:0到memcached服务器，并递增，其后每个加入集群的CN都会递增它
  //直到当前加入集群的节点到达MaxCN的数量，才会进入后续活动，这个是一个利用memcached服务器来进行同步的实例
}

DSM::~DSM() {}

void DSM::registerThread(){

  if (thread_id != -1)
    return;

  thread_id = appID.fetch_add(1); //fetch_add返回操作之前的值，因此thread_id从0开始递增
  thread_tag = thread_id + (((uint64_t)this->getMyNodeID()) << 32) + 1;//看起来thread_tag是高位为mynodeID。然后低位为thread_id+1
  //thread_tag的低位从1开始递增

  iCon = thCon[thread_id];

  iCon->message->initRecv();
  iCon->message->initSend();
  rdma_buffer = (char *)cache.data + thread_id * 12 * define::MB;//按照12MB为每个thread分配cachepool的rdma_buffer
  //cache_pool的钱thread_num个12MB的空间被用作各自thread的buffer区域，同时后续的set_buffer似乎还按照coro来进一步划分每个线程的每个执行的coro的buffer
  //进一步来看，这里为每个线程，从cache_pool的起始地址划分了12MB的块，且每个12MB的块作为一个偏移起始地址，后续紧跟的1MB空间被按照128KB，划分给8个Coro
  //每个核心占有128KB的buffer，后续进行进一步划分
  for (int i = 0; i < define::kMaxCoro; ++i) {
    rbuf[i].set_buffer(rdma_buffer + i * define::kPerCoroRdmaBuf); 
    //PerCoroRdmaBuf大小是128K，8*128k=1M
  }
  //因此，这里每个thread都将从cache_pool获得一个12MB大小的空间，并且，每个线程还会为每个coro来进行buffer区域的划分，每个coro的独有buffer大小为128KB
}

void DSM::initRDMAConnection_Compute() {

  Debug::notifyInfo("Machine NR: %d", conf.MemoryNodeNum);

  remoteInfo = new RemoteConnection[conf.MemoryNodeNum];  //同理这里的machineNR也变成了针对MemoryNodeNum数量的
    // every node has MAX_APP_THREAD of local thread, and each server has NR_DIRECTORY of memory regions,
    // Besides there are conf.MemoryNodeNum of nodes in the cluster
  for (int i = 0; i < MAX_APP_THREAD; ++i) {
      // the code below just create the queue pair as compute node. also initialize the
      // local cache memory region on this machine
    thCon[i] =
        new ThreadConnection(i, (void *)cache.data, cache.size * define::GB,
                             conf.MemoryNodeNum, remoteInfo);//与DSMConatiner.cpp中的内容对应，这里创建的是针对每个MN endpoint的qp
  }

//  for (int i = 0; i < NR_DIRECTORY; ++i) {
//      // the connection below just create the queue pair for DSM. also initialize the DSM memory region
//      // in this machine.
//    dirCon_[i] =
//        new DirectoryConnection(i, (void *)baseAddr, conf.dsmSize * define::GB,
//                                conf.MemoryNodeNum, remoteInfo);
//  }
  // THe DSM keeper will set up the queue pair connection
  keeper = new DSMKeeper(thCon, dirCon, remoteInfo, conf.MemoryNodeNum, conf.ComputeNodeNum);
  keeper->initialization();
  myNodeID = keeper->getMyNodeID();  //这里的myNodeID就是指的是CN的节点号，以serverenter过程中的server_num-1来看的
  //而这里的server_num又是指的是CN的数量，每次有新的CN加入集群的时候，都会递增一次memcached服务器上的该数字，同时得到递增的mynodeID号
}






//后续的DSM内容为基于DSM封装的connection进行rdma读写













void DSM::read(char *buffer, GlobalAddress gaddr, size_t size, bool signal,
               CoroContext *ctx) {
  if (ctx == nullptr) {
    rdmaRead(iCon->data[0][gaddr.nodeID], (uint64_t)buffer,
             remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset, size,
             iCon->cacheLKey, remoteInfo[gaddr.nodeID].dsmRKey[0], signal);
  } else {
    rdmaRead(iCon->data[0][gaddr.nodeID], (uint64_t)buffer,
             remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset, size,
             iCon->cacheLKey, remoteInfo[gaddr.nodeID].dsmRKey[0], true,
             ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}

void DSM::read_sync(char *buffer, GlobalAddress gaddr, size_t size,
                    CoroContext *ctx) {
  read(buffer, gaddr, size, true, ctx);

  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }
}

void DSM::write(const char *buffer, GlobalAddress gaddr, size_t size,
                bool signal, CoroContext *ctx) {
//    printf("node id is %d\n",gaddr.nodeID);
  if (ctx == nullptr) {
    rdmaWrite(iCon->data[0][gaddr.nodeID], (uint64_t)buffer,
              remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset, size,
              iCon->cacheLKey, remoteInfo[gaddr.nodeID].dsmRKey[0], -1, signal);
  } else {
    rdmaWrite(iCon->data[0][gaddr.nodeID], (uint64_t)buffer,
              remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset, size,
              iCon->cacheLKey, remoteInfo[gaddr.nodeID].dsmRKey[0], -1, true,
              ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}

void DSM::write_sync(const char *buffer, GlobalAddress gaddr, size_t size,
                     CoroContext *ctx) {
  write(buffer, gaddr, size, true, ctx);

  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }
}

void DSM::fill_keys_dest(RdmaOpRegion &ror, GlobalAddress gaddr, bool is_chip) {
  ror.lkey = iCon->cacheLKey;
  if (is_chip) {
    ror.dest = remoteInfo[gaddr.nodeID].lockBase + gaddr.offset;
    ror.remoteRKey = remoteInfo[gaddr.nodeID].lockRKey[0];
  } else {
    ror.dest = remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset;
    ror.remoteRKey = remoteInfo[gaddr.nodeID].dsmRKey[0];
  }
}

void DSM::write_batch(RdmaOpRegion *rs, int k, bool signal, CoroContext *ctx) {

  int node_id = -1;
  for (int i = 0; i < k; ++i) {

    GlobalAddress gaddr;
    gaddr.val = rs[i].dest;
    node_id = gaddr.nodeID;
    fill_keys_dest(rs[i], gaddr, rs[i].is_lock_mr);
  }

  if (ctx == nullptr) {
    rdmaWriteBatch(iCon->data[0][node_id], rs, k, signal);
  } else {
    rdmaWriteBatch(iCon->data[0][node_id], rs, k, true, ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}

void DSM::write_batch_sync(RdmaOpRegion *rs, int k, CoroContext *ctx) {
  write_batch(rs, k, true, ctx);

  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }
}

void DSM::write_faa(RdmaOpRegion &write_ror, RdmaOpRegion &faa_ror,
                    uint64_t add_val, bool signal, CoroContext *ctx) {
  int node_id;
  {
    GlobalAddress gaddr;
    gaddr.val = write_ror.dest;
    node_id = gaddr.nodeID;

    fill_keys_dest(write_ror, gaddr, write_ror.is_lock_mr);
  }
  {
    GlobalAddress gaddr;
    gaddr.val = faa_ror.dest;

    fill_keys_dest(faa_ror, gaddr, faa_ror.is_lock_mr);
  }
  if (ctx == nullptr) {
    rdmaWriteFaa(iCon->data[0][node_id], write_ror, faa_ror, add_val, signal);
  } else {
    rdmaWriteFaa(iCon->data[0][node_id], write_ror, faa_ror, add_val, true,
                 ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}
void DSM::write_faa_sync(RdmaOpRegion &write_ror, RdmaOpRegion &faa_ror,
                         uint64_t add_val, CoroContext *ctx) {
  write_faa(write_ror, faa_ror, add_val, true, ctx);
  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }
}

void DSM::write_cas(RdmaOpRegion &write_ror, RdmaOpRegion &cas_ror,
                    uint64_t equal, uint64_t val, bool signal,
                    CoroContext *ctx) {
  int node_id;
  {
    GlobalAddress gaddr;
    gaddr.val = write_ror.dest;
    node_id = gaddr.nodeID;

    fill_keys_dest(write_ror, gaddr, write_ror.is_lock_mr);
  }
  {
    GlobalAddress gaddr;
    gaddr.val = cas_ror.dest;

    fill_keys_dest(cas_ror, gaddr, cas_ror.is_lock_mr);
  }
  if (ctx == nullptr) {
    rdmaWriteCas(iCon->data[0][node_id], write_ror, cas_ror, equal, val,
                 signal);
  } else {
    rdmaWriteCas(iCon->data[0][node_id], write_ror, cas_ror, equal, val, true,
                 ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}
void DSM::write_cas_sync(RdmaOpRegion &write_ror, RdmaOpRegion &cas_ror,
                         uint64_t equal, uint64_t val, CoroContext *ctx) {
  write_cas(write_ror, cas_ror, equal, val, true, ctx);
  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }
}

void DSM::cas_read(RdmaOpRegion &cas_ror, RdmaOpRegion &read_ror,
                   uint64_t equal, uint64_t val, bool signal,
                   CoroContext *ctx) {

  int node_id;
  {
    GlobalAddress gaddr;
    gaddr.val = cas_ror.dest;
    node_id = gaddr.nodeID;
    fill_keys_dest(cas_ror, gaddr, cas_ror.is_lock_mr);
  }
  {
    GlobalAddress gaddr;
    gaddr.val = read_ror.dest;
    fill_keys_dest(read_ror, gaddr, read_ror.is_lock_mr);
  }

  if (ctx == nullptr) {
    rdmaCasRead(iCon->data[0][node_id], cas_ror, read_ror, equal, val, signal);
  } else {
    rdmaCasRead(iCon->data[0][node_id], cas_ror, read_ror, equal, val, true,
                ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}

bool DSM::cas_read_sync(RdmaOpRegion &cas_ror, RdmaOpRegion &read_ror,
                        uint64_t equal, uint64_t val, CoroContext *ctx) {
  cas_read(cas_ror, read_ror, equal, val, true, ctx);

  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }

  return equal == *(uint64_t *)cas_ror.source;
}

void DSM::cas(GlobalAddress gaddr, uint64_t equal, uint64_t val,
              uint64_t *rdma_buffer, bool signal, CoroContext *ctx) {

  if (ctx == nullptr) {
    rdmaCompareAndSwap(iCon->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
                       remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset, equal,
                       val, iCon->cacheLKey,
                       remoteInfo[gaddr.nodeID].dsmRKey[0], signal);
  } else {
    rdmaCompareAndSwap(iCon->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
                       remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset, equal,
                       val, iCon->cacheLKey,
                       remoteInfo[gaddr.nodeID].dsmRKey[0], true, ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}

bool DSM::cas_sync(GlobalAddress gaddr, uint64_t equal, uint64_t val,
                   uint64_t *rdma_buffer, CoroContext *ctx) {   
  cas(gaddr, equal, val, rdma_buffer, true, ctx);

  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }

  return equal == *rdma_buffer;
}

//void DSM::cas_mask(GlobalAddress gaddr, uint64_t equal, uint64_t val,
//                   uint64_t *rdma_buffer, uint64_t mask, bool signal) {
//  rdmaCompareAndSwapMask(iCon->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
//                         remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset, equal,
//                         val, iCon->cacheLKey,
//                         remoteInfo[gaddr.nodeID].dsmRKey[0], mask, signal);
//}

//bool DSM::cas_mask_sync(GlobalAddress gaddr, uint64_t equal, uint64_t val,
//                        uint64_t *rdma_buffer, uint64_t mask) {
//  cas_mask(gaddr, equal, val, rdma_buffer, mask);
//  ibv_wc wc;
//  pollWithCQ(iCon->cq, 1, &wc);
//
//  return (equal & mask) == (*rdma_buffer & mask);
//}

//void DSM::faa_boundary(GlobalAddress gaddr, uint64_t add_val,
//                       uint64_t *rdma_buffer, uint64_t mask, bool signal,
//                       CoroContext *ctx) {
//  if (ctx == nullptr) {
//    rdmaFetchAndAddBoundary(iCon->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
//                            remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset,
//                            add_val, iCon->cacheLKey,
//                            remoteInfo[gaddr.nodeID].dsmRKey[0], mask, signal);
//  } else {
//    rdmaFetchAndAddBoundary(iCon->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
//                            remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset,
//                            add_val, iCon->cacheLKey,
//                            remoteInfo[gaddr.nodeID].dsmRKey[0], mask, true,
//                            ctx->coro_id);
//    (*ctx->yield)(*ctx->master);
//  }
//}

//void DSM::faa_boundary_sync(GlobalAddress gaddr, uint64_t add_val,
//                            uint64_t *rdma_buffer, uint64_t mask,
//                            CoroContext *ctx) {
//  faa_boundary(gaddr, add_val, rdma_buffer, mask, true, ctx);
//  if (ctx == nullptr) {
//    ibv_wc wc;
//    pollWithCQ(iCon->cq, 1, &wc);
//  }
//}

void DSM::read_dm(char *buffer, GlobalAddress gaddr, size_t size, bool signal,
                  CoroContext *ctx) {

  if (ctx == nullptr) {
    rdmaRead(iCon->data[0][gaddr.nodeID], (uint64_t)buffer,
             remoteInfo[gaddr.nodeID].lockBase + gaddr.offset, size,
             iCon->cacheLKey, remoteInfo[gaddr.nodeID].lockRKey[0], signal);
  } else {
    rdmaRead(iCon->data[0][gaddr.nodeID], (uint64_t)buffer,
             remoteInfo[gaddr.nodeID].lockBase + gaddr.offset, size,
             iCon->cacheLKey, remoteInfo[gaddr.nodeID].lockRKey[0], true,
             ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}

void DSM::read_dm_sync(char *buffer, GlobalAddress gaddr, size_t size,
                       CoroContext *ctx) {
  read_dm(buffer, gaddr, size, true, ctx);

  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }
}

void DSM::write_dm(const char *buffer, GlobalAddress gaddr, size_t size,
                   bool signal, CoroContext *ctx) {
  if (ctx == nullptr) {
    rdmaWrite(iCon->data[0][gaddr.nodeID], (uint64_t)buffer,
              remoteInfo[gaddr.nodeID].lockBase + gaddr.offset, size,
              iCon->cacheLKey, remoteInfo[gaddr.nodeID].lockRKey[0], -1,
              signal);
  } else {
    rdmaWrite(iCon->data[0][gaddr.nodeID], (uint64_t)buffer,
              remoteInfo[gaddr.nodeID].lockBase + gaddr.offset, size,
              iCon->cacheLKey, remoteInfo[gaddr.nodeID].lockRKey[0], -1, true,
              ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}

void DSM::write_dm_sync(const char *buffer, GlobalAddress gaddr, size_t size,
                        CoroContext *ctx) {
  write_dm(buffer, gaddr, size, true, ctx);

  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }
}

void DSM::cas_dm(GlobalAddress gaddr, uint64_t equal, uint64_t val,
                 uint64_t *rdma_buffer, bool signal, CoroContext *ctx) {

  if (ctx == nullptr) {
    rdmaCompareAndSwap(iCon->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
                       remoteInfo[gaddr.nodeID].lockBase + gaddr.offset, equal,
                       val, iCon->cacheLKey,
                       remoteInfo[gaddr.nodeID].lockRKey[0], signal);
  } else {
    rdmaCompareAndSwap(iCon->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
                       remoteInfo[gaddr.nodeID].lockBase + gaddr.offset, equal,
                       val, iCon->cacheLKey,
                       remoteInfo[gaddr.nodeID].lockRKey[0], true,
                       ctx->coro_id);
    (*ctx->yield)(*ctx->master);
  }
}

bool DSM::cas_dm_sync(GlobalAddress gaddr, uint64_t equal, uint64_t val,
                      uint64_t *rdma_buffer, CoroContext *ctx) {
  cas_dm(gaddr, equal, val, rdma_buffer, true, ctx);

  if (ctx == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }

  return equal == *rdma_buffer;
}

//void DSM::cas_dm_mask(GlobalAddress gaddr, uint64_t equal, uint64_t val,
//                      uint64_t *rdma_buffer, uint64_t mask, bool signal) {
//  rdmaCompareAndSwapMask(iCon->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
//                         remoteInfo[gaddr.nodeID].lockBase + gaddr.offset,
//                         equal, val, iCon->cacheLKey,
//                         remoteInfo[gaddr.nodeID].lockRKey[0], mask, signal);
//}

//bool DSM::cas_dm_mask_sync(GlobalAddress gaddr, uint64_t equal, uint64_t val,
//                           uint64_t *rdma_buffer, uint64_t mask) {
//  cas_dm_mask(gaddr, equal, val, rdma_buffer, mask);
//  ibv_wc wc;
//  pollWithCQ(iCon->cq, 1, &wc);
//
//  return (equal & mask) == (*rdma_buffer & mask);
//}
//
//void DSM::faa_dm_boundary(GlobalAddress gaddr, uint64_t add_val,
//                          uint64_t *rdma_buffer, uint64_t mask, bool signal,
//                          CoroContext *ctx) {
//  if (ctx == nullptr) {
//
//    rdmaFetchAndAddBoundary(iCon->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
//                            remoteInfo[gaddr.nodeID].lockBase + gaddr.offset,
//                            add_val, iCon->cacheLKey,
//                            remoteInfo[gaddr.nodeID].lockRKey[0], mask, signal);
//  } else {
//    rdmaFetchAndAddBoundary(iCon->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
//                            remoteInfo[gaddr.nodeID].lockBase + gaddr.offset,
//                            add_val, iCon->cacheLKey,
//                            remoteInfo[gaddr.nodeID].lockRKey[0], mask, true,
//                            ctx->coro_id);
//    (*ctx->yield)(*ctx->master);
//  }
//}
//
//void DSM::faa_dm_boundary_sync(GlobalAddress gaddr, uint64_t add_val,
//                               uint64_t *rdma_buffer, uint64_t mask,
//                               CoroContext *ctx) {
//  faa_dm_boundary(gaddr, add_val, rdma_buffer, mask, true, ctx);
//  if (ctx == nullptr) {
//    ibv_wc wc;
//    pollWithCQ(iCon->cq, 1, &wc);
//  }
//}
//
uint64_t DSM::poll_rdma_cq(int count) {
  ibv_wc wc;
  pollWithCQ(iCon->cq, count, &wc);

  return wc.wr_id;
}

bool DSM::poll_rdma_cq_once(uint64_t &wr_id) {
  ibv_wc wc;
  int res = pollOnce(iCon->cq, 1, &wc);

  wr_id = wc.wr_id;

  return res == 1;
}