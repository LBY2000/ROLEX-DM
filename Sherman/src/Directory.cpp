#include "Directory.h"
#include "Common.h"

#include "Connection.h"

//#include <gperftools/profiler.h>

GlobalAddress g_root_ptr = GlobalAddress::Null();
int g_root_level = -1;
bool enable_cache ;

Directory::Directory(DirectoryConnection *dCon, RemoteConnection *remoteInfo,
                     uint32_t ComputemachineNR, uint16_t dirID, uint16_t nodeID)
    : dCon(dCon), remoteInfo(remoteInfo), ComputeMachineNR(ComputemachineNR), dirID(dirID),
      nodeID(nodeID), dirTh(nullptr){ //这里的nodeID指的是serverNum-1形成的那个nodeID

  { // chunck alloctor
    GlobalAddress dsm_start;
    uint64_t per_directory_dsm_size = dCon->dsmSize / NR_DIRECTORY;  //这里按照dir_connection划分了不同的dsm_pool位置，先按照1个线程来理解
    dsm_start.nodeID = nodeID;  //这里的NodeID指的是servernum-1的结果，因此可以认为是一个和Memory_node数量相关的变量
    //这里的nodeID确实是DSM_container的ID，也就是MemoryNode的情况，同时这里的nodeID猜测是地址访问验证，后续的dsm_start.offset可能是按照线程划分操作域
    dsm_start.offset = per_directory_dsm_size * dirID;  //在这里，不太明白实际的内存管理访问
    chunckAlloc = new GlobalAllocator(dsm_start, per_directory_dsm_size);  //表示每个dir_connection的内存分配器的起始地址有dsm_start确定，同时
    //划分每个分配器可以拥有的大小
  }

  dirTh = new std::thread(&Directory::dirThread, this);  //后续dirTh主要是接收来自远端的消息，然后进行内存分配
  //后续便以异步线程的形式继续进行操作
}

Directory::~Directory() { delete chunckAlloc; }

void Directory::dirThread() {

  bindCore(23 - dirID);
  Debug::notifyInfo("dir %d launch!\n", dirID);

  while (true) {
    struct ibv_wc wc;
    pollWithCQ(dCon->cq, 1, &wc);

    switch (int(wc.opcode)) {
    case IBV_WC_RECV: // control message
    {

      auto *m = (RawMessage *)dCon->message->getMessage();  //首先从DirectoryConnection里获得messageConnection

      process_message(m);

      break;
    }
    case IBV_WC_RDMA_WRITE: {
      break;
    }
    case IBV_WC_RECV_RDMA_WITH_IMM: {

      break;
    }
    default:
      assert(false);
    }
  }
}

void Directory::process_message(const RawMessage *m) {
  RawMessage *send = nullptr;
  switch (m->type) {
  case RpcType::MALLOC: {  //如果message的内容是MALLOC，则进行内存分配

    send = (RawMessage *)dCon->message->getSendPool();

    send->addr = chunckAlloc->alloc_chunck();
    break;
  }

  case RpcType::NEW_ROOT: {  //形成新的root

    if (g_root_level < m->level) {
      g_root_ptr = m->addr;
      g_root_level = m->level;
      // can not set as 1. if set as 1 then the root node will be cached and no page search
      // will go through it, then it will trigger a bug in function insert internal.
      if (g_root_level >= 4) {
        enable_cache = true;
      }
    }

    break;
  }

  default:
    assert(false);
  }

  if (send) {
    dCon->sendMessage2App(send, m->node_id, m->app_id);  //分配完毕的结果返回给远端
    //而除了Global_Allocator以外，其他的情况都不需要回复
  }
}