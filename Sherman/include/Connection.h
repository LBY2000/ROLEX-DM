#ifndef __CONNECTION_H__
#define __CONNECTION_H__

#include "Common.h"
#include "RawMessageConnection.h"

#include "ThreadConnection.h"
#include "DirectoryConnection.h"

struct RemoteConnection {
    // directory
    uint64_t dsmBase;

    uint32_t dsmRKey[NR_DIRECTORY];
    uint32_t dirMessageQPN[NR_DIRECTORY]; //因为每个dirConnection都有一个RAWConnection，其中又只有一个QP
    ibv_ah *appToDirAh[MAX_APP_THREAD][NR_DIRECTORY];

    // cache
    uint64_t cacheBase;

    // lock memory
    uint64_t lockBase;
    uint32_t lockRKey[NR_DIRECTORY];

    // app thread
    uint32_t appRKey[MAX_APP_THREAD];
    uint32_t appMessageQPN[MAX_APP_THREAD];  //CN端的Message的QP的QPN
    ibv_ah *dirToAppAh[NR_DIRECTORY][MAX_APP_THREAD];   //同样是用于节点级别寻址的ibv_ah
};

#endif /* __CONNECTION_H__ */
