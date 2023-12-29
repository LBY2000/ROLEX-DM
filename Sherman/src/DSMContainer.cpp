//
// Created by ruihong on 5/21/22.
//
#include "Connection.h"
#include "DSMContainer.h"
const char *DSMContainer::OK = "OK";
const char *DSMContainer::ServerPrefix = "SPre";

//void DSMContainer::initRDMAConnection_Memory() {
//
//    Debug::notifyInfo("Machine NR: %d", conf.MemoryNodeNum);
//
//    remoteCon = new RemoteConnection[conf.MemoryNodeNum];
//    // every node has MAX_APP_THREAD of local thread, and each server has NR_DIRECTORY of memory regions,
//    // Besides there are conf.MemoryNodeNum of nodes in the cluster
//
//    for (int i = 0; i < NR_DIRECTORY; ++i) {
//        // the connection below just create the queue pair for DSM. also initialize the DSM memory region
//        // in this machine.
//        dirCon_[i] =
//                new DirectoryConnection(i, (void *)baseAddr, conf.dsmSize * define::GB,
//                                        conf.MemoryNodeNum, remoteInfo);
//    }
//    // THe DSM keeper will set up the queue pair connection
////    keeper = new DSMKeeper(thCon_, dirCon_, remoteInfo, conf.MemoryNodeNum);
//
////    myNodeID = keeper->getMyNodeID();
//}
//TODO: seperate into initLocalMeta_memroy and initLocalMeta_compute
void DSMContainer::initLocalMeta() {
    //What is the difference between dsmPool and dsmMR Answer dsmPool is the pointer of dsmMR
    localMeta.dsmBase = (uint64_t)dirCon_[0]->dsmPool;
    localMeta.lockBase = (uint64_t)dirCon_[0]->lockPool;
//    localMeta.cacheBase = (uint64_t)thCon_[0]->cachePool;
    localMeta.node_type = Memory;
//    // per thread APP
//    for (int i = 0; i < MAX_APP_THREAD; ++i) {
//        localMeta.appTh[i].lid = thCon_[i]->ctx.lid;
//        localMeta.appTh[i].rKey = thCon_[i]->cacheMR->rkey;
//        memcpy((char *)localMeta.appTh[i].gid, (char *)(&thCon_[i]->ctx.gid),
//               16 * sizeof(uint8_t));
//
//        localMeta.appUdQpn[i] = thCon_[i]->message->getQPN();
//    }

    // per thread DIR
    for (int i = 0; i < NR_DIRECTORY; ++i) {
        localMeta.dirTh[i].lid = dirCon_[i]->ctx.lid;
        localMeta.dirTh[i].rKey = dirCon_[i]->dsmMR->rkey;
        localMeta.dirTh[i].lock_rkey = dirCon_[i]->lockMR->rkey;
        memcpy((char *)localMeta.dirTh[i].gid, (char *)(&dirCon_[i]->ctx.gid),
               16 * sizeof(uint8_t));

        localMeta.dirUdQpn[i] = dirCon_[i]->message->getQPN();
        uint8_t* p = localMeta.dirTh[i].gid;
        fprintf(stdout,
                "Remote GID =%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x\n ",
                p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10],
                p[11], p[12], p[13], p[14], p[15]);
        printf("Put lid : 0x%x, qpn : 0x%x\n", localMeta.dirTh[i].lid, localMeta.dirUdQpn[i]);
    }


}
void DSMContainer::serverEnter(){
    memcached_return rc;
    uint64_t serverNum;

    while (true){
        rc = memcached_increment(memc, MEMORY_NUM_KEY, strlen(MEMORY_NUM_KEY), 1,
                                 &serverNum);   //事实上无论参数里限定max_server_num为多少，这里都是每开一个ServerNode，就递增一次，因此
        if (rc == MEMCACHED_SUCCESS){    //参数里指定的是max_server_num,

            myNodeID = serverNum - 1;    //因此这里是，每运行一次./MemoryServer才会增加一次myNodeID，暂时这里先当一台来用

            printf("I am memory servers %d [%s]\n", myNodeID, getIP());
            return;
        }
        fprintf(stderr, "Server %d Counld't incr value and get ID: %s, retry...\n",
                myNodeID, memcached_strerror(memc, rc));
        usleep(10000);
    }
}
void DSMContainer::serverConnect(){   

    size_t l;
    uint32_t flags;
    memcached_return rc;

    while (curServer < ComputemaxServer){
        //后续将在此处持续跟踪连接的computeServer，并持续陷入循环中
        //同时，当没有computeNode连接的时候，serverNumStr始终为0，因此curServer < serverNum是一直不成立的;这里会一直等待连接到最大数量的ComputeNode
        char *serverNumStr = memcached_get(memc, COMPUTE_NUM_KEY,
                                           strlen(COMPUTE_NUM_KEY), &l, &flags, &rc);
        if (rc != MEMCACHED_SUCCESS) {
            fprintf(stderr, "Server %d Counld't get serverNum: %s, retry\n", myNodeID,
                    memcached_strerror(memc, rc));
            continue;
        }
        uint32_t serverNum = atoi(serverNumStr);
        free(serverNumStr);

        // /connect server K
        for (size_t k = curServer; k < serverNum; ++k) {   //会根据读入的serverNum，依次根据现有的连接的computeNode数量连入
          //curServer会在Keeper的基类中被初始化为0
//            if (k != myNodeID) {
                connectNode(k);
                printf("I connect compute server %zu\n", k);
//            }
        }
        curServer = serverNum;  //将curserver和当前CN数量对齐，并使得下一次的连接可以从新的serverNum里开始
    }
}
bool DSMContainer::connectNode(uint16_t remoteID) {  //这里应该是设置对CN_Node的rdma元数据交换和qp_stage_trans

    setDataToRemote(remoteID);  //主要是记录一些qpn数据，但其实还是写在本地localMeta上

    std::string setK = setKey(remoteID);  //表明本机MN_nodeid - CN_nodeid 的metadata
    memSet(setK.c_str(), setK.size(), (char *)(&localMeta), sizeof(localMeta));  // 以 "xM-xC:localMeta"的键值对形式写入memcached服务器

    std::string getK = getKey(remoteID);     //反之，获得从CN_nodeid - MN_nodeid的metadata
    ExchangeMeta *remoteMeta = (ExchangeMeta *)memGet(getK.c_str(), getK.size()); //同理也是从memcached服务器读相关"xC-xM:remoteMeta"读远端meta
//    if (remoteMeta->node_type == Compute)
    assert(remoteMeta->node_type == Compute);
    setDataFromRemote(remoteID, remoteMeta);  //利用读到的qpn信息转换qp状态

    free(remoteMeta);
    return true;
}

void DSMContainer::setDataToRemote(uint16_t remoteID) {
    for (int i = 0; i < NR_DIRECTORY; ++i) {
        auto &c = dirCon_[i];

        for (int k = 0; k < MAX_APP_THREAD; ++k) {
            localMeta.dirRcQpn2app[i][k] = c->data2app[k][remoteID]->qp_num;
        }
    }

//    for (int i = 0; i < MAX_APP_THREAD; ++i) {
//        auto &c = thCon_[i];
//        for (int k = 0; k < NR_DIRECTORY; ++k) {
//            localMeta.appRcQpn2dir[i][k] = c->data[k][remoteID]->qp_num;
//        }
//
//    }
}

void DSMContainer::setDataFromRemote(uint16_t remoteID, ExchangeMeta *remoteMeta) {
    for (int i = 0; i < NR_DIRECTORY; ++i) {
        auto &c = dirCon_[i];

        for (int k = 0; k < MAX_APP_THREAD; ++k) {
            auto &qp = c->data2app[k][remoteID];

            assert(qp->qp_type == IBV_QPT_RC);
            modifyQPtoInit(qp, &c->ctx);
            modifyQPtoRTR(qp, remoteMeta->appRcQpn2dir[k][i],
                          remoteMeta->appTh[k].lid, remoteMeta->appTh[k].gid,
                          &c->ctx);
            modifyQPtoRTS(qp);
        }
    }

//    for (int i = 0; i < MAX_APP_THREAD; ++i) {
//        auto &c = thCon_[i];
//        for (int k = 0; k < NR_DIRECTORY; ++k) {
//            auto &qp = c->data[k][remoteID];
//
//            assert(qp->qp_type == IBV_QPT_RC);
//            modifyQPtoInit(qp, &c->ctx);
//            modifyQPtoRTR(qp, remoteMeta->dirRcQpn2app[k][i],
//                          remoteMeta->dirTh[k].lid, remoteMeta->dirTh[k].gid,
//                          &c->ctx);
//            modifyQPtoRTS(qp);
//        }
//    }

    auto &info = remoteCon[remoteID];  //这里针对的是某个remoteconnection，而remoteconnection在CN端又与CN的数量有关
//    info.dsmBase = remoteMeta->dsmBase;
    info.cacheBase = remoteMeta->cacheBase;
//    info.lockBase = remoteMeta->lockBase;

//    for (int i = 0; i < NR_DIRECTORY; ++i) {
//        info.dsmRKey[i] = remoteMeta->dirTh[i].rKey;
//        info.lockRKey[i] = remoteMeta->dirTh[i].lock_rkey;
//        info.dirMessageQPN[i] = remoteMeta->dirUdQpn[i];
//
//        for (int k = 0; k < MAX_APP_THREAD; ++k) {
//            struct ibv_ah_attr ahAttr;
//            fillAhAttr(&ahAttr, remoteMeta->dirTh[i].lid, remoteMeta->dirTh[i].gid,
//                       &thCon_[k]->ctx);
//            info.appToDirAh[k][i] = ibv_create_ah(thCon_[k]->ctx.pd, &ahAttr);
//
//            assert(info.appToDirAh[k][i]);
//        }
//    }


    for (int i = 0; i < MAX_APP_THREAD; ++i) {
        info.appRKey[i] = remoteMeta->appTh[i].rKey;
        info.appMessageQPN[i] = remoteMeta->appUdQpn[i];
        uint8_t* p = remoteMeta->appTh[i].gid;
        fprintf(stdout,
                "Remote GID =%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x\n ",
                p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10],
                p[11], p[12], p[13], p[14], p[15]);
        printf("Received lid : 0x%x, qpn : 0x%x\n", remoteMeta->appTh[i].lid, info.appMessageQPN[i]);

        for (int k = 0; k < NR_DIRECTORY; ++k) {
            struct ibv_ah_attr ahAttr;
            fillAhAttr(&ahAttr, remoteMeta->appTh[i].lid, remoteMeta->appTh[i].gid,
                       &dirCon_[k]->ctx);
            info.dirToAppAh[k][i] = ibv_create_ah(dirCon_[k]->ctx.pd, &ahAttr);

            assert(info.dirToAppAh[k][i]);
        }
    }
}

void DSMContainer::connectMySelf() {
    setDataToRemote(getMyNodeID());
    setDataFromRemote(getMyNodeID(), &localMeta);
}

void DSMContainer::initRouteRule() {
    std::string k =
            std::string(ServerPrefix) + std::to_string(this->getMyNodeID());
    memSet(k.c_str(), k.size(), getMyIP().c_str(), getMyIP().size());
}

void DSMContainer::barrier(const std::string &barrierKey) {

    std::string key = std::string("barrier-") + barrierKey;
    if (this->getMyNodeID() == 0) {
        memSet(key.c_str(), key.size(), "0", 1);
    }
    memFetchAndAdd(key.c_str(), key.size());
    while (true) {
        uint64_t v = std::stoull(memGet(key.c_str(), key.size()));
        if (v == this->getComputeServerNR()) {
            return;
        }
    }
}

uint64_t DSMContainer::sum(const std::string &sum_key, uint64_t value){
    std::string key_prefix = std::string("sum-") + sum_key;

    std::string key = key_prefix + std::to_string(this->getMyNodeID());
    memSet(key.c_str(), key.size(), (char *)&value, sizeof(value));

    uint64_t ret = 0;
    for (int i = 0; i < this->getComputeServerNR(); ++i) {
        key = key_prefix + std::to_string(i);
        ret += *(uint64_t *)memGet(key.c_str(), key.size());
    }
    return ret;
}
