//
// Created by ruihong on 5/25/22.
//
#include "DSMContainer.h"
int kReadRatio;
int kThreadCount;
int kComputeNodeCount;
int kMemoryNodeCount;
bool table_scan;
bool random_range_scan;
bool use_range_query;
void parse_args(int argc, char *argv[]){
    if (argc < 5){
        printf("Usage: ./benchmark kComputeNodeCount kMemoryNodeCount kReadRatio kThreadCount tablescan\n");
        exit(-1);
    }

    kComputeNodeCount = atoi(argv[1]);
    kMemoryNodeCount = atoi(argv[2]);
    kReadRatio = atoi(argv[3]);
    kThreadCount = atoi(argv[4]);
    int scan_number = atoi(argv[5]);
    if(scan_number == 0){
        table_scan = false;
        random_range_scan = false;
    }
    else if (scan_number == 1){
        table_scan = true;
        random_range_scan = false;
    }else{
        table_scan = false;
        random_range_scan = true;
    }

    printf("kComputeNodeCount %d, kMemoryNodeCount %d, kReadRatio %d, kThreadCount %d, tablescan %d\n", kComputeNodeCount,
           kMemoryNodeCount, kReadRatio, kThreadCount, scan_number);
}
int main(int argc,char* argv[])   //总体来说，这里是完成了各种connection的建立，往memcached服务器写元数据，以及构建与CN的连接，并未涉及具体索引
{
    //所以./MemoryServer封装的是一个MN-Node实体，它初始化本地directoryConnection数量，并发起对远端CN节点的RDMA建链，通过memcached服务器进行元数据交换
    //并且还会完成remote[CN_Num]的填充填写，每加入一个./MemoryServer，就会执行一次serverEnter()，从而在memcached服务器递增ServerNum的数量
    parse_args(argc, argv);
    DSMConfig conf;
    conf.ComputeNodeNum = kComputeNodeCount;  //在这里，虽然只有一个节点，但是目前猜测，是想用一个程序模拟NodeNum个节点对外的连接
    conf.MemoryNodeNum = kMemoryNodeCount;
    ThreadConnection *thCon[MAX_APP_THREAD];     //这里实际上没有使用threadConnection
    DirectoryConnection *dirCon[NR_DIRECTORY];   //这里的NR_DIRECTORY是用来限制每个CN_node的本地线程数量
    //DSMConfig conf;
    auto keeper = new DSMContainer(thCon, dirCon, conf, conf.ComputeNodeNum, conf.MemoryNodeNum);
    //在MN一端，实际上thCon是没有被使用的
    //DSMContainter首先进行DSM一侧的内存分配

    keeper->initialization();
    //但无法执行到此处，除非CN端发起最大数量的computeNode连接
    while(1){    }  //持续死循环，这是因为，依靠pthread_create创建的多线程会与main函数线程共享资源，如果main函数结束，pthread未结束，则可能导致无法正确
                //回收该线程的相关资源。因此在这里使用while(1)死循环，是为了让MN端的pthread能够一直执行下去，直到强制关闭线程
                //为了线程安全，这里在main函数退出前建议用pthread_join来等待线程完成
}

//Memoryserver这一端完成了RDMA资源的申请，和接收来自CN端的建链请求，并且转换本地qp状态，此后由异步线程dirTH来进行远端的rdma操作的监听