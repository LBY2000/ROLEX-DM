#include <gflags/gflags.h>
#include <iostream>

#include "rlib/core/nicinfo.hh"      /// RNicInfo
#include "rlib/core/rctrl.hh"        /// RCtrl
#include "rlib/core/common.hh"       /// LOG


#include "rolex/huge_region.hh"
#include "rolex/trait.hpp"
#include "rolex/model_allocator.hpp"
#include "rolex/remote_memory.hh"
#include "../load_data.hh"



DEFINE_int64(port, 8888, "Server listener (UDP) port.");
//利用gflags来定义一个命令行参数，--port，同时如果要使用的话即为FLAGS_port
DEFINE_uint64(leaf_num, 10000000, "The number of registed leaves.");
// DEFINE_uint64(reg_leaf_region, 101, "The name to register an MR at rctrl for data nodes.");
//同样是利用gflags来定义另一个命令行参数, --leaf_num，如果要使用的话还是要用FLAGS_leaf_num



using namespace rdmaio;
//这个是在common.hh里定义的命名空间
using namespace rolex;
//这个好像整个项目的多处都有相关定义


volatile bool running = false;
//看起来像是一个用于并发访问控制的变量
std::atomic<size_t> ready_threads(0);
//ready_threads后面是一个用于线程同步条件的判断变量。在这里，每个线程启动后都会导致read_threads的递增，直到达到预定义的线程数量
//这个声明表示其为一个原子变量，且类型为size_t
RCtrl* ctrl;
//RCtrl类在/deps/rlib/core/rctrl.hh中定义，应该是用来管理RDMA通信的类，用来管理注册的内存区域，响应请求等等

rolex_t *rolex_index;
//在traits.hpp中有定义，实际上为class ROLEX的别名，后者出现在rolex/rolex.hpp中，看样子主要是用来管理ROLEX索引的类

void prepare();
void run_benchmark(size_t sec);
void *run_fg(void *param);

int main(int argc, char** argv)
{ 
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  //利用gflags类来实现对命令行参数的管理，在其后，--形式的命令行参数将变为FLAGS_XX形式的变量

  bench::load_benchmark_config();
  //用于加载benchmark的函数
  //在这个函数中，利用DEFINE_XX定义了许多命令行参数，以及可被用FLAGS_XX使用的参数
  //定义了一个WORKLOAD的枚举结构来表示不同的工作负载名字
  //其后还定义了一个名为BenConfig的参数结构体
  //load_benchmark_config()函数内的内容主要是：依据输入的命令行参数，来为BenConfig结构体初始化参数；或者说，初始化基准测试的参数
  prepare();
  // rolex_index->print_data();
  //
  run_benchmark(5);
  // sleep(10);

  return 0;
}

void prepare(){
  const usize MB = 1024 * 1024;
  //先定义MB的大小为1Ki*1Ki，基本上符合1Mi的大小
  ctrl = new RCtrl(FLAGS_port);  //RCtrl类里只有SRpcHandler，因此貌似只能处理Recv的消息
  //所以似乎这里的ctrl类绑定了一个监听本机FLAGS_port端口上的Channel
  //利用这里，命名行定义的port参数，初始化一个RDMA连接管理RDMA配置信息类ctrl
  RM_config conf(ctrl, 1024 * MB, FLAGS_leaf_num*sizeof(leaf_t), FLAGS_reg_leaf_region, FLAGS_leaf_num);
  //这里，RM_config是一个用于管理存储和远程内存配置相关信息的类，它接受ctrl为参数，同时标记远程内存空间分配区域的大小
  //RM_config是一个纯配置信息记录类，和bench_config一样
  remote_memory_t* RM = new remote_memory_t(conf); 
  //这个remote_memory_t类是RemoteMemory类的别名
  //RemoteMemory类将会吸收ctrl和conf来初始化配置RDMA通信的信息；从而开始进行RDMA连接的管理和远程内存分配的管理等等
  //总之，`RemoteMemory` 类用于封装远程内存的管理和分配操作。它根据提供的配置信息，在 `rdmaio::RCtrl` 管理的 RDMA 连接上创建并注册
  //  模型区域和叶子区域，然后通过内存分配器提供对这些区域的内存分配。

  //在这个RM里，似乎是先调用Huge_region()内的函数进行本地内存分配，然后再转化为RCtrl里的远程注册的内存？？
  //总之，现在RM封装了注册在远程RDMA区域里的内存信息
  load_data();
  //在load_benchmark_config()中，完成了ycsba,ycsbb等等这样格式的输入参数到实际负载名的map转换
  //这里利用switch语句和case实现了负载名到加载数据集的转换
  //在他的测试数据集中，只有normal和lognormal数据是自己手动生成的，其他类型的数据集都要自己加载
  //其他类型的数据集由于他的处理逻辑原因，可能要自己重写这个加载数据集和测试的部分
  //其中，在命令行参数中，nkeys指定数量的KV对，会在exist_keys这个vector容器中存储;non_exist_keys也同理
  //最终，exist_keys会在后续中被使用，先进行预排序，当然，去重在更前面
  LOG(2) << "[processing data]";
  std::sort(exist_keys.begin(), exist_keys.end());
  //先进行初次的排序
  exist_keys.erase(std::unique(exist_keys.begin(), exist_keys.end()), exist_keys.end());
  //去掉随机生成的重复KV
  std::sort(exist_keys.begin(), exist_keys.end());
  //再排序一下
  for(size_t i=1; i<exist_keys.size(); i++){
      assert(exist_keys[i]>=exist_keys[i-1]);
  }
  //验证是不是有序了
  rolex_index = new rolex_t(RM, exist_keys, exist_keys);
  // rolex_index->print_data();
  //这里，rolex_t就是rolex/rolex.hpp中的类ROLEX的别名；同时，作者为了省事，直接将K也作为了V;在这里由于传入了RM和K和V，因此会导致调用第二个构造函数
  //在这里，RM会让ROLEX的RometeMemory类也得到初始化，同时会启动train()函数来进行针对已有排序KV的模型训练过程
  //训练好后，ROLEX变成初始化的学习型索引，这个时候是维持在误差范围内的的OptimalPLR
  RDMA_LOG(2) << "Data distribution bench server started!";
  RM->start_daemon();
  //这里会启动一个线程来执行daemon()函数，这个函数里的asm volatile("" ::: "memory");是设置一个内存屏障，使得running变量的修改被其他线程可见
  //似乎daemon函数启动后就可以开始正常的RDMA连接请求处理了
  //这个darmon处理连接请求是依靠Rctrl类来进行的
}



void run_benchmark(size_t sec) {
    pthread_t threads[BenConfig.threads];
    //这个BenConfig的内容在main函数的bench::load_benchmark_config()中配置了BenConfig的内容，且BenConfig是一个结构体
    //这里是按照bench配置里的threads数量配置线程数
    thread_param_t thread_params[BenConfig.threads];
    // check if parameters are cacheline aligned
    //猜测这里的意思是，按照线程数量创建线程参数的数量，且线程参数已经在rolex_util_back中对齐了
    for (size_t i = 0; i < BenConfig.threads; i++) {
        ASSERT ((uint64_t)(&(thread_params[i])) % CACHELINE_SIZE == 0) <<
            "wrong parameter address: " << &(thread_params[i]);
    }
    //这里依靠ASSERT检查多线程参数是否对齐
    running = false;
    for(size_t worker_i = 0; worker_i < BenConfig.threads; worker_i++){    //依靠先前bench里规定的线程数量创建线程
        thread_params[worker_i].thread_id = worker_i;                      
        thread_params[worker_i].throughput = 0;                            //为每个线程配置好线程参数
        int ret = pthread_create(&threads[worker_i], nullptr, run_fg,      //每个线程在后台执行的是run_fg()函数
                                (void *)&thread_params[worker_i]);         //这里按照线程号来创建线程，并传递每个线程缓存行对齐的参数
        ASSERT (ret==0) <<"Error:" << ret;                                 //如果线程创建失败则提示信息
    }

    LOG(3)<<"[micro] prepare data ...";      
    while (ready_threads < BenConfig.threads) sleep(0.5);  //由于后续run_fg()中的线程启动时才会导致read_threads++,因此这里可能是等待线程
                                                           //都运行起来了，再执行后面的操作
    running = true;
    std::vector<size_t> tput_history(BenConfig.threads, 0);
    size_t current_sec = 0;
    while (current_sec < sec) {
        sleep(1);
        uint64_t tput = 0;
        for (size_t i = 0; i < BenConfig.threads; i++) {
            tput += thread_params[i].throughput - tput_history[i];
            tput_history[i] = thread_params[i].throughput;                  //看这里是想计算一个吞吐，但是没有理清这个计算逻辑
        }
        LOG(2)<<"[micro] >>> sec " << current_sec << " throughput: " << tput;
        ++current_sec;   //以1s为单位进行的
    }

    running = false;
    void *status;
    for (size_t i = 0; i < BenConfig.threads; i++) {
        int rc = pthread_join(threads[i], &status);
        ASSERT (!rc) "Error:unable to join," << rc;
    }   //等待多个子线程的终止

    size_t throughput = 0;
    for (auto &p : thread_params) {
        throughput += p.throughput;
    }    //累加吞吐量
    LOG(2)<<"[micro] Throughput(op/s): " << throughput / sec;   //计算每秒的吞吐量
}

void *run_fg(void *param){   //这个函数是每个线程会运行的函数
    thread_param_t &thread_param = *(thread_param_t *)param;   //先把这个参数记下来，这两个参数分别是吞吐量和id
    uint32_t thread_id = thread_param.thread_id;       //记录下当前的线程id

    std::random_device rd;                              //利用硬件实现真正的随机数种子
    std::mt19937 gen(rd());                             // gen是一个随机数生成器
    std::uniform_real_distribution<> ratio_dis(0, 1);   // ratio_dis是一个均匀分布

    size_t non_exist_key_n_per_thread = nonexist_keys.size() / BenConfig.threads;     //non_exist_key数量被均匀分配到每个线程中
    size_t non_exist_key_start = thread_id * non_exist_key_n_per_thread;              //确定每个线程的non_exist_key的开始读取地址
    size_t non_exist_key_end = (thread_id + 1) * non_exist_key_n_per_thread;          //确定每个线程的non_exist_key的终止读取地址
    std::vector<K> op_keys(nonexist_keys.begin() + non_exist_key_start,               //以偏移量的方式计算开始和终止的插入key
                                   nonexist_keys.begin() + non_exist_key_end);

    LOG(2) << "[micro] Worker" << thread_id << " Ready.";
    size_t query_i = 0, insert_i = 0, delete_i = 0, update_i = 0;
    // exsiting keys fall within range [delete_i, insert_i)
    ready_threads++;
    V dummy_value = 1234;  //可能是设置了假的value值

    while (!running)
        ;
    //int i=0;
	while (running) {   //下面的是增删改查的基本操作
        double d = ratio_dis(gen);  //这里是利用随机数生成器和种子来生成一个0-1之间的随机分布的数
        //具体的逻辑是，每个running轮次就执行一个操作；ratio_dis是一个0-1之间的随机分布，依靠gen来生成0-1之间的随机数
        //然后，如果这个随机数落到了read_ratio的比例中，就执行读；其他的情况也一样；从而依靠概率来模拟键值对操作情况
        if (d <= BenConfig.read_ratio){                   // search
            K dummy_key = exist_keys[query_i % exist_keys.size()];  //先利用查询的次数来随机定位一个已有的key
            rolex_index->search(dummy_key, dummy_value);
            query_i++;
            if (unlikely(query_i == exist_keys.size())){
                query_i = 0;
            //    break;
            }
        }else if (d <= BenConfig.read_ratio+BenConfig.insert_ratio){  // insert，概率落到read_ratio-insert_ratio之间了
            K dummy_key = nonexist_keys[insert_i % nonexist_keys.size()];   //利用待插入的KV，随机挑选一个进行插入操作
            rolex_index->insert(dummy_key, dummy_key);
            insert_i++;
            if (unlikely(insert_i == nonexist_keys.size())) {
                insert_i = 0;
            }
        } else if (d <= BenConfig.read_ratio+BenConfig.insert_ratio+BenConfig.update_ratio){    // update
            K dummy_key = nonexist_keys[update_i % nonexist_keys.size()];
            rolex_index->update(dummy_key, dummy_key);    //update操作的原理也类似，依靠概率分布来选择这个操作；同时依靠次数选择
            update_i++;
            if (unlikely(update_i == nonexist_keys.size())) {
                update_i = 0;
            }
        }  else {                // remove
            K dummy_key = exist_keys[delete_i % exist_keys.size()]; //delete是基本操作中剩下的；然后现在，原作者还没有给出scan操作的解法
            rolex_index->remove(dummy_key);                         //论文中的实验部分确实也是缺少scan操作的性能对比
            delete_i++;
            if (unlikely(delete_i == exist_keys.size())){
                delete_i = 0;
            }
        }
        thread_param.throughput++;
    }   
    pthread_exit(nullptr);
}