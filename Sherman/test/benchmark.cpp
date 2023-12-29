#include "Timer.h"
#include "Tree.h"
#include "zipf.h"
#include "third_party/random.h"

#include <city.h>
#include <stdlib.h>
#include <thread>
#include <time.h>
#include <unistd.h>
#include <vector>
#include <fstream>

// #define USE_CORO
const int kCoroCnt = 3;

// #define BENCH_LOCK

const int kTthreadUpper = 23;

extern uint64_t cache_miss[MAX_APP_THREAD][8];
extern uint64_t cache_hit[MAX_APP_THREAD][8];
extern uint64_t invalid_counter[MAX_APP_THREAD][8];  
extern uint64_t lock_fail[MAX_APP_THREAD][8];
extern uint64_t pattern[MAX_APP_THREAD][8];
extern uint64_t hot_filter_count[MAX_APP_THREAD][8];
extern uint64_t hierarchy_lock[MAX_APP_THREAD][8];
extern uint64_t handover_count[MAX_APP_THREAD][8];  //目前暂不清楚这些变量的具体含义
//是一些统计量，但是只用到了[][0]这一个

const int kMaxThread = 32;

int kReadRatio;
bool pure_write = false;
int kThreadCount;
int kComputeNodeCount;
int kMemoryNodeCount;
bool table_scan = false;
bool random_range_scan = false;
bool use_range_query = true;  //该命令参数似乎是在控制一些东西

uint64_t kKeySpace = 64 * define::MB;
//uint64_t kKeySpace = 50*1024*1024; // bigdata  好像是50MB的大小
//uint64_t kKeySpace = 50*1024*1024; //cloudlab
double kWarmRatio = 0.8;

double zipfan = 0;

std::thread th[kMaxThread];
uint64_t tp[kMaxThread][8];

extern volatile bool need_stop;
extern uint64_t latency[MAX_APP_THREAD][LATENCY_WINDOWS];
uint64_t latency_th_all[LATENCY_WINDOWS];

Tree *tree;
DSM *dsm;

inline Key to_key(uint64_t k) {  //city_hash形成随机key编码
  return (CityHash64((char *)&k, sizeof(k)) + 1) % kKeySpace;
}

class RequsetGenBench : public RequstGen {

public:
  RequsetGenBench(int coro_id, DSM *dsm, int id)
      : coro_id(coro_id), dsm(dsm), id(id){
    seed = rdtsc();
    mehcached_zipf_init(&state, kKeySpace, zipfan,
                        (rdtsc() & (0x0000ffffffffffffull)) ^ id);
  }

  Request next() override {
    Request r;
    uint64_t dis = mehcached_zipf_next(&state);

    r.k = to_key(dis);
    r.v = 23;
    r.is_search = rand_r(&seed) % 100 < kReadRatio;

    tp[id][0]++;

    return r;
  }

private:
  int coro_id;
  DSM *dsm;
  int id;

  unsigned int seed;
  struct zipf_gen_state state;  //zipf分布的生成器
};

RequstGen *coro_func(int coro_id, DSM *dsm, int id){
  return new RequsetGenBench(coro_id, dsm, id);
}  //目前还不清楚含义

Timer bench_timer;
std::atomic<int64_t> warmup_cnt{0};
std::atomic_bool ready{false};
extern bool enable_cache;


//在这里，thread_run()做了两件事，一件是进行预热插入，插入50M个键值对，构建基础的tree_index，
//另一件是在死循环中按照输入的KReadRatio来控制插入的比例
//因此KReadRatio实际上是一种概率控制，而此时，这里的不同数据集也可以加入进来，到这个里面来使用

//最后，在while(true)的死循环里还记录了延迟信息的统计
void thread_run(int id){
    Random64 rand(id);

    bindCore(id);

  dsm->registerThread();//registerThread的本质是利用APPID记录当前已经有多少thread加入了
  //同时，这里的thread_id是一个DSM的thread_locl变量，和threadConnection的ID也不是一回事
  //除此之外，threadID还做了一件初始化本地buffer的事情

#ifndef BENCH_LOCK
  uint64_t all_thread = kThreadCount * dsm->getClusterSize();
  uint64_t my_id = kThreadCount * dsm->getMyNodeID() + id;
  //这里的my_id指的是整个集群里的thread编号
  printf("I am %d\n", my_id);

  if (id == 0) {
    bench_timer.begin();  //由主线程启动计时器
  }

  uint64_t end_warm_key = kKeySpace; //KKeySpace规定了bench的操作规模，每个线程都负责一起插入kKeySpace规模的KV
    enable_cache = true;             //每个线程负责插入自己的当前thread_id对应的那些key-value
  //kWarmRatio *
  for (uint64_t i = 1; i < end_warm_key; ++i){  //插入规模
      // we can not sequentially pop up the data. Otherwise there will be a bug.
      if (i % all_thread == my_id) {    
//      tree->insert(i, i * 2);
        tree->insert(to_key(i), i * 2);   //CN多线程的thread_run
//        tree->insert(rand.Next()%(kKeySpace), i * 2);

        }
      if (i % 4000000 == 0 && id ==0){
          printf("warm up number: %lu\n", i);
      }
  }
    if (table_scan){
        for (uint64_t i = 1; i < end_warm_key; ++i) {
            if (i % all_thread == my_id) {  //每个线程负责插入自己id负责范围内的key-value
                Value v;
                tree->search(to_key(i),v);
            }
            if (i % 1000000 == 0 ){
                printf("cache warm up number: %lu\r", i);
            }
        }
    }

  warmup_cnt.fetch_add(1);

  if (id == 0) {
    while (warmup_cnt.load() != kThreadCount)
      ;
    printf("node %d finish\n", dsm->getMyNodeID());
    dsm->barrier("warm_finish");//由每个node的0线程作为主线程进行一波同步
    //barrier不是同步一个node的所有线程，而是同步所有CN_node

    uint64_t ns = bench_timer.end();
    printf("warmup time %lds\n", ns / 1000 / 1000 / 1000);

    tree->index_cache_statistics();
    tree->clear_statistics();

    ready.store(true);

    warmup_cnt.store(0);
  }

  while (warmup_cnt.load() != 0)
    ;

#endif

#ifdef USE_CORO
  tree->run_coroutine(coro_func, id, kCoroCnt);//这里主要是协程的实现内容

#else

  /// without coro
  unsigned int seed = rdtsc();
  struct zipf_gen_state state;
  mehcached_zipf_init(&state, kKeySpace, zipfan,
                      (rdtsc() & (0x0000ffffffffffffull)) ^ id);

  Timer timer;
  Value *value_buffer = (Value *)malloc(sizeof(Value) * 1024 * 1024);
  int print_counter = 0;
  uint64_t scan_pos = 0;
    int range_length = 1000*1000;
  while (true) {  //这里的死循环表明了后续的插入次数是无限的，因而也导致了后续的内存开销的爆炸

    if (need_stop || id >= kTthreadUpper) {
      while (true)
        ;
    }
    // the dis range is [0, 64M]
//    uint64_t dis = mehcached_zipf_next(&state);
    uint64_t key;
//      key = rand.Next()%(kKeySpace - range_length);
//    uint64_t key = to_key(dis);

    Value v;

    timer.begin();
    size_t finished_ops = 0;
#ifdef BENCH_LOCK
    if (dsm->getMyNodeID() == 0) {
      while (true)
        ;
    }
    tree->lock_bench(key);
#else
      if (table_scan){

          if (use_range_query){
              finished_ops = tree->range_query(scan_pos, scan_pos + 1000*1000, value_buffer);
              scan_pos += 1000*1000;
              if(scan_pos > kKeySpace)
                  break;
          }else{
              tree->search(scan_pos,v);
              scan_pos++;
              if(scan_pos > kKeySpace)
                  break;
          }

      }else if(random_range_scan){
          key = rand.Next()%(kKeySpace - range_length);
          finished_ops = tree->range_query(scan_pos, scan_pos + 1000*1000, value_buffer);
      }else{
//          assert(scan_)
          key = rand.Next()%(kKeySpace);  //实际的测试代码里，数据集是由rand函数来生成的，因此实际上zipf分布生成器并没有被使用
          if (!pure_write && rand_r(&seed) % 100 < kReadRatio) { // GET,这里pure_write被设置为false，如果输入KReadRatio=0，那么pure_write可能被置为1  
//        printf("Get one key");
          //但实际上并没有哪里修改pure_write的内容
              tree->search(key, v);  //保证落入到KReadRatio的进行读操作

          }else{
              v = 12;
              tree->insert(key, v);   //而除此之外则进行写操作
          }
      }

    print_counter++;
    if (print_counter%100000 == 0)
    {
        printf("%d key-value pairs hase been executed\r", print_counter);
    }
//      if (print_counter%100000 == 0)
//      {
//          printf("the generated distributed key is %d\n", dis);
//      }
#endif
    auto us_10 = timer.end() / 100;
    if (us_10 >= LATENCY_WINDOWS) {
      us_10 = LATENCY_WINDOWS - 1;
    }
    latency[id][us_10]++;
      if ((table_scan&&use_range_query) || random_range_scan){
          tp[id][0] += finished_ops;
      }else{
          tp[id][0]++;
      }

  }

#endif
}

void parse_args(int argc, char *argv[]) {
  if (argc != 6) {
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

void cal_latency() {  //某种统计量的计算
  uint64_t all_lat = 0;
  for (int i = 0; i < LATENCY_WINDOWS; ++i) {
    latency_th_all[i] = 0;
    for (int k = 0; k < MAX_APP_THREAD; ++k) {
      latency_th_all[i] += latency[k][i];
    }
    all_lat += latency_th_all[i];
  }

  uint64_t th50 = all_lat / 2;
  uint64_t th90 = all_lat * 9 / 10;
  uint64_t th95 = all_lat * 95 / 100;
  uint64_t th99 = all_lat * 99 / 100;
  uint64_t th999 = all_lat * 999 / 1000;

  uint64_t cum = 0;
  for (int i = 0; i < LATENCY_WINDOWS; ++i) {
    cum += latency_th_all[i];

    if (cum >= th50) {
      printf("p50 %f\t", i / 10.0);
      th50 = -1;
    }
    if (cum >= th90) {
      printf("p90 %f\t", i / 10.0);
      th90 = -1;
    }
    if (cum >= th95) {
      printf("p95 %f\t", i / 10.0);
      th95 = -1;
    }
    if (cum >= th99) {
      printf("p99 %f\t", i / 10.0);
      th99 = -1;
    }
    if (cum >= th999) {
      printf("p999 %f\n", i / 10.0);
      th999 = -1;
      return;
    }
  }
}

int main(int argc, char *argv[]){

    std::cout << "Using Boost "
              << BOOST_VERSION / 100000     << "."  // major version
              << BOOST_VERSION / 100 % 1000 << "."  // minor version
              << BOOST_VERSION % 100                // patch level
              << std::endl;
  parse_args(argc, argv);

  DSMConfig config;
  config.ComputeNodeNum = kComputeNodeCount;
  config.MemoryNodeNum = kMemoryNodeCount;
  dsm = DSM::getInstance(config);  //现在的疑问是，在何处转换了message的qp？？

  dsm->registerThread();
  //registerThread本质上是从cache_pool拿一块区域出来，初始化每个线程的rbuf区域，做了一些分块切割
  //至此，已经完成了rdma的建链，已经MN和CN加入集群，开辟自己的内存区域


  tree = new Tree(dsm);  //CN利用DSM模块来进行对远程内存池的访问
  //主要工作是初始化本地锁表，然后声明属于该treeIndex的local_cache
  //最后，申请了root_leaf的内存并写入该内存，并且在root_ptr_ptr的位置写入了该根节点的指针



#ifndef BENCH_LOCK
  if (dsm->getMyNodeID() == 0) {//主节点进行预热的tree_build行为
    for (uint64_t i = 1; i < 1024000; ++i) {
//        printf("insert key %d", i);
      tree->insert(to_key(i), i * 2);    //main函数里的预插入，先构建一部分tree_index，后续都是对此的操作
      //tree->insert做了啥还要具体再细看
//        tree->insert(i, i * 2);
    }
  }
#endif

  dsm->barrier("benchmark");//利用memcached服务器同步CN节点的做法，这里主要是多个CN节点之间的同步

  for (int i = 0; i < kThreadCount; i++){  //按照预先定义的thread参数来运行，生成bench下的多线程
    th[i] = std::thread(thread_run, i);
  }

#ifndef BENCH_LOCK
  while (!ready.load())
    ;
#endif

//后续看起来像是在打印一些结果，例如吞吐等等


  timespec s, e;
  uint64_t pre_tp = 0;
  uint64_t pre_ths[MAX_APP_THREAD];
  for (int i = 0; i < MAX_APP_THREAD; ++i) {
    pre_ths[i] = 0;
  }

  int count = 0;



  clock_gettime(CLOCK_REALTIME, &s);
  while (true) {
      // throutput every 10 second
    sleep(10);
    clock_gettime(CLOCK_REALTIME, &e);
    int microseconds = (e.tv_sec - s.tv_sec) * 1000000 +
                       (double)(e.tv_nsec - s.tv_nsec) / 1000;

    uint64_t all_tp = 0;
    for (int i = 0; i < kThreadCount; ++i) {
      all_tp += tp[i][0];
//      tp[i][0] = 0;
    }
    uint64_t cap = all_tp - pre_tp;
    pre_tp = all_tp;
      printf("cap is %lu\n", cap);

    for (int i = 0; i < kThreadCount; ++i) {
      auto val = tp[i][0];
      // printf("thread %d %ld\n", i, val - pre_ths[i]);
      pre_ths[i] = val;
    }

    uint64_t all = 0;
    uint64_t hit = 0;
//    uint64_t realhit = 0;
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
      all += (cache_hit[i][0] + cache_miss[i][0]);
      hit += cache_hit[i][0];
      //May be we need atomic variable here.
        cache_hit[i][0] = 0;
        cache_miss[i][0] = 0;
//      realhit += invalid_counter[i][0];
    }

    uint64_t fail_locks_cnt = 0;
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
      fail_locks_cnt += lock_fail[i][0];
      lock_fail[i][0] = 0;
    }
    // if (fail_locks_cnt > 500000) {
    //   // need_stop = true;
    // }

    //  pattern
    uint64_t pp[8];
    memset(pp, 0, sizeof(pp));
    for (int i = 0; i < 8; ++i) {
      for (int t = 0; t < MAX_APP_THREAD; ++t) {
        pp[i] += pattern[t][i];
        pattern[t][i] = 0;
      }
    }

    uint64_t hot_count = 0;
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
      hot_count += hot_filter_count[i][0];
      hot_filter_count[i][0] = 0;
    }

    uint64_t hier_count = 0;
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
      hier_count += hierarchy_lock[i][0];
      hierarchy_lock[i][0] = 0;
    }

    uint64_t ho_count = 0;
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
      ho_count += handover_count[i][0];
      handover_count[i][0] = 0;
    }

    clock_gettime(CLOCK_REALTIME, &s);

    if (++count % 3 == 0 && dsm->getMyNodeID() == 0) {
      cal_latency();
    }

    double per_node_tp = cap * 1.0 / microseconds;
    uint64_t cluster_tp = dsm->sum((uint64_t)(per_node_tp * 1000));
    // uint64_t cluster_we = dsm->sum((uint64_t)(hot_count));
    // uint64_t cluster_ho = dsm->sum((uint64_t)(ho_count));

    printf("%d, throughput %.4f\n", dsm->getMyNodeID(), per_node_tp);

//    if (dsm->getMyNodeID() == 0) {
      printf("cluster throughput %.3f\n", cluster_tp / 1000.0);

      // printf("WE %.3f HO %.3f\n", cluster_we * 1000000ull / 1.0 /
      // microseconds,
      //        cluster_ho * 1000000ull / 1.0 / microseconds);

//      if (pure_write && hit * 1.0 / all >= 0.99){
//          std::ofstream write_output_file;
//          write_output_file.open ("purewrite_performance.txt");
//          char buffer[ 200 ];
//          snprintf( buffer, sizeof( buffer ),
//                    "%d, throughput %.4f\n", dsm->getMyNodeID(), per_node_tp );
//
//          write_output_file << buffer;
//          write_output_file.close();
//          exit(0);
//      }
//        // this is the real cache hit ratge
//      if (hit * 1.0 / all >= 0.999){
//          char buffer[ 200 ];
//          snprintf( buffer, sizeof( buffer ),
//                    "%d, throughput %.4f\n", dsm->getMyNodeID(), per_node_tp );
//          std::ofstream myfile;
//          myfile.open ("pureread_performance.txt");
//          myfile << buffer;
//          myfile.close();
//          printf("switch to pure write\n");
//          kReadRatio = 0;
//          pure_write = true;
//
//      }


      printf("cache hit rate: %lf\n", hit * 1.0 / all);
      // printf("ACCESS PATTERN");
      // for (int i = 0; i < 8; ++i) {
      //   printf("\t%ld", pp[i]);
      // }
      // printf("\n");
      // printf("%d fail locks: %ld %s\n", dsm->getMyNodeID(), fail_locks_cnt,
      //        getIP());

      // printf("hot count %ld\t hierarchy count %ld\t handover %ld\n",
      // hot_count,
      //        hier_count, ho_count);
//    }
  }

  return 0;
}