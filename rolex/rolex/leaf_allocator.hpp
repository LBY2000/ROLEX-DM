#pragma once

#include <iostream>
#include <optional>


#include "xutils/marshal.hh"
#include "xutils/spin_lock.hh"
#include "r2/src/common.hh"

using namespace r2;


namespace rolex {


/**
 * @brief Currently, it incurs high overhead when the compute nodes allcate memory on memory nodes
 *        So, we preallocate the data leave //所以说这个内存管理确实是个问题吗？
 * 
 * @tparam Leaf the type that we allocate
 * @tparam S the size of a data leaf
 */
template <typename Leaf, usize S>   //leaf是要分配的类型，S是一个leaf类型的大小
class LeafAllocator{
  ::xstore::util::SpinLock lock;
  //自旋锁在获取锁失败的时候会不断尝试重新获取锁
  //相比之下，互斥锁则会在获取锁失败的时候睡眠，直到锁被释放，然后唤醒睡眠线程
  //自旋锁相比互斥锁会少一个线程调度开销，没有睡眠和唤醒的开销；自旋锁更适合的场景是临界区比较小的情况
  char *mem_pool = nullptr;      /// the start memory of the allocated data leaves 
  const u64 total_sz = 0;        /// the total size of the register memory that we can allocate
  u64 cur_alloc_sz = 0;          /// the size that has been allocated
   
public:
  usize cur_alloc_num = 0;
  
  //所以5个成员变量，1个自旋锁，1个内存池地址，1个总的可用空间大小，1个当前分配的大小和1个当前已经分配出去的叶节点数量
  /**
   * @brief Construct LeafAllocator with region m;
   *           preallcote leaves and preserve 2*sizeof(u64) for [used, total]
   * 
   * @param m the start address of the pool
   * @param t the total size of the pool
   */

  explicit LeafAllocator(char *m, const u64 &t) : mem_pool(m), total_sz(t){
    prealloc_leaves((total_sz-sizeof(u64))/S);   //这里因为原论文里说到过，每个内存池都有8字节的数据作为分配元数据管理，所以要减去这8字节
    //如果没有预先指定分配的叶节点数量，那么就将内存池内所有的叶节点都分配出去
  }

  /**
   * @brief Construct LeafAllocator with region m; preallocate leaves with the given leaf_num
   *  
   * @param leaf_num the predefined leaf_num
   */
  explicit LeafAllocator(char *m, const u64 &t, const u64 &leaf_num) : mem_pool(m), total_sz(t) {  //explicit是为了防止不必要的隐式类型转换，提高代码可阅读性
    LOG(3) << "leaf_num: "<<leaf_num<<" , (total_sz-sizeof(u64))/S): "<<(total_sz-sizeof(u64))/S;
    //先告诉别人，你指定的leaf_num数量和一共可以获得的叶数量
    ASSERT(leaf_num<(total_sz-sizeof(u64))/S) << "Small leaf region for allocating "<<(total_sz-sizeof(u64))/S;
    //如果没有分配满则会提醒
    prealloc_leaves(leaf_num);
    //按照预先定义的leaf_num数量进行分配
  }

  inline auto used_num() -> u64 {    
    return ::xstore::util::Marshal<u64>::deserialize(mem_pool, sizeof(u64)); //这个deserialize()函数的实现方式是，将mem_pool开始位置的数据，向后查找sizeof(u64)个偏移量
    //并记录该数值，返回；这样的做法相当于直接从mem_pool拿元数据
  }

  inline auto allocated_num() -> u64 {
    return ::xstore::util::Marshal<u64>::deserialize(mem_pool+sizeof(u64), sizeof(u64)); //不太明白这个地址的含义:mem_pool+sizeof(u64)，总的来说就是去该地址拿一个数据
  }

  // =========== access the leaf ============
  auto get_leaf(u64 num) -> char * {
    ASSERT(num <= used_num()) << "Exceed the Preallocated leaves. [used_num:num] " << used_num() <<" " << num;
    //get_leaf似乎是依靠这个叶节点的num编号来获得的，其中num编号要小于已经使用的叶节点数量；否则断言会给出警告
    return mem_pool + 2*sizeof(u64) + num*S;
    //这里的2个sizeof(u64)是因为有两个8字节元数据，已经使用的叶节点数量和预先分配的总共的叶节点数量
    //相当于按照num号进行偏移量转换，基地址+该偏移的leaf数，从而在这个偏移量上获得请求的leaf
  }

  // ============ allocate leaves ===============
  auto fetch_new_leaf() -> std::pair<char *, u64> {
    u64 num = fetch_and_add();  //这里获得的还是旧的可用叶节点数量
    // if(num>=allocated_num()) 
    //   LOG(2) <<"Preallocated " <<allocated_num()<< " leaves are insufficient for num: "<<num << ", key "<<key;
    ASSERT(num < allocated_num()) << "Preallocated " <<allocated_num()<< " leaves are insufficient for num: "<<num;
    //如果已使用的数量大于等于了总共预先分配的叶节点数量，就会打印提示说预先分配的叶节点数量不够用了
    return {mem_pool + 2*sizeof(u64) + num*S, num};
  }
  

private:
  auto alloc() -> ::r2::Option<char *> {
    lock.lock();
    if (cur_alloc_sz + S <= total_sz) {
      //如果有空闲空间
      auto res = mem_pool + cur_alloc_sz;
      //cur_alloc_sz已经添加了两个元数据：已经使用的Leaf数量和总共可用的leaf数量
      cur_alloc_sz += S;
      //由于要新分配一个叶节点，因此会使得内存池大小扩张S，其中S为一个Leaf的大小
      cur_alloc_num += 1;
      //新叶节点多分配一个，所以当前的分配数量会递增
      lock.unlock();
      return res;
    }
    lock.unlock();
    //否则就是当前分配的空闲空间数量少于总共的空间数量
    LOG(4) << cur_alloc_sz << " " << total_sz;
    return {};
  }

  auto dealloc(char *data) { ASSERT(false) << "not implemented"; }    //没有实现dealloc()函数


  //  ======= Preallocate leaves to store data ============
  void prealloc_leaves(u64 leaf_num){   
    // 1.init the metadata (current number of the leaf, total number of allocated leaves)
    u64 cur_num = 0;
    ASSERT(cur_alloc_sz==0) << "LeafAllocator has been used since cur_alloc_sz != 0.";
    //预先分配阶段，因为没有在使用的叶节点，否则会报错，重置内存池
    memcpy(mem_pool, &cur_num, sizeof(u64));
    //将当前的数量0初始化到mem_pool，这个应该相当于used_num()函数里所说的那样
    cur_alloc_sz += sizeof(u64);
    //添加元数据的内存消耗记录
    memcpy(mem_pool+cur_alloc_sz, &leaf_num, sizeof(u64));
    //在used_num()规定的，8字节的使用数量后，添加总共可用的leaf_num数量，这个是根据传参决定的；如果不指定leaf_num参数则会将内存池内可获得的容量几乎全用来初始化leaf_num
    cur_alloc_sz += sizeof(u64);
    //更新添加另一个元数据，leaf_num的空间消耗
    // 2.preallocate the data leaves
    for(int i=0; i<leaf_num; i++) {
      Leaf* cur_leaf = new (reinterpret_cast<Leaf*>(alloc().value())) Leaf();
      //这里的循环分配的逻辑是，先调用alloc()函数进行分配，但是可能得到的是一个::r2::Option<char *>类型的变量
      //然后通过.value()来解析这样的对象，得到一个char *类型的指针
      //最后通过强制类型转换，获得一个Leaf *类型的变量
      //这里是一种C++中使用的，填充内存池的办法
    }
    LOG(3) << "Preallocate leaf number--> [used, total]: [" 
           << ::xstore::util::Marshal<u64>::deserialize(mem_pool, sizeof(u64)) << ", "
           <<::xstore::util::Marshal<u64>::deserialize(mem_pool+sizeof(u64), sizeof(u64))<<"]";
    //打印实际使用的和可用的叶节点数量，这里的两个deserialize()函数将会分别复制mem_pool的开头8字节和接下来的8字节数据
  }

  /**
   * @brief Obtain the number of current ideal leaves and add the number with 1
   *          this function is used for memory node, rather than the compute node
   * 
   * @return u64 the number of current ideal leaves
   */
  auto fetch_and_add() -> u64{
    lock.lock();
    auto res = ::xstore::util::Marshal<u64>::deserialize(mem_pool, sizeof(u64));  //首先利用deserialize()函数，获得mem_pool开头8字节的已用叶节点数量
    auto res_add = res+1;  //然后在这个基础上设置一个新变量来递增1位
    // LOG(2) << "now used "<<res_add;
    // ASSERT(res_add != 0) << "fetch_and_add, before add 1: "<<res<< ", add 1: "<<res_add;
    memcpy(mem_pool, &(res_add), sizeof(u64));    //然后将新数量递增到mem_pool的8字节元数据区
    auto read_res = ::xstore::util::Marshal<u64>::deserialize(mem_pool, sizeof(u64));  //复制完成后再来查看是否更改成功
    ASSERT(res_add == read_res) << "fetch_and_add, affer add 1: "<<res_add<< ", read_res: "<<read_res;
    //进行对比，查看是否分配成功；理论上是一致的，而如果断言失败那么会打印理论上加1后使用的叶节点数量，以及实际读到的叶节点数量
    lock.unlock();
    return res;  //这里返回的还是旧的，已经分配出去的叶节点数量
  }

};



} // namespace rolex