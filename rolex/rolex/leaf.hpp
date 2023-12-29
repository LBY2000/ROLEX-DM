#pragma once

#include <limits>
#include <iostream>
#include <optional>
#include <utility> 

#include "r2/src/common.hh"


using namespace r2;


namespace rolex {


template<usize N = 4, typename K = u64, typename V = u64>//N是大小，表示使用的KV槽数，这里默认是4，但是rolex传入参数是64，表示一个叶子64个节点
struct __attribute__((packed)) Leaf  //__attribute__((packed))是告诉编译器leaf成员紧凑排列，而不去做字节填充，从而减小结构体大小
{                                    //有些时候编译器会填充字节来对齐内存访问，从而保证对某些变量的访问可以在特定的内存边界上进行，提高访问速度
  K keys[N];
  V vals[N];
  //首先，其成员定义了keys[N]和vals[N]
  Leaf() {   //leaf对象的构造函数，将每个KV的key初始化置为无效值
    for (uint i = 0; i < N; ++i) {
      this->keys[i] = invalidKey();
    }
  }

  inline static K invalidKey() { return std::numeric_limits<K>::max(); }
  //这里，invalidkey的含义是返回某个类型下的无效key值
  //std::numeric_limits<K>::max();调用后，返回的是某个类型的最大值或者最小值，这里实际调用的是max，因此会返回一个最大值以表示无效值

  inline static usize max_slot() { return N; }
  //返回叶节点包含的slots的最大数量，也就是N
    
  bool isfull() { return keys[N-1]==invalidKey()? false:true; }
  //判断是否满,这里采用的办法就是查看叶节点的最后一个slots是否被设置为了最值，也就是无效值
  bool isEmpty() { return keys[0]==invalidKey(); }
  //判断是否是空,类似的,只要查看第一个slots是不是无效值
  K last_key() { return keys[N-1]; }
  //返回最后一的slot的key,也就是返回最后一个slot的key
  /**
   * @brief The start size of vals
   */
  static auto value_start_offset() -> usize { return offsetof(Leaf, vals); }
  //这个是C++独有的计划偏移量的方式;这里计算的是leaf结构体的vals成员的起始位置
  //然后加了->usize可能是为了明确指明返回类型，和auto的类型推断可能存在冗余?

  // ================== API functions: search, update, insert, remove ==================
  auto search(const K &key, V &val) -> bool {  //这里val是引用传递的，然后返回类型加了->bool,猜测是为了表明搜索成功还是不成功
    if(keys[0]>key) return false;  //由于是排序的，因此如果该叶节点中的最小值都比待查值大了,那么一定是不成功的
    for (uint i = 0; i < N; ++i) {   //然后在叶节点内进行顺序遍历
      if (this->keys[i] == key) {
        val = vals[i];
        return true;
      }
    }
    return false;
  }

  auto contain(const K &key) ->bool {   //这个应该是存在性判断,就是说,是否存在这个key
    if(keys[0]>key) return false;
    for (uint i = 0; i < N; ++i) {
      if (this->keys[i] == key) 
        return true;
    }
    return false;
  }

  auto update(const K &key, const V &val) -> bool { //update的过程和search类似,但是因为是修改,所以是拿新值覆盖旧值
    if(keys[0]>key) return false;   //查找逻辑也相同,先判断是不是在这个也节点内,然后如果在的话再继续查找
    for (uint i = 0; i < N; ++i) {
      if (this->keys[i] == key) {
        vals[i] = val;
        return true;
      }
    }
    return false;
  }

  // fixme: what if the leaf is empty?
  auto insertHere(const K &key) -> bool { return key>=keys[0]; }  //这个应该是判断，是不是应该插入到这个叶节点以及这个叶节点之后的slots

  /**
   * worth: guarantee to insert here by leaf_table
   *  1.not full?
   *  2.full?
   * 
   */
  // auto insert(const K &key, const V &val) -> bool {
  //   ASSERT(keys[0]<=key);
  //   for (uint i = 0; i < N; ++i) {
  //     if (this->keys[i] == key) {
  //       vals[i] = val;
  //       return true;
  //     }
  //   }
  //   return false;
  // }

  /**
   * @brief Insert data while keeping all data sorted
   *     Note: this function assume that the leaf is not full, used for init
   * 
   * @return u8 the inseted position
   */
  auto insert_not_full(const K &key, const V &val) -> u8 {  //这里猜测，采用u8类型是为了节省空间，毕竟8bits也就1字节，但是也够用了
    u8 i=0;      //这个是往还没满的叶节点进行插入操作
    for(; i<N; i++){       
      if(this->keys[i] == key) return 0;  //这个表明，虽然执行插入操作，但是已有key存在了，再插入只是更新操作
      if(this->keys[i] > key) break;      //这个表面，恰到现在的i是待插入位置的后一个位置，因为按照排序，这个地方插入待插入的key可以保证排序维持
    }
    u8 j=i;      //首先标记查找位置j，这个是从新key该插入的位置i开始
    while(j<N && this->keys[j]!=invalidKey()) j++;  //确保在叶节点大小N的范围内，并且使得后续的查找键都是有效的
    if(j>=N) return N;    //如果查找超过了叶节点范围就返回位置N，实际上这也是个无效位置，因为有效位置是0-N-1
    std::copy_backward(keys+i, keys+j, keys+j+1);   
    //这个std::copy_backward()是逆向复制函数，其实也是移动，但是好处是，先将keys+j的元素移动到keys+j+1上，然后逆向复制
    //最后会将keys+i的元素移动到keys+i+1上；这么做的好处就是，如果正向复制的话，那么keys+i先复制到keys+i+1会覆盖掉原来keys+i+1上的数据
    std::copy_backward(vals+i, vals+j, vals+j+1);
    //同理，逆向复制vals上的元素
    this->keys[i] = key;  //待腾出位置后，将元素插入到其该去的地方
    this->vals[i] = val;  //val的插入同
    return i;             //最后返回插入的位置
  }

  /**
   * @brief Remove the data and reset the empty slot as invaidKey()
   * 
   * @return std::optional<u8> the idx of the removed key in the leaf if success
   */
  auto remove(const K &key) -> bool {   //移除就是删除
    for (u8 i = 0; i < N; ++i) {
      if (this->keys[i] == key) {        //先在叶节点内查找是否存在和待删除key一样的key
        std::copy(keys+i+1, keys+N, keys+i);  //找到位置后会将该位置后的k/v全部前移一位
        std::copy(vals+i+1, vals+N, vals+i);
        keys[N-1] = invalidKey();             //因为是从i+1往后一直到叶节点结束，因此这里最后一位实际上也被前移了，可能最后一位不是无效数字
      //所以这里采用了将最后一位置为无效key的做法
        return true;
      }
    }
    return false;
  }

  void range(const K& key, const int n, std::vector<V> &r_vals) {
    int i=0;
    while(i<N && r_vals.size()<n) {   //这段代码的逻辑就是，从查找到的，大于等于待查key的位置开始，就不断向scan返回的vector中加入新元素
      if(keys[i] == invalidKey()) break;
      if(keys[i] >= key) r_vals.push_back(this->vals[i]);   //直到达到scan要求的数量或者到达叶节点末尾才会跳出循环
      i++;
    }
  }


  // ============== functions for degugging ================
  void print() {
    for(int i=0; i<N; i++) {
      std::cout<<keys[i]<<" ";     //打印函数，打印这个叶节点内的所有key
    }
    std::cout<<std::endl;
  }

  void self_check() {    //这个self_check是检查是否存在破坏排序的情况
    u8 i=N-1;
    while(keys[i]==invalidKey()) i--;   //从最后一个节点开始，查找有效的key开始的位置
    for(; i>0; i--) ASSERT(keys[i]>keys[i-1]) << "Bad Leaf!";   //然后在有效key的最后一位开始验证排序
  }


  //  ============ functions for remote machines =================
  /**
   * @brief Insert the data from the compute nodes
   *    Fixme: if need construct new leaf, we need insert existing data to the new one
   * 
   * @return std::pair<u8, u8> <the state of the insertion, the position>
   *    state: 0--> key exists,   1 --> insert here, 2--> insert here and create a new leaf,  3 --> insert to next leaf
   */
  auto insert_to_remote(const K &key, const V &val) -> std::pair<u8, u8> { //这里的pair,第一个u8应该是状态，第二个u8是待插入位置
    u8 i=0;                                                                //上面解释了各个状态的含义
    for (; i < N; ++i) {
      if (this->keys[i] == key) return std::make_pair(0, i);   // key exists
      if (this->keys[i] > key) break;                          
    }  //在上一个插入状态中，因为以及在位置i处找到了存在的key，因此返回状态<0,i>表示已经在本叶i处找到了key
    if(i>=N) return std::make_pair(3, 0); // insert to next leaf
    //这个时候，break不是在0-N-1的i中发生的，因此说明查找到尽头了还没有找到，所以是插入到相邻位置的叶节点中
    //否则，表面插入位置确实是在本叶
    u8 state = 1;
    u8 j=i;   //按照先前的break结果，跳出的位置i就是待插入的位置
    if(isfull()) {  //如果满了，那么就是状态2，也就是插入这里但是这个叶节点已满，所以需要创建一个新叶节点
      j=N-1;
      state = 2;
    } else {  //如果不满的话
      while(this->keys[j]!=invalidKey()) j++;  //跟先前的插入操作一样，找逆向复制的开始位置
    }
    ASSERT(j<N) << "Something wrong in data leaves"; //这个断言来验证叶节点有效性
    while(i!=j) {     //这个是手动的逆向复制过程
      this->keys[j] = keys[j-1];
      this->vals[j] = vals[j-1];
      j--;
    }
    this->keys[i] = key;  //插入数据
    this->vals[i] = val;
    return std::make_pair(state, 0);  //按照插入结果返回
    //这里四种状态都考虑到了
  }

};


} // namespace rolex