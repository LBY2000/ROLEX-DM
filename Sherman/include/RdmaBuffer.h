#if !defined(_RDMA_BUFFER_H_)
#define _RDMA_BUFFER_H_

#include "Common.h"

// abstract rdma registered buffer
class RdmaBuffer {

private:
  static const int kPageBufferCnt = 8;    // async, buffer safty
  static const int kSiblingBufferCnt = 8; // async, buffer safty
  static const int kCasBufferCnt = 8;     // async, buffer safty

  char *buffer;

  uint64_t *cas_buffer;
  uint64_t *unlock_buffer;
  uint64_t *zero_64bit;
  char *page_buffer;
  char *sibling_buffer;
  char *entry_buffer;

  int page_buffer_cur;
  int sibling_buffer_cur;
  int cas_buffer_cur;

  int kPageSize;

public:
  RdmaBuffer(char *buffer) {
    set_buffer(buffer);

    page_buffer_cur = 0;
    sibling_buffer_cur = 0;
    cas_buffer_cur = 0;
  }

  RdmaBuffer() = default;

  void set_buffer(char *buffer) {

    // printf("set buffer %p\n", buffer);

    kPageSize = std::max(kLeafPageSize, kInternalPageSize);//大小为2K
    this->buffer = buffer;
    cas_buffer = (uint64_t *)buffer;//cas_buffer的起始地址为buffer，且按照uint64的size大小划分了8个  
    unlock_buffer =
        (uint64_t *)((char *)cas_buffer + sizeof(uint64_t) * kCasBufferCnt);  //unlock_buffer在cas_buffer之后，往后偏移8个uint64的大小位置
    zero_64bit = (uint64_t *)((char *)unlock_buffer + sizeof(uint64_t));  //unlock_buffer大小看起来好像是uint64的大小，其后为zero_64bit
    page_buffer = (char *)zero_64bit + sizeof(uint64_t); //zero_64bit同样为一个uint64的大小
    sibling_buffer = (char *)page_buffer + kPageSize * kPageBufferCnt; //page_buffer的大小为8个KPageSize，也即16KB
    entry_buffer = (char *)sibling_buffer + kPageSize * kSiblingBufferCnt;  //同样的,sibling_buffer的大小本质上也为8个KPageSize,16KB
    *zero_64bit = 0;  //entry_buffer是在sibling_buffer之后的部分，看起来像是128K除去前面各种buffer区域的部分，大约是64KB不到的大小

    assert((char *)zero_64bit + 8 - buffer < define::kPerCoroRdmaBuf); //这里assert是什么意思??
  }

  uint64_t *get_cas_buffer() {
    cas_buffer_cur = (cas_buffer_cur + 1) % kCasBufferCnt; //cas_buffer_cur是一个int变量，而cas_buffer是一个char *变量
    return cas_buffer + cas_buffer_cur; //这里是一个指针加法，cas_buffer是uint64*的指针，其加1本质上是偏移1个uint64大小，因此
    //这里实际上是到下一个uint64的buffer区间上去
  }

  uint64_t *get_unlock_buffer() const { return unlock_buffer; }

  uint64_t *get_zero_64bit() const { return zero_64bit; }

  char *get_page_buffer() {
    page_buffer_cur = (page_buffer_cur + 1) % kPageBufferCnt;
    //这里无论是get_cas_buffer还是page_buffer，都先+1再取模，而非一开始就严格保证从0开始，
    //猜测是为了不搞特殊情况，使得这些的编程逻辑更加通用，不然还要区分是不是从0开始分配
    return page_buffer + (page_buffer_cur * kPageSize);
  }

  char *get_range_buffer() {
    return page_buffer;
  }

  char *get_sibling_buffer() {
    sibling_buffer_cur = (sibling_buffer_cur + 1) % kSiblingBufferCnt;
    return sibling_buffer + (sibling_buffer_cur * kPageSize);
  }

  char *get_entry_buffer() const { return entry_buffer; }
};

#endif // _RDMA_BUFFER_H_
