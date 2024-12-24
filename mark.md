# p1 2024.12.19 ~ 12.22

总的来说,p1要实现一个buffer bool, 它包含三个部分(LRU-K Replacement Policy, Disk Scheduler, Buffer Pool Manager). 

* **LRU-K 替换策略（LRU-K Replacement Policy）：**
  * **功能：** 用于管理缓冲池中的页面，当缓冲池满时，决定哪些页面应被淘汰。
  * **LRU-K 策略：** LRU-K（Least Recently Used with K parameter）算法通过跟踪每个页面的第 K 次最近使用时间，选择那些在第 K 次使用后最久未被访问的页面进行替换。
* **磁盘调度器（Disk Scheduler）：**
  * **功能：** 负责协调缓冲池与磁盘之间的数据传输，处理读写请求。
  * **工作原理：** 将来自缓冲池管理器的读写请求放入一个线程安全的队列中，由后台线程异步地从队列中获取请求，并调用磁盘管理器执行相应的读写操作。
  * **实现：** 需要确保请求队列的线程安全，并有效管理后台线程的生命周期，以保证数据传输的可靠性和效率。
* **缓冲池管理器（Buffer Pool Manager）：**
  * **功能：** 负责在内存中管理页面的缓存，处理页面的获取、固定（pin）、解固定（unpin）、刷新（flush）等操作。
  * **工作原理：** 当需要访问某个页面时，缓冲池管理器首先检查该页面是否在缓冲池中；如果不在，则通过磁盘调度器从磁盘加载页面到缓冲池。当需要腾出空间时，使用 LRU-K 替换策略选择要淘汰的页面。
  * **实现：** 需要维护页面与帧的映射关系、管理页面的固定计数和脏页标记，并与磁盘调度器和替换策略组件协同工作。

## task1 2024.12.19 finish
要实现一个lru-k算法,优先置换访问少于k次, 同为少于k次或大于等于k次, 优先置换其中最早访问的
<!-- 大体思路：维护std::map<size_t, std::deque<frame_id_t>> distance -> frame_id_t 其中少于k次视为distance为0   -->

* 大体思路: 维护  std::map<size_t, size_t> ppriority_evict_; // first_time -> frame_id
和 std::map<size_t, size_t> priority_evict_; // distance -> frame_id
第一个代表小于k次的按时间排序的id, 第二个是等于k次的
当插入或者更新evictble时将上一次的删除 加入新的

## task2 2024.12.20
在startWorkerThread中循环提取任务队列中的DiskRequest,创造一个线程join

### 1.stl如std::unordered_map插入时 会尝试调用其值类型 需要默认构造函数
#### **（1）通过下标 `operator[]` 插入元素**

```
cpp
复制代码
std::unordered_map<int, std::string> my_map;
my_map[1] = "value";
```

在这种情况下：

* 如果 `key=1` 的元素不存在，`operator[]` 会先创建一个新的键值对 `1 -> T()`。
* 这需要调用 `T`（`std::string`）的默认构造函数，将其初始化为默认值（如空字符串 `""`）。
* 然后，再对这个默认构造的值赋值为 `"value"`。

如果是insert, emplace(显示插入构造好的值) 或是 用智能指针则不用(智能指针本身有默认构造函数)

### 使用 std::lock_guard 来管理互斥锁
* **RAII（资源获取即初始化）：**

  * `std::lock_guard` 在创建时自动调用 `lock()` 加锁。
  * 在销毁时（作用域结束）自动调用 `unlock()` 解锁。

* **不可手动解锁：**
  * `std::lock_guard` 没有提供显式的 `unlock` 方法，这样可以防止因误操作而忘记释放锁。

* **轻量级：**
  * 它是一个简单的类模板，通常在编译时会被优化为零开销。