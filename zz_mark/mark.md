![alt text](image.png)
# p1 2024.12.19 ~ 12.22

�ܵ���˵,p1Ҫʵ��һ��buffer bool, ��������������(LRU-K Replacement Policy, Disk Scheduler, Buffer Pool Manager). 

* **LRU-K �滻���ԣ�LRU-K Replacement Policy����**
  * **���ܣ�** ���ڹ�������е�ҳ�棬���������ʱ��������Щҳ��Ӧ����̭��
  * **LRU-K ���ԣ�** LRU-K��Least Recently Used with K parameter���㷨ͨ������ÿ��ҳ��ĵ� K �����ʹ��ʱ�䣬ѡ����Щ�ڵ� K ��ʹ�ú����δ�����ʵ�ҳ������滻��
* **���̵�������Disk Scheduler����**
  * **���ܣ�** ����Э������������֮������ݴ��䣬�����д����
  * **����ԭ��** �����Ի���ع������Ķ�д�������һ���̰߳�ȫ�Ķ����У��ɺ�̨�߳��첽�شӶ����л�ȡ���󣬲����ô��̹�����ִ����Ӧ�Ķ�д������
  * **ʵ�֣�** ��Ҫȷ��������е��̰߳�ȫ������Ч�����̨�̵߳��������ڣ��Ա�֤���ݴ���Ŀɿ��Ժ�Ч�ʡ�
* **����ع�������Buffer Pool Manager����**
  * **���ܣ�** �������ڴ��й���ҳ��Ļ��棬����ҳ��Ļ�ȡ���̶���pin������̶���unpin����ˢ�£�flush���Ȳ�����
  * **����ԭ��** ����Ҫ����ĳ��ҳ��ʱ������ع��������ȼ���ҳ���Ƿ��ڻ�����У�������ڣ���ͨ�����̵������Ӵ��̼���ҳ�浽����ء�����Ҫ�ڳ��ռ�ʱ��ʹ�� LRU-K �滻����ѡ��Ҫ��̭��ҳ�档
  * **ʵ�֣�** ��Ҫά��ҳ����֡��ӳ���ϵ������ҳ��Ĺ̶���������ҳ��ǣ�������̵��������滻�������Эͬ������

## task1 2024.12.19 finish
Ҫʵ��һ��lru-k�㷨,�����û���������k��, ͬΪ����k�λ���ڵ���k��, �����û�����������ʵ�
<!-- ����˼·��ά��std::map<size_t, std::deque<frame_id_t>> distance -> frame_id_t ��������k����ΪdistanceΪ0   -->

* ����˼·: ά��  std::map<size_t, size_t> ppriority_evict_; // first_time -> frame_id
�� std::map<size_t, size_t> priority_evict_; // distance -> frame_id
��һ������С��k�εİ�ʱ�������id, �ڶ����ǵ���k�ε�
��������߸���evictbleʱ����һ�ε�ɾ�� �����µ�

## task2 2024.12.20
��startWorkerThread��ѭ����ȡ��������е�DiskRequest,����һ���߳�join

### 1.stl��std::unordered_map����ʱ �᳢�Ե�����ֵ���� ��ҪĬ�Ϲ��캯��
#### **��1��ͨ���±� `operator[]` ����Ԫ��**

```
cpp
���ƴ���
std::unordered_map<int, std::string> my_map;
my_map[1] = "value";
```

����������£�

* ��� `key=1` ��Ԫ�ز����ڣ�`operator[]` ���ȴ���һ���µļ�ֵ�� `1 -> T()`��
* ����Ҫ���� `T`��`std::string`����Ĭ�Ϲ��캯���������ʼ��ΪĬ��ֵ������ַ��� `""`����
* Ȼ���ٶ����Ĭ�Ϲ����ֵ��ֵΪ `"value"`��

�����insert, emplace(��ʾ���빹��õ�ֵ) ���� ������ָ������(����ָ�뱾����Ĭ�Ϲ��캯��)

### ʹ�� std::lock_guard ����������
* **RAII����Դ��ȡ����ʼ������**

  * `std::lock_guard` �ڴ���ʱ�Զ����� `lock()` ������
  * ������ʱ��������������Զ����� `unlock()` ������

* **�����ֶ�������**
  * `std::lock_guard` û���ṩ��ʽ�� `unlock` �������������Է�ֹ��������������ͷ�����

* **��������**
  * ����һ���򵥵���ģ�壬ͨ���ڱ���ʱ�ᱻ�Ż�Ϊ�㿪����

# p2 2024.12.24 ~ 12.30
ʵ��һ������չ��ϣ, Ҫ��ʵ���̰߳�ȫ�Ĳ�ѯ��д�롢ɾ����

����չ��ϣ��**Extendible Hashing**����һ�ֶ�̬��ϣ���������ڽ����ͳ��ϣ������Զ�̬����ʱ���ܳ��ֵ������½����⡣��ͨ��������ϣ��Ľṹ������Ҫʱ��չ������С��Ĵ�С���Ӷ��ṩ�˸�Ч�Ĳ��롢ɾ���Ͳ��Ҳ�����

�����ǹ��ڿ���չ��ϣ�Ĺؼ��㣺

### ����˼��

1. **����λ����**��\
   ����չ��ϣ���ù�ϣ�������ɵĶ�����ֵ�������ݽ��л��֡���������Щ��ϣֵ��ǰ׺���������ݵĴ洢λ�á�

2. **Ŀ¼��Ͱ�ķ���**��

   * **Ŀ¼**��Directory�����洢ָ��Ͱ��Bucket����ָ�룬ÿ��Ŀ¼���Ӧһ��Ͱ��
   * **Ͱ**��Bucket����ʵ�ʴ洢���ݵĵط��� ���ĳ��Ͱ�����װ���¸������ݣ���Ŀ¼�Ĵ�С������չ������Ƶ�����·���������ϣ��

3. **��̬��չ**��

   * ÿ��Ͱ��һ��**�ֲ���ȣ�Local Depth��**����ʾ��ǰͰʹ���˶���λ�����ֹ�ϣֵ��
   * Ŀ¼��һ��**ȫ����ȣ�Global Depth��**����ʾĿ¼�е�ǰ�����Ҫ�Ĺ�ϣλ����\
     ��Ͱ���ʱ������ѡ��**����Ͱ**����������Ҫ��չĿ¼��

## task1
�˽��ƶ����壬ʵ��һ��PageGuard�࣬���Ƕ�page�İ�װ����NewPageGuarded��ʱ����ж�/д��������������ʱ�Զ�������  

## task2
### hash
hash�ĸ�max_depth_λΪdirectoryIndex, ��local_depth_ΪBucketIndex. global_depth_ͬ���ǵ�λ��global_depth����local_depth�����, ���Զ��hash��ӳ�䵽ͬһ��bucket. 

### Header Page
�ò��ֵ�max depth�̶�����Ҫ�����������ܹ��������洢key��BucketPageλ�õ�Directory Page��Header Page�е�λ�á�����ͨ�� HashToDirecotryIndexʵ��
�ŵ㣺������չ�����ڲ�����д��

### Directory Page
global_depth: ǰglobal_dp��Ϊ��directory�е�����,��bucket��ʱ, ͨ���Ƚ�global_depth��local_depth��split  
local_depth: ͨ����global_depth�Ƚ��жϵ�ǰbucket��ָ������  

### GetSplitImageIndex 
* ����Bucket��local\_depth��2��bucket\_idx��01 
* ���ѳ�����Bucket��old\_bucket\_idx��001 new\_bucket\_idx��101

# p3 2025.1.3 ~ 

* Task1��Access Method Executors. ���� SeqScan��Insert��Delete��IndexScan �ĸ����ӡ�

* Task2��Aggregation and Join Executors. ���� Aggregation��NestedLoopJoin��NestedIndexJoin �������ӡ�

* Task3��Sort + Limit Executors and Top-N Optimization. ���� Sort��Limit��TopN �������ӣ��Լ�ʵ�ֽ� Sort + Limit �Ż�Ϊ TopN ���ӡ�and Top-N Optimization. ���� Sort��Limit��TopN �������ӣ��Լ�ʵ�ֽ� Sort + Limit �Ż�Ϊ TopN ���ӡ�

* Leaderboard Task��Ϊ Optimizer ʵ���µ��Ż����򣬰��� Hash Join��Join Reordering��Filter Push Down��Column Pruning �ȵȣ������������ sql ���ִ�е�Խ��Խ�á�

* Parser 
һ�� sql ��䣬���Ⱦ��� Parser ����һ�ó����﷨�� AST��Bustub �в����� libpg_query �⽫ sql ��� parse Ϊ AST��
* Binder
�ڵõ� AST �󣬻���Ҫ����Щ����󶨵����ݿ�ʵ���ϣ������ Binder �Ĺ���������������һ�� sql��

```sql
SELECT colA FROM table1;
```

���� `SELECT` �� `FROM` �ǹؼ��֣�`colA` �� `table1` �Ǳ�ʶ����Binder ���� AST������Щ����󶨵���Ӧ��ʵ���ϡ�ʵ���� Bustub �������ĸ��� c++ �ࡣ����ɺ󣬵õ��Ľ����һ�� Bustub ����ֱ������������������ Bustub AST��

## task1
Ҫ���ռ����� ��Catalog TableHeap

### SeqScan

12��31��