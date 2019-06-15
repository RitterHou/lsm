# lsm

### Theory

1. 在内存中用SkipList进行存储（内存表：memtable）
2. 内存中的数据到达一定大小后有序的写到磁盘上（Sorted String Table 简称SSTable）
3. 磁盘上的数据大小达到了一定的阈值，触发一次归并排序
4. 由于数据存储是有序的，所以我们只需要维护一个稀疏的key到offset的索引即可
5. 每一次内存中的写入都需要在translog中追加一条数据，防止进程崩溃导致内存中的数据丢失，由于日志信息是顺序追加写入到磁盘上，所以效率很高；当内存中的指定数据被写到磁盘上之后，对应的日志信息就可以删掉了

参考：<https://github.com/Vonng/ddia/blob/master/ch3.md#sstables%E5%92%8Clsm%E6%A0%91>

### Contribute

下载依赖

    go mod download
    go mod vendor

