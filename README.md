# Anura: Key-Value Store
powered by Scala®

A simple key-value store based on the LSM-Tree Algorithm.      

- [x] Single machine
- [ ] Master-slave replication
- [x] Single-threaded
- [ ] Multi-threaded
- [x] AVL-Tree implementation of MemTable
- [ ] Red-Black-Tree implementation of MemTable
- [x] Gzip Compression of SSTables' blocks
- [x] Bloom-Filter for fast Key lookup
- [ ] Write-Append-Log for fault-recovery
- [x] Naive Compaction
- [ ] Leveled Compaction
- [ ] Size-tiered Compaction
- [ ] LRU Cache    

# TODO
- Support for Generic Byte Array
- Adding Tests
- Benchmark test
- Profiling
- Research better compression and encoding solutions
