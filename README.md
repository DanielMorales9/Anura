# Anura: Key-Value Store
powered by Scala®

A simple key-value store based on the LSM-Tree Algorithm.    
This repo has an educational purpose, 
and it is meant as a place to improve my knowledge on database design.  

- [x] Single machine
- [ ] Master-slave replication
- [x] Single-threaded
- [ ] Multi-threaded
- [x] AVL-Tree implementation of MemTable
- [ ] Red-Black-Tree implementation of MemTable
- [x] Gzip Compression of SSTables' blocks
- [ ] Bloom-Filter for fast Key lookup
- [ ] Write-Append-Log for fault-recovery
- [ ] Leveled Compaction
- [ ] Size-tiered Compaction
 
The only value that the datastore currently supports is integer.    
Benchmark test and profiling are still ongoing.     
