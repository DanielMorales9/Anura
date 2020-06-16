package com.lsm

import java.io.File

import bloomfilter.mutable.BloomFilter
import com.lsm.core.{Compaction, LSMTree, MemNode, NaiveCompaction, SSTable}
import com.lsm.utils.{Constants, FileUtils}

import scala.collection.mutable


class Anura(memTableSize: Int = 100,
            numSSTables: Int = 100,
            expectedElements: Int = 1000,
            falsePositiveRate: Double = 0.1,
            db_path: String = ".") extends CommandInterface {

  val lsm: LSMTree = initLSMTree()
  val compactor: Compaction = initCompaction()
  val bloomFilter: BloomFilter[String] = initBloomFilter()

  def initCompaction(): Compaction = {
    new NaiveCompaction(db_path, numSSTables)
  }

  def initLSMTree(): LSMTree = {
    new LSMTree(initSSTables, db_path, memTableSize)
  }

  def initBloomFilter(): BloomFilter[String] = {
    val bloomFilter = BloomFilter[String](expectedElements, falsePositiveRate)

    compactor.compact(lsm)

    // recovering Bloom Filter from SSTables
    val queue = mutable.Queue[MemNode]()
    var iterators = lsm.sstables
      .map(t => t.iterator)
      .filter(t => t.hasNext)
    iterators.foreach(f => queue.enqueue(f.next()))

    while (queue.nonEmpty) {
      val el = queue.dequeue()
      bloomFilter.add(el.key)

      iterators = iterators.filter(t => t.hasNext)
      iterators.foreach(f => queue.enqueue(f.next()))
    }

    bloomFilter
  }

  def initSSTables: List[SSTable] = {
    val file = new File(db_path)
    val files = FileUtils.getListOfFiles(file, Constants.SSTABLE_EXT, Constants.SPARSE_IDX_EXT)

    if (files.length == 0) {
      List.empty[SSTable]
    } else {
      files.groupBy(f => {
        f.getName.split("\\.")(0)
      }).map(v => new SSTable(v._2)).toList.sortBy(-_.serial)
    }
  }

  override def get(key: String): Option[MemNode] = {
    if (bloomFilter.mightContain(key)) {
      lsm.get(key)
    }
    else {
      Option.empty[MemNode]
    }
  }

  override def put(key: String, value: Int): Unit = {
    if(lsm.isFull){
      // buffer is full and must be written to disk
      lsm.flushMemTable()

    }

    if (compactor.needsCompaction(lsm)) {
      // too many sstables
      compactor.compact(lsm)
    }

    // put key-value pair to LSMTree
    lsm.put(key, value)

    // adding Key to BloomFilter
    bloomFilter.add(key)
  }

  override def delete(key: String): Int = {
    if (bloomFilter.mightContain(key)) {
      lsm.delete(key)
    }
    else {
      1
    }
  }
}
