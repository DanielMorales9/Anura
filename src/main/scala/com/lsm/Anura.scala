package com.lsm

import java.io.File

import bloomfilter.mutable.BloomFilter
import com.lsm.core.{LSMTree, MemNode, SSTable}
import com.lsm.utils.{Constants, FileUtils}

import scala.collection.mutable

trait CommandInterface {

  def get(key: String): Option[MemNode]
  def put(key: String, value: Int): Unit
  def delete(key: String): Int

}

class Anura(memTableSize: Int = 100,
            numSSTables: Int = 100,
            expectedElements: Int = 1000,
            falsePositiveRate: Double = 0.1,
            db_path: String = ".") extends CommandInterface {

  val MEMTABLE_SIZE: Int = memTableSize
  val SSTABLES_SIZE: Int = numSSTables

  var lsm: LSMTree = initLSMTree()
  var bloomFilter: BloomFilter[String] = initBloomFilter()


  def initLSMTree(): LSMTree = {
    new LSMTree(initSSTables, db_path)
  }

  def initBloomFilter(): BloomFilter[String] = {
    val bloomFilter = BloomFilter[String](expectedElements, falsePositiveRate)

    lsm.compaction()

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
    if(lsm.memTable.size == MEMTABLE_SIZE){
      // buffer is full and must be written to disk
      lsm.flushMemTable()

    }

    if (lsm.sstables.size == SSTABLES_SIZE) {
      lsm.compaction()
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
