package com.lsm.controllers

import bloomfilter.mutable.BloomFilter
import com.lsm.core.{MemNode, SSTable}

import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.collection.mutable

class BloomFilterController(
    expectedElements: Int,
    falsePositiveRate: Double
) {
  private val marker = new ReentrantReadWriteLock()
  private val readMarker = marker.readLock
  private val writeMarker = marker.writeLock

  val bloomFilter: BloomFilter[String] =
    BloomFilter[String](expectedElements, falsePositiveRate)

  def init(sstables: List[SSTable]): Unit = {
    // recovering Bloom Filter from SSTables
    // TODO improve readability
    val queue = mutable.Queue[MemNode]()
    var iterators = sstables
      .map(t => t.iterator)
      .filter(t => t.hasNext)
    iterators.foreach(f => queue.enqueue(f.next()))

    while (queue.nonEmpty) {
      val el = queue.dequeue()
      bloomFilter.add(el.key)

      iterators = iterators.filter(t => t.hasNext)
      iterators.foreach(f => queue.enqueue(f.next()))
    }
  }

  def mightContain(key: String): Boolean = {
    readMarker.lock()
    try {
      bloomFilter.mightContain(key);
    } finally {
      readMarker.unlock()
    }
  }

  def add(key: String): Unit = {
    writeMarker.lock()
    try {
      bloomFilter.add(key);
    } finally {
      writeMarker.unlock()
    }
  }

}
