package com.lsm.services

import bloomfilter.mutable.BloomFilter
import com.lsm.core.{MemNode, SSTable}
import org.slf4j.LoggerFactory

import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.collection.mutable

class BloomFilterService(
    expectedElements: Int,
    falsePositiveRate: Double
) {

  private val logger = LoggerFactory.getLogger(getClass.getSimpleName)

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
    logger.debug("MightContain START")
    try {
      bloomFilter.mightContain(key);
    } finally {
      logger.debug("MightContain END")
      readMarker.unlock()
    }
  }

  def add(key: String): Unit = {
    writeMarker.lock()
    logger.debug("add START")
    try {
      bloomFilter.add(key);
    } finally {
      logger.debug("add END")
      writeMarker.unlock()
    }
  }

}
