package com.lsm

import com.lsm.controllers.{
  BloomFilterController,
  LSMController,
  StatsController
}
import com.lsm.core._

import java.util.concurrent.locks.ReentrantLock

class Anura(
    memTableSize: Int = 100,
    numSSTables: Int = 100,
    expectedElements: Int = 1000,
    falsePositiveRate: Double = 0.1,
    db_path: String = "."
) extends CommandInterface {

  val lsmCtrl: LSMController = new LSMController(db_path, memTableSize)
  val compactor: Compaction = new NaiveCompaction(db_path, numSSTables)
  val bloomFilterCtrl: BloomFilterController =
    new BloomFilterController(expectedElements, falsePositiveRate)
  val stats = new StatsController()
  private val marker = new ReentrantLock

  // init
  compactor.compact(lsmCtrl.lsm)
  bloomFilterCtrl.init(lsmCtrl.sstables)

  override def get(key: String): Option[MemNode] = {
    marker.lock()
    try {
      var opt = Option.empty[MemNode]

      if (bloomFilterCtrl.mightContain(key)) {
        opt = lsmCtrl.get(key)
        stats.upsert("expectedTrue", 1)
      }

      if (opt.isEmpty) {
        stats.upsert("actualFalse", 1)
      } else {
        stats.upsert("actualTrue", 1)
      }

      opt
    } finally {
      marker.unlock()
    }
  }

  override def put(key: String, value: Int): Unit = {
    marker.lock()
    try {

      if (lsmCtrl.isFull) {

        // buffer is full and must be written to disk
        lsmCtrl.flushMemTable()
      }

      // TODO needs to become a background thread
      if (compactor.needsCompaction(lsmCtrl.lsm)) {
        // too many sstables
        compactor.compact(lsmCtrl.lsm)
      }

      // put key-value pair to LSMTree
      lsmCtrl.put(key, value)

      // adding Key to BloomFilter
      bloomFilterCtrl.add(key)

    } finally {
      marker.unlock();
    }
  }

  override def delete(key: String): Int = {
    marker.lock()
    try {
      var res = 1
      if (bloomFilterCtrl.mightContain(key)) {
        res = lsmCtrl.delete(key)
        stats.upsert("expectedTrue", 1)
      }

      stats.upsert("actualTrue", 1 - res)
      stats.upsert("actualFalse", res)

      res
    } finally {
      marker.unlock()
    }
  }

  def printStats(): Unit = stats.printStats()

}
