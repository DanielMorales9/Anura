package com.lsm.controllers

import com.lsm.core.{Compaction, SSTable}

class CompactionController(
    compaction: Compaction,
    lsmCtrl: LSMController,
    intervalInMs: Int = 1000
) extends Thread {

  // handle gracefull shutdown
  this.setDaemon(true)

  def needsCompaction(): Boolean =
    compaction.needsCompaction(lsmCtrl.getLSMTree)

  def compact(): Unit = {
    val oldSSTables: List[SSTable] = lsmCtrl.duplicateSSTables()
    val newSSTables = compaction.compact(oldSSTables)
    lsmCtrl.replaceSSTables(newSSTables)
    oldSSTables.foreach(f => f.delete())
  }

  override def run(): Unit = {
    while (true) {
      if (needsCompaction()) {
        compact()
        println("Compacted")
      }
      println("WAIT Compaction")
      Thread.sleep(intervalInMs)
    }

  }
}
