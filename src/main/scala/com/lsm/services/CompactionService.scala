package com.lsm.services

import com.lsm.core.{Compaction, SSTable}
import org.slf4j.LoggerFactory

class CompactionService(
    compaction: Compaction,
    lsmCtrl: LSMService,
    intervalInMs: Int = 1000
) extends Thread {

  private val logger = LoggerFactory.getLogger(getClass.getSimpleName)

  // TODO handle graceful shutdown
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
        logger.debug("Compaction Finished")
      }
      logger.debug("Compaction Check")
      Thread.sleep(intervalInMs)
    }

  }
}
