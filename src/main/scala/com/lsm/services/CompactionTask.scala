package com.lsm.services

import com.lsm.core.{Compaction, SSTable}
import org.slf4j.LoggerFactory

class CompactionTask(
    compaction: Compaction,
    lsmController: LSMService,
    intervalInMs: Int = 1000
) extends Thread {

  private val logger = LoggerFactory.getLogger(getClass.getSimpleName)
  private var finished = false
  setName("bg-compaction-task")

  def terminate(): Unit = {
    finished = true
  }

  def needsCompaction(): Boolean =
    compaction.needsCompaction(lsmController.getLSMTree)

  def compact(): Unit = {
    val oldSSTables: List[SSTable] = lsmController.duplicateSSTables()
    val newSSTables = compaction.compact(oldSSTables)
    lsmController.replaceSSTables(newSSTables)
    oldSSTables.foreach(f => f.delete())
  }

  override def run(): Unit = {
    while (!finished) {
      if (needsCompaction()) {
        compact()
        logger.debug("Compaction Finished")
      }
      logger.debug("Compaction Check")
      Thread.sleep(intervalInMs)
    }

  }
}
