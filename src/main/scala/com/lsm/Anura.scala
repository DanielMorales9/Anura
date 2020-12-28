package com.lsm

import com.lsm.controllers.CommandController
import com.lsm.services.{
  BloomFilterService,
  CompactionTask,
  LSMService,
  StatsService
}
import org.slf4j.LoggerFactory

class Anura extends CommandController {

  private val logger = LoggerFactory.getLogger(getClass.getSimpleName)

  val lsmService: LSMService = SimpleFactory.getLSMService
  val bloomFilterService: BloomFilterService =
    SimpleFactory.getBloomFilterService
  val statsService: StatsService = SimpleFactory.getStatsService
  val compactionService: CompactionTask = SimpleFactory.getCompactionService

  // init
  compactionService.compact()
  bloomFilterService.init(lsmService.sstables)
  compactionService.start()

  def printStats(): Unit = statsService.printStats()

  def shutdown(): Unit = {
    logger.info("Shutting down")
    compactionService.terminate()
    compactionService.join()
    logger.info("Thank you")
  }

}
