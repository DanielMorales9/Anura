package com.lsm

import com.lsm.controllers.CommandController
import com.lsm.core._
import com.lsm.services.{
  BloomFilterService,
  CompactionService,
  LSMService,
  StatsService
}
import org.slf4j.LoggerFactory

class Anura(
    memTableSize: Int = 100,
    numSSTables: Int = 100,
    expectedElements: Int = 1000,
    falsePositiveRate: Double = 0.1,
    dpPath: String = "."
) extends CommandController {

  private val logger = LoggerFactory.getLogger(getClass.getSimpleName)

  val lsmService: LSMService = new LSMService(dpPath, memTableSize)
  val bloomFilterService: BloomFilterService =
    new BloomFilterService(expectedElements, falsePositiveRate)
  val statsService = new StatsService()
  val compactionService: CompactionService = new CompactionService(
    new NaiveCompaction(dpPath, numSSTables),
    lsmService
  )

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
