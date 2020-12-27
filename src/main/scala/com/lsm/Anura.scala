package com.lsm

import com.lsm.controllers.CommandController
import com.lsm.core._
import com.lsm.services.{
  BloomFilterService,
  CompactionService,
  LSMService,
  StatsService
}

class Anura(
    memTableSize: Int = 100,
    numSSTables: Int = 100,
    expectedElements: Int = 1000,
    falsePositiveRate: Double = 0.1,
    dpPath: String = "."
) extends CommandController {

  val lsmService: LSMService = new LSMService(dpPath, memTableSize)
  val compactionService: CompactionService = new CompactionService(
    new NaiveCompaction(dpPath, numSSTables),
    lsmService
  )
  val bloomFilterService: BloomFilterService =
    new BloomFilterService(expectedElements, falsePositiveRate)
  val statsService = new StatsService()

  // init
  compactionService.compact()
  bloomFilterService.init(lsmService.sstables)
  compactionService.start()

  def printStats(): Unit = statsService.printStats()

}
