package com.lsm
import com.lsm.core.NaiveCompaction
import com.lsm.services.{
  BloomFilterService,
  CompactionTask,
  LSMService,
  StatsService
}

import java.io.FileInputStream
import java.util.Properties

object SimpleFactory {

  private val prop = loadProperties()

  private val dpPath: String = getPropertyAsString("dp.path")
  private val lsmService: LSMService =
    new LSMService(dpPath, getPropertyAsInt("dp.memtable.size"))
  private val bloomFilterService: BloomFilterService =
    new BloomFilterService(
      getPropertyAsInt("bloom-filter.expected-elements"),
      getPropertyAsDouble("bloom-filter.false-positive-rate")
    )
  private val statsService = new StatsService()
  private val compactionService: CompactionTask = new CompactionTask(
    new NaiveCompaction(dpPath, getPropertyAsInt("compaction.sstable.limit")),
    lsmService
  )

  def getLSMService: LSMService = lsmService
  def getBloomFilterService: BloomFilterService = bloomFilterService
  def getStatsService: StatsService = statsService
  def getCompactionService: CompactionTask = compactionService

  private def loadProperties(): Properties = {
    val prop = new Properties(loadDefaultProperties)
    val file = getClass.getClassLoader
      .getResource(String.format("%s.properties", "anura"))
      .getFile
    prop.load(new FileInputStream(file))
    prop
  }

  private def loadDefaultProperties = {
    val defaultProps = new Properties()
    defaultProps.setProperty("dp.path", "./db")
    defaultProps.setProperty("dp.memtable.size", "100")
    defaultProps.setProperty("compaction.sstable.limit", "10")
    defaultProps.setProperty("bloom-filter.expected-elements", "1000000")
    defaultProps.setProperty("bloom-filter.false-positive-rate", "0.01")
    defaultProps
  }

  private def getPropertyAsString(key: String): String = {
    prop.getProperty(key)
  }

  private def getPropertyAsInt(key: String): Int = {
    getPropertyAsString(key).toInt
  }

  private def getPropertyAsDouble(key: String): Double = {
    getPropertyAsString(key).toDouble
  }

}
