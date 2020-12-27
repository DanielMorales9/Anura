package com.lsm

import com.lsm.utils.RandomKV
import org.slf4j.LoggerFactory

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.{Executors, TimeUnit};

class CommandGenerator(
    iteration: Int,
    random: RandomKV,
    cli: CommandInterface
) extends Runnable {

  private val logger = LoggerFactory.getLogger(getClass.getSimpleName)

  override def run(): Unit = {
    val key = random.nextKey()

    val j = iteration + 1
    random.nextValue(3) match {
      case 0 =>
        val opt = cli.get(key)
        if (opt.isDefined) {
          logger.info(String.format("%s GET: %s", j, opt.get.toString))
        } else {
          logger.info(String.format("%d GET: No Such Element", j))
        }

      case 1 =>
        val value = random.nextValue(10e6.toInt)
        cli.put(key, value)
        logger.info(String.format("%d PUT: %s,%d", j, key, value))

      case 2 =>
        if (cli.delete(key) == 0) {
          logger.info(String.format("%d DELETE: %s", j, key))
        } else {
          logger.info(String.format("%d DELETE: No Such Element", j))
        }

    }
  }
}

object Main {

  private val logger = LoggerFactory.getLogger(getClass.getSimpleName)

  def initDB(
      random: RandomKV,
      cli: CommandInterface,
      range: Range
  ): Unit = {
    range.foreach(f => {
      var key = random.nextKey()
      while (key contains ",") key = random.nextKey()
      val value = random.nextValue(10e6.toInt)
      cli.put(key, value)
      logger.info(String.format("%s: PUT %s,%d", f + 1, key, value))
    })
  }

  val cpuCount = 3
  def main(args: Array[String]): Unit = {
    val transactions = 1000000

    val db = new Anura(
      memTableSize = 1000,
      numSSTables = 10,
      expectedElements = (transactions * 0.70).toInt,
      falsePositiveRate = 0.001,
      dpPath = "db"
    )
    val random = new RandomKV()

    val start = Instant.now()
    // initDB(random, db, 0 until 1000)

    val pool = Executors.newFixedThreadPool(cpuCount)
    (0 until transactions).foreach(it => {
      pool.execute(new CommandGenerator(it, random, db))
    })

    pool.shutdown()
    pool.awaitTermination(10, TimeUnit.MINUTES)
    val end = Instant.now()

    val diffInSecs = ChronoUnit.SECONDS.between(start, end)

    logger.info(String.format("Benchmark: %s seconds", diffInSecs))
    db.printStats()

  }

}
