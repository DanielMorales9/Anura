package com.lsm

import com.lsm.utils.RandomKV

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit;

class CommandGenerator(
    iteration: Int,
    random: RandomKV,
    cli: CommandInterface
) extends Runnable {

  override def run(): Unit = {
    val key = random.nextKey()

    val j = iteration + 1
    random.nextValue(3) match {
      case 0 =>
        val opt = cli.get(key)
        if (opt.isDefined) {
          println(String.format("%s GET: %s", j, opt.get.toString))
        } else {
          println(String.format("%d GET: No Such Element", j))
        }

      case 1 =>
        val value = random.nextValue(10e6.toInt)
        cli.put(key, value)
        println(String.format("%d PUT: %s,%d", j, key, value))

      case 2 =>
        if (cli.delete(key) == 0) {
          println(String.format("%d DELETE: %s", j, key))
        } else {
          println(String.format("%d DELETE: No Such Element", j))
        }

    }
  }
}

object Main {

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
      println(String.format("%s: PUT %s,%d", f + 1, key, value))
    })
  }

  val cpuCount = 3;
  def main(args: Array[String]): Unit = {
    val transactions = 100000

    val db = new Anura(
      memTableSize = 1000,
      numSSTables = 10,
      expectedElements = (transactions * 0.70).toInt,
      falsePositiveRate = 0.001,
      db_path = "db"
    )
    val random = new RandomKV()

    val start = Instant.now()
    // initDB(random, db, 0 until 1000)

    val pool = java.util.concurrent.Executors.newFixedThreadPool(cpuCount)
    (0 until transactions).foreach(it => {
      pool.execute(new CommandGenerator(it, random, db))
    })

    pool.shutdown()
    pool.awaitTermination(10, TimeUnit.MINUTES)
    val end = Instant.now()

    val diffInSecs = ChronoUnit.SECONDS.between(start, end)

    println(String.format("Benchmark: %s seconds", diffInSecs))
    println(String.format("FALSE POSITIVE: %f", db.false_positive))
    println(String.format("EXPECTED TRUE: %d", db.expected_true))
    println(String.format("ACTUAL TRUE: %d", db.actual_true))
    println(String.format("ACTUAL FALSE: %d", db.actual_false))

  }

}
