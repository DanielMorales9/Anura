package com.lsm
import java.util.Locale

import scala.util.Random

object RandomString {

  val upper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
  val lower: String = upper.toLowerCase(Locale.ROOT)
  val digits = "0123456789"
  val alphanum: String = upper + lower + digits

  /**
   * Generate a random string.
   */
  def nextString(random: scala.util.Random, length: Int): String = {
    val buf = new StringBuilder()
    for (_ <- 0 until length) {
      buf += alphanum(random.nextInt(alphanum.length))
    }
    buf.toString
  }

}

object Main {

  def generateCommand(i: Int, random: scala.util.Random, cli: CommandInterface): Any = {
    val key = getNextKey(random)

    val j = i + 1
    random.nextInt(3) match {
      case 0 =>
        val opt = cli.get(key)
        if (opt.isDefined) {
          println(String.format("%s GET: %s", j, opt.get.toString))
        }
        else {
          println(String.format("%d GET: No Such Element", j))
        }

      case 1 =>
        val value = random.nextInt(10e6.toInt)
        cli.put(key, value)
        println(String.format("%d PUT: %s,%d", j, key, value))

      case 2 =>
        if (cli.delete(key)== 0) {
          println(String.format("%d DELETE: %s", j, key))
        } else {
          println(String.format("%d DELETE: No Such Element", j))
        }

    }
  }

  private def getNextKey(random: Random): String = {
    RandomString.nextString(random, 10)
  }

  def initDB(random: scala.util.Random, cli: CommandInterface, range: Range): Unit = {
    range.foreach(f => {
      var key = getNextKey(random)
      while (key contains ",") key = getNextKey(random)
      val value = random.nextInt(10e6.toInt)
      cli.put(key, value)
      println(String.format("%s: PUT %s,%d", f+1, key, value))
    })
  }

  def main(args: Array[String]): Unit = {
    val transactions = 1000000
    val db = new Anura(
      memTableSize = 1000,
      numSSTables = 10,
      expectedElements = (transactions * 0.70).toInt,
      falsePositiveRate = 0.001,
      db_path = "db")

    // initDB(new scala.util.Random, db, 0 until 1000)

    val r = new scala.util.Random

    (0 until transactions).foreach(f => {
      generateCommand(f, r, db)
    })

    println(String.format("FALSE POSITIVE: %f", db.false_positive))
    println(String.format("EXPECTED TRUE: %d", db.expected_true))
    println(String.format("ACTUAL TRUE: %d", db.actual_true))
    println(String.format("ACTUAL FALSE: %d", db.actual_false))

  }
}
