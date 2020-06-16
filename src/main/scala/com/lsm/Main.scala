package com.lsm
import scala.util.Random

import java.util.Locale

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

  def generateCommand(random: scala.util.Random, lsm: CommandInterface): Any = {
    val key = getNextKey(random)

    random.nextInt(3) match {
      case 0 =>
        val opt = lsm.get(key)
        if (opt.isDefined) {
          println(String.format("GET: %s", opt.get.toString))
        }
        else {
          println("GET: No Such Element")
        }

      case 1 =>
        val value = random.nextInt(10e6.toInt)
        lsm.put(key, value)
        println(String.format("PUT: %s,%d", key, value))

      case 2 =>
        if (lsm.delete(key)== 0) {
          println(String.format("DELETE: %s", key))
        } else {
          println("DELETE: No Such Element")
        }

    }
  }

  private def getNextKey(random: Random): String = {
    RandomString.nextString(random, 10)
  }

  def initDB(random: scala.util.Random, lsm: CommandInterface, range: Range): Unit = {
    range.foreach(f => {
      println(String.format("%s: INIT", f))
      var key = getNextKey(random)
      while (key contains ",") key = getNextKey(random)
      val value = random.nextInt(10e6.toInt)
      lsm.put(key, value)
    })
  }

  def main(args: Array[String]): Unit = {
    val db = new Anura(memTableSize = 1000, numSSTables = 5, db_path = "db")
    initDB(new scala.util.Random, db, 0 until 1000)

    val r = new scala.util.Random

    (0 until 100000).foreach(f => {
      println(String.format("%s: INSTRUCTION", f))
      generateCommand(r, db)
    })

  }
}
