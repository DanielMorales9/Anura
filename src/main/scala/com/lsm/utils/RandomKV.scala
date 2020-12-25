package com.lsm.utils

import java.util.Locale

class RandomKV {

  val random = new scala.util.Random()
  val upper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
  val lower: String = upper.toLowerCase(Locale.ROOT)
  val digits = "0123456789"
  val alphanum: String = upper + lower + digits

  /**
    * Generate a random string.
    */
  def nextKey(length: Int = 10): String = {
    val buf = new StringBuilder()
    for (_ <- 0 until length) {
      buf += alphanum(random.nextInt(alphanum.length))
    }
    buf.toString
  }

  def nextValue(length: Int): Int = {
    random.nextInt(length)
  }

}
