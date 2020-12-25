package com.lsm.controllers

import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.collection.mutable;

class StatsController() {

  private val countMap = new mutable.HashMap[String, Int]().withDefaultValue(0)

  private val marker = new ReentrantReadWriteLock()
  private val readMarker = marker.readLock
  private val writeMarker = marker.writeLock

  def upsert(key: String, value: Int): Unit = {
    writeMarker.lock()
    try {
      countMap(key) += value
    } finally {
      writeMarker.unlock()
    }
  }

  private def false_positive: Double = {
    // TODO this may cause ArithmeticException division by zero
    (1.0 * countMap("expectedTrue")) / (countMap("actualFalse") + countMap(
      "actualTrue"
    ))
  }

  def printStats() {
    readMarker.lock()
    try {
      println(String.format("FALSE POSITIVE: %f", this.false_positive))
      println(String.format("EXPECTED TRUE: %d", countMap("expectedTrue")))
      println(String.format("ACTUAL TRUE: %d", countMap("actualTrue")))
      println(String.format("ACTUAL FALSE: %d", countMap("actualFalse")))
    } finally {
      readMarker.unlock()
    }
  }

}
