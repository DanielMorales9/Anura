package com.lsm.controllers

import com.lsm.CommandInterface
import com.lsm.core.MemNode
import com.lsm.services.{BloomFilterService, LSMService, StatsService}
import org.slf4j.LoggerFactory

import java.util.concurrent.locks.ReentrantLock

abstract class CommandController extends CommandInterface {

  private val logger = LoggerFactory.getLogger(getClass.getSimpleName)

  protected val lsmService: LSMService;
  protected val bloomFilterService: BloomFilterService;
  protected val statsService: StatsService;

  private val getMarker = new ReentrantLock()
  private val putMarker = new ReentrantLock()
  private val delMarker = new ReentrantLock()

  def get(key: String): Option[MemNode] = {
    getMarker.lock()
    logger.debug("GET START")
    try {
      var opt = Option.empty[MemNode]

      if (bloomFilterService.mightContain(key)) {
        opt = lsmService.get(key)
        statsService.upsert("expectedTrue", 1)
      }

      if (opt.isEmpty) {
        statsService.upsert("actualFalse", 1)
      } else {
        statsService.upsert("actualTrue", 1)
      }

      opt
    } finally {
      logger.debug("GET END")
      getMarker.unlock()
    }
  }

  def put(key: String, value: Int): Unit = {
    putMarker.lock()
    logger.debug("PUT START")
    try {
      // TODO This might be a condition variable instead
      //  it could flushMemTable twice if two variable
      if (lsmService.isFull) {
        // buffer is full and must be written to disk
        lsmService.flushMemTable()
      }

      // put key-value pair to LSMTree
      lsmService.put(key, value)

      // adding Key to BloomFilter
      bloomFilterService.add(key)

    } finally {
      logger.debug("PUT END")
      putMarker.unlock();
    }
  }

  def delete(key: String): Int = {
    delMarker.lock()
    logger.debug("DELETE START")
    try {
      var res = 1
      if (bloomFilterService.mightContain(key)) {
        res = lsmService.delete(key)
        statsService.upsert("expectedTrue", 1)
      }

      statsService.upsert("actualTrue", 1 - res)
      statsService.upsert("actualFalse", res)

      res
    } finally {
      logger.debug("DELETE END")
      delMarker.unlock()
    }
  }

}
