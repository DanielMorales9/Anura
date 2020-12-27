package com.lsm.services

import com.lsm.core.{LSMTree, MemNode, SSTable}
import com.lsm.utils.{Constants, FileUtils}
import org.slf4j.LoggerFactory

import java.io.File
import java.util.concurrent.locks.ReentrantReadWriteLock

class LSMService(db_path: String, memTableSize: Int) {

  private val logger = LoggerFactory.getLogger(getClass.getSimpleName)

  val lsm: LSMTree = new LSMTree(initSSTables, db_path, memTableSize)
  private val marker = new ReentrantReadWriteLock()
  private val readMarker = marker.readLock
  private val writeMarker = marker.writeLock

  def initSSTables: List[SSTable] = {
    val file = new File(db_path)
    if (!file.exists()) file.mkdir()

    val files = FileUtils.getListOfFiles(
      file,
      Constants.SSTABLE_EXT,
      Constants.SPARSE_IDX_EXT
    )

    if (files.length == 0) {
      List.empty[SSTable]
    } else {
      files
        .groupBy(f => {
          f.getName.split("\\.")(0)
        })
        .map(v => new SSTable(v._2))
        .toList
        .sortBy(-_.serial)
    }
  }

  def isFull: Boolean = {
    readMarker.lock()
    logger.debug("isFull START")
    try {
      lsm.isFull
    } finally {
      logger.debug("isFull END")
      readMarker.unlock()
    }
  }

  def sstables: List[SSTable] = {
    readMarker.lock()
    logger.debug("sstables START")
    try {
      lsm.sstables
    } finally {
      logger.debug("sstables END")
      readMarker.unlock()
    }
  }

  def duplicateSSTables(): List[SSTable] = {
    writeMarker.lock()
    logger.debug("duplicateSSTables START")
    try {
      lsm.sstables.map(f => f)
    } finally {
      logger.debug("replaceSSTables END")
      writeMarker.unlock()
    }
  }

  def replaceSSTables(newSSTables: List[SSTable]): Unit = {
    writeMarker.lock()
    logger.debug("replaceSSTables START")
    try {
      lsm.sstables = newSSTables
    } finally {
      logger.debug("replaceSSTables END")
      writeMarker.unlock()
    }
  }

  def flushMemTable(): Unit = {
    writeMarker.lock()
    logger.debug("flushMemTable START")
    try {
      lsm.flushMemTable()
    } finally {
      logger.debug("flushMemTable END")
      writeMarker.unlock()
    }
  }

  def getLSMTree: LSMTree = {
    readMarker.lock()
    logger.debug("getLSMTree START")
    try {
      lsm
    } finally {
      logger.debug("getLSMTree END")
      readMarker.unlock()
    }
  }

  def get(key: String): Option[MemNode] = {
    readMarker.lock()
    logger.debug("get START")
    try {
      lsm.get(key)
    } finally {
      logger.debug("get END")
      readMarker.unlock()
    }
  }

  def put(key: String, value: Int): Unit = {
    writeMarker.lock()
    logger.debug("put START")
    try {
      lsm.put(key, value)
    } finally {
      logger.debug("put END")
      writeMarker.unlock()
    }
  }

  def delete(key: String): Int = {
    writeMarker.lock()
    logger.debug("delete START")
    try {
      lsm.delete(key)
    } finally {
      logger.debug("delete END")
      writeMarker.unlock()
    }
  }

}
