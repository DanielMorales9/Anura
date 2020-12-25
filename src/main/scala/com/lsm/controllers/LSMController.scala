package com.lsm.controllers

import com.lsm.core.{LSMTree, MemNode, SSTable}
import com.lsm.utils.{Constants, FileUtils}

import java.io.File
import java.util.concurrent.locks.ReentrantReadWriteLock

class LSMController(db_path: String, memTableSize: Int) {

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
    try {
      lsm.isFull
    } finally {
      readMarker.unlock()
    }
  }

  def sstables: List[SSTable] = {
    readMarker.lock()
    try {
      lsm.sstables
    } finally {
      readMarker.unlock()
    }
  }

  def flushMemTable(): Unit = {
    writeMarker.lock()
    try {
      lsm.flushMemTable()
    } finally {
      writeMarker.unlock()
    }
  }

  def get(key: String): Option[MemNode] = {
    readMarker.lock()
    try {
      lsm.get(key)
    } finally {
      readMarker.unlock()
    }
  }

  def put(key: String, value: Int): Unit = {
    writeMarker.lock()
    try {
      lsm.put(key, value)
    } finally {
      writeMarker.unlock()
    }
  }

  def delete(key: String): Int = {
    writeMarker.lock()
    try {
      lsm.delete(key)
    } finally {
      writeMarker.unlock()
    }
  }

}
