package com.lsm.core

import java.io._
import java.nio.file.{Files, Paths}
import java.util.Date

import com.lsm.utils.{ByteCompressor, Constants, FileUtils}

import scala.collection.mutable.ListBuffer
import scala.io.Source

object SparseInMemoryIndexEncoder {

  def loadSparseInMemoryIndex(f: File): Array[(String, Int)] = {
    val source = Source.fromFile(f)

    val index = source
      .getLines
      .map(s => s.split(','))
      .map(s => (s(0), s(1).toInt)).toArray

    source.close

    index
  }

  def writeSparseInMemoryIndex(sparseIndexPath: String, sparseIndex: Array[(String, Int)]): Unit = {
    val file = new File(sparseIndexPath)
    val bw = new BufferedWriter(new FileWriter(file))
    val content = sparseIndex.map(line => String.format("%s,%d", line._1, line._2)).mkString("\r\n")
    bw.write(content)
    bw.close()
  }
}

class SparseInMemoryIndex extends Iterable[(String, Int)] {

  var path: String = ""
  var sparseIndex: Array[(String, Int)] = Array.empty[(String, Int)]
  var length: Int = 0

  def delete(): Unit = Files.deleteIfExists(Paths.get(path))

  def this(file: File) {
    this()
    path = file.getAbsolutePath
    sparseIndex = SparseInMemoryIndexEncoder.loadSparseInMemoryIndex(file)
    length = sparseIndex.length
  }

  def this(path: String, sparseIndex: Array[(String, Int)]) {
    this()
    this.path = path
    this.sparseIndex = sparseIndex.toArray
    this.length = sparseIndex.length
    SparseInMemoryIndexEncoder.writeSparseInMemoryIndex(path, sparseIndex)
  }

  def getKeyByIndex(mid: Int): String = sparseIndex(mid)._1
  def getOffsetByIndex(idx: Int): Int = sparseIndex(idx)._2

  def binSearch(key: String): Int = {
    var l = 0
    var r = sparseIndex.length - 1
    var ans = -1
    var mid = -1
    while (l <= r) {
      mid = (r + l) / 2
      if (getKeyByIndex(mid) <= key) {
        ans = mid
        l = mid + 1
      }
      else {
        r = mid - 1
      }
    }
    ans
  }

  override def iterator: Iterator[(String, Int)] = sparseIndex.iterator
}

class SSTable extends Iterable[MemNode] {
  /**
   * An SSTable is a Sorted Single Table that keeps data sorted by key and keys appears only once.
   *
   * Its disk and memory representation are different.
   * On disk it's a sequence of entries sorted by keys and grouped by blocks.
   * Each block having 1KB of data and compressed on disk.
   *
   * In memory, is a sparse HashTable which keeps a byte offset to a compressed file block.
   * Instead of keeping all keys, byte offset-pairs, we keep only the first key to each block.
   */

  var serial: Long = 0L
  var sstablePath: String = ""
  var sparseIndex: SparseInMemoryIndex = new SparseInMemoryIndex()

  private val CHUNK_SIZE = 50
  private val PATH_FORMAT = "%s/%s.%s"

  def this(files: Array[File]) {
    /**
     * SSTable constructor to read SSTable from disk
     */
    this()
    require(files.length == 2)

    this.serial = this.getSerial(files(0))
    this.sstablePath = this.getSSTablePath(files)
    this.sparseIndex = this.getSparseIndex(files)
  }

  def this(db_path: String, memTable: List[MemNode]) {
    /**
     * SSTable constructor to write memTable to disk
     */
    this()
    serial = new Date().getTime
    sstablePath = String.format(PATH_FORMAT, db_path, serial, Constants.SSTABLE_EXT)
    val index = writeMemTableToDisk(memTable)

    val sparseIndexPath = String.format(PATH_FORMAT, db_path, serial, Constants.SPARSE_IDX_EXT)
    sparseIndex = new SparseInMemoryIndex(sparseIndexPath, index)
  }

  def delete(): Unit = {
    sparseIndex.delete()
    Files.deleteIfExists(Paths.get(sstablePath))
  }

  def writeMemTableToDisk(memTable: List[MemNode]): Array[(String, Int)] = {
    var offset = 0
    val file = new File(sstablePath)
    val bw = new FileOutputStream(file, false)
    val index = ListBuffer.empty[(String, Int)]

    memTable.grouped(CHUNK_SIZE).foreach(lines => {
      index.addOne(lines.head.key, offset)
      val content = lines.map(line => line.toString).mkString("\r\n") + "\r\n"
      val byteArray = ByteCompressor.compress(content)
      bw.write(byteArray, 0, byteArray.length)
      offset = offset + byteArray.length
    })

    bw.close()
    index.toArray
  }

  private def getSerial(file: File): Long = {
    file.getName.split("\\.")(0).toLong
  }

  private def getSparseIndex(files: Array[File]): SparseInMemoryIndex = {
    val indexFile  = FileUtils.findFileByExtension(Constants.SPARSE_IDX_EXT, files)
    new SparseInMemoryIndex(indexFile)
  }

  private def getSSTablePath(files: Array[File]): String = FileUtils.getPath(Constants.SSTABLE_EXT)(files)

  private def getEndOffset(idx: Int): Int = {
    if (idx == sparseIndex.length) {
      getSSTableSizeInBytes
    }
    else {
      sparseIndex.getOffsetByIndex(idx)
    }
  }

  def readAndFormatBlock(arrayIndex: Int): Array[MemNode] = {
    val startOffset = sparseIndex.getOffsetByIndex(arrayIndex)
    val endOffset = getEndOffset(arrayIndex + 1)

    val len = endOffset - startOffset
    val raf = new RandomAccessFile(sstablePath, "r")
    val buffer = new Array[Byte](len)
    raf.seek(startOffset)
    raf.read(buffer, 0, len)
    val block = ByteCompressor.decompress(buffer)

    val results = block.map(v => {
      val s = v.split(",")
      new MemNode(s(0), s(1).toInt, s(2).toBoolean)
    })

    raf.close()
    results
  }

  def search(key: String): Option[MemNode] = {
    sparseIndex.binSearch(key) match {
      case -1 => Option.empty
      case arrayIndex => readAndFormatBlock(arrayIndex).find(n => n.key == key)
    }
  }

  private def getSSTableSizeInBytes: Int = {
    new File(sstablePath).length().toInt
  }

  override def iterator: Iterator[MemNode] = {
    new Iterator[MemNode]() {
      val sparseIndexIterator: Iterator[Int] = (0 until sparseIndex.length).iterator
      var blockIterator: Iterator[MemNode] = _

      def hasNext(): Boolean = {
        (if (blockIterator != null) blockIterator.hasNext else false) || sparseIndexIterator.hasNext
      }

      def next(): MemNode = {
        if (!this.hasNext()) {
          throw new NoSuchElementException
        }

        if(blockIterator == null || !blockIterator.hasNext) {
          val i = this.sparseIndexIterator.next()
          blockIterator = readAndFormatBlock(i).iterator
        }
        blockIterator.next()
      }
    }
  }
}
