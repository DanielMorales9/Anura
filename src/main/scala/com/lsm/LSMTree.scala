package com.lsm

import java.io._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object MemNodeOrdering extends Ordering[MemNode] {
  def compare(a: MemNode, b: MemNode): Int = a.key.compare(b.key)
}

class MemTable {

  val memTable = new AVLTree[MemNode]()(MemNodeOrdering)

  def size: Int = memTable.size

  def search(key: String): Option[MemNode] = {
    memTable.search(new MemNode(key, 0))
  }

  def += (node: MemNode): MemTable = {
    memTable += node
    this
  }

  def toList() = {
    memTable.toList
  }

}

class LSMTree(bufferSize: Int = 100, numSSTables: Int = 100, db_path: String = ".") {

  val BUFFER_SIZE: Int = bufferSize
  val NUM_SSTABLES: Int = numSSTables

  var memTable = new MemTable
  var sstables: List[SSTable] = initSSTables

  private def initSSTables: List[SSTable] = {
    val files = FileUtils.getListOfFiles(new File(db_path), Constants.SSTABLE_EXT, Constants.SPARSE_IDX_EXT)

    if (files.length == 0) {
      List.empty[SSTable]
    } else {
      files.groupBy(f => {
        f.getName.split("\\.")(0)
      })
        .map(v => new SSTable(v._2)).toList.sortBy(-_.serial)
    }
  }

  private def deleteOldSSTables(): Unit = {
    sstables.foreach(f => f.delete())
  }

  def sstablesCompaction(): Unit = {
    if (sstables.length > numSSTables) {
      val newMemTable = merge(sstables)
      val sstable = new SSTable(db_path, newMemTable)
      deleteOldSSTables()
      sstables = sstable::Nil
    }

  }

  private def appendToPriorityQueue(iterators: List[Iterator[MemNode]],
                                    priorityQueue: mutable.PriorityQueue[MemNode],
                                    hashSet: mutable.HashSet[String]): Unit = {
    iterators.foreach(f => {
      val node = f.next()
      if (!hashSet.contains(node.key)) {
        priorityQueue.addOne(node)
        hashSet.addOne(node.key)
      }
    })
  }

  private def merge(sstables: List[SSTable]): List[MemNode] = {
    /**
     * Merge SSTables together on disk
     */

    val newMemTable = ListBuffer.empty[MemNode]

    var iterators = sstables.map(s => s.iterator).filter(v => v.hasNext)

    val hashSet = new mutable.HashSet[String]()
    val priorityQueue = new mutable.PriorityQueue[MemNode]()(MemNodeOrdering)

    appendToPriorityQueue(iterators, priorityQueue, hashSet)

    while(priorityQueue.nonEmpty) {
      val memNode = priorityQueue.dequeue()

      if (!memNode.thumbStone) {
        newMemTable += memNode
      }

      iterators = iterators.filter(t => t.hasNext)
      appendToPriorityQueue(iterators, priorityQueue, hashSet)
    }

    newMemTable.toList
  }

  def searchMemory(key: String): Option[MemNode] = {
    /**
     * Searches memory for a given key.
     * Args:
     *  key (String): key to search for.
     * Result:
     *  memNode (MemNode): result of memory search, if not exists can be also Nil
     */

    // the memTable is implemented using an array instead of self-balanced tree
    memTable.search(key)
  }

  def searchDisk(key: String): Option[MemNode] = {
    /**
     * Searches disk for a given key.
     * Args:
     *  key (String): key to search for.
     */

    val result = sstables.flatMap(sstable => sstable.search(key)).take(1)

    if (result.isEmpty) {
      Option.empty
    }
    else {
      Some(result.head)
    }
  }

  def spillMemTableToDisk(): Unit = {
    /**
     * Write MemTable to Disk.
     */

    // Sort the MemTable first
    val sortedMemTable = memTable.toList()

    // create new SSTable from memTable
    val sstable = new SSTable(db_path, sortedMemTable)

    // transform MemTable to SSTable and append to list of SSTables
    val newSSTables = sstables.toBuffer
    newSSTables += sstable
    sstables = newSSTables.sortBy(-_.serial).toList

    // discard current memTable
    memTable = new MemTable
  }

  def get(key: String): Option[MemNode] = {
    /**
     * Retrieves node associated with a given key.
     * Args:
     *  key (String): key to search for.
     */

    searchMemory(key).orElse(searchDisk(key))
  }

  def put(key: String, value: Int): Unit = {
    /**
     * Places a given key and value into the lsm tree as a node.
     * Args:
     *  key (String): key to place in node.
     *  val (Int): value to place in node.
     */

    if(memTable.size == BUFFER_SIZE){
      // buffer is full and must be written to disk
      spillMemTableToDisk()
      // run compaction algorithm
      sstablesCompaction()
    }

    // put or update
    val optMemNode = searchMemory(key)
    if (optMemNode.isDefined) {
      val memNode = optMemNode.get
      memNode.setValue(value)
      memNode.setThumbStone(false)
    }
    else {
      memTable += new MemNode(key, value)
    }
  }

  def delete(key: String): Int = {
    /**
     * Deletes the node containing the given key.
     * Args:
     *  key (String): key associated with node to delete.
     */

    val optMemNode = get(key)

    if (optMemNode.isDefined) {
      val memNode = optMemNode.get
      memNode.setThumbStone(true)
      0
    } else {
      1
    }

  }
}


