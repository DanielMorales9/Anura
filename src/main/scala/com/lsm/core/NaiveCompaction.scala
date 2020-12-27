package com.lsm.core

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class NaiveCompaction(dbPath: String, numSSTables: Int) extends Compaction {

  override def needsCompaction(lsm: LSMTree): Boolean = {
    lsm.sstables.size >= numSSTables
  }

  def compact(sstables: List[SSTable]): List[SSTable] = {
    // make new SSTable from old SSTables
    new SSTable(dbPath, merge(sstables)) :: Nil
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

    while (priorityQueue.nonEmpty) {
      val memNode = priorityQueue.dequeue()

      if (!memNode.thumbStone) {
        newMemTable += memNode
      }

      iterators = iterators.filter(t => t.hasNext)
      appendToPriorityQueue(iterators, priorityQueue, hashSet)
    }

    newMemTable.toList
  }

  private def appendToPriorityQueue(
      iterators: List[Iterator[MemNode]],
      priorityQueue: mutable.PriorityQueue[MemNode],
      hashSet: mutable.HashSet[String]
  ): Unit = {
    iterators.foreach(f => {
      val node = f.next()
      if (!hashSet.contains(node.key)) {
        priorityQueue.addOne(node)
        hashSet.addOne(node.key)
      }
    })
  }
}
