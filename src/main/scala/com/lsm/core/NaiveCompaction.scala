package com.lsm.core

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class NaiveCompaction(db_path: String, numSSTables: Int) extends Compaction {

  val SSTABLES_SIZE: Int = numSSTables

  def compact(lsm: LSMTree): Unit = {

    // merge SSTables
    val newMemTable = merge(lsm.sstables)

    // make New SSTable from merged SSTable
    val sstable = new SSTable(db_path, newMemTable)

    // delete lsm.sstables
    lsm.sstables.foreach(f => f.delete())

    // set new sstables
    lsm.sstables = sstable :: Nil
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

  override def needsCompaction(lsm: LSMTree): Boolean = { lsm.sstables.size == numSSTables }
}
