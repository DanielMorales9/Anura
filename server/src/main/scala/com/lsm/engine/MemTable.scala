package com.lsm.engine

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

  def toList: List[MemNode] = {
    memTable.toList
  }

}
