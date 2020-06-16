package com.lsm.core

object MemNodeOrdering extends Ordering[MemNode] {
  def compare(a: MemNode, b: MemNode): Int = a.key.compare(b.key)
}

class LSMTree(var sstables: List[SSTable], db_path: String, val memTableSize: Int) {

  var memTable = new MemTable

  def isFull: Boolean = memTable.size == memTableSize

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

  def flushMemTable(): Unit = {
    /**
     * Write MemTable to Disk.
     */

    // Sort the MemTable first
    val sortedMemTable = memTable.toList

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


