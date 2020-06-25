package com.lsm.engine

class MemNode(var key: String, var value: Int, var tombStone: Boolean = false) {

  def setValue(value: Int): Unit = this.value = value

  def setTombStone(thumbStone: Boolean): Unit = this.tombStone = thumbStone

  override def toString: String = {
    String.format("%s,%d,%b", key, value, tombStone)
  }
}
