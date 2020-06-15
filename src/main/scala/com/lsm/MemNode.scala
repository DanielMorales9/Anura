package com.lsm

class MemNode(var key: String, var value: Int, var thumbStone: Boolean = false) {

  def setValue(value: Int): Unit = this.value = value

  def setThumbStone(thumbStone: Boolean): Unit = this.thumbStone = thumbStone

  override def toString: String = {
    String.format("%s,%d,%b", key, value, thumbStone)
  }
}
