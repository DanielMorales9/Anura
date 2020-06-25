package com.lsm.engine

trait Compaction {

  def needsCompaction(lsm: LSMTree): Boolean

  def compact(lsm: LSMTree): Unit
}
