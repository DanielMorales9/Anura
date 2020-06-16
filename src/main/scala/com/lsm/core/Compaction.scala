package com.lsm.core

trait Compaction {

  def needsCompaction(lsm: LSMTree): Boolean

  def compact(lsm: LSMTree): Unit
}
