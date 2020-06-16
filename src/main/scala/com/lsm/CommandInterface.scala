package com.lsm

import com.lsm.core.MemNode

trait CommandInterface {

  def get(key: String): Option[MemNode]

  def put(key: String, value: Int): Unit

  def delete(key: String): Int

}
