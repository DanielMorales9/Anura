package com.lsm.engine

trait CommandInterface {

  def get(key: String): Option[Int]

  def put(key: String, value: Int): Unit

  def delete(key: String): Int

}
