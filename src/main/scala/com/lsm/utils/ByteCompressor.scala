package com.lsm.utils

import java.io.{BufferedReader, ByteArrayInputStream, ByteArrayOutputStream, InputStreamReader}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import scala.jdk.StreamConverters._


object ByteCompressor {

  private val CHARSET = "UTF-8"

  @throws[Exception]
  def compress(str: String): Array[Byte] = {
    val obj = new ByteArrayOutputStream
    val gzip = new GZIPOutputStream(obj)
    gzip.write(str.getBytes(CHARSET))
    gzip.close()
    obj.toByteArray
  }

  @throws[Exception]
  def decompress(byteArray: Array[Byte]): Array[String] = {
    val gzip = new GZIPInputStream(new ByteArrayInputStream(byteArray))
    val bufferedReader = new BufferedReader(new InputStreamReader(gzip, CHARSET))
    val stream = bufferedReader.lines().toScala(Array)
    gzip.close()
    stream
  }

}
