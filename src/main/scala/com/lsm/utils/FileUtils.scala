package com.lsm.utils

import java.io.{File, FileNotFoundException}

object FileUtils {

  def getListOfFiles(dir: File, extensions: String*): Array[File] = {
    dir.listFiles
      .filter(_.isFile)
      .filter(file => extensions.exists(file.getName.endsWith(_)))
  }

  def getPath(extension: String)(files: Array[File]): String = {
    findFileByExtension(extension, files).getAbsolutePath
  }

  def findFileByExtension(extension: String, files: Array[File]): File = {
    files.find(f => f.getName.endsWith(extension)).getOrElse(throw new FileNotFoundException)
  }

}
