package bita
package util

/**
 * @author Samira Tasharofi (tasharo1@illinois.edu)
 */

import scala.collection.mutable.ArrayBuffer
import java.io.FileWriter
import java.io.{ File, FileInputStream, FileOutputStream }

/**
 * Provides various functions for managing files and directories. Specially,
 * facilitates managing trace files.
 */
object FileHelper {

  def getFiles(filesPath: String, filter: String => Boolean): Array[String] = {
    var result = ArrayBuffer[String]()
    var dir = new File(filesPath)
    if (dir.exists) {
      var files = new File(filesPath).listFiles().filter(f => filter(f.getName()))
      files = files.sortBy(f => f.lastModified())

      for (file <- files) {
        result.+=(file.getAbsolutePath())
      }
    }

    return result.toArray

  }

  def sortTracesByName(traces: Array[String], sortPattern: String): Array[String] = {
    var sortedTraces = ArrayBuffer[String]()

    // Add the random trace first
    var randomTraces = traces.filter(name => name.contains("random"))
    for (i <- 1 to randomTraces.length) {
      var randomTrace = randomTraces.filter(name => name.contains("-random%s-".format(i)))
      sortedTraces.++=(randomTrace)
    }

    var scheduleTraces = traces.filter(name => name.contains("schedule"))
    for (i <- 1 to scheduleTraces.length) {
      var trace = scheduleTraces.filter(name => name.contains(sortPattern.format(i)))
      sortedTraces.++=(trace)
    }

    // Add the remaining traces if their names do not match with sort pattern
    for (trace <- traces) {
      if (!sortedTraces.contains(trace))
        sortedTraces.+=(trace)
    }
    return sortedTraces.toArray[String]
  }

  def copyFiles(sources: Array[String], destDir: String) {
    for (source <- sources) {
      val src = new File(source)
      val name = src.getName()
      val dest = new File(destDir + name)
      new FileOutputStream(dest) getChannel () transferFrom (
        new FileInputStream(src) getChannel, 0, Long.MaxValue)
    }
  }

  def deleteFiles(filesPath: String, filter: String => Boolean) {

    val dir = new File(filesPath)
    if (dir.exists()) {
      var toBeDeletedFiles = new File(filesPath).listFiles().filter(file => filter(file.getName()))

      for (file <- toBeDeletedFiles) file.delete()
    }
  }

  def emptyDir(dirPath: String) {
    var dir = new File(dirPath)
    if (dir.exists()) {
      var files = dir.listFiles()

      for (file <- files) {
        if (!file.delete()) println("Could not delete the file")
      }
    }

  }

  def getWriter(filePath: String, append: Boolean = false): FileWriter = {

    var fileDir = new File(filePath).getParentFile()
    if (!fileDir.exists()) {
      fileDir.mkdirs()
      fileDir.setExecutable(true, true);
      fileDir.setReadable(true, true);
      fileDir.setWritable(true, true);
    }

    return new FileWriter(filePath, append)
  }

}