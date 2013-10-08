package bita

/**
 * @author Samira Tasharofi (tasharo1@illinois.edu)
 */

import java.io.FileWriter

/**
 * The writer that facilitates writing to a file.
 */
case class Writer(file: String) {

  var writer =
    if (file != null) util.FileHelper.getWriter(file)
    else null

  def write(message: String) {
    if (writer != null)
      writer.write(message + "\n")
    println(message)

  }

  def close() {
    if (writer != null) {
      writer.flush()
      writer.close()

    }
  }

  def flush() {
    if (writer != null) {
      writer.flush()

    }
  }
}

/**
 * The logger that can log messages in both files and standard output.
 */
class Logger(name: String) {

  var enable = false

  private var logFileWriter: Writer = null

  def setLogFile(logFile: String) {
    logFileWriter = Writer(logFile)

  }

  def log(logMessage: String, title: String = "") {
    if (enable) {
      var message = logMessage
      if (!title.equals("")) message = name + ": " + title + "- " + message
      if (logFileWriter != null) {
        logFileWriter.write(message)
        logFileWriter.flush()
      }
      print(name + ": " + message)
    }
  }

  def logLine(logMessage: String, title: String = "") {
    log(logMessage + "\n", title)
  }

}



