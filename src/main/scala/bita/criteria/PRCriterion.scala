package bita.criteria

/**
 * @author Samira Tasharofi (tasharo1@illinois.edu)
 */
import akka.actor.ActorRef
import scala.collection.mutable.{ HashMap }
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashSet
import java.io.FileWriter
import bita._
import java.io.BufferedReader
import java.io.FileReader
import scala.collection.SortedMap
import scala.collection.immutable.TreeMap
import bita.ScheduleOptimization
import bita.ScheduleOptimization._
import bita.schedulegeneration._

/**
 * Pair of Receives in each actor. This is a less restrictive version of pair of PCR in
 * which the pair do not need to be consecutive in the trace.
 */
object PRCriterion extends Criterion {

  name = "PR"
  logger = new Logger(name)


  def satisfy(t: Trace, i: Int, j: Int, k: Int = 0): Boolean = {
    var ei = t.getEvent(i)
    var ej = t.getEvent(j)
    return ei.receiverIDStr == ej.receiverIDStr
  }

  def generateSchedules(name: String, randomTracesPath: Array[String], generatedSchedulesPath: String,
    optimization: ScheduleOptimization = REORDER_TAIL): Array[String] = {
    PRScheduleGenerator.generateSchedules(name, randomTracesPath, generatedSchedulesPath,
      optimization)
  }

  /**
   * Creates a file for each actor and writes the hash values of receive events
   * happened in that actor into that file.
   */

  def groupAndWriteRceivesBasedOnReceiver(traceFiles: Array[String], resultWriter: bita.Writer): (HashMap[Int, ArrayBuffer[String]], HashMap[Int, String]) = {
    var actorFiles = HashMap[Int, ArrayBuffer[String]]()
    var actorHashToFriendlyName = HashMap[Int, String]()
    var newTraceFiles = traceFiles
    for (traceFile <- newTraceFiles) {
      resultWriter.write(traceFile)
      var trace = Trace.parse(traceFile, true)
      var traceHashCodes = ArrayBuffer[Int]()

      var actorsToWriterMap = HashMap[Int, FileWriter]()
      for (i <- 0 to trace.size - 1) {
        var event = trace.getEvent(i)
        traceHashCodes.+=(event.hashCodeInTrace)

        var actorHash = LogicalActor.getHashCode(event.receiverIDStr, traceHashCodes.toArray)

        var writer =
          if (actorsToWriterMap.contains(actorHash))
            actorsToWriterMap.get(actorHash).get
          else {
            var fileName = traceFile.replace("-trace.txt", "-trace-" + event.receiverIDStr + actorHash + ".txt")
            var newWriter = util.FileHelper.getWriter(fileName)
            actorsToWriterMap.put(actorHash, newWriter)
            if (actorFiles.contains(actorHash)) {
              actorFiles.get(actorHash).get.+=(fileName)
            } else {
              var fileArray = ArrayBuffer[String]()
              fileArray.+=(fileName)
              actorFiles.put(actorHash, fileArray)
              actorHashToFriendlyName.put(actorHash, event.receiverIDStr)
            }
            newWriter
          }
        if (!event.promiseResponse)
          writer.write(event.hashCodeInTrace + "\n")
      }
      for ((actor, writer) <- actorsToWriterMap) {
        writer.close()
      }

    }
    (actorFiles, actorHashToFriendlyName)

  }

  /**
   * Measures the coverage of traces. Traces cover a pair of (r,r') if both orderings of r->r' and
   * r'->r that satisfy PR are covered by traces.
   */
  def measureCoverage(traceFiles: Array[String], resultFile: String = null, detailInterval: Int = -1): Int = {
    println("measuring the coverage for %s.... ".format(name))
    var start = System.currentTimeMillis()

    var writer = Writer(resultFile)

    // Each trace file is read and the coverage information is accumulated in an array buffer
    var actorFilesAndNames = groupAndWriteRceivesBasedOnReceiver(traceFiles, writer)
    var actorFiles = actorFilesAndNames._1
    var actorHashToName = actorFilesAndNames._2
    var totalCoverageValue = 0
    var intervalCoverage = HashMap[Int, Int]()
    var actorsCoverageValue = HashMap[String, Int]()
    var coveredPairs = HashSet[(Int, Int)]()
    var notCoveredPairs = HashSet[(Int, Int)]()

    for ((actor, files) <- actorFiles) {

      coveredPairs.clear()
      notCoveredPairs.clear()
      var traceCounter = 0

      for (file <- files) {
        traceCounter += 1
        var hashLines = ArrayBuffer[String]()
        io.Source.fromFile(file).getLines.copyToBuffer(hashLines)
        for (i <- 0 to hashLines.length - 1) {
          for (j <- i + 1 to hashLines.length - 1) {
            var pair = (hashLines(i).toInt, hashLines(j).toInt)
            var reversePair = (hashLines(j).toInt, hashLines(i).toInt)

            if (!coveredPairs.contains(pair) && !coveredPairs.contains(reversePair)) {
              var reversePairExists = notCoveredPairs.contains(reversePair)

              if (reversePairExists) {
                totalCoverageValue += 1
                coveredPairs.+=(pair)
                notCoveredPairs.-=(reversePair)

              } else {
                notCoveredPairs.+=(pair)
              }
            }
          }
        }

        if (detailInterval > 0 && (traceCounter % detailInterval == 0)) {
          if (intervalCoverage.contains(traceCounter)) {
            var curValue = intervalCoverage.get(traceCounter).get
            intervalCoverage.update(traceCounter, curValue + coveredPairs.size)
          } else {
            intervalCoverage.put(traceCounter, coveredPairs.size)
          }
        }
      }

      actorsCoverageValue.put(actorHashToName.get(actor).get, coveredPairs.size)

    }
    var end = System.currentTimeMillis()

    val sortedIntervalCoverage = TreeMap(intervalCoverage.toSeq: _*)
    sortedIntervalCoverage.toSeq.sortBy(_._1)
    for ((traceCounter, coverage) <- sortedIntervalCoverage) {
      writer.write("Total covered pairs after %s traces = %s\n ".format(traceCounter, coverage))
      writer.write("--------------------------------------- \n ")

    }

    for ((actor, coverage) <- actorsCoverageValue) {
      writer.write("*** Covered pairs for actor %s = %s".format(actor, coverage))
      writer.write("=============================================================")

    }

    writer.write("Total covered pairs (for all actors) = " + totalCoverageValue)
    writer.write("*************************************************************")

    writer.write("Coverage measurement time for %s traces: %s sec \n".format(traceFiles.size, (end - start) / 1000))

    writer.close()

    return -1
  }
}