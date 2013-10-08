package bita
package schedulegeneration

/**
 * @author Samira Tasharofi (tasharo1@illinois.edu)
 */

import scala.collection.mutable.ArrayBuffer
import akka.dispatch.Envelope
import scala.collection.mutable.HashSet
import akka.pattern.PromiseActorRef
import java.io.File
import java.io.FileWriter
import scala.collection.mutable.HashMap
import scala.annotation.tailrec
import ScheduleOptimization._
import criteria._
import scala.collection.SortedMap
import bita.util.MessageHelper._

/**
 * Generates schedules for PR criterion.
 */
object PRScheduleGenerator extends ScheduleGenerator {

  logger = new Logger("PRSchedGen")
  logger.enable = false

  criterion = PRCriterion
  var coveredPairs: HashSet[(Int, Int)] = _

  def generateSchedules(name: String, randomTracesPath: Array[String], generatedSchedulesPath: String,
    optimization: ScheduleOptimization = REORDER_TAIL): Array[String] = {

    logger.logLine("==================================================")
    logger.logLine("Genarting schedule for optmization: " + optimization)
    logger.logLine("==================================================")

    var randomTraces = for (traceFile <- randomTracesPath) yield Trace.parse(traceFile, true)

    coveredPairs = HashSet[(Int, Int)]()

    updateCoveredPairs(randomTraces)

    val newTraces =

      optimization match {

        case NONE => generateSchedules_NONE(randomTraces, PRCriterion)

        case REORDER_TAIL => generateSchedules_REORDER_TAIL(randomTraces)

        case MAX_INDEPENDENT => generateSchedules_MAX_INDEPENDENT(randomTraces)
      }

    var generatedScheduleFile = generatedSchedulesPath + name + "-%s-schedule.txt"

    var schedules = ArrayBuffer[String]()
    var index = 1
    for (newTrace <- newTraces) {
      var scheduleFileName = generatedScheduleFile.format(index)
      newTrace.outputTrace(scheduleFileName)
      schedules.+=(scheduleFileName)
      index += 1
    }

    return schedules.toArray
  }

  /**
   * Considers initial traces that improve coverage domain estimation.
   */
  def getUsefulTraces(randomTracesPath: Array[String]): HashMap[String, Trace] = {

    var estimatedCoveredPairs = HashSet[(Int, Int)]()
    var usefulTraces = HashMap[String, Trace]()
    for (traceFile <- randomTracesPath) {
      var trace = Trace.parse(traceFile, true) // parse the trace by filling the hash codes

      var traceCoverage = 0
      for (i <- 0 to trace.size - 1) {
        for (j <- i + 1 to trace.size - 1) {
          var eventI = trace.getEvent(i)
          var eventJ = trace.getEvent(j)
          val pair = (eventJ.hashCodeInTrace, eventI.hashCodeInTrace)
          val reversePair = (eventI.hashCodeInTrace, eventJ.hashCodeInTrace)

          if (criterion.satisfy(trace, i, j) && canBeReordered(i, j, trace) &&
            !estimatedCoveredPairs.contains(pair) && !estimatedCoveredPairs.contains(reversePair)) {
            estimatedCoveredPairs.+=(pair)
            traceCoverage += 1
          }
        }
      }
      if (traceCoverage == 0) {
        logger.logLine("The number of useful traces is %s which less than the number of real traces %s".format(usefulTraces.size, randomTracesPath.length))
        logger.logLine("Estimated covearge is: " + estimatedCoveredPairs.size)
        return usefulTraces
      } else {
        usefulTraces.+=(traceFile -> trace)
        logger.logLine("Including the trace " + traceFile + " which adds " + traceCoverage + " to the covearge")
      }
    }
    logger.logLine("All of the traces are useful")
    logger.logLine("Estimated covearge is: " + estimatedCoveredPairs.size)

    return usefulTraces
  }

  /*
 *   ************************************************************************************************
 *   ********************************   MAX_INDEPENDENT  ********************************************
 *   ************************************************************************************************
 */
  /**
   * Generates schedules while applying MAX_INDEPENDENT optimization
   */
  private def generateSchedules_MAX_INDEPENDENT(traces: Array[Trace]): ArrayBuffer[Trace] = {
    val generatedTraces: ArrayBuffer[Trace] = new ArrayBuffer[Trace]
    var estimatedTailCoverage = 0

    for (trace <- traces) {
      val length = trace.size

      /*
       * Considers the pairs as far as possible in the trace 
       * to obtain longer possible tail. i starts from the beginning and
       * j starts from end of trace.
       */
      for (i <- 0 to length - 1) {

        for (j <- length - 1 to i + 1 by -1) {
          var eventI = trace.getEvent(i)
          var eventJ = trace.getEvent(j)
          var eventIID = eventI.message.asInstanceOf[LogicalMessage].originalCreatorID
          var eventJID = eventJ.message.asInstanceOf[LogicalMessage].originalCreatorID
          val pair = (eventJ.hashCodeInTrace, eventI.hashCodeInTrace) //((eventJID, eventIID))

          if (criterion.satisfy(trace, i, j) && canBeReordered(i, j, trace) && !coveredPairs.contains(pair)) {
            logger.logLine("New Schedule " + (generatedTraces.size + 1))
            logger.logLine("Reorder %s \n and \n  %s".format(eventI, eventJ))

            var newTrace = reorderAndGenerate(i, j, trace, i - 1, true)
            var relaxedPointIndex = newTrace.getRelaxedIndex

            newTrace = reorderTail_MAX_INDEPENDENT(newTrace, i, relaxedPointIndex)
            newTrace.setRelaxedIndex(relaxedPointIndex)

            generatedTraces.+=(newTrace)
            if (generatedTraces.size == maxSchedule) {
              return generatedTraces
            }
          } else if (coveredPairs.contains(pair)) {
            logger.logLine("---- Skip generating schedule, the pairs %s are already covered".format(pair))
          }
        }
      }
    }

    return generatedTraces
  }

  /**
   * Reorders the tail while applying MAX_INDEPENDENT optimization.
   */
  private def reorderTail_MAX_INDEPENDENT(trace: Trace, startIndex: Int, tailIndex: Int): Trace = {
    val length = trace.getTrace.size

    /*
     * i starts from beginning and j starts from end of the tail because of 
     * MAX_INDEPENDENT optimization.
     */
    for (i <- tailIndex to length - 1) {
      for (j <- length - 1 to i + 1 by -1) {

        var eventI = trace.getEvent(i)
        var eventJ = trace.getEvent(j)
        var messageI = eventI.message.asInstanceOf[LogicalMessage]
        var messageJ = eventJ.message.asInstanceOf[LogicalMessage]
        val pair = (eventJ.hashCodeInTrace, eventI.hashCodeInTrace)

        if (criterion.satisfy(trace, i, j) && canBeReordered(i, j, trace) && !coveredPairs.contains(pair)) {
          logger.logLine("++++++ %s and %s are furhter reordered ".format((messageI.message + messageI.originalCreatorID.toString()),
            messageJ.message + messageJ.originalCreatorID.toString()))
          var newTrace = reorderAndGenerate(i, j, trace, i - 1, true)
          return reorderTail_MAX_INDEPENDENT(newTrace, startIndex, newTrace.getRelaxedIndex)
        }
        if (coveredPairs.contains(pair))
          logger.logLine("!!!!! The pair %s are not further reordred since they are already covered".format(pair))
      }
    }

    updateCoveredPairs(trace, startIndex, tailIndex)

    /*
     * Optimization: if reordering events in the tail does not lead to 
     * covering more uncovered orderings, cut it from the trace.
     */
    trace.cutTraceEnd(tailIndex)

    return trace
  }

  /*
 *   ************************************************************************************************
 *   ********************************   REORDER_TAIL  ***********************************************
 *   ************************************************************************************************
 */
  /**
   * Generates schedules while applying REORDER_TAIL optimization
   */
  private def generateSchedules_REORDER_TAIL(traces: Array[Trace]): ArrayBuffer[Trace] = {

    val generatedTraces: ArrayBuffer[Trace] = new ArrayBuffer[Trace]
    var estimatedTailCoverage = 0

    for (trace <- traces) {
      val length = trace.getTrace.size
      for (i <- 0 to length - 1) {
        for (j <- i + 1 to length - 1) {

          var eventI = trace.getEvent(i)
          var eventJ = trace.getEvent(j)
          var eventIID = eventI.message.asInstanceOf[LogicalMessage].originalCreatorID
          var eventJID = eventJ.message.asInstanceOf[LogicalMessage].originalCreatorID
          val pair = (eventJ.hashCodeInTrace, eventI.hashCodeInTrace)

          if (criterion.satisfy(trace, i, j) && canBeReordered(i, j, trace) && !coveredPairs.contains(pair)) {
            var newTrace = reorderAndGenerate(i, j, trace, i - 1, true)

            logger.logLine("New Schedule  %s for pair %s".format((generatedTraces.size + 1), pair))

            var relaxedPointIndex = newTrace.getRelaxedIndex

            newTrace = reorderTail_REORDER_TAIL(newTrace, i, relaxedPointIndex)

            newTrace.setRelaxedIndex(relaxedPointIndex)

            generatedTraces.+=(newTrace)

            if (generatedTraces.size == maxSchedule) {
              return generatedTraces
            }

          } else if (coveredPairs.contains(pair))
            logger.logLine("---- skip generating schedule, the pairs %s are already covered".format(pair))

        }
      }
    }
    return generatedTraces
  }

  /**
   * Reorders the tail while applying REORDER_TAIL heuristic.
   */
  private def reorderTail_REORDER_TAIL(trace: Trace, startIndex: Int, tailIndex: Int): Trace = {
    val length = trace.getTrace.size

    for (i <- tailIndex to length - 1) {
      for (j <- i + 1 to length - 1) {
        var eventI = trace.getEvent(i)
        var eventJ = trace.getEvent(j)
        var messageI = eventI.message.asInstanceOf[LogicalMessage]
        var messageJ = eventJ.message.asInstanceOf[LogicalMessage]
        val pair = (eventJ.hashCodeInTrace, eventI.hashCodeInTrace)

        if (criterion.satisfy(trace, i, j) && canBeReordered(i, j, trace) && !coveredPairs.contains(pair)) {
          logger.logLine("+++++ %s and %s are furhter reordered ".format((messageI.message + messageI.originalCreatorID.toString()),
            messageJ.message + messageJ.originalCreatorID.toString()))
          var newTrace = reorderAndGenerate(i, j, trace, i - 1, true)
          addToCoveredPairs(pair)
          return reorderTail_REORDER_TAIL(newTrace, startIndex, newTrace.getRelaxedIndex)
        } else if (coveredPairs.contains(pair))
          logger.logLine("!!!!! the pair %s are not further reordred since they are already covered".format(pair))
      }
    }

    updateCoveredPairs(trace, startIndex, tailIndex)

    /*
     * Optimization: if reordering events in the tail does not lead to 
     * covering more uncovered orderings, cut it from the trace.
     */
    trace.cutTraceEnd(tailIndex)
    return trace
  }

  /*
 *   ************************************************************************************************
 *   ********************************   NONE          ***********************************************
 *   ************************************************************************************************
 */
  /**
   * Generates schedules without applying any optimizations. Each generated schedule
   * covers one uncovered ordering.
   */
  private def generateSchedules_NONE(traces: Array[Trace], criterion: Criterion): ArrayBuffer[Trace] = {
    val generatedTraces: ArrayBuffer[Trace] = new ArrayBuffer[Trace]
    var traceCount = 0

    for (trace <- traces) {
      val length = trace.getTrace.size
      var saturated = true
      for (i <- 0 to length - 1) {
        for (j <- i + 1 to length - 1) {
          var eventI = trace.getEvent(i)
          var eventJ = trace.getEvent(j)
          var eventIID = eventI.message.asInstanceOf[LogicalMessage].originalCreatorID
          var eventJID = eventJ.message.asInstanceOf[LogicalMessage].originalCreatorID
          val pair = (eventJ.hashCodeInTrace, eventI.hashCodeInTrace) //((eventJID, eventIID))

          if (criterion.satisfy(trace, i, j) && canBeReordered(i, j, trace) && !coveredPairs.contains(pair)) {

            var newTrace = reorderAndGenerate(i, j, trace, i - 1, false)
            var relaxedPointIndex = newTrace.getRelaxedIndex

            generatedTraces.+=(newTrace)
            updateCoveredPairs(Array(newTrace), i)
            saturated = false
            traceCount += 1
            if (generatedTraces.size == maxSchedule) return generatedTraces
          } else if (coveredPairs.contains(pair))
            logger.logLine("the pair %s are already covered".format(pair))
        }
      }
    }

    return generatedTraces
  }

  private def addToCoveredPairs(pair: (Int, Int)) {
    coveredPairs.+=(pair)
  }

  private def updateCoveredPairs(traces: Array[Trace], startIndex: Int = 0): Int = {
    logger.logLine("calling update pairs")
    var lastUsefulIndex = 0

    for (trace <- traces) {
      val length = trace.size
      for (i <- startIndex to length - 1) {
        for (j <- i + 1 to length - 1) {
          var eventI = trace.getEvent(i)
          var eventJ = trace.getEvent(j)
          var eventIID = eventI.message.asInstanceOf[LogicalMessage].originalCreatorID
          var eventJID = eventJ.message.asInstanceOf[LogicalMessage].originalCreatorID
          val pair = (eventI.hashCodeInTrace, eventJ.hashCodeInTrace)
          if (criterion.satisfy(trace, i, j) && canBeReordered(i, j, trace) &&
            !coveredPairs.contains(pair)) {
            coveredPairs.+=(pair)
            lastUsefulIndex = j
          }
        }
      }
    }

    return lastUsefulIndex
  }

  private def updateCoveredPairs(trace: Trace, startIndex: Int, tailIndex: Int) {
    logger.logLine("calling update pairs")
    val length = trace.size
    for (i <- startIndex to tailIndex) {
      for (j <- i + 1 to length - 1) {

        var eventI = trace.getEvent(i)
        var eventJ = trace.getEvent(j)
        var eventIMessage = eventI.message.asInstanceOf[LogicalMessage]
        var eventJMessage = eventJ.message.asInstanceOf[LogicalMessage]

        if (j <= tailIndex || eventJMessage.creatorID.creatorIndex < tailIndex) {
          val pair =
            (eventI.hashCodeInTrace, eventJ.hashCodeInTrace)
          if (criterion.satisfy(trace, i, j) && canBeReordered(i, j, trace)) {
            coveredPairs.+=(pair)
          }
        }
      }
    }

  }

}