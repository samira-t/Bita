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
import bita.ScheduleOptimization._
import bita.ScheduleOptimization

import criteria._

/**
 * Generates schedules for PCR criterion.
 */
object PCRScheduleGenerator extends ScheduleGenerator {

  logger = new Logger("PCRSchedGen")
  logger.enable = false

  criterion = PCRCriterion
  var coveredPairs: HashSet[(Int, Int)] = _

  def generateSchedules(name: String, randomTracesPath: Array[String], generatedSchedulesPath: String,
    optimization: ScheduleOptimization = REORDER_TAIL): Array[String] = {

    logger.logLine("==================================================")
    logger.logLine("Genarting schedule for optmization: " + optimization)
    logger.logLine("==================================================")

    var parseWithHash = true //(randomTracesPath.length > 1)
    val randomTraces = for (traceFile <- randomTracesPath) yield Trace.parse(traceFile, parseWithHash)
    coveredPairs = HashSet[(Int, Int)]()
    updateCoverageForConsecutivePairs(randomTraces)

    val newTraces =

      optimization match {

        case NONE => generateSchedules_NONE(randomTraces)

        case REORDER_TAIL => generateSchedules_REORDER_TAIL(randomTraces)

        case _ => null

      }

    var schedules = ArrayBuffer[String]()
    if (newTraces != null) {

      var generatedScheduleFile = generatedSchedulesPath + name + "-%s-schedule.txt"

      var index = 1
      for (newTrace <- newTraces) {
        var scheduleFileName = generatedScheduleFile.format(index)
        schedules.+=(scheduleFileName)
        newTrace.outputTrace(scheduleFileName)
        index += 1
      }
    }
    return schedules.toArray
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

    val generatedTraces = ArrayBuffer[Trace]()
    val maxScheduleForEachTrace =
      if (maxSchedule > 0)
        Math.ceil(maxSchedule / traces.length)
      else -1
    for (trace <- traces) {

      if (maxSchedule > 0 && generatedTraces.size >= maxSchedule) {
        return generatedTraces
      }

      var continue = true
      var traceScheduleCount = 0
      val length = trace.size

      for (i <- 0 to length - 1 if continue) {
        var consecutive = true
        for (j <- i + 1 to length - 1 if continue) {
          var eventIID = trace.getEvent(i).message.asInstanceOf[LogicalMessage].originalCreatorID
          var eventJID = trace.getEvent(j).message.asInstanceOf[LogicalMessage].originalCreatorID
          val originalPair =
            (trace.getEvent(i).hashCodeInTrace, trace.getEvent(j).hashCodeInTrace)
          val reversepair =
            (trace.getEvent(j).hashCodeInTrace, trace.getEvent(i).hashCodeInTrace)

          if (criterion.satisfy(trace, i, j)) {

            if (canBeReordered(i, j, trace)) {

              if (!coveredPairs.contains(originalPair)) {

                if (consecutive) {
                  sys.error("******* Error in intial covearge update ")
                } else { // make them consecutive

                  var newTrace = bringCloseAndGenerate(i, j, trace, i - 1, true)
                  var relaxedPointIndex = newTrace.getRelaxedIndex
                  newTrace = reorderTail_REORDER_TAIL(newTrace, relaxedPointIndex + 1)
                  newTrace.setRelaxedIndex(relaxedPointIndex)

                  generatedTraces.+=(newTrace)
                  if (generatedTraces.size == maxSchedule) return generatedTraces

                  traceScheduleCount += 1
                  updateCoverageForConsecutivePairs(Array(newTrace))

                  logger.logLine("+++ %s: bring close %s,%s =  %s: ".format(generatedTraces.size, i, j, originalPair))
                }

              } else
                logger.logLine("---- skip generating schedule, the pairs %s are already covered".format(originalPair))

              if (coveredPairs.contains(originalPair) && !coveredPairs.contains(reversepair)) {

                var newTrace = reorderAndGenerate(i, j, trace, i - 1, true)
                var relaxedPointIndex = newTrace.getRelaxedIndex
                newTrace = reorderTail_REORDER_TAIL(newTrace, relaxedPointIndex + 1)
                newTrace.setRelaxedIndex(relaxedPointIndex)

                generatedTraces.+=(newTrace)
                if (generatedTraces.size == maxSchedule) return generatedTraces

                traceScheduleCount += 1

                logger.logLine("+++ %s : add new trace for pair %s".format(generatedTraces.size, reversepair))
              } else
                logger.logLine("---- skip generating schedule, the pairs %s are already covered".format(reversepair))
            }
            consecutive = false

          }
          continue = maxSchedule < 0 ||
            (traceScheduleCount < maxScheduleForEachTrace && generatedTraces.size < maxSchedule)

        }
      }
    }
    return generatedTraces
  }

  private def reportEstimatedCoverage {
    var coverage = 0
    for (((e1, e2)) <- coveredPairs) {
      if (coveredPairs.contains(((e2, e1))))
        coverage += 1
    }
    println("****** esimated covered pairs = " + (coverage / 2))
  }

  private def reorderTail_REORDER_TAIL(trace: Trace, tailIndex: Int): Trace = {
    val length = trace.size

    for (i <- tailIndex to length - 1) {
      var consecutive = true
      for (j <- i + 1 to length - 1) {

        var messageI = trace.getEvent(i).message.asInstanceOf[LogicalMessage]
        var messageJ = trace.getEvent(j).message.asInstanceOf[LogicalMessage]
        val originalPair = (trace.getEvent(i).hashCodeInTrace, trace.getEvent(j).hashCodeInTrace)
        val reversePair = (trace.getEvent(j).hashCodeInTrace, trace.getEvent(i).hashCodeInTrace)

        if (criterion.satisfy(trace, i, j)) {

          if (canBeReordered(i, j, trace)) {

            if (coveredPairs.contains(originalPair) && !coveredPairs.contains(reversePair)) { // add the reverse pair

              var newTrace = reorderAndGenerate(i, j, trace, i - 1, true)
              logger.logLine(" ^^%s^^ : %s".format(newTrace.getRelaxedIndex, newTrace.getEvent(newTrace.getRelaxedIndex).toString()))
              return reorderTail_REORDER_TAIL(newTrace, newTrace.getRelaxedIndex + 1)

            } else if (!coveredPairs.contains(originalPair)) { // add the pair

              var newTrace = bringCloseAndGenerate(i, j, trace, i - 1, true)
              logger.logLine(" ^^%s^^ : %s".format(newTrace.getRelaxedIndex, newTrace.getEvent(newTrace.getRelaxedIndex).toString()))
              return reorderTail_REORDER_TAIL(newTrace, newTrace.getRelaxedIndex + 1)
            } else
              logger.logLine("!!!!! the pair %s are not further reordred since they are already covered".format(reversePair))
          }

          // event after j won't be consecutive to i 
          consecutive = false
        }
      }
    }

    /*
     * Optimization: if reordering events in the tail does not lead to 
     * covering more uncovered orderings, cut it from the trace.
     */
    //This optimization for PCR criterion seems to make the results worse
    trace.cutTraceEnd(tailIndex)
    updateCoverageForConsecutivePairs(Array(trace))
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
  private def generateSchedules_NONE(traces: Array[Trace]): ArrayBuffer[Trace] = {

    val opt = NONE
    val generatedTraces: ArrayBuffer[Trace] = new ArrayBuffer[Trace]

      for (trace <- traces) {
        val length = trace.getTrace.size
        for (i <- 0 to length - 1) {
          var consecutive = true
          for (j <- i + 1 to length - 1) {

            var eventIID = trace.getEvent(i).message.asInstanceOf[LogicalMessage].originalCreatorID
            var eventJID = trace.getEvent(j).message.asInstanceOf[LogicalMessage].originalCreatorID
            val originalPair = 
              (trace.getEvent(i).hashCodeInTrace, trace.getEvent(j).hashCodeInTrace)
            val reversepair = 
              (trace.getEvent(j).hashCodeInTrace, trace.getEvent(i).hashCodeInTrace)

            if (criterion.satisfy(trace, i, j)) {
              if (canBeReordered(i, j, trace)) {

                if (!coveredPairs.contains(originalPair)) {

                  if (consecutive) {
                    sys.error("******* Error in intial covearge update ")
                  } else { // make them consecutive
                    var newTrace = bringCloseAndGenerate(i, j, trace, i - 1, false)
                    generatedTraces.+=(newTrace)
                    
                    if (generatedTraces.size == maxSchedule) return generatedTraces

                    updateCoverageForConsecutivePairs(Array(newTrace))

                    logger.logLine(" Added a new trace %s by making closer %s ".format(generatedTraces.size,
                      (eventIID, eventJID)))
                  }
                } else
                  logger.logLine("---- skip generating schedule, the pairs %s are already covered".format(originalPair))

                if (coveredPairs.contains(originalPair) && !coveredPairs.contains(reversepair)) {

                  var newTrace = reorderAndGenerate(i, j, trace, i - 1, false)

                  generatedTraces.+=(newTrace)
                  if (generatedTraces.size == maxSchedule) return generatedTraces

                  updateCoverageForConsecutivePairs(Array(newTrace))
                  logger.logLine(" Added a new trace %s by cahnging the order %s ".format(generatedTraces.size,
                    (eventIID, eventJID)))

                } else
                  logger.logLine("---- skip generating schedule, the pairs %s are already covered".format(reversepair))
              }

              consecutive = false
            }
          }
        }
      }
    return generatedTraces
  }

  private def updateCoverageForConsecutivePairs(traces: Array[Trace]) {
    for (trace <- traces) {
      val length = trace.size
      for (i <- 0 to length - 1) {
        var consecutive = true
        for (j <- i + 1 to length - 1 if (consecutive)) {
          if (criterion.satisfy(trace, i, j)) {
            var eventIID = trace.getEvent(i).message.asInstanceOf[LogicalMessage].originalCreatorID
            var eventJID = trace.getEvent(j).message.asInstanceOf[LogicalMessage].originalCreatorID
            val pair = 
              (trace.getEvent(i).hashCodeInTrace, trace.getEvent(j).hashCodeInTrace)
            coveredPairs.+=(pair)
            consecutive = false
          }
        }
      }
    }

  }

}