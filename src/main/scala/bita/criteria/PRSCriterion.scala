package bita
package criteria

/**
 * @author Samira Tasharofi (tasharo1@illinois.edu)
 */
import akka.actor.ActorRef
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashSet
import java.io.FileWriter
import bita.ScheduleOptimization
import bita.ScheduleOptimization._
import bita.schedulegeneration._

object PRSCriterion extends Criterion {

  name = "PRS"
  logger = new Logger(name)

  //override def optimizations = ScheduleOptimization.values.-(MAX_INDEPENDENT, REORDER_TAIL)

  def satisfy(trace: Trace, i: Int, j: Int, k: Int = 0): Boolean = {
    var ei = trace.getEvent(i)
    var ej = trace.getEvent(j)
    return ej.stop.contains(ei.receiverIDStr) || ei.stop.contains(ej.receiverIDStr)
  }

  def generateSchedules(name: String, randomTracesPath: Array[String], generatedSchedulesPath: String,
    optimization: ScheduleOptimization = REORDER_TAIL): Array[String] = {
      //TODO: fill later
    return null
  }

  def measureCoverage(traceFiles: Array[String], resultFile: String = null, detailInterval: Int = -1): Int = {
    //TODO: fill later
    return -1
  }

}