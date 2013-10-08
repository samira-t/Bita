package bita
package criteria

/**
 * @author Samira Tasharofi (tasharo1@illinois.edu)
 */
import akka.actor.ActorRef
import bita.ScheduleOptimization
import bita.ScheduleOptimization._

/**
 * Each criterion implements/overrides the methods of this trait.
 */
trait Criterion {

  import Constants._

  var name: String = _

  var logger: Logger = _
  
  /**
   * Optimizations for schedule generation. Overriding this method
   * adds/removes some optimizations.
   */
  def optimizations =  ScheduleOptimization.values


  def satisfy(t: Trace, i: Int, j: Int, k: Int = 0): Boolean

  /**
   * Measures the coverage of a list of execution traces.
   */
  def measureCoverage(traceFiles: Array[String], resultFile: String = null, detailInterval: Int = -1): Int

  /**
   * Generates schedules with the default optmization of REORDER_TAIL.
   */
  def generateSchedules(name: String, randomTracesPath: Array[String], generatedSchedulesPath: String,
    optimization: ScheduleOptimization = REORDER_TAIL): Array[String]

}

