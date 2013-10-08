package bita

/**
 * @author Samira Tasharofi (tasharo1@illinois.edu)
 */

import scala.collection.mutable.HashSet

/**
 * Constants used in various modules
 */

object Constants {

  val SYSTEM_GUARDIAN = "akka.actor.LocalActorRefProvider$SystemGuardian"
  val GUARDIAN = "akka.actor.LocalActorRefProvider$Guardian"

  val CommentPattern = "(//.*|==.*)".r

  /* for gatling */
  var HTBug = false
  var HWBug = false
  var RWBug = false
  var HW2Bug = false

}

/**
 * Different optimizations for schedule generation.
 *
 * @author Samira Tasharofi (tasharo1@illinois.edu)
 */
object ScheduleOptimization extends Enumeration {
  type ScheduleOptimization = Value

  /**
   * Different heuristics can be applied to improve schedule generation:
   *  <ul>
   *    <li><b>NONE</b> -
   *      Just cover an uncovered  pair and remove the tail</li>
   *    <li><b>REORDER_TAIL</b> -
   *      keep the tail and further schedule an uncovered pair in the
   * 	  tail starting from the beginning.</li>
   *    <li><b>MAX_INDEPENDENT_REORDER</b> -
   *      find and reorder an uncovered pair that have maximum distance
   *      in the trace. A heuristic to get closer to find the dimension of POS.
   *      This is useful for PR.</li>
   *  </ul>
   */

  val NONE, REORDER_TAIL, MAX_INDEPENDENT = Value
}

