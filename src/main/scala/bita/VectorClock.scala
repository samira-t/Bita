package bita

/**
 * @author Samira Tasharofi (tasharo1@illinois.edu)
 */

import akka.actor.ActorRef
import scala.collection.mutable.HashMap

object VectorClock {
  val VCMapPattern = "(.*Map\\()(.*)(\\).*)".r
  val VCPattern = "(.*)( -> )(\\d*)(.*)".r
  /**
   * Parses a string and extracts the vector clock.
   */
  def parse(vcMapString: String): VectorClock = {
    var vcObject = new VectorClock

    vcMapString match {
      case VCMapPattern(_, vcs, _) => {
        var vcArrays = vcs.split(",")
        for (vc <- vcArrays) {
          vc match {
            case VCPattern(actorPath, _, vcValue, _) => {
              vcObject.put(actorPath.trim(), vcValue.toInt)
            }
            case _ => () //empty vc
          }
        }
      }
      case _ => ()
    }
    return vcObject
  }
}

/**
 * Used for computing happens-before relation among receive events.
 */
case class VectorClock() {
  var vc = new HashMap[String, Int]
  def <=(otherVC: VectorClock): Boolean =
    {
      for ((actorRef, clock) <- vc) {
        if (!otherVC.vc.contains(actorRef) || clock > otherVC.vc.get(actorRef).get)
          return false
      }

      return true
    }

  def >=(otherVC: VectorClock): Boolean =
    {
      if (vc.isEmpty && otherVC.vc.isEmpty) return true
      for ((actorRef, clock) <- otherVC.vc) {
        if (!vc.contains(actorRef) || vc.get(actorRef).get < clock)
          return false
      }

      return true
    }

  /**
   * Updates the clock value to the maximum of the current and given clock values.
   */
  def updateToMax(otherVC: VectorClock) {
    for ((actorRef, clock) <- otherVC.vc) {
      if (!vc.contains(actorRef) || vc.get(actorRef).get < clock)
        vc.put(actorRef, clock)
    }

  }

  /**
   * Increases the clock value of an actor.
   */
  def increase(actorPath: String) {
    if (vc.contains(actorPath)) {
      var curClock = vc.get(actorPath).get
      vc.put(actorPath, curClock + 1)
    } else vc.put(actorPath, 1)

  }

  /**
   * Adds the clock value of an actor to the vector clock.
   */
  def put(actorPath: String, clockValue: Int) {
    vc.put(actorPath, clockValue)
  }

  override def clone(): VectorClock = {
    var cloneVC = new VectorClock
    cloneVC.vc = vc.clone()
    return cloneVC
  }

  override def toString(): String = {
    return vc.toString()
  }

}

