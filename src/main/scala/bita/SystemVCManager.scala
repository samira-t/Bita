package bita

/**
 * @author Samira Tasharofi (tasharo1@illinois.edu)
 */

import java.util.concurrent.ConcurrentHashMap
import util.ActorPathHelper._

/**
 * The actor system vector clock manager that manages the vector clocks for computing
 * the happens-before relation.
 */
case class SystemVCManager() {
  import util.ActorPathHelper._

  private var vectorClocks = new ConcurrentHashMap[String, VectorClock]

  def resetClocks() {
    vectorClocks.clear()
  }
  /**
   * Updates the vector clock when an actor is created.
   */
  def updateVectorClockForCreate(parentPath: String, childPath: String): VectorClock = {
    var returnVC: VectorClock = null

    if (!vectorClocks.containsKey(childPath)) {
      val parent = getDeadLetterIfSystem(parentPath)
      if (vectorClocks.containsKey(parent)) {
        var vc = vectorClocks.get(parent)
        vc.increase(parent)
        returnVC = vc.clone()
        //Logger.logLine(vc.vc.toString(), "*** update vc for creation")

      } else {
        var newVC = new VectorClock
        newVC.increase(parent)
        vectorClocks.put(parent, newVC)
        returnVC = newVC.clone()

        //Logger.logLine(newVC.vc.toString(), "**** update new vc for creation")
      }
      var childVC = vectorClocks.get(parent).clone()
      //Logger.logLine(childVC.toString(), "*** new child vc:")
      vectorClocks.put(childPath, childVC)
    }
    return returnVC

    /*
     * else do nothing
     * Otherwise, the child and parent vc have been updated since the child sent some
     * messages in preStart method.
     */

  }
  /**
   * Updates the vector clock when a message is received.
   */
  def updateVectorClockForReceive(receiverActorPath: String, msgVC: VectorClock): VectorClock = {
    val receiver = getDeadLetterIfSystem(receiverActorPath)

    if (vectorClocks.containsKey(receiver)) {
      var vc = vectorClocks.get(receiver)
      vc.increase(receiver)
      vc.updateToMax(msgVC)
      //Logger.logLine(vc.vc.toString(), "update vc")
      return vc.clone()

    } else {
      var newVC = VectorClock()
      newVC.increase(receiver)
      newVC.updateToMax(msgVC)
      vectorClocks.put(receiver, newVC)
      //Logger.logLine(newVC.vc.toString(), "update new vc")
      return newVC.clone()
    }

  }

  /**
   * Returns vector clock of an actor.
   */
  def getVC(actorPath: String) = vectorClocks.get(actorPath)

  /**
   * Updates the vector clock when a message is sent.
   */
  def updateVectorClockForSend(senderActorPath: String): VectorClock = {
    val sender = getDeadLetterIfSystem(senderActorPath)
    if (vectorClocks.containsKey(sender)) {
      var vc = vectorClocks.get(sender)
      vc.increase(sender)
      //Logger.logLine(vc.vc.toString(), "update vc for send")
      return vc.clone()

    } else {
      var newVC = VectorClock()
      newVC.increase(sender)
      vectorClocks.put(sender, newVC)
      //Logger.logLine(newVC.vc.toString(), "update new vc for send")
      return newVC.clone()
    }

  }

  /**
   * Returns the "deadletter path" if the actor is a created by system.
   * Called by methods of this class.
   */
  private def getDeadLetterIfSystem(actorPath: String) = {
    if (isSystemCreatedActor(actorPath) || isTemp(actorPath))
      DeadLetterActorPath
    else actorPath
  }

}
