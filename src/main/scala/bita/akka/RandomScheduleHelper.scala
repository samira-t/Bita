package akka.bita

/**
 * @author Michael Pradel (michael@binaervarianz.de)
 * @author Samira Tasharofi (tasharo1@illinois.edu)
 */

import akka.actor.ActorSystem
import scala.collection.mutable.Map
import scala.collection.mutable.Queue
import scala.collection.JavaConversions._
import akka.actor.ActorRef
import akka.actor.ScalaActorRef
import java.util.Random
//import scala.concurrent.duration.Duration
import akka.util.Duration
import java.util.concurrent.TimeUnit
import java.util.LinkedList
import bita.util.ActorPathHelper._
import bita.util.MessageHelper._
import bita.LogicalMessage
import akka.routing.RoutedActorRef

/**
 * Adds random delays before message delivery. 
 * The delays are chosen so that the the sender-receiver constraint is presevred.
 */
object RandomScheduleHelper {

  private var theSystem: ActorSystem = _
  private val senderReceiverToLastScheduledTime = Map[(String, String), Long]()
  private var random = new Random(System.currentTimeMillis())
  private var maxDelay = 100 // milliseconds
  private var tickDuration = -1L // milliseconds, read from config when theSystem is set
  private var minDistance = 0L // initialized when tickDuration is set
  
  def delay(receiver: ScalaActorRef, message: Any, sender: ActorRef) = synchronized {

    if (message == null)
      println(receiver+ " "+sender)
    if (theSystem != null && message != null &&

      !message.isInstanceOf[LogicalMessage] && // It is not already delayed

      !isSystemCreatedActor(receiver.path.toString) && // It is not system related messages

      /* 
       * The routers do not get messages from dispatcher; therefore, the broadcast would assign
       * the same ID to all messages if they are delayed and a logical message is created for them. 
       * Ignoring routers solves the problem.
       * */
      !receiver.isInstanceOf[RoutedActorRef] &&

      //The message is not a system message for actor creation.
      !isCreationMessage(message) &&

      !isStopMessage(message) &&
      
      // The message is not sent from the system actors. 
      // It is checked in scheduler around send to promise
      (sender == null || !isSystemCreatedActor(sender.path.toString))) {

      assert(tickDuration > 0)

      val realSenderPath = Scheduler.getActorPath(sender)
      val receiverPath = receiver.path.toString()
      val now = System.nanoTime
      val delay = computeDelay((realSenderPath, receiverPath), now)
      Duration.create(delay, TimeUnit.NANOSECONDS)
    } else null
  }

  private def computeDelay(senderReceiverPair: (String, String), now: Long) = {
    val lastScheduledTime = senderReceiverToLastScheduledTime.getOrElse(senderReceiverPair, now - tickDuration)
    val newDelay = random.nextInt(maxDelay) * 1000000
    val completeDelay = if ((now + newDelay) > (lastScheduledTime + minDistance)) newDelay else ((lastScheduledTime + minDistance) - now)
    val newScheduledTime = now + completeDelay

    //To make sure last time is less than new time
    assert(lastScheduledTime < newScheduledTime, "last time= %s is less than new time = %s".format(lastScheduledTime, newScheduledTime))

    senderReceiverToLastScheduledTime.put(senderReceiverPair, newScheduledTime)
    completeDelay
  }

  def setMaxDelay(delay:Int)= synchronized {
    maxDelay = delay
  }
  
  def maxDelayms = maxDelay
  
  def setSystem(system: ActorSystem) = synchronized {
    reset()
    theSystem = system
    tickDuration = theSystem.settings.config.getMilliseconds("akka.scheduler.tick-duration")
    minDistance = (tickDuration * 1000000 * 1.2).toLong // nanoseconds
  }

  def getSystem(): ActorSystem = synchronized {
    return theSystem
  }

  def reset() = synchronized {
    theSystem = null
    senderReceiverToLastScheduledTime.clear()
    random = new Random(System.currentTimeMillis())
  }
}
