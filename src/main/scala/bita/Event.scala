package bita

/**
 * @author Samira Tasharofi (tasharo1@illinois.edu)
 */

import akka.actor.ActorRef
import scala.collection.mutable.HashSet
import scala.collection.mutable.ArrayBuffer
import util.ActorPathHelper._
import scala.collection.mutable.HashMap

/**
 * Provides functions for receive events of a trace.
 *
 * @author Samira Tasharofi (tasharo1@illinois.edu)
 */

object Event {

  private var logger = new Logger("EventObj")

  /* The pattern is used for parsing the schedule */
  val EventPattern = "(\\d+)(: <)(LogicalActor\\(.*\\)|null)(,)(LogicalMessage\\(.*\\)\\))(,)(LogicalActor\\(.*\\)|null)(,)(Map\\(.*\\)|null)(,)(true|false)(,)(true|false)(>.*)".r

  /**
   * Parses a single line of the trace file and creates its corresponding
   * receive event object.
   */
  def parse(eventStr: String): Event = {
    eventStr match {
      case EventPattern(indexStr, _, receiverStr, _, logicalMessageStr, _, senderStr, _, vcMapStr, _, promiseResponseStr, _, cmhStr, _) => {
        val message = LogicalMessage.parse(logicalMessageStr)
        val vc = VectorClock.parse(vcMapStr)
        val promiseResponse = promiseResponseStr.toBoolean
        val cmh = cmhStr.toBoolean
        return Event(indexStr.toInt, message, senderStr, receiverStr, vc, promiseResponse, cmh)
      }
      case Constants.CommentPattern(_) => return null
      case _ => println("**** ERROR! Could not parse: " + eventStr); return null
    }
  }

  /**
   * The hash value of each receive event for identifying receive events across executions.
   * It is obtained from the hash values of sender, receiver, and the message.
   *
   * Considering only the hash value of the receiver and the message would also work since
   * the sender hash value is taken into account when computing the hash value of a message.
   *
   */
  def getHashCode(eventStr: String, traceHashCodes: Array[Int]): Int = {
    eventStr match {
      case EventPattern(indexStr, _, receiverStr, _, logicalMessageStr, _, senderStr, _, vcMapStr, _, promiseResponseStr, _, cmhStr, _) => {
        val eventHashString = LogicalMessage.getHashCode(logicalMessageStr, traceHashCodes).toString() +
          LogicalActor.getHashCode(senderStr, traceHashCodes).toString() +
          LogicalActor.getHashCode(receiverStr, traceHashCodes).toString()
        logger.logLine( /*eventHashString +","+*/ eventHashString.hashCode().toString())
        return eventHashString.hashCode()
      }
      case Constants.CommentPattern(_) => return -1
      case _ => println("**** ERROR! Could not parse: " + eventStr); return -1
    }

  }

  /**
   * Receive event of the entry point.
   */
  def rootEvent = Event(-1, "", SystemGuardianActorPath, SystemGuardianActorPath, null, false)

  /**
   * Receive event that starts the actor system scheduler.
   */
  def schedulerEvent = new Event(-2, "", SystemGuardianActorPath, SystemGuardianActorPath, null, false) {

    override def increaseClock(receiver: String) {
      if (receiver != null) {
        if (receiversSeqNumMap.contains(receiver)) {
          receiversSeqNumMap.update(receiver, receiversSeqNumMap.get(receiver).get + 1)
        } else
          receiversSeqNumMap.put(receiver, 1)
      }
    }

    override def getID(receiver: String) =
      if (receiver != null)
        EventID(index, receiversSeqNumMap.get(receiver).get)
      else null

  }

  /**
   * Receive event that starts HTTP servers.
   */
  def httpEvent = Event(-3, "", SystemGuardianActorPath, SystemGuardianActorPath, null, false)

}

/**
 * The receive event class.
 */
case class Event(var index: Int, message: Any, var senderIDStr: String, var receiverIDStr: String, vc: VectorClock, promiseResponse: Boolean, var cmh: Boolean = false) {
  val create = ArrayBuffer[String]()
  val stop = HashSet[String]()
  var createSequenceNum = 0
  var hashCodeInTrace: Int = -1
  /*
   * It keeps a sequence number for each receiver actor that receives the messages created by this
   * event. It is used for generating
   * the ID of the events created by this event.
   */
  var receiversSeqNumMap = HashMap[String, Int]()

  /* Increases the sequence number of event for the receiver path. */
  def increaseClock(receiverPath: String) {
    if (receiverPath != null) {
      if (receiversSeqNumMap.contains(receiverPath)) {
        val currClock = receiversSeqNumMap.get(receiverPath).get + 1
        receiversSeqNumMap.update(receiverPath, currClock)
        assert(receiversSeqNumMap.get(receiverPath).get == currClock)
      } else
        receiversSeqNumMap.put(receiverPath, 1)
    } else /* If the path is null, then increases the creation sequence number. */
      createSequenceNum += 1
  }

  def getID(receiverPath: String) = {
    if (receiverPath != null)
      EventID(index, receiversSeqNumMap.get(receiverPath).get)
    else EventID(index, createSequenceNum)

  }

  /**
   * Returns the ID of the receive event that sends the message of this receive event.
   */
  def getCreatorID = message.asInstanceOf[LogicalMessage].creatorID

  /**
   * Clones the event but replaces the creatorID, sender, and receiver of the event with the
   * information provided in its arguments.
   */
  def cloneEvent(messageCreatorID: EventID, newSenderStr: String, newReceiverStr: String): Event = {
    val logicalMessage = message.asInstanceOf[LogicalMessage]
    val newEvent = Event(index, logicalMessage.cloneByNewCreatorID(messageCreatorID), newSenderStr, newReceiverStr, vc, promiseResponse)
    newEvent.hashCodeInTrace = hashCodeInTrace
    newEvent.stop.++=(stop)
    newEvent.cmh = cmh
    return newEvent
  }

  override def toString(): String = {
    var newMessage = message.asInstanceOf[LogicalMessage]
    newMessage.message = newMessage.message.toString.replace("\n", "").replace(",", ";")
    index + ": <" + receiverIDStr + "," + newMessage + "," + senderIDStr + "," + vc + "," + promiseResponse + "," + cmh + ">"
  }

  override def clone(): Event = {
    val clonedEvent = Event(index, message, senderIDStr, receiverIDStr, vc, promiseResponse)
    clonedEvent.hashCodeInTrace = hashCodeInTrace
    clonedEvent.cmh = cmh
    clonedEvent
  }
}

/**
 * The ID of receive event which is used when forcing a schedule. Contains the message creator index and the sequence
 * number. It is an alternate light weight solution for identifying receive events in a trace.
 * The hash code solution is used for identifying receive events across different traces.
 */
case class EventID(creatorIndex: Int, seqNum: Int) {
  def ==(otherID: EventID): Boolean = {
    (this.creatorIndex == otherID.creatorIndex && this.seqNum == otherID.seqNum)
  }

  override def hashCode(): Int = {
    (creatorIndex.toString() + seqNum.toString()).hashCode()
  }
}

/**
 * The ID of the receive event
 */
object EventID {
  val EventIDPattern = "(.*EventID\\()(-?\\d*)(,)(-?\\d*)(\\)*)".r

  def parse(eventIDStr: String): EventID = {
    eventIDStr match {
      case EventIDPattern(_, index, _, seq, _) => {
        EventID(index.toInt, seq.toInt)
      }
    }
  }

  def getHashCode(eventIDStr: String): Int = {
    eventIDStr match {
      case EventIDPattern(_, index, _, seq, _) => {
        return (index + seq).hashCode()
      }
    }
  }
}
