package bita

/**
 * @author Samira Tasharofi (tasharo1@illinois.edu)
 */

import akka.actor.ActorRef
import scala.collection.mutable.HashMap
import akka.dispatch.Envelope

/**
 *  Logical message is used for uniquely identifying messages.
 */
case class LogicalMessage(var message: Any, var creatorID: EventID, vc: VectorClock = null) {

  var originalCreatorID: EventID = creatorID

  def cloneByNewCreatorID(newCreatorID: EventID): LogicalMessage = {

    var clonedMessage = LogicalMessage(message, newCreatorID, vc)
    if (this.originalCreatorID == this.creatorID) {
      clonedMessage.originalCreatorID = this.creatorID
    } else {
      clonedMessage.originalCreatorID = this.originalCreatorID
      //      println("avoid changing original id twice" + this.originalCreatorID +
      //        " " + creatorID + " " + newCreatorID)
    }

    return clonedMessage
  }

  override def clone(): LogicalMessage = {
    val clonedMessage = LogicalMessage(message, creatorID, vc)
    clonedMessage.originalCreatorID = originalCreatorID
    clonedMessage
  }

}

/**
 * Provides functions for logical message creation and getting its hash code.
 */
object LogicalMessage {

  private var logger = new Logger("LogMsgObj")

  val LogicalMessagePattern = "(.*LogicalMessage\\()(.*)(,)(EventID\\(.*\\))(,)(Map\\(.*\\))(\\).*)".r
  val ActorPattern = "(.*Actor\\[)(.*)(\\].*)".r

  def parse(logicalMessageString: String): LogicalMessage = {
    logicalMessageString match {
      case LogicalMessagePattern(_, message, _, messageIDStr, _, vcMapStr, _) => {

        val messageID = EventID.parse(messageIDStr)
        val vc = VectorClock.parse(vcMapStr)
        return LogicalMessage(message, messageID, vc)

      }
    }

  }

  def getHashCode(logicalMessageString: String, traceHashCodes: Array[Int]): Int = {

    logicalMessageString match {

      case LogicalMessagePattern(_, message, _, creatorIDStr, _, vcMapStr, _) => {
        val creatorID = EventID.parse(creatorIDStr)

        // Get the message type by removing the argument value
        val hasParameter = (message.indexOf("(") >= 0)
        val isObject = (message.indexOf("@") >= 0)
        val messageType = if (hasParameter) message.substring(0, message.indexOf("("))
        else if (isObject) message.substring(0, message.indexOf("@"))
        else message

        /*
         * TODO: In the case that the message does not have parameter, we do not need to include 
         * parent hashCode.
         * In that case the parent hash code may occur many times in the trace
         */
        val parentHashCode = if (creatorID.creatorIndex >= 0) traceHashCodes(creatorID.creatorIndex) else creatorID.creatorIndex
        var logicalMessageHashString =
          if (message.contains("ReceiveTimeout") || message.contains("Heartbeat")) // they have no object parameter
            message
          else {
            var parentHashCodeString = parentHashCode.toString()
            var parentHashCodeOccurances = traceHashCodes.filter(_ == parentHashCode)
            if (parentHashCodeOccurances.length > 1) {
              parentHashCodeString += parentHashCodeOccurances.length.toString
            }
            //if (hasParameter)
            messageType + parentHashCodeString + creatorID.seqNum.toString()
            //else messageType 
          }
        logger.log(messageType + "+" + parentHashCode.toString() + "+" + creatorID.seqNum.toString() + ": ")
        //print(message + " ")
        return logicalMessageHashString.hashCode()
      }
    }
  }

}

