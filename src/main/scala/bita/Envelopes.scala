package bita

import akka.actor.ActorRef
import scala.collection.mutable.HashMap
import akka.dispatch.Envelope

/**
 * @author Samira Tasharofi (tasharo1@illinois.edu)
 */


/**
 *  The envelope used for synchronous message response  
 */
case class PromiseEnvelope(message: LogicalMessage, sender: ActorRef)

/** 
 * The held envelope of both synchronous and asynchronous messaging.
 */
case class HeldEnvelope(receiver: ActorRef, envelope: Envelope, promiseEnvelope: PromiseEnvelope) {

  val logicalMessage = if (envelope != null) envelope.message.asInstanceOf[LogicalMessage]
  else promiseEnvelope.message

  val sender = if (envelope != null) envelope.sender else promiseEnvelope.sender

  override def toString() = logicalMessage.message.toString + ", " +
    logicalMessage.creatorID + ", " + sender + "," + receiver
}

object HeldEnvelope {

  def apply(receiver: ActorRef, envelope: Envelope): HeldEnvelope = {
    HeldEnvelope(receiver, envelope, null)
  }

  def apply(receiver: ActorRef, promiseEnvelope: PromiseEnvelope): HeldEnvelope = {
    HeldEnvelope(receiver, null, promiseEnvelope)
  }

}

/**
 * The envelope used in the schedule.
 */
case class ScheduleEnvelope(receiver: String, logicalMessage: LogicalMessage, cmh:Boolean)
