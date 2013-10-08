package bita
package util

/**
 * @author Samira Tasharofi (tasharo1@illinois.edu)
 */

import scala.collection.mutable.ArrayBuffer
import java.io.FileWriter
import sun.misc.IOUtils
import java.io.{ File, FileInputStream, FileOutputStream }
import akka.actor.ActorPath
import akka.actor.ActorRef
import scala.collection.immutable.HashSet

/**
 * Provides various functions for messages sent by external entities or system.
 */
object MessageHelper {
  import ActorPathHelper._

  /*messages sent by Gatling HTTP handler*/
  val gatlingHttpHandlerMessages = HashSet("OnHeaderWriteCompleted",
    "OnContentWriteCompleted",
    "OnStatusReceived",
    "OnHeadersReceived",
    "OnBodyPartReceived",
    "OnCompleted",
    "OnThrowable")

  val schedulerMessages = HashSet("ReceiveTimeout")

  val actorCreationMessage = "CreateRandomNameChild" // for Akka 2.0
  val newActorCreationMessage = "NullMessage" // for Akka 2.1

  val actorStopMessage = "StopChild"

  val initializeLoggerMessage = "InitializeLogger"

  val loggerInitializedMessage = "LoggerInitialized"

  def isCreationMessage(message: Any) =
    message.toString.contains(actorCreationMessage) || message.toString.contains(newActorCreationMessage)

  def isStopMessage(message: Any) =
    message.toString.contains(actorStopMessage)

  def isSystemInitializeMessage(sender: ActorRef, message: Any, receiver: ActorRef) =
    (message.toString.contains(initializeLoggerMessage) && isSystemCreatedActor(receiver.path.toString)) ||
      (message.toString.contains(loggerInitializedMessage) && isSystemCreatedActor(sender.path.toString))

  def isHttpHandlerMessage(message: Any): Boolean = {
    val messageStr = message.toString
    val hasParameter = (messageStr.indexOf("(") >= 0)
    val isObject = (messageStr.indexOf("@") >= 0)
    val messageType = if (hasParameter) messageStr.substring(0, messageStr.indexOf("("))
    else if (isObject) messageStr.substring(0, messageStr.indexOf("@"))
    else messageStr
    return gatlingHttpHandlerMessages.contains(messageType)
  }

  def isSchedulerMessage(message: Any) = schedulerMessages.contains(message.toString)

}
