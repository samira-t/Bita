package bita

/**
 * @author Samira Tasharofi (tasharo1@illinois.edu)
 */

/**
 *  The logical actor is used for uniquely identifying each actor.
 */
case class LogicalActor(var creatorID: EventID, var objectType: String)

/**
 * Provides functions for logical actor creation and getting its hash code.
 */
object LogicalActor {

  val DeadLetterID = LogicalActor("EntryPoint")
  val GatlingHTTPHandlerID = LogicalActor("GatlingHttpHandler")
  val SchedulerID = LogicalActor("Scheduler")

  def apply(objectType: String): LogicalActor = {
    LogicalActor(EventID(-1, -1), objectType)
  }

  def apply(creatorID: EventID): LogicalActor = {
    LogicalActor(creatorID, "")
  }

  val LogicalActorPattern = "(.*LogicalActor\\()(EventID\\(.*\\))(,)(.*)(\\))".r

  def parse(actorIDStr: String): LogicalActor = {
    actorIDStr match {
      case LogicalActorPattern(_, eventIDStr, _, objectType, _) => {
        LogicalActor(EventID.parse(eventIDStr), objectType)
      }
      case "null" => return null
    }
  }

  def getHashCode(ActorIDStr: String, traceHashCodes: Array[Int]): Int = {
    ActorIDStr match {
      case LogicalActorPattern(_, eventIDStr, _, objectType, _) => {
        val creatorID = EventID.parse(eventIDStr)
        val parentHashCode = if (creatorID.creatorIndex >= 0) traceHashCodes(creatorID.creatorIndex) else creatorID.creatorIndex

        return (objectType + parentHashCode.toString() + creatorID.seqNum.toString()).hashCode()
      }
      case "null" => return -1
    }
  }
}
