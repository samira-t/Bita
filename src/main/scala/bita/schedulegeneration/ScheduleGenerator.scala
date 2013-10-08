package bita
package schedulegeneration

/**
 * @author Samira Tasharofi (tasharo1@illinois.edu)
 */

import scala.collection.mutable.ArrayBuffer
import akka.dispatch.Envelope
import scala.collection.mutable.HashSet
import akka.pattern.PromiseActorRef
import java.io.File
import java.io.FileWriter
import scala.collection.mutable.HashMap
import scala.annotation.tailrec
import bita.ScheduleOptimization._

import criteria._

trait ScheduleGenerator {

  protected var logger: Logger = _

  protected var criterion: Criterion = _


  /**
   * Limits the number of generated schedules. 
   * It is useful when the number of schedules is huge.
   */
  var maxSchedule: Int = -1

  def generateSchedules(name: String, randomTracesPath: Array[String], 
      generatedSchedulesPath: String, optimization: ScheduleOptimization=REORDER_TAIL): Array[String]

  /**
   * This method checks for other constraints such as constraints imposed by 
   * synchronous messaging and sender-receiver (FIFO) constraints.
   */
  private def haveOtherReorderingConstraints(i: Int, j: Int, trace: Trace): Boolean = {

    val eventI = trace.getEvent(i)
    val eventJ = trace.getEvent(j)
    return (haveFIFOConstraint(i, j, trace) || eventI.promiseResponse || eventJ.promiseResponse)
  }

  private def haveFIFOConstraint(i: Int, j: Int, trace: Trace): Boolean = {

    val eventI = trace.getEvent(i)
    val eventJ = trace.getEvent(j)

    val receiverI = eventI.receiverIDStr
    val receiverJ = eventJ.receiverIDStr
    var senderI = eventI.senderIDStr
    var senderJ = eventJ.senderIDStr

    return ((senderI == senderJ && receiverI == receiverJ))
  }

  /**
   * Uses vector clocks to determine whether two receives are casually related.
   */
  private def isCausallyRelated(i: Int, j: Int, trace: Trace): Boolean = {

    val eventI = trace.getEvent(i)
    val eventJ = trace.getEvent(j)

    val receiverI = eventI.receiverIDStr
    val receiverJ = eventJ.receiverIDStr
    var senderI = eventI.senderIDStr
    var senderJ = eventJ.senderIDStr

    var logicalMessageJ = trace.getEvent(j).message.asInstanceOf[LogicalMessage]
    var creatorIndex = logicalMessageJ.creatorID.creatorIndex

    if (creatorIndex == i) return true
    else {
      if (i == j || logicalMessageJ.vc >= eventI.vc) return true
    }

    return false
  }

  /**
   * Determines whether two receives can be reordered in a feasible schedule. 
   */
  protected def canBeReordered(i: Int, j: Int, t: Trace): Boolean = {
    return !isCausallyRelated(i, j, t) && !haveOtherReorderingConstraints(i, j, t)
  }

  /**
   * Generates a new schedule by reordering events at i and j. 
   */
  protected def reorderAndGenerate(i: Int, j: Int, originalTrace: Trace, prefixIndex: Int, keepTail: Boolean): Trace = {
    val generatedTrace: Trace = new Trace
    val indexMap = new HashMap[Int, Int]

    copyPrefix(originalTrace, generatedTrace, prefixIndex)

    val noCausallyRelatedIJ = new ArrayBuffer[Int]()

    var mustHappenBeforeI = getMustHappenBeforeEvents(i, originalTrace, prefixIndex + 1)
    var mustHappenBeforeJ = getMustHappenBeforeEvents(j, originalTrace, prefixIndex + 1)

    for (k <- prefixIndex + 1 to i - 1) {
      if (mustHappenBeforeI.contains(k) || mustHappenBeforeJ.contains(k)) {
        val event = originalTrace.getEvent(k)
        addChangedEvent(event, generatedTrace, indexMap)
      } else {
        noCausallyRelatedIJ.+=(k)
      }
    }

    for (k <- i + 1 to j - 1) {
      if ((mustHappenBeforeJ.contains(k) && !isCausallyRelated(i, k, originalTrace))) {
        val event = originalTrace.getEvent(k)
        addChangedEvent(event, generatedTrace, indexMap)
      } else if (!isCausallyRelated(i, k, originalTrace)) {
        noCausallyRelatedIJ.+=(k)
      }
    }

    val eventI = originalTrace.getEvent(i)
    val eventJ = originalTrace.getEvent(j)
    addChangedEvent(eventJ, generatedTrace, indexMap)
    generatedTrace.setRelaxedIndex(generatedTrace.size)
    addChangedEvent(eventI, generatedTrace, indexMap)

    if (keepTail)
      copyTail(originalTrace, generatedTrace, i, j, noCausallyRelatedIJ, indexMap)

    updateCreatorIndex(indexMap, generatedTrace)

    return generatedTrace
  }

  /**
   * Generates a new schedule by making two events at i and j consecutive.
   */
  protected def bringCloseAndGenerate(i: Int, j: Int, originalTrace: Trace, prefixIndex: Int, keepTail: Boolean): Trace = {
    val generatedTrace: Trace = new Trace
    val indexMap = new HashMap[Int, Int]

    copyPrefix(originalTrace, generatedTrace, prefixIndex)

    val noCausallyRelatedIJ = new ArrayBuffer[Int]()

    var mustHappenBeforeI = getMustHappenBeforeEvents(i, originalTrace, prefixIndex + 1)
    var mustHappenBeforeJ = getMustHappenBeforeEvents(j, originalTrace, prefixIndex + 1)

    for (k <- prefixIndex + 1 to i - 1) {
      if (mustHappenBeforeI.contains(k) || mustHappenBeforeJ.contains(k)) {
        val event = originalTrace.getEvent(k)
        addChangedEvent(event, generatedTrace, indexMap)
      } else {
        noCausallyRelatedIJ.+=(k)
      }
    }

    // Copy events that must happen before event J
    for (k <- i + 1 to j - 1) {
      if (mustHappenBeforeJ.contains(k) && !isCausallyRelated(i, k, originalTrace)) {
        val event = originalTrace.getEvent(k)
        addChangedEvent(event, generatedTrace, indexMap)
      } else if (!isCausallyRelated(i, k, originalTrace)) {
        noCausallyRelatedIJ.+=(k)
      }
    }

    // Now copy events at I and then J
    val eventI = originalTrace.getEvent(i)
    addChangedEvent(eventI, generatedTrace, indexMap)
    
    generatedTrace.setRelaxedIndex(generatedTrace.size)

    val eventJ = originalTrace.getEvent(j)
    addChangedEvent(eventJ, generatedTrace, indexMap)

    if (keepTail) copyTail(originalTrace, generatedTrace, i, j, noCausallyRelatedIJ, indexMap)

    updateCreatorIndex(indexMap, generatedTrace)

    return generatedTrace
  }

  /*
   * Checks for three constraints: causality, sync messaging, and FIFO
   */
  private def getMustHappenBeforeEvents(eventIndex: Int, trace: Trace, startIndex: Int): HashSet[Int] = {
    import util.ActorPathHelper._

    var mustHappenBeforeEvents = HashSet[Int]()
    var eventCreatorIndex = trace.getEvent(eventIndex).getCreatorID.creatorIndex

    //Causally related to event at <code>eventIndex</code> 
    for (i <- startIndex to eventIndex - 1) {
      if (isCausallyRelated(i, eventIndex, trace)) {
        mustHappenBeforeEvents.+=(i)
        logger.logLine("Causally related: added %s for index %s ".format(i, eventIndex))
      }
    }

    // Preserving the FIFO constraints
    for (i <- startIndex to eventIndex - 1) {
      if (haveFIFOConstraint(i, eventIndex, trace)) {
        var creatorIndex = trace.getEvent(i).getCreatorID.creatorIndex
        if (creatorIndex == eventCreatorIndex ||
          (creatorIndex < startIndex && creatorIndex < eventCreatorIndex)) {
          mustHappenBeforeEvents.+=(i)
          logger.logLine("FIFO: added %s for index %s ".format(i, eventIndex))
        }
      }
    }

    for (i <- startIndex to eventIndex - 1) {
      var event = trace.getEvent(i)

      //Checking for casually related to current events added because of FIFO constraints
      var causuallyRelatedToCurrentEvents = false
      for (fifoIndex <- mustHappenBeforeEvents if !causuallyRelatedToCurrentEvents) {
        if (isCausallyRelated(i, fifoIndex, trace))
          causuallyRelatedToCurrentEvents = true
      }

      if ( causuallyRelatedToCurrentEvents) {
        mustHappenBeforeEvents.+=(i)
      } else if (event.promiseResponse) { //Synchronous messaging constraints
        var promiseCount = 1
        var creatorIndex = event.message.asInstanceOf[LogicalMessage].creatorID.creatorIndex
        var creatorEvent = trace.getEvent(creatorIndex)

        // find the last asynchronous receive event that causes the message sent synchronously
        while (promiseCount > 0 && creatorIndex >= startIndex) {
          if (creatorEvent.promiseResponse) promiseCount += 1
          else promiseCount -= 1
          creatorIndex = creatorEvent.message.asInstanceOf[LogicalMessage].creatorID.creatorIndex
          creatorEvent = trace.getEvent(creatorIndex)
        }

        // synchronous messaging is a part of the added events
        if (mustHappenBeforeEvents.contains(creatorIndex)) { 
          for (k <- startIndex to i - 1) {
            if (isCausallyRelated(k, i, trace))
              mustHappenBeforeEvents.+=(k)
          }
          mustHappenBeforeEvents.+=(i)
        }
      }
    }

    return mustHappenBeforeEvents

  }

  private def copyPrefix(originalTrace: Trace, generatedTrace: Trace, i: Int) {
    for (k <- 0 to i) {
      val event = originalTrace.getEvent(k).clone()
      generatedTrace.addEventAndUpdateIndex(event)
    }

  }

  /**
   * Copies any event after i and j which are caused by i and j.
   */
  private def copyTail(originalTrace: Trace, generatedTrace: Trace, i: Int, j: Int, noCausallyRelated: ArrayBuffer[Int],
    indexMap: HashMap[Int, Int]) {

    for (k <- noCausallyRelated) {
      val event = originalTrace.getEvent(k)
      addChangedEvent(event, generatedTrace, indexMap)
    }

    for (k <- j + 1 to originalTrace.size - 1) {
      val event = originalTrace.getEvent(k)
      if (!event.receiverIDStr.contains(Constants.GUARDIAN) &&
        !event.senderIDStr.contains(Constants.SYSTEM_GUARDIAN)) {
        if (!isCausallyRelated(i, k, originalTrace) && !isCausallyRelated(j, k, originalTrace)) {
          addChangedEvent(event, generatedTrace, indexMap)
        }
      }
    }

  }

  /**
   * Updates the creator index of actors and messages after they are reordered. 
   */
  private def updateCreatorIndex(indexMap: HashMap[Int, Int], trace: Trace) {
    val updatedGeneratedTrace = new ArrayBuffer[Event]
    val unUpdatedGeneratedTrace = trace.getTrace
    for (event <- unUpdatedGeneratedTrace) {

      // update message creator index
      var msgCreatorID = event.message.asInstanceOf[LogicalMessage].creatorID
      var newMsgCreatorID =
        if (indexMap.get(msgCreatorID.creatorIndex) != None) {
          EventID(indexMap.get(msgCreatorID.creatorIndex).get, msgCreatorID.seqNum)
        } else {
          EventID(msgCreatorID.creatorIndex, msgCreatorID.seqNum)
        }

      // update the receiver creator index
      var receiverActor = LogicalActor.parse(event.receiverIDStr)
      var newReceiverStr =
        if (indexMap.get(receiverActor.creatorID.creatorIndex) != None) {
          var newIndex = indexMap.get(receiverActor.creatorID.creatorIndex).get
          receiverActor.creatorID = EventID(newIndex, receiverActor.creatorID.seqNum)
          receiverActor.toString
        } else event.receiverIDStr

      //update the sender creatorIndex
      var senderActor = LogicalActor.parse(event.senderIDStr)
      var newSenderStr =
        if (indexMap.get(senderActor.creatorID.creatorIndex) != None) {
          var newIndex = indexMap.get(senderActor.creatorID.creatorIndex).get
          senderActor.creatorID = EventID(newIndex, senderActor.creatorID.seqNum)
          senderActor.toString
        } else event.senderIDStr

      updatedGeneratedTrace.+=(event.cloneEvent(newMsgCreatorID, newSenderStr, newReceiverStr))
    }

    // apply updates to the new trace
    unUpdatedGeneratedTrace.clear
    unUpdatedGeneratedTrace.++=(updatedGeneratedTrace)
  }

  /**
   * Adds events to the generated trace/schedule by updating their indexes.
   */
  private def addChangedEvent(event: Event, generatedTrace: Trace, indexMap: HashMap[Int, Int]) {
    val clonedEvent = event.clone()
    val newIndex = generatedTrace.addEventAndUpdateIndex(clonedEvent) 
    if (event.index != newIndex) {
      indexMap.put(event.index, newIndex)
    }
  }

  def writeTracesToFile(traces: ArrayBuffer[Trace], fileName: String) {
    var file = new File("newTraces.txt")
    var writer = new FileWriter(fileName)
    for (trace <- traces) {
      for (e <- trace.getTrace) {
        writer.write(e.toString())
        writer.write("\n")
      }
      writer.write("\n\n")
    }
    writer.close()
  }
}
