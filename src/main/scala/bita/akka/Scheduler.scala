
package akka.bita

/**
 * @author Samira Tasharofi (tasharo1@illinois.edu)
 */

import scala.collection.mutable.HashSet
import scala.collection.mutable.ListBuffer
import akka.actor.ActorCell
import akka.actor.ActorRef
import akka.dispatch.Envelope
import akka.pattern.PromiseActorRef
import akka.dispatch.DefaultPromise
import scala.collection.immutable.StringLike
import akka.actor.ActorSystem
import akka.actor.Actor
import java.util.concurrent.CountDownLatch
import akka.actor.ScalaActorRef
import bita.util.ActorPathHelper._
import bita.util.MessageHelper._
import akka.actor.ActorPath
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer

/**
 * The scheduler object accepts a schedule and if the schedule is not null, then
 * forces that schedule during the execution.
 * The schedule is a sequence of receive events. When forcing a schedule,
 * it matches the receive events of execution with the receive events of schedule using their IDs.
 *
 * It also records the execution in a <code>Trace</code> object.
 */

object Scheduler {
  import bita._
  import bita.LogicalActor._
  import bita.util._

  private var heldEnvelopes = HashSet[HeldEnvelope]()
  private var schedule: ListBuffer[ScheduleEnvelope] = null
  private var relaxedPoint = -1
  private var relaxedPointIndex = -1

  private var scheduleEvelopesHashSet: HashSet[(EventID, String)] = null

  private var currentReceives = HashMap[String, Int]()
  // Map from promise actor reference to parent path
  private var promiseToParentMap = HashMap[ActorRef, String]()
  // Map from actor reference to its ID.
  private var actorPathToLogicalIDMap = HashMap[String, LogicalActor]()
  private var system: ActorSystem = _
  private var vcManager = SystemVCManager()
  private var trace = Trace()

  private var traceFileName = ""

  var emptyScheduleLatch = new CountDownLatch(0)
  var lockTimeoutSend = false

  private var logger = new Logger("Scheduler")

  logger.enable = false

  private var isIdleCount = 0

  /**
   * Mock actors are used to identify external entities communicating with the actor system.
   */
  def initForMockActors() {
    actorPathToLogicalIDMap.put(DeadLetterActorPath, DeadLetterID)
    actorPathToLogicalIDMap.put(HttpActorPath, GatlingHTTPHandlerID)
    actorPathToLogicalIDMap.put(SchedulerActorPath, SchedulerID)
    currentReceives.put(DeadLetterActorPath, Event.rootEvent.index)
    currentReceives.put(HttpActorPath, Event.httpEvent.index)
    currentReceives.put(SchedulerActorPath, Event.schedulerEvent.index)
  }

  /**
   * Indicates whether the system is running in random mode.
   */
  def isRandom(receiver: ScalaActorRef, message: Any, sender: ActorRef): Boolean = synchronized {
    (heldEnvelopes.size == 0) && (schedule == null || schedule.size == 0)
  }
  /**
   * Sets the name of the trace file in which the execution is recorded.
   */
  def setTraceName(traceName: String) = synchronized {
    traceFileName = traceName
  }
  /**
   * Sets the schedule of execution to the given sequence of envelopes.
   */
  def setSchedule(envelopes: ScheduleEnvelope*) = synchronized {
    schedule = new ListBuffer[ScheduleEnvelope]
    for (envelope <- envelopes) {
      schedule = schedule.:+(envelope)
    }
  }
  /**
   * Sets the system variable to the running actor system.
   */
  def setSystem(actorSystem: ActorSystem) = synchronized {
    system = actorSystem
    sendTimeoutIfNeeded()
  }

  /**
   * Sets the schedule of execution to the schedule written in a file.
   */
  def setSchedule(scheduleFile: String) = synchronized {
    traceFileName = scheduleFile.replace(".txt", "-trace.txt")
    schedule = new ListBuffer[ScheduleEnvelope]
    scheduleEvelopesHashSet = HashSet[(EventID, String)]()
    relaxedPointIndex = -1

    val lines = io.Source.fromFile(scheduleFile).getLines
    while (lines.hasNext) {
      var line = lines.next()
      val event = Event.parse(line)
      if (event != null) {
        var logicalMessage = event.message.asInstanceOf[LogicalMessage]
        schedule.+=(ScheduleEnvelope(event.receiverIDStr, logicalMessage, event.cmh))
        scheduleEvelopesHashSet.add((logicalMessage.creatorID, event.receiverIDStr))
      } else if (relaxedPointIndex == -1) relaxedPointIndex = schedule.size - 1
    }

    relaxedPoint = schedule.size - relaxedPointIndex + 1

    if (schedule.size > 0) emptyScheduleLatch = new CountDownLatch(1)
    //Logger.logLine(schedule.toString())
  }
  /**
   * Resets the state of scheduler.
   */
  def reset() = synchronized {
    heldEnvelopes.clear()
    schedule = null
    relaxedPoint = -1
    relaxedPointIndex = -1
    currentReceives.clear()
    promiseToParentMap.clear()
    //promiseToLogicalMessageMap.clear()
    actorPathToLogicalIDMap.clear()
    vcManager.resetClocks()
    trace.reset()
    emptyScheduleLatch = new CountDownLatch(0)
    system = null
    isIdleCount = 0
    ActorPathHelper.reset()
    //initForMockActors()
  }

  /**
   *  This method is called from akka.bita.patterns.ask to map the promise actors
   *  to their parents (for synchronous messaging).
   */
  def addPromiseToParentMap(promiseRef: PromiseActorRef, parent: String) = synchronized {
    promiseToParentMap.put(promiseRef, parent)
  }

  /**
   * This method is called from aspectj file when a receive event creates an actor.
   */
  def addCreationEvent(child: ActorRef, parent: ActorRef) = synchronized {

    setRootActor(child)
    if (parent == null) logger.logLine("null parent ref")
    updateStatusForActorCreation(child.path.toString, parent.path.toString)
  }

  private def updateStatusForActorCreation(childPath: String, parentPath: String): Object = {
    if (!actorPathToLogicalIDMap.contains(childPath) ||
      actorPathToLogicalIDMap.get(childPath).get.creatorID == EventID(-1, -1)) { //It has not been added before

      logger.logLine("&&&&&&&& %s creates %s".format(parentPath, childPath))
      var newVC = vcManager.updateVectorClockForCreate(parentPath, childPath)

      val creatorIndex = if (currentReceives.contains(parentPath))
        currentReceives.get(parentPath).get
      else currentReceives.get(DeadLetterActorPath).get
      logger.logLine("&&&&&&&& creator Index = " + creatorIndex + currentReceives)

      val e = trace.getEvent(creatorIndex)
      e.create.+=(childPath)
      e.increaseClock(null)
      if (e.receiverIDStr.contains("HttpRequestAction")) {
        vcManager.updateVectorClockForReceive(HttpActorPath, newVC)
      }

      if (!actorPathToLogicalIDMap.contains(childPath)) {
        actorPathToLogicalIDMap.put(childPath, LogicalActor(e.getID(null)))
      } else {
        actorPathToLogicalIDMap.get(childPath).get.creatorID = e.getID(null)
      }
    }
    return null

  }

  /**
   *  This method is called from aspectj file when an actor is started. It is used
   *  to map the actor path to the actor dynamic type.
   */
  def startActor(actorCell: ActorCell, actorObject: Actor) = synchronized {
    logger.logLine("starts the actor" + actorObject)

    val actorPath = actorCell.self.path.toString()
    val actorObjectClass = actorObject.getClass().toString().replace("class ", "")
    //TODO: How bout if the map does not have the actorPath (index -1)
    if (actorPathToLogicalIDMap.contains(actorPath))
      actorPathToLogicalIDMap.get(actorPath).get.objectType = actorObjectClass
    else
      actorPathToLogicalIDMap.put(actorPath, LogicalActor(actorObjectClass))

    checkForDispatch()
  }

  /**
   *  This method is called from aspectj file when a message stops an actor.
   *  It is useful when considering criteria that look for the stop events.
   */
  def addStopEvent(stoppedRef: ActorRef, parent: ActorRef) = synchronized {
    var parentPath = parent.path.toString
    var stoppedPath = stoppedRef.path.toString()
    if (!isRoot(stoppedPath)) { //It is not the root of the whole system akka://SystemName/
      if (isUserGuardian(parentPath))
        parentPath = DeadLetterActorPath
      val e = trace.getEvent(currentReceives.get(parentPath).get)
      e.stop.+=(actorPathToLogicalIDMap.get(stoppedPath).get.toString)
      logger.logLine("actor %s stops actor %s".format(parent, stoppedRef))
    }
  }

  /**
   * This method is called from aspectj file when a message changes the actor message handler
   * via using <code>become</code> and <code>unbecome</code> APIs.
   */
  def setCMH(actor: ActorRef) = synchronized {
    var index = currentReceives.get(actor.path.toString).get
    var e = trace.getEvent(index)
    if (e.promiseResponse) {
      var stop = false
      var recIDStr = e.receiverIDStr
      for (i <- index - 1 to 0 by -1 if !stop) {
        var te = trace.getEvent(i)
        if (te.receiverIDStr == recIDStr && !te.promiseResponse) {
          te.cmh = true
          stop = true
        }
      }
    } else
      e.cmh = true
  }

  /**
   *  This method is called from aspectj file when sending a message to a promise
   *  (response of a synchronous message sending).
   */
  def aroundSendToPromise(promiseActorRef: PromiseActorRef, message: Object, sender: ActorRef = null): Object = synchronized {
    checkForError(message.toString)

    logger.logLine("***** Send to promise " + message + " " + sender + " ")
    if ((isIdleCount < 2) && (sender == null || !isSystemCreatedActor(sender.path.toString))) {

      //Find the sender actor. If sender is a PromiseRef, then find the creator of the PromiseRef
      var senderActorPath = getActorPath(sender, message)

      // Find the receiver actor. If receiver is a PromiseRef, find the creator of PromiseRef
      var receiverActorPath = getActorPath(promiseActorRef)

      /*
     * If the message is logical message, it means it has been removed from held messages.
     * Therefore, there is no need to check it with the schedule.
     */
      if (message.isInstanceOf[LogicalMessage]) {
        var logicalMessage = message.asInstanceOf[LogicalMessage]
        updateVCAndScheduleAndTraceForReceive(logicalMessage, senderActorPath, receiverActorPath, true)
        return logicalMessage.message.asInstanceOf[AnyRef]

      } else {
        //If the message is not logical message, it needs to check the order with schedule 

        var logicalMessage = createLogicalMessageForSend(message, senderActorPath, receiverActorPath)
        var promiseEnvelope =
          PromiseEnvelope(logicalMessage, sender)

        /* 
       * Check the order with the schedule, if the message can be sent, return the message;
       *  otherwise return null
       */
        if (canSend(logicalMessage, receiverActorPath, true)) {
          updateVCAndScheduleAndTraceForReceive(logicalMessage, senderActorPath, receiverActorPath, true)
          return message
        } else {
          heldEnvelopes.+=(HeldEnvelope(promiseActorRef, promiseEnvelope))
          return null
        }
      }
    } else {
      // In the case of random scheduling, the message might be a logical message;
      // however, in other cases it is not
      if (message.isInstanceOf[LogicalMessage])
        return (message.asInstanceOf[LogicalMessage]).message.asInstanceOf[AnyRef]
      else return message
    }
  }

  /**
   * updates the trace and the vector clocks after the receive event.
   */
  private def updateVCAndScheduleAndTraceForReceive(logicalMessage: LogicalMessage, senderActorPath: String,
    receiverActorPath: String, isPromiseResponse: Boolean) {

    if (isIdleCount < 2) {

      if (logicalMessage.message.toString.startsWith("IsIdle")) isIdleCount += 1
      var newVC = vcManager.updateVectorClockForReceive(receiverActorPath, logicalMessage.vc)

      var senderActorIDStr =
        if (actorPathToLogicalIDMap.get(senderActorPath) != None)
          actorPathToLogicalIDMap.get(senderActorPath).get.toString()
        else {
          //"null"// happens when the message is sent via guardian, etc.
          DeadLetterID.toString()
        }

      var receiverActorIDStr =
        if (actorPathToLogicalIDMap.get(receiverActorPath) != None)
          actorPathToLogicalIDMap.get(receiverActorPath).get.toString()
        else //"null"// happens when the message is received by guardian, etc.
          DeadLetterID.toString()

      var (inSchedule, cmh) = removeFromHeadOfSchedule(logicalMessage, receiverActorIDStr)
      var outOfSchedule = !inSchedule

      /* 
     * If the message is a timeout message, update the sequence number for that receiver 
     * in scheduler event.
     */

      if (senderActorPath.equals(SchedulerActorPath)) {
        var creatorEvent = trace.getEvent(logicalMessage.creatorID.creatorIndex)
        creatorEvent.increaseClock(receiverActorPath)
        //assert(logicalMessage.creatorID == creatorEvent.getID(receiverActorPath))
        logicalMessage.creatorID = creatorEvent.getID(receiverActorPath)
      }

      var newIndex = trace.addEvent(logicalMessage, senderActorIDStr, receiverActorIDStr, isPromiseResponse, outOfSchedule,
        newVC, cmh)

      currentReceives.put(receiverActorPath, newIndex)
    }
    logger.logLine("m=%s, id=%s, sender=%s, rec=%s".format(logicalMessage.message, logicalMessage.creatorID, senderActorPath, receiverActorPath),
      "--trace " + (trace.getTrace().size - 1))
    //println("--trace %s : m=%s, id=%s, sender=%s, rec=%s".format((trace.getTrace().size - 1),
    //   logicalMessage.message, logicalMessage.creatorID, senderActorPath, receiverActorPath))
  }

  /**
   * removes the first envelope from the head of schedule.
   */
  private def removeFromHeadOfSchedule(logicalMessage: LogicalMessage, receiverIDStr: String): (Boolean, Boolean) = {

    var matchIDInscheduleEvelopesHashSet: (EventID, String) = null
    var cmh = false

    if (schedule != null && schedule.size > 0) {
      var matchWithHead = false
      val messageCreatorIndex = logicalMessage.creatorID.creatorIndex
      val messageCreatorID = EventID(trace.getScheduleIndex(messageCreatorIndex), logicalMessage.creatorID.seqNum)

      var receiverActor = LogicalActor.parse(receiverIDStr)
      var adjustedCreatorID = EventID(trace.getScheduleIndex(receiverActor.creatorID.creatorIndex), receiverActor.creatorID.seqNum)
      receiverActor.creatorID = adjustedCreatorID
      var messageReceiverIDStr = receiverActor.toString()

      val scheduleHeadCreatorID = schedule.head.logicalMessage.creatorID
      val scheduleHeadReceiverIDStr = schedule.head.receiver
      val scheduleHeadMessage = schedule.head.logicalMessage.message

      // Check for timeout messages first
      if (isSchedulerMessage(scheduleHeadMessage)) {
        logger.logLine(scheduleHeadReceiverIDStr + " " + messageReceiverIDStr)
        matchWithHead = (scheduleHeadReceiverIDStr.equals(messageReceiverIDStr) && isSchedulerMessage(logicalMessage.message))
        matchIDInscheduleEvelopesHashSet = (scheduleHeadCreatorID, scheduleHeadReceiverIDStr)
        if (matchWithHead) {
          lockTimeoutSend = false
          logger.logLine((logicalMessage.message, messageReceiverIDStr) + " " + (scheduleHeadMessage, scheduleHeadReceiverIDStr), "=== removed from head")
        } else {
          logger.logLine((logicalMessage.message, messageReceiverIDStr) + " " + (scheduleHeadMessage, scheduleHeadReceiverIDStr), "=== not match with head")
        }
      } else {

        // Comparing creator IDs might be very strict. Let's relax it by comparing the creator index and the receiver.
        //if (scheduleHeadCreatorID.creatorIndex == messageCreatorID.creatorIndex && headReceiverIDStr.equals(receiverIDStr)) {
        if (scheduleHeadCreatorID == messageCreatorID && scheduleHeadReceiverIDStr.equals(messageReceiverIDStr)) {
          logger.logLine((messageCreatorID, messageReceiverIDStr) + " " + (scheduleHeadCreatorID, scheduleHeadReceiverIDStr), "=== removed from head")
          matchIDInscheduleEvelopesHashSet = (scheduleHeadCreatorID, scheduleHeadReceiverIDStr)
          matchWithHead = true
        }

      }
      if (matchWithHead) {
        var en = schedule.remove(0)
        cmh = en.cmh
        assert(matchIDInscheduleEvelopesHashSet != null)
        scheduleEvelopesHashSet.-=(matchIDInscheduleEvelopesHashSet)
        if (schedule.size == 0) emptyScheduleLatch.countDown()
        return (true, cmh)

      } else {

        logger.logLine(messageCreatorID + " " + scheduleHeadCreatorID, "do not remove from head")
        return (false, cmh)
      }
    }

    emptyScheduleLatch.countDown()

    return (true, cmh)

  }
  /*
 * This method is used for testing Gatling to identify error messages.
 */
  private def checkForError(message: String) {
    if (message.contains("Error(java.lang.IllegalArgumentException:")
      || message.contains("doesn't support message")) {
      println("***************************** BUG DETECTED *******************" + message)
      println("Stop scheduling..... ")
      reportError(message)
    }
  }

  /*
 * This method is used for testing Gatling to identify the bug and report the error.
 */
  def reportError(error: String) = synchronized {
    var bug =
      if (error.contains("doesn't support message")) {
        if (error.contains("Terminator"))
          "HTBug"
        else {
          if (Constants.HWBug)
            "HWBug"
          else
            "HW2Bug"
        }
      } else error
    var traceFile = traceFileName.replace("-trace.txt", "-%s-trace.txt".format(bug))
    finish(traceFile)
    sys.exit(1)

  }

  /**
   *  This method is called from aspectj file around sending a message to an actor.
   *  According to the schedule, it decides whether the message can be delivered
   *  or it should be held. It also updates the vector clocks.
   */
  def aroundSend(envelope: Envelope, rec: ActorCell): Envelope = synchronized {
    checkForError(envelope.message.toString)
    setRootActor(rec.self)

    if ((isIdleCount < 2) && !isCreationMessage(envelope.message) && !isStopMessage(envelope.message)) {

      implicit val system = rec.system
      var newEnvelope = envelope
      var realMessage = envelope.message
      var logicalMessage: LogicalMessage = null
      var senderActor = envelope.sender.path.toString()
      var receiverActor = rec.self.path.toString()

      //checkForDeadLetterActorRef(envelope.sender)
      var isPromise = (envelope.sender != null) && envelope.sender.isInstanceOf[PromiseActorRef]

      //Wrap the message with the logical ID
      if (envelope.message.isInstanceOf[LogicalMessage]) {
        logicalMessage = envelope.message.asInstanceOf[LogicalMessage]
        realMessage = logicalMessage.message
      } else {
        senderActor = getActorPath(envelope.sender, envelope.message)
        logicalMessage = createLogicalMessageForSend(envelope.message, senderActor, receiverActor)
        newEnvelope = Envelope(logicalMessage, envelope.sender)(system)
      }

      // Check the order of messages if the schedule is not null or empty
      if (canSend(logicalMessage, receiverActor, isPromise)) return newEnvelope
      else {
        if (!isSchedulerMessage(realMessage)) {
          heldEnvelopes.+=(HeldEnvelope(rec.self, newEnvelope))
        }
        return null
      }
    } else {
      //setRootActor(rec.self)
      return envelope
    }
  }

  /**
   * Determines if a message can be delivered.
   */
  private def canSend(logicalMessage: LogicalMessage, receiver: String, isPromise: Boolean = false): Boolean = {

    // Check the order of messages if the schedule is not null or empty
    if (schedule == null || schedule.size == 0) return true

    var creatorIndex = logicalMessage.creatorID.creatorIndex
    var messageCreatorID = EventID(trace.getScheduleIndex(creatorIndex), logicalMessage.creatorID.seqNum)
    var messageReceiverIDStr = actorPathToLogicalIDMap.get(receiver).getOrElse(DeadLetterID).toString
    var receiverActor = LogicalActor.parse(messageReceiverIDStr)
    var adjustedCreatorID = EventID(trace.getScheduleIndex(receiverActor.creatorID.creatorIndex), receiverActor.creatorID.seqNum)
    receiverActor.creatorID = adjustedCreatorID
    messageReceiverIDStr = receiverActor.toString()
    //println(messageReceiverIDStr + " "+messageCreatorID)

    if (isSchedulerMessage(schedule.head.logicalMessage.message)) {
      var message = logicalMessage.message
      var canSendTimeout = (isSchedulerMessage(message) && schedule.head.receiver.equals(messageReceiverIDStr)
        && !lockTimeoutSend)
      if (canSendTimeout) lockTimeoutSend = true // do not send timeout unil this one is received
      return canSendTimeout
    }

    // This one is very strict. let's make it more relaxed and just compare the creator index and the receiver
    if (schedule.head.logicalMessage.creatorID == messageCreatorID && schedule.head.receiver.equals(messageReceiverIDStr))
      return true
    //    if (schedule.head.logicalMessage.creatorID.creatorIndex == messageCreatorID.creatorIndex
    //      && schedule.head.receiver.equals(receiverIDStr)) return true

    // TODO: scheduleEvelopesHashSet.contains(messageCreatorID) should be changed:only index and receiver
    if (isPromise && schedule.size <= relaxedPoint && !scheduleEvelopesHashSet.contains((messageCreatorID, messageReceiverIDStr))
      && creatorIndex >= relaxedPointIndex)
      return true
    else {
      logger.logLine((logicalMessage.message, messageReceiverIDStr, messageCreatorID) + "," + (schedule.head.receiver, schedule.head.logicalMessage.creatorID), "cannot go")

      return false
    }
  }

  def aroundPromiseResult(promise: DefaultPromise[Any], result: Any) = synchronized {
    //checkForDispatch()
  }

  /**
   *  This method is called from aspectj file when receiving/processing a message.
   *  It updates the schedule and the vector clocks.
   */
  def aroundInvoke(envelope: Envelope, actorCell: ActorCell): Envelope = synchronized {

    //unwrap the logical envelope and send it to the actor before processing
    var realEnvelope = envelope
    if (envelope.message.isInstanceOf[LogicalMessage]) {
      val logicalMessage = envelope.message.asInstanceOf[LogicalMessage]

      //Get the appropriate sender
      var senderActor = getActorPath(envelope.sender, logicalMessage.message)

      updateVCAndScheduleAndTraceForReceive(logicalMessage, senderActor, actorCell.self.path.toString(), false)
      realEnvelope = Envelope(envelope.message.asInstanceOf[LogicalMessage].message, envelope.sender)(actorCell.system)
    } //else //println("the message is not logical envelope: " + envelope.message)
    else {
      logger.logLine("m=%s,  sender=%s, rec=%s".format(envelope.message, envelope.sender, actorCell.self),
        "-- out of trace ")
      //println("-- out of trace: m=%s,  sender=%s, rec=%s".format(envelope.message, envelope.sender, actorCell.self)
      // )
    }

    //Return the real unwrapped envelope
    return realEnvelope
  }

  /**
   * This method is called from aspectj file after each send and each receive.
   * It checks if some held messages can be delivered now
   */
  def checkForDispatch(): Boolean = synchronized {
    for (heldEnvelope <- heldEnvelopes) {
      logger.logLine("Check for disapthcing: " + heldEnvelope)
      if (heldEnvelope != null && canSend(heldEnvelope)) {
        logger.logLine("can be dispatched: " + heldEnvelope)

        heldEnvelopes.-=(heldEnvelope)

        deliverEnvelope(heldEnvelope)
        logger.logLine(heldEnvelope.toString(), "**** delivered")
        return true
      }

    }
    sendTimeoutIfNeeded()
    return false
  }

  /**
   * Sends the timeout messages if it is required according to the schedule. It handles the
   * programs with <code>ReceiveTimeout</code> messages.
   */
  def sendTimeoutIfNeeded() {
    if (schedule != null && schedule.size > 0 && isSchedulerMessage(schedule.head.logicalMessage.message)) {
      if (system != null) {
        var receiverPath: String = null
        for ((actorPath, actorID) <- actorPathToLogicalIDMap if (receiverPath == null)) {
          if (actorID.toString.equals(schedule.head.receiver))
            receiverPath = actorPath
        }
        if (receiverPath != null) {
          logger.logLine("Sending Timeout to " + system.actorFor(receiverPath) + " " + receiverPath)
          system.actorFor(receiverPath) ! akka.actor.ReceiveTimeout
        } else {
          logger.logLine("ERRORR! The receiver of time out does not exits in the system")
        }
      } else
        logger.logLine("The schedule needs timeout and the system is nuull")
    }
  }

  /**
   * Decides whether a held envelope can be delivered.
   */
  private def canSend(heldEnvelope: HeldEnvelope): Boolean = {
    var logicalMessage = if (heldEnvelope.envelope != null)
      heldEnvelope.envelope.message.asInstanceOf[LogicalMessage]
    else
      heldEnvelope.promiseEnvelope.message

    var receiverPath = getActorPath(heldEnvelope.receiver)
    return canSend(logicalMessage, receiverPath) //heldEnvelope.receiver.path.toString())

  }
  /**
   * Delivers a held envelope.
   */
  private def deliverEnvelope(heldEnvelope: HeldEnvelope) {
    logger.logLine("delivering envelope..." + heldEnvelope)
    if (heldEnvelope.envelope != null)
      heldEnvelope.receiver.!(heldEnvelope.envelope.message)(heldEnvelope.envelope.sender)
    else
      heldEnvelope.receiver.!(heldEnvelope.promiseEnvelope.message)(heldEnvelope.promiseEnvelope.sender)
    logger.logLine("delivered envelope..." + heldEnvelope)
  }
  /**
   * Sets the root actor of the system.
   */
  private def setRootActor(actor: ActorRef): Unit = {

    if (actor != null && !rootIsSet) {
      setRoot(actor.path.toString)
      initForMockActors()
    }
  }

  /**
   * Wraps a message into a logical message.
   */
  private def createLogicalMessageForSend(message: Any, senderPath: String, receiverPath: String): LogicalMessage = {
    val index = getMessageCreatorIndex(senderPath, receiverPath)

    //if (index == -1) currentReceives.put(sender, -1)
    val creator = trace.getEvent(index)
    creator.increaseClock(receiverPath)
    var vc = vcManager.updateVectorClockForSend(senderPath)
    return LogicalMessage(message, creator.getID(receiverPath), vc)
  }

  /**
   * Returns the index of receive event in the trace that sent a message.
   */
  private def getMessageCreatorIndex(msgSenderPath: String, receiverPath: String): Int = {

    /*
     * If the message is sent to HTTP Action, then find the last Http action
     * and return that index as the creator index
     */
    if (msgSenderPath.equals(HttpActorPath)) {
      for (i <- trace.size - 1 to 0 by -1) {
        var event = trace.getEvent(i)
        if (event.create.contains(receiverPath)) {
          return i
        }
      }
      logger.logLine("ERROR! Could not find ")

    }

    if (currentReceives.contains(msgSenderPath)) {
      return currentReceives.get(msgSenderPath).get
    } else if (isUserCreatedActor(msgSenderPath)) {

      var parentPath = ActorPath.fromString(msgSenderPath).parent
      var parentPathString = parentPath.toString

      while (!isUserGuardian(parentPathString)) {
        // If the message is sent in preStart method of the actor.
        if (currentReceives.contains(parentPathString)) {
          logger.logLine("calling add create event form pre start ... ")
          updateStatusForActorCreation(msgSenderPath, parentPathString)
          val senderCreatorID = actorPathToLogicalIDMap.get(msgSenderPath).get.creatorID
          assert(senderCreatorID != EventID(-1, -1))
          return senderCreatorID.creatorIndex
        }
        parentPath = parentPath.parent
        parentPathString = parentPath.toString
      }
      return -1
    }
    return -1
  }

  /**
   * Creates and returns the logical message of message which is going to be sent.
   */
  def createLogicalMessageForSend(message: Any, sender: ActorRef, receiver: ScalaActorRef): LogicalMessage = synchronized {
    val senderPath = getActorPath(sender, message)
    val receiverPath = getActorPath(receiver)
    val lmsg = createLogicalMessageForSend(message, senderPath, receiverPath)
    return lmsg
  }

  /**
   * Returns the path of the receiver actor.
   * It consider the following cases:
   *  <ul>
   *    <li><b>The reference is <b>null</b> or the path is deadletter -</b>
   *      <ul>
   *      <li>if the message is a message sent by HTTPServers, then it
   *        returns HTTP actor path
   *      </li>
   *      <li>if the message is sent by system scheduler, then it
   *        returns Scheduler actor path.
   *      </li>
   *      <li>otherwise, it
   *        returns deadletter path.
   *      </li>
   *      </ul>
   *    </li>
   *    <li><b>The reference is a <code>Promise</code> reference</b> -
   *      returns parent path.
   * 	</li>
   *    <li><b>Otherwise</b> -
   *      returns the actor path.
   *    </li>
   *  </ul>
   */
  def getActorPath(actorRef: ActorRef, message: Any = null): String = synchronized {

    if (actorRef == null || actorRef.path.toString.equals(DeadLetterActorPath)) {
      if (message != null) {
        if (isHttpHandlerMessage(message)) {
          logger.logLine("****** sender is Http Handler")
          return HttpActorPath
        }
        if (isSchedulerMessage(message)) {
          //logger.logLine("****** sender is Scheudler")
          return SchedulerActorPath
        }
      }
      return DeadLetterActorPath
    }

    if (actorRef.isInstanceOf[PromiseActorRef] && promiseToParentMap.contains(actorRef)) {
      //println(actorRef + promiseToParentMap.get(actorRef).toString)
      return promiseToParentMap.get(actorRef).get
    } else
      return actorRef.path.toString()

  }

  /**
   * Writes the trace of execution into the traceFile.
   */
  def finish(traceFile: String) = synchronized {
    trace.outputTrace(traceFile)
  }

  def getTrace: Trace = synchronized {
    trace
  }

  def getPromiseToParentMap: HashMap[ActorRef, String] = synchronized {
    return promiseToParentMap
  }

}
