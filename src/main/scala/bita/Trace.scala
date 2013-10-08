package bita

/**
 * @author Samira Tasharofi (tasharo1@illinois.edu)
 */

import java.io.File
import java.io.FileWriter
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import akka.actor.ActorRef
import akka.dispatch.Envelope
import scala.collection.mutable.HashSet
import akka.actor.ActorCell


object Trace {
  
/**
 * Parses a trace file and returns a trace object.
 */
  def parse(traceFile: String, fillHash: Boolean = false): Trace = {
    val lines = io.Source.fromFile(traceFile).getLines
    val trace = new Trace
    val hashCodes = ArrayBuffer[Int]()
    while (lines.hasNext) {
      var line = lines.next()
      var event = Event.parse(line)
      if (event != null) {
        if (fillHash) {
          var eventHash = Event.getHashCode(line, hashCodes.toArray[Int])
          hashCodes.+=(eventHash)
          event.hashCodeInTrace = eventHash
        }
        trace.addEvent(event)
      } else
        trace.setRelaxedIndex(trace.getTrace().size)
    }
    return trace
  }
/**
 * Writes the hash codes of events in a trace file into a file.
 */
  def writeHashCodes(traceFile: String, hashFilePath: String): Array[Int] = {
    var lines = io.Source.fromFile(traceFile).getLines
    val hashCodes = ArrayBuffer[Int]()

    var line = ""
    var eventHash = -1
    var hashWriter = new FileWriter(hashFilePath)
    while (lines.hasNext) {
      line = lines.next()
      eventHash = Event.getHashCode(line, hashCodes.toArray[Int])
      hashCodes.+=(eventHash)
      hashWriter.write(eventHash.toString() + "\n")
    }
    hashWriter.close()
    return hashCodes.toArray[Int]

  }

}

/**
 * A sequence of events that presents an execution or a schedule.
 */
case class Trace() {
  import util.MessageHelper._

  private var isIdleCount = 0
  private var completeTrace = ArrayBuffer[Event]()
  private var mockActorsEvents = HashSet[Event]()
  //private var causalityGraph = HashMap[Int, HashSet[Int]]()
  /**
   * The index from which the scheduler can allow some messages not included in
   * the schedule to be delivered. 
   */
  private var relaxedIndex = -1
  private var timeoutSeqMap = HashMap[String, Int]()

  private var outOfScheduleIndexes = HashSet[Int]()

  def reset() {
    completeTrace.clear()
    //causalityGraph.clear()
    outOfScheduleIndexes.clear()
    relaxedIndex = -1
    mockActorsEvents.clear()
    isIdleCount = 0
    timeoutSeqMap.clear()
  }

  private def initializeMockActorsEvents() {
    mockActorsEvents = HashSet[Event](Event.rootEvent, Event.schedulerEvent, Event.httpEvent)
  }

  def addEvent(message: Any, sender: String, receiver: String, isPromiseResponse: Boolean,
    outOfSchedule: Boolean, vc: VectorClock = null, cmh: Boolean = false): Int = {

    var logicalMessage = message.asInstanceOf[LogicalMessage]

    if (logicalMessage.message.toString.startsWith("IsIdle"))
      isIdleCount += 1

    if (isSchedulerMessage(logicalMessage.message)) {
      var newSeq = 1
      if (timeoutSeqMap.contains(receiver)) {
        newSeq = timeoutSeqMap.get(receiver).get + 1
      }
      timeoutSeqMap.put(receiver, newSeq)

      var creatorID = logicalMessage.creatorID
      logicalMessage.creatorID = EventID(creatorID.creatorIndex, newSeq)
    }

    var event = Event(completeTrace.size, logicalMessage, sender, receiver, vc, isPromiseResponse, cmh)
    var index = addEvent(event)
    if (outOfSchedule) {
      outOfScheduleIndexes.+=(index)
    }
    return index
  }

  def setRelaxedIndex(index: Int) {
    relaxedIndex = index
  }

  def getRelaxedIndex: Int = {
    return relaxedIndex
  }

  /**
   * Maps the index of an event in the trace to the index of that event in the schedule. 
   */
  def getScheduleIndex(traceIndex: Int): Int = {
    var scheduleIndex = traceIndex
    if (outOfScheduleIndexes.contains(traceIndex)) {
      return -2
    }
    for (changedIndex <- outOfScheduleIndexes) {
      if (traceIndex >= changedIndex)
        scheduleIndex -= 1

    }
    return scheduleIndex
  }

  def addEventAndUpdateIndex(event: Event): Int = {
    event.index = completeTrace.size
    completeTrace.+=(event)
    return event.index
  }

  private def addEvent(event: Event): Int = {
    completeTrace.+=(event)
    return event.index
  }

//  private def addEdge(map: HashMap[Int, HashSet[Int]], i: Int, j: Int) {
//    val sink = map.get(i)
//    if (sink == None) {
//      val newSink = new HashSet[Int]
//      newSink.add(j)
//      map.put(i, newSink)
//    } else {
//      val existingSink = sink.get
//      existingSink.add(j)
//      map.put(i, existingSink)
//    }
//  }

//  private def addEdges(map: HashMap[Int, HashSet[Int]], i: Int, jSet: HashSet[Int]) {
//    val sink = map.get(i)
//    if (sink == None) {
//      val newSink = new HashSet[Int]
//      newSink.++=(jSet)
//      map.put(i, newSink)
//    } else {
//      val existingSink = sink.get
//      existingSink.++=(jSet)
//      map.put(i, existingSink)
//    }
//  }

  def cutTraceEnd(endIndex: Int) {
    completeTrace = completeTrace.slice(0, endIndex)
  }

  def outputTrace(traceFile: String) {

    var writer = util.FileHelper.getWriter(traceFile)
    var index = -1

    for (event <- completeTrace) {
      index += 1
      if (index == relaxedIndex) {
        writer.write("===============================================================\n")

      }

      writer.write(event.toString())
      writer.write("\n")

    }
//    writer.write("===============================================================\n")
//    for (generationRelation <- causalityGraph) {
//      writer.write(completeTrace(generationRelation._1) + "->" + generationRelation._2 + "\n")
//    }
    writer.close()
    println("The output is written in " + traceFile)

  }

  /**
   * Returns the event at by giving the index.
   */
  def getEvent(index: Int): Event = {
    if (index < 0) {
      if (mockActorsEvents.size == 0) initializeMockActorsEvents()
      for (event <- mockActorsEvents) {
        if (event.index == index) return event
      }
    }

    if (index >= 0 && index < completeTrace.length)
      return completeTrace(index)
    else return null
  }

  def size = completeTrace.length

  def getTrace(): ArrayBuffer[Event] = {
    return completeTrace
  }

//  def getCausality(): HashMap[Int, HashSet[Int]] = {
//    computeCausality()
//    return causalityGraph
//  }

//  def computeCausality() {
//    val happensBeforeMap = computeHappensBefore
//
//    for (i <- completeTrace.size - 1 to 0 by -1) {
//      if (happensBeforeMap.contains(i)) {
//        var kSet = happensBeforeMap.get(i).get
//        val jSet = new HashSet[Int]
//        for (k <- kSet) {
//          if (causalityGraph.contains(k)) {
//            jSet.++=(causalityGraph.get(k).get)
//          }
//        }
//        addEdges(causalityGraph, i, jSet)
//      }
//    }
//
//    //println("\n\n" + causalityGraph + "\n")
//  }

//  private def computeHappensBefore(): HashMap[Int, HashSet[Int]] = {
//    val happensBeforeMap: HashMap[Int, HashSet[Int]] = new HashMap[Int, HashSet[Int]]
//
//    for (i <- 0 to completeTrace.size - 2) {
//      for (j <- i + 1 to completeTrace.size - 1) {
//        val eventI = completeTrace(i)
//        val eventJ = completeTrace(j)
//        if (satisfyHappensBefore(eventI, eventJ)) {
//          addEdge(happensBeforeMap, i, j)
//        }
//      }
//    }
//    return happensBeforeMap
//  }
//
//  private def satisfyHappensBefore(eventI: Event, eventJ: Event): Boolean = {
//    var isHappensBefore = false
//    if (eventI.receiverIDStr == eventJ.receiverIDStr) { // re(i).receiver = re(j).receiver
//      isHappensBefore = true
//    }
//    if (eventJ.receiverIDStr != null && eventI.create.contains(eventJ.receiverIDStr)) { // re(j).rec \in create(i)
//      addEdge(causalityGraph, eventI.index, eventJ.index)
//      isHappensBefore = true
//    }
//    if (eventI.receiverIDStr != null && eventJ.stop.contains(eventI.receiverIDStr)) { // re(i).rec \in stop(j)
//      isHappensBefore = true
//    }
//
//    // re(j).msg \n sent(i)
//    var messageID_I = eventI.index
//
//    var parentMessageID_J = eventJ.message.asInstanceOf[LogicalMessage].creatorID.creatorIndex
//    if (messageID_I != -1 && messageID_I == parentMessageID_J) {
//      addEdge(causalityGraph, eventI.index, eventJ.index)
//      isHappensBefore = true
//    }
//    return isHappensBefore
//  }

  override def toString(): String = {
    completeTrace.toString()

  }
}