package test
package sample
package leader

/**
 * @author Samira Tasharofi (tasharo1@illinois.edu)
 */
import akka.actor.{ ActorSystem, Actor, Props, ActorRef }
import bita.{ ScheduleEnvelope, LogicalMessage, EventID }
import java.util.concurrent.CountDownLatch
import scala.collection.mutable.HashSet
import akka.util.Timeout
import akka.bita.pattern.Patterns._
import scala.collection.mutable.ArrayBuffer

/**
 * The implementation of buggy leader election algorithm described in
 * http://www.springerlink.com/content/bpdeff986nkfwgjp/fulltext.pdf
 */

case class Init(candidates: Array[ActorRef])
case class Capture(candidate: ActorRef, candidatePriority: Int, candidateCaptured: Array[ActorRef])
case class Elect(candidate: ActorRef)
case class Accept(candidate: ActorRef, candidateCaptured: Array[ActorRef])
case class Down(candidate: ActorRef)
case object Kill

object State extends Enumeration {
  type State = Value
  val CANDIDATE, CAPTURED, SURRENDERED, ELECTED, KILLED = Value
}

class NodeActor(priority: Int, bugLatch: CountDownLatch, simulator: ActorRef, electLatch: CountDownLatch) extends Actor {
  import State._
  var leader: ActorRef = _
  var state: State = KILLED
  var candidates = ArrayBuffer[ActorRef]()
  var iterationCaptured = ArrayBuffer[ActorRef]()
  var downNodes = HashSet[ActorRef]()

  var countedDownElectLatch = false

  private def changeState(newState: State) {
    state = newState
    if (state == ELECTED) {
      simulator ! Elected(self, candidates)
    }
    log("update state " + priority + ": " + state)
  }

  override def receive() = {

    case Init(candidates: Array[ActorRef]) => {
      candidates.foreach(this.candidates.+=(_))
      changeState(CANDIDATE)
      sender ! 'OK

    }

    case msg @ _ if (state == KILLED) => log("killed received message " + msg)

    // it is called when the actor starts its execution
    case 'Start => {
      broadcast("capture")
    }

    case Capture(candidate, candidatePriority, candidateCaptured) => {
      if (downNodes.contains(candidate))
        downNodes.remove(candidate)

      state match {
        case CANDIDATE => {
          if (candidateCaptured.size > iterationCaptured.size
            || (candidateCaptured.size == iterationCaptured.size && priority > candidatePriority)) {
            // we lose
            candidate ! Accept(self, iterationCaptured.toArray[ActorRef])
            changeState(CAPTURED)
            leader = candidate
          }
        }

        case ELECTED => candidate ! Elect(self)

        case CAPTURED => ()

        case SURRENDERED => ()
      }

    }
    case Accept(candidate, candidateCaptured) => {

      if (downNodes.contains(candidate))
        downNodes.remove(candidate)

      state match {

        case CANDIDATE => {
          iterationCaptured.+=(candidate)
          candidateCaptured.foreach(iterationCaptured.+=(_))
          checkMajority()
        }

        case CAPTURED => leader ! Accept(candidate, candidateCaptured)

        case SURRENDERED => ()

        case ELECTED => candidate ! Elect(self)
      }
    }

    case Elect(candidate) => {

      if (state == ELECTED) {
        bugLatch.countDown()
        logError("Bug found!!!!!!!!  ")
      } else {
        if (downNodes.contains(candidate))
          downNodes.remove(candidate)

        leader = candidate
        changeState(SURRENDERED)
        iterationCaptured.clear()
      }

      if (!countedDownElectLatch) {
        electLatch.countDown()
        countedDownElectLatch = true
      }

      println("receive a leader")
    }

    case Down(candidate) => {
      if (state == KILLED) log(" I am killed")
      if (!downNodes.contains(candidate))
        downNodes.add(candidate);
      if (leader == candidate)
        leader = null;

      if (state == CANDIDATE) {
        checkMajority()
      } else if (leader == null
        && (state == SURRENDERED || state == CAPTURED)) {
        broadcast("capture")
        checkMajority()
      }
    }

    case Kill => {
      changeState(KILLED)
      broadcast("down")
      println("killed")
    }
  }

  def broadcast(msg: String) {
    try {
      msg match {
        case "capture" => candidates.filterNot(_ == self).foreach(_ ! Capture(self, priority, iterationCaptured.toArray[ActorRef]))
        case "elect" => candidates.filterNot(_ == self).foreach(_ ! Elect(self))
        case "down" => candidates.filterNot(_ == self).foreach(_ ! Down(self))
      }
    } catch {

      case ex: Throwable => {
        println(ex.printStackTrace())
        println(iterationCaptured)
        if (candidates == null)
          println("candidate is null")
      }
    }

  }

  def checkMajority() {
    val numCaptured = iterationCaptured.size + 1
    val numCandidates = candidates.size
    val numdownNodes = downNodes.size
    if ((numCaptured > numCandidates / 2)
      || (numCaptured + numdownNodes == numCandidates)) {
      log("I am the leader")
      changeState(ELECTED)

      /**
       * Code to automatically detect deadlocks in Erlang's LE
       */
      leader = self
      broadcast("elect")
      electLatch.countDown()
      println("elected as the leader")

    } else {
      changeState(CANDIDATE)
    }
  }

  private def log(msg: String) {
  }

  private def logError(msg: String) {
    print("Error! " + this.priority + ": " + msg)
  }
}

case class Elected(node: ActorRef, nodes: ArrayBuffer[ActorRef])
class Simulator(scenario: Int, leaderLatch: CountDownLatch) extends Actor {

  implicit val timeout = Timeout(10000)

  var round = 0

  override def receive() = {

    case Elected(node, nodes) => if (scenario == 1 && round == 0) {
      node ! Kill
      node ! Init(nodes.toArray[ActorRef])
      node ! 'Start
      round += 1
      leaderLatch.countDown
      
    } else {
      leaderLatch.countDown
    }
  }

}
