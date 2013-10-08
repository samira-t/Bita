package test
package sample

import akka.actor.{ ActorSystem, Actor, Props, ActorRef }
import java.util.Random
import scala.collection.mutable.HashMap
import akka.dispatch.Promise
import java.util.concurrent.CountDownLatch
import scala.collection.mutable.HashSet

/**
 * @author Samira Tasharofi
 *
 * (Translated from Groovy)
 * http://code.google.com/p/gparallelizer/source/browse/trunk/src/test/groovy/groovyx/gpars/samples/actors/DemoSleepingBarber.groovy?r=1125
 */

case class Enter(customer: ActorRef)
case object Next
case object Full
case object Wait
case object Start
case object Done
case object Go

object CustomerState extends Enumeration {
  val Requested, NotServed, Waiting, BeingServed, Served, Exception = Value
}

class Monitor(customerNum: Int, promise: Promise[HashMap[CustomerState.Value, Int]]) extends Actor {
  import CustomerState._
  var stateCount = new HashMap[CustomerState.Value, Int]
  var currCustomers = HashSet[ActorRef]()
  var receiveCount = 0
  def receive = {
    case state: CustomerState.Value ⇒
      {
        if (!currCustomers.contains(sender)) {
          if (stateCount.keySet.contains(state))
            stateCount.put(state, (stateCount.get(state).get + 1))
          else
            stateCount.put(state, 1)
          currCustomers.+=(sender)
          if (currCustomers.size == customerNum) promise.success(stateCount)
        }
      }

  }

}

class Barber extends Actor {
  val random = new Random()
  var count = 0

  def receive = {
    case Enter(customer) ⇒ {
      customer ! Start
      doTheHaircut(random)
      customer ! Done
      context.sender ! Next

    }
    case Wait ⇒ {
      // No customer
      context.sender ! Next
    }
  }

  private def doTheHaircut(random: Random) {
    Thread.sleep(random.nextInt(10) * 100)
  }
}

class Customer(name: String, waitingRoom: ActorRef, monitor: ActorRef) extends Actor {
  import CustomerState._
  var state: CustomerState.Value = _
  def start: Receive = {
    case Go => {
      waitingRoom ! Enter(self)
      context.become(requested)
    }
    case _ => reportBug //message cannot be handled
  }

  def requested: Receive = {
    case Full ⇒ {
      monitor ! NotServed
      context.become(exit)
    }
    case Wait ⇒ {
      context.become(waiting)
    }
    case Start ⇒ {
      context.become(beingServed)
    }
    case _ => reportBug //message cannot be handled
  }

  def waiting: Receive = {
    case Start ⇒ {
      context.become(beingServed)
    }
    case _ => reportBug //message cannot be handled
  }

  def beingServed: Receive = {
    case Done ⇒ {
      monitor ! Served
      context.become(exit)
    }
    case _ => reportBug //message cannot be handled
  }

  def exit: Receive = {
    case _ => ()
  }
  def receive() = start

  def reportBug = monitor ! Exception
}

class WaitingRoom(capacity: Int, barber: ActorRef) extends Actor {
  private var waitingCustomers = List[ActorRef]()
  private var barberWaiting = true

  def receive() = {
    case Enter(customer) ⇒ {
      if (waitingCustomers.length == capacity) {
        context.sender ! Full
      } else {
        waitingCustomers ::= (customer)
        if (barberWaiting) {
          barberWaiting = false
          self ! Next
        } else {
          context.sender ! Wait
        }
      }
    }
    case Next ⇒ {
      if (waitingCustomers.length > 0) {
        val customer = waitingCustomers(0)
        waitingCustomers = waitingCustomers.drop(1)
        barber ! Enter(customer)
      } else {
        barberWaiting = true
      }
    }
  }
}
