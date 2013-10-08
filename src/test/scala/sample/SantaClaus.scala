package test
package sample

import akka.actor.{ ActorSystem, Actor, Props, ActorRef }
import akka.bita.pattern.Patterns._
import akka.dispatch.Await
import akka.util.Timeout

case class Reindeer(workers: List[ActorRef])
case class Elves(workers: List[ActorRef])
case object IncReWaitCount
case class Leave(worker: ActorRef)
case class OK

class Santa() extends Actor {
  var reWaitCount: Int = 0
  var currGroup: List[ActorRef] = List()
  var canServe = true

  def receive() = {
    case Elves(workers) if (reWaitCount == 0 && canServe) => {
      println("serve elves")
      for (worker <- workers) {
        worker ! SantaWorkerGo(self)
      }
      currGroup = workers
      canServe = false
    }
    case Reindeer(workers) if (canServe) => {
      println("serve reindeer")
      for (worker <- workers) {
        worker ! SantaWorkerGo(self)
      }
      currGroup = workers
      reWaitCount -= 1
      canServe = false
    }
    case Leave(worker) => {
      currGroup = currGroup.-(worker)
      if (currGroup.length == 0) canServe = true
    }
    case IncReWaitCount => {
      reWaitCount += 1
      sender ! OK
    }
  }
}

case class AddReMember(worker: ActorRef)
case class AddElfMember(worker: ActorRef)

class Secretary(santa: ActorRef, re_count: Int, el_count: Int) extends Actor {
  var reGroup: List[ActorRef] = List()
  var elGroup: List[ActorRef] = List()
  def receive() = {
    case AddReMember(worker) => {
      reGroup = reGroup.::(worker)
      if (reGroup.length == re_count) {
        implicit val timeout = Timeout(20000)
        Await.result(ask(santa, IncReWaitCount), timeout.duration)
        santa ! Reindeer(reGroup.toList)
        reGroup = List()
      }
    }
    case AddElfMember(worker) => {
      elGroup = elGroup.::(worker)
      if (elGroup.length == el_count) {
        santa ! Elves(elGroup.toList)
        elGroup = List()

      }
    }
  }
}

case class SantaWorkerGo(santa: ActorRef)
case class StartSantaWorker

class SantaWorker(isRe: Boolean, secretary: ActorRef) extends Actor {
  var serviced = false
  def receive() = {
    case StartSantaWorker => if (isRe) secretary ! AddReMember(self) else secretary ! AddElfMember(self)
    case SantaWorkerGo(santa) => serviced = true; santa ! Leave(self)
  }
}