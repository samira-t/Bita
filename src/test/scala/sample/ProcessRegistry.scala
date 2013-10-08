package test
package sample

import akka.actor._
import akka.actor.Actor._
import akka.bita.pattern.Patterns._
import java.util.Random
import akka.actor.{ ActorSystem, Actor, Props, ActorRef }
import scala.collection.mutable.{ HashMap, HashSet }
import akka.util.Timeout
import akka.dispatch.Await
import java.util.concurrent.CountDownLatch

/**
 * @author Samira Tasharofi
 *
 * (Translated from Erlang)
 * http://portal.acm.org/citation.cfm?id=1596574
 */

case class Reg(pid: Int)
case class Where(name: String)
case class Unreg(name: String)
case class Send(name: String, msg: String)
case class Audit(name: String)
case class Down(pid: Int)

class RegistryServer(ets: ActorRef, runTime: ActorRef, finishLatch: CountDownLatch) extends Actor {
  implicit val timeout = Timeout(20000)

  def receive() = {
    case Reg(pid: Int) ⇒ {
      Logger.log("Reg(pid: Int)")
      val monitor = context.actorOf(Props(new ProcessMonitor(pid, runTime, self, finishLatch)))
      monitor ! 'Start
      val result = Await.result(ask(ets, InsertNewBackward(pid, monitor)), timeout.duration)
      Logger.log("registered: " + pid)
      finishLatch.countDown()
    }
    case Audit(name) ⇒ {
      Logger.log("Audit(name)")
      val pid = Await.result(ask(ets, Lookup(name)), timeout.duration).asInstanceOf[Int]
      val alive = Await.result(ask(runTime, IsProcessAlive(pid)), timeout.duration).asInstanceOf[Boolean]
      if (!alive) {
        Await.result(ask(ets, DeleteEntry(pid)), timeout.duration)
      }
      sender.!()
    }
    case Down(pid: Int) ⇒ {
      Logger.log("Down(pid: Int)" + sender)
      if (finishLatch.getCount() > 0)
        if (Await.result(ask(ets, GetMatch(pid)), timeout.duration) != None)
          Await.result(ask(ets, DeleteEntry(pid)), timeout.duration)
    }
    case Unreg(name) ⇒ {
      Logger.log("Unreg(name)")
      val pid = Await.result(ask(ets, Lookup(name)), timeout.duration).asInstanceOf[Int]
      Await.result(ask(ets, DeleteEntry(pid)), timeout.duration)
      sender.!()
    }
    case Where(name) ⇒ {
      val pid = Await.result(ask(ets, Lookup(name)), timeout.duration).asInstanceOf[Int]
      Logger.log("Where(name) ")
      if (Await.result(ask(runTime, IsProcessAlive(pid)), timeout.duration).asInstanceOf[Boolean])
        sender.!(pid)
      else sender.!(-1)
      //Logger.log("Where(name) ")
    }
  }
}

case class Register(name: String, pid: Int)
class Client(server: ActorRef, runTime: ActorRef, ets: ActorRef, registerCountLatch: CountDownLatch, exceptionLatch: CountDownLatch) extends Actor {
  var pid = -1
  implicit val timeout = Timeout(20000)
  var name = ""
  var exceptionIsThrown = false
  def receive() = {
    case Spawn(name) ⇒ {
      val id = Await.result(ask(runTime, Spawn(name)), timeout.duration).asInstanceOf[Int]
      sender.!(id)
    }
    case Kill(pid) ⇒ {
      Await.result(ask(runTime, Kill(pid)), timeout.duration)
    }
    case Register(name, pid) ⇒ {
      reg(name, pid)
      //self.stop
    }
  }

  def reg(name: String, pid: Int) {
    var result = Await.result(ask(ets, InsertNewForward(name, pid)), timeout.duration).asInstanceOf[Boolean]
    if (!result) {
      var pidInTable = Await.result(ask(server, Where(name)), timeout.duration).asInstanceOf[Int]
      if (pidInTable == -1) {
        /**/
        Await.result(ask(server, Audit(name)), timeout.duration)
        result = Await.result(ask(ets, InsertNewForward(name, pid)), timeout.duration).asInstanceOf[Boolean]
        if (!result) {
          pidInTable = Await.result(ask(server, Where(name)), timeout.duration).asInstanceOf[Int]
          if (!result && pidInTable == -1) {
            exceptionIsThrown = true
            //throw new Exception()
            Logger.log("*************************************88Exception")
            println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% eaxception is thrown")
            registerCountLatch.countDown()
            exceptionLatch.countDown()
            //throw new Exception()
            //context.stop(self)
            //self.stop
          }
        } else {
          server ! Reg(pid)
        }
      } else {
        registerCountLatch.countDown()
        println(name + ": could not register")
      }
    } else {
      server ! Reg(pid)
    }
  }
}

class ProcessMonitor(pid: Int, runTime: ActorRef, server: ActorRef, finishLatch: CountDownLatch) extends Actor {
  implicit val timeout = Timeout(10000)

  //  override def preStart() {
  //    self ! 'Start
  //  }
  def receive() = {
    case 'Start ⇒
      try {
        //if (finishLatch.getCount > 0) {
        var alive = Await.result(ask(runTime, IsProcessAlive(pid)), timeout.duration).asInstanceOf[Boolean]
        //          while (alive && finishLatch.getCount > 0) {
        //            Thread.sleep(100)
        //           // if (finishLatch.getCount > 0)
        //            alive = Await.result(ask(runTime, IsProcessAlive(pid)), timeout.duration).asInstanceOf[Boolean]
        //          }
        if (!alive) {
          server ! Down(pid)
        }
        else {
          self ! 'Start
        }
        Logger.log("Down sent")
        //  }
      } catch {
        case ex: Throwable =>
          println(ex.printStackTrace())
          //context.stop(self)
      }

    //context.stop(self)
    //self.stop()
  }
}

case class InsertNewForward(name: String, pid: Int)
case class InsertNewBackward(pid: Int, monitor: ActorRef)
case class Lookup(name: String)
case class GetMatch(pid: Int)
case class DeleteEntry(pid: Int)

class ETS extends Actor {
  var forwardTable = HashMap[String, Int]()
  var backwardTable = HashMap[Int, ActorRef]()
  println("started ETS")
  def receive() = {
    case InsertNewForward(name, pid) ⇒ {
      Logger.log("insert forward")
      if (forwardTable.contains(name)) {
        sender.!(false)
      } else {
        forwardTable.+=((name, pid))
        sender.!(true)
        /**/ //self.reply(false)
      }
    }
    case InsertNewBackward(pid, monitor) ⇒ {
      Logger.log("InsertNewBackward")
      if (backwardTable.contains(pid)) {
        sender.!(false)
      } else {
        backwardTable.+=((pid, monitor))
        /**/
        sender.!(true)
      }
    }
    case Lookup(name) ⇒ {
      Logger.log("Lookup(name) ")
      forwardTable.get(name) match {
        case None ⇒ sender.!(-1)
        case Some(pid) ⇒ sender.!(pid)
      }
    }
    case GetMatch(pid) ⇒ {
      Logger.log("GetMatch")
      sender.!(backwardTable.get(pid))
    }
    case DeleteEntry(pid) ⇒ {
      Logger.log("DeleteEntry")
      backwardTable.get(pid) match {
        case None ⇒ sender.!()
        case Some(monitor) ⇒ {
          var name = ""
          val forwardElements = forwardTable.elements
          while (forwardElements.hasNext && name.equals("")) {
            val elem = forwardElements.next
            if (elem._2 == pid)
              name = elem._1
          }
          forwardTable.-=(name)
          backwardTable.-=(pid)
          sender.!()
        }
      }
    }
  }
}

case class Spawn(name: String)
case class Kill(pid: Int)
case class IsProcessAlive(pid: Int)

class RunTime extends Actor {
  private var currentID = 0
  private var alive = HashSet[Int]()
  private var dead = HashSet[Int]()
  def receive() = {
    case Spawn(name: String) ⇒ {
      currentID += 1
      alive.add(currentID)
      sender.!(currentID)
    }
    case Kill(pid: Int) ⇒ {
      dead.add(pid)
      alive.remove(pid)
      sender.!()
    }

    case IsProcessAlive(pid: Int) ⇒ {
      sender.!(alive.contains(pid))
    }

  }
}

object Logger {
  var debug = true
  def log(s: String) {
    if (debug) println(s)
  }
}