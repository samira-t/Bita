package test
package sample

import scala.collection.mutable.Queue

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.dispatch.Await
import akka.dispatch.Future
import akka.bita.pattern.Patterns._
import akka.util.Timeout
import akka.bita.Scheduler
import bita.schedulegeneration.ScheduleGenerator
import org.scalatest._

class BoundedBuffer2(size: Int) extends Actor {

  var buffer = new Queue[Int]()

  def receive() = {
    case Get => {
      if (buffer.size > 0) sender ! buffer.dequeue()
      else sender ! -1
    }
    case Put(x: Int) => {
      if (buffer.size < size) {
        add(x)
      }
    }
  }

  def add(x: Int) {
    buffer.enqueue(x)
  }

  def testReceive: Receive = {
    case Get => {
      if (buffer.size > 0) sender ! buffer.dequeue()
      else sender ! -1
    }
    case Put(x: Int) => {
      if (buffer.size < size) {
        add(x)
      }
    }
  }
}

class Producer2(buffer: ActorRef) extends Actor {

  def receive() = {
    case CallPut(x: Int) => {
      buffer ! Put(x)
      context.stop(buffer)
    }
  }

}

class Consumer2(buffer: ActorRef) extends Actor {
  var a: ActorRef = null
  implicit val timeout = Timeout(5000)
  def receive = {
    case CallGet => {
      if (a == null)
        a = context.actorOf(Props(new BoundedBuffer2(3)), "aaakk")
      var result = Await.result(ask(buffer, Get).mapTo[Int], timeout.duration)
      sender ! result
      //      context.stop(a)
    }
    case CallPut(x) => {
      if (a != null)
        a ! Put(x)
      //      var future = ask(buffer, GetProfiler).mapTo[Int]
      //      var int = Await.result(future, timeout.duration)
    }
  }
}
