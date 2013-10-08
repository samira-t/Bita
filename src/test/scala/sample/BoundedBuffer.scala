package test
package sample
import scala.collection.mutable.Queue
import akka.bita.Scheduler
import akka.actor.{ ActorSystem, Actor, Props, ActorRef }
import akka.bita.pattern.Patterns._
import akka.dispatch.Await
import akka.util.Timeout
import akka.dispatch.DefaultPromise
import akka.dispatch.Future
import bita.{ ScheduleEnvelope, LogicalMessage, EventID }
import java.util.concurrent.CountDownLatch

case class Get
case class Put(x: Int)

class BoundedBuffer(size: Int, latch: CountDownLatch) extends Actor {

  var buffer = new Queue[Int]()

  def receive() = {
    case Get => {
      latch.countDown()

      if (buffer.size > 0) sender ! buffer.dequeue()
      else sender ! -1
    }
    case Put(x: Int) => {
      latch.countDown()

      if (buffer.size < size) {
        buffer.enqueue(x)
      }
    }
  }

}

case class CallPut(x: Int)
class Producer(buffer: ActorRef) extends Actor {

  def receive() = {
    case CallPut(x: Int) => {
      if (x > 0) {
        var prodChild = context.actorOf(Props(new ProducerChild(buffer)))
        prodChild ! CallPut(x)
      } else
        buffer ! Put(x)
    }
  }

}

class ProducerChild(buffer: ActorRef) extends Actor {

  def receive() = {
    case CallPut(x: Int) => {
      buffer ! Put(x)
    }
  }

}

case class CallGet
class Consumer(buffer: ActorRef) extends Actor {
  implicit val timeout = Timeout(20000)
  def receive() = {
    case CallGet => {
      var r = Await.result(ask(buffer, Get), timeout.duration)
      sender ! r
    }
  }
}



