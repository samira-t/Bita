package test
package sample

import akka.actor.{ ActorSystem, Actor, Props, ActorRef }
import scala.util.Random
import java.util.concurrent.CountDownLatch

/**
 * The description in the Groovy web page:
 * http: //code.google.com/p/gparallelizer/source/browse/trunk/src/test/groovy/groovyx/gpars/samples/actors/DemoLoadBalancer.groovy?r=1125
 * "Demonstrates work balancing among adaptable set of workers.
 * The load balancer receives tasks and queues them in a temporary task queue.
 * When a worker finishes his assignment, it asks the load balancer for a new task.
 * If the load balancer doesn't have any tasks available in the task queue, the worker is stopped.
 * If the number of tasks in the task queue exceeds certain limit, a new worker is created
 * to increase size of the worker pool."
 */

object random extends Random

case object StartWorking
case object NeedMoreWork
case object WorkToDo
case object EXIT

class LoadBalancer(queueSizeTrigger: Int, latch: CountDownLatch) extends Actor {
  var workers = 0
  var taskQueue = List[Any]()
  def receive() = {
    case NeedMoreWork => {
      if (taskQueue.length == 0) {
        println("No more tasks in the task queue. Terminating the worker.")
        sender ! EXIT
        workers -= 1
        if(workers == 0)
          latch.countDown()
      } else {
        val msg = taskQueue.head
        sender ! msg
        taskQueue = taskQueue.drop(1)
      }
    }
    case msg @ WorkToDo => {
      taskQueue ::= msg
      if ((workers == 0) || (taskQueue.length >= queueSizeTrigger)) {
        println("Need more workers. Starting one.")
        workers += 1
        var newWorker = context.actorOf(Props(new Worker(self)))
        newWorker ! StartWorking
      }
    }
  }
}

class Worker(balancer: ActorRef) extends Actor {
  def receive() = {
    case msg @ WorkToDo => {
      processMessage(msg)
      balancer ! NeedMoreWork
    }
    case EXIT => {
      context.stop(self)
    }
    case StartWorking => { balancer ! NeedMoreWork }
  }

  private def processMessage(msg: Any) {
    synchronized {
      Thread.sleep(random.nextInt(5000))
    }

  }
}