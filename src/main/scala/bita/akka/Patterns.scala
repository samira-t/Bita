package akka.bita
package pattern

/**
 * @author Samira Tasharofi (tasharo1@illinois.edu)
 */

import akka.actor.ActorRef
import akka.dispatch.Future
import akka.util.Duration
import akka.util.Timeout
import akka.dispatch.Envelope
import akka.actor.ActorSystem
import akka.actor.InternalActorRef
import akka.dispatch.Promise
import akka.pattern.AskTimeoutException
import akka.pattern.PromiseActorRef
import Scheduler._
import bita.util._

object Patterns {

  /**
   * The calls for <code>ask</code> or <code>?</code> to the Akka library should be replaced 
   * with a call to this method. 
   * The purpose is to record some information which will be used when computing happens-before
   * relation and enforcing a schedule.
   */
  def ask(actorRef: ActorRef, message: Any)(implicit sender: ActorRef = null, timeout: Timeout): Future[Any] = {
    actorRef match {
      case ref: InternalActorRef if ref.isTerminated =>
        actorRef.tell(message)
        Promise.failed(new AskTimeoutException("sending to terminated ref breaks promises"))(ref.provider.dispatcher)
      case ref: InternalActorRef =>
        val provider = ref.provider
        if (timeout.duration.length <= 0) {
          actorRef.tell(message)
          Promise.failed(new AskTimeoutException("not asking with negative timeout"))(provider.dispatcher)
        } else {
          val a = PromiseActorRef(provider, timeout)
          if (sender == null) {
            Scheduler.addPromiseToParentMap(a, ActorPathHelper.DeadLetterActorPath)
          } else {
            Scheduler.addPromiseToParentMap(a, sender.path.toString())
          }
          if (!actorRef.isTerminated)
          actorRef.tell(message, a)
          var res = a.result
          return res
        }
      case _ => throw new IllegalArgumentException("incompatible ActorRef " + actorRef)
    }
  }
}