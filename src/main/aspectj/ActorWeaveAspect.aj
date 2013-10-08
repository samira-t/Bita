package bita;

import akka.actor.ActorSystem;
import akka.actor.ActorCell;
import akka.actor.ActorRef;
import akka.dispatch.Envelope;
import akka.dispatch.Dispatcher;
import akka.bita.Scheduler;
import akka.actor.Actor;
import akka.actor.ScalaActorRef;
import akka.actor.ActorContext;
import akka.pattern.*;

import java.util.Random;
import java.util.HashMap;
import scala.Tuple2;
import akka.bita.RandomScheduleHelper;
import akka.util.FiniteDuration;
  
  
privileged public aspect ActorWeaveAspect {

	private final Random random = new Random(Double.doubleToLongBits(Math
			.random()));
	private final int maxDelay = 200; // milliseconds

	HashMap<Tuple2<ActorRef, ScalaActorRef>, Long> senderReceiverToMinReceiveTime = new HashMap<Tuple2<ActorRef, ScalaActorRef>, Long>();

	void around(ScalaActorRef receiver, Object msg, ActorRef sender):
    execution(* akka.actor.ScalaActorRef.$bang(..)) &&
    args(msg, sender) &&
    target(receiver)
  {
		boolean isRandom = Scheduler.isRandom(receiver, msg, sender);
		if (isRandom) {
			FiniteDuration delay = RandomScheduleHelper.delay(receiver, msg,
					sender);
			if (delay == null) { // don't delay
				proceed(receiver, msg, sender);
			} else {
				// System.out.println("Random scheduling ...");
				final ScalaActorRef finalReceiver = receiver;
				final Object finalMsg = Scheduler.createLogicalMessageForSend(
						msg, sender, receiver);// msg;
				final ActorRef finalSender = sender;

				ActorSystem system = RandomScheduleHelper.getSystem();
				// The system may get null before checking the condition here
				if (system != null) {
					system.scheduler().scheduleOnce(delay, new Runnable() {
						public void run() {
							proceed(finalReceiver, finalMsg, finalSender);
						}
					});
				} else {
					proceed(receiver, msg, sender);
				}
			}
		} else
			proceed(receiver, msg, sender);
	}

	Object around(Dispatcher dispatcher, ActorCell receiver, Envelope envelop):
    execution(* akka.dispatch.Dispatcher.dispatch(..)) &&
    args(receiver, envelop, ..) &&
    target(dispatcher)
  {
		Envelope en = Scheduler.aroundSend(envelop, receiver);
		// System.out.println("new ::: calling dispatch ... ");
		if (en == null) {
			return null;
		} else
			return proceed(dispatcher, receiver, en);
	}

	Object around(ActorCell actorCell, Envelope envelope):
    execution(* akka.actor.ActorCell.invoke(..)) &&
    args(envelope, ..) &&
    target(actorCell)
  {

		Envelope en = Scheduler.aroundInvoke(envelope, actorCell);
		return proceed(actorCell, en);

	}

	Object around(akka.pattern.PromiseActorRef promiseActorRef,
			ActorRef sender, Object message):
    execution(* akka.pattern.PromiseActorRef.$bang(..)) &&
    args(message, sender, ..) &&
    target(promiseActorRef)
  {
		// System.out.println("Promise get message "+ message);
		Object realMessage = null;
		realMessage = Scheduler.aroundSendToPromise(promiseActorRef, message,
				sender);
		if (realMessage != null)
			return proceed(promiseActorRef, sender, realMessage);
		else
			return null;
	}

	after(Dispatcher dispatcher, ActorCell receiver, Envelope envelop):
    execution(* akka.dispatch.Dispatcher.dispatch(..)) &&
    args(receiver, envelop, ..) &&
    target(dispatcher)
  {
		Scheduler.checkForDispatch();
	}

	after(akka.pattern.PromiseActorRef promiseActorRef, ActorRef sender,
			Object message):
    execution(* akka.pattern.PromiseActorRef.$bang(..)) &&
    args(message, sender, ..) &&
    target(promiseActorRef)
  {
		Scheduler.checkForDispatch();
	}

	after(ActorCell actorCell, Envelope envelope):
    execution(* akka.actor.ActorCell.invoke(..)) &&
    args(envelope, ..) &&
    target(actorCell)
  {

		Scheduler.checkForDispatch();

	}

	after(ActorContext context):
    execution(* akka.actor.ActorContext.become(..)) &&
    target(context)
  {
		Scheduler.setCMH(context.self());
	}

	before(ActorContext context):
	    execution(* akka.actor.ActorContext.unbecome()) &&
	    target(context)
	  {
		// System.out.println("unbecome");
		Scheduler.setCMH(context.self());
	}

	Object around(akka.actor.ActorContext context) :
  	execution(* akka.actor.ActorRefFactory.actorOf(..)) &&
  	target(context)
  {
		Object child = proceed(context);
		Scheduler.addCreationEvent((ActorRef) child, context.self());
		return child;
	}

	after(akka.actor.ActorCell actorCell) returning(Actor actor):
  	execution(* akka.actor.ActorCell.newActor(..)) &&
  	target(actorCell)
  {
		Scheduler.startActor(actorCell, actor);

	}

	after(akka.actor.ActorContext context, akka.actor.ActorRef actor):
  	execution(* akka.actor.ActorRefFactory.stop(..)) &&
  	args(actor) && 
  	target(context)
  {
		Scheduler.addStopEvent(actor, context.self());
	}

}