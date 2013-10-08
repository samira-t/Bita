package test
package sample

import akka.actor.{ ActorSystem, Actor, Props, ActorRef }
import akka.dispatch.Await
import akka.util.Timeout
import akka.bita.pattern.Patterns._
import java.util.concurrent.CountDownLatch

case object LoggedOn
case class Stop(reason: String)
case object ReceiverNotFound
case object Sent
case object LoggedOff
case object NoReply
case class MessageFrom(fromName: String, message: String)

case class MessageTo(toName: String, message: String)
case object Logoff
case object StartMessenger

class MessengerClient(name: String, server: ActorRef, bugLatch:CountDownLatch,msgLatch:CountDownLatch) extends Actor {
  var receivedMsg: Object = null
  implicit val timeout = Timeout(20000)

  def notLoggedIn:Receive = {
    case StartMessenger => {
      var r = Await.result(ask(server, Server_Logon(self, name)), timeout.duration)
      r match {
        case LoggedOn => context.become(loggedIn)
        case Stop(reason) => {
          println("cannot login again")
          //exit(reason)
          bugLatch.countDown()
          msgLatch.countDown()
        }
      }
    }
    case msg => println("unexpected message" +msg + name)
      bugLatch.countDown()
  }
  
  def loggedIn:Receive = {
    
    case msg@MessageTo(toName, message) => {
      //println(msg)
      var r = Await.result(ask(server, Server_MessageTo(self, toName, message)), timeout.duration)
      r match {
        case Sent => ()
        case Stop(reason) => 
          println("Stop")
          msgLatch.countDown()
          //context.become(notLoggedIn)
        
        case ReceiverNotFound => 
          receivedMsg = ReceiverNotFound
          msgLatch.countDown()
      }
    }

    case Logoff => {
      server ! Server_Logoff(self)
      sender ! 'OK
      println("log off")
      context.become(notLoggedIn)
    }

    case MessageFrom(fromName, message) => {
      receivedMsg = MessageFrom(fromName, message)
      println("Message received from " + fromName + ": " + message)
      msgLatch.countDown()

    }
    
    case msg => println("unexpected message"+ msg)
      bugLatch.countDown()
    
  }
  
  def receive() = notLoggedIn
  
  
}

case class Server_Logon(from: ActorRef, name: String)
case class Server_Logoff(from: ActorRef)
case class Server_MessageTo(from: ActorRef, to: String, message: String)

class MessengerServer extends Actor {
  var users: Map[ActorRef, String] = Map[ActorRef, String]()

  def receive() = {
    case Server_Logon(from, name) => {
      if (users.valuesIterator.contains(name)) {
        sender ! Stop("user_exists_at_other_node")
      } else {
        sender ! LoggedOn
        users = users.+(from -> name)
      }
    }
    case Server_Logoff(from) => {
      if (users.keySet.contains(from)) {
        users = users.-(from)
      }
    }
    case Server_MessageTo(from, to, message) => {
      val fromName = users(from)
      //check if the sender is logged on
      if (fromName != null) {
        //check if the receiver is logged on
        users.find(user => user._2.equals(to)) match {
          case Some(user) => user._1 ! MessageFrom(fromName, message); sender ! Sent
          case None => sender ! ReceiverNotFound
        }
      } else {
        sender ! Stop("you_are_not_logged_on")
      }
    }
  }
}