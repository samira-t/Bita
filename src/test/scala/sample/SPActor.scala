package test
package sample

import akka.actor.{ ActorSystem, Actor, Props, ActorRef }
import scala.collection.mutable.HashMap

/**
 * Finds the shortest path from the root node to other nodes in a directed graph
 *
 * The root node is specified by sending a message SpMsg with d = 0 and  w = 0
 *
 */

/* 
* "d" is the neighbor's(sender's) current distance and "w" is distance of the receiver from neighbor(sender)
*/
case class SpMsg(d: Int, w: Int)
case class AddNeighbor(n: ActorRef, d: Int)

class SPActor(i:Int) extends Actor {
  var distance = -1
  var parent: ActorRef = null

  private var neighbors = HashMap[ActorRef,Int]()//: List[(ActorRef, Int)] = Nil

  def addNeighbor(n: ActorRef, d: Int) {
    //neighbors = neighbors ::: List((n, d))
    neighbors.+=(n->d)
  }

  def receive() = {
    case msg @ SpMsg(d, w) => {
      if (distance == -1 || distance > (d + w)) {
        distance = (d + w)
        parent = sender
        //println(i+": "+distance)
        for ((spProcess,nw) <- neighbors)
          spProcess ! SpMsg(distance, nw)
      }
    }
    case AddNeighbor(n: ActorRef, d: Int) => {
      addNeighbor(n, d)
      sender ! 'OK
    }
  }
}