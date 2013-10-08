package test
package sample

import akka.actor.{ ActorSystem, Actor, Props, ActorRef }
import scala.util.Random
import java.util.concurrent.CountDownLatch

case class Sort(input: Array[Int])
case class Result(res: Array[Int])

class QuickSort extends Actor {
  var result_part1 = Array[Int]()
  var result_part2 = Array[Int]()
  var middle = Array[Int]()

  var requester: ActorRef = null
  var receivedResultsCount = 0

  def receive() = {
    case Sort(input: Array[Int])  => {
      requester = sender
      dispatch(input)
    }
    case Result(res) => {
      if (receivedResultsCount == 0) result_part1 = res
      else result_part2 = res
      receivedResultsCount += 1
      if (receivedResultsCount == 2) {
        val finalResult = mergeResults()
        requester ! Result(finalResult)
      }
    }
  }

  private def dispatch(xs: Array[Int]) {
    if (xs.length <= 1) requester ! Result(xs)
    else {
      receivedResultsCount = 0

      val pivot = xs(0)
      val left = (xs filter (pivot >))
      val right = (xs filter (pivot <))
      
      var sortLeft = context.actorOf(Props(new QuickSort()))
      var sortRight = context.actorOf(Props(new QuickSort()))
      sortLeft ! Sort(left)
      sortRight ! Sort(right)

      middle = (xs filter (pivot ==))
    }
  }

  private def mergeResults(): Array[Int] = {

    // merge result_part1, result_part2, and middle
    var finalResult: Array[Int] = null
    if (result_part1.isEmpty) {
      if (middle.first <= result_part2.first)
        finalResult = middle ++ result_part2
      else
        finalResult = result_part2 ++ middle
    } else if (result_part2.isEmpty) {
      if (middle.first <= result_part1.first)
        finalResult = middle ++ result_part1
      else
        finalResult = result_part1 ++ middle
    } else {
      if (result_part1.last <= result_part2.first)
        finalResult = result_part1 ++ middle ++ result_part2
      else
        finalResult = result_part2 ++ middle ++ result_part1
    }
    return finalResult
  }
}