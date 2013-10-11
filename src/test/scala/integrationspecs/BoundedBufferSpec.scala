package test
package integrationspecs

import scala.collection.mutable.Queue
import akka.bita.Scheduler
import akka.actor.{ ActorSystem, Actor, Props, ActorRef }
import akka.bita.pattern.Patterns._
import akka.dispatch.Await
import akka.util.Timeout
import akka.dispatch.DefaultPromise
import akka.dispatch.Future
import org.scalatest._
import java.util.concurrent.CountDownLatch
import sample._
import bita.util.FileHelper
import bita.criteria._
import bita.ScheduleOptimization._
import bita.ScheduleOptimization
import scala.collection.mutable.ArrayBuffer
import bita.util._
import akka.bita.RandomScheduleHelper

class BufferSpec extends TestHelper with FlatSpec {

  val name = "buffer"
  val criteria = Array[Criterion]( PCRCriterion, PRCriterion)

  val bufferCapacity = 3
  val putMsgNum = 2
  val getMsgNum = 0

  var allTracesDir = "test-results/buffer/%s-%s-%s/".format(bufferCapacity, putMsgNum, getMsgNum)
  var generatedSchedulesNum = -1

  // Test the bounded buffer example with all criteria and optimizations.
  "Different optimizations" should "be tested for buffer" in {

    var randomTraceDir = allTracesDir + "random-new/"
    testRandom(name, randomTraceDir, 1)
    var randomTrace = FileHelper.getFiles(randomTraceDir, (name => name.contains("-trace.txt")))

    for (criterion <- criteria) {

      for (opt <- criterion.optimizations) {

        var scheduleDir = allTracesDir + "schedule-%s-%s/".format(criterion.name, opt)
        FileHelper.emptyDir(scheduleDir)
        FileHelper.deleteFiles(scheduleDir, (name => name.contains("-schedule.txt") || name.contains("-schedule-trace.txt")))
        generateAndTestGeneratedSchedules(name, randomTrace, scheduleDir, criterion, opt)
        
        // Measure the coverage and output the results into resultFile.
        var resultFile = scheduleDir + "schedule%s-result.txt".format(opt)
        // Must consider the first random trace for measuring the coverage.
        FileHelper.copyFiles(randomTrace,scheduleDir)
        var traceFiles = FileHelper.getFiles(scheduleDir, (name => name.contains("-trace.txt")))
        generatedSchedulesNum = traceFiles.length
        criterion.measureCoverage(traceFiles, resultFile)
      }
    }
  }
  
  def run() {

    // The number of messages that need to be processed in the buffer.
    val latch = new CountDownLatch(putMsgNum + getMsgNum)
    implicit val timeout = Timeout(30000)
    system = ActorSystem()
    RandomScheduleHelper.setSystem(system)
    
    var buffer = system.actorOf(Props(new BoundedBuffer(bufferCapacity, latch)))
    var prod = system.actorOf(Props(new Producer(buffer)))
    var cons = system.actorOf(Props(new Consumer(buffer)))

    for (i <- 1 to putMsgNum) {
      prod ! CallPut(i)
    }

    for (i <- 1 to getMsgNum) {
      val reply = Await.result(ask(cons, CallGet), timeout.duration)
      println("reply %s = %s".format(i, reply))
    }

    latch.await()
  }
}

