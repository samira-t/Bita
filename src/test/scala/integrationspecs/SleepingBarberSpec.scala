package test
package integrationspecs

import scala.collection.mutable.{ HashMap, ArrayBuffer, Queue }
import akka.bita.Scheduler
import akka.actor.{ ActorSystem, Actor, Props, ActorRef }
import akka.bita.pattern.Patterns._
import akka.dispatch.Await
import akka.util.Timeout
import akka.dispatch.DefaultPromise
import akka.dispatch.{ Promise, Future }
import bita.{ ScheduleEnvelope, LogicalMessage, EventID }
import sample._
import bita.util.FileHelper
import bita.criteria._
import bita.ScheduleOptimization._
import bita.ScheduleOptimization
import bita.util.TestHelper
import org.scalatest._
import akka.bita.RandomScheduleHelper

/**
 * @author Samira Tasharofi
 *
 */

class BarberSpec extends bita.util.TestHelper with FunSpec {

  val name = "barber"
  val round = 0

  // feel free to change these parameters to test the barber with various configurations.
  var capacity = 2
  var customerNum = 3
  implicit val timeout = Timeout(100000)

  // test with PMHR criterion
  val criterion = PMHRCriterion

  var allTracesDir = "test-results/barber/%s_%s_%s/%s/".format(name, capacity, customerNum, round)
  var randomTracesDir = allTracesDir + "random/"
  var randomTracesTestDir = allTracesDir + "random-test/"

  var generatedSchedulesNum = -1

  describe("Barber Test") {

    it(" should test randomly within a timeout", Tag("random-timeout")) {
      /* 5 minutes timeout 5*60 = 300 sec */
      testRandomByTime(name, randomTracesTestDir, 300)
    }

    // Generates a random trace which will be used for schedule generation.
    it(" should generate a random trace", Tag("random")) {

      FileHelper.emptyDir(randomTracesDir)
      var traceFiles = FileHelper.getFiles(randomTracesDir, (name => name.contains("-trace.txt")))
      var traceIndex = traceFiles.length + 1
      var newTraceName = name + "-random%s-trace.txt".format(traceIndex)
      testRandom(name, randomTracesDir, 1)
    }

    it(" should generate schedules ", Tag("generate")) {
      var randomTrace = FileHelper.getFiles(randomTracesDir, (name => name.contains("-trace.txt")))
      for (opt <- criterion.optimizations.-(NONE)) {
        var scheduleDir = allTracesDir + "%s-%s/schedules/".format(criterion.name, opt)
        FileHelper.emptyDir(scheduleDir)
        generateSchedules(name, randomTrace, scheduleDir, criterion, opt, -1)
      }
    }

    it(" should test the generated schddules ", Tag("test")) {

      for (opt <- criterion.optimizations.-(NONE)) {
        var scheduleDir = allTracesDir + "%s-%s/schedules/".format(criterion.name, opt)

        var traceFiles = FileHelper.getFiles(scheduleDir, (name => name.contains("-trace.txt")))
        var scheduleIndex = traceFiles.length + 1
        var newScheduleFileName = name + "-%s-schedule.txt".format(scheduleIndex)
        testGeneratedSchedules(scheduleDir)
      }
    }

    it(" should generate and test schedules ", Tag("generate-test")) {

      var randomTrace = FileHelper.getFiles(randomTracesDir, (name => name.contains("-trace.txt")))
      for (opt <- criterion.optimizations.-(NONE)) {
        var scheduleDir = allTracesDir + "%s-%s/schedules/".format(criterion.name, opt)

        FileHelper.emptyDir(scheduleDir)
        generateAndTestGeneratedSchedules(name, randomTrace, scheduleDir, criterion, opt, -1)
      }
    }
    it(" should measure the coverage of testing with schedules ", Tag("coverage")) {

      // The number of traces after which the coverage should be measured.
      var interval = 5
      for (opt <- criterion.optimizations.-(NONE)) {
        var scheduleDir = allTracesDir + "%s-%s/schedules/".format(criterion.name, opt)
        var randomTraces = FileHelper.getFiles(randomTracesDir, (name => name.contains("-trace.txt")))
        FileHelper.copyFiles(randomTraces, scheduleDir)

        var resultFile = scheduleDir + "%s-%s-result.txt".format(criterion.name, opt)
        var traceFiles = FileHelper.getFiles(scheduleDir, (name => name.contains("-trace.txt")))
        traceFiles = FileHelper.sortTracesByName(traceFiles, "-%s-")
        criterion.measureCoverage(traceFiles, resultFile, interval)
      }

    }
  }

  def run {

    system = ActorSystem()
    RandomScheduleHelper.setSystem(system)

    var promise = Promise[HashMap[CustomerState.Value, Int]]()(system.dispatcher)

    var monitor = system.actorOf(Props(new Monitor(customerNum, promise)))
    var barber = system.actorOf(Props(new Barber))

    var waitingRoom = system.actorOf(Props(new WaitingRoom(capacity, barber)))
    var customers = new Array[ActorRef](customerNum)

    for (i <- 0 to customerNum - 1) {
      customers(i) = system.actorOf(Props(new Customer(i + "", waitingRoom, monitor)))
    }

    for (i <- 0 to customerNum - 1) {
      customers(i) ! Go
    }

    var result = Await.result(promise.mapTo[HashMap[CustomerState.Value, Int]], timeout.duration)

    if (result.asInstanceOf[HashMap[CustomerState.Value, Int]].contains(CustomerState.Exception))
      bugDetected = true

    println(result)

  }

}

