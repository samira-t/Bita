//package test
//package integrationspecs
//
//import akka.actor.{ ActorSystem, Actor, Props, ActorRef }
//import test.util.TestHelper
//import bita.criteria._
//import test.sample._
//import bita.util.FileHelper
//import akka.util.Timeout
//import akka.dispatch.Await
//import akka.bita.pattern.Patterns._
//import akka.bita.RandomScheduleHelper
//
//class QuickSortSpec extends TestHelper {
//  implicit val timeout = Timeout(30000)
//
//  val criteria = Array[Criterion]( /*PCRCriterion, */ PRCriterion)
//  val name = "quicksort"
//
//  var allTracesDir = "src/test/scala/integrationspecs/quicksort/"
//  var generatedSchedulesNum = -1
//  var maxSchedule = 100
//  var detailInterval = 5
//  var maxRandomTraces = 10
//
//  var input1: Array[Int] = Array[Int](12, 30, 11, 40, 78, 20, 10, 13)
//
//  "Different optimizations" should "tested for quicksort" in {
//
//    var randomTraceDir = allTracesDir + "random-schedule/"
//    //FileHelper.emptyDir(randomTraceDir)
//    //generateRandomTraces(name, randomTraceDir, maxRandomTraces)
//    var randomTraces = FileHelper.getFiles(randomTraceDir, (name => name.contains("-trace.txt")))
//    randomTraces = FileHelper.sortTracesByName(randomTraces, "-random%s-")
//
//    for (criterion <- criteria) {
//      for (opt <- criterion.optimizations) {
//
//        var scheduleDir = allTracesDir + "%s-%s/".format(criterion.name, opt)
//
//        FileHelper.emptyDir(scheduleDir)
//
//        var usedRandomTraces = //generateSchedules(name, randomTraces, scheduleDir, criterion, opt, maxSchedule)
//          generateAndTestGeneratedSchedules(name, randomTraces, scheduleDir, criterion, opt, maxSchedule)
//        FileHelper.copyFiles(usedRandomTraces, scheduleDir)
//
//        var resultFile = scheduleDir + "%s-%s-coverage-int%s.txt".format(criterion.name, opt, detailInterval)
//        var traceFiles = FileHelper.getFiles(scheduleDir, (name => name.contains("-trace.txt")))
//        traceFiles = FileHelper.sortTracesByName(traceFiles, "-%s-")
//        //generatedSchedulesNum = traceFiles.length
//        criterion.measureCoverage(traceFiles, resultFile, detailInterval)
//      }
//    }
//  }
//
//  def run() {
//    system = ActorSystem()
//    RandomScheduleHelper.setSystem(system)
//
//    var qsort = system.actorOf(Props(new QuickSort()))
//
//    var result1 = Await.result(ask(qsort, Sort(input1)), timeout.duration)
//    //    var result2 = Await.result(ask(qsort, Sort(input2)), timeout.duration)
//    result1 match {
//      case Result(result) =>
//        assert(isSorted(result, input1))
//    }
//    //    result2 match {
//    //      case Result(result) =>
//    //        assert(isSorted(result, input2))
//    //    }
//  }
//
//  def isSorted(result: Array[Int], input: Array[Int]): Boolean = {
//    //check if the result if sorted array of input
//    if (input.size == result.size) {
//      if (result.size > 0) {
//        var inputListSorted = input.toList.sort((e1, e2) => (e1 < e2))
//        for (i <- 0 to result.size - 1) {
//          if (result(i) != inputListSorted(i)) return false
//        }
//      }
//      return true
//    } else return false
//  }
//}