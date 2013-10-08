package bita
package util

/**
 * @author Samira Tasharofi (tasharo1@illinois.edu)
 */

import java.io.File
import akka.actor.ActorSystem
import java.io.FileWriter
import scala.collection.mutable.ArrayBuffer
import ScheduleOptimization._
import criteria._
import schedulegeneration._
import akka.bita.{ Scheduler, RandomScheduleHelper }

/**
 * A helper for testing with Bita.
 */
trait TestHelper {

  var bugDetected = false
  var reportBug = false
  var tracesWithBug = ArrayBuffer[(String, Long)]()
  var curTraceFile = ""
  var startTime = 0L
  var endTime = 0L

  var timeReport = ""

  var system: ActorSystem = _

  def generateRandomTrace(traceFile: String) {
    curTraceFile = traceFile
    Scheduler.setTraceName(traceFile)
    run()
    Scheduler.finish(traceFile)
    println("======================================================")
    afterEach()
  }

  /**
   * Executes the run method <code>num</code> times. The goal is to
   * obtain some random execution traces.
   * 
   * @param name (the name of program which will be used in creating trace file)
   * @param workingDir (the directory in which the trace file will be written)
   * @param num (the number of times the program will be tested randomly)
   */
  def testRandom(name: String, workingDir: String, num: Int) {
    startTime = System.currentTimeMillis()

    var traceFileName = workingDir + name + "-random%s-trace.txt"
    for (i <- 1 to num) {
      generateRandomTrace(traceFileName.format(i))
    }
    var end = System.currentTimeMillis()
    timeReport += "Random scheduling time for %s runs: %s sec \n".format(num, (end - startTime) / 1000)
    reportBugsAndTime(workingDir)
  }

  /**
   * Executes the run method multiple times until the timeout reaches. The goal
   * is to test randomly within a timeout and obtain some random execution traces.
   */
  def testRandomByTime(name: String, traceFilesPath: String, timeoutsec: Long) {
    var traceFileName = traceFilesPath + name + "-random%s-trace.txt"

    startTime = System.currentTimeMillis()
    var end = System.currentTimeMillis()
    var timePassed = (end - startTime) / 1000

    var i = 1
    while (timePassed < timeoutsec && tracesWithBug.size == 0) {
      generateRandomTrace(traceFileName.format(i))
      end = System.currentTimeMillis()
      timePassed = (end - startTime) / 1000
      i += 1
    }
    timeReport += "Random scheduling time for %s runs: %s sec \n".format(i, (end - startTime) / 1000)
    reportBugsAndTime(traceFilesPath)
  }

  /**
   * Tests with a schedule.
   */
  def testSchedule(scheduleFilePath: String) {
    println("test schedule: " + scheduleFilePath)
    Scheduler.setSchedule(scheduleFilePath)
    run()
    var traceFile = scheduleFilePath.replace(".txt", "-trace.txt")
    Scheduler.finish(traceFile)
    println("======================================================")

    curTraceFile = traceFile
    afterEach()
  }

  /**
   * Generates schedules given random execution traces and the criterion.
   */
  def generateSchedules(name: String, randomTracesPath: Array[String], generatedScheudlesDir: String, criterion: Criterion,
    optimization: ScheduleOptimization, maxSchedule: Int = -1): Array[String] = {

    var start = System.currentTimeMillis()
    val scheduleGenerator =
      criterion match {
        case PRCriterion => PRScheduleGenerator
        case PCRCriterion => PCRScheduleGenerator
        case PMHRCriterion => PMHRScheduleGenerator
      }

    scheduleGenerator.maxSchedule = maxSchedule
    val schedules = scheduleGenerator.generateSchedules(name: String, randomTracesPath, generatedScheudlesDir, optimization)

    var end = System.currentTimeMillis()
    var report = "Schedule generation time %s-%s for %s schedules: %s sec \n".format(criterion.name, optimization, schedules.size, ((end - start) / 1000))
    timeReport += report
    reportTimeForGeneration(generatedScheudlesDir, report, criterion.name, optimization)

    return schedules
  }

  /**
   * Tests with the schedules in the <code>schedulePath</code>.
   */
  def testGeneratedSchedules(schedulesPath: String) {
    var start = System.currentTimeMillis()
    println("\n========= testing generated schedules in %s ===============".format(schedulesPath))
    var scheduleFiles = FileHelper.getFiles(schedulesPath, (fileName => fileName.contains("-schedule.txt")))
    scheduleFiles = FileHelper.sortTracesByName(scheduleFiles, "-%s-")

    for (scheduleFileAbsolutePath <- scheduleFiles) {
      var traceFile = scheduleFileAbsolutePath.replace(".txt", "-trace.txt")
      testSchedule(scheduleFileAbsolutePath)
      afterEach()
    }

    var end = System.currentTimeMillis()
    timeReport += "Test generated schedules time for %s schedules: %s sec \n".format(scheduleFiles.size, ((end - startTime) / 1000))
    reportBugsAndTime(schedulesPath)

  }

  /**
   * Reports the time and schedules in which the test fails.
   */
  private def reportBugsAndTime(schedulesPath: String) {
    if (reportBug) {
      var writer = FileHelper.getWriter(schedulesPath + "time-bug-report.txt")
      writer.write(timeReport)
      println(timeReport)
      var report = "******* NUMBER OF TRACES WITH BUG = " + tracesWithBug.length
      writer.write(report + "\n")
      println(report)
      report = "******* TRACES WITH BUG *******"
      writer.write(report + "\n")
      for ((trace, time) <- tracesWithBug) {
        writer.write("Time= " + time + ", " + trace + "\n");
      }
      writer.close()
      tracesWithBug.clear()
      timeReport = ""
    }
  }

  /**
   * Reports the time of schedule generation.
   */
  private def reportTimeForGeneration(schedulesPath: String, report: String,
    criterion: String, opt: ScheduleOptimization) {
    var writer = FileHelper.getWriter(schedulesPath + "../" +
      "time-gen_%s_%s.txt".format(criterion, opt), true)
    writer.write(report)
    println(report)
    writer.write(report + "\n")
    writer.close()
  }

  /**
   * Generates schedules using the initial random traces and tests with the generated schedules.
   */
  def generateAndTestGeneratedSchedules(name: String, randomTracesPath: Array[String], generatedScheudlesDir: String,
    criterion: Criterion, optimization: ScheduleOptimization, maxSchedule: Int = -1): Array[String] = {
    startTime = System.currentTimeMillis()
    var usedTraces = generateSchedules(name, randomTracesPath, generatedScheudlesDir, criterion, optimization, maxSchedule)
    testGeneratedSchedules(generatedScheudlesDir)
    endTime = System.currentTimeMillis()
    return usedTraces
  }

  /**
   * Obtains random initial traces, generates schedules and tests with the generated schedules.
   */
  def runGenerateSchedulesAndTest(name: String, workingDir: String, randomTraceNum: Int = 1, criterion: Criterion, optimization: ScheduleOptimization = NONE) {
    var randomTraces = new Array[String](randomTraceNum)
    for (i <- 1 to randomTraceNum) {
      var randomTracePath = workingDir + name + "-random" + i + "-trace.txt"
      randomTraces(i - 1) = randomTracePath
      generateRandomTrace(randomTracePath)
    }
    generateAndTestGeneratedSchedules(name, randomTraces, workingDir, criterion, optimization)
  }

  /**
   * The method that runs the program under test. Should be implemented in test.
   */
  def run()

  def waitForSchedule() {
    Scheduler.emptyScheduleLatch.await()
  }

  /**
   * It is called after each invocation to <code>run</code> method to clear
   * the state and shutdown the actor system.
   */
  def afterEach() {
    RandomScheduleHelper.reset()
    if (bugDetected) {
      println("***********BUG DETECTED**************")
      var end = System.currentTimeMillis()
      tracesWithBug.+=((curTraceFile, (end - startTime) / 1000))
    }
    if (system != null) {
      system.shutdown()
      system.awaitTermination()
    }
    bugDetected = false
    Scheduler.reset()
  }

}

