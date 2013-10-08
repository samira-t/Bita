package bita
package util

/**
 * @author Samira Tasharofi (tasharo1@illinois.edu)
 */

import scala.collection.mutable.ArrayBuffer
import java.io.FileWriter
import sun.misc.IOUtils
import java.io.{ File, FileInputStream, FileOutputStream }
import akka.actor.ActorPath

/**
 * Provides various functions for paths of actors created by system.
 */
object ActorPathHelper {
  private var root: String = null

  def setRoot(actorPath: String) {
    var elements = actorPath.split("/")
    root = elements(0) + "/" + elements(1) + "/" + elements(2)
  }

  def reset() = root = null

  def rootIsSet = (root != null)

  def SystemGuardianActorPath = "%s/system".format(root)
  def UserGuardianActorPath = "%s/user".format(root)
  def DeadLetterActorPath = "%s/deadLetters".format(root)
  def HttpActorPath = "%s/http".format(root)
  def SchedulerActorPath = "%s/timer".format(root)

  def isSystemGuardian(actorPath: String) = actorPath.endsWith("/system")

  def isRoot(actorPath: String) = (root != null) && actorPath.endsWith("%s/".format(root))

  def isUserGuardian(actorPath: String) = actorPath.endsWith("/user")

  def isUserDeadLetter(actorPath: String) = actorPath.endsWith("/deadLetters")

  def isTemp(actorPath: String) = actorPath.contains("/temp")

  def isSystemCreatedActor(actorPath: String) = actorPath.contains("/system") || isUserGuardian(actorPath)

  def isUserCreatedActor(actorPath: String) = actorPath.contains("/user/")
}