package bita

/**
 * @author Samira Tasharofi (tasharo1@illinois.edu)
 */

import sbt._
import sbt.Keys._
import com.typesafe.sbtaspectj.AspectjPlugin
import com.typesafe.sbtaspectj.AspectjPlugin.{ Aspectj, inputs, aspectFilter, weave }

object SetakBuild extends Build {
  lazy val sample = Project(
    id = "bita",
    base = file("."),
    settings = Defaults.defaultSettings ++ AspectjPlugin.settings ++ Seq(
      organization := "cs.edu.uiuc",
      version := "0.1",
      scalaVersion := "2.9.2",
      resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/",
      libraryDependencies += "com.typesafe.akka" % "akka-actor" % "2.0.3",
      libraryDependencies += "org.scalatest" %% "scalatest" % "2.0.M5b",

 
      parallelExecution in Test := false,

      inputs in Aspectj <<=  update map { report =>
        report.matching(moduleFilter(organization = "com.typesafe.akka", name = "akka-actor"))
      },
      
      aspectFilter in Aspectj := {
        (jar, aspects) =>
          {
            if (jar.name.contains("akka-actor") )
              aspects filter (jar => (jar.name.startsWith("Actor")))
            else Seq.empty[File]
          }
      },
      
      fullClasspath in Test <<= AspectjPlugin.useInstrumentedJars(Test),
      fullClasspath in Runtime <<= AspectjPlugin.useInstrumentedJars(Runtime)))
}
