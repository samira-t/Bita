#!/bin/sh
# Builds and creates a jar file containing the tool and the instrumented library

sbt compile
sbt aspectj:weave
cd target/scala-2.9.2/classes/
jar -xvf ../aspectj/akka-actor-2.0.3-instrumented.jar
rm -r META-INF/
cd ../../..
sbt package
cp target/scala-2.9.2/bita_2.9.2-0.1-Release.jar ../TestRelease/lib/

