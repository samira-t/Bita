Bita
====
Bita is a coverage-guided tool for testing actor programs written in [Akka](http://akka.io/). 
Based on three covearge criteria, it generates schedules and runs the program with the generated schedules.

Usage
====
To compile the source code:

<code>sbt compile</code>

To run test for bounded buffer example:

<code> sbt 'test-only test.integrationspecs.BufferSpec'</code>

This command runs the buffer program to obtain a random execution trace, generates schedules for all criteria and all optmizations,
runs the programs with those schedules, and measures the coverage obtained by running the program with each set of generated
schedules.

These steps in the test case for sleeping barber example is splitted into multiple tests. 
To run test for that obtains a random execution trace:

<code>> sbt 'test-only test.integrationspecs.BufferSpec --- -n random'</code>

To generate schedules (the criterion in the test in confugured for PMHR; however, you can change it if you want

<code>> sbt 'test-only test.integrationspecs.BufferSpec --- -n generate'</code>

To test with the generated schedules:

<code>> sbt 'test-only test.integrationspecs.BufferSpec --- -n test'</code>

To measure the coverage:

<code>> sbt 'test-only test.integrationspecs.BufferSpec --- -n coverage'</code>

To run with random scheduling (currently the it runs with a timeout of 5 minutes but can be configured with different
timeout):

<code>> sbt 'test-only test.integrationspecs.BufferSpec --- -n random-timeout'</code>

To create jar file:
<code>> release.sh</code>





