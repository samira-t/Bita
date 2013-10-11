Bita
====
Bita is a coverage-guided tool for testing actor programs written in [Akka](http://akka.io/). 
Based on three covearge criteria, it generates schedules and runs the program with the generated schedules.

## Usage

### Build

To compile the source code:

<code>> sbt compile</code>

To create a jar file from the source code:

<code>> release.sh</code>

This command will create the jar file in `target/scala-2.9.2/`. For the information about how to use the jar file see
[How to Use by Example](http://bita.cs.illinois.edu/example.html) page.


### Run 

#### Bounded Buffer Example

To run the test for bounded buffer example:

<code>> sbt 'test-only test.integrationspecs.BufferSpec'</code>

This command runs the buffer program to obtain a random execution trace, generates schedules for all criteria and all optmizations,
runs the programs with those schedules, and measures the coverage obtained by running the program with each set of generated
schedules.

#### Sleeping Barber Example

Various steps of testing mentioned in the bounded buffer example are splitted into multiple tests in the test case for 
sleeping barber.

To run the test that obtains a random execution trace:

<code>> sbt 'test-only test.integrationspecs.BarberSpec --- -n random'</code>

To generate schedules (the criterion in the test in configured for PMHR; however, you can change it if you want)

<code>> sbt 'test-only test.integrationspecs.BarberSpec --- -n generate'</code>

To test with the generated schedules:

<code>> sbt 'test-only test.integrationspecs.BarberSpec --- -n test'</code>

To measure the coverage:

<code>> sbt 'test-only test.integrationspecs.BarberSpec --- -n coverage'</code>

To run with random scheduling (currently the it runs with a timeout of 5 minutes but it can be configured with different
timeout):

<code>> sbt 'test-only test.integrationspecs.BarberSpec --- -n random-timeout'</code>







